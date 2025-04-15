'use strict';

//const admin = require("firebase-admin");
const { Firestore } = require('@google-cloud/firestore');
const { ExecutionsClient } = require('@google-cloud/workflows');
const { PubSub } = require('@google-cloud/pubsub');
const { CloudTasksClient } = require('@google-cloud/tasks');
const logger = require('./logger');
const config = require('../config.json');
const Handlebars = require("handlebars");
const utils = require('./utils');
const momentz = require('moment-timezone');
const MomentRange = require('moment-range');
const moment = MomentRange.extendMoment(momentz);
const axios = require('axios');
const { google, outlook, office365, yahoo, ics } = require("calendar-link");
const { ERRCODE } = require('./errcode');
const _ = require('lodash');
const XLSX = require('xlsx');
const mysql = require('./mysql');
const twilio = require('twilio');
const vcard = require('./vcard');
const cm = require('./cache_manager');

// var meetingApp = admin.initializeApp({
//     credential: admin.credential.cert(config.SERVICE_ACCOUNT)
// }, "meetingApp");


const pubSubClient = new PubSub({
    projectId: config.GCP.PROJECT_ID,
    keyFilename: (__dirname + config.GCP.KEY_FILE_PATH).replace('/lib/', '/')
});
const dbClient = new Firestore({
    projectId: config.GCP.PROJECT_ID,
    keyFilename: (__dirname + config.GCP.KEY_FILE_PATH).replace('/lib/', '/')
});
const taskClient = new CloudTasksClient({
    projectId: config.GCP.PROJECT_ID,
    keyFilename: (__dirname + config.GCP.KEY_FILE_PATH).replace('/lib/', '/')
});

async function get_meeting_qna(payload) {
    return new Promise(async (resolve, reject) => {
        let ret_val = { status: -1 };

        if (!payload.key || !payload.key.instanceId || !payload.key.clientId || !payload.key.eventId) {
            ret_val = ERRCODE.PAYLOAD_ERROR;
            reject(ret_val);
        }

        const event_base_path = `/${payload.key.instanceId}_${payload.key.clientId}${payload.key.eventId}`;
        let attendeeId = payload.auth.data.UserId;

        if (payload.data?.AttendeeId) {
            attendeeId = payload.data.AttendeeId.toString();
        }

        try {
            // Get the attendee document
            const attendeeRef = await dbClient
                .collection(event_base_path)
                .doc("AttendeeList")
                .collection("Attendees")
                .doc(attendeeId)
                .get();

            if (!attendeeRef.exists) {
                ret_val = ERRCODE.PAYLOAD_ERROR;
                reject(ret_val);
            }

            const attendeeData = attendeeRef.data();
            const registrationType = attendeeData?.RegistrationType?.RegistrationType?.toLowerCase();
            console.log("registrationType", registrationType)

            if (!registrationType) {
                ret_val = ERRCODE.PAYLOAD_ERROR;
                reject(ret_val);
            }

            // Get questions collection
            const questionSnap = await dbClient
                .collection(event_base_path)
                .doc("MeetingQnA")
                .collection("questions")
                .where("isPublished", "==", true)
                .orderBy("displayOrder")
                .get();

            let questions = [];

            questionSnap.forEach(doc => {
                const data = doc.data();
                const roleVisibility = data.roleVisibility?.map(r => r.toLowerCase()) || [];
                console.log("roleVisibility", roleVisibility)

                if (roleVisibility.includes(registrationType)) {
                    questions.push({ id: doc.id, ...data });
                }
            });

            ret_val.status = 0;
            ret_val.result = questions;
            resolve(ret_val);
        } catch (err) {
            logger.log(err);
            ret_val.err = err;
            reject(ret_val);
        }
    });
}
async function set_meeting_qna(payload) {
    return new Promise(async (resolve, reject) => {
        let ret_val = { status: -1 };

        if (!payload.key || !payload.key.instanceId || !payload.key.clientId || !payload.key.eventId ||
            !payload.data || !Array.isArray(payload.data.answers)) {
            ret_val = ERRCODE.PAYLOAD_ERROR;
            return reject(ret_val);
        }
        let iceId = `${payload.key.instanceId}_${payload.key.clientId}${payload.key.eventId}`;
        let event_base_path = `/${iceId}`;

        let attendeeId = payload.auth.data.UserId;
        if (payload.data?.AttendeeId) {
            attendeeId = payload.data.AttendeeId.toString();
        }
        console.log("attendeeId", attendeeId)

        try {
            // Validate attendee
            let attendeeRef = dbClient
                .collection(event_base_path)
                .doc("AttendeeList")
                .collection("Attendees")
                .doc(attendeeId);

            let attendeeDoc = await attendeeRef.get();
            let attendeeData = attendeeDoc.data();
            let registrationType = attendeeData?.RegistrationType?.RegistrationType?.toLowerCase();
            let registrationTypeEntityId = attendeeData?.RegistrationType?.RegistrationTypeEntityId ?? null;
            console.log("registrationType", registrationType)
            console.log("registrationTypeEntityId", registrationTypeEntityId)
            if (!attendeeDoc.exists) {
                ret_val = ERRCODE.PAYLOAD_ERROR;
                return reject(ret_val);
            }

            // Process answers in Firestore
            let batch = dbClient.batch();
            let mysqlValues = [];

            for (const answer of payload.data.answers) {
                if (answer.questionId && answer.selectedValue && answer.questionLabel) {
                    let answerRef = attendeeRef.collection("MeetingQnA").doc(answer.questionId);
                    batch.set(answerRef, {
                        selectedValue: answer.selectedValue,
                        questionLabel: answer.questionLabel,
                        updatedAt: new Date()
                    }, { merge: true });

                    // Prepare MySQL bulk insert values
                    mysqlValues.push([
                        iceId, // iceId
                        attendeeId,
                        registrationType || "",
                        registrationTypeEntityId || "",
                        answer.questionId,
                        answer.questionLabel,
                        answer.selectedValue
                    ]);
                }
            }

            // Commit Firestore batch
            await batch.commit();

            // Perform bulk insert in MySQL
            if (mysqlValues.length > 0) {
                const sql = `
                    INSERT INTO qna (iceId, attendeeId, attendeeType, attendeeTypeEntityId, questionId, questionLabel, selectedValue, insertDateTime, updateDateTime)
                    VALUES ${mysqlValues.map(() => '(?, ?, ?, ?, ?, ?, ?, NOW(), NOW())').join(', ')}
                    ON DUPLICATE KEY UPDATE 
                        selectedValue = VALUES(selectedValue), 
                        questionLabel = VALUES(questionLabel), 
                        updateDateTime = NOW();
                `;

                const flattenedValues = mysqlValues.flat(); // Convert array of arrays into single array

                await mysql.executeQuery(sql, flattenedValues);
            }
            ret_val.status = 0;
            ret_val.message = "Answers saved successfully";
            return resolve(ret_val);
        } catch (err) {
            console.error("Error in setMeetingQnA:", err);
            ret_val.err = err;
            return reject(ret_val);
        }
    });
}
async function draft_attendees(payload) {
    let ret_val = { status: -1 }
    try {
        const iceId = `${payload.key.instanceId}_${payload.key.clientId}${payload.key.eventId}`;
        const eventBasePath = `/${iceId}`;
        console.log("eventBasePath", eventBasePath);
        let attendeeId = (payload.data.attendeeId) ? payload.data.attendeeId : payload.auth.data.UserId;
        //console.log(attendeeId)
        attendeeId = attendeeId.toString().trim();
        //console.log(attendeeId)        
        let attendeeDoc = await dbClient.collection(eventBasePath).doc("AttendeeList").collection("Attendees").doc(attendeeId).get();
        let attendeeData = attendeeDoc.data();
        let registrationTypeEntityId = attendeeData?.RegistrationType?.RegistrationTypeEntityId ?? null;
        if (payload.data?.sponsorId) {
            registrationTypeEntityId = payload.data.SponsorId
        }

        //console.log("registrationTypeEntityId", registrationTypeEntityId)
        if (!attendeeDoc.exists || !registrationTypeEntityId) {
            ret_val.err = new Error("Attendee not found or registrationTypeEntityId is missing");
            throw ret_val
        }



        let excludedAttendeeIds = new Set();
        // Fetch attendees with less than 2 confirmed meetings from MySQL
        const confirmedMeetings = await mysql.executeQuery(
            `SELECT inviteeId, COUNT(*) AS count FROM meeting 
             WHERE requestStatus = 'confirmed'
             GROUP BY inviteeId`, []);
        if (confirmedMeetings && confirmedMeetings.length > 0) {
            excludedAttendeeIds = new Set(
                confirmedMeetings.filter(m => m.count >= 2).map(m => m.inviteeId)
            );
        }

        let draftedMeetings = await mysql.executeQuery(
            `SELECT inviteeId, requestorId FROM meeting 
             WHERE requestorTypeEntityId = ? AND requestStatus = 'draft'`,
            [registrationTypeEntityId],
            true
        );
        // Always ensure it's an array
        draftedMeetings = Array.isArray(draftedMeetings) ? draftedMeetings : [];


        let filteredDraftedMeetings = []
        if (draftedMeetings && draftedMeetings.length > 0) {
            filteredDraftedMeetings = draftedMeetings.filter(
                attendee => !excludedAttendeeIds.has(attendee.inviteeId)
            );
        }
        //draftedMeetings looks like { inviteeId: '1324000', requestorId: '99934194' }
        let draftedAttendees = [];
        if (filteredDraftedMeetings && filteredDraftedMeetings.length > 0) {
            // Fetch attendee details from Firestore
            for (const { inviteeId, requestorId } of filteredDraftedMeetings) {
                let attendeeSnapshot = await dbClient.collection(eventBasePath).doc("AttendeeList").collection("Attendees").doc(inviteeId).get();
                let repSnapshot = await dbClient.collection(eventBasePath).doc("AttendeeList").collection("Attendees").doc(requestorId).get();
                if (attendeeSnapshot.exists && repSnapshot.exists) {
                    let attendeeData = attendeeSnapshot.data();
                    let repData = repSnapshot.data();
                    draftedAttendees.push({
                        ...attendeeData,
                        draftedBy: {
                            name: repData.Name,
                            email: repData.Email,
                            designation: repData.Designation,
                            company: repData.Company
                        }
                    });
                }
            }
        }
        ret_val.status = 0;
        ret_val.attendees = draftedAttendees;
        return { draftedAttendees };
    } catch (error) {
        console.error("Error fetching drafted attendees:", error);
        ret_val.err = error;
        throw ret_val;
    }
}
async function available_attendees(payload) {
    //return new Promise(async (resolve, reject) => {
    let ret_val = { status: -1 };
    try {

        if (!payload.key || !payload.key.instanceId || !payload.key.clientId || !payload.key.eventId) {
            ret_val = ERRCODE.PAYLOAD_ERROR;
            throw ret_val;
        }
        if (!payload.data) {
            payload.data = {}
        }
        const { instanceId, clientId, eventId } = payload.key;
        const iceId = `${instanceId}_${clientId}${eventId}`;
        const eventBasePath = `/${iceId}`;
        const attendeeId = (payload.data.attendeeId) ? payload.data.attendeeId : payload.auth.data.UserId;

        const attendeeDoc = await dbClient.collection(eventBasePath).doc("AttendeeList").collection("Attendees").doc(attendeeId).get();
        const attendeeData = attendeeDoc.data();

        const registrationTypeEntityId = attendeeData?.RegistrationType?.RegistrationTypeEntityId ?? null;



        if (!attendeeDoc.exists || !registrationTypeEntityId) {
            ret_val.err = "Attendee not found or registrationTypeEntityId is missing";
            throw ret_val;
        }



        const limit = payload.data.limit || 2000;
        const page = payload.data.page || 1;
        const filterObj = payload.data.filter || { operator: "AND", data: [] };
        const sort = payload.data.sort || { field: "Name", order: "ASC" };



        const confirmedMeetings = await mysql.executeQuery(
            `SELECT inviteeId, COUNT(*) AS count FROM meeting 
             WHERE requestStatus = 'confirmed' GROUP BY inviteeId`
        );
        let excludedIds = new Set();
        if (confirmedMeetings && confirmedMeetings.length > 0) {
            excludedIds = new Set(
                confirmedMeetings.filter(row => row.count >= 2).map(row => row.inviteeId)
            );
        }

        const draftedMeetings = await mysql.executeQuery(
            `SELECT inviteeId FROM meeting WHERE requestorTypeEntityId = ? AND requestStatus = 'save as draft'`,
            [registrationTypeEntityId]
        );
        let draftedIds = new Set();
        if (draftedMeetings && draftedMeetings.length > 0) {
            const draftedIds = new Set(draftedMeetings.map(row => row.inviteeId));
        }

        const cacheKey = `${config.INSTANCE}/${eventBasePath}/AttendeeList`;


        const snapshot = await _get_from_cache_or_db(cacheKey, async () => {
            const userSnap = await dbClient.collection(eventBasePath).doc("AttendeeList").collection("Attendees")
                .where("RegistrationType.RegistrationType", "==", "Attendee")
                .get();
            return !userSnap.empty ? userSnap : null;
        });

        let attendees = [];

        snapshot.forEach(doc => {
            const data = doc.data();
            const formattedData = {
                AttendeeId: data.AttendeeId,
                Name: data.Name,
                Email: data.Email,
                Company: data.Company,
                Designation: data.Designation,
                RegistrationType: data.RegistrationType,
                CreatedDate: data.CreatedDate,
                Phone: data.Phone,
                VCard: data.VCard,
                Meetings: data.Meetings,
                Slots: data.Slots
            }
            const id = data.AttendeeId;

            const matches = filterObj.data.map(f => _wild_card_match(data[f.field], f.value));
            const matchesSearch = filterObj.operator === "OR" ? matches.some(Boolean) : matches.every(Boolean);

            const isConfirmed = excludedIds.has(id);
            const isDrafted = draftedIds.has(id);

            if (!isConfirmed && !isDrafted && matchesSearch) {
                attendees.push(formattedData);
            }
        });

        // Sorting
        attendees.sort((a, b) => {
            const valA = a[sort.field]?.toLowerCase?.() || a[sort.field] || "";
            const valB = b[sort.field]?.toLowerCase?.() || b[sort.field] || "";
            if (valA < valB) return sort.order === "DESC" ? 1 : -1;
            if (valA > valB) return sort.order === "DESC" ? -1 : 1;
            return 0;
        });

        // Simulate pagination
        const offset = (page - 1) * limit;
        const paginatedAttendees = attendees.slice(offset, offset + limit);
        ret_val.status = 0;
        ret_val.data = {
            attendees: paginatedAttendees,
            total: attendees.length,
            page,
            totalPages: Math.ceil(attendees.length / limit),
        }
        return ret_val;
    } catch (err) {
        console.log(err);
        ret_val.err = err;
        throw ret_val;
    }
    //})
}
async function available_speakers(payload) {
    let ret_val = { status: -1 };

    try {
        const { instanceId, clientId, eventId } = payload.key;
        const iceId = `${instanceId}_${clientId}${eventId}`;
        const eventBasePath = `/${iceId}`;

        // Step 1: Fetch available speakers

        const cacheKey = `${config.INSTANCE}/${eventBasePath}/SpeakerList`;

        const snapshot = await _get_from_cache_or_db(cacheKey, async () => {
            const userSnap = await dbClient.collection(eventBasePath).doc("SpeakerList").collection("Speakers").get();
            return !userSnap.empty ? userSnap : null;
        });


        if (snapshot.empty) {
            ret_val.status = 0;
            ret_val.data = [];
            return ret_val;
        }

        // Step 2: Process speaker details
        if (snapshot.docs && snapshot.docs.length > 0) {
            ret_val.data = snapshot.docs.map(doc => {
                const data = doc.data();
                return {
                    speakerId: doc.id,
                    name: data.Name || `${data.FirstName || ''} ${data.LastName || ''}`.trim(),
                    email: data.Email || '',
                    company: data.Company || '',
                    designation: data.Designation || '',
                    phone: data.Phone || data.Mobile || '',
                    vcard: data.VCard || {},
                    profilePictureUrl: data.ProfilePictureURL || '',
                    createdDate: data.CreatedDate || '',
                    lastModifiedDate: data.LastModifiedDate || '',
                    resources: data.Resources || [],
                    socialLinks: data.SocialLinks || [],
                    website: data.Website || ''
                };
            });
        }

        ret_val.status = 0;
        return ret_val;

    } catch (err) {
        console.error("Error in available_speakers:", err);
        ret_val = ERRCODE.UNKNOWN_ERROR;
        return ret_val;
    }
}
async function available_sponsors(payload) {
    let ret_val = { status: -1 };

    try {
        const { instanceId, clientId, eventId } = payload.key;
        const iceId = `${instanceId}_${clientId}${eventId}`;
        const eventBasePath = `/${iceId}`;

        // Step 1: Fetch all sponsors
        let cacheKey = `${config.INSTANCE}/${eventBasePath}/SponsorList/all`;
        let showAll = payload.data?.showAll === true; // ensure it's explicitly true
        let sponsorsRef = dbClient.collection(`${eventBasePath}/SponsorList/Sponsors`);
        if (!showAll) {
            cacheKey = `${config.INSTANCE}/${eventBasePath}/SponsorList/prefered`;
            sponsorsRef = sponsorsRef.where('isMeetingEnabled', '==', true);
        }
        // let clearCache = payload.data?.clearCache === true; // ensure it's explicitly true
        // if(clearCache){
        //     cm.removeFromCache({cacheKey:cacheKey});
        // }
        // const sponsorSnap = await sponsorsRef.get();
        //console.log("sponsorSnap", sponsorSnap.docs.length)
        // let snapshot = await _get_from_cache_or_db(cacheKey, async () => {
        //     let userSnap = await sponsorsRef.get();
        //     return !userSnap.empty ? userSnap : null;
        // });

        let snapshot = await sponsorsRef.get();
        //console.log("sponsorSnap", snapshot.docs.length)

        let allSponsors = {};
        if (snapshot.docs && snapshot.docs.length > 0) {
            snapshot.docs.forEach(doc => {
                let data = doc.data();
                allSponsors[doc.id] = {
                    sponsorId: doc.id,
                    sponsorDetails: {
                        Name: data.Name || '',
                        Email: data.Email || '',
                        Phone: data.Phone || '',
                        Company: data.Company || '',
                        Category: data.Category || {},
                        Booth: data.Booth || '',
                        VCard: data.VCard || {},
                        Meetings: data.Meetings || [],
                        Slots: data.Slots || [],
                        Logo: data.Logo || ''
                    },
                    confirmedMeetingCount: 0,
                    confirmedMeetings: []
                };
            });
        }
        //console.log("allSponsors", Object.keys(allSponsors).length)

        if (allSponsors && Object.keys(allSponsors).length > 0) {

            // Step 2: Fetch confirmed meetings
            const confirmedMeetings = await mysql.executeQuery(
                `SELECT * FROM meeting WHERE iceId = ? AND requestStatus = 'confirmed'`,
                [iceId]
            );

            if (!confirmedMeetings && !confirmedMeetings.length) {
                ret_val.status = 0;
                ret_val.data = Object.values(allSponsors);
                return ret_val;
            }

            // Step 3: Organize meetings by sponsor
            const sponsorMeetingMap = {}; // sponsorId: [meeting, ...]
            const attendeeIds = new Set();

            for (const m of confirmedMeetings) {
                const isRequestorSponsor = m.requestorType.toLowerCase() === "sponsor";
                const sponsorId = isRequestorSponsor ? m.requestorTypeEntityId : m.inviteeTypeEntityId;
                const attendeeId = isRequestorSponsor ? m.inviteeId : m.requestorId;
                attendeeIds.add(attendeeId);

                if (!sponsorMeetingMap[sponsorId]) sponsorMeetingMap[sponsorId] = [];
                sponsorMeetingMap[sponsorId].push({
                    meetingId: m.id,
                    slot: m.slot,
                    timestamp: m.timestamp,
                    attendeeId
                });
            }

            // Step 4: Fetch attendee details
            const attendeeDocs = await Promise.all(
                [...attendeeIds].map(id =>
                    dbClient.doc(`${eventBasePath}/AttendeeList/Attendees/${id}`).get()
                )
            );

            const attendeeMap = {};
            for (const doc of attendeeDocs) {
                if (doc.exists) {
                    const d = doc.data();
                    attendeeMap[d.AttendeeId] = {
                        Name: d.Name || `${d.FirstName || ''} ${d.LastName || ''}`.trim(),
                        Designation: d.Designation || '',
                        Company: d.Company || '',
                        Phone: d.Phone || d.Mobile || '',
                    };
                }
            }

            // Step 5: Merge meeting data into sponsors
            for (const sponsorId in sponsorMeetingMap) {
                const meetings = sponsorMeetingMap[sponsorId];
                if (!allSponsors[sponsorId]) continue;

                allSponsors[sponsorId].confirmedMeetingCount = meetings.length;
                allSponsors[sponsorId].confirmedMeetings = meetings.map(m => ({
                    ...m,
                    ...attendeeMap[m.attendeeId]
                }));
            }

            //console.log("allSponsors values", Object.values(allSponsors))
            ret_val.data = Object.values(allSponsors);
        }
        ret_val.status = 0;
        return ret_val;

    } catch (err) {
        console.error("Error in available_sponsors:", err);
        ret_val = ERRCODE.UNKNOWN_ERROR;
        return ret_val;
    }
}
async function meeting_attendees(payload) {
    const ret_val = { status: -1, summary: {}, attendees: [] };

    const { instanceId, clientId, eventId } = payload.key || {};
    const sponsorId = payload.data?.sponsorId;

    if (!instanceId || !clientId || !eventId) {
        throw new Error("Missing instanceId, clientId, or eventId in payload.key");
    }
    const iceId = `${instanceId}_${clientId}${eventId}`;
    const eventPath = `/${iceId}`;


    // Always fetch all meetings for the event
    const meetings = await mysql.executeQuery(
        `SELECT * FROM meeting WHERE iceId = ?`,
        [iceId]
    );

    const attendeeStats = {};

    for (const meeting of meetings) {
        let attendeeId;

        if (sponsorId) {
            const relatedToSponsor = meeting.requestorTypeEntityId === sponsorId || meeting.inviteeTypeEntityId === sponsorId;
            if (!relatedToSponsor) continue;
            attendeeId = meeting.inviteeId === sponsorId ? meeting.requestorId : meeting.inviteeId;
        } else {
            attendeeId = meeting.inviteeId;
        }

        if (!attendeeStats[attendeeId]) {
            attendeeStats[attendeeId] = {
                attendeeId,
                drafted: 0,
                requested: 0,
                confirmed: 0,
                cancelled: 0
            };
        }

        const status = meeting.requestStatus;
        if (status === 'draft') attendeeStats[attendeeId].drafted++;
        else if (status === 'requested') attendeeStats[attendeeId].requested++;
        else if (status === 'confirmed') attendeeStats[attendeeId].confirmed++;
        else if (status === 'cancelled') attendeeStats[attendeeId].cancelled++;
    }

    const attendeeIds = Object.keys(attendeeStats);

    // Firestore fetches
    const attendeeDocs = await Promise.all(
        attendeeIds.map(id =>
            dbClient.collection(`${eventPath}/AttendeeList/Attendees`).doc(id).get()
        )
    );

    for (const doc of attendeeDocs) {
        const data = doc.data();
        const attendeeId = doc.id;
        const stats = attendeeStats[attendeeId];

        if (!data || !stats) continue;

        ret_val.attendees.push({
            AttendeeId: attendeeId,
            Name: data.Name || '',
            Designation: data.Designation || '',
            Company: data.Company || '',
            Phone: data.Phone || '',
            Meetings: data.Meetings || [],
            Slots: data.Slots || [],
            VCard: data.VCard || {},
            Drafted: stats.drafted,
            Requested: stats.requested,
            Confirmed: stats.confirmed,
            Cancelled: stats.cancelled
        });
    }

    // Sort attendees by most drafted
    ret_val.attendees.sort((a, b) => b.Drafted - a.Drafted);

    if (sponsorId) {
        const sponsorDoc = await dbClient
            .collection(`${eventPath}/SponsorList/Sponsors`)
            .doc(sponsorId.toString())
            .get();

        ret_val.summary = {
            sponsorId: sponsorId,
            sponsorName: sponsorDoc.exists ? sponsorDoc.data().Name : '',
            draftedCount: ret_val.attendees.reduce((sum, a) => sum + a.Drafted, 0),
            requestedCount: ret_val.attendees.reduce((sum, a) => sum + a.Requested, 0),
            confirmedCount: ret_val.attendees.reduce((sum, a) => sum + a.Confirmed, 0),
            cancelledCount: ret_val.attendees.reduce((sum, a) => sum + a.Cancelled, 0)
        };
    } else {
        ret_val.summary = {
            totalConfirmed: meetings.filter(m => m.requestStatus === 'confirmed').length
        };
    }

    ret_val.status = 0;
    return ret_val;
}
async function attendee_meetings(payload) {
    const ret_val = { status: -1, summary: {}, meetings: [] };

    const { instanceId, clientId, eventId } = payload.key || {};
    const attendeeId = payload.data?.attendeeId;

    if (!instanceId || !clientId || !eventId || !attendeeId) {
        ret_val.err = "Missing instanceId, clientId, eventId, or attendeeId in payload";
        return ret_val;
    }

    const iceId = `${instanceId}_${clientId}${eventId}`;
    const eventPath = `/${iceId}`;

    // Fetch all meetings involving the attendee
    const meetings = await mysql.executeQuery(
        `SELECT * FROM meeting 
         WHERE iceId = ? AND (requestorId = ? OR inviteeId = ?)`,
        [iceId, attendeeId, attendeeId]
    );

    const statusCounts = {
        draft: 0,
        requested: 0,
        confirmed: 0,
        cancelled: 0
    };

    for (const meeting of meetings) {
        // Determine the sponsorId for this meeting
        let sponsorId = null;

        if (meeting.requestorId === attendeeId) {
            sponsorId = meeting.inviteeTypeEntityId;
        } else if (meeting.inviteeId === attendeeId) {
            sponsorId = meeting.requestorTypeEntityId;
        }

        let sponsorName = '';
        if (sponsorId) {
            const sponsorDoc = await dbClient
                .collection(`${eventPath}/SponsorList/Sponsors`)
                .doc(sponsorId.toString())
                .get();

            sponsorName = sponsorDoc.exists ? sponsorDoc.data().Name || '' : '';
        }

        const status = meeting.requestStatus;
        let slot = ''
        if (status === 'confirmed') {
            slot = meeting.requestMeetingSlot
        }
        if (statusCounts.hasOwnProperty(status)) {
            statusCounts[status]++;
        }

        ret_val.meetings.push({
            SponsorId: sponsorId,
            SponsorName: sponsorName,
            Slot: slot,
            Status: status
        });
    }

    // Summary of meeting statuses
    ret_val.summary = {
        draft: statusCounts.draft,
        requested: statusCounts.requested,
        confirmed: statusCounts.confirmed,
        cancelled: statusCounts.cancelled
    };

    ret_val.status = 0;
    return ret_val;
}
async function save_as_draft(payload) {
    const results = await Promise.allSettled(
        payload.data.inviteeIds.map(inviteeId => {
            const individualPayload = {
                ...payload,
                data: { inviteeId }
            };
            return _save_as_draft(individualPayload);
        })
    );

    return results.map((result, idx) => {
        if (result.status === "fulfilled") {
            return { inviteeId: payload.data.inviteeIds[idx], success: true, message: result.value.message };
        } else {
            return { inviteeId: payload.data.inviteeIds[idx], success: false, error: result.reason.message };
        }
    });
}
async function remove_from_draft(payload) {
    try {
        const iceId = `${payload.key.instanceId}_${payload.key.clientId}${payload.key.eventId}`;
        const eventBasePath = `/${iceId}`;
        const requestorId = payload.auth.data.UserId;
        const inviteeIds = payload.data.inviteeIds;

        const requestorRef = db.collection(eventBasePath).doc("AttendeeList").collection("Attendees").doc(requestorId);
        const requestorDoc = await requestorRef.get();
        const requestorData = requestorDoc.data();
        const registrationTypeEntityId = requestorData?.RegistrationType?.RegistrationTypeEntityId ?? null;

        if (!requestorDoc.exists || !registrationTypeEntityId) {
            throw new Error("Requestor not found or registrationTypeEntityId is missing");
        }

        if (!Array.isArray(inviteeIds) || inviteeIds.length === 0) {
            throw new Error("No inviteeIds provided for removal");
        }

        // Build placeholders (?, ?, ...) dynamically for the IN clause
        const placeholders = inviteeIds.map(() => '?').join(',');
        const values = [registrationTypeEntityId, ...inviteeIds];

        await mysql.executeQuery(
            `DELETE FROM meeting 
             WHERE requestorTypeEntityId = ? AND inviteeId IN (${placeholders}) AND requestStatus = 'save as draft'`,
            values
        );

        return { success: true, message: "Invitees removed from draft successfully" };
    } catch (error) {
        console.error("Error removing attendee(s) from draft:", error);
        throw error;
    }
}
async function old_request_meetings(payload) {
    const MIN_REQUESTS = 2;
    const ret_val = { status: -1, created: [], skipped: [], cancelled: [] };

    if (!payload?.key || !payload?.data?.RequestorId || !Array.isArray(payload?.data?.AttendeeIds)) {
        throw { status: -1, message: "Invalid payload" };
    }

    const { instanceId, clientId, eventId } = payload.key;
    const iceId = `${instanceId}_${clientId}${eventId}`;
    const eventPath = `/${iceId}`;
    const requestorId = (payload.data.RequestorId) ? payload.data.RequestorId : payload.auth.data.UserId;
    const attendeeIds = payload.data.AttendeeIds;

    // Fetch Requestor Info
    const requestorRef = dbClient.collection(`${eventPath}/AttendeeList/Attendees`).doc(requestorId.toString());
    const requestorDoc = await requestorRef.get();
    const requestor = requestorDoc.data();


    let registrationType = requestor?.RegistrationType?.RegistrationType?.toLowerCase();
    let registrationTypeEntityId = requestor?.RegistrationType?.RegistrationTypeEntityId ?? null;
    if (registrationType !== 'sponsor' || !registrationTypeEntityId) {
        throw { status: -1, message: "Only sponsor representatives can request meetings." };
    }

    if (attendeeIds.length < MIN_REQUESTS) {
        throw { status: -1, message: `Minimum ${MIN_REQUESTS} attendees required.` };
    }

    const now = new Date();

    // Step 1: Process attendees in parallel
    const attendeeFetchPromises = attendeeIds.map(async (id) => {
        const doc = await dbClient.collection(`${eventPath}/AttendeeList/Attendees`).doc(id.toString()).get();
        const attendee = doc.data();
        if (!attendee || !attendee.AttendeeId) return null;

        const existingMeetings = await mysql.executeQuery(
            "SELECT meetingId, requestStatus FROM meeting WHERE iceId = ? AND (requestorId = ? OR inviteeId = ?) AND requestStatus = 'confirmed'",
            [iceId, attendee.AttendeeId, attendee.AttendeeId], true
        );

        if (existingMeetings.length >= 2) {
            // Cancel pending/requested meetings
            // await executeQuery(
            //     "UPDATE meeting SET requestStatus = 'cancelled' WHERE iceId = ? AND (requestorId = ? AND inviteeId = ?) AND requestStatus IN ('draft', 'requested')",
            //     [iceId, attendee.AttendeeId, attendee.AttendeeId]
            // );
            return { skipped: attendee.AttendeeId };
        }

        return { valid: attendee };
    });

    let attendeeResults = await Promise.allSettled(attendeeFetchPromises);
    //console.log("attendeeResults", attendeeResults)
    let validAttendees = [];
    attendeeResults.forEach(result => {
        if (result.status === 'fulfilled') {
            if (result.value?.valid) validAttendees.push(result.value.valid);
            if (result.value?.skipped) ret_val.skipped.push(result.value.skipped);
        }
    });
    //console.log("validAttendees", validAttendees)

    // Step 2: Create meetings in parallel
    const meetingPromises = validAttendees.map(async (invitee) => {
        const meetingData = {
            Requestor: {
                AttendeeId: requestor.AttendeeId,
                Name: requestor.Name,
                Company: requestor.Company,
                Designation: requestor.Designation
            },
            Invitee: {
                AttendeeId: invitee.AttendeeId,
                Name: invitee.Name,
                Company: invitee.Company,
                Designation: invitee.Designation
            },
            Slots: [],
            Status: 'Requested',
            CreateDateTime: now
        };

        try {
            // Create meeting in Firestore
            const meetingRef = await dbClient.collection(`${eventPath}/MeetingList/Meetings`).add(meetingData);
            const meetingId = meetingRef.id;

            await meetingRef.set({ MeetingId: meetingId }, { merge: true });

            // Update requestor's document
            await dbClient.collection(`${eventPath}/AttendeeList/Attendees`).doc(requestor.AttendeeId.toString()).set({
                Meetings: Firestore.FieldValue.arrayUnion(meetingId)
            }, { merge: true });

            //Add analytics
            const MeetingAnalyticData = {
                meetingId,
                meetingTsUTC: moment(payload.data.Slots[0]).utc().unix(),
                meetingType: "one2one",
                subject: payload.data.Message || "",
                timeZone: payload.data.Timezone || "UTC",
                updated: moment().utc().unix(),
                status: 'Requested'
            };
            await _add_meeting_analytics(payload, MeetingAnalyticData, meetingData.Requestor, meetingData.Invitee);

            // ✅ Update MySQL meeting table
            await mysql.executeQuery(
                `INSERT INTO meeting (meetingCode, iceId, requestorId,requestorType,requestorTypeEntityId, inviteeId,inviteeTypeEntityId, requestStatus, requestUpdateDateTime)
                 VALUES (?, ?, ?, ?, ?, ?, '', 'requested', ?)
                 ON DUPLICATE KEY UPDATE requestStatus = 'requested'`,
                [meetingId, iceId, requestor.AttendeeId, registrationType, registrationTypeEntityId, invitee.AttendeeId, now], true
            );

            return { success: true, meetingId };
        } catch (err) {
            console.error(`❌ Failed to create meeting for ${invitee.AttendeeId}:`, err);
            return { success: false, attendeeId: invitee.AttendeeId };
        }
    });

    const creationResults = await Promise.allSettled(meetingPromises);

    creationResults.forEach(result => {
        if (result.status === 'fulfilled') {
            const res = result.value;
            if (res.success) {
                ret_val.created.push(res.meetingId);
            } else {
                ret_val.skipped.push(res.attendeeId);
            }
        } else {
            console.error("Unhandled rejection during meeting creation:", result.reason);
        }
    });

    ret_val.status = 0;
    return ret_val;
}
async function request_meetings(payload) {
    const ret_val = { status: -1, created: [], skipped: [], cancelled: [] };

    if (!payload?.key || !payload?.data?.RequestorId) {
        throw { status: -1, message: "Invalid payload" };
    }

    const { instanceId, clientId, eventId } = payload.key;
    const iceId = `${instanceId}_${clientId}${eventId}`;
    const eventPath = `/${iceId}`;
    const requestorId = payload.data.RequestorId || payload.auth?.data?.UserId;
    let attendeeIds = payload.data?.AttendeeIds

    const now = new Date();

    // Fetch requestor data
    const requestorSnap = await dbClient.collection(`${eventPath}/AttendeeList/Attendees`).doc(requestorId.toString()).get();
    const requestor = requestorSnap.data();
    if (!requestor || !requestor.RegistrationType) {
        throw { status: -1, message: "Invalid or missing requestor data" };
    }

    let sponsorInPayload = false;
    let registrationType = requestor.RegistrationType.RegistrationType.toLowerCase();
    let registrationTypeEntityId = requestor.RegistrationType.RegistrationTypeEntityId ?? null;

    // Check if requestor is sponsor
    if (registrationType === "sponsor") {
        sponsorInPayload = true;
    }
    if (!attendeeIds) {
        attendeeIds = []
    }

    if (sponsorInPayload && attendeeIds.length === 0) {
        const draftMeetings = await mysql.executeQuery(
            `SELECT inviteeId FROM meeting 
             WHERE iceId = ? 
             AND requestorType = 'sponsor' 
             AND requestorTypeEntityId = ? 
             AND requestStatus = 'draft'`,
            [iceId, registrationTypeEntityId],
            true
        );

        if (!draftMeetings.length) {
            throw { status: -1, message: "No draft attendees found for sponsor." };
        }

        attendeeIds = draftMeetings.map(row => row.inviteeId);
        payload.data.AttendeeIds = attendeeIds;
    }

    // If not, check if any invitee is sponsor
    let inviteeDocs = await Promise.all(attendeeIds.map(id =>
        dbClient.collection(`${eventPath}/AttendeeList/Attendees`).doc(id.toString()).get()
    ));

    const invitees = inviteeDocs
        .map(doc => doc.exists ? doc.data() : null)
        .filter(Boolean);

    if (!sponsorInPayload) {
        for (const invitee of invitees) {
            const regType = invitee?.RegistrationType?.RegistrationType?.toLowerCase();
            if (regType === "sponsor") {
                sponsorInPayload = true;
                registrationTypeEntityId = invitee.RegistrationType.RegistrationTypeEntityId ?? null;
                break;
            }
        }
    }

    if (!sponsorInPayload && !registrationTypeEntityId) {
        registrationTypeEntityId = requestor.RegistrationType.RegistrationTypeEntityId ?? '';
    }



    const MIN_REQUESTS = sponsorInPayload ? config.SPONSOR_MIN_REQUESTS : config.ATTENDEE_MIN_REQUESTS;
    const MAX_REQUESTS = sponsorInPayload ? config.SPONSOR_MAX_REQUESTS : config.ATTENDEE_MAX_REQUESTS;
    console.log("MIN_REQUESTS===", MIN_REQUESTS)
    console.log("MAX_REQUESTS===", MAX_REQUESTS)
    if (attendeeIds.length < MIN_REQUESTS) {
        throw { status: -1, message: `Minimum ${MIN_REQUESTS} request required.` };
    }
    if (attendeeIds.length > MAX_REQUESTS) {
        throw { status: -1, message: `Maximun ${MAX_REQUESTS} request allowed.` };
    }

    // Step 1: Filter valid invitees
    const attendeeCheckPromises = invitees.map(async (attendee) => {
        const confirmedMeetings = await mysql.executeQuery(
            "SELECT meetingId FROM meeting WHERE iceId = ? AND (requestorId = ? OR inviteeId = ?) AND requestStatus = 'confirmed'",
            [iceId, attendee.AttendeeId, attendee.AttendeeId],
            true
        );

        if (confirmedMeetings.length >= 2) {
            return { skipped: attendee.AttendeeId };
        }

        const existingMeetings = await mysql.executeQuery(
            "SELECT meetingId FROM meeting WHERE iceId = ? AND (requestorId = ? OR inviteeId = ?) AND (requestorTypeEntityId = ? OR inviteeTypeEntityId = ?)  AND requestStatus = 'confirmed'",
            [iceId, attendee.AttendeeId, attendee.AttendeeId, registrationTypeEntityId, registrationTypeEntityId],
            true
        );
        if (existingMeetings.length >= 1) {
            return { skipped: attendee.AttendeeId };
        }

        return { valid: attendee };
    });

    const checkResults = await Promise.allSettled(attendeeCheckPromises);
    const validAttendees = [];

    for (const result of checkResults) {
        if (result.status === "fulfilled") {
            const val = result.value;
            if (val.valid) validAttendees.push(val.valid);
            if (val.skipped) ret_val.skipped.push(val.skipped);
        }
    }

    // Step 2: Create meetings
    const meetingPromises = validAttendees.map(async (invitee) => {
        const meetingData = {
            Requestor: {
                AttendeeId: requestor.AttendeeId,
                Name: requestor.Name,
                Company: requestor.Company,
                Designation: requestor.Designation,
                Phone: requestor.Phone
            },
            Invitee: {
                AttendeeId: invitee.AttendeeId,
                Name: invitee.Name,
                Company: invitee.Company,
                Designation: invitee.Designation,
                Phone: invitee.Phone
            },
            Slots: [],
            Status: 'Requested',
            CreateDateTime: now
        };

        try {
            // Create meeting in Firestore
            const meetingRef = await dbClient.collection(`${eventPath}/MeetingList/Meetings`).add(meetingData);
            const meetingId = meetingRef.id;

            await meetingRef.set({ MeetingId: meetingId }, { merge: true });

            // Add MeetingId to requestor document
            // const requestorDocRef = dbClient.collection(`${eventPath}/AttendeeList/Attendees`).doc(requestor.AttendeeId.toString());
            // const requestorDoc = await requestorDocRef.get();
            // const requestorData = requestorDoc.data();

            // if (!requestorData.Meetings || !Array.isArray(requestorData.Meetings)) {
            //     await requestorDocRef.set({ Meetings: [meetingId] }, { merge: true });
            // } else if (!requestorData.Meetings.includes(meetingId)) {
            //     await requestorDocRef.update({
            //         Meetings: Firestore.FieldValue.arrayUnion(meetingId)
            //     });
            // }

            // ✅ Insert into MySQL
            await mysql.executeQuery(
                `INSERT INTO meeting (meetingCode, iceId, requestorId, requestorType, requestorTypeEntityId, inviteeId, inviteeType, inviteeTypeEntityId, requestStatus, requestUpdateDateTime)
                 VALUES (?, ?, ?, ?, ?, ?, 'attendee','', 'requested', ?)
                 ON DUPLICATE KEY UPDATE meetingCode=?, requestStatus = 'requested', requestUpdateDateTime=?`,
                [meetingId, iceId, requestor.AttendeeId, registrationType, registrationTypeEntityId, invitee.AttendeeId, now, meetingId, now],
                true
            );

            // Add analytics
            const MeetingAnalyticData = {
                meetingId,
                meetingTsUTC: moment(payload.data.Slots[0]).utc().unix(),
                meetingType: "one2one",
                subject: payload.data.Message || "",
                timeZone: payload.data.Timezone || "UTC",
                updated: moment().utc().unix(),
                status: 'Requested'
            };
            await _add_meeting_analytics(payload, MeetingAnalyticData, requestor, invitee);

            return { success: true, meetingId };
        } catch (err) {
            console.error(`❌ Failed to create meeting for ${invitee.AttendeeId}:`, err);
            return { success: false, attendeeId: invitee.AttendeeId };
        }
    });

    const creationResults = await Promise.allSettled(meetingPromises);

    for (const result of creationResults) {
        if (result.status === "fulfilled") {
            const res = result.value;
            if (res.success) ret_val.created.push(res.meetingId);
            else ret_val.skipped.push(res.attendeeId);
        } else {
            console.error("Unhandled error in meeting creation:", result.reason);
        }
    }

    ret_val.status = 0;
    return ret_val;
}
async function available_slots(payload) {
    const { key, data = {}, auth } = payload;
    const { instanceId, clientId, eventId } = key;
    const iceId = `${instanceId}_${clientId}${eventId}`;
    const eventPath = `/${iceId}`;
    const result = { status: -1, availableSlots: [] };

    // Fetch all configured slots from settings
    const configRef = await dbClient.collection(`${eventPath}/MeetingList/Settings`).doc("Config").get();
    if (!configRef.exists) throw new Error("Config not found");
    const config = configRef.data();
    const allSlots = config.Slots ?? [];

    const getEffectiveSlots = async (attendeeId) => {
        const attendeeRef = await dbClient.collection(`${eventPath}/AttendeeList/Attendees`).doc(attendeeId.toString()).get();
        if (!attendeeRef.exists) return [];

        const user = attendeeRef.data();
        const regType = user?.RegistrationType?.RegistrationType?.toLowerCase();
        const regEntityId = user?.RegistrationType?.RegistrationTypeEntityId;

        // If sponsor rep, get sponsor's slots
        if (regType === 'sponsor' && regEntityId) {
            const sponsorRef = await dbClient.collection(`${eventPath}/SponsorList/Sponsors`).doc(regEntityId.toString()).get();
            if (sponsorRef.exists) {
                const sponsor = sponsorRef.data();
                return sponsor.Slots || [];
            }
        }

        return user.Slots || [];
    };

    const requestorId = data?.requestorId;
    const inviteeId = data?.inviteeId;

    // Logic to determine which slots to fetch
    if (requestorId && inviteeId) {
        // Get common slots between sponsor (related to requestor or invitee) and the other attendee
        const reqSlots = await getEffectiveSlots(requestorId);
        const invSlots = await getEffectiveSlots(inviteeId);
        result.availableSlots = allSlots.filter(slot => reqSlots.includes(slot) && invSlots.includes(slot));
    } else if (requestorId || inviteeId) {
        const userId = requestorId || inviteeId;
        const userSlots = await getEffectiveSlots(userId);
        result.availableSlots = allSlots.filter(slot => !userSlots.includes(slot));
    } else {
        // Default to logged-in user
        const loggedInUserId = auth?.data?.UserId;
        const userSlots = await getEffectiveSlots(loggedInUserId);
        result.availableSlots = allSlots.filter(slot => !userSlots.includes(slot));
    }

    result.status = 0;
    return result;
}
function confirm_meeting(payload) {
    return new Promise(async (resolve, reject) => {
        logger.log(payload)
        let ret_val = { status: -1 };
        if (!payload.key) {
            ret_val = ERRCODE.PAYLOAD_ERROR
            reject(ret_val)
            return;
        }
        if (!payload.key.instanceId || !payload.key.clientId || !payload.key.eventId) {
            ret_val = ERRCODE.PAYLOAD_ERROR
            reject(ret_val)
            return;
        }
        const iceId = `${payload.key.instanceId}_${payload.key.clientId}${payload.key.eventId}`;
        const event_base_path = `/${iceId}`;
        let meetingRef = await dbClient.collection(event_base_path).doc("MeetingList").collection("Meetings").doc(payload.data.MeetingId).get()

        let Meeting = meetingRef.data();
        Meeting.Slots = [payload.data.Slot]
        const tasks = [];

        if (Meeting.Status === "Accepted") {
            ret_val.err = new Error("Meeting already accepted.");
            reject(ret_val)
        }

        // Fetch attendee data
        const [inviteeSnap, requestorSnap] = await Promise.all([
            dbClient.collection(event_base_path).doc("AttendeeList").collection("Attendees").doc(Meeting.Invitee.AttendeeId).get(),
            dbClient.collection(event_base_path).doc("AttendeeList").collection("Attendees").doc(Meeting.Requestor.AttendeeId).get(),
        ]);

        if (!inviteeSnap.exists || !requestorSnap.exists) {
            ret_val.err = new Error("Invitee or Requestor not found.");
            reject(ret_val)
        }

        let Invitee = inviteeSnap.data();
        let Requestor = requestorSnap.data();

        // Determine who is sponsor rep
        let isRequestorSponsorRep = Requestor.RegistrationType.RegistrationType === "Sponsor";
        let sponsorRep = isRequestorSponsorRep ? Requestor : Invitee;
        let attendee = isRequestorSponsorRep ? Invitee : Requestor;
        let sponsorId = sponsorRep.RegistrationType.RegistrationTypeEntityId;

        // Fetch sponsor data
        const sponsorSnap = await dbClient.collection(event_base_path).doc("SponsorList").collection("Sponsors").doc(sponsorId).get();
        if (!sponsorSnap.exists) {
            ret_val.err = new Error("Sponsor not found.");
            reject(ret_val)
        }

        let Sponsor = sponsorSnap.data();
        //console.log("Sponsor", Sponsor)

        // Initialize fields if missing
        sponsorRep.Meetings = sponsorRep.Meetings || [];
        sponsorRep.Slots = sponsorRep.Slots || [];
        attendee.Meetings = attendee.Meetings || [];
        attendee.Slots = attendee.Slots || [];
        Sponsor.Meetings = Sponsor.Meetings || [];
        Sponsor.Slots = Sponsor.Slots || [];

        // Check sponsor slot constraints
        if (Sponsor.Slots.length >= 10) {
            ret_val.err = new Error("Sponsor slots are full.");
            reject(ret_val)
        }

        const conflictingSponsorSlot = Meeting.Slots.some(slot => Sponsor.Slots.includes(slot));
        if (conflictingSponsorSlot) {
            ret_val.err = new Error("Sponsor slot already booked.");
            reject(ret_val)
        }

        // Check attendee slot constraints
        if (attendee.Slots.length >= 2) {
            ret_val.err = new Error("Attendee slots are full.");
            reject(ret_val)
        }

        const conflictingAttendeeSlot = Meeting.Slots.some(slot => attendee.Slots.includes(slot));
        if (conflictingAttendeeSlot) {
            ret_val.err = new Error("Attendee slot already booked.");
            reject(ret_val)
        }

        // Push meeting ID and slots
        if (!sponsorRep.Meetings.includes(payload.data.MeetingId)) {
            sponsorRep.Meetings.push(payload.data.MeetingId);
        }
        if (!attendee.Meetings.includes(payload.data.MeetingId)) {
            attendee.Meetings.push(payload.data.MeetingId);
        }
        if (!Sponsor.Meetings.includes(payload.data.MeetingId)) {
            Sponsor.Meetings.push(payload.data.MeetingId);
        }

        Meeting.Slots.forEach(slot => {
            sponsorRep.Slots.push(slot);
            attendee.Slots.push(slot);
            Sponsor.Slots.push(slot);
        });

        // Update Firestore
        tasks.push(
            dbClient.collection(event_base_path).doc("MeetingList").collection("Meetings").doc(payload.data.MeetingId)
                .set({ SponsorId: sponsorId, LastUpdatedDateTime: new Date(), Slots: Meeting.Slots, Status: "Accepted" }, { merge: true })
        );

        tasks.push(
            dbClient.collection(event_base_path).doc("AttendeeList").collection("Attendees").doc(sponsorRep.AttendeeId)
                .set({ Meetings: sponsorRep.Meetings, Slots: sponsorRep.Slots }, { merge: true })
        );

        tasks.push(
            dbClient.collection(event_base_path).doc("AttendeeList").collection("Attendees").doc(attendee.AttendeeId)
                .set({ Meetings: attendee.Meetings, Slots: attendee.Slots }, { merge: true })
        );

        tasks.push(
            dbClient.collection(event_base_path).doc("SponsorList").collection("Sponsors").doc(sponsorId)
                .set({ Meetings: Sponsor.Meetings, Slots: Sponsor.Slots }, { merge: true })
        );

        // Update MySQL
        tasks.push(
            mysql.executeQuery('UPDATE meeting SET requestStatus = ?, requestMeetingSlot = ? WHERE meetingCode = ?', ['confirmed', payload.data.Slot, payload.data.MeetingId], true)
        );
        //1324000 99934194 
        Promise.all(tasks)
            .then(async (res) => {
                let tasks = []
                if (res.length) {
                    let topicName = 'confirm-meeting';
                    let pubsubPayload = {
                        Payload: payload,
                        Meeting: Meeting,
                        Requestor: Requestor,
                        Invitee: Invitee
                    }
                    //console.log("pubsubPayload", pubsubPayload)
                    // let RequestorData = {
                    //     Initials: (Invitee.Tags || ""),
                    //     Name: (Invitee.Name || ""),
                    //     ProfilePictureURL: (Invitee.ProfilePictureURL || ""),
                    //     MeetingType: "One2OneMeeting/MeetingAccepted",
                    //     NotificationMessage: "Meeting confirmed by " + (Invitee.Name || ""),
                    //     NotificationTitle: "Meeting Request Confirmed"
                    // }

                    // if (RequestorData.MeetingType && payload.auth) {
                    //     let NotificationTemplatePath = '/OA_UAT/NotificationTPL/' + RequestorData.MeetingType;
                    //     let TempRef = await dbClient.doc(NotificationTemplatePath).get()
                    //     if (TempRef.exists && payload.auth) {
                    //         let NotificationTemplate = TempRef.data()
                    //         let notificationMessageTemplate = Handlebars.compile(NotificationTemplate.Message);
                    //         RequestorData.NotificationMessage = notificationMessageTemplate(Invitee);
                    //         let notificationTitleTemplate = Handlebars.compile(NotificationTemplate.Title);
                    //         RequestorData.NotificationTitle = notificationTitleTemplate(Invitee);

                    //     }
                    // }
                    // tasks.push(utils.savePushAttendee(payload, event_base_path, Requestor.AttendeeId, RequestorData))

                    // let InviteeData = {
                    //     Initials: (Requestor.Tags || ""),
                    //     Name: (Requestor.Name || ""),
                    //     ProfilePictureURL: (Requestor.ProfilePictureURL || ""),
                    //     MeetingType: "One2OneMeeting/MeetingAccepted",
                    //     NotificationMessage: "Meeting confirmed with " + (Requestor.Name || ""),
                    //     NotificationTitle: "Meeting Request Confirmed"
                    // }

                    // if (InviteeData.MeetingType && payload.auth) {
                    //     let NotificationTemplatePath = '/OA_UAT/NotificationTPL/' + InviteeData.MeetingType;
                    //     let TempRef = await dbClient.doc(NotificationTemplatePath).get()
                    //     if (TempRef.exists && payload.auth) {
                    //         let NotificationTemplate = TempRef.data()
                    //         let notificationMessageTemplate = Handlebars.compile(NotificationTemplate.Message);
                    //         InviteeData.NotificationMessage = notificationMessageTemplate(Requestor);
                    //         let notificationTitleTemplate = Handlebars.compile(NotificationTemplate.Title);
                    //         InviteeData.NotificationTitle = notificationTitleTemplate(Requestor);

                    //     }
                    // }

                    // tasks.push(utils.savePushAttendee(payload, event_base_path, Invitee.AttendeeId, InviteeData))

                    let payloadBuffer = Buffer.from(JSON.stringify(pubsubPayload))
                    tasks.push(pubSubClient.topic(topicName).publishMessage({
                        data: payloadBuffer,
                        attributes: { source: 'meeting-confirmation', type: 'reminder' }
                    }));
                    let result = await Promise.allSettled(tasks)
                    ret_val.status = 0;
                    ret_val.MeetingId = payload.data.MeetingId;
                    ret_val.result = result;
                    console.log("ret_val", ret_val)
                    resolve(ret_val)
                } else {
                    reject(ret_val)
                }
            })
            .catch((err) => {
                console.log(err);
                ret_val = ERRCODE.UNKNOWN_ERROR
                reject(ret_val)
            })
    })
}
// function pubsub_confirm_meeting(pubsubPayload) {
//     return new Promise(async (resolve, reject) => {
//         logger.log(pubsubPayload)
//         let ret_val = { status: -1 }
//         let payload = pubsubPayload.Payload;
//         let Requestor = pubsubPayload.Requestor;
//         let Invitee = pubsubPayload.Invitee;
//         let Meeting = pubsubPayload.Meeting;

//         const instance_base_path = "/" + payload.key.instanceId;
//         const iceId = `${payload.key.instanceId}_${payload.key.clientId}${payload.key.eventId}`;
//         const event_base_path = `/${iceId}`;

//         let startDateTime = moment(Meeting.Slots[0]).utc().format('MMM DD, YYYY hh:mm A');
//         let timeZone = 'UTC';
//         if (Meeting.Timezone) {
//             startDateTime = moment.tz(Meeting.Slots[0], 'UTC').tz(Meeting.Timezone).format('MMM DD, YYYY hh:mm A');
//             timeZone = Meeting.Timezone;
//         }

//         let MeetingAnalyticData = {
//             meetingId: payload.data.MeetingId,
//             meetingTsUTC: moment(Meeting.Slots[0]).utc().unix(),
//             meetingType: "one2one",
//             subject: (Meeting.Message || ""),
//             timeZone: (Meeting.Timezone || ""),
//             updated: moment().utc().unix(),
//             status: Meeting.Status
//         }
//         let ptasks = []
//         ptasks.push(_add_meeting_analytics(payload, MeetingAnalyticData, Requestor, Invitee))
//         Promise.allSettled(ptasks)
//             .then(res => {
//                 let tasks = []
//                 tasks.push(dbClient.collection(instance_base_path).doc("ClientList").collection("Clients").doc(payload.key.clientId).get())
//                 tasks.push(dbClient.collection(instance_base_path).doc("mailtpl").collection("Meeting").doc("Confirmed").get())
//                 tasks.push(dbClient.collection(instance_base_path).doc("mailtpl").collection("Meeting").doc("Confirmed").get())
//                 tasks.push(dbClient.collection(event_base_path).doc("MeetingList").collection("Settings").doc("Config").get())
//                 tasks.push(dbClient.collection(event_base_path).doc("EventInfo").get())
//                 return Promise.all(tasks)
//             })
//             .then((res) => {
//                 let tasks = [];
//                 let Client = res[0].data();
//                 let Event = res[4].data();
//                 let MeetingConfig = res[3].data();
//                 let customDomain = (Client.CustomDomain) ? _add_https(Client.CustomDomain).replace(/\/?$/, '/') : 'https://onair.e2m.live/';
//                 let MeetingUrl = customDomain + 'mymeetings';

//                 let calendar_duration = [30, "minute"]
//                 if (MeetingConfig.Duration) {
//                     calendar_duration = [parseInt(MeetingConfig.Duration), "minute"]
//                 }
//                 if (MeetingConfig.Items) {
//                     for (let i = 0; i < MeetingConfig.Items.length; i++) {
//                         let item = MeetingConfig.Items[i]
//                         let mCurrentStartDate = moment(item.StartDate).add((item.AvailabilityFromTime - 120), 's').toDate();
//                         let mCurrentEndDate = moment(item.EndDate).add(item.AvailabilityToTime + 120, 's').toDate();
//                         if (moment(Meeting.Slots[0]).isBetween(mCurrentStartDate, mCurrentEndDate)) {
//                             calendar_duration = [parseInt(item.Duration), "minute"]
//                         }
//                     }
//                 }


//                 if (res[1].exists) {

//                     let EmailTemplate;
//                     let EmailPayload;
//                     let emailTemplate;
//                     let emailTemplateSub;
//                     let Placeholders;
//                     let email_body_html;
//                     let email_subject;
//                     let calendarEvent;
//                     EmailTemplate = res[1].data();
//                     //logger.log(EmailTemplate)
//                     // if (Requestor.Timezone) {
//                     //     startDateTime = moment.tz(Meeting.Slots[0], 'UTC').tz(Requestor.Timezone).format('DD-MMM-YYYY hh:mm A');
//                     //     timeZone = Requestor.Timezone;
//                     // }

//                     calendarEvent = {
//                         title: "Meeting with " + Invitee.Name,
//                         description: Meeting.Message,
//                         start: moment(Meeting.Slots[0]).utc().format("YYYY-MM-DD H:mm:ss ZZ"),
//                         duration: calendar_duration,
//                     };
//                     //logger.log(calendarEvent)
//                     Placeholders = {
//                         ReceiverName: (Requestor.Name || ""),
//                         SenderName: (Invitee.Name || ""),
//                         Title: (Invitee.Title || ""),
//                         Company: (Invitee.Company || ""),
//                         Email: Invitee.Email,
//                         StartDateTime: startDateTime,
//                         Timezone: timeZone,
//                         Team: "Team",
//                         Message: (Meeting.Message || ""),
//                         MeetingUrl: (MeetingUrl || ""),
//                         ClientName: (Client.ClientName || ""),
//                         EventLogo: (Event.EventLogo || ""),
//                         AddToGoogle: google(calendarEvent),
//                         AddToOutlook: outlook(calendarEvent),
//                         AddToOffice365: office365(calendarEvent),
//                         AddToYahoo: yahoo(calendarEvent),
//                         AddToIcs: ics(calendarEvent)
//                     };
//                     // if (!Placeholders.Company) {
//                     //     EmailTemplate.html.replace('<span style="color: #34495e; font-size: 14px; line-height: 19.6px;"><span style="font-size: 18px; line-height: 25.2px;">Company: </span><span style="font-size: 18px; line-height: 25.2px;">{{Company}}</span></span><br />', '');
//                     // }
//                     // if (!Placeholders.Message) {
//                     //     EmailTemplate.html.replace('<span style="color: #34495e; font-size: 14px; line-height: 19.6px;"><span style="font-size: 18px; line-height: 25.2px;">Message: </span><span style="font-size: 18px; line-height: 25.2px;">{{Message}}</span></span>', '');
//                     // }

//                     emailTemplate = Handlebars.compile(EmailTemplate.html);
//                     emailTemplateSub = Handlebars.compile(EmailTemplate.subject);
//                     email_body_html = emailTemplate(Placeholders);
//                     email_subject = emailTemplateSub(Placeholders)
//                     EmailPayload = {
//                         from: {
//                             email: EmailTemplate.from,
//                             name: Client.ClientName
//                         },
//                         to: {
//                             name: Requestor.Name,
//                             email: Requestor.Email
//                         },
//                         cc: EmailTemplate.cc,
//                         bcc: EmailTemplate.bcc,
//                         reply_to: EmailTemplate.reply_to,
//                         subject: email_subject,
//                         html: email_body_html
//                     };
//                     let RequestorNotification = {
//                         Initials: (Invitee.Tags || ""),
//                         Name: (Invitee.Name || ""),
//                         ProfilePictureURL: (Invitee.ProfilePictureURL || ""),
//                         MeetingType: "One2OneMeeting/MeetingAccepted",
//                         NotificationMessage: "Meeting confirmed by " + (Invitee.Tags || "") + " " + (Invitee.Name || ""),
//                         NotificationTitle: "Meeting Request Confirmed",
//                         SendToName: Requestor.Name,
//                         SendToPhone: Requestor.Phone
//                     }
//                     tasks.push(_send_email(Client, EmailPayload))
//                     // tasks.push(utils.sendPushAttendee(payload, event_base_path, Requestor.AttendeeId, RequestorNotification))
//                     if (MeetingConfig.SendSMS) {
//                         tasks.push(_send_sms(RequestorNotification))
//                     }
//                 }
//                 if (res[2].exists) {
//                     let EmailTemplate;
//                     let EmailPayload;
//                     let emailTemplate;
//                     let emailTemplateSub;
//                     let Placeholders;
//                     let email_body_html;
//                     let email_subject;
//                     let calendarEvent;
//                     EmailTemplate = res[2].data();
//                     //logger.log(EmailTemplate)
//                     // if (Requestor.Timezone) {
//                     //     startDateTime = moment.tz(Meeting.Slots[0], 'UTC').tz(Requestor.Timezone).format('DD-MMM-YYYY hh:mm A');
//                     //     timeZone = Requestor.Timezone;
//                     // }

//                     calendarEvent = {
//                         title: "Meeting with " + Requestor.Name,
//                         description: Meeting.Message,
//                         start: moment(Meeting.Slots[0]).utc().format("YYYY-MM-DD H:mm:ss ZZ"),
//                         duration: calendar_duration,
//                     };
//                     //logger.log(calendarEvent)
//                     Placeholders = {
//                         ReceiverName: (Invitee.Name || ""),
//                         SenderName: (Requestor.Name || ""),
//                         Title: (Requestor.Title || ""),
//                         Company: (Requestor.Company || ""),
//                         Email: Requestor.Email,
//                         StartDateTime: startDateTime,
//                         Timezone: timeZone,
//                         Team: "Team",
//                         Message: (Meeting.Message || ""),
//                         MeetingUrl: (MeetingUrl || ""),
//                         ClientName: (Client.ClientName || ""),
//                         EventLogo: (Event.EventLogo || ""),
//                         AddToGoogle: google(calendarEvent),
//                         AddToOutlook: outlook(calendarEvent),
//                         AddToOffice365: office365(calendarEvent),
//                         AddToYahoo: yahoo(calendarEvent),
//                         AddToIcs: ics(calendarEvent)
//                     };
//                     // if (!Placeholders.Company) {
//                     //     EmailTemplate.html.replace('<span style="color: #34495e; font-size: 14px; line-height: 19.6px;"><span style="font-size: 18px; line-height: 25.2px;">Company: </span><span style="font-size: 18px; line-height: 25.2px;">{{Company}}</span></span><br />', '');
//                     // }
//                     // if (!Placeholders.Message) {
//                     //     EmailTemplate.html.replace('<span style="color: #34495e; font-size: 14px; line-height: 19.6px;"><span style="font-size: 18px; line-height: 25.2px;">Message: </span><span style="font-size: 18px; line-height: 25.2px;">{{Message}}</span></span>', '');
//                     // }

//                     emailTemplate = Handlebars.compile(EmailTemplate.html);
//                     emailTemplateSub = Handlebars.compile(EmailTemplate.subject);
//                     email_body_html = emailTemplate(Placeholders);
//                     email_subject = emailTemplateSub(Placeholders)
//                     EmailPayload = {
//                         from: {
//                             email: EmailTemplate.from,
//                             name: Client.ClientName
//                         },
//                         to: {
//                             name: Invitee.Name,
//                             email: Invitee.Email
//                         },
//                         cc: EmailTemplate.cc,
//                         bcc: EmailTemplate.bcc,
//                         reply_to: EmailTemplate.reply_to,
//                         subject: email_subject,
//                         html: email_body_html
//                     };
//                     let InviteeNotification = {
//                         Initials: (Requestor.Tags || ""),
//                         Name: (Requestor.Name || ""),
//                         ProfilePictureURL: (Requestor.ProfilePictureURL || ""),
//                         MeetingType: "One2OneMeeting/MeetingAccepted",
//                         NotificationMessage: "Meeting confirmed with " + (Requestor.Tags || "") + " " + (Requestor.Name || ""),
//                         NotificationTitle: "Meeting Request Confirmed",
//                         SendToName: Invitee.Name,
//                         SendToPhone: Invitee.Phone
//                     }
//                     tasks.push(_send_email(Client, EmailPayload))
//                     //tasks.push(utils.sendPushAttendee(payload, event_base_path, Invitee.AttendeeId, InviteeNotification))
//                     if (MeetingConfig.SendSMS) {
//                         tasks.push(_send_sms(InviteeNotification))
//                     }
//                 }
//                 tasks.push(_attach_meeting_reminders(payload.key, Meeting))
//                 return Promise.allSettled(tasks)
//             })
//             .then((res) => {
//                 //logger.log(res)
//                 ret_val.status = 0;
//                 ret_val.result = payload.data;
//                 resolve(ret_val)
//             })
//             .catch((err) => {
//                 logger.log(err);
//                 ret_val = ERRCODE.UNKNOWN_ERROR
//                 reject(ret_val)
//                 return;
//             })
//     })
// }

async function pubsub_confirm_meeting(pubsubPayload) {
    let ret_val = { status: -1 }
    try {
        console.log("pubsubPayload", pubsubPayload)

        let payload = pubsubPayload.Payload;
        let Requestor = pubsubPayload.Requestor;
        let Invitee = pubsubPayload.Invitee;
        let Meeting = pubsubPayload.Meeting;

        const instance_base_path = `/${payload.key.instanceId}`;
        const iceId = `${payload.key.instanceId}_${payload.key.clientId}${payload.key.eventId}`;
        const event_base_path = `/${iceId}`;
        const meeting_doc_path = `${event_base_path}/MeetingList/Meetings/${payload.data.MeetingId}`;
        const tasks = [];



        // Fetch client and event data
        const [clientSnap, eventSnap] = await Promise.all([
            dbClient.doc(`${instance_base_path}/ClientList/Clients/${payload.key.clientId}`).get(),
            dbClient.doc(`${event_base_path}/EventInfo`).get()
        ]);
        const Client = clientSnap.data();
        const Event = eventSnap.data();

        // Fetch email template: first check event_base_path, then fallback to instance_base_path
        let emailTemplateDoc = await dbClient.doc(`${event_base_path}/mailtpl/Meeting/Confirmed`).get();
        if (!emailTemplateDoc.exists) {
            emailTemplateDoc = await dbClient.doc(`${instance_base_path}/mailtpl/Meeting/Confirmed`).get();
        }
        if (!emailTemplateDoc.exists) throw new Error("Email template not found for Confirmation");

        const EmailTemplate = emailTemplateDoc.data();

        // Format the meeting start time
        const startDateTime = Meeting.Timezone
            ? moment.tz(Meeting.Slots[0], 'UTC').tz(Meeting.Timezone).format("MMM DD, YYYY hh:mm A")
            : moment(Meeting.Slots[0]).utc().format("MMM DD, YYYY hh:mm A");
        const timeZone = Meeting.Timezone || "UTC";
        const customDomain = Client.CustomDomain
            ? _add_https(Client.CustomDomain).replace(/\/?$/, '/')
            : 'https://onair.e2m.live/';
        const MeetingUrl = customDomain + 'mymeetings';

        // Create calendar event details
        const calendarEvent = {
            title: `Meeting with ${Invitee.Name}`,
            description: Meeting.Message || '',
            start: moment(Meeting.Slots[0]).utc().format("YYYY-MM-DD H:mm:ss ZZ"),
            duration: [30, "minute"]
        };

        // Common placeholders for the email and calendar links
        const commonPlaceholders = {
            StartDateTime: startDateTime,
            Timezone: timeZone,
            MeetingUrl: MeetingUrl,
            ClientName: Client.ClientName || "",
            EventLogo: Event.EventLogo || "",
            Message: Meeting.Message || "",
            AddToGoogle: google(calendarEvent),
            AddToOutlook: outlook(calendarEvent),
            AddToOffice365: office365(calendarEvent),
            AddToYahoo: yahoo(calendarEvent),
            AddToIcs: ics(calendarEvent),
            Team: "Team"
        };

        // Compile email template
        const emailTemplate = Handlebars.compile(EmailTemplate.html);
        const emailSubjectTemplate = Handlebars.compile(EmailTemplate.subject);

        // Recipients array: requestor and invitee
        const recipients = [
            { role: "Requestor", data: Requestor, counterpart: Invitee },
            { role: "Invitee", data: Invitee, counterpart: Requestor }
        ];

        // Loop through the recipients and send email/SMS
        for (const { role, data, counterpart } of recipients) {
            const placeholders = {
                ...commonPlaceholders,
                ReceiverName: data.Name || "",
                SenderName: counterpart.Name || "",
                Title: counterpart.Title || "",
                Company: counterpart.Company || "",
                Email: data.Email
            };

            const emailBody = emailTemplate(placeholders);
            const emailSubject = emailSubjectTemplate(placeholders);

            const EmailPayload = {
                from: {
                    email: EmailTemplate.from,
                    name: Client.ClientName
                },
                to: {
                    name: data.Name,
                    email: data.Email
                },
                cc: EmailTemplate.cc,
                bcc: EmailTemplate.bcc,
                reply_to: EmailTemplate.reply_to,
                subject: emailSubject,
                html: emailBody
            };

            // Send email if the email is available
            if (data.Email) tasks.push(_send_email(Client, EmailPayload));

            // Send SMS if enabled and phone is available
            if (Meeting.SendSMS && data.Phone) {
                const smsText = `Confirmation: Meeting with ${counterpart.Name} on ${startDateTime}`;
                //tasks.push(_send_sms({ to: data.Phone, message: smsText }));
                console.log("SMS to be sent:", smsText);
            }
        }
        tasks.push(_attach_meeting_reminders(payload.key, Meeting))
        // Await all tasks (email and SMS sending)
        await Promise.allSettled(tasks);

        // Update meeting status to 'Confirmed'
        await dbClient.doc(meeting_doc_path).update({ Status: 'Confirmed' });

        // Respond back with success
        ret_val.status = 0
        ret_val.message = `Meeting ${payload.data.MeetingId} confirmed and notifications sent`
    } catch (err) {
        console.error("Error in pubsub_confirm_meeting:", err);
        ret_val.message = "Unknown error";
    }
    return ret_val
}
async function meeting_reminder(req, res) {
    try {
        const payload = req.body;
        const { instanceId, clientId, eventId } = payload.key;
        const { meetingId, offset, docPath } = payload.data;

        const iceId = `${instanceId}_${clientId}${eventId}`;
        const instance_base_path = `/${instanceId}`;
        const event_base_path = `/${iceId}`;
        const tasks = [];

        const meetingSnap = await dbClient.doc(docPath).get();
        if (!meetingSnap.exists) throw new Error(`Meeting ${meetingId} not found`);
        const meeting = meetingSnap.data();

        const [requestorSnap, inviteeSnap] = await Promise.all([
            dbClient.doc(`${instance_base_path}/AttendeeList/Attendees/${meeting.RequestorId}`).get(),
            dbClient.doc(`${instance_base_path}/AttendeeList/Attendees/${meeting.InviteeId}`).get()
        ]);

        const Requestor = requestorSnap.data();
        const Invitee = inviteeSnap.data();

        const [clientSnap, eventSnap] = await Promise.all([
            dbClient.doc(`${instance_base_path}/ClientList/Clients/${clientId}`).get(),
            dbClient.doc(`${event_base_path}/EventInfo`).get()
        ]);
        const Client = clientSnap.data();
        const Event = eventSnap.data();

        let emailTemplateDoc = await dbClient.doc(`${event_base_path}/mailtpl/Meeting/Reminder`).get();
        if (!emailTemplateDoc.exists) {
            emailTemplateDoc = await dbClient.doc(`${instance_base_path}/mailtpl/Meeting/Reminder`).get();
        }
        if (!emailTemplateDoc.exists) throw new Error("Email template not found for Reminder");

        const EmailTemplate = emailTemplateDoc.data();

        const startDateTime = meeting.Timezone
            ? moment.tz(meeting.Slots[0], 'UTC').tz(meeting.Timezone).format("MMM DD, YYYY hh:mm A")
            : moment(meeting.Slots[0]).utc().format("MMM DD, YYYY hh:mm A");
        const timeZone = meeting.Timezone || "UTC";
        const customDomain = Client.CustomDomain
            ? _add_https(Client.CustomDomain).replace(/\/?$/, '/')
            : 'https://onair.e2m.live/';
        const MeetingUrl = customDomain + 'mymeetings';

        const calendarEvent = {
            title: `Meeting with ${Invitee.Name}`,
            description: meeting.Message || '',
            start: moment(meeting.Slots[0]).utc().format("YYYY-MM-DD H:mm:ss ZZ"),
            duration: [30, "minute"]
        };

        const commonPlaceholders = {
            StartDateTime: startDateTime,
            Timezone: timeZone,
            MeetingUrl: MeetingUrl,
            ClientName: Client.ClientName || "",
            EventLogo: Event.EventLogo || "",
            Message: meeting.Message || "",
            AddToGoogle: google(calendarEvent),
            AddToOutlook: outlook(calendarEvent),
            AddToOffice365: office365(calendarEvent),
            AddToYahoo: yahoo(calendarEvent),
            AddToIcs: ics(calendarEvent),
            Team: "Team"
        };

        const emailTemplate = Handlebars.compile(EmailTemplate.html);
        const emailSubjectTemplate = Handlebars.compile(EmailTemplate.subject);

        const recipients = [
            { role: "Requestor", data: Requestor, counterpart: Invitee },
            { role: "Invitee", data: Invitee, counterpart: Requestor }
        ];

        for (const { role, data, counterpart } of recipients) {
            const placeholders = {
                ...commonPlaceholders,
                ReceiverName: data.Name || "",
                SenderName: counterpart.Name || "",
                Title: counterpart.Title || "",
                Company: counterpart.Company || "",
                Email: data.Email
            };

            const emailBody = emailTemplate(placeholders);
            const emailSubject = emailSubjectTemplate(placeholders);

            const EmailPayload = {
                from: {
                    email: EmailTemplate.from,
                    name: Client.ClientName
                },
                to: {
                    name: data.Name,
                    email: data.Email
                },
                cc: EmailTemplate.cc,
                bcc: EmailTemplate.bcc,
                reply_to: EmailTemplate.reply_to,
                subject: emailSubject,
                html: emailBody
            };

            if (data.Email) tasks.push(_send_email(Client, EmailPayload));

            if (meeting.SendSMS && data.Phone) {
                const smsText = `Reminder: Meeting with ${counterpart.Name} on ${startDateTime}`;
                console.log("Reminder: SMS to be sent:", smsText);
                //tasks.push(_send_sms({ to: data.Phone, message: smsText }));
            }
        }

        await Promise.allSettled(tasks);
        res.status(200).json({ status: 0, message: `Reminder sent for meeting ${meetingId}` });
    } catch (err) {
        console.error("Error in send_meeting_reminder:", err);
        res.status(500).json({ status: -1, error: err.message || "Unknown error" });
    }
}

async function auto_confirm_meetings(payload) {
    const { instanceId, clientId, eventId } = payload.key;
    const iceId = `${instanceId}_${clientId}${eventId}`;

    // Fetch participants with less than 10 confirmed meetings for sponsors and less than 2 for attendees
    const sponsors = await _available_participants(iceId, "Sponsor", 10);
    const attendees = await _available_participants(iceId, "Attendee", 2);

    // Fetch confirmed meeting counts
    const confirmedCounts = await _confirmed_meetings_count(iceId);
    sponsors.forEach(s => s.confirmed = confirmedCounts[s.attendeeId] || 0);
    attendees.forEach(a => a.confirmed = confirmedCounts[a.attendeeId] || 0);

    // Sort by confirmed meetings (ascending order)
    sponsors.sort((a, b) => a.confirmed - b.confirmed);
    attendees.sort((a, b) => a.confirmed - b.confirmed);

    // Fetch QnA responses
    const sponsorQnA = await _qna_response(iceId, sponsors);
    const attendeeQnA = await _qna_response(iceId, attendees);

    // Fetch availability
    const sponsorAvailability = await _availability(sponsors);
    const attendeeAvailability = await _availability(attendees);

    for (const sponsor of sponsors) {
        if (sponsor.confirmed >= 10) continue;  // Skip sponsors with 10 or more confirmed meetings

        const sponsorSlots = sponsorAvailability[sponsor.attendeeId] || [];
        const sponsorShifts = _classify_shifts(sponsorSlots);

        for (const attendee of attendees) {
            if (attendee.confirmed >= 2) continue;  // Skip attendees with 2 or more confirmed meetings

            const attendeeSlots = attendeeAvailability[attendee.attendeeId] || [];
            const attendeeShifts = _classify_shifts(attendeeSlots);

            const sponsorAns = sponsorQnA[sponsor.attendeeId] || [];
            const attendeeAns = attendeeQnA[attendee.attendeeId] || [];

            // Match QnA responses
            const qnaMatch = await _qna_match(sponsorAns, attendeeAns);
            if (!qnaMatch) continue;  // Skip if QnA does not match

            // Try exact slot match
            const matchingSlot = sponsorSlots.find(slot => attendeeSlots.includes(slot));
            if (matchingSlot) {
                // Request the meeting
                let res = await request_meetings({
                    key: payload.key,
                    data: {
                        AttendeeIds: [attendee.attendeeId],
                        RequestorId: sponsor.attendeeId
                    }
                });
                // Confirm the meeting
                await confirm_meeting({
                    key: payload.key,
                    data: {
                        MeetingId: res.created[0],
                        Slot: matchedSlot
                    }
                });
                sponsor.confirmed++;
                attendee.confirmed++;
                continue;
            }

            // Fallback: try shift match
            if (sponsor.confirmed < 10) {
                const sharedShift = sponsorShifts.find(shift => attendeeShifts.includes(shift));
                if (sharedShift) {
                    // Try matching slots within the same shift
                    const fallbackSlot = sponsorSlots.find(slot => _get_shift(slot) === sharedShift);
                    if (fallbackSlot) {
                        // Request the meeting
                        let res = await request_meetings({
                            key: payload.key,
                            data: {
                                AttendeeIds: [attendee.attendeeId],
                                RequestorId: sponsor.attendeeId
                            }
                        });
                        // Confirm the meeting
                        await confirm_meeting({
                            key: payload.key,
                            data: {
                                MeetingId: res.created[0],
                                Slot: fallbackSlot
                            }
                        });
                        sponsor.confirmed++;
                        attendee.confirmed++;
                    }
                }
            }
        }
    }
}


async function _get_from_cache_or_db(cacheKey, fallbackFn) {
    const cachedValue = await cm.getFromCache({ cacheKey: cacheKey });
    //console.log("Cached value:", cachedValue);
    if (cachedValue?.cacheValueJson) {
        return cachedValue.cacheValueJson;
    }
    const freshData = await fallbackFn();
    if (freshData) {
        await cm.storeInCache({
            cacheKey,
            cacheValueJson: freshData,
            expirySeconds: 3600 * 3
        });
    }
    return freshData;
};
async function _save_as_draft(payload) {
    try {
        let iceId = `${payload.key.instanceId}_${payload.key.clientId}${payload.key.eventId}`;
        let eventBasePath = `/${iceId}`;
        let requestorId = payload.auth.data.UserId;
        let inviteeId = payload.data.inviteeId;

        let requestorRef = dbClient.collection(eventBasePath).doc("AttendeeList").collection("Attendees").doc(requestorId);
        let requestorDoc = await requestorRef.get();
        let requestorData = requestorDoc.data();
        let registrationType = requestorData?.RegistrationType?.RegistrationType?.toLowerCase();
        let registrationTypeEntityId = requestorData?.RegistrationType?.RegistrationTypeEntityId ?? null;

        if (!requestorDoc.exists || !registrationTypeEntityId) {
            throw new Error("Requestor not found or registrationTypeEntityId is missing");
        }

        const [confirmedMeetings, draftedInfo] = await Promise.all([
            mysql.executeQuery(
                `SELECT COUNT(*) AS count FROM meeting 
                 WHERE iceId= ? AND inviteeId = ? AND requestStatus = 'confirmed'`,
                [iceId, inviteeId], true
            ),
            mysql.executeQuery(
                `SELECT 
                    COUNT(DISTINCT inviteeId) AS total, 
                    SUM(CASE WHEN inviteeId = ? THEN 1 ELSE 0 END) AS alreadyDrafted 
                 FROM meeting 
                 WHERE iceId= ? AND requestorTypeEntityId = ?`,
                [inviteeId, iceId, registrationTypeEntityId], true
            )
        ]);

        if (confirmedMeetings[0].count >= 2) {
            throw new Error("Invitee already has two confirmed meetings");
        }

        const { total, alreadyDrafted } = draftedInfo[0];

        if (total >= config.SPONSOR_MAX_REQUESTS) {
            throw new Error("Maximum of 40 drafted attendees reached for this sponsor");
        }

        if (alreadyDrafted > 0) {
            throw new Error("Invitee is already saved as a draft");
        }

        // Save as draft
        await mysql.executeQuery(
            `INSERT INTO meeting (iceId, requestorId, inviteeId, requestorType, requestorTypeEntityId,inviteeType, inviteeTypeEntityId,requestStatus) 
             VALUES (?, ?, ?, ?, ?, 'attendee','','draft')`,
            [iceId, requestorId, inviteeId, registrationType, registrationTypeEntityId]
        );

        return { success: true, message: "Invitee saved as draft successfully" };
    } catch (error) {
        console.error("Error saving attendee as draft:", error);
        throw error;
    }
}
function _wild_card_match(fieldValue, filterValue) {
    if (!filterValue) return true;
    let pattern = filterValue.trim();

    // Automatically wrap with * if not present
    if (!pattern.includes("*")) {
        pattern = `*${pattern}*`;
    }

    const regexPattern = pattern
        .toLowerCase()
        .replace(/[-/\\^$+?.()|[\]{}]/g, "\\$&") // Escape special chars
        .replace(/\*/g, ".*"); // Convert * to .*

    const regex = new RegExp(`${regexPattern}`, "i"); // Removed ^...$
    return regex.test(fieldValue?.toLowerCase?.() || "");
}
async function _attach_meeting_reminders(key, meeting) {
    //console.log("Attaching meeting reminders for meeting:", meeting);
    const project = config.GCP.PROJECT_ID;
    const queue = 'o2o-reminder-queue';
    const location = config.GCP.LOCATION_ID;
    const reminderAPI = config.GCP.API_BASE + "meeting-reminder";

    const ret_val = { status: -1 };

    try {
        const { instanceId, clientId, eventId } = key;
        const iceId = `${instanceId}_${clientId}${eventId}`;
        const event_base_path = `/${iceId}`;
        const meetingId = meeting.MeetingId; // adapt based on your doc structure
        const slotTime = meeting.Slots?.[0];


        if (!slotTime) throw new Error("Missing Slots in meeting data");

        // Fetch SendRemider config
        const configRef = dbClient.doc(`${event_base_path}/MeetingList/Settings/Config`);
        const configSnap = await configRef.get();

        const reminderOffsets = configSnap.exists ? (configSnap.data().SendRemider || [5]) : [5];

        const meetingStart = moment.utc(slotTime);
        const headers = { "Content-Type": "application/json" };
        const parent = taskClient.queuePath(project, location, queue);
        //console.log("Parent path:", parent);
        //console.log("Meeting start time:", meetingStart.format());
        //console.log("Reminder offsets:", reminderOffsets);

        for (const offset of reminderOffsets) {
            const scheduleTime = {
                seconds: meetingStart.clone().subtract(offset, 'minutes').unix()
            };

            const reminderPayload = {
                key: { instanceId, clientId, eventId },
                data: {
                    meetingId,
                    offset,
                    docPath: `${event_base_path}/MeetingList/Meetings/${meetingId}`
                }
            };
            //console.log("Scheduling reminder payload:", reminderPayload);

            const task = {
                httpRequest: {
                    headers,
                    httpMethod: 'POST',
                    url: reminderAPI,
                    body: Buffer.from(JSON.stringify(reminderPayload)).toString("base64")
                },
                scheduleTime
            };

            const request = { parent, task };
            const [response] = await taskClient.createTask(request);
            console.log(`Scheduled reminder (${offset}min before): ${response.name}`);
        }

        ret_val.status = 0;
        return ret_val;

    } catch (err) {
        console.error("Error in _attach_meeting_reminders:", err);
        return ERRCODE.UNKNOWN_ERROR;
    }
}
function _send_email(Client, EmailPayload) {
    return new Promise(async (resolve, reject) => {
        let tasks = [];
        let ret_val = ERRCODE.UNKNOWN_ERROR
        if (Client.CustomSMTP && Client.SMTPServerAddress && Client.SMTPUserName && Client.SMTPPassword) {
            let smtp_config = {
                host: Client.SMTPServerAddress,
                auth: {
                    user: Client.SMTPUserName,
                    pass: Client.SMTPPassword
                }
            };
            if (Client.SMTPEncryptionType == 'SSL') {
                smtp_config.secure = true;
            }
            if (Client.SMTPPortNumber) {
                smtp_config.port = Client.SMTPPortNumber;
            }
            EmailPayload.from.email = Client.SMTPUserName;
            console.log("Sending email using SMTP", EmailPayload);
            tasks.push(utils.sendEmailSMTP(smtp_config, EmailPayload))

        } else {
            let SENDGRIDApiKey = (Client.SENDGRIDApiKey || config.SENDGRID_API_KEY)
            console.log("Sending email using SENDGRID", EmailPayload);
            tasks.push(utils.sendEmail(EmailPayload))
        }
        Promise.all(tasks)
            .then((res) => {
                resolve(res)
            })
            .catch(err => {
                console.log(err);
                reject(ret_val)
                return;
            })
    })
}
async function _send_sms({ to, message }) {
    try {
        // Replace this with your SMS provider's API logic, example using Twilio
        const accountSid = config.TWILIO.SID;
        const authToken = config.TWILIO.TOKEN;
        const fromPhoneNumber = config.TWILIO.FROM; // Your Twilio phone number

        // // Use the twilio client
        // const client = twilio(accountSid, authToken);

        // // Send the SMS
        // const smsResponse = await client.messages.create({
        //     body: message,         // SMS content
        //     from: fromPhoneNumber, // From your Twilio number
        //     to: to                 // Recipient phone number
        // });

        // console.log('SMS sent successfully:', smsResponse.sid);
        return { success: true, message: 'SMS sent successfully' };
    } catch (error) {
        console.error('Error sending SMS:', error);
        return { success: false, error: error.message || 'Failed to send SMS' };
    }
}

// Helper function to classify slots into shifts
function _classify_shifts(slots) {
    const shifts = new Set();
    for (const slot of slots) {
        shifts.add(_get_Shift(slot));
    }
    return Array.from(shifts);
}
// Helper function to determine if a slot is in the morning or afternoon
function _get_Shift(slot) {
    const hour = moment.utc(slot).hour();
    return hour < 12 ? "morning" : "afternoon";
}
// Fetch participants below the confirmed meeting threshold (less than 10 for sponsors and less than 2 for attendees)
async function _available_participants(iceId, type, limit) {
    const queryType = type === "Sponsor" ? "requestorId" : "inviteeId";
    const queryTypeEntityId = type === "Sponsor" ? "requestorTypeEntityId" : "inviteeTypeEntityId";

    // Query to get participants with fewer than the limit number of confirmed meetings
    const [rows] = await mysql.execute(`
        SELECT DISTINCT 
            m.${queryType} AS attendeeId, 
            m.${queryTypeEntityId} AS sponsorId
        FROM 
            e2m_o2o_prd.meeting m
        LEFT JOIN 
            (SELECT requestorId AS id FROM e2m_o2o_prd.meeting WHERE requestStatus = 'Confirmed'
            UNION ALL
            SELECT inviteeId AS id FROM e2m_o2o_prd.meeting WHERE requestStatus = 'Confirmed') confirmed
        ON m.${queryType} = confirmed.id
        WHERE m.iceId = ?
        AND m.${queryType} IS NOT NULL
        AND (confirmed.id IS NULL OR confirmed.id != m.${queryType})  -- Less than the threshold confirmed meetings
        LIMIT ?
    `, [iceId, limit]);

    return rows.map(r => ({
        attendeeId: r.attendeeId,
        sponsorId: type === "Sponsor" ? r.sponsorId : undefined,
        confirmed: 0,  // Initialize confirmed to 0
    }));
}
// Fetch confirmed meetings count for sponsors and attendees
async function _confirmed_meetings_count(iceId) {
    const [rows] = await mysql.execute(`
        SELECT 
            m.requestorId AS participantId, 
            COUNT(m.meetingId) AS confirmed
        FROM e2m_o2o_prd.meeting m
        WHERE m.iceId = ? AND m.requestStatus = 'Confirmed'
        GROUP BY m.requestorId
        UNION
        SELECT 
            m.inviteeId AS participantId, 
            COUNT(m.meetingId) AS confirmed
        FROM e2m_o2o_prd.meeting m
        WHERE m.iceId = ? AND m.requestStatus = 'Confirmed'
        GROUP BY m.inviteeId
    `, [iceId, iceId]);

    const confirmedCounts = {};
    rows.forEach(row => {
        confirmedCounts[row.participantId] = row.confirmed;
    });

    return confirmedCounts;
}
// Fetch availability of participants from Firestore
async function _availability(participants) {
    const availabilityMap = {};
    const promises = participants.map(async ({ attendeeId }) => {
        const doc = await dbClient.doc(`AttendeeList/Attendees/${attendeeId}`).get();
        const data = doc.data();
        availabilityMap[attendeeId] = data?.Meeting?.Slots || [];
    });
    await Promise.all(promises);
    return availabilityMap;
}
// Fetch QnA responses grouped by attendeeId
async function _qna_response(iceId, participants) {
    const ids = participants.map(p => p.attendeeId);
    const placeholders = ids.map(() => "?").join(",");
    const [rows] = await mysql.execute(`
        SELECT attendeeId, questionLabel, selectedValue
        FROM e2m_o2o_prd.qna
        WHERE iceId = ? AND attendeeId IN (${placeholders})
        ORDER BY questionId
    `, [iceId, ...ids]);

    const qnaMap = {};
    for (const row of rows) {
        if (!qnaMap[row.attendeeId]) qnaMap[row.attendeeId] = [];
        qnaMap[row.attendeeId].push(`${row.questionLabel}: ${row.selectedValue}`);
    }
    return qnaMap;
}
// QnA similarity using OpenAI
async function _qna_match(sponsorQnA, attendeeQnA) {
    const prompt = `
Compare the following responses from a sponsor and an attendee.
Return a similarity score between 0 and 1.

Sponsor:
${sponsorQnA.join("\n")}

Attendee:
${attendeeQnA.join("\n")}

Respond only with a number.`;

    const response = await openai.chat.completions.create({
        model: "gpt-4",
        messages: [{ role: "user", content: prompt }],
    });

    const score = parseFloat(response.choices[0].message.content);
    return score >= 0.7;
}





function set_meeting_config(payload) {
    return new Promise(async (resolve, reject) => {
        let ret_val = { status: -1 };
        logger.log(payload)
        let event_base_path = "/" + payload.key.instanceId + "_" + payload.key.clientId + payload.key.eventId;
        let tasks = []
        let pk = payload.key;
        let pd = payload.data;
        pd.Duration = (pd.Duration || 30)
        pd.EventTimezone = (pd.EventTimezone || 'UTC')
        pd.Items = (pd.Items || [{
            StartDate: pd.StartDate,
            EndDate: pd.EndDate,
            AvailabilityFromTime: pd.AvailabilityFromTime,
            AvailabilityToTime: pd.AvailabilityToTime,
            Duration: pd.Duration,
            EventTimezone: pd.EventTimezone
        }])
        //console.log(pd.Items)
        for (let i = 0; i < pd.Items.length; i++) {
            let item = pd.Items[i];
            item.Duration = (item.Duration || pd.Duration);
            item.EventTimezone = pd.EventTimezone;
            let config_payload = {
                index: i,
                key: pk,
                data: item
            }
            tasks.push(_meeting_config(config_payload))

        }
        Promise.allSettled(tasks)
            .then(async (res) => {
                //console.log(res)
                ret_val.result = []
                pd.Slots = []
                let ranges = []
                for (let i = 0; i < res.length; i++) {
                    ret_val.result[i] = {}
                    ret_val.result[i].status = res[i].status;
                    if (res[i].status == 'fulfilled') {
                        if (res[i].value.status == 0) {
                            let resObj = res[i].value.result;
                            if (i === 0) {
                                pd.StartDate = resObj.StartDate;
                                pd.EndDate = resObj.EndDate;
                                pd.StartDateTime = resObj.StartDateTime;
                                pd.EndDateTime = resObj.EndDateTime;

                                ranges.push(moment.range([moment(pd.StartDateTime), moment(pd.EndDateTime)]));
                                if (resObj.Slots && resObj.Slots.length) {
                                    pd.Slots = resObj.Slots
                                }
                            } else {
                                let range = moment.range([moment(resObj.StartDateTime), moment(resObj.EndDateTime)]);
                                let range_flag = true;
                                ranges.forEach(xrange => {
                                    if (range.overlaps(xrange)) {
                                        range_flag = false
                                    }
                                })
                                if (range_flag) {
                                    ranges.push(moment.range([moment(resObj.StartDateTime), moment(resObj.EndDateTime)]));
                                    //console.log(resObj.StartDate)
                                    //console.log(resObj.EndDate)
                                    if (pd.StartDateTime > resObj.StartDateTime) {
                                        pd.StartDate = resObj.StartDate;
                                        pd.StartDateTime = resObj.StartDateTime;
                                    }
                                    if (pd.EndDateTime < resObj.EndDateTime) {
                                        pd.EndDate = resObj.EndDate;
                                        pd.EndDateTime = resObj.EndDateTime;
                                    }
                                    if (resObj.Slots && resObj.Slots.length) {
                                        pd.Slots = [...pd.Slots, ...resObj.Slots];
                                    }

                                } else {
                                    ret_val.result[i].status = 'rejected'
                                    ret_val.result[i].reason = 'Date Time Range Overlapped'

                                }

                            }
                            ret_val.result[i].StartDateTime = resObj.StartDateTime;
                            ret_val.result[i].EndDateTime = resObj.EndDateTime;
                            pd.Items[i].StartDateTime = resObj.StartDateTime
                            pd.Items[i].EndDateTime = resObj.EndDateTime
                        }
                    } else {
                        ret_val.result[i].reason = res[i].reason;
                    }
                    pd.Items[i].StartDate = new Date(pd.Items[i].StartDate)
                    pd.Items[i].EndDate = new Date(pd.Items[i].EndDate)
                    delete pd.Items[i].EventTimezone
                }
                if (pd.Slots.length) {
                    //console.log(pd)
                    await dbClient.collection(event_base_path).doc("MeetingList").collection("Settings").doc("Config").set(pd, { merge: true })
                    ret_val.status = 0;
                }
                resolve(ret_val)
            })
            .catch(err => {
                console.log(err)
                ret_val = ERRCODE.UNKNOWN_ERROR
                reject(ret_val)
            })
    })
}
function _meeting_config(payload) {
    return new Promise(async (resolve, reject) => {
        //logger.log(payload)
        let ret_val = { status: -1, result: {} };
        try {
            let tasks = []
            if (!payload.key) {
                ret_val = ERRCODE.PAYLOAD_ERROR
                reject(ret_val)
                return;
            }
            if (!payload.key.instanceId || !payload.key.clientId || !payload.key.eventId) {
                ret_val = ERRCODE.PAYLOAD_ERROR
                reject(ret_val)
                return;
            }
            const instance_base_path = "/" + payload.key.instanceId;
            const event_base_path = "/" + payload.key.instanceId + "_" + payload.key.clientId + payload.key.eventId;

            const StartDate = new Date(payload.data.StartDate);
            const EndDate = new Date(payload.data.EndDate);


            //console.log(payload.data.AvailabilityFromTime)
            //console.log(payload.data.AvailabilityToTime)
            let startString = moment(StartDate).add(payload.data.AvailabilityFromTime, 's').toDate();
            let endString = moment(StartDate).add(payload.data.AvailabilityToTime, 's').toDate();

            //console.log(startString +'     '+endString)
            //console.log(endString)
            let duration = parseInt(payload.data.Duration);
            console.log(startString + '     ' + endString);
            console.log('duration: ' + duration + ' val: ' + moment(endString).diff(moment(startString), 'minutes'))
            if (moment(endString).diff(moment(startString), 'minutes') < duration) {
                ret_val = ERRCODE.PAYLOAD_ERROR
                reject(ret_val)
                return;
            }

            let loop = StartDate;
            let i = 0;
            while (loop <= EndDate) {
                let mCurrentStartDate = moment(loop).add(payload.data.AvailabilityFromTime, 's').toDate();
                let mCurrentEndDate = moment(loop).add(payload.data.AvailabilityToTime, 's').toDate();
                tasks.push(_slots(mCurrentStartDate, mCurrentEndDate, duration, payload.data.EventTimezone))
                var newDate = loop.setDate(loop.getDate() + 1);
                loop = new Date(newDate);
                if (i >= 31) {
                    break
                }
                i++
            }
            Promise.all(tasks)
                .then(async (results) => {
                    let Slots = []
                    if (results.length) {
                        for (let result of results) {
                            Slots = [...Slots, ...result]
                        }
                    }
                    ret_val.result.Slots = Slots
                    ret_val.result.StartDate = new Date(payload.data.StartDate);
                    ret_val.result.EndDate = new Date(payload.data.EndDate);
                    ret_val.result.StartDateTime = moment(new Date(payload.data.StartDate)).add(payload.data.AvailabilityFromTime, 's').toDate();
                    ret_val.result.EndDateTime = moment(new Date(payload.data.EndDate)).add(payload.data.AvailabilityToTime, 's').toDate();
                    ret_val.status = 0
                    resolve(ret_val)
                })
                .catch((err) => {
                    logger.log(err);
                    reject(ret_val);
                })
        } catch (err) {
            logger.log(err);
            ret_val - ERRCODE.UNKNOWN_ERROR
            reject(ret_val);
        }
    })
}
function get_meeting_config(payload) {
    return new Promise(async (resolve, reject) => {
        logger.log(payload)
        let ret_val = { status: -1 };
        if (!payload.key) {
            ret_val = ERRCODE.PAYLOAD_ERROR
            reject(ret_val)
            return;
        }
        if (!payload.key.instanceId || !payload.key.clientId || !payload.key.eventId) {
            ret_val = ERRCODE.PAYLOAD_ERROR
            reject(ret_val)
            return;
        }

        const instance_base_path = "/" + payload.key.instanceId;
        const event_base_path = "/" + payload.key.instanceId + "_" + payload.key.clientId + payload.key.eventId;
        dbClient.collection(event_base_path).doc("MeetingList").collection("Settings").doc("Config").get()
            .then((docRef) => {
                ret_val.status = 0;
                let docData = docRef.data()
                if (docData.Items) {
                    for (let i = 0; i < docData.Items.length; i++) {
                        let startSecond = docData.Items[i].StartDate._seconds + docData.Items[i].AvailabilityFromTime
                        let endSecond = docData.Items[i].EndDate._seconds + docData.Items[i].AvailabilityToTime

                        let startStringET = moment(startSecond * 1000).utc().format('YYYY-MM-DD hh:mm a');
                        docData.Items[i].MeetingStartDateTimeUTC = moment.tz(startStringET, 'YYYY-MM-DD hh:mm a', docData.EventTimezone).utc().format();

                        let endStringET = moment(endSecond * 1000).utc().format('YYYY-MM-DD hh:mm a');
                        docData.Items[i].MeetingEndDateTimeUTC = moment.tz(endStringET, 'YYYY-MM-DD hh:mm a', docData.EventTimezone).utc().format();

                        docData.Items[i].MeetingStartTimestampUTC = moment(docData.Items[i].MeetingStartDateTimeUTC).valueOf()
                        docData.Items[i].MeetingEndTimestampUTC = moment(docData.Items[i].MeetingEndDateTimeUTC).valueOf()


                    }
                    for (let key in docData.Items[0]) {
                        docData[key] = docData.Items[0][key]
                    }
                }
                docData.ExcludeGroupToRequestMeeting = (docData.ExcludeGroupToRequestMeeting || [])
                docData.MeetingRestrictionEnabled = (docData.MeetingRestrictionEnabled || false)


                ret_val.data = docData;
                resolve(ret_val);
            })
            .catch((err) => {
                logger.log(err);
                ret_val.err = err
                reject(ret_val);
            })
    })
}
function get_meetings(payload) {
    return new Promise(async (resolve, reject) => {
        logger.log(payload)
        let ret_val = { status: -1 };
        if (!payload.key) {
            ret_val = ERRCODE.PAYLOAD_ERROR
            reject(ret_val)
            return;
        }
        if (!payload.key.instanceId || !payload.key.clientId || !payload.key.eventId) {
            ret_val = ERRCODE.PAYLOAD_ERROR
            reject(ret_val)
            return;
        }
        const instance_base_path = "/" + payload.key.instanceId;
        const event_base_path = "/" + payload.key.instanceId + "_" + payload.key.clientId + payload.key.eventId;
        let attendeeId = payload.auth.data.UserId
        if (payload.data) {
            if (payload.data.AttendeeId) {
                attendeeId = payload.data.AttendeeId.toString()
            }
        }
        dbClient.collection(event_base_path).doc("AttendeeList").collection("Attendees").doc(attendeeId).get()
            .then((attendeeRef) => {
                let tasks = [];
                if (!attendeeRef.exists) {
                    ret_val = ERRCODE.PAYLOAD_ERROR
                    reject(ret_val);
                }
                let Attendee = attendeeRef.data();
                //logger.log(Attendee.AttendeeId)
                if (Attendee.Meetings) {
                    for (let Meeting of Attendee.Meetings) {
                        tasks.push(dbClient.collection(event_base_path).doc("MeetingList").collection("Meetings").doc(Meeting).get())
                    }
                }
                return Promise.all(tasks)
            })
            .then((Meetings) => {

                ret_val.status = 0;
                ret_val.result = { Pending: [], Accepted: [], Rejected: [], Canceled: [], FYA: [] }
                if (Meetings.length) {
                    for (let Meeting of Meetings) {
                        let MeetingData = Meeting.data();
                        if (MeetingData) {
                            if (MeetingData.Status) {
                                ret_val.result[MeetingData.Status].push(MeetingData)
                            }
                        }
                    }
                }
                return dbClient.collection(event_base_path).doc("MeetingList").collection("Meetings").where('Invitee.AttendeeId', '==', attendeeId).where('Status', '==', "Pending").get()
            })
            .then((querySnapshot) => {
                querySnapshot.docs.forEach(Meeting => {
                    let MeetingData = Meeting.data();
                    ret_val.result['FYA'].push(MeetingData)
                })
                resolve(ret_val)
            })
            .catch((err) => {
                logger.log(err);
                ret_val.err = err;
                reject(err);
            })
    })
}
function download_meetings(payload) {
    return new Promise(async (resolve, reject) => {
        let ret_val = { status: -1 };

        if (!payload.key || !payload.key.instanceId || !payload.key.clientId || !payload.key.eventId) {
            ret_val = ERRCODE.PAYLOAD_ERROR;
            reject(ret_val);
            return;
        }

        const event_base_path = "/" + payload.key.instanceId + "_" + payload.key.clientId + payload.key.eventId;
        let attendeeId = null;
        let userEmail = payload.data.Email;

        if (payload.data && payload.data.AttendeeId) {
            attendeeId = payload.data.AttendeeId.toString();
        } else if (userEmail) {
            let attendeeSnapshot = await dbClient.collection(event_base_path).doc("AttendeeList").collection("Attendees")
                .where('Email', '==', userEmail)
                .get();

            if (!attendeeSnapshot.empty) {
                let attendeeData = attendeeSnapshot.docs[0].data();
                attendeeId = attendeeData.AttendeeId;
                console.log("attendeeId", attendeeId)
            } else {
                ret_val = ERRCODE.PAYLOAD_ERROR;
                ret_val.message = 'No attendee found with the provided email.';
                reject(ret_val);
                return;
            }
        } else {
            ret_val = ERRCODE.PAYLOAD_ERROR;
            ret_val.message = 'No attendee found with the provided email or AttendeeId.';
            reject(ret_val);
            return;
        }

        try {
            // Fetch the duration from the config document
            let configRef = await dbClient.collection(event_base_path).doc("MeetingList").collection("Settings").doc("Config").get();
            let EventTimezone = configRef.exists ? configRef.data().EventTimezone : 'UTC';
            let attendeeRef = await dbClient.collection(event_base_path).doc("AttendeeList").collection("Attendees").doc(attendeeId).get();
            if (!attendeeRef.exists) {
                ret_val = ERRCODE.PAYLOAD_ERROR;
                reject(ret_val);
                return;
            }

            let Attendee = attendeeRef.data();

            let attendeeEmail = Attendee.Email;
            let filename = cleanEmailForFilename(attendeeEmail);

            // Check if there are any meetings for this attendee
            if (!Attendee.Meetings || Attendee.Meetings.length === 0) {
                const wb = XLSX.utils.book_new();
                const ws = XLSX.utils.aoa_to_sheet([["No meetings found for this attendee"]]);
                XLSX.utils.book_append_sheet(wb, ws, "No Meetings");

                const wbout = XLSX.write(wb, { bookType: 'xlsx', type: 'base64' });
                ret_val.file_data = wbout;
                ret_val.status = 0;
                resolve(ret_val);
                return;
            }

            let meetingTasks = [];

            if (Attendee.Meetings) {
                for (let meetingId of Attendee.Meetings) {
                    meetingTasks.push(dbClient.collection(event_base_path).doc("MeetingList").collection("Meetings").doc(meetingId).get());
                }
            }

            let meetingDocs = await Promise.all(meetingTasks);

            let meetingsByDay = {};
            for (let i = 0; i < meetingDocs.length; i++) {
                let meetingDoc = meetingDocs[i];
                let meetingData = meetingDoc.data();
                if (meetingData && meetingData.Status === 'Accepted') {
                    let startTime = moment(meetingData.Slots[0]).tz(EventTimezone); // Start time is the first slot

                    // Fix DST/Timezone difference by formatting in the correct timezone
                    const startTimeFormatted = startTime.format('HH:mm'); // Time formatted in AM/PM without seconds

                    let delegateName, delegateEmail, delegateCompany;
                    if (meetingData.Requestor.Email === userEmail) {
                        delegateName = `${meetingData.Invitee.FirstName} ${meetingData.Invitee.LastName}`;
                        delegateEmail = meetingData.Invitee.Email;

                        let inviteeRef = await dbClient.collection(event_base_path).doc("AttendeeList").collection("Attendees").doc(meetingData.Invitee.AttendeeId).get();
                        let invitee = inviteeRef.data();
                        delegateCompany = invitee.Company;
                    } else {
                        delegateName = `${meetingData.Requestor.FirstName} ${meetingData.Requestor.LastName}`;
                        delegateEmail = meetingData.Requestor.Email;
                        let requestorRef = await dbClient.collection(event_base_path).doc("AttendeeList").collection("Attendees").doc(meetingData.Requestor.AttendeeId).get();
                        let requestor = requestorRef.data();
                        delegateCompany = requestor.Company;
                    }

                    let meetingDay = startTime.format('DD-MMM-YYYY'); // Group by date (day)
                    if (!meetingsByDay[meetingDay]) {
                        meetingsByDay[meetingDay] = [];
                    }

                    // Add meeting data for this day
                    meetingsByDay[meetingDay].push({
                        MeetingId: meetingData.MeetingId,
                        StartTime: startTimeFormatted, // Now stores the time in AM/PM format
                        DelegateName: delegateName || '',
                        DelegateEmail: delegateEmail || '',
                        DelegateCompany: delegateCompany || ''
                    });
                }
            };

            Object.keys(meetingsByDay).forEach(day => {
                meetingsByDay[day].sort((a, b) => a.StartTime.localeCompare(b.StartTime)); // Sort by StartTime
            });

            const wb = XLSX.utils.book_new();

            Object.keys(meetingsByDay).forEach(day => {
                const worksheetData = meetingsByDay[day].map(meeting => ({
                    StartTime: meeting.StartTime, // This will now be in 'hh:mm A' format
                    DelegateName: meeting.DelegateName,
                    DelegateEmail: meeting.DelegateEmail,
                    DelegateCompany: meeting.DelegateCompany
                }));

                const header = ['StartTime', 'DelegateName', 'DelegateEmail', 'DelegateCompany'];
                const ws = XLSX.utils.json_to_sheet(worksheetData, { header });

                const cols = [
                    { wpx: 200 }, // StartTime
                    { wpx: 300 }, // DelegateName
                    { wpx: 300 }, // DelegateEmail
                    { wpx: 300 }, // DelegateCompany
                ];
                ws['!cols'] = cols;

                const headerStyle = {
                    fill: { fgColor: { rgb: "FFFF00" } }, // Yellow background
                    font: { bold: true }
                };

                ['A1', 'B1', 'C1', 'D1'].forEach(cell => {
                    ws[cell].s = headerStyle;
                });

                // Manually set the StartTime cells to the correct format
                worksheetData.forEach((row, index) => {
                    const startTimeCellRef = `A${index + 2}`; // StartTime cells
                    if (ws[startTimeCellRef]) {
                        ws[startTimeCellRef].t = 's'; // Set to string type
                    }
                });

                XLSX.utils.book_append_sheet(wb, ws, `${day}`);
            });

            console.log(JSON.stringify(meetingsByDay))

            wb.Workbook = { Views: [{ activeTab: 0 }] };

            const wbout = XLSX.write(wb, { bookType: 'xlsx', type: 'base64' });

            ret_val.file_data = wbout;
            ret_val.file_name = filename;
            ret_val.status = 0;
            resolve(ret_val);
        } catch (err) {
            console.error(err);
            ret_val.err = err;
            reject(err);
        }
    });
}
function get_meeting_detail(payload) {
    return new Promise(async (resolve, reject) => {
        logger.log(payload)
        let ret_val = { status: -1 };
        if (!payload.key) {
            ret_val = ERRCODE.PAYLOAD_ERROR
            reject(ret_val)
            return;
        }
        if (!payload.key.instanceId || !payload.key.clientId || !payload.key.eventId) {
            ret_val = ERRCODE.PAYLOAD_ERROR
            reject(ret_val)
            return;
        }
        const instance_base_path = "/" + payload.key.instanceId;
        const event_base_path = "/" + payload.key.instanceId + "_" + payload.key.clientId + payload.key.eventId;
        dbClient.collection(event_base_path).doc("MeetingList").collection("Meetings").doc(payload.data.MeetingId).get()
            .then((Meeting) => {
                if (!Meeting.exists) {
                    ret_val = ERRCODE.PAYLOAD_ERROR
                    reject(ret_val)
                    return;
                }
                ret_val.status = 0;
                ret_val.result = Meeting.data();
                resolve(ret_val)
            })
            .catch((err) => {
                logger.log(err);
                ret_val.err = err;
                reject(err);
            })
    })
}
function get_meeting_slots(payload) {
    return new Promise(async (resolve, reject) => {
        logger.log(payload)
        let tasks = [];
        let RequestorId;
        let InviteeId;
        let ret_val = { status: -1 };
        if (!payload.key) {
            ret_val = ERRCODE.PAYLOAD_ERROR
            reject(ret_val)
            return;
        }
        if (!payload.key.instanceId || !payload.key.clientId || !payload.key.eventId) {
            ret_val = ERRCODE.PAYLOAD_ERROR
            reject(ret_val)
            return;
        }
        RequestorId = payload.auth.data.UserId
        //RequestorId = "1324000";
        if (payload.data) {
            if (payload.data.attendeeId) {
                InviteeId = payload.data.attendeeId
            }
        }
        const instance_base_path = "/" + payload.key.instanceId;
        const event_base_path = "/" + payload.key.instanceId + "_" + payload.key.clientId + payload.key.eventId;
        tasks.push(dbClient.collection(event_base_path).doc("MeetingList").collection("Settings").doc("Config").get())
        tasks.push(dbClient.collection(event_base_path).doc("AttendeeList").collection("Attendees").doc(RequestorId).get())
        if (InviteeId) {
            tasks.push(dbClient.collection(event_base_path).doc("AttendeeList").collection("Attendees").doc(InviteeId).get())
        }
        Promise.all(tasks)
            .then(async (res) => {

                if (!res[0].exists) {
                    ret_val.err = 'Config Not Found';
                    reject(ret_val);
                    return;
                }
                if (!res[1].exists) {
                    ret_val.err = 'Requestor Not Found';
                    reject(ret_val);
                    return;
                }

                let Config = res[0].data();
                //console.log(Config.Slots)
                //console.log(Config.Slots.length)
                let Requestor = res[1].data();
                //console.log(Requestor.Slots.length)
                let availableSlots = []
                if (Config.Slots.length) {
                    if (Requestor.Slots) {
                        availableSlots = Config.Slots.filter(slot => !Requestor.Slots.includes(slot));
                    } else {
                        availableSlots = Config.Slots
                    }
                }
                if (InviteeId) {
                    if (res[2].exists) {
                        let Invitee = res[2].data();
                        //let InviteeConfirmedSlots = await _get_confirmed_meeting_slots(event_base_path, Invitee)
                        let InviteeConfirmedSlots = Invitee.Slots
                        if (InviteeConfirmedSlots) {
                            availableSlots = availableSlots.filter(slot => !InviteeConfirmedSlots.includes(slot));
                        }
                    }
                }
                ret_val.status = 0
                ret_val.slots = [];
                for (let slot of availableSlots) {
                    // let start = slot;
                    // var startTime = moment(start, 'hh:mm A');
                    // let end = moment(start).add(Config.AvailabilityToTime, 's').toDate();
                    // var endTime = moment(endString, 'hh:mm a');
                    ret_val.slots.push(slot)
                }
                if (payload.data) {
                    if (payload.data.fromDateStr && payload.data.toDateStr) {
                        ret_val.slots = ret_val.slots.filter(slot => {
                            //let dateStr = moment(slot).format('YYYY-MM-DD')
                            //return (moment(payload.data.fromDateStr) >= moment(slot) && moment(payload.data.toDateStr) < moment(slot))
                            payload.data.fromDateStr = moment((moment(payload.data.fromDateStr).valueOf()) - 1000).format()
                            return moment(slot).isBetween(payload.data.fromDateStr, payload.data.toDateStr);
                        });
                    }
                }
                resolve(ret_val)
            })
            .catch((err) => {
                logger.log(err);
                ret_val.err = err;
                reject(ret_val);
            })
    })
}
function create_meetings(payload) {
    return new Promise(async (resolve, reject) => {
        logger.log(payload)
        const instance_base_path = "/" + payload.key.instanceId;
        const event_base_path = "/" + payload.key.instanceId + "_" + payload.key.clientId + payload.key.eventId;
        let ret_val = { status: -1 }
        let workflow = new ExecutionsClient();
        let workflow_name = "create-meetings"

        let res = await dbClient.collection(event_base_path + '/MeetingList/Uploads').add({ Total: payload.data.length, Processed: 0, Failed: 0 })
        payload.key.docId = res.id;
        payload.api = {
            url: config.GCP.API_BASE + 'create-meeting',
            x_api_key: config.GCP.X_API_KEY
        }
        try {
            let createExecutionRes = await workflow.createExecution({
                parent: workflow.workflowPath(config.GCP.PROJECT_ID, config.GCP.LOCATION_ID, workflow_name),
                execution: {
                    argument: payload
                }
            });
            let executionName = createExecutionRes[0].name;
            ret_val.status = 0
            ret_val.docPath = event_base_path + '/MeetingList/Uploads/' + res.id
            ret_val.executionName = executionName
            resolve(ret_val)
        } catch (err) {
            logger.log(err);
            ret_val = ERRCODE.WORKFLOW_ERROR
            reject(ret_val)
        }
    })
}
function create_meeting(payload) {
    return new Promise(async (resolve, reject) => {
        logger.log(payload)
        let ret_val = { status: -1 }
        let pk = payload.key
        let pd = payload.data
        const instance_base_path = "/" + pk.instanceId;
        const event_base_path = "/" + pk.instanceId + "_" + pk.clientId + pk.eventId;
        try {
            let tasks = []
            tasks.push(dbClient.collection(event_base_path).doc("AttendeeList").collection("Attendees").where('Email', '==', pd.RequestorEmail).get())
            tasks.push(dbClient.collection(event_base_path).doc("AttendeeList").collection("Attendees").where('Email', '==', pd.InviteeEmail).get())
            let res = await Promise.all(tasks)
            if (!res[0].empty && !res[1].empty) {
                if (res[0].docs[0].exists && res[1].docs[0].exists) {
                    let ReqMeeting = {
                        key: pk,
                        data: {
                            RequestorId: res[0].docs[0].data().AttendeeId,
                            InviteeId: res[1].docs[0].data().AttendeeId,
                            Slots: pd.Slots
                        }
                    }
                    let MeetingRes = await request_meeting(ReqMeeting);
                    await dbClient.collection(event_base_path + '/MeetingList/Uploads').doc(pk.docId).set({ Processed: Firestore.FieldValue.increment(1) }, { merge: true })
                    ret_val.result = { MeetingId: MeetingRes.MeetingId, status: 'Requested' }
                    if (MeetingRes.status >= 0) {
                        if (pd[i].Status === 1) {
                            let AcceptMeeting = {
                                key: pk,
                                data: {
                                    MeetingId: MeetingRes.MeetingId
                                }
                            }
                            let AcceptRes = await accept_meeting(AcceptMeeting);
                            ret_val.result = { MeetingId: MeetingRes.MeetingId, status: 'Requested' }
                        }
                    }

                }
            }
            ret_val.status = 0
        } catch (err) {
            logger.log(err)
            await dbClient.collection(event_base_path + '/MeetingList/Uploads').doc(pk.docId).set({ Failed: Firestore.FieldValue.increment(1) }, { merge: true })
            ret_val = ERRCODE.PAYLOAD_ERROR
        }
        resolve(ret_val)
    })
}
function store_meetings(payload) {
    return new Promise(async (resolve, reject) => {
        logger.log(payload)
        let ret_val = { status: -1, results: {} }
        let pk = payload.key
        let pd = payload.data
        const instance_base_path = "/" + pk.instanceId;
        const event_base_path = "/" + pk.instanceId + "_" + pk.clientId + pk.eventId;

        try {
            for (let i = 0; i < pd.length; i++) {
                try {
                    let tasks = []
                    tasks.push(dbClient.collection(event_base_path).doc("AttendeeList").collection("Attendees").where('Email', '==', pd[i].RequestorEmail).get())
                    tasks.push(dbClient.collection(event_base_path).doc("AttendeeList").collection("Attendees").where('Email', '==', pd[i].InviteeEmail).get())
                    let res = await Promise.all(tasks)

                    if (!res[0].empty && !res[1].empty) {
                        if (res[0].docs[0].exists && res[1].docs[0].exists) {
                            let ReqMeeting = {
                                key: pk,
                                data: {
                                    RequestorId: res[0].docs[0].data().AttendeeId,
                                    InviteeId: res[1].docs[0].data().AttendeeId,
                                    Slots: pd[i].Slots
                                }
                            }

                            let MeetingRes = await request_meeting(ReqMeeting);
                            ret_val.results['Row' + i] = { MeetingId: MeetingRes.MeetingId, status: 'Requested' }
                            if (MeetingRes.status >= 0) {
                                if (pd[i].Status === 1) {
                                    let AcceptMeeting = {
                                        key: pk,
                                        data: {
                                            MeetingId: MeetingRes.MeetingId
                                        }
                                    }
                                    let AcceptRes = await accept_meeting(AcceptMeeting);
                                    ret_val.results['Row' + i] = { MeetingId: MeetingRes.MeetingId, status: 'Requested' }
                                }
                            }

                        }
                    }
                    ret_val.status = 0
                } catch (err) {
                    console.log(err)
                    ret_val.results['Row' + i] = ERRCODE.PAYLOAD_ERROR
                }
            }
            resolve(ret_val)
        } catch (err) {
            logger.log(err)
            ret_val = ERRCODE.UNKNOWN_ERROR
            reject(err)
        }
    })
}
function cancel_meeting(payload) {
    return new Promise(async (resolve, reject) => {
        logger.log(payload)
        let Meeting;
        let Invitee;
        let Requestor;
        let ret_val = { status: -1 };
        if (!payload.key) {
            ret_val = ERRCODE.PAYLOAD_ERROR
            reject(ret_val)
            return;
        }
        if (!payload.key.instanceId || !payload.key.clientId || !payload.key.eventId) {
            ret_val = ERRCODE.PAYLOAD_ERROR
            reject(ret_val)
            return;
        }
        const event_base_path = "/" + payload.key.instanceId + "_" + payload.key.clientId + payload.key.eventId;
        dbClient.collection(event_base_path).doc("MeetingList").collection("Meetings").doc(payload.data.MeetingId).get()
            .then((meetingRef) => {
                let tasks = []
                if (meetingRef.exists) {
                    Meeting = meetingRef.data();
                    tasks.push(dbClient.collection(event_base_path).doc("AttendeeList").collection("Attendees").doc(Meeting.Requestor.AttendeeId).get())
                    tasks.push(dbClient.collection(event_base_path).doc("AttendeeList").collection("Attendees").doc(Meeting.Invitee.AttendeeId).get())
                }
                return Promise.all(tasks)
            })
            .then((res) => {
                let tasks = [];
                if (res.length) {
                    Invitee = Meeting.Invitee
                    if (Meeting.Status !== "Canceled" && Meeting.Status !== "Rejected") {
                        if (res[0].exists) {
                            Requestor = res[0].data();
                            if (!Requestor.Meetings) {
                                Requestor.Meetings = []
                            }
                            if (!Requestor.Slots) {
                                Requestor.Slots = []
                            }
                            Requestor.Meetings = Requestor.Meetings.filter(meeting => meeting != payload.data.MeetingId);
                            Requestor.Slots = Requestor.Slots.filter(slot => !Meeting.Slots.includes(slot));
                            tasks.push(dbClient.collection(event_base_path).doc("AttendeeList").collection("Attendees").doc(Requestor.AttendeeId).set({ Meetings: Requestor.Meetings, Slots: Requestor.Slots }, { merge: true }))
                        }
                        if (res[1].exists) {
                            Invitee = res[1].data();
                        }
                        if (Meeting.Status == "Accepted") {

                            if (!Invitee.Meetings) {
                                Invitee.Meetings = []
                            }
                            if (!Invitee.Slots) {
                                Invitee.Slots = []
                            }
                            Invitee.Meetings = Invitee.Meetings.filter(meeting => meeting != payload.data.MeetingId);
                            Invitee.Slots = Invitee.Slots.filter(slot => !Meeting.Slots.includes(slot));

                            tasks.push(dbClient.collection(event_base_path).doc("AttendeeList").collection("Attendees").doc(Invitee.AttendeeId).set({ Meetings: Invitee.Meetings, Slots: Invitee.Slots }, { merge: true }))
                        }
                        tasks.push(dbClient.collection(event_base_path).doc("MeetingList").collection("Meetings").doc(payload.data.MeetingId).set({ LastUpdatedDateTime: new Date(), Status: "Canceled" }, { merge: true }));
                    } else {
                        ret_val = ERRCODE.DUPLICATE_OPERATION
                    }
                } else {
                    ret_val = ERRCODE.PAYLOAD_ERROR
                }
                return Promise.all(tasks);
            })
            .then(async (res) => {
                let tasks = []
                if (res.length) {
                    let topicName = 'cancel-meeting';
                    let pubsubPayload = {
                        Payload: payload,
                        Requestor: Requestor,
                        Invitee: Invitee,
                        Meeting: Meeting
                    }
                    if (payload.auth.data.UserId == Invitee.AttendeeId) {
                        Requestor = Meeting.Requestor
                        let data = {
                            Initials: (Invitee.Tags || ""),
                            Name: (Invitee.Name || ""),
                            ProfilePictureURL: (Invitee.ProfilePictureURL || ""),
                            MeetingType: "One2OneMeeting/MeetingCancelled",
                            NotificationMessage: "One to One meeting with " + (Invitee.Name || "") + " has been canceled",
                            NotificationTitle: "O2O meeting Request Canceled"
                        }

                        if (data.MeetingType && payload.auth) {
                            let NotificationTemplatePath = '/OA_UAT/NotificationTPL/' + data.MeetingType;
                            let TempRef = await dbClient.doc(NotificationTemplatePath).get()
                            if (TempRef.exists && payload.auth) {
                                let NotificationTemplate = TempRef.data()
                                let notificationMessageTemplate = Handlebars.compile(NotificationTemplate.Message);
                                data.NotificationMessage = notificationMessageTemplate(Invitee);
                                let notificationTitleTemplate = Handlebars.compile(NotificationTemplate.Title);
                                data.NotificationTitle = notificationTitleTemplate(Invitee);

                            }
                        }

                        tasks.push(utils.savePushAttendee(payload, event_base_path, Requestor.AttendeeId, data))
                    } else if (payload.auth.data.UserId == Requestor.AttendeeId) {
                        Invitee = Meeting.Invitee
                        let data = {
                            Initials: (Requestor.Tags || ""),
                            Name: (Requestor.Name || ""),
                            ProfilePictureURL: (Requestor.ProfilePictureURL || ""),
                            MeetingType: "One2OneMeeting/MeetingCancelled",
                            NotificationMessage: "One to One meeting with " + (Requestor.Name || "") + " has been canceled",
                            NotificationTitle: "O2O meeting Request Canceled"
                        }

                        if (data.MeetingType && payload.auth) {
                            let NotificationTemplatePath = '/OA_UAT/NotificationTPL/' + data.MeetingType;
                            let TempRef = await dbClient.doc(NotificationTemplatePath).get()
                            if (TempRef.exists && payload.auth) {
                                let NotificationTemplate = TempRef.data()
                                let notificationMessageTemplate = Handlebars.compile(NotificationTemplate.Message);
                                data.NotificationMessage = notificationMessageTemplate(Requestor);
                                let notificationTitleTemplate = Handlebars.compile(NotificationTemplate.Title);
                                data.NotificationTitle = notificationTitleTemplate(Requestor);

                            }
                        }

                        tasks.push(utils.savePushAttendee(payload, event_base_path, Invitee.AttendeeId, data))
                    }
                    let payloadBuffer = Buffer.from(JSON.stringify(pubsubPayload))
                    tasks.push(pubSubClient.topic(topicName).publish(payloadBuffer))
                    let result = await Promise.allSettled(tasks)
                    ret_val.status = 0;
                    ret_val.MeetingId = payload.data.MeetingId;
                    ret_val.result = result;
                    resolve(ret_val)
                } else {
                    reject(ret_val)
                }
            })
            .catch((err) => {
                console.log(err);
                ret_val = ERRCODE.UNKNOWN_ERROR
                reject(ret_val)
            })
    })
}
function pubsub_cancel_meeting(pubsubPayload) {
    return new Promise(async (resolve, reject) => {
        logger.log(pubsubPayload)
        let ret_val = { status: -1 }
        let payload = pubsubPayload.Payload;
        let Requestor = pubsubPayload.Requestor;
        let Invitee = pubsubPayload.Invitee;
        let Meeting = pubsubPayload.Meeting;
        const instance_base_path = "/" + payload.key.instanceId;
        const event_base_path = "/" + payload.key.instanceId + "_" + payload.key.clientId + payload.key.eventId;
        let startDateTime = moment(Meeting.Slots[0]).utc().format('MMM DD, YYYY hh:mm A');
        let timeZone = 'UTC';
        if (Meeting.Timezone) {
            startDateTime = moment.tz(Meeting.Slots[0], 'UTC').tz(Meeting.Timezone).format('MMM DD, YYYY hh:mm A');
            timeZone = Meeting.Timezone;
        }
        let MeetingAnalyticData = {
            meetingId: payload.data.MeetingId,
            meetingTsUTC: moment(Meeting.Slots[0]).utc().unix(),
            meetingType: "one2one",
            subject: (Meeting.Message || ""),
            timeZone: (Meeting.Timezone || ""),
            updated: moment().utc().unix(),
            status: Meeting.Status
        }
        let ptasks = []
        ptasks.push(_add_meeting_analytics(payload, MeetingAnalyticData, Requestor, Invitee))
        Promise.allSettled(ptasks)
            .then(res => {
                let tasks = []
                tasks.push(dbClient.collection(instance_base_path).doc("ClientList").collection("Clients").doc(payload.key.clientId).get())
                tasks.push(dbClient.collection(instance_base_path).doc("mailtpl").collection("Meeting").doc("Cancelled").get())
                tasks.push(dbClient.collection(event_base_path).doc("EventInfo").get())
                return Promise.all(tasks)
            })
            .then((res) => {
                let tasks = [];
                let EmailTemplate;
                let EmailPayload;
                let emailTemplate;
                let emailTemplateSub;
                let Placeholders;
                let email_body_html;
                let email_subject;
                let Client = res[0].data()
                let Event = res[2].data()
                let customDomain = (Client.CustomDomain) ? _add_https(Client.CustomDomain).replace(/\/?$/, '/') : 'https://onair.e2m.live/';
                let MeetingUrl = customDomain + 'mymeetings';
                if (res[1].exists) {
                    if (payload.auth.data.UserId == Invitee.AttendeeId) {
                        EmailTemplate = res[1].data();
                        Requestor = Meeting.Requestor
                        // if (Requestor.Timezone) {
                        //     startDateTime = moment.tz(Meeting.Slots[0], 'UTC').tz(Requestor.Timezone).format('DD-MMM-YYYY hh:mm A');
                        //     timeZone = Requestor.Timezone;
                        // }

                        Placeholders = {
                            ReceiverName: Requestor.Name,
                            SenderName: Invitee.Name,
                            Title: (Invitee.Title || ""),
                            Company: (Invitee.Company || ""),
                            Email: Invitee.Email,
                            StartDateTime: startDateTime,
                            Timezone: timeZone,
                            Team: "Team",
                            Message: (Meeting.Message || ""),
                            MeetingUrl: (MeetingUrl || ""),
                            ClientName: (Client.ClientName || ""),
                            EventLogo: (Event.EventLogo || "")
                        };
                        // if (!Placeholders.Company) {
                        //     EmailTemplate.html.replace('<span style="color: #34495e; font-size: 14px; line-height: 19.6px;"><span style="font-size: 18px; line-height: 25.2px;">Company: </span><span style="font-size: 18px; line-height: 25.2px;">{{Company}}</span></span><br />', '');
                        // }
                        // if (!Placeholders.Message) {
                        //     EmailTemplate.html.replace('<span style="color: #34495e; font-size: 14px; line-height: 19.6px;"><span style="font-size: 18px; line-height: 25.2px;">Message: </span><span style="font-size: 18px; line-height: 25.2px;">{{Message}}</span></span>', '');
                        // }
                        emailTemplate = Handlebars.compile(EmailTemplate.html);
                        emailTemplateSub = Handlebars.compile(EmailTemplate.subject);
                        email_body_html = emailTemplate(Placeholders);
                        email_subject = emailTemplateSub(Placeholders)
                        EmailPayload = {
                            from: {
                                email: EmailTemplate.from,
                                name: Client.ClientName
                            },
                            to: {
                                name: Requestor.Name,
                                email: Requestor.Email
                            },
                            cc: EmailTemplate.cc,
                            bcc: EmailTemplate.bcc,
                            reply_to: EmailTemplate.reply_to,
                            subject: email_subject,
                            html: email_body_html
                        };
                        let RequestedNotification = {
                            Initials: (Invitee.Tags || ""),
                            Name: (Invitee.Name || ""),
                            ProfilePictureURL: (Invitee.ProfilePictureURL || ""),
                            MeetingType: "One2OneMeeting/MeetingCancelled",
                            NotificationMessage: "One to One meeting with " + (Invitee.Tags || "") + " " + (Invitee.Name || "") + " has been canceled",
                            NotificationTitle: "O2O meeting Request Canceled"
                        }
                        tasks.push(_send_email(Client, EmailPayload))
                        tasks.push(utils.sendPushAttendee(payload, event_base_path, Requestor.AttendeeId, RequestedNotification))

                    } else if (payload.auth.data.UserId == Requestor.AttendeeId) {
                        EmailTemplate = res[1].data();
                        Invitee = Meeting.Invitee;
                        // if (Invitee.Timezone) {
                        //     startDateTime = moment.tz(Meeting.Slots[0], 'UTC').tz(Invitee.Timezone).format('DD-MMM-YYYY hh:mm A');
                        //     timeZone = Invitee.Timezone;
                        // }
                        Placeholders = {
                            ReceiverName: Invitee.Name,
                            SenderName: Requestor.Name,
                            Title: (Requestor.Title || ""),
                            Company: (Requestor.Company || ""),
                            Email: Requestor.Email,
                            StartDateTime: startDateTime,
                            Timezone: timeZone,
                            Team: "Team",
                            Message: (Meeting.Message || ""),
                            MeetingUrl: (MeetingUrl || ""),
                            ClientName: (Client.ClientName || ""),
                            EventLogo: (Event.EventLogo || "")
                        };
                        // if (!Placeholders.Company) {
                        //     EmailTemplate.html.replace('<span style="color: #34495e; font-size: 14px; line-height: 19.6px;"><span style="font-size: 18px; line-height: 25.2px;">Company: </span><span style="font-size: 18px; line-height: 25.2px;">{{Company}}</span></span><br />', '');
                        // }
                        // if (!Placeholders.Message) {
                        //     EmailTemplate.html.replace('<span style="color: #34495e; font-size: 14px; line-height: 19.6px;"><span style="font-size: 18px; line-height: 25.2px;">Message: </span><span style="font-size: 18px; line-height: 25.2px;">{{Message}}</span></span>', '');
                        // }
                        emailTemplate = Handlebars.compile(EmailTemplate.html);
                        emailTemplateSub = Handlebars.compile(EmailTemplate.subject);
                        email_body_html = emailTemplate(Placeholders);
                        email_subject = emailTemplateSub(Placeholders)
                        EmailPayload = {
                            from: {
                                email: EmailTemplate.from,
                                name: Client.ClientName
                            },
                            to: {
                                name: Invitee.Name,
                                email: Invitee.Email
                            },
                            cc: EmailTemplate.cc,
                            bcc: EmailTemplate.bcc,
                            reply_to: EmailTemplate.reply_to,
                            subject: email_subject,
                            html: email_body_html
                        };
                        let ReceivedNotification = {
                            Initials: (Requestor.Tags || ""),
                            Name: (Requestor.Name || ""),
                            ProfilePictureURL: (Requestor.ProfilePictureURL || ""),
                            MeetingType: "One2OneMeeting/MeetingCancelled",
                            NotificationMessage: "One to One meeting with " + (Requestor.Tags || "") + " " + (Requestor.Name || "") + " at " + startDateTime + " (" + timeZone + ") has been canceled",
                            NotificationTitle: "O2O meeting Request Canceled"
                        }

                        tasks.push(_send_email(Client, EmailPayload))
                        tasks.push(utils.sendPushAttendee(payload, event_base_path, Invitee.AttendeeId, ReceivedNotification))
                    }
                }

                return Promise.allSettled(tasks)
            })
            .then((res) => {
                ret_val.status = 0;
                ret_val.result = payload.data;
                resolve(ret_val)
            })
            .catch((err) => {
                logger.log(err);
                ret_val = ERRCODE.UNKNOWN_ERROR
                reject(ret_val)
                return;
            })
    })
}
function reject_meeting(payload) {
    return new Promise(async (resolve, reject) => {
        logger.log(payload)
        let Meeting;
        let Invitee;
        let Requestor;
        let ret_val = { status: -1 };
        if (!payload.key) {
            ret_val = ERRCODE.PAYLOAD_ERROR
            reject(ret_val)
            return;
        }
        if (!payload.key.instanceId || !payload.key.clientId || !payload.key.eventId) {
            ret_val = ERRCODE.PAYLOAD_ERROR
            reject(ret_val)
            return;
        }
        const event_base_path = "/" + payload.key.instanceId + "_" + payload.key.clientId + payload.key.eventId;
        dbClient.collection(event_base_path).doc("MeetingList").collection("Meetings").doc(payload.data.MeetingId).get()
            .then((meetingRef) => {
                let tasks = []
                Meeting = meetingRef.data();
                tasks.push(dbClient.collection(event_base_path).doc("AttendeeList").collection("Attendees").doc(Meeting.Requestor.AttendeeId).get())
                tasks.push(dbClient.collection(event_base_path).doc("AttendeeList").collection("Attendees").doc(Meeting.Invitee.AttendeeId).get())
                return Promise.all(tasks)
            })
            .then((res) => {
                let tasks = [];
                if (res.length) {
                    if (Meeting.Status !== "Canceled" && Meeting.Status !== "Rejected") {

                        if (res[0].exists) {
                            Requestor = res[0].data();
                            if (!Requestor.Meetings) {
                                Requestor.Meetings = []
                            }
                            if (!Requestor.Slots) {
                                Requestor.Slots = []
                            }
                            Requestor.Meetings = Requestor.Meetings.filter(meeting => meeting != payload.data.MeetingId);
                            Requestor.Slots = Requestor.Slots.filter(slot => !Meeting.Slots.includes(slot));

                            tasks.push(dbClient.collection(event_base_path).doc("AttendeeList").collection("Attendees").doc(Requestor.AttendeeId).set({ Meetings: Requestor.Meetings, Slots: Requestor.Slots }, { merge: true }))
                        }
                        if (res[1].exists) {
                            Invitee = res[1].data();
                        }
                        if (Meeting.Status == "Accepted") {
                            if (!Invitee.Meetings) {
                                Invitee.Meetings = []
                            }
                            if (!Invitee.Slots) {
                                Invitee.Slots = []
                            }
                            Invitee.Meetings = Invitee.Meetings.filter(meeting => meeting != payload.data.MeetingId);
                            Invitee.Slots = Invitee.Slots.filter(slot => !Meeting.Slots.includes(slot));
                            tasks.push(dbClient.collection(event_base_path).doc("AttendeeList").collection("Attendees").doc(Invitee.AttendeeId).set({ Meetings: Invitee.Meetings, Slots: Invitee.Slots }, { merge: true }))
                        }

                        tasks.push(dbClient.collection(event_base_path).doc("MeetingList").collection("Meetings").doc(payload.data.MeetingId).set({ LastUpdatedDateTime: new Date(), Status: "Rejected" }, { merge: true }));
                    } else {
                        ret_val = ERRCODE.DUPLICATE_OPERATION
                    }
                } else {
                    ret_val = ERRCODE.PAYLOAD_ERROR
                }
                return Promise.all(tasks);
            })
            .then(async (res) => {
                let tasks = []
                if (res.length) {
                    let topicName = 'reject-meeting';
                    let pubsubPayload = {
                        Payload: payload,
                        Requestor: Requestor,
                        Invitee: Invitee,
                        Meeting: Meeting
                    }
                    if (payload.auth.data.UserId == Invitee.AttendeeId) {
                        Requestor = Meeting.Requestor
                        let data = {
                            Initials: (Invitee.Tags || ""),
                            Name: (Invitee.Name || ""),
                            ProfilePictureURL: (Invitee.ProfilePictureURL || ""),
                            MeetingType: "One2OneMeeting/MeetingDeclined",
                            NotificationMessage: "One to One meeting with " + (Invitee.Name || "") + " has been declined",
                            NotificationTitle: "O2O meeting Request Declined"
                        }


                        if (data.MeetingType && payload.auth) {
                            let NotificationTemplatePath = '/OA_UAT/NotificationTPL/' + data.MeetingType;
                            let TempRef = await dbClient.doc(NotificationTemplatePath).get()
                            if (TempRef.exists && payload.auth) {
                                let NotificationTemplate = TempRef.data()
                                let notificationMessageTemplate = Handlebars.compile(NotificationTemplate.Message);
                                data.NotificationMessage = notificationMessageTemplate(Invitee);
                                let notificationTitleTemplate = Handlebars.compile(NotificationTemplate.Title);
                                data.NotificationTitle = notificationTitleTemplate(Invitee);

                            }
                        }

                        tasks.push(utils.savePushAttendee(payload, event_base_path, Requestor.AttendeeId, data))
                    } else if (payload.auth.data.UserId == Requestor.AttendeeId) {
                        Invitee = Meeting.Invitee
                        let data = {
                            Initials: (Requestor.Tags || ""),
                            Name: (Requestor.Name || ""),
                            ProfilePictureURL: (Requestor.ProfilePictureURL || ""),
                            MeetingType: "One2OneMeeting/MeetingCancelled",
                            NotificationMessage: "One to One meeting with " + (Requestor.Name || "") + " has been declined",
                            NotificationTitle: "O2O meeting Request Declined"
                        }


                        if (data.MeetingType && payload.auth) {
                            let NotificationTemplatePath = '/OA_UAT/NotificationTPL/' + data.MeetingType;
                            let TempRef = await dbClient.doc(NotificationTemplatePath).get()
                            if (TempRef.exists && payload.auth) {
                                let NotificationTemplate = TempRef.data()
                                let notificationMessageTemplate = Handlebars.compile(NotificationTemplate.Message);
                                data.NotificationMessage = notificationMessageTemplate(Requestor);
                                let notificationTitleTemplate = Handlebars.compile(NotificationTemplate.Title);
                                data.NotificationTitle = notificationTitleTemplate(Requestor);

                            }
                        }

                        tasks.push(utils.savePushAttendee(payload, event_base_path, Invitee.AttendeeId, data))
                    }
                    let payloadBuffer = Buffer.from(JSON.stringify(pubsubPayload))
                    tasks.push(pubSubClient.topic(topicName).publish(payloadBuffer))
                    let results = await Promise.allSettled(tasks)
                    ret_val.status = 0;
                    ret_val.MeetingId = payload.data.MeetingId;
                    ret_val.result = results;
                    resolve(ret_val)
                } else {
                    reject(ret_val)
                }
            })
            .catch((err) => {
                console.log(err);
                ret_val = ERRCODE.UNKNOWN_ERROR
                reject(ret_val)
            })
    })
}
function pubsub_reject_meeting(pubsubPayload) {
    return new Promise(async (resolve, reject) => {
        logger.log(pubsubPayload)
        let ret_val = { status: -1 }
        let payload = pubsubPayload.Payload;
        let Requestor = pubsubPayload.Requestor;
        let Invitee = pubsubPayload.Invitee;
        let Meeting = pubsubPayload.Meeting;

        const instance_base_path = "/" + payload.key.instanceId;
        const event_base_path = "/" + payload.key.instanceId + "_" + payload.key.clientId + payload.key.eventId;
        let startDateTime = moment(Meeting.Slots[0]).utc().format('MMM DD, YYYY hh:mm A');
        let timeZone = 'UTC';
        if (Meeting.Timezone) {
            startDateTime = moment.tz(Meeting.Slots[0], 'UTC').tz(Meeting.Timezone).format('MMM DD, YYYY hh:mm A');
            timeZone = Meeting.Timezone;
        }
        let MeetingAnalyticData = {
            meetingId: payload.data.MeetingId,
            meetingTsUTC: moment(Meeting.Slots[0]).utc().unix(),
            meetingType: "one2one",
            subject: (Meeting.Message || ""),
            timeZone: (Meeting.Timezone || ""),
            updated: moment().utc().unix(),
            status: Meeting.Status
        }
        let ptasks = []
        ptasks.push(_add_meeting_analytics(payload, MeetingAnalyticData, Requestor, Invitee))
        Promise.allSettled(ptasks)
            .then(res => {
                let tasks = []
                tasks.push(dbClient.collection(instance_base_path).doc("ClientList").collection("Clients").doc(payload.key.clientId).get())
                tasks.push(dbClient.collection(instance_base_path).doc("mailtpl").collection("Meeting").doc("Rejected").get())
                tasks.push(dbClient.collection(event_base_path).doc("EventInfo").get())
                return Promise.all(tasks)
            })
            .then((res) => {
                let tasks = [];
                let EmailTemplate;
                let EmailPayload;
                let emailTemplate;
                let emailTemplateSub;
                let Placeholders;
                let email_body_html;
                let email_subject;
                let Client = res[0].data()
                let Event = res[2].data()
                let customDomain = (Client.CustomDomain) ? _add_https(Client.CustomDomain).replace(/\/?$/, '/') : 'https://onair.e2m.live/';
                let MeetingUrl = customDomain + 'mymeetings';
                if (res[1].exists) {
                    if (payload.auth.data.UserId == Invitee.AttendeeId) {
                        EmailTemplate = res[1].data();
                        Requestor = Meeting.Requestor;
                        // if (Requestor.Timezone) {
                        //     startDateTime = moment.tz(Meeting.Slots[0], 'UTC').tz(Requestor.Timezone).format('DD-MMM-YYYY hh:mm A');
                        //     timeZone = Requestor.Timezone;
                        // }

                        Placeholders = {
                            ReceiverName: Requestor.Name,
                            SenderName: Invitee.Name,
                            Title: (Invitee.Title || ""),
                            Company: (Invitee.Company || ""),
                            Email: Invitee.Email,
                            StartDateTime: startDateTime,
                            Timezone: timeZone,
                            Team: "Team",
                            Message: (Meeting.Message || ""),
                            MeetingUrl: (MeetingUrl || ""),
                            ClientName: (Client.ClientName || ""),
                            EventLogo: (Event.EventLogo || "")
                        };
                        // if (!Placeholders.Company) {
                        //     EmailTemplate.html.replace('<span style="color: #34495e; font-size: 14px; line-height: 19.6px;"><span style="font-size: 18px; line-height: 25.2px;">Company: </span><span style="font-size: 18px; line-height: 25.2px;">{{Company}}</span></span><br />', '');
                        // }
                        // if (!Placeholders.Message) {
                        //     EmailTemplate.html.replace('<span style="color: #34495e; font-size: 14px; line-height: 19.6px;"><span style="font-size: 18px; line-height: 25.2px;">Message: </span><span style="font-size: 18px; line-height: 25.2px;">{{Message}}</span></span>', '');
                        // }
                        emailTemplate = Handlebars.compile(EmailTemplate.html);
                        emailTemplateSub = Handlebars.compile(EmailTemplate.subject);
                        email_body_html = emailTemplate(Placeholders);
                        email_subject = emailTemplateSub(Placeholders)
                        EmailPayload = {
                            from: {
                                email: EmailTemplate.from,
                                name: Client.ClientName
                            },
                            to: {
                                name: Requestor.Name,
                                email: Requestor.Email
                            },
                            cc: EmailTemplate.cc,
                            bcc: EmailTemplate.bcc,
                            reply_to: EmailTemplate.reply_to,
                            subject: email_subject,
                            html: email_body_html
                        };
                        let RequestedNotification = {
                            Initials: (Invitee.Tags || ""),
                            Name: (Invitee.Name || ""),
                            ProfilePictureURL: (Invitee.ProfilePictureURL || ""),
                            MeetingType: "One2OneMeeting/MeetingDeclined",
                            NotificationMessage: "One to One meeting with " + (Invitee.Tags || "") + " " + (Invitee.Name || "") + " has been declined",
                            NotificationTitle: "O2O meeting Request Declined"
                        }
                        tasks.push(_send_email(Client, EmailPayload))
                        tasks.push(utils.sendPushAttendee(payload, event_base_path, Requestor.AttendeeId, RequestedNotification))

                    } else if (payload.auth.data.UserId == Requestor.AttendeeId) {
                        EmailTemplate = res[1].data();
                        Invitee = Meeting.Invitee
                        // if (Invitee.Timezone) {
                        //     startDateTime = moment.tz(Meeting.Slots[0], 'UTC').tz(Invitee.Timezone).format('DD-MMM-YYYY hh:mm A');
                        //     timeZone = Invitee.Timezone;
                        // }

                        Placeholders = {
                            ReceiverName: Invitee.Name,
                            SenderName: Requestor.Name,
                            Title: (Requestor.Title || ""),
                            Company: (Requestor.Company || ""),
                            Email: Requestor.Email,
                            StartDateTime: startDateTime,
                            Timezone: timeZone,
                            Team: "Team",
                            Message: (Meeting.Message || ""),
                            MeetingUrl: (MeetingUrl || ""),
                            ClientName: (Client.ClientName || ""),
                            EventLogo: (Event.EventLogo || "")
                        };
                        // if (!Placeholders.Company) {
                        //     EmailTemplate.html.replace('<span style="color: #34495e; font-size: 14px; line-height: 19.6px;"><span style="font-size: 18px; line-height: 25.2px;">Company: </span><span style="font-size: 18px; line-height: 25.2px;">{{Company}}</span></span><br />', '');
                        // }
                        // if (!Placeholders.Message) {
                        //     EmailTemplate.html.replace('<span style="color: #34495e; font-size: 14px; line-height: 19.6px;"><span style="font-size: 18px; line-height: 25.2px;">Message: </span><span style="font-size: 18px; line-height: 25.2px;">{{Message}}</span></span>', '');
                        // }
                        emailTemplate = Handlebars.compile(EmailTemplate.html);
                        emailTemplateSub = Handlebars.compile(EmailTemplate.subject);
                        email_body_html = emailTemplate(Placeholders);
                        email_subject = emailTemplateSub(Placeholders)
                        EmailPayload = {
                            from: {
                                email: EmailTemplate.from,
                                name: Client.ClientName
                            },
                            to: {
                                name: Invitee.Name,
                                email: Invitee.Email
                            },
                            cc: EmailTemplate.cc,
                            bcc: EmailTemplate.bcc,
                            reply_to: EmailTemplate.reply_to,
                            subject: email_subject,
                            html: email_body_html
                        };
                        let ReceivedNotification = {
                            Initials: (Requestor.Tags || ""),
                            Name: (Requestor.Name || ""),
                            ProfilePictureURL: (Requestor.ProfilePictureURL || ""),
                            MeetingType: "One2OneMeeting/MeetingCancelled",
                            NotificationMessage: "One to One meeting with " + (Requestor.Tags || "") + " " + (Requestor.Name || "") + " has been declined",
                            NotificationTitle: "O2O meeting Request Declined"
                        }

                        tasks.push(_send_email(Client, EmailPayload))
                        tasks.push(utils.sendPushAttendee(payload, event_base_path, Invitee.AttendeeId, ReceivedNotification))
                    }
                }
                return Promise.allSettled(tasks)
            })
            .then((res) => {
                ret_val.status = 0;
                ret_val.result = payload.data;
                resolve(ret_val)
            })
            .catch((err) => {
                logger.log(err);
                ret_val = ERRCODE.UNKNOWN_ERROR
                reject(ret_val)
                return;
            })
    })
}
function delete_meetings(payload) {
    return new Promise(async (resolve, reject) => {
        logger.log(payload)
        let ret_val = { status: -1 };
        if (!payload.key) {
            ret_val = ERRCODE.PAYLOAD_ERROR
            reject(ret_val)
            return;
        }
        if (!payload.key.instanceId || !payload.key.clientId || !payload.key.eventId) {
            ret_val = ERRCODE.PAYLOAD_ERROR
            reject(ret_val)
            return;
        }
        const instance_base_path = "/" + payload.key.instanceId;
        const event_base_path = "/" + payload.key.instanceId + "_" + payload.key.clientId + payload.key.eventId;
        let tasks = []
        for (let i = 0; i < payload.data.Meetings.length; i++) {
            tasks.push(_delete_meeting_single(event_base_path, payload.data.Meetings[i]))
        }
        Promise.all(tasks)
            .then(async (res) => {
                let tasks = [];
                let deleted_meetings = {};
                for (let i = 0; i < res.length; i++) {
                    let item = res[i];
                    for (let j = 0; j < item.data.length; j++) {

                        if (deleted_meetings[item.data[j].AttendeeId]) {
                            deleted_meetings[item.data[j].AttendeeId].Meetings.push(item.data[j].MeetingId)
                        } else {
                            deleted_meetings[item.data[j].AttendeeId] = { Meetings: [], Slots: [] };
                            deleted_meetings[item.data[j].AttendeeId].Meetings.push(item.data[j].MeetingId)
                        }

                        if (deleted_meetings[item.data[j].AttendeeId]) {
                            deleted_meetings[item.data[j].AttendeeId].Slots.push(item.data[j].Slot)
                        } else {
                            deleted_meetings[item.data[j].AttendeeId] = { Meetings: [], Slots: [] };
                            deleted_meetings[item.data[j].AttendeeId].Slots.push(item.data[j].MeetingId)
                        }
                    }

                }
                for (const [key, value] of Object.entries(deleted_meetings)) {
                    tasks.push(_update_attendee_meeting_slots(event_base_path, key, value));
                }
                await Promise.all(tasks)
                ret_val.status = 0;
                ret_val.result = res
                resolve(ret_val)
            })
            .catch((err) => {
                logger.log(err);
                ret_val = ERRCODE.UNKNOWN_ERROR
                reject(ret_val)
                return;
            })
    })
}

function _delete_meeting_single(event_base_path, MeetingId) {
    return new Promise(async (resolve, reject) => {
        let Meeting;
        let Invitee;
        let Requestor;
        let ret_val = { status: -1, data: [] };
        dbClient.collection(event_base_path).doc("MeetingList").collection("Meetings").doc(MeetingId).get()
            .then((meetingRef) => {
                let tasks = []
                Meeting = meetingRef.data();
                tasks.push(dbClient.collection(event_base_path).doc("AttendeeList").collection("Attendees").doc(Meeting.Requestor.AttendeeId).get())
                tasks.push(dbClient.collection(event_base_path).doc("AttendeeList").collection("Attendees").doc(Meeting.Invitee.AttendeeId).get())
                return Promise.all(tasks)
            })
            .then((res) => {
                let tasks = [];
                if (res[0].exists) {
                    Requestor = res[0].data();
                    if (!Requestor.Meetings) {
                        Requestor.Meetings = []
                    }
                    if (!Requestor.Slots) {
                        Requestor.Slots = []
                    }

                    ret_val.data.push({
                        AttendeeId: Meeting.Requestor.AttendeeId,
                        MeetingId: MeetingId,
                        Slot: Meeting.Slots[0]
                    })

                    //Requestor.Meetings = Requestor.Meetings.filter(meeting => meeting != MeetingId);
                    //Requestor.Slots = Requestor.Slots.filter(slot => !Meeting.Slots.includes(slot));
                    //tasks.push(dbClient.collection(event_base_path).doc("AttendeeList").collection("Attendees").doc(Requestor.AttendeeId).set({ Meetings: Requestor.Meetings, Slots: Requestor.Slots }, { merge: true }))
                }
                if (res[1].exists) {
                    Invitee = res[1].data();
                    if (!Invitee.Meetings) {
                        Invitee.Meetings = []
                    }
                    if (!Invitee.Slots) {
                        Invitee.Slots = []
                    }

                    ret_val.data.push({
                        AttendeeId: Meeting.Invitee.AttendeeId,
                        MeetingId: MeetingId,
                        Slot: Meeting.Slots[0]
                    })
                    // Invitee.Meetings = Invitee.Meetings.filter(meeting => meeting != MeetingId);
                    // Invitee.Slots = Invitee.Slots.filter(slot => !Meeting.Slots.includes(slot));
                    // tasks.push(dbClient.collection(event_base_path).doc("AttendeeList").collection("Attendees").doc(Invitee.AttendeeId).set({ Meetings: Invitee.Meetings, Slots: Invitee.Slots }, { merge: true }))
                }
                tasks.push(dbClient.collection(event_base_path).doc("MeetingList").collection("Meetings").doc(MeetingId).delete())
                return Promise.all(tasks);
            })
            .then((res) => {
                ret_val.status = 0;
                ret_val.result = res
                resolve(ret_val)
            })
            .catch((err) => {
                logger.log(err);
                ret_val = ERRCODE.UNKNOWN_ERROR
                reject(ret_val)
                return;
            })
    })
}
function _update_attendee_meeting_slots(event_base_path, attendeeId, data) {
    return new Promise(async (resolve, reject) => {
        let Meeting;
        let Attendee;
        let ret_val = { status: -1, data: [] };
        dbClient.collection(event_base_path).doc("AttendeeList").collection("Attendees").doc(attendeeId).get()
            .then((attendee) => {
                Attendee = attendee.data();
                if (!Attendee.Meetings) {
                    Attendee.Meetings = []
                }
                if (!Attendee.Slots) {
                    Attendee.Slots = []
                }

                let Meetings = Attendee.Meetings.filter(meetingId => !data.Meetings.includes(meetingId));
                let Slots = Attendee.Slots.filter(slot => !data.Slots.includes(slot));
                return dbClient.collection(event_base_path).doc("AttendeeList").collection("Attendees").doc(attendeeId).set({ Meetings: Meetings, Slots: Slots }, { merge: true })
            })
            .then((res) => {
                ret_val.status = 0;
                ret_val.result = res
                resolve(ret_val)
            })
            .catch((err) => {
                logger.log(err);
                ret_val = ERRCODE.UNKNOWN_ERROR
                reject(ret_val)
                return;
            })
    })
}
function _slots(startString, endString, duration, Timezone) {
    return new Promise(async (resolve, reject) => {
        let ret_val = ERRCODE.UNKNOWN_ERROR
        try {
            startString = moment(startString).utc().format('YYYY-MM-DD hh:mm a')
            endString = moment(endString).utc().format('YYYY-MM-DD hh:mm a')

            var start = moment.tz(startString, 'YYYY-MM-DD hh:mm a', Timezone).utc();
            var end = moment.tz(endString, 'YYYY-MM-DD hh:mm a', Timezone).utc();
            //var end = moment(endString, 'YYYY-MM-DD hh:mm a');
            // round starting minutes up to nearest 15 (12 --> 15, 17 --> 30)
            // note that 59 will round up to 60, and moment.js handles that correctly
            start.minutes(Math.ceil(start.minutes() / duration) * duration);
            var result = [];
            var current = moment(start);
            if (current && end) {
                while (current < end) {
                    result.push(current.format('YYYY-MM-DDTHH:mm:ss[Z]'));
                    current.add(duration, 'minutes');
                }
            }
            resolve(result);
        } catch (err) {
            logger.log(err);
            reject(ret_val)
            return;

        }
    })
}
function _get_confirmed_meeting_slots(event_base_path, Attendee) {
    return new Promise(async (resolve, reject) => {
        let tasks = [];
        let slots = [];
        let ret_val = { status: -1 };
        if (Attendee.Meetings) {
            for (let i = 0; i < Attendee.Meetings.length; i++) {
                tasks.push(dbClient.collection(event_base_path).doc("MeetingList").collection("Meetings").doc(Attendee.Meetings[i]).get())
            }
        }
        Promise.all(tasks)
            .then((res) => {
                if (res.length) {
                    for (let i = 0; i < res.length; i++) {
                        let MeetingData = res[i].data()
                        if (MeetingData) {
                            if (MeetingData.Status == "Accepted") {
                                slots.push(MeetingData.Slots[0])
                            }
                        }
                    }
                }
                resolve(slots)
            })
            .catch((err) => {
                logger.log(err);
                ret_val = ERRCODE.UNKNOWN_ERROR
                reject(ret_val)
                return;
            })
    })
}
function _add_meeting_analytics(payload, meeting, requestor, invitee) {
    return new Promise(async (resolve, reject) => {
        let ret_val = { status: -1 };
        let url = config.ANALYTICS.MEETING.URL;
        let data = {
            instanceId: payload.key.instanceId,
            clientId: payload.key.clientId,
            eventId: payload.key.eventId,
            requester: {
                id: requestor.AttendeeId,
                name: requestor.Name,
                email: requestor.Email,
                company: requestor.Company,
                designation: requestor.Designation,
                phone: requestor.Phone
            },
            invitee: {
                id: invitee.AttendeeId,
                name: invitee.Name,
                email: invitee.Email,
                company: invitee.Company,
                designation: invitee.Designation,
                phone: invitee.Phone
            },
            meetingId: meeting.meetingId,
            meetingTsUTC: meeting.meetingTsUTC, //-- UTC timestamp in seconds
            meetingType: meeting.meetingType,
            subject: meeting.subject,
            timeZone: meeting.timeZone,
            status: meeting.status,
            meta: {
                //-- should be omitted if called from client side
                callerIP: payload.ip,
                userAgent: payload.user_agent,
            },
        };
        axios.post(url, data, { headers: { 'Content-Type': 'application/json' } })
            .then((res) => {
                ret_val.status = 0;
                resolve(ret_val)
            })
            .catch((err) => {
                logger.log(err)
                ret_val = ERRCODE.UNKNOWN_ERROR
                reject(ret_val)
                return;
            })
    })
}
function _add_https(url) {
    if (!/^(?:f|ht)tps?\:\/\//.test(url)) {
        url = "https://" + url;
    }
    return url;
}
function cleanEmailForFilename(email) {
    // Extract the first part of the email before '@'
    let emailUsername = email.split('@')[0].toLowerCase();

    // Replace any character that is not a letter, number, or period
    return emailUsername.replace(/[^a-z0-9.]/gi, '_');
}


module.exports = {
    createMeetings: create_meetings,
    createMeeting: create_meeting,
    storeMeetings: store_meetings,
    getMeetings: get_meetings,
    getMeetingDetail: get_meeting_detail,
    getMeetingSlots: get_meeting_slots,
    cancelMeeting: cancel_meeting,
    pubsubCancelMeeting: pubsub_cancel_meeting,
    rejectMeeting: reject_meeting,
    pubsubRejectMeeting: pubsub_reject_meeting,
    deleteMeetings: delete_meetings,
    setMeetingConfig: set_meeting_config,
    getMeetingConfig: get_meeting_config,
    downloadMeetings: download_meetings,

    getMeetingQnA: get_meeting_qna,
    setMeetingQnA: set_meeting_qna,
    availableAttendees: available_attendees,
    availableSpeakers: available_speakers,
    availableSponsors: available_sponsors,
    draftAttendees: draft_attendees,
    saveAsDraft: save_as_draft,
    removeFromDraft: remove_from_draft,
    requestMeetings: request_meetings,
    meetingAttendees: meeting_attendees,
    attendeeMeetings: attendee_meetings,
    confirmMeeting: confirm_meeting,
    pubsubConfirmMeeting: pubsub_confirm_meeting,
    meetingReminder: meeting_reminder
}