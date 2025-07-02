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
const { htmlToText } = require('html-to-text');
const OpenAI = require('openai');

// var meetingApp = admin.initializeApp({
//     credential: admin.credential.cert(config.SERVICE_ACCOUNT)
// }, "meetingApp");

const { createClient } = require('@supabase/supabase-js')

const supabase = createClient(config.SUPABASE.DATABASE, config.SUPABASE.KEY)

const openai = new OpenAI({
    apiKey: config.OPENAI.API_KEY, // This is the default and can be omitted
});

const pubSubClient = new PubSub({
    projectId: config.GCP.PROJECT_ID,
    keyFilename: (__dirname + config.GCP.KEY_FILE_PATH).replace('/lib/', '/')
    // keyFilename: ('E:\\Debashis\\OnAir\\E2M_AI\\OnAirEXT_Dev\\creds\\prd_key.json')
});
const dbClient = new Firestore({
    projectId: config.GCP.PROJECT_ID,
    keyFilename: (__dirname + config.GCP.KEY_FILE_PATH).replace('/lib/', '/')
    // keyFilename: ('E:\\Debashis\\OnAir\\E2M_AI\\OnAirEXT_Dev\\creds\\prd_key.json')
});
const taskClient = new CloudTasksClient({
    projectId: config.GCP.PROJECT_ID,
    keyFilename: (__dirname + config.GCP.KEY_FILE_PATH).replace('/lib/', '/')
    // keyFilename: ('E:\\Debashis\\OnAir\\E2M_AI\\OnAirEXT_Dev\\creds\\prd_key.json')
});

async function get_meeting_qna(payload) {
    return new Promise(async (resolve, reject) => {
        let ret_val = { status: -1 };

        if (!payload.key || !payload.key.instanceId || !payload.key.clientId || !payload.key.eventId) {
            return reject(ERRCODE.PAYLOAD_ERROR);
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
                return reject(ERRCODE.PAYLOAD_ERROR);
            }

            const attendeeData = attendeeRef.data();
            const registrationType = attendeeData?.RegistrationType?.RegistrationType?.toLowerCase();
            const registrationTypeEntityId = attendeeData?.RegistrationType?.RegistrationTypeEntityId;

            if (!registrationType) {
                return reject(ERRCODE.PAYLOAD_ERROR);
            }

            // Get questions
            const questionSnap = await dbClient
                .collection(event_base_path)
                .doc("MeetingQnA")
                .collection("questions")
                .where("isPublished", "==", true)
                .orderBy("displayOrder")
                .get();

            let questions = [];

            for (const doc of questionSnap.docs) {
                const data = doc.data();
                const roleVisibility = data.roleVisibility?.map(r => r.toLowerCase()) || [];

                if (!roleVisibility.includes(registrationType)) continue;

                const questionId = doc.id;

                // Determine QnA answer path
                let answerDocPath = null;

                if (registrationType === "sponsor" && registrationTypeEntityId) {
                    answerDocPath = `${event_base_path}/SponsorList/Sponsors/${registrationTypeEntityId}/MeetingQnA/${questionId}`;
                } else {
                    answerDocPath = `${event_base_path}/AttendeeList/Attendees/${attendeeId}/MeetingQnA/${questionId}`;
                }

                let answerData = null;

                try {
                    const answerDoc = await dbClient.doc(answerDocPath).get();
                    if (answerDoc.exists) {
                        answerData = answerDoc.data();
                    }
                } catch (err) {
                    console.warn(`Error fetching answer for question ${questionId}:`, err);
                }

                questions.push({
                    id: questionId,
                    ...data,
                    selectedValue: answerData?.selectedValue || null,
                    updateBy: answerData?.updateBy || null
                });
            }

            ret_val.status = 0;
            ret_val.result = questions;
            return resolve(ret_val);
        } catch (err) {
            logger.log(err);
            ret_val.err = err;
            return reject(ret_val);
        }
    });
}
async function set_meeting_qna(payload) {
    // console.log(payload)
    return new Promise(async (resolve, reject) => {
        let ret_val = { status: -1 };

        if (!payload.key || !payload.key.instanceId || !payload.key.clientId || !payload.key.eventId ||
            !payload.data || !Array.isArray(payload.data.answers)) {
            ret_val = ERRCODE.PAYLOAD_ERROR;
            return reject(ret_val);
        }

        const iceId = `${payload.key.instanceId}_${payload.key.clientId}${payload.key.eventId}`;
        const event_base_path = `/${iceId}`;
        const onDuplicateUpdate = payload.data?.onDuplicateUpdate === true;

        const CurrentUid = payload.auth.data.UserId;
        const inputAttendeeId = payload.data?.AttendeeId?.toString();
        const attendeeId = inputAttendeeId || CurrentUid;

        try {
            const attendeeRef = dbClient.collection(`${event_base_path}/AttendeeList/Attendees`).doc(attendeeId);
            const attendeeDoc = await attendeeRef.get();

            if (!attendeeDoc.exists) {
                ret_val = ERRCODE.PAYLOAD_ERROR;
                return reject(ret_val);
            }

            const attendeeData = attendeeDoc.data();
            const registrationType = attendeeData?.RegistrationType?.RegistrationType?.toLowerCase();
            const registrationTypeEntityId = attendeeData?.RegistrationType?.RegistrationTypeEntityId ?? null;

            const isSponsorRep = registrationType === "sponsor";
            const qnaOwnerId = isSponsorRep ? registrationTypeEntityId : attendeeId;
            const entityType = isSponsorRep ? "sponsor" : "attendee";
            const updateBy = CurrentUid;

            const qnaDocRef = dbClient.doc(`${event_base_path}/${isSponsorRep ? 'SponsorList' : 'AttendeeList'}/${isSponsorRep ? 'Sponsors' : 'Attendees'}/${qnaOwnerId}`);

            // ❌ Prevent multiple sponsor reps from updating QnA unless explicitly allowed
            if (isSponsorRep) {
                const existingQnA = await qnaDocRef.collection("MeetingQnA").limit(1).get();
                if (!existingQnA.empty && !onDuplicateUpdate) {
                    return resolve({
                        status: -1,
                        message: "QnA already submitted by another representative of this sponsor. Updates not allowed."
                    });
                }
            }

            // ✅ Build Firestore and MySQL write batches
            const batch = dbClient.batch();
            const mysqlValues = [];

            for (const answer of payload.data.answers) {
                if (answer.questionId && answer.selectedValue && answer.questionLabel) {
                    const answerRef = qnaDocRef.collection("MeetingQnA").doc(answer.questionId);
                    batch.set(answerRef, {
                        selectedValue: answer.selectedValue,
                        questionLabel: answer.questionLabel,
                        updateBy,
                        updatedAt: new Date()
                    }, { merge: true });

                    mysqlValues.push([
                        iceId,
                        qnaOwnerId,
                        entityType,
                        answer.questionId,
                        answer.questionLabel,
                        answer.selectedValue,
                        updateBy
                    ]);
                }
            }

            await batch.commit();
            // console.log(mysqlValues)
            try {
                if (mysqlValues.length > 0) {
                    // const sql = `
                    // INSERT INTO qna (
                    //     iceId, entityId, entityType, 
                    //     questionId, questionLabel, selectedValue, 
                    //     updateBy, insertDateTime, updateDateTime
                    // )
                    // VALUES ${mysqlValues.map(() => '(?, ?, ?, ?, ?, ?, ?, NOW(), NOW())').join(', ')}
                    // ON DUPLICATE KEY UPDATE 
                    //     selectedValue = VALUES(selectedValue), 
                    //     questionLabel = VALUES(questionLabel), 
                    //     updateBy = VALUES(updateBy), 
                    //     updateDateTime = NOW();
                    // `;

                    // const flattenedValues = mysqlValues.flat();
                    // await mysql.executeQuery(sql, flattenedValues);

                    const supabaseData = mysqlValues.map(values => ({
                        iceId: values[0],
                        entityId: values[1],
                        entityType: values[2],
                        questionId: values[3],
                        questionLabel: values[4],
                        selectedValue: values[5],
                        updateBy: values[6],
                        insertDateTime: new Date().toISOString(),
                        updateDateTime: new Date().toISOString()
                    }));
                    const { data, error } = await supabase
                        .from('qna')
                        .upsert(supabaseData, {
                            onConflict: 'iceId,entityId,entityType,questionId', // Specify your unique constraint columns
                            ignoreDuplicates: false // This ensures updates happen on conflict
                        });
                    if (error) {
                        console.log('Error upserting:', error);
                    }
                }

            } catch (err) {
                console.error("Error in SUPABASE:", err);
            }
            return resolve({ status: 0, message: "Answers saved successfully" });
        } catch (err) {
            console.error("Error in set_meeting_qna:", err);
            return reject({ status: -1, err });
        }
    });
}

function _allow_send_email(email) {
    email = email || "";
    email = email.toLowerCase().trim();
    if (!email) {
        return false;
    }
    const allowedDomains = ["@webspiders.com"];
    const allowedEmails = [
        "Robprevett@outlook.com",
        "shane.g.white88@gmail.com",
        "laurenmayyoga@gmail.com",
        "julia.corner@gmail.com",
        "rob@robprevett.com",
        "lauren138@hotmail.com",
        "julia.c@internetretailing.net",
        "natalie@internetretailing.net",
        "rob@retailx.net",
        "shane@internetretailing.net",
        "laurenc@retailx.net",
        "quill@internetretailing.net",
        "marvin@internetretailing.net",
        "hannah@internetretailing.net",
        "zoe@internetretailing.net",
        "alaina@internetretailing.net",
        "catherine@internetretailing.net",
        "eden@retailx.net",
        "sara@internetretailing.net"
    ];
    for (let i = 0; i < allowedDomains.length; i++) {
        const domain = allowedDomains[i];
        if (email.endsWith(domain.toLowerCase())) {
            return true;
        }
    }
    if (allowedEmails.includes(email)) {
        return true;
    }
    return false;
}

async function draft_attendees(payload) {
    let ret_val = { status: -1 }
    try {
        const iceId = `${payload.key.instanceId}_${payload.key.clientId}${payload.key.eventId}`;
        const eventBasePath = `/${iceId}`;
        console.log("eventBasePath", eventBasePath);
        let attendeeId = (payload.data.attendeeId) ? payload.data.attendeeId : payload.auth.data.UserId;//"99936195";//
        //console.log(attendeeId)
        //attendeeId = attendeeId.toString().trim();
        console.log(attendeeId)
        let attendeeDoc = await dbClient.collection(eventBasePath).doc("AttendeeList").collection("Attendees").doc(attendeeId).get();
        let attendeeData = attendeeDoc.data();
        //console.log("attendeeData", attendeeData)
        let registrationTypeEntityId = attendeeData.RegistrationType?.RegistrationTypeEntityId || null;
        if (payload.data.sponsorId) {
            registrationTypeEntityId = payload.data.sponsorId
        }

        console.log("registrationTypeEntityId", registrationTypeEntityId)
        if (!attendeeDoc.exists || !registrationTypeEntityId) {
            ret_val.err = new Error("Attendee not found or registrationTypeEntityId is missing");
            throw ret_val
        }



        let excludedAttendeeIds = new Set();
        // Fetch attendees with less than 2 confirmed meetings from MySQL
        // const confirmedMeetings = await mysql.executeQuery(
        //     `SELECT inviteeId, COUNT(*) AS count FROM meeting 
        //      WHERE requestStatus = 'confirmed'
        //      GROUP BY inviteeId`, []);

        const confirmedMeetingsR = await supabase.rpc('get_confirmed_meetings', {});
        const confirmedMeetings = confirmedMeetingsR.data;
        if (confirmedMeetings && confirmedMeetings.length > 0) {
            excludedAttendeeIds = new Set(
                confirmedMeetings.filter(m => m.count >= 2).map(m => m.inviteeId)
            );
        }

        // let draftedMeetings = await mysql.executeQuery(
        //     `SELECT inviteeId, requestorId FROM meeting 
        //      WHERE requestorTypeEntityId = ? AND requestStatus = 'draft'`,
        //     [registrationTypeEntityId],
        //     true
        // );
        const draftedMeetingsR = await supabase.from('meeting')
            .select('"inviteeId", "requestorId"')
            .eq('requestorTypeEntityId', registrationTypeEntityId)
            .eq('requestStatus', 'draft');
        // Always ensure it's an array
        let draftedMeetings = draftedMeetingsR.data;
        draftedMeetings = Array.isArray(draftedMeetings) ? draftedMeetings : [];

        console.log("draftedMeetings", draftedMeetings)


        let filteredDraftedMeetings = []
        if (draftedMeetings && draftedMeetings.length > 0) {
            filteredDraftedMeetings = draftedMeetings.filter(
                attendee => !excludedAttendeeIds.has(attendee.inviteeId)
            );
        }
        //draftedMeetings looks like { inviteeId: '1324000', requestorId: '99934194' }

        console.log("filteredDraftedMeetings", filteredDraftedMeetings)

        let draftedAttendees = [];
        if (filteredDraftedMeetings && filteredDraftedMeetings.length > 0) {
            // Fetch attendee details from Firestore
            for (const { inviteeId, requestorId } of filteredDraftedMeetings) {
                let attendeeSnapshot = await dbClient.collection(eventBasePath).doc("AttendeeList").collection("Attendees").doc(inviteeId).get();
                let repSnapshot = await dbClient.collection(eventBasePath).doc("AttendeeList").collection("Attendees").doc(requestorId).get();
                if (attendeeSnapshot.exists && repSnapshot.exists) {
                    let attendeeData = attendeeSnapshot.data();
                    console.log("attendeeData=====", attendeeData)
                    let repData = repSnapshot.data();
                    console.log("repData====", repData)
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
        return ret_val;
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

        const registrationTypeEntityId = attendeeData?.RegistrationType?.RegistrationTypeEntityId ?? '';
        const sponsorId = (payload.data.sponsorId) ? payload.data.sponsorId : registrationTypeEntityId;
        // const sponsorId = payload.data.sponsorId;


        if (!sponsorId) {
            ret_val.err = "Sponsor not found";
            return ret_val;
        }



        const limit = payload.data.limit || 2000;
        const page = payload.data.page || 1;
        const filterObj = payload.data.filter || { operator: "AND", data: [] };
        const sort = payload.data.sort || { field: "Name", order: "ASC" };

        // const [confirmedMeetings, requestedMeetings, draftedMeetings] = await Promise.all([
        //     mysql.executeQuery(`
        //         (SELECT inviteeId as attendeeId, COUNT(*) AS count FROM meeting 
        //         WHERE requestStatus = 'confirmed' AND  requestorTypeEntityId=?
        //         GROUP BY inviteeId) UNION (SELECT requestorId as attendeeId, COUNT(*) AS count FROM meeting 
        //         WHERE requestStatus = 'confirmed' AND  inviteeTypeEntityId=?
        //         GROUP BY requestorId)`, [sponsorId, sponsorId]),
        //     mysql.executeQuery(
        //         `SELECT inviteeId FROM meeting 
        //          WHERE requestorTypeEntityId = ? AND requestStatus IN ('requested','cancelled')`,
        //         [sponsorId]
        //     ),
        //     mysql.executeQuery(
        //         `SELECT inviteeId FROM meeting 
        //          WHERE requestorTypeEntityId = ? AND requestStatus = 'draft'`,
        //         [sponsorId]
        //     )
        // ]);
        const [confirmedMeetingsR, requestedMeetingsR, draftedMeetingsR] = await Promise.all([
            supabase.rpc('get_confirmed_meetings', { sponsor_id: sponsorId }),
            supabase.from('meeting')
                .select('inviteeId')
                .eq('requestorTypeEntityId', sponsorId)
                .in('requestStatus', ['requested', 'cancelled']),
            supabase.from('meeting')
                .select('inviteeId')
                .eq('requestorTypeEntityId', sponsorId)
                .eq('requestStatus', 'draft')
        ]);
        // console.log(confirmedMeetingsR)
        const confirmedMeetings = confirmedMeetingsR.data;
        const requestedMeetings = requestedMeetingsR.data;
        const draftedMeetings = draftedMeetingsR.data;
        // console.log(requestedMeetings)
        // console.log(draftedMeetings)
        // console.log(confirmedMeetings)
        const excludedIds = new Set(
            (confirmedMeetings || [])
                .filter(row => row.count >= 2)
                .map(row => row.attendeeId)
        );

        const requestedIds = new Set((requestedMeetings || []).map(row => row.inviteeId));
        const draftedIds = new Set((draftedMeetings || []).map(row => row.inviteeId));

        const cacheKey = `${iceId}/AttendeeList`;
        //let clearCache = payload.data?.clearCache === true; // ensure it's explicitly true
        let clearCache = payload.data?.clearCache !== false; // ensure it implicitly true
        if (clearCache) {
            cm.removeFromCache({ cacheKey: cacheKey });
        }

        const docs = await _get_from_cache_or_db(cacheKey, async () => {
            const snapshot = await dbClient.collection(eventBasePath).doc("AttendeeList").collection("Attendees")
                .where("RegistrationType.RegistrationType", "==", "Attendee")
                .get();
            return !snapshot.empty ? snapshot.docs : [];
        });

        let attendees = [];

        console.log("docs.length", docs.length)

        if (docs && docs.length > 0) {
            docs.forEach(doc => {
                const data = doc.data;
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
                    Slots: data.Slots,
                    isDrafted: false
                }
                const id = data.AttendeeId;

                // const matches = filterObj.data.map(f => _wild_card_match(data[f.field], f.value));
                // const matchesSearch = filterObj.operator === "OR" ? matches.some(Boolean) : matches.every(Boolean);

                const isConfirmed = excludedIds.has(id);
                const isRequested = requestedIds.has(id);
                const isDrafted = draftedIds.has(id);

                // if (!isConfirmed && !isRequested && matchesSearch) {
                if (!isConfirmed && !isRequested) {
                    if (isDrafted) {
                        formattedData.isDrafted = isDrafted
                    }
                    attendees.push(formattedData);
                }
            });
            console.log("attendees.length", attendees.length)

            if (attendees.length > 0) {
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

                ret_val.data = {
                    attendees: paginatedAttendees,
                    total: attendees.length,
                    page,
                    totalPages: Math.ceil(attendees.length / limit),
                }
            }
        }
        ret_val.status = 0;
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

        const cacheKey = `${iceId}/SpeakerList`;

        //let clearCache = payload.data?.clearCache === true; // ensure it's explicitly true
        let clearCache = payload.data?.clearCache !== false; // ensure it implicitly true
        if (clearCache) {
            cm.removeFromCache({ cacheKey: cacheKey });
        }

        const docs = await _get_from_cache_or_db(cacheKey, async () => {
            const snapshot = await dbClient.collection(eventBasePath).doc("SpeakerList").collection("Speakers").get();
            return !snapshot.empty ? snapshot.docs : [];
        });

        // Step 2: Process speaker details
        if (docs && docs.length > 0) {
            ret_val.data = docs.map(doc => {
                const data = doc.data;
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
        let cacheKey = `${iceId}/SponsorList/all`;
        let showAll = payload.data?.showAll === true; // ensure it's explicitly true
        let sponsorsRef = dbClient.collection(`${eventBasePath}/SponsorList/Sponsors`);
        if (!showAll) {
            cacheKey = `${iceId}/SponsorList/prefered`;
            sponsorsRef = sponsorsRef.where('isMeetingEnabled', '==', true);
        } else {
            sponsorsRef = sponsorsRef.where('IsPublished', '==', true);
        }
        //let clearCache = payload.data?.clearCache === true; // ensure it's explicitly true
        let clearCache = payload.data?.clearCache !== false; // ensure it implicitly true
        if (clearCache) {
            cm.removeFromCache({ cacheKey: cacheKey });
        }
        const sponsorSnap = await sponsorsRef.get();
        console.log("sponsorSnap", sponsorSnap.docs.length)
        let docs = await _get_from_cache_or_db(cacheKey, async () => {
            let snapshot = await sponsorsRef.get();
            return !snapshot.empty ? snapshot.docs : [];
        });

        //let snapshot = await sponsorsRef.get();
        //console.log("sponsorSnap", snapshot.docs.length)

        let allSponsors = {};
        if (docs && docs.length > 0) {
            docs.forEach(doc => {
                let data = doc.data;
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
                        Logo: data.Logo || '',
                        isMeetingEnabled: data.isMeetingEnabled || '',
                        MappedContacts: data.MappedContacts || [],
                    },
                    confirmedMeetingCount: 0,
                    confirmedMeetings: []
                };
            });
        }
        //console.log("allSponsors", Object.keys(allSponsors).length)

        if (allSponsors && Object.keys(allSponsors).length > 0) {

            // // Step 2: Fetch confirmed meetings
            // const confirmedMeetings = await mysql.executeQuery(
            //     `SELECT * FROM meeting WHERE iceId = ? AND requestStatus = 'confirmed'`,
            //     [iceId]
            // );

            // if (!confirmedMeetings && !confirmedMeetings.length) {
            //     ret_val.status = 0;
            //     ret_val.data = Object.values(allSponsors);
            //     return ret_val;
            // }

            // Step 3: Organize meetings by sponsor
            const sponsorMeetingMap = {}; // sponsorId: [meeting, ...]
            const attendeeIds = new Set();

            // for (const m of confirmedMeetings) {
            //     const isRequestorSponsor = m.requestorType.toLowerCase() === "sponsor";
            //     const sponsorId = isRequestorSponsor ? m.requestorTypeEntityId : m.inviteeTypeEntityId;
            //     const attendeeId = isRequestorSponsor ? m.inviteeId : m.requestorId;
            //     attendeeIds.add(attendeeId);

            //     if (!sponsorMeetingMap[sponsorId]) sponsorMeetingMap[sponsorId] = [];
            //     sponsorMeetingMap[sponsorId].push({
            //         meetingId: m.id,
            //         slot: m.slot,
            //         timestamp: m.timestamp,
            //         attendeeId
            //     });
            // }

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
    const ret_val = { status: -1, summary: {}, meetings: [] };

    const { instanceId, clientId, eventId } = payload.key || {};
    const sponsorId = payload.data?.sponsorId;

    if (!instanceId || !clientId || !eventId) {
        throw new Error("Missing instanceId, clientId, or eventId in payload.key");
    }

    const iceId = `${instanceId}_${clientId}${eventId}`;
    const eventPath = `/${iceId}`;

    // Fetch all meetings for this event except drafts
    // const meetings = await mysql.executeQuery(
    //     `SELECT * FROM meeting WHERE iceId = ? AND requestStatus != 'draft'`,
    //     [iceId]
    // );

    const { data: meetings, error } = await supabase
        .from('meeting')
        .select('*')
        .eq('"iceId"', iceId)
        .not('"requestStatus"', 'eq', 'draft');

    if (error) console.log(error);

    // Step 1: Build global requested count for each non-sponsor attendee
    const attendeeRequestedCount = {};
    for (const meeting of meetings) {
        const isRequestorSponsor = meeting.requestorType === 'sponsor';
        const isInviteeSponsor = meeting.inviteeType === 'sponsor';

        if (meeting.requestStatus === 'requested') {
            if (isRequestorSponsor && !isInviteeSponsor) {
                attendeeRequestedCount[meeting.inviteeId] = (attendeeRequestedCount[meeting.inviteeId] || 0) + 1;
            } else if (isInviteeSponsor && !isRequestorSponsor) {
                attendeeRequestedCount[meeting.requestorId] = (attendeeRequestedCount[meeting.requestorId] || 0) + 1;
            }
        }
    }
    //console.log(attendeeRequestedCount)

    // Step 2: Filter sponsor-related meetings
    const sponsorMeetings = meetings.filter(m =>
        m.requestorTypeEntityId === sponsorId || m.inviteeTypeEntityId === sponsorId
    );

    // Step 3: Collect unique user IDs to fetch
    const userIdsToFetch = new Set();
    const userIdTypeMap = {};

    for (const m of sponsorMeetings) {
        userIdsToFetch.add(m.requestorId);
        userIdsToFetch.add(m.inviteeId);
        userIdTypeMap[m.requestorId] = m.requestorType;
        userIdTypeMap[m.inviteeId] = m.inviteeType;
    }

    // Step 4: Batch fetch user data
    const userCache = {};
    const attendeeRefs = [];
    const repRefs = [];

    for (const uid of userIdsToFetch) {
        const type = userIdTypeMap[uid];
        const path = type === 'sponsorRep'
            ? `${eventPath}/SponsorList/Representatives`
            : `${eventPath}/AttendeeList/Attendees`;
        const ref = dbClient.collection(path).doc(uid);
        if (type === 'sponsorRep') {
            repRefs.push(ref);
        } else {
            attendeeRefs.push(ref);
        }
    }

    const [repSnaps, attendeeSnaps] = await Promise.all([
        Promise.all(repRefs.map(r => r.get())),
        Promise.all(attendeeRefs.map(r => r.get()))
    ]);

    [...repSnaps, ...attendeeSnaps].forEach(snap => {
        if (snap.exists) {
            const data = snap.data();
            userCache[snap.id] = {
                Id: snap.id,
                Name: data.Name || '',
                Designation: data.Designation || '',
                Company: data.Company || '',
                Email: data.Email || '',
                Phone: data.Phone || '',
                Attending: data.Attending === true ? 'true' : data.Attending === false ? 'false' : ''
            };
        }
    });

    // Step 5: Build final meeting output in parallel
    ret_val.meetings = sponsorMeetings.map(meeting => {
        const isRequestorSponsor = meeting.requestorTypeEntityId === sponsorId;
        const nonSponsorId = isRequestorSponsor ? meeting.inviteeId : meeting.requestorId;

        return {
            MeetingId: meeting.meetingCode,
            Status: meeting.requestStatus,
            RequestorInfo: userCache[meeting.requestorId] || {},
            InviteeInfo: userCache[meeting.inviteeId] || {},
            NonSponsorRequestedCount: attendeeRequestedCount[nonSponsorId] || 0,
            CreateDateTime: meeting.requestDateTime,
            LastUpdatedDateTime: meeting.requestUpdateDateTime,
        };
    });

    // Sort by NonSponsorRequestedCount ascending
    ret_val.meetings.sort((a, b) => a.NonSponsorRequestedCount - b.NonSponsorRequestedCount);

    // Step 6: Fetch sponsor name once
    const sponsorDoc = await dbClient
        .collection(`${eventPath}/SponsorList/Sponsors`)
        .doc(sponsorId.toString())
        .get();

    ret_val.summary = {
        sponsorId,
        sponsorName: sponsorDoc.exists ? sponsorDoc.data().Name : '',
        totalMeetings: ret_val.meetings.length
    };

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

    // // Fetch all meetings involving the attendee
    // const meetings = await mysql.executeQuery(
    //     `SELECT * FROM meeting 
    //      WHERE iceId = ? AND (requestorId = ? OR inviteeId = ?)`,
    //     [iceId, attendeeId, attendeeId]
    // );

    const { data: meetings, error } = await supabase
        .from('meeting')
        .select('*')
        .eq('"iceId"', iceId)
        .or(`"requestorId".eq.${attendeeId},"inviteeId".eq.${attendeeId}`);

    if (error) console.log(error);

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
            MeetingId: meeting.id,
            MeetingCode: meeting.meetingCode,
            RequestorId: meeting.requestorId,
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

async function meeting_config(payload) {
    let ret_val = { status: -1 }
    try {
        const { instanceId, clientId, eventId } = payload.key;
        const iceId = `${instanceId}_${clientId}${eventId}`;
        const eventPath = `/${iceId}`;
        const configCollection = `${eventPath}/MeetingList/Settings`;
        const configDoc = await dbClient.collection(configCollection).doc('Config').get();
        ret_val.status = 0
        ret_val.data = configDoc.exists ? configDoc.data() : null
    }
    catch (err) {
        console.log(err);
    }
    return ret_val;
}

async function get_meetings(payload) {
    try {
        const { instanceId, clientId, eventId } = payload.key;
        const iceId = `${instanceId}_${clientId}${eventId}`;
        const current_uid = payload.data?.UserId || payload.auth?.data?.UserId;
        const eventPath = `${instanceId}_${clientId}${eventId}`;
        const attendeesPath = `${eventPath}/AttendeeList/Attendees`;

        // Determine if the current user is a sponsor rep or attendee
        const attendeeDoc = await dbClient.collection(attendeesPath).doc(current_uid).get();
        if (!attendeeDoc.exists) return { status: -1, message: "User not found." };

        const attendeeData = attendeeDoc.data();
        const registrationType = attendeeData?.RegistrationType?.RegistrationType?.toLowerCase();
        const registrationTypeEntityId = attendeeData?.RegistrationType?.RegistrationTypeEntityId;
        console.log("registrationType", registrationType)
        console.log("registrationTypeEntityId", registrationTypeEntityId)
        console.log("current_uid", current_uid)
        // let meetingQuery = ``;
        // let replacements = [iceId];

        // if (registrationType === 'sponsor') {
        //     // Sponsor Rep - Fetch their individual meetings and their sponsor's meetings
        //     meetingQuery = `
        //     SELECT *,
        //       CASE 
        //         WHEN (requestorTypeEntityId = ? AND requestStatus = 'requested') THEN 'FYI'
        //         WHEN (inviteeTypeEntityId = ? AND requestStatus = 'requested') THEN 'FYA'
        //         ELSE ''
        //       END AS meeting_state
        //     FROM e2m_o2o_prd.meeting
        //     WHERE iceId = ? AND (requestorTypeEntityId = ? OR inviteeTypeEntityId = ?)
        //     ORDER BY requestMeetingSlot
        //   `;
        //     replacements = [
        //         registrationTypeEntityId, registrationTypeEntityId,
        //         iceId,
        //         registrationTypeEntityId, registrationTypeEntityId
        //     ];
        // } else {
        //     // Attendee - Fetch only meetings where they are involved
        //     meetingQuery = `
        //     SELECT *,
        //       CASE 
        //         WHEN requestorId = ? AND requestStatus = 'requested' THEN 'FYI'
        //         WHEN inviteeId = ? AND requestStatus = 'requested' THEN 'FYA'
        //         ELSE ''
        //       END AS meeting_state
        //     FROM e2m_o2o_prd.meeting
        //     WHERE iceId = ? AND (requestorId = ? OR inviteeId = ?)
        //     ORDER BY requestMeetingSlot
        //   `;
        //     replacements = [current_uid, current_uid, iceId, current_uid, current_uid];
        // }

        // const rows = await mysql.executeQuery(meetingQuery, replacements, true);
        // console.log("registrationType", registrationType)
        let entity_id = registrationType === 'sponsor' ? registrationTypeEntityId : current_uid;
        // console.log("entity_id", entity_id)
        const [confirmedMeetings] = await Promise.all([
            supabase.rpc('get_meetings', {
                // user_type: registrationType,
                // registration_type_entity_id: registrationTypeEntityId,
                // user_id: current_uid,
                // ice_id: iceId
                user_type: registrationType,
                entity_id: registrationType === 'sponsor' ? registrationTypeEntityId : current_uid,
                ice_id: iceId,
            })
        ]);

        if (confirmedMeetings.error) {
            console.error('Error:', confirmedMeetings.error);
        }
        let rows = confirmedMeetings.data || [];
        const results = await Promise.all(rows.map(async (row) => {
            // console.log('row:', row);
            const [requestorDoc, inviteeDoc] = await Promise.all([
                dbClient.collection(attendeesPath).doc(row.requestorId).get(),
                dbClient.collection(attendeesPath).doc(row.inviteeId).get(),
            ]);

            const requestor = requestorDoc.exists ? requestorDoc.data() : {};
            const invitee = inviteeDoc.exists ? inviteeDoc.data() : {};

            const result = {
                MeetingId: row.meetingCode,
                Status: row.requestStatus,
                Requestor: {
                    AttendeeId: row.requestorId,
                    Name: requestor.Name,
                    Company: requestor.Company,
                    Designation: requestor.Designation,
                    Phone: requestor.Phone
                },
                Invitee: {
                    AttendeeId: row.inviteeId,
                    Name: invitee.Name,
                    Company: invitee.Company,
                    Designation: invitee.Designation,
                    Phone: invitee.Phone
                },
                CreateDateTime: row.requestUpdateDateTime,
                Remarks: row.remarks || null,
                MeetingState: row.meeting_state,
                Attended: row.attended,
            };

            if (row.requestStatus?.toLowerCase() === 'confirmed') {
                result.Slot = row.requestMeetingSlot;
            }

            return result;
        }));

        return { status: 0, meetings: results };
    } catch (err) {
        console.log(err);
        return { status: -1, error: err };
    }
}

async function get_meetings_wsql(payload) {
    // const { instanceId, clientId, eventId } = payload.key;
    // const iceId = `${instanceId}_${clientId}${eventId}`;
    // const current_uid = payload.data?.UserId || payload.auth?.data?.UserId;

    // const predefinedEvents = [
    //     "E1743163129042",
    //     "E1743163021441",
    //     "E1743163201304",
    //     "E1743162911584",
    //     "E1743162842566",
    //     "E1743162762857"
    // ];

    // const eventIds = eventId === "E1742214690559"
    //     ? [iceId]
    //     : predefinedEvents.map(e => `${instanceId}_${clientId}${e}`);

    // let attendeeData = null;
    // let registrationType = null;
    // let registrationTypeEntityId = null;

    // if (eventId === "E1742214690559") {
    //     const attendeesPath = `${instanceId}_${clientId}${eventId}/AttendeeList/Attendees`;
    //     const attendeeDoc = await dbClient.collection(attendeesPath).doc(current_uid).get();
    //     if (!attendeeDoc.exists) return { status: -1, message: "User not found." };
    //     attendeeData = attendeeDoc.data();
    //     registrationType = attendeeData?.RegistrationType?.RegistrationType?.toLowerCase();
    //     registrationTypeEntityId = attendeeData?.RegistrationType?.RegistrationTypeEntityId || null;
    // } else {
    //     const attendeePromises = predefinedEvents.map(async (evt) => {
    //         const eventPath = `${instanceId}_${clientId}${evt}/AttendeeList/Attendees`;
    //         const attendeeDoc = await dbClient.collection(eventPath).doc(current_uid).get();
    //         if (attendeeDoc.exists) {
    //             const data = attendeeDoc.data();
    //             const type = data?.RegistrationType?.RegistrationType?.toLowerCase();
    //             const typeEntityId = data?.RegistrationType?.RegistrationTypeEntityId || null;

    //             if (type === 'sponsor') {
    //                 const sponsorPath = `${instanceId}_${clientId}${evt}/SponsorList/Sponsors/${typeEntityId}`;
    //                 const sponsorDoc = await dbClient.doc(sponsorPath).get();
    //                 if (sponsorDoc.exists && sponsorDoc.data()?.IsPublished) {
    //                     return { data, registrationType: type, registrationTypeEntityId: sponsorDoc.id };
    //                 }
    //             } else {
    //                 return { data, registrationType: type, registrationTypeEntityId: typeEntityId };
    //             }
    //         }
    //         return null;
    //     });

    //     const attendeeResults = await Promise.all(attendeePromises);
    //     const validAttendee = attendeeResults.find(res => res !== null);

    //     if (!validAttendee) return { status: -1, message: "User not found in any of the predefined events." };

    //     attendeeData = validAttendee.data;
    //     registrationType = validAttendee.registrationType;
    //     registrationTypeEntityId = validAttendee.registrationTypeEntityId;

    //     if (registrationType === 'sponsor' && !registrationTypeEntityId) {
    //         return { status: -1, message: "Sponsor is not published." };
    //     }
    // }


    // let meetingQuery = ``;
    // let replacements = [];

    // if (registrationType === 'sponsor') {
    //     meetingQuery = `
    //     SELECT *,
    //       CASE 
    //         WHEN (requestorTypeEntityId = ? AND requestStatus = 'requested') THEN 'FYI'
    //         WHEN (inviteeTypeEntityId = ? AND requestStatus = 'requested') THEN 'FYA'
    //         ELSE ''
    //       END AS meeting_state
    //     FROM e2m_o2o_prd.meeting
    //     WHERE iceId IN (${eventIds.map(() => "?").join(",")}) 
    //       AND (requestorTypeEntityId = ? OR inviteeTypeEntityId = ?)
    //     ORDER BY requestMeetingSlot`;

    //     replacements = [
    //         registrationTypeEntityId, registrationTypeEntityId,
    //         ...eventIds,
    //         registrationTypeEntityId, registrationTypeEntityId
    //     ];

    // } else {
    //     meetingQuery = `
    //     SELECT *,
    //       CASE 
    //         WHEN requestorId = ? AND requestStatus = 'requested' THEN 'FYI'
    //         WHEN inviteeId = ? AND requestStatus = 'requested' THEN 'FYA'
    //         ELSE ''
    //       END AS meeting_state
    //     FROM e2m_o2o_prd.meeting
    //     WHERE iceId IN (${eventIds.map(() => "?").join(",")}) 
    //       AND (requestorId = ? OR inviteeId = ?)
    //     ORDER BY requestMeetingSlot`;

    //     replacements = [
    //         current_uid, current_uid,
    //         ...eventIds,
    //         current_uid, current_uid
    //     ];
    // }

    // const rows = await mysql.executeQuery(meetingQuery, replacements, true);

    // const results = await Promise.all(rows.map(async (row) => {
    //     const [requestorDoc, inviteeDoc] = await Promise.all([
    //         dbClient.collection(`${eventIds[0]}/AttendeeList/Attendees`).doc(row.requestorId).get(),
    //         dbClient.collection(`${eventIds[0]}/AttendeeList/Attendees`).doc(row.inviteeId).get(),
    //     ]);

    //     const requestor = requestorDoc.exists ? requestorDoc.data() : {};
    //     const invitee = inviteeDoc.exists ? inviteeDoc.data() : {};

    //     const result = {
    //         MeetingId: row.meetingCode,
    //         Status: row.requestStatus,
    //         Requestor: {
    //             AttendeeId: row.requestorId,
    //             Name: requestor.Name,
    //             Company: requestor.Company,
    //             Designation: requestor.Designation,
    //             Phone: requestor.Phone
    //         },
    //         Invitee: {
    //             AttendeeId: row.inviteeId,
    //             Name: invitee.Name,
    //             Company: invitee.Company,
    //             Designation: invitee.Designation,
    //             Phone: invitee.Phone
    //         },
    //         CreateDateTime: row.requestUpdateDateTime,
    //         Remarks: row.remarks || null,
    //         MeetingState: row.meeting_state
    //     };

    //     if (row.requestStatus?.toLowerCase() === 'confirmed') {
    //         result.Slot = row.requestMeetingSlot;
    //     }

    //     return result;
    // }));

    // return { status: 0, meetings: results };
}

async function get_meetings_firebase(payload) {
    const { instanceId, clientId, eventId } = payload.key;
    const iceId = `${instanceId}_${clientId}${eventId}`;
    const current_uid = payload.data?.UserId || payload.auth?.data?.UserId;

    let predefinedEvents = [];

    if (eventId === "E1742214690559") {
        predefinedEvents = [
            "E1742214690559"
        ];
    } else if (eventId === "E1747670277539") {
        predefinedEvents = [
            "E1747670277539"
        ];
    } else {
        predefinedEvents = [
            "E1743163129042",
            "E1743163021441",
            "E1743163201304",
            "E1743162911584",
            "E1743162842566",
            "E1743162762857"
        ];
    }

    let attendeeDataArray = [];

    // if (eventId === "E1742214690559") {
    //     const attendeesPath = `${instanceId}_${clientId}${eventId}/AttendeeList/Attendees`;
    //     const attendeeDoc = await dbClient.collection(attendeesPath).doc(current_uid).get();
    //     if (!attendeeDoc.exists) return { status: -1, message: "User not found." };
    //     attendeeDataArray = [attendeeDoc.data()];
    // } else if (eventId === "E1747670277539") {
    //     const attendeesPath = `${instanceId}_${clientId}${eventId}/AttendeeList/Attendees`;
    //     const attendeeDoc = await dbClient.collection(attendeesPath).doc(current_uid).get();
    //     if (!attendeeDoc.exists) return { status: -1, message: "User not found." };
    //     attendeeDataArray = [attendeeDoc.data()];
    // } else {
    const attendeePromises = predefinedEvents.map(async (evt) => {
        const eventPath = `${instanceId}_${clientId}${evt}/AttendeeList/Attendees`;
        const attendeeDoc = await dbClient.collection(eventPath).doc(current_uid).get();
        return attendeeDoc.exists ? attendeeDoc.data() : null;
    });

    const attendeeResults = await Promise.all(attendeePromises);
    attendeeDataArray = attendeeResults.filter(res => res !== null);
    if (attendeeDataArray.length === 0) return { status: -1, message: "User not found in any of the predefined events." };

    const mergedMeetingIds = new Set();

    for (const evt of predefinedEvents) {
        const eventPath = `${instanceId}_${clientId}${evt}/AttendeeList/Attendees`;
        const doc = await dbClient.collection(eventPath).doc(current_uid).get();
        if (doc.exists) {
            const meetings = doc.data()?.Meetings || [];
            meetings.forEach(meetingId => mergedMeetingIds.add(`${evt}:${meetingId}`));
        }
    }

    attendeeDataArray.forEach((attendee) => {
        attendee.Meetings = Array.from(mergedMeetingIds).map(item => {
            const [evt, meetingId] = item.split(":");
            return { meetingId, eventId: evt };
        });
    });
    // }

    const allMeetingDetails = Array.from(new Set(attendeeDataArray.flatMap(a => a.Meetings || [])));

    if (!allMeetingDetails.length) return { status: 0, meetings: [] };

    const uniqueMeetingDetails = Array.from(new Map(allMeetingDetails.map(m => [m.meetingId + m.eventId, m])).values());

    const meetingCache = new Map();

    const meetingPromises = uniqueMeetingDetails.map(async ({ meetingId, eventId }) => {
        if (!meetingId || !eventId) return null;

        const cacheKey = `${eventId}:${meetingId}`;
        if (meetingCache.has(cacheKey)) return meetingCache.get(cacheKey);

        const meetingPath = `${instanceId}_${clientId}${eventId}/MeetingList/Meetings/${meetingId}`;

        const meetingDoc = await dbClient.doc(meetingPath).get();
        if (meetingDoc.exists) {
            const data = meetingDoc.data();
            meetingCache.set(cacheKey, data);
            return data;
        }
        return null;
    });

    const meetings = (await Promise.all(meetingPromises)).filter(Boolean);

    const results = meetings.map((meeting) => ({
        MeetingId: meeting.MeetingId,
        Status: meeting.Status,
        Requestor: meeting.Requestor,
        Invitee: meeting.Invitee,
        CreateDateTime: meeting.CreateDateTime,
        Remarks: meeting.remarks || null,
        MeetingState: meeting.Status,
        Slot: meeting.Status?.toLowerCase() === 'confirmed' ? meeting.Slots[0] : null
    })).sort((a, b) => new Date(a.Slot) - new Date(b.Slot));

    return { status: 0, meetings: results };
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


    const sponsorId = data?.sponsorId;

    // Logic to determine which slots to fetch
    if (sponsorId) {
        // Get common slots between sponsor (related to requestor or invitee) and the other attendee
        const sponsorSlot = await _get_slots(eventPath, sponsorId);
        result.availableSlots = allSlots.filter(slot => !sponsorSlot.includes(slot));
    }
    result.status = 0;
    return result;
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
        const requestorId = payload.auth.data.UserId;//"1324000";// 
        const inviteeIds = payload.data.inviteeIds;

        const requestorRef = dbClient.collection(eventBasePath).doc("AttendeeList").collection("Attendees").doc(requestorId);
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

        // await mysql.executeQuery(
        //     `DELETE FROM meeting 
        //      WHERE requestorTypeEntityId = ? AND inviteeId IN (${placeholders}) AND requestStatus = 'draft'`,
        //     values
        // );
        console.log("registrationTypeEntityId", registrationTypeEntityId)
        console.log("placeholders", placeholders)
        const { data, error } = await supabase
            .from('meeting')
            .delete()
            .in('inviteeId', values)
            .eq('requestorTypeEntityId', registrationTypeEntityId)
            .eq('requestStatus', 'draft');
        return { status: 0, message: "Invitees removed from draft successfully" };
    } catch (error) {
        console.error("Error removing attendee(s) from draft:", error);
        return { status: -1, message: "Error removing attendee(s) from draft" };
    }
}



// =============================================================================
// ENHANCED SPONSOR REPRESENTATIVE SELECTION SYSTEM
// =============================================================================

// ⚠️ IMPORTANT: Add this global variable at the top of your meeting.js file
// Global slot reservation tracker for batch processing
const batchSlotReservations = new Map(); // Map<batchId, Set<slot>>

/*
SPONSOR REP SELECTION LOGIC:

When an ATTENDEE requests meetings with SPONSORS:

1. **Get All Sponsor Representatives**
   - Find all sponsor reps for the target sponsor entity
   - Filter out reps who already have meetings with the requestor

2. **Priority-Based Selection**
   Priority 1: Slot Availability
   - Check if sponsor rep has available time slots from payload
   - Skip reps who have no available payload slots
   
   Priority 2: Meeting Count  
   - Select rep with fewer confirmed meetings
   - Ensures fair distribution across representatives
   
   Priority 3: Random Selection
   - If meeting counts are identical, select randomly
   - Prevents bias in selection

3. **Smart Validation**
   - Validates sponsor meeting enablement
   - Checks for existing conflicts 
   - Ensures proper entity relationships

4. **Seamless Integration**
   - Selected rep becomes the meeting invitee
   - Original sponsor entity ID maintained for tracking
   - Proper database relationships preserved

EXAMPLE FLOW:
Attendee requests meeting with "TechCorp Sponsor" + slots ["10:00", "14:00"]

TechCorp Representatives:
- Rep A: 2 meetings, available at 10:00, 14:00 ✅ SELECTED (fewer meetings)
- Rep B: 3 meetings, available at 10:00, 14:00
- Rep C: 2 meetings, no availability in payload slots ❌ SKIPPED

Result: Meeting created with Rep A, tracked under TechCorp entity
*/

// =============================================================================
// ENHANCED REQUEST_MEETINGS FUNCTION - Replace your existing function
// =============================================================================

async function request_meetings(payload) {
    console.log(payload)
    const ret_val = { status: -1, created: [], skipped: [], cancelled: [] };
    let SPONSORID = null

    // Generate unique batch ID for this request to track slot reservations
    const batchId = `batch_${Date.now()}_${Math.random().toString(36).substring(7)}`;

    if (!payload?.key || !payload?.data?.RequestorId) {
        return { status: -1, message: "Invalid payload" };
    }
    const { instanceId, clientId, eventId } = payload.key;
    const iceId = `${instanceId}_${clientId}${eventId}`;
    const eventPath = `/${iceId}`;
    const requestorId = payload.data.RequestorId || payload.auth?.data?.UserId;
    let inviteeIds = payload.data?.InviteeIds;

    const eventInfoDoc = await dbClient.doc(`${eventPath}/EventInfo`).get();
    const eventInfo = eventInfoDoc.data();
    const WhoCanRequestMeeting = eventInfo?.WhoCanRequestMeeting;

    const now = new Date();
    let IsCreatedByAI = payload.data?.IsCreatedByAI || 0;

    // Fetch requestor data
    const requestorSnap = await dbClient.collection(`${eventPath}/AttendeeList/Attendees`).doc(requestorId.toString()).get();
    let requestor = requestorSnap.data();
    if (!requestor || !requestor.RegistrationType) {
        return { status: -1, message: "Invalid or missing requestor data" };
    }

    let RequestorIsSponsor = false;
    let registrationType = requestor.RegistrationType.RegistrationType.toLowerCase();
    let registrationTypeEntityId = requestor.RegistrationType.RegistrationTypeEntityId ?? null;

    if (registrationType === "sponsor") {
        RequestorIsSponsor = true;
    }

    // Get meeting limits from config
    const MAX_CONFIRM_REQUESTOR = RequestorIsSponsor ? (eventInfo.SPONSOR_MAX_CONFIRM_REQUEST || 0) : (eventInfo.ATTENDEE_MAX_CONFIRM_REQUEST || 0);
    const MAX_CONFIRM_INVITEE = RequestorIsSponsor ? (eventInfo.ATTENDEE_MAX_CONFIRM_REQUEST || 0) : (eventInfo.SPONSOR_MAX_CONFIRM_REQUEST || 0);

    console.log(`Limits - Requestor: ${MAX_CONFIRM_REQUESTOR}, Invitee: ${MAX_CONFIRM_INVITEE}`);

    // Permission validation
    const sponsorCanRequest = WhoCanRequestMeeting.includes('sponsor');
    const attendeeCanRequest = WhoCanRequestMeeting.includes('attendee');
    const aiCanRequest = WhoCanRequestMeeting.includes('ai') && IsCreatedByAI === 1;

    if (IsCreatedByAI === 1 && aiCanRequest) {
        // Skip further role checks for AI-created requests
    } else if (RequestorIsSponsor) {
        if (!sponsorCanRequest) {
            return { status: -1, message: "Sponsors are not allowed to request meetings." };
        }
    } else {
        if (!attendeeCanRequest) {
            return { status: -1, message: "Attendees are not allowed to request meetings." };
        }
    }

    // Enhanced validation: Check current meeting counts for all participants
    const allParticipantIds = [requestorId, ...(inviteeIds || [])];
    const participantMeetingCounts = await _getParticipantMeetingCounts(iceId, allParticipantIds);
    const existingMeetingPairs = await _getExistingMeetingPairs(iceId, IsCreatedByAI, requestorId, inviteeIds || []);

    // Check requestor's current meeting count
    const requestorCurrentMeetings = participantMeetingCounts[requestorId] || 0;
    if (requestorCurrentMeetings >= MAX_CONFIRM_REQUESTOR) {
        return {
            status: -1,
            message: `Requestor already has maximum confirmed meetings (${requestorCurrentMeetings}/${MAX_CONFIRM_REQUESTOR})`
        };
    }

    // Calculate how many new meetings requestor can have
    const requestorAvailableSlots = MAX_CONFIRM_REQUESTOR - requestorCurrentMeetings;
    console.log(`Requestor can have ${requestorAvailableSlots} more meetings`);

    console.log(`Requestor (${requestorId}) is a ${registrationType} with entity ID ${registrationTypeEntityId}`);
    console.log(`inviteeIds:`, inviteeIds);

    if (!inviteeIds) inviteeIds = [];

    // Handle different invitee ID sources based on requestor type
    if (RequestorIsSponsor && inviteeIds.length === 0) {

        // Sponsor requesting meetings - get draft attendees
        const { data: draftMeetings, error } = await supabase
            .from('meeting')
            .select('"inviteeId"')
            .eq('"iceId"', iceId)
            .eq('"requestorType"', 'sponsor')
            .eq('"requestorTypeEntityId"', registrationTypeEntityId)
            .eq('"requestStatus"', 'draft');

        if (!draftMeetings || draftMeetings?.length === 0) {
            throw { status: -1, message: "No draft attendees found for sponsor." };
        }
        if (draftMeetings && draftMeetings.length > 0) {
            inviteeIds = draftMeetings.map(row => row.inviteeId);
        }
        payload.data.InviteeIds = inviteeIds;
        console.log(`Sponsor ${registrationTypeEntityId} requesting meetings with ${inviteeIds.length} attendees`);
    } else if (!RequestorIsSponsor && inviteeIds.length > 0) {
        // Attendee requesting meetings with sponsors - inviteeIds should be sponsor entity IDs
        console.log(`Attendee ${requestorId} requesting meetings with ${inviteeIds.length} sponsors`);

    }

    // Enhanced invitee validation with meeting limits and existing meeting checks
    let validInvitees = [];

    for (const inviteeId of inviteeIds) {
        const validationResult = await _validateMeetingEligibility(
            iceId,
            requestorId,
            inviteeId,
            RequestorIsSponsor,
            registrationTypeEntityId,
            MAX_CONFIRM_INVITEE,
            participantMeetingCounts,
            existingMeetingPairs,
            eventPath,
            payload.data.Slots || []  // Pass payload slots for sponsor rep selection
        );

        if (validationResult.isValid) {
            // Handle sponsor rep selection for attendee requestors
            if (!RequestorIsSponsor && validationResult.selectedRepId) {
                console.log(`Selected sponsor rep ${validationResult.selectedRepId} for sponsor ${validationResult.originalSponsorId}`);
                validInvitees.push({
                    ...validationResult.inviteeData,
                    originalSponsorId: validationResult.originalSponsorId,
                    isSelectedRep: true
                });
            } else {
                requestor = validationResult.requestorData;
                validInvitees.push(validationResult.inviteeData);
            }
        } else {
            ret_val.skipped.push({
                inviteeId: inviteeId,
                reason: validationResult.reason
            });
        }
    }

    console.log(`Valid invitees: ${validInvitees.length}, Skipped: ${ret_val.skipped.length}`);

    // Limit valid invitees to requestor's available slots
    if (validInvitees.length > requestorAvailableSlots) {
        const excessInvitees = validInvitees.splice(requestorAvailableSlots);
        excessInvitees.forEach(invitee => {
            ret_val.skipped.push({
                inviteeId: invitee.AttendeeId,
                reason: `Requestor meeting limit reached (${MAX_CONFIRM_REQUESTOR})`
            });
        });
    }

    if (validInvitees.length === 0) {
        return {
            status: 0,
            message: "No valid invitees available for meetings",
            created: ret_val.created,
            skipped: ret_val.skipped
        };
    }

    // Step 2: Create meetings with enhanced validation, slot assignment, and batch management
    const meetingPromises = validInvitees.map(async (invitee, index) => {
        // Determine optimal slot for this specific pairing
        let assignedSlot = null;

        if (payload.data.Slots && payload.data.Slots.length > 0) {
            // Get sponsor ID for slot selection
            let targetSponsorId = null;
            let attendeeId = null;

            if (RequestorIsSponsor) {
                targetSponsorId = registrationTypeEntityId;
                attendeeId = invitee.AttendeeId;
            } else {
                // For attendee requestor, use the sponsor entity ID (original or selected rep's)
                targetSponsorId = invitee.originalSponsorId || invitee.RegistrationType?.RegistrationTypeEntityId;
                attendeeId = requestor.AttendeeId;
            }

            console.log(`Slot selection for meeting ${index + 1}: sponsor=${targetSponsorId}, attendee=${attendeeId}, batch=${batchId}`);

            if (targetSponsorId) {
                try {
                    // CRITICAL: Use enhanced slot selection with batch tracking
                    // This prevents multiple meetings from getting the same slot
                    assignedSlot = await _selectOptimalSlot(
                        iceId,
                        targetSponsorId,
                        payload.data.Slots,
                        attendeeId, // Pass the correct attendee ID
                        batchId
                    );
                    console.log(`✅ Assigned slot ${assignedSlot} to meeting ${index + 1} (${requestor.AttendeeId} ↔ ${invitee.AttendeeId}) for batch ${batchId}`);
                } catch (error) {
                    console.warn(`❌ Failed to assign slot for meeting ${index + 1}:`, error.message);
                    // Continue without assigned slot - will be handled in pubsub
                }
            } else {
                console.warn(`❌ No target sponsor ID found for meeting ${index + 1}`);
            }
        } else {
            console.log(`No payload slots provided for meeting ${index + 1}`);
        }

        const meetingData = {
            Requestor: {
                AttendeeId: requestor.AttendeeId,
                Name: requestor.Name,
                Email: requestor.Email,
                Company: requestor.Company,
                Designation: requestor.Designation,
                Phone: requestor.Phone
            },
            Invitee: {
                AttendeeId: invitee.AttendeeId,
                Name: invitee.Name,
                Email: invitee.Email,
                Company: invitee.Company,
                Designation: invitee.Designation,
                Phone: invitee.Phone
            },
            Slots: assignedSlot ? [assignedSlot] : [], // Assign the optimal slot immediately
            Status: 'Requested',
            CreateDateTime: now,
            // Additional metadata for tracking
            AssignedSlot: assignedSlot,
            SponsorEntityId: RequestorIsSponsor ? registrationTypeEntityId : (invitee.originalSponsorId || invitee.RegistrationType?.RegistrationTypeEntityId),
            BatchId: batchId
        };

        try {
            let meetingId = null;
            if (invitee.preBookedMeeting) {
                meetingId = invitee.preBookedMeeting.meeting_id;
                let meetingRef = dbClient.collection(`${eventPath}/MeetingList/Meetings`).doc(meetingId);
                await meetingRef.set({
                    MeetingId: invitee.preBookedMeeting.meeting_id,
                    Requestor: meetingData.Requestor,
                    Invitee: meetingData.Invitee,
                    Slots: meetingData.Slots,
                    Status: meetingData.Status,
                    AssignedSlot: meetingData.AssignedSlot,
                    SponsorEntityId: meetingData.SponsorEntityId,
                    BatchId: meetingData.BatchId
                }, { merge: true });
            }
            else {
                let meetingRef = await dbClient.collection(`${eventPath}/MeetingList/Meetings`).add(meetingData);
                meetingId = meetingRef.id;
                await meetingRef.set({ MeetingId: meetingId }, { merge: true });
                console.log(`Creating meeting ${meetingId} with batchId: ${batchId}`);
            }



            // Update meeting table with proper validation and assigned slot
            if (!RequestorIsSponsor) {
                let inviteeType = invitee.RegistrationType.RegistrationType.toLowerCase();
                let inviteeTypeEntityId = invitee.RegistrationType.RegistrationTypeEntityId ?? null;

                // If this is a selected sponsor rep, use the original sponsor entity ID
                if (invitee.isSelectedRep && invitee.originalSponsorId) {
                    inviteeTypeEntityId = invitee.originalSponsorId;
                    console.log(`Using original sponsor entity ID: ${inviteeTypeEntityId} for selected rep: ${invitee.AttendeeId}`);
                }

                const meetingRecord = {
                    "meetingCode": meetingId,
                    "iceId": iceId,
                    "requestorId": requestor.AttendeeId,
                    "requestorType": 'attendee',
                    "requestorTypeEntityId": '',
                    "inviteeId": invitee.AttendeeId,
                    "inviteeType": inviteeType,
                    "inviteeTypeEntityId": inviteeTypeEntityId,
                    "requestStatus": 'requested',
                    "requestUpdateDateTime": now,
                    "isCreatedByAI": IsCreatedByAI,
                    "requestMeetingSlot": assignedSlot,
                    "batchId": batchId
                };

                console.log(`Inserting meeting record with batchId: ${batchId}`, meetingRecord);

                let supaRes; // Use 'let' since you're reassigning it
                if (invitee.preBookedMeeting) {
                    supaRes = await supabase
                        .from('meeting')
                        .update(meetingRecord)
                        .match({ iceId: meetingRecord.iceId, meetingCode: meetingRecord.meetingCode });
                } else {
                    supaRes = await supabase
                        .from('meeting')
                        .upsert(meetingRecord, {
                            onConflict: 'iceId,requestorId,inviteeId', // Use composite key
                            ignoreDuplicates: false
                        });
                }
                if (supaRes.error) {
                    console.log("create meeting supabase error:", supaRes.error); // Fixed: use supaRes.error
                } else {
                    console.log("Meeting created successfully with batchId:", batchId);
                }

                SPONSORID = inviteeTypeEntityId;
            }
            else {
                const meetingRecord = {
                    "meetingCode": meetingId,
                    "iceId": iceId,
                    "requestorId": requestor.AttendeeId,
                    "requestorType": registrationType,
                    "requestorTypeEntityId": registrationTypeEntityId,
                    "inviteeId": invitee.AttendeeId,
                    "inviteeType": 'attendee',
                    "inviteeTypeEntityId": '',
                    "requestStatus": 'requested',
                    "sendEmail": 1,
                    "requestUpdateDateTime": now,
                    "isCreatedByAI": IsCreatedByAI,
                    "requestMeetingSlot": assignedSlot,
                    "batchId": batchId
                };

                console.log(`Inserting sponsor meeting record with batchId: ${batchId}`, meetingRecord);

                let supaRes;
                if (invitee.preBookedMeeting) {
                    supaRes = await supabase
                        .from('meeting')
                        .update(meetingRecord)
                        .match({ iceId: meetingRecord.iceId, meetingCode: meetingRecord.meetingCode });
                } else {
                    supaRes = await supabase
                        .from('meeting')
                        .upsert(meetingRecord, {
                            onConflict: 'iceId,requestorId,inviteeId', // Use composite key
                            ignoreDuplicates: false
                        });
                }

                if (supaRes.error) {
                    console.log("create meeting supabase error:", error);
                } else {
                    console.log("Sponsor meeting created successfully with batchId:", batchId);
                }
                SPONSORID = registrationTypeEntityId
            }

            const meetingDoc = await dbClient.collection(`${eventPath}/MeetingList/Meetings`).doc(meetingId).get();
            return {
                success: true,
                meetingData: meetingDoc.data(),
                sponsorId: SPONSORID,
                batchId: batchId,
                inviteeId: invitee.AttendeeId,
                assignedSlot: assignedSlot // Pass the assigned slot info
            };
        } catch (err) {
            console.error(`Failed to create meeting with ${invitee.AttendeeId}`, err);
            return { success: false, attendeeId: invitee.AttendeeId };
        }
    });

    const creationResults = await Promise.allSettled(meetingPromises);
    const publishPromises = [];

    for (const result of creationResults) {
        if (result.status === "fulfilled") {
            const res = result.value;
            if (res.success) {
                let topicName = 'rm-request-meeting';
                let pubsubPayload = {
                    Payload: {
                        ...payload,
                        data: {
                            ...payload.data,
                            batchId: res.batchId // Pass batch ID to pubsub
                        }
                    },
                    Meeting: res.meetingData,
                    SponsorId: res.sponsorId
                };
                pubsubPayload.Meeting.Timezone = eventInfo.TimeZone;
                if (!RequestorIsSponsor || IsCreatedByAI) {
                    pubsubPayload.confirm_meeting = true;
                }

                let payloadBuffer = Buffer.from(JSON.stringify(pubsubPayload));
                const publishPromise = pubSubClient.topic(topicName).publishMessage({
                    data: payloadBuffer,
                    attributes: { source: 'request-meeting', batchId: res.batchId }
                });

                publishPromises.push(publishPromise);
                ret_val.created.push(res.meetingData.MeetingId);
            } else {
                let remarks = "Failed to create meeting";
                const { data, error } = await supabase
                    .from('meeting')
                    .update({
                        "requestStatus": 'skipped',
                        "requestUpdateDateTime": now,
                        "remarks": remarks
                    })
                    .match({
                        "iceId": iceId,
                        "requestorId": requestorId,
                        "inviteeId": res.attendeeId,
                        "requestStatus": 'requested'
                    });
                ret_val.skipped.push({ inviteeId: res.attendeeId, remarks: remarks });
            }
        }
    }

    // Await all pubsub publish calls in parallel
    await Promise.all(publishPromises);

    // Clean up batch reservations after all meetings are processed
    setTimeout(() => {
        _clearBatchReservations(batchId);
    }, 5000); // Clear after 5 seconds to allow for processing completion

    console.log(`Meeting request summary:`, {
        created: ret_val.created.length,
        skipped: ret_val.skipped.length,
        batchId: batchId,
        slotsAssigned: ret_val.created.length,
        totalSlotRequests: validInvitees.length
    });

    ret_val.status = 0;
    return ret_val;
}

// =============================================================================
// ENHANCED PUBSUB_REQUEST_MEETING FUNCTION - Replace your existing function
// =============================================================================

async function pubsub_request_meeting(pubsubPayload) {
    let ret_val = { status: -1 }
    try {
        console.log("pubsub_request_meeting payload", pubsubPayload)

        let payload = pubsubPayload.Payload;
        let Meeting = pubsubPayload.Meeting;
        let Requestor = Meeting.Requestor;
        let Invitee = Meeting.Invitee;
        let SponsorId = pubsubPayload.SponsorId
        let confirm_meeting_flag = pubsubPayload.confirm_meeting ? pubsubPayload.confirm_meeting : false

        const instance_base_path = `/${payload.key.instanceId}`;
        const iceId = `${payload.key.instanceId}_${payload.key.clientId}${payload.key.eventId}`;
        const event_base_path = `/${iceId}`;
        const meeting_doc_path = `${event_base_path}/MeetingList/Meetings/${Meeting.MeetingId}`;
        const tasks = [];

        const configDoc = await dbClient.collection(event_base_path).doc("MeetingList").collection("Settings").doc("Config").get();
        let configData = configDoc.exists ? configDoc.data() : [];

        console.log("confirm_meeting_flag", confirm_meeting_flag)

        if (confirm_meeting_flag) {
            let selectedSlot = null;

            // Check if meeting already has an assigned slot
            if (Meeting.AssignedSlot) {
                selectedSlot = Meeting.AssignedSlot;
                console.log(`Using pre-assigned slot: ${selectedSlot} for meeting ${Meeting.MeetingId}`);

                await confirm_meeting({
                    key: payload.key,
                    data: { MeetingId: Meeting.MeetingId, Slot: selectedSlot },
                    auth: payload.auth
                });

                ret_val.status = 0;
                ret_val.message = `Meeting ${Meeting.MeetingId} confirmed on pre-assigned slot ${selectedSlot} and notifications sent.`;
            }
            else if (payload.data.Slots && payload.data.Slots.length > 0) {
                // Fallback: Enhanced slot selection logic with batch tracking (if no pre-assigned slot)
                const batchId = payload.data.batchId || `batch_${Date.now()}_${Math.random()}`;
                console.log(`No pre-assigned slot found. Processing meeting ${Meeting.MeetingId} with batch ${batchId}`);

                selectedSlot = await _selectOptimalSlot(
                    iceId,
                    SponsorId,
                    payload.data.Slots,
                    Requestor.AttendeeId,
                    batchId
                );

                console.log(`Selected slot ${selectedSlot} for meeting ${Meeting.MeetingId}`);

                await confirm_meeting({
                    key: payload.key,
                    data: { MeetingId: Meeting.MeetingId, Slot: selectedSlot },
                    auth: payload.auth
                });

                ret_val.status = 0;
                ret_val.message = `Meeting ${Meeting.MeetingId} confirmed on slot ${selectedSlot} and notifications sent.`;
            } else {
                // Fallback to existing logic when no specific slots provided
                console.log("No specific slots provided, using fallback logic");

                const { allSlots, confirmedSlotsMap: sponsorConfirmed } = await _confirmed_meetings_slots_by_sponsors(iceId);
                const { confirmedSlotsMap: attendeeConfirmed, preferredSlotsMap } = await _confirmed_meetings_slots_by_attendees(iceId);

                const sponsorUsed = sponsorConfirmed[SponsorId] || new Set();
                const attendeePreferred = new Set(preferredSlotsMap[Requestor.AttendeeId] || []);
                const attendeeUsed = attendeeConfirmed[Requestor.AttendeeId] || new Set();

                // Calculate sponsor available slots
                const sponsorAvailable = new Set(allSlots.filter(slot => !sponsorUsed.has(slot)));

                // Find a matching slot (preferred by attendee AND available for sponsor)
                let matchedSlot = null;

                if (attendeePreferred.size > 0) {
                    // Try to match with attendee preferred slots
                    for (const slot of allSlots) {
                        if (sponsorAvailable.has(slot) && attendeePreferred.has(slot) && !attendeeUsed.has(slot)) {
                            matchedSlot = slot;
                            break;
                        }
                    }
                } else {
                    // No preferred slots — pick the first sponsor-available slot not used by attendee
                    for (const slot of allSlots) {
                        if (sponsorAvailable.has(slot) && !attendeeUsed.has(slot)) {
                            matchedSlot = slot;
                            break;
                        }
                    }
                }

                if (!matchedSlot) {
                    ret_val.status = 1;
                    ret_val.message = `No common available slot found between sponsor and attendee.`;
                } else {
                    await confirm_meeting({
                        key: payload.key,
                        data: { MeetingId: Meeting.MeetingId, Slot: matchedSlot },
                        auth: payload.auth
                    });
                    ret_val.status = 0;
                    ret_val.message = `Meeting ${Meeting.MeetingId} confirmed on slot ${matchedSlot} and notifications sent.`;
                }
            }
        }
        else {
            // Email sending logic when not auto-confirming
            console.log("Sending email notifications for meeting", Meeting.MeetingId);

            const { data, error } = await supabase
                .from('meeting')
                .update({ "sendEmail": 1 })
                .in('"meetingCode"', [Meeting.MeetingId]);

            if (error) {
                console.error("Error updating sendEmail flag:", error);
            }

            ret_val.status = 0
            ret_val.message = `Meeting ${Meeting.MeetingId} notifications sent`
        }
    } catch (err) {
        console.error("Error in pubsub_request_meeting:", err);
        ret_val.message = err.message || "Unknown error";
    }
    return ret_val
}

// =============================================================================
// VALIDATION HELPER FUNCTIONS - Add these new functions
// =============================================================================

/**
 * Get current meeting counts for all participants
 */
async function _getParticipantMeetingCounts(iceId, participantIds) {
    try {
        if (!participantIds || participantIds.length === 0) {
            return {};
        }

        const { data, error } = await supabase.rpc('get_participant_meeting_counts', {
            ice_id: iceId,
            participant_ids: participantIds
        });

        if (error) {
            console.error("Error getting participant meeting counts:", error);
            return {};
        }

        const counts = {};
        data.forEach(row => {
            counts[row.participant_id] = row.meeting_count;
        });

        // Initialize missing participants with 0
        participantIds.forEach(id => {
            if (!(id in counts)) {
                counts[id] = 0;
            }
        });

        return counts;
    } catch (error) {
        console.error("Error in _getParticipantMeetingCounts:", error);
        return {};
    }
}

/**
 * Check for existing meetings between requestor and invitees
 */
async function _getExistingMeetingPairs(iceId, IsCreatedByAI, requestorId, inviteeIds) {
    try {
        if (!inviteeIds || inviteeIds.length === 0) {
            return new Set();
        }

        const { data, error } = await supabase.rpc('get_existing_meeting_pairs', {
            ice_id: iceId,
            input_requestor_id: requestorId,
            invitee_ids: inviteeIds,
            is_ai_request: IsCreatedByAI
        });

        if (error) {
            console.error("Error getting existing meeting pairs:", error);
            return new Set();
        }

        const existingPairs = new Set();
        data.forEach(row => {
            const pair = `${row.requestor_id}-${row.invitee_id}`;
            const reversePair = `${row.invitee_id}-${row.requestor_id}`;
            existingPairs.add(pair);
            existingPairs.add(reversePair);
        });

        return existingPairs;
    } catch (error) {
        console.error("Error in _getExistingMeetingPairs:", error);
        return new Set();
    }
}

/**
 * Comprehensive validation for meeting eligibility with proper sponsor/attendee handling
 */
async function _validateMeetingEligibility(
    iceId,
    requestorId,
    inviteeId,
    requestorIsSponsor,
    requestorEntityId,
    maxConfirmInvitee,
    participantMeetingCounts,
    existingMeetingPairs,
    eventPath,
    payloadSlots = []
) {
    try {
        console.log(`Validating meeting: requestor=${requestorId} (sponsor: ${requestorIsSponsor}), invitee=${inviteeId}`);
        let ret_val = { isValid: false, reason: "Unknown error" };
        let preBookedMeeting = null;
        let sponsorEntityId = null;
        let pairKey1, pairKey2;
        // Check if meeting already exists between these participants
        if (requestorIsSponsor) {
            pairKey1 = `${requestorEntityId}-${inviteeId}`;
            pairKey2 = `${inviteeId}-${requestorEntityId}`;
        } else {
            let inviteeSnap = await dbClient.collection(`${eventPath}/AttendeeList/Attendees`).doc(inviteeId.toString()).get();
            let invitee = inviteeSnap.data();
            let inviteeEntityId = invitee.RegistrationType.RegistrationTypeEntityId ?? null;

            pairKey1 = `${requestorId}-${inviteeEntityId}`;
            pairKey2 = `${inviteeEntityId}-${requestorId}`;
        }

        if (existingMeetingPairs.has(pairKey1) || existingMeetingPairs.has(pairKey2)) {
            console.log("Meeting already exists between requestor and invitee");
            return {
                isValid: false,
                reason: "Meeting already exists between these participants"
            };
        }

        if (requestorIsSponsor) {

            sponsorEntityId = requestorEntityId;

            // Get all sponsor representatives for this sponsor entity
            let sponsorReps = await _getSponsorRepresentatives(iceId, requestorEntityId, eventPath);


            if (!sponsorReps || sponsorReps.length === 0) {
                return {
                    isValid: false,
                    reason: "No representatives found for this sponsor"
                };
            }

            console.log(`Found ${sponsorReps.length} representatives for sponsor ${requestorEntityId}`);

            // Select the best sponsor representative based on criteria
            const bestSponsorRep = await _selectBestSponsorRep(
                iceId,
                sponsorReps,
                inviteeId,
                payloadSlots,
                participantMeetingCounts,
                existingMeetingPairs
            );

            if (!bestSponsorRep.isValid) {
                return {
                    isValid: false,
                    reason: bestSponsorRep.reason
                };
            }

            console.log(`Selected sponsor rep: ${bestSponsorRep.selectedRep.AttendeeId} for sponsor entity ${requestorEntityId}`);




            // CASE 1: SPONSOR REQUESTING MEETING WITH ATTENDEE
            console.log(`Case 1: Sponsor ${requestorEntityId} requesting meeting with attendee ${inviteeId}`);

            // Fetch attendee data from Firestore
            const attendeeDoc = await dbClient.collection(`${eventPath}/AttendeeList/Attendees`).doc(inviteeId).get();

            if (!attendeeDoc.exists) {
                return {
                    isValid: false,
                    reason: "Attendee not found"
                };
            }

            const attendeeData = attendeeDoc.data();

            // Verify invitee is actually an attendee
            const attendeeRegistrationType = attendeeData.RegistrationType?.RegistrationType?.toLowerCase();
            if (attendeeRegistrationType !== 'attendee') {
                return {
                    isValid: false,
                    reason: "Sponsors can only request meetings with attendees"
                };
            }

            // Check attendee's current meeting count (inviteeId in this case is the attendee)
            const attendeeMeetingCount = participantMeetingCounts[inviteeId] || 0;
            if (attendeeMeetingCount >= maxConfirmInvitee) {
                return {
                    isValid: false,
                    reason: `Attendee has reached maximum meetings (${attendeeMeetingCount}/${maxConfirmInvitee})`
                };
            }

            // Check for sponsor-attendee specific conflicts
            const sponsorAttendeeConflict = await _checkSponsorAttendeeConflict(
                iceId,
                requestorEntityId,
                inviteeId
            );

            if (sponsorAttendeeConflict.hasConflict) {

                if (sponsorAttendeeConflict.existingMeetings && sponsorAttendeeConflict.existingMeetings.length > 0 && sponsorAttendeeConflict.existingMeetings[0].meeting_status == 'requested') {
                    preBookedMeeting = sponsorAttendeeConflict.existingMeetings[0];
                } else {
                    return {
                        isValid: false,
                        meeting: sponsorAttendeeConflict.existingMeetings,
                        reason: sponsorAttendeeConflict.reason
                    };
                }


            }

            ret_val.isValid = true;
            ret_val.requestorData = bestSponsorRep.selectedRep;
            ret_val.inviteeData = attendeeData
            ret_val.originalSponsorId = sponsorEntityId;
            ret_val.selectedRepId = bestSponsorRep.selectedRep.AttendeeId;
            ret_val.availableSlots = bestSponsorRep.availableSlots;
            if (preBookedMeeting) {
                ret_val.inviteeData.preBookedMeeting = preBookedMeeting;
            }

            return ret_val;

        } else {
            // CASE 2: ATTENDEE REQUESTING MEETING WITH SPONSOR
            console.log(`Case 2: Attendee ${requestorId} requesting meeting with sponsor ${inviteeId}`);

            // Fetch sponsor data from Firestore SponsorList
            console.log(`Sponsor path: ${eventPath}/SponsorList/Sponsors/${inviteeId}`);
            const sponsorDoc = await dbClient.doc(`${eventPath}/SponsorList/Sponsors/${inviteeId}`).get();

            if (!sponsorDoc.exists) {
                console.log(`Sponsor ${inviteeId} not found in Firestore`);
                return {
                    isValid: false,
                    reason: "Sponsor not found"
                };
            }

            const sponsorData = sponsorDoc.data();
            //console.log(`Sponsor data:`, sponsorData);

            // Check if sponsor has meeting enabled
            if (!sponsorData?.isMeetingEnabled) {
                console.log(`Sponsor ${inviteeId} has meetings disabled`);
                return {
                    isValid: false,
                    reason: "Meetings are disabled for this sponsor"
                };
            }

            // For sponsor meeting count, we need to count meetings where this sponsor entity is involved
            // This means checking participantMeetingCounts for the sponsor entity ID, not individual reps
            sponsorEntityId = inviteeId; // In this case, inviteeId IS the sponsor entity ID

            // Get sponsor meeting count - this should be based on sponsor entity, not individual reps
            const sponsorMeetingCount = await _getSponsorEntityMeetingCount(iceId, sponsorEntityId);

            if (sponsorMeetingCount >= maxConfirmInvitee) {
                return {
                    isValid: false,
                    reason: `Sponsor has reached maximum meetings (${sponsorMeetingCount}/${maxConfirmInvitee})`
                };
            }

            // Check for existing meetings between this attendee and sponsor entity
            const sponsorAttendeeConflict = await _checkSponsorAttendeeConflict(
                iceId,
                sponsorEntityId,
                requestorId
            );

            if (sponsorAttendeeConflict.hasConflict) {

                if (sponsorAttendeeConflict.existingMeetings && sponsorAttendeeConflict.existingMeetings.length > 0 && sponsorAttendeeConflict.existingMeetings[0].meeting_status == 'requested') {
                    preBookedMeeting = sponsorAttendeeConflict.existingMeetings[0];
                } else {
                    return {
                        isValid: false,
                        meeting: sponsorAttendeeConflict.existingMeetings,
                        reason: sponsorAttendeeConflict.reason
                    };
                }


            }

            // Get all sponsor representatives for this sponsor entity
            let sponsorReps = await _getSponsorRepresentatives(iceId, sponsorEntityId, eventPath);

            if (!sponsorReps || sponsorReps.length === 0) {
                return {
                    isValid: false,
                    reason: "No representatives found for this sponsor"
                };
            }

            console.log(`Found ${sponsorReps.length} representatives for sponsor ${sponsorEntityId}`);

            // Select the best sponsor representative based on criteria
            const bestSponsorRep = await _selectBestSponsorRep(
                iceId,
                sponsorReps,
                requestorId,
                payloadSlots,
                participantMeetingCounts,
                existingMeetingPairs
            );

            if (!bestSponsorRep.isValid) {
                return {
                    isValid: false,
                    reason: bestSponsorRep.reason
                };
            }

            console.log(`Selected sponsor rep: ${bestSponsorRep.selectedRep.AttendeeId} for sponsor entity ${sponsorEntityId}`);

            // Return the selected sponsor rep data with proper tracking
            ret_val.isValid = true;
            ret_val.inviteeData = bestSponsorRep.selectedRep;
            ret_val.originalSponsorId = sponsorEntityId;
            ret_val.selectedRepId = bestSponsorRep.selectedRep.AttendeeId;
            ret_val.availableSlots = bestSponsorRep.availableSlots;
            if (preBookedMeeting) {
                ret_val.inviteeData.preBookedMeeting = preBookedMeeting;
            }

            return ret_val;
        }

    } catch (error) {
        console.error("Error in _validateMeetingEligibility:", error);
        return {
            isValid: false,
            reason: "Validation error occurred"
        };
    }
}

/**
 * Get meeting count for a specific sponsor entity (not individual reps)
 */
async function _getSponsorEntityMeetingCount(iceId, sponsorEntityId) {
    try {
        const { data, error } = await supabase.rpc('get_sponsor_entity_meeting_count', {
            ice_id: iceId,
            sponsor_entity_id: sponsorEntityId
        });

        if (error) {
            console.error("Error getting sponsor entity meeting count:", error);
            return 0;
        }

        return data && data.length > 0 ? data[0].meeting_count : 0;
    } catch (error) {
        console.error("Error in _getSponsorEntityMeetingCount:", error);
        return 0;
    }
}

/**
 * Get all sponsor representatives for a sponsor entity
 */
async function _getSponsorRepresentatives(iceId, sponsorEntityId, eventPath) {
    try {
        console.log(`Getting sponsor representatives for entity: ${sponsorEntityId}`);

        const snapshot = await dbClient
            .collection(`${eventPath}/AttendeeList/Attendees`)
            .where("RegistrationType.RegistrationType", "==", "Sponsor")
            .where("RegistrationType.RegistrationTypeEntityId", "==", sponsorEntityId)
            .get();

        if (snapshot.empty) {
            console.log(`No representatives found for sponsor entity ${sponsorEntityId}`);
            return [];
        }

        const reps = snapshot.docs.map(doc => ({
            AttendeeId: doc.id,
            ...doc.data()
        }));

        console.log(`Found ${reps.length} representatives for sponsor entity ${sponsorEntityId}`);
        return reps;

    } catch (error) {
        console.error("Error getting sponsor representatives:", error);
        return [];
    }
}

/**
 * Select the best sponsor representative based on availability and meeting count
 */
async function _selectBestSponsorRep(
    iceId,
    sponsorReps,
    requestorId,
    payloadSlots,
    participantMeetingCounts,
    existingMeetingPairs
) {
    try {
        console.log(`Selecting best sponsor rep from ${sponsorReps.length} representatives`);
        console.log(`Payload slots: ${payloadSlots.join(', ')}`);

        // // Step 1: Filter out reps who already have meetings with the requestor
        // const availableReps = [];

        // for (const rep of sponsorReps) {
        //     const repId = rep.AttendeeId;
        //     const pairKey1 = `${requestorId}-${repId}`;
        //     const pairKey2 = `${repId}-${requestorId}`;

        //     // Skip if meeting already exists
        //     if (existingMeetingPairs.has(pairKey1) || existingMeetingPairs.has(pairKey2)) {
        //         console.log(`Skipping rep ${repId} - meeting already exists with requestor`);
        //         continue;
        //     }

        //     availableReps.push(rep);
        // }

        // if (availableReps.length === 0) {
        //     return {
        //         isValid: false,
        //         reason: "All sponsor representatives already have meetings with this attendee"
        //     };
        // }

        // Step 2: Get slot availability for each rep (if payload slots provided)
        let eligibleReps = [];

        if (payloadSlots && payloadSlots.length > 0) {
            console.log("Checking slot availability for payload slots");

            for (const rep of sponsorReps) {
                const repId = rep.AttendeeId;
                const repBookedSlots = await _getAttendeeBookedSlots(iceId, repId);

                // Check if rep has any payload slots available
                const availablePayloadSlots = payloadSlots.filter(slot => !repBookedSlots.has(slot));

                if (availablePayloadSlots.length > 0) {
                    eligibleReps.push({
                        ...rep,
                        availableSlots: availablePayloadSlots,
                        meetingCount: participantMeetingCounts[repId] || 0
                    });
                    console.log(`Rep ${repId} has ${availablePayloadSlots.length} available slots, ${participantMeetingCounts[repId] || 0} meetings`);
                } else {
                    console.log(`Rep ${repId} has no available slots from payload`);
                }
            }
        } else {
            // No specific slots provided, all available reps are eligible
            eligibleReps = sponsorReps.map(rep => ({
                ...rep,
                availableSlots: [],
                meetingCount: participantMeetingCounts[rep.AttendeeId] || 0
            }));
        }

        if (eligibleReps.length === 0) {
            return {
                isValid: false,
                reason: "No sponsor representatives available for the requested time slots"
            };
        }

        // Step 3: Sort by meeting count (ascending) and select the best one
        eligibleReps.sort((a, b) => {
            // First priority: fewer meetings
            if (a.meetingCount !== b.meetingCount) {
                return a.meetingCount - b.meetingCount;
            }

            // Second priority: more available slots (if payload slots provided)
            if (payloadSlots && payloadSlots.length > 0) {
                if (a.availableSlots.length !== b.availableSlots.length) {
                    return b.availableSlots.length - a.availableSlots.length;
                }
            }

            // Third priority: random selection for identical candidates
            return Math.random() - 0.5;
        });

        const selectedRep = eligibleReps[0];

        console.log(`Selected sponsor rep: ${selectedRep.AttendeeId} with ${selectedRep.meetingCount} meetings`);

        return {
            isValid: true,
            selectedRep: selectedRep,
            availableSlots: selectedRep.availableSlots
        };

    } catch (error) {
        console.error("Error selecting best sponsor rep:", error);
        return {
            isValid: false,
            reason: "Error occurred while selecting sponsor representative"
        };
    }
}

/**
 * Check for sponsor-attendee specific conflicts - ENHANCED VERSION
 */
async function _checkSponsorAttendeeConflict(iceId, sponsorEntityId, attendeeId) {
    try {
        console.log(`Checking sponsor-attendee conflict: sponsor entity ${sponsorEntityId} ↔ attendee ${attendeeId}`);

        // Check if there are any existing meetings between this sponsor entity and attendee
        // This includes meetings with ANY representative of the same sponsor organization
        const { data, error } = await supabase.rpc('check_sponsor_attendee_conflict', {
            ice_id: iceId,
            sponsor_entity_id: sponsorEntityId,
            attendee_id: attendeeId
        });

        if (error) {
            console.error("Error checking sponsor-attendee conflict:", error);
            return { hasConflict: false };
        }

        if (data && data.length > 0) {
            console.log(`Conflict found: Sponsor entity ${sponsorEntityId} already has ${data.length} meeting(s) with attendee ${attendeeId}`);
            console.log(`Existing meetings:`, data.map(row => `${row.meeting_id} (${row.meeting_status})`));

            return {
                hasConflict: true,
                reason: "Attendee already has a meeting with this sponsor organization",
                existingMeetings: data
            };
        }

        console.log(`No conflict: Sponsor entity ${sponsorEntityId} has no existing meetings with attendee ${attendeeId}`);
        return { hasConflict: false };

    } catch (error) {
        console.error("Error in _checkSponsorAttendeeConflict:", error);
        return { hasConflict: false };
    }
}

// =============================================================================
// ENHANCED SLOT SELECTION FUNCTIONS - Add these new functions
// =============================================================================

/**
 * Enhanced slot selection function that considers:
 * 1. Sponsors with fewer meetings get priority
 * 2. Slots with fewer sponsor representatives get priority  
 * 3. Current sponsor rep availability in the slot
 * 4. Batch-level slot conflict prevention
 * 5. Random selection as fallback
 */
async function _selectOptimalSlot(iceId, currentSponsorId, availableSlots, attendeeId, batchId = null) {
    try {
        console.log(`🎯 Selecting slot for sponsor ${currentSponsorId}, attendee ${attendeeId}, batch ${batchId}`);
        console.log(`📋 Available slots: ${availableSlots.join(', ')}`);

        // Initialize batch reservation tracking if batchId provided
        if (batchId && !batchSlotReservations.has(batchId)) {
            batchSlotReservations.set(batchId, new Set());
            console.log(`🆕 Initialized batch reservations for ${batchId}`);
        }

        // Get batch-reserved slots to avoid conflicts within the same batch
        const batchReservedSlots = batchId ? (batchSlotReservations.get(batchId) || new Set()) : new Set();
        console.log(`🚫 Batch reserved slots: [${Array.from(batchReservedSlots).join(', ')}]`);

        // Step 1: Get sponsor meeting counts to prioritize sponsors with fewer meetings
        const sponsorMeetingCounts = await _getSponsorMeetingCounts(iceId);
        const currentSponsorMeetingCount = sponsorMeetingCounts[currentSponsorId] || 0;
        console.log(`📊 Sponsor ${currentSponsorId} has ${currentSponsorMeetingCount} existing meetings`);

        // Step 2: Get slot-wise sponsor representative distribution
        const slotSponsorDistribution = await _getSlotSponsorDistribution(iceId, availableSlots);
        console.log(`📈 Slot distribution:`, slotSponsorDistribution);

        // Step 3: Get current sponsor's available slots (not already booked)
        const currentSponsorAvailableSlots = await _getSponsorAvailableSlots(iceId, currentSponsorId);
        console.log(`✅ Sponsor available slots: [${currentSponsorAvailableSlots.join(', ')}]`);

        // Step 4: Get attendee's unavailable slots (already booked)
        const attendeeBookedSlots = await _getAttendeeBookedSlots(iceId, attendeeId);
        console.log(`❌ Attendee booked slots: [${Array.from(attendeeBookedSlots).join(', ')}]`);

        // Step 5: Filter slots based on ALL availability criteria
        const viableSlots = availableSlots.filter(slot => {
            const sponsorAvailable = currentSponsorAvailableSlots.includes(slot);
            const attendeeAvailable = !attendeeBookedSlots.has(slot);
            const batchAvailable = !batchReservedSlots.has(slot);

            console.log(`🔍 Slot ${slot}: sponsor=${sponsorAvailable}, attendee=${attendeeAvailable}, batch=${batchAvailable}`);

            return sponsorAvailable && attendeeAvailable && batchAvailable;
        });

        console.log(`✅ Viable slots after filtering: [${viableSlots.join(', ')}]`);

        if (viableSlots.length === 0) {
            console.warn(`❌ No viable slots for sponsor ${currentSponsorId}, attendee ${attendeeId}`);

            // Try to find any available slot from the original list (allow batch overlap if necessary)
            const fallbackSlots = availableSlots.filter(slot =>
                currentSponsorAvailableSlots.includes(slot) &&
                !attendeeBookedSlots.has(slot)
                // Don't exclude batch reserved for fallback - allow overlap if necessary
            );

            if (fallbackSlots.length === 0) {
                throw new Error("No viable slots available for both sponsor and attendee");
            }

            console.log(`🔄 Using fallback slots: [${fallbackSlots.join(', ')}]`);
            const selectedSlot = fallbackSlots[Math.floor(Math.random() * fallbackSlots.length)];

            // Reserve the selected slot for this batch
            if (batchId) {
                batchSlotReservations.get(batchId).add(selectedSlot);
                console.log(`🔒 Reserved fallback slot ${selectedSlot} for batch ${batchId}`);
            }

            return selectedSlot;
        }

        // Step 6: Score each viable slot
        const scoredSlots = viableSlots.map(slot => {
            const sponsorCountInSlot = slotSponsorDistribution[slot] || 0;

            // Scoring logic:
            // - Lower sponsor count in slot = higher priority (inverse scoring)
            // - Add randomness for tie-breaking
            const score = {
                slot: slot,
                sponsorCountInSlot: sponsorCountInSlot,
                // Inverse scoring: fewer sponsors = higher score
                slotScore: 1000 - sponsorCountInSlot,
                randomFactor: Math.random() * 100
            };

            // Final score combines slot availability with randomness
            score.finalScore = score.slotScore + score.randomFactor;

            return score;
        });

        // Step 7: Sort by final score (highest first)
        scoredSlots.sort((a, b) => b.finalScore - a.finalScore);

        console.log(`🏆 Slot scoring results:`, scoredSlots.map(s => `${s.slot}(${s.sponsorCountInSlot} sponsors, score: ${s.finalScore.toFixed(1)})`));

        // Step 8: Return the best slot and reserve it
        const selectedSlot = scoredSlots[0].slot;

        // CRITICAL: Reserve the selected slot for this batch to prevent conflicts
        if (batchId) {
            batchSlotReservations.get(batchId).add(selectedSlot);
            console.log(`🔒 Reserved optimal slot ${selectedSlot} for batch ${batchId}`);
            console.log(`📋 Total batch reservations: [${Array.from(batchSlotReservations.get(batchId)).join(', ')}]`);
        }

        console.log(`🎯 SELECTED: ${selectedSlot} for sponsor ${currentSponsorId} (${currentSponsorMeetingCount} meetings, ${scoredSlots[0].sponsorCountInSlot} sponsors in slot)`);

        return selectedSlot;

    } catch (error) {
        console.error("❌ Error in _selectOptimalSlot:", error);
        // Fallback to random selection
        const randomIndex = Math.floor(Math.random() * availableSlots.length);
        const selectedSlot = availableSlots[randomIndex];

        // Reserve even the fallback slot
        if (batchId) {
            if (!batchSlotReservations.has(batchId)) {
                batchSlotReservations.set(batchId, new Set());
            }
            batchSlotReservations.get(batchId).add(selectedSlot);
            console.log(`🔒 Reserved random fallback slot ${selectedSlot} for batch ${batchId}`);
        }

        console.log(`🎲 Fallback: randomly selected slot ${selectedSlot}`);
        return selectedSlot;
    }
}

/**
 * Get meeting counts per sponsor to prioritize sponsors with fewer meetings
 */
async function _getSponsorMeetingCounts(iceId) {
    try {
        const { data, error } = await supabase.rpc('get_sponsor_meeting_counts', {
            ice_id: iceId
        });

        if (error) {
            console.error("Error getting sponsor meeting counts:", error);
            return {};
        }

        const sponsorCounts = {};
        data.forEach(row => {
            sponsorCounts[row.sponsor_id] = row.meeting_count;
        });

        return sponsorCounts;
    } catch (error) {
        console.error("Error in _getSponsorMeetingCounts:", error);
        return {};
    }
}

/**
 * Get the distribution of sponsor representatives across different slots
 */
async function _getSlotSponsorDistribution(iceId, availableSlots) {
    try {
        const { data, error } = await supabase.rpc('get_slot_sponsor_distribution', {
            ice_id: iceId,
            slot_list: availableSlots
        });

        if (error) {
            console.error("Error getting slot sponsor distribution:", error);
            return {};
        }

        const distribution = {};
        data.forEach(row => {
            distribution[row.slot] = row.sponsor_count;
        });

        // Initialize slots with 0 if not present
        availableSlots.forEach(slot => {
            if (!(slot in distribution)) {
                distribution[slot] = 0;
            }
        });

        return distribution;
    } catch (error) {
        console.error("Error in _getSlotSponsorDistribution:", error);
        // Fallback: return empty distribution
        const fallback = {};
        availableSlots.forEach(slot => {
            fallback[slot] = 0;
        });
        return fallback;
    }
}

/**
 * Get slots where the current sponsor is available (not already booked)
 */
async function _getSponsorAvailableSlots(iceId, sponsorId) {
    try {
        // Get all configured slots
        const configDoc = await dbClient.collection(`/${iceId}/MeetingList/Settings`).doc("Config").get();
        const allSlots = configDoc.exists ? (configDoc.data().Slots || []) : [];

        // Get sponsor's booked slots
        const { data, error } = await supabase.rpc('get_sponsor_booked_slots', {
            ice_id: iceId,
            sponsor_id: sponsorId
        });

        if (error) {
            console.error("Error getting sponsor booked slots:", error);
            return allSlots; // Fallback to all slots
        }

        const bookedSlots = new Set(data.map(row => row.slot));

        // Return available slots (all slots minus booked slots)
        return allSlots.filter(slot => !bookedSlots.has(slot));
    } catch (error) {
        console.error("Error in _getSponsorAvailableSlots:", error);
        return []; // Return empty array on error
    }
}

/**
 * Get attendee's already booked slots
 */
async function _getAttendeeBookedSlots(iceId, attendeeId) {
    try {
        const { data, error } = await supabase.rpc('get_attendee_booked_slots', {
            ice_id: iceId,
            attendee_id: attendeeId
        });

        if (error) {
            console.error("Error getting attendee booked slots:", error);
            return new Set();
        }

        return new Set(data.map(row => row.slot));
    } catch (error) {
        console.error("Error in _getAttendeeBookedSlots:", error);
        return new Set();
    }
}

/**
 * Clear batch reservations after processing is complete
 */
function _clearBatchReservations(batchId) {
    if (batchSlotReservations.has(batchId)) {
        batchSlotReservations.delete(batchId);
        console.log(`Cleared batch reservations for ${batchId}`);
    }
}






async function consolidated_send_email(payload) {
    const iceId = `${payload.key.instanceId}_${payload.key.clientId}${payload.key.eventId}`;
    const event_base_path = `/${iceId}`;
    const sponsorCache = {};
    let ret_val = { status: -1 }
    try {

        let eventRef = await dbClient.doc(`${event_base_path}/EventInfo`).get()
        let event = eventRef.data()

        // Step 1: Fetch all meetings with sendEmail = true
        //     const meetingResults = await mysql.executeQuery(`
        //     SELECT * FROM e2m_o2o_prd.meeting 
        //     WHERE iceId = ? AND sendEmail = 1 AND requestStatus = 'requested'
        // `, [iceId], true);

        const { data: meetingResults, error } = await supabase
            .from('meeting')
            .select('*')
            .eq('iceId', iceId)
            .eq('sendEmail', 1)
            .eq('requestStatus', 'requested');

        if (error) {
            console.error('Error fetching meetings:', error);
        } else {
            console.log('Meetings:', meetingResults);
        }

        const attendeeMap = {};  // Map of attendeeId -> meetings

        for (const meeting of meetingResults) {
            const inviteeId = meeting.inviteeId;
            if (!attendeeMap[inviteeId]) attendeeMap[inviteeId] = [];
            attendeeMap[inviteeId].push(meeting);
        }
        console.log(attendeeMap)

        const tasks = [];

        for (const [inviteeId, meetings] of Object.entries(attendeeMap)) {
            //console.log(inviteeId)
            const inviteeDoc = await dbClient.collection(`${event_base_path}/AttendeeList/Attendees`).doc(inviteeId).get();
            if (!inviteeDoc.exists) continue;
            const invitee = inviteeDoc.data();



            const eventdoc = await dbClient.doc(`${event_base_path}/EventInfo`).get()
            if (!eventdoc.exists) continue;
            const event = eventdoc.data();

            const linksList = await Promise.all(meetings.map(async meeting => {
                const requestorDoc = await dbClient.collection(`${event_base_path}/AttendeeList/Attendees`).doc(meeting.requestorId).get();
                if (requestorDoc.exists) {
                    const requestor = requestorDoc.data();
                    let sponsor = sponsorCache[meeting.requestorId];
                    if (!sponsor) {
                        const sponsorDoc = await dbClient.doc(`${event_base_path}/SponsorList/Sponsors/${meeting.requestorTypeEntityId}`).get();
                        sponsor = sponsorDoc.exists ? sponsorDoc.data() : {};
                        sponsorCache[meeting.requestorId] = sponsor;
                    }

                    let sponsorName = sponsor.Name || meeting.RequestorName || "Sponsor";
                    let url = `https://meet.e2m.live?mid=${meeting.meetingCode}&sid=${meeting.requestorTypeEntityId}&eid=${payload.key.eventId}&email=${invitee.Email}`;
                    //return `<li><a href="${url}">Meeting with ${requestor.Name} from (${sponsorName})</a></li>`;
                    let ret_val = { requestorName: requestor.Name, sponsorName: sponsorName, url: url }
                    return ret_val;
                }
            }));

            const emailBody = `<!DOCTYPE html>
                <html lang="en">
                <head>
                <meta charset="UTF-8" />
                <meta name="viewport" content="width=device-width, initial-scale=1.0"/>
                <style>
                    body {
                    font-family: 'Segoe UI', Tahoma, Geneva, Verdana, sans-serif;
                    margin: 0;
                    padding: 0;
                    background-color: #f2f4f6;
                    }
                    .email-wrapper {
                    max-width: 600px;
                    margin: auto;
                    background: #ffffff;
                    border-radius: 8px;
                    overflow: hidden;
                    box-shadow: 0 2px 6px rgba(0,0,0,0.05);
                    }
                    .header {
                    text-align: center;
                    background-color: #004085;
                    padding: 20px;
                    }
                    .header img {
                    max-height: 50px;
                    }
                    .content {
                    padding: 30px 20px;
                    }
                    h2 {
                    color: #004085;
                    font-size: 22px;
                    margin-bottom: 20px;
                    }
                    p {
                    font-size: 16px;
                    color: #333;
                    margin-bottom: 15px;
                    }
                    .meeting-card {
                    border: 1px solid #e1e4e8;
                    border-radius: 6px;
                    padding: 15px 20px;
                    margin-bottom: 15px;
                    background-color: #fafbfc;
                    }
                    .meeting-title {
                    font-weight: 600;
                    color: #222;
                    margin-bottom: 10px;
                    }
                    .button {
                    display: inline-block;
                    padding: 10px 16px;
                    font-size: 14px;
                    background-color: #007bff;
                    color: white;
                    border-radius: 5px;
                    text-decoration: none;
                    font-weight: 500;
                    }
                    .button:hover {
                    background-color: #0056b3;
                    }
                    .footer {
                    font-size: 12px;
                    color: #999;
                    text-align: center;
                    padding: 20px;
                    background-color: #f8f9fa;
                    }

                    @media (max-width: 600px) {
                    .content {
                        padding: 20px 15px;
                    }
                    }
                </style>
                </head>
                <body>
                <div class="email-wrapper">
                    <div class="header">
                    <!-- Replace this URL with your logo -->
                    <img src="${event.EventLogo}" alt="Company Logo">
                    </div>
                    <div class="content">
                    <h2>New Meeting Request</h2>
                    <p>Hi ${invitee.Name},</p>
                    <p>You have new meeting requests at <strong>${event.EventGroupName}</strong>. Please review them below:</p>

                    <!-- Repeat this block for each meeting -->
                    ${linksList.map(link => `
                    <div class="meeting-card">
                        <div class="meeting-title">Meeting with ${link.requestorName} from (${link.sponsorName})</div>
                        <a href="${link.url}" class="button">Accept Invitation</a>
                    </div>
                    `).join('')}

                    </div>
                    <div class="footer">
                    &copy; ${new Date().getFullYear()} Your Company Name. All rights reserved.
                    </div>
                </div>
                </body>
                </html>`;

            const EmailPayload = {
                from: {
                    email: "noreply@e2m.live",
                    name: "Meeting Platform"
                },
                to: {
                    name: invitee.Name,
                    email: invitee.Email
                },
                subject: "You have new meeting requests",
                html: emailBody
            };
            console.log(EmailPayload)

            if (_allow_send_email(invitee.Email)) {
                tasks.push(_send_email({}, EmailPayload));
            }

            // Step 3: Mark all these meetings' sendEmail = 0
            const meetingIds = meetings.map(m => `'${m.meetingCode}'`).join(',');
            tasks.push(
                // mysql.executeQuery(`
                //                     UPDATE e2m_o2o_prd.meeting 
                //                     SET sendEmail = 0 
                //                     WHERE meetingCode IN (${meetingIds})
                //                 `)
                supabase
                    .from('meeting')
                    .update({ sendEmail: 0 })
                    .in('meetingCode', meetingIds)
            );
        }

        await Promise.allSettled(tasks);
        ret_val.status = 0
    }
    catch (err) {
        console.log(err)
    }
    return ret_val

}

async function confirm_meeting(payload) {
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
        const eventInfoDoc = await dbClient.doc(`${event_base_path}/EventInfo`).get();
        const eventInfo = eventInfoDoc.data();
        let meetingRef = await dbClient.collection(event_base_path).doc("MeetingList").collection("Meetings").doc(payload.data.MeetingId).get()

        let Meeting = meetingRef.data();
        Meeting.Slots = [payload.data.Slot]
        const tasks = [];

        if (Meeting.Status === "Confirmed") {
            ret_val.err = new Error("Meeting already confirmed.");
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
        // if (Sponsor.Slots.length >= 10) {
        if (Sponsor.Slots.length >= (eventInfo.SPONSOR_MAX_CONFIRM_REQUEST || 0)) {
            ret_val.err = new Error("Sponsor slots are full.");
            reject(ret_val)
        }

        const conflictingSponsorSlot = Meeting.Slots.some(slot => Sponsor.Slots.includes(slot));
        if (conflictingSponsorSlot) {
            ret_val.err = new Error("Sponsor slot already booked.");
            reject(ret_val)
        }

        // // Check attendee slot constraints
        // if (attendee.Slots.length >= 2) {
        //     ret_val.err = new Error("Attendee slots are full.");
        //     reject(ret_val)
        // }

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
                .set({ SponsorId: sponsorId, LastUpdatedDateTime: new Date(), Slots: Meeting.Slots, Status: "Confirmed" }, { merge: true })
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
            // mysql.executeQuery('UPDATE meeting SET requestStatus = ?, requestMeetingSlot = ? WHERE meetingCode = ?', ['confirmed', payload.data.Slot, payload.data.MeetingId], true)
            supabase
                .from('meeting')
                .update({
                    "requestStatus": 'confirmed',
                    "requestMeetingSlot": payload.data.Slot
                })
                .eq('"meetingCode"', payload.data.MeetingId)
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
                    pubsubPayload.Meeting.Timezone = eventInfo.TimeZone;
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
                        attributes: { source: 'confirm-meeting' }
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
async function pubsub_confirm_meeting(pubsubPayload) {
    let ret_val = { status: -1 }
    try {
        // const stopSending = true;
        // if (stopSending) {
        //     ret_val = { status: -35, msg: "Meeting confirmation disabled" }
        //     return ret_val;
        // }
        console.log("pubsubPayload", pubsubPayload)

        let payload = pubsubPayload.Payload;
        let Requestor = pubsubPayload.Requestor;
        let Invitee = pubsubPayload.Invitee;
        let Meeting = pubsubPayload.Meeting;
        if (payload && payload.data && payload.data.Timezone) {
            Meeting.Timezone = payload.data.Timezone;
        }
        console.log("Meeting.Timezone: ", Meeting.Timezone)
        const instance_base_path = `/${payload.key.instanceId}`;
        const iceId = `${payload.key.instanceId}_${payload.key.clientId}${payload.key.eventId}`;
        const event_base_path = `/${iceId}`;
        const meeting_doc_path = `${event_base_path}/MeetingList/Meetings/${payload.data.MeetingId}`;
        const tasks = [];

        const configDoc = await dbClient.collection(event_base_path).doc("MeetingList").collection("Settings").doc("Config").get();
        let configData = configDoc.exists ? configDoc.data() : {};
        const meetingDoc = await dbClient.doc(meeting_doc_path).get();
        let meetingData = meetingDoc.exists ? meetingDoc.data() : {};

        let requestor = Meeting.Requestor;
        let invitee = Meeting.Invitee;

        let sponsorData = {};
        if (meetingData.SponsorId) {
            let sponsorDoc = await dbClient.doc(`${event_base_path}/SponsorList/Sponsors/${meetingData.SponsorId}`).get();
            if (!sponsorDoc.exists) throw new Error("Sponsor not found");
            sponsorData = sponsorDoc.data();

            for (let i = 0; i < sponsorData.MappedContacts.length; i++) {
                if (sponsorData.MappedContacts[i].AttendeeId == meetingData.Requestor.AttendeeId) {
                    requestor = meetingData.Requestor;
                    invitee = meetingData.Invitee;
                    break;
                } else if (sponsorData.MappedContacts[i].AttendeeId == meetingData.Invitee.AttendeeId) {
                    requestor = meetingData.Invitee;
                    invitee = meetingData.Requestor;
                    break;
                }
            }
        }


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
        const MeetingUrl = `https://meet.e2m.live?mid=${Meeting.MeetingId}&email=${Invitee.Email}`;


        // Create calendar event details
        const calendarEvent = {
            title: `Meeting with ${Invitee.Name}`,
            description: Meeting.Message || '',
            start: moment(Meeting.Slots[0]).utc().format("YYYY-MM-DD H:mm:ss ZZ"),
            duration: [configData.Duration, "minute"]
        };

        // Common placeholders for the email and calendar links
        const commonPlaceholders = {
            StartDateTime: startDateTime + ' ' + (Event.TimeZone || "UTC"),
            Duration: configData.Duration || "",
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
            let attendeeNameAndCompany = invitee.Name;
            if (invitee.Company) {
                attendeeNameAndCompany += " (" + invitee.Company + ")";
            }
            let sponsorRepNameAndCompany = requestor.Name;
            if (requestor.Company) {
                sponsorRepNameAndCompany += " (" + requestor.Company + ")";
            }

            const placeholders = {
                ...commonPlaceholders,
                ReceiverName: data.Name || "",
                SenderName: counterpart.Name || "",
                Title: counterpart.Title || "",
                Company: counterpart.Company || "",
                Email: data.Email,

                AttendeeName: invitee.Name || "",
                AttendeeEmail: invitee.Email || "",
                AttendeeCompany: invitee.Company || "",
                AttendeeNameAndCompany: attendeeNameAndCompany,
                SenderName: requestor.Name || "",
                SponsorRepName: requestor.Name || "",
                SponsorRepNameAndCompany: sponsorRepNameAndCompany,
                SenderCompany: requestor.Company || "",
                SponsorName: sponsorData.Name || "",
                BoothNo: sponsorData.BoothNo || "",



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
                html: emailBody,//emailBody
            };

            try {
                const meetingTime = Meeting.Timezone
                    ? moment.tz(Meeting.Slots[0], 'UTC').tz(Meeting.Timezone).format("YYYY MM DD HH mm")
                    : moment(Meeting.Slots[0]).utc().format("YYYY MM DD HH mm");
                console.log(meetingTime);
                let meetingTimeArr = meetingTime.split(" ");
                let start = [parseInt(meetingTimeArr[0]), parseInt(meetingTimeArr[1]), parseInt(meetingTimeArr[2]), parseInt(meetingTimeArr[3]), parseInt(meetingTimeArr[4])];
                console.log(start);

                const icsPayload = {
                    start: start,
                    duration: { minutes: 10 },
                    title: emailSubject,
                    description: emailSubject,//htmlToText(emailBody, { wordwrap: 130 }),
                    location: "UK",
                    status: "CONFIRMED",
                    organizer: { "name": Event.EventGroupName, "email": EmailTemplate.from },
                    // organizer: { "name": Event.EventGroupName, "email": data.Email },
                    // url: event_url || "",
                    // geo: geo_location,--
                    // alarms: alerms,--
                    attendees: [
                        {
                            name: data.Name,
                            email: data.Email,
                            rsvp: true,
                            partstat: 'ACCEPTED',
                            role: 'REQ-PARTICIPANT'
                        }
                    ],
                    // alarms: [
                    //     { action: 'display', trigger: { minutes: 10, before: true } }
                    // ],
                    // calName: Event.EventGroupName,
                };

                const attachmentICS = await utils.createICS(icsPayload);
                EmailPayload.attachmentICS = attachmentICS;
            } catch (err) {
                console.error("Error in createICS:", err);
                // ret_val.message = "Unknown error";
            }

            // Send email if the email is available
            if (_allow_send_email(data.Email)) {
                tasks.push(_send_email(Client, EmailPayload));
            }

            // Send SMS if enabled and phone is available
            if (configData.SendSMS && data.Phone && _allow_send_email(data.Email)) {
                const smsText = `Great news! Your meeting with ${counterpart.Name}, ${counterpart.Designation} at ${counterpart.Company}, is confirmed for ${startDateTime} at ${Event.EventGroupName}.`;
                tasks.push(_send_sms({ to: data.Phone, message: smsText }));
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
            dbClient.doc(`${instance_base_path}/AttendeeList/Attendees/${meeting.Requestor.AttendeeId}`).get(),
            dbClient.doc(`${instance_base_path}/AttendeeList/Attendees/${meeting.Invitee.AttendeeId}`).get()
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
        const MeetingUrl = `https://meet.e2m.live?mid=${meetingId}&email=${meeting.Invitee.Email}`;

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

            if (_allow_send_email(data.Email)) {
                tasks.push(_send_email(Client, EmailPayload));
            }

            if (meeting.SendSMS && data.Phone && _allow_send_email(data.Email)) {
                const smsText = `Reminder: You have a meeting with ${counterpart.Name}, ${counterpart.Designation} at ${counterpart.Company} on ${startDateTime} during ${Event.EventGroupName}.`;
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
async function validate_meeting(payload) {
    return new Promise(async (resolve, reject) => {
        logger.log(payload)
        let ret_val = { status: -1 };
        let Email = payload.data?.Email
        let MeetingId = payload.data?.MeetingId
        if (!payload.key) {
            ret_val = ERRCODE.PAYLOAD_ERROR
            reject(ret_val)
            return;
        }
        if (!payload.key.instanceId || !payload.key.clientId || !payload.key.eventId || !Email || !MeetingId) {
            ret_val = ERRCODE.PAYLOAD_ERROR
            reject(ret_val)
            return;
        }

        const iceId = `${payload.key.instanceId}_${payload.key.clientId}${payload.key.eventId}`;
        const event_base_path = `/${iceId}`;
        let meetingRef = await dbClient.collection(event_base_path).doc("MeetingList").collection("Meetings").doc(MeetingId).get()

        let Meeting = meetingRef.data();
        if (Email !== Meeting.Invitee.Email) {
            ret_val = ERRCODE.ACCESS_DENIED
            reject(ret_val)
            return;
        }

        if (Meeting.Status === "Confirmed") {
            ret_val.err = new Error("Meeting already confirmed.");
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
        // const conflictingSponsorSlot = Meeting.Slots.some(slot => Sponsor.Slots.includes(slot));
        // if (conflictingSponsorSlot) {
        //     ret_val.err = new Error("Sponsor slot already booked.");
        //     reject(ret_val)
        // }

        // // Check attendee slot constraints
        // if (attendee.Slots.length >= 2) {
        //     ret_val.err = new Error("Attendee slots are full.");
        //     reject(ret_val)
        // }

        // const conflictingAttendeeSlot = Meeting.Slots.some(slot => attendee.Slots.includes(slot));
        // if (conflictingAttendeeSlot) {
        //     ret_val.err = new Error("Attendee slot already booked.");
        //     reject(ret_val)
        // }
        ret_val.status = 0
        resolve(ret_val)
    })
}
async function accept_meeting(payload) {
    return new Promise(async (resolve, reject) => {
        logger.log(payload)
        let ret_val = { status: -1 };
        let Email = payload.data?.Email
        let MeetingId = payload.data?.MeetingId
        let Slot = payload.data?.Slot
        if (!payload.key) {
            ret_val = ERRCODE.PAYLOAD_ERROR
            reject(ret_val)
            return;
        }
        if (!payload.key.instanceId || !payload.key.clientId || !payload.key.eventId || !Email || !MeetingId) {
            ret_val = ERRCODE.PAYLOAD_ERROR
            reject(ret_val)
            return;
        }

        const iceId = `${payload.key.instanceId}_${payload.key.clientId}${payload.key.eventId}`;
        const event_base_path = `/${iceId}`;
        const eventInfoDoc = await dbClient.doc(`${event_base_path}/EventInfo`).get();
        const eventInfo = eventInfoDoc.data();
        let meetingRef = await dbClient.collection(event_base_path).doc("MeetingList").collection("Meetings").doc(MeetingId).get()

        let Meeting = meetingRef.data();
        if (Email !== Meeting.Invitee.Email) {
            ret_val = ERRCODE.ACCESS_DENIED
            reject(ret_val)
            return;
        }


        Meeting.Slots = [Slot]
        const tasks = [];

        if (Meeting.Status === "Confirmed") {
            ret_val.err = new Error("Meeting already confirmed.");
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

        // // Check attendee slot constraints
        // if (attendee.Slots.length >= 2) {
        //     ret_val.err = new Error("Attendee slots are full.");
        //     reject(ret_val)
        // }

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
                .set({ SponsorId: sponsorId, LastUpdatedDateTime: new Date(), Slots: Meeting.Slots, Status: "Confirmed" }, { merge: true })
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
            // mysql.executeQuery('UPDATE meeting SET requestStatus = ?, requestMeetingSlot = ? WHERE meetingCode = ?', ['confirmed', payload.data.Slot, payload.data.MeetingId], true)
            supabase
                .from('meeting')
                .update({
                    requestStatus: 'confirmed',
                    requestMeetingSlot: payload.data.Slot
                })
                .eq('meetingCode', payload.data.MeetingId)
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
                        Invitee: Invitee,
                        Sponsor: Sponsor
                    }
                    pubsubPayload.Meeting.Timezone = eventInfo.TimeZone;
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
                        attributes: { source: 'confirm-meeting' }
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
async function cancel_meeting(payload) {
    return new Promise(async (resolve, reject) => {
        let ret_val = { status: -1 };
        try {
            let Email = payload.data?.Email
            let MeetingId = payload.data?.MeetingId
            if (!payload.key) {
                ret_val = ERRCODE.PAYLOAD_ERROR
                reject(ret_val)
                return;
            }
            if (!payload.key.instanceId || !payload.key.clientId || !payload.key.eventId || !Email || !MeetingId) {
                ret_val = ERRCODE.PAYLOAD_ERROR
                reject(ret_val)
                return;
            }

            const { instanceId, clientId, eventId } = payload.key;
            const iceId = `${instanceId}_${clientId}${eventId}`;
            const event_base_path = `/${iceId}`;

            let meetingRef = await dbClient.collection(event_base_path).doc("MeetingList").collection("Meetings").doc(MeetingId).get()

            let Meeting = meetingRef.data();
            let meetingSlot;
            if (Email !== Meeting.Invitee.Email) {
                ret_val = ERRCODE.ACCESS_DENIED
                reject(ret_val)
                return;
            } else {
                meetingSlot = Meeting.Slots[0];
            }


            if (Meeting.Status === "Cancelled") {
                ret_val.err = new Error("Meeting already cancelled.");
                reject(ret_val)
            }
            const inviteeRef = dbClient.collection(event_base_path).doc("AttendeeList").collection("Attendees").doc(Meeting.Invitee.AttendeeId);
            const requestorRef = dbClient.collection(event_base_path).doc("AttendeeList").collection("Attendees").doc(Meeting.Requestor.AttendeeId);
            // Fetch attendee data
            const [inviteeSnap, requestorSnap] = await Promise.all([
                await inviteeRef.get(),
                await requestorRef.get(),
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
            const sponsorRef = dbClient.collection(event_base_path).doc("SponsorList").collection("Sponsors").doc(sponsorId);
            const sponsorSnap = await sponsorRef.get();
            if (!sponsorSnap.exists) {
                ret_val.err = new Error("Sponsor not found.");
                reject(ret_val)
            }

            // Update Firestore meeting status to Cancelled
            let meeting_doc = dbClient.collection(event_base_path).doc("MeetingList").collection("Meetings").doc(MeetingId)
            await meeting_doc.update({
                Status: "Cancelled",
                LastUpdatedDateTime: new Date(),
            });
            inviteeRef.update({
                Slots: Firestore.FieldValue.arrayRemove(meetingSlot),
                LastModifiedDate: new Date(),
            });
            requestorRef.update({
                Slots: Firestore.FieldValue.arrayRemove(meetingSlot),
                LastModifiedDate: new Date(),
            });
            sponsorRef.update({
                Slots: Firestore.FieldValue.arrayRemove(meetingSlot),
                LastModifiedDate: new Date(),
            });

            // Update MySQL meeting table
            // await mysql.executeQuery(
            //     `UPDATE meeting SET requestStatus = ?, requestUpdateDateTime = ? WHERE meetingCode = ?`,
            //     ['cancelled', new Date(), payload.data.MeetingId],
            //     true
            // );

            await supabase
                .from('meeting')
                .update({
                    requestStatus: 'cancelled',
                    requestUpdateDateTime: new Date().toISOString()  // ISO string recommended
                })
                .eq('meetingCode', payload.data.MeetingId);

            ret_val.status = 0;
            ret_val.message = "Meeting cancelled successfully.";
            ret_val.MeetingId = payload.data.MeetingId;
            resolve(ret_val);
        } catch (err) {
            console.error("Cancel Meeting Error:", err);
            reject({ status: -1, message: "Unknown error occurred." });
        }
    });
}
async function mark_meeting_attended(payload) {
    return new Promise(async (resolve, reject) => {
        let ret_val = { status: -1 };
        try {
            if (!payload || !payload.key || !payload.data || !payload.key.instanceId || !payload.key.clientId || !payload.key.eventId || !payload.data.MeetingId || !payload.data.hasOwnProperty("Attended")) {
                ret_val = ERRCODE.PAYLOAD_ERROR
                resolve(ret_val)
                return;
            }
            let MeetingId = payload.data.MeetingId

            const { instanceId, clientId, eventId } = payload.key;
            const iceId = `${instanceId}_${clientId}${eventId}`;
            const instance_base_path = `/${payload.key.instanceId}`;
            const event_base_path = `/${iceId}`;
            // console.log("event_base_path", event_base_path)
            let meetingRef = await dbClient.collection(event_base_path).doc("MeetingList").collection("Meetings").doc(MeetingId).get()

            let Meeting = meetingRef.exists ? meetingRef.data() : {};
            // let meetingSlot;
            // if (Email !== Meeting.Invitee.Email) {
            //     ret_val = ERRCODE.ACCESS_DENIED
            //     reject(ret_val)
            //     return;
            // } else {
            //     meetingSlot = Meeting.Slots[0];
            // }

            if (Meeting.Status !== "Confirmed") {
                ret_val.err = new Error("Meeting not Confirmed.");
                resolve(ret_val)
            }

            // Update Firestore meeting status to Cancelled
            let meeting_doc = dbClient.collection(event_base_path).doc("MeetingList").collection("Meetings").doc(MeetingId)
            await meeting_doc.update({
                Attended: (payload.data.Attended == true),
                LastUpdatedDateTime: new Date(),
            });

            // Update Supabase meeting table
            const { data, error } = await supabase
                .from('meeting')
                .update({ "attended": (payload.data.Attended == true), "requestUpdateDateTime": new Date() })
                .eq('"meetingCode"', Meeting.MeetingId);

            if (error) {
                console.error("Error updating sendEmail flag:", error);
            }

            ret_val.status = 0;
            if (payload.data.Attended == true) {
                ret_val.message = "Meeting marked as attended successfully.";
            } else {
                ret_val.message = "Meeting marked as absent successfully.";
            }
            ret_val.MeetingId = payload.data.MeetingId;

            // Send notification to Event Organizer
            if (!payload.data.Attended) {
                const [clientSnap, eventSnap] = await Promise.all([
                    dbClient.doc(`${instance_base_path}/ClientList/Clients/${payload.key.clientId}`).get(),
                    dbClient.doc(`${event_base_path}/EventInfo`).get()
                ]);
                const Client = clientSnap.data();
                const Event = eventSnap.data();
                // Fetch email template: first check event_base_path, then fallback to instance_base_path
                let emailTemplateDoc = await dbClient.doc(`${event_base_path}/mailtpl/Meeting/Absent`).get();
                if (!emailTemplateDoc.exists) {
                    emailTemplateDoc = await dbClient.doc(`${instance_base_path}/mailtpl/Meeting/Absent`).get();
                }
                if (!emailTemplateDoc.exists) throw new Error("Email template not found for Absent");

                const EmailTemplate = emailTemplateDoc.data();
                let requestor = Meeting.Requestor;
                let invitee = Meeting.Invitee;

                let sponsorData = {};
                if (Meeting.SponsorId) {
                    let sponsorDoc = await dbClient.doc(`${event_base_path}/SponsorList/Sponsors/${Meeting.SponsorId}`).get();
                    if (!sponsorDoc.exists) throw new Error("Sponsor not found");
                    sponsorData = sponsorDoc.data();

                    for (let i = 0; i < sponsorData.MappedContacts.length; i++) {
                        if (sponsorData.MappedContacts[i].AttendeeId == Meeting.Requestor.AttendeeId) {
                            requestor = Meeting.Requestor;
                            invitee = Meeting.Invitee;
                            break;
                        } else if (sponsorData.MappedContacts[i].AttendeeId == Meeting.Invitee.AttendeeId) {
                            requestor = Meeting.Invitee;
                            invitee = Meeting.Requestor;
                            break;
                        }
                    }
                }

                const emailTemplate = Handlebars.compile(EmailTemplate.html);
                const emailSubjectTemplate = Handlebars.compile(EmailTemplate.subject);

                const startDateTime = Meeting.Timezone
                    ? moment.tz(Meeting.Slots[0], 'UTC').tz(Meeting.Timezone).format("MMM DD, YYYY hh:mm A")
                    : moment(Meeting.Slots[0]).utc().format("MMM DD, YYYY hh:mm A");
                const timeZone = Meeting.Timezone || "UTC";
                const commonPlaceholders = {
                    StartDateTime: startDateTime,
                    Timezone: timeZone,
                    // MeetingUrl: MeetingUrl,
                    ClientName: Client.ClientName || "",
                    EventLogo: Event.EventLogo || "",
                    Message: Meeting.Message || "",
                    // AddToGoogle: google(calendarEvent),
                    // AddToOutlook: outlook(calendarEvent),
                    // AddToOffice365: office365(calendarEvent),
                    // AddToYahoo: yahoo(calendarEvent),
                    // AddToIcs: ics(calendarEvent),
                    Team: "Team"
                };
                const placeholders = {
                    ...commonPlaceholders,
                    SenderCompanyName: requestor.Company || "",
                    SenderName: requestor.Name || "",
                    OrganizerName: Event.OrganiserName || "",
                    SponsorName: sponsorData.Name || "",

                    AttendeeName: invitee.Name || "",
                    AttendeeEmail: invitee.Email || "",
                    AttendeeCompany: invitee.Company || "",
                    ReceiverCompanyName: invitee.Company || "",
                    Company: invitee.Company || "",
                    ReceiverName: invitee.Name || "",
                    Name: invitee.Name || "",
                    Title: invitee.Title || "",
                    Email: invitee.Email
                };
                const emailBody = emailTemplate(placeholders);
                const emailSubject = emailSubjectTemplate(placeholders);

                const EmailPayload = {
                    from: {
                        email: EmailTemplate.from,
                        name: Client.ClientName
                    },
                    to: {
                        name: Event.OrganiserName || "",
                        email: Event.OrganiserEmail || "",
                    },
                    cc: EmailTemplate.cc,
                    bcc: EmailTemplate.bcc,
                    reply_to: EmailTemplate.reply_to,
                    subject: emailSubject,
                    html: emailBody,//emailBody
                };
                // console.log("EmailPayload", EmailPayload);
                _send_email(Client, EmailPayload);
            }
            resolve(ret_val);
        } catch (err) {
            console.error("Marke Meeting Attended Error:", err);
            resolve({ status: -1, message: "Unknown error occurred." });
        }
    });
}
async function on_sms_replied(payload) {
    try {
        //const to_numbers = ['+447400103529', '+447900908411'];
        const to_numbers = ['+919831775969'];

        // if (payload.To == '+19295520827') {
        if (payload.To == '+447360276203') {
            let sms_body = `E2M Alert! Received message from [${payload.From}]:
---
${payload.Body}`;

            let tasks = [];
            for (let number of to_numbers) {
                tasks.push(_send_sms({ to: number, message: sms_body }));
            }

            await Promise.allSettled(tasks);
            console.log('sent SMS messages.');
        }
    } catch (ex) {
        console.log(ex);
    }

    return 0;
}

// AI Meeting Setter
async function ai_confirm_meeting(payload) {
    const { instanceId, clientId, eventId } = payload.key;
    const iceId = `${instanceId}_${clientId}${eventId}`;
    const event_base_path = `/${iceId}`;
    let requestorId = payload.data?.requestorId || payload.auth?.data?.UserId;

    // Check if AI matchmaking is allowed for this event
    const eventInfoDoc = await dbClient.doc(`/${iceId}/EventInfo`).get();
    const eventInfo = eventInfoDoc.data();
    if (!eventInfo?.WhoCanRequestMeeting?.includes('ai')) {
        return { status: -1, message: "AI matchmaking not allowed for this event." };
    }

    // Prepare QnA prompt template
    const defaultTemplate = `...`; // Same template as above
    const promptTemplateRaw = eventInfo?.AIMatchPrompt || defaultTemplate;
    const MATCH_SCORE = 0.5
    //const MATCH_SCORE = eventInfo?.AIMatchScore || 0.7;

    // Check both collections in parallel
    const [attendeeDoc, sponsorDoc] = await Promise.all([
        dbClient.collection(`/${iceId}/AttendeeList/Attendees`).doc(requestorId).get(),
        dbClient.collection(`/${iceId}/SponsorList/Sponsors`).doc(requestorId).get()
    ]);

    let requestor = attendeeDoc.data();

    if (requestor && requestor.Attending == false) {
        return {
            status: -1,
            message: "Requestor not Attending."
        };
    }

    // If not found as attendee, use sponsor data
    if (!requestor && sponsorDoc.exists) {
        const sponsorData = sponsorDoc.data();
        if (sponsorData.isMeetingEnabled) {
            requestor = {
                ...sponsorData,
                RegistrationType: {
                    RegistrationType: "sponsor",
                    RegistrationTypeEntityId: requestorId
                }
            };
        } else {
            return {
                status: -1,
                message: "Meeting is not enabled for the sponsor."
            };
        }
    }

    if (!requestor) {
        return {
            status: -1,
            message: "User not found."
        };
    }

    const registrationType = requestor.RegistrationType?.RegistrationType?.toLowerCase();
    const registrationTypeEntityId = requestor.RegistrationType?.RegistrationTypeEntityId ?? null;
    const isSponsor = registrationType === "sponsor";

    const requestorLimit = isSponsor ? (eventInfo.SPONSOR_MAX_CONFIRM_REQUEST || 0) : (eventInfo.ATTENDEE_MAX_CONFIRM_REQUEST || 0);

    // Slot setup: fetch confirmed + available slots
    let tasks = [];
    tasks.push(_confirmed_meetings_count(iceId));
    tasks.push(_confirmed_meetings_slots_by_sponsors(iceId));
    tasks.push(_confirmed_meetings_slots_by_attendees(iceId));
    const [confirmedCounts, sponsorRes, attendeeRes] = await Promise.all(tasks);

    // Sponsor-specific logic: get reps, total confirmed count, and QnA
    let sponsorId = null;
    let requestorQnA = {};
    let reps = [];
    let confirmedCountBySponsor = 0;

    if (isSponsor) {
        sponsorId = registrationTypeEntityId;
        reps = await _sponsor_representatives(iceId, sponsorId);
        reps.forEach(rep => {
            rep.confirmed = confirmedCounts[rep.attendeeId] || 0;
            confirmedCountBySponsor += rep.confirmed;
        });
        reps.sort((a, b) => a.confirmed - b.confirmed);
        requestorId = reps[0].attendeeId;
        requestorQnA = await _qna_response(iceId, 'Sponsor', [{ entityId: sponsorId }]);
    } else {
        requestorQnA = await _qna_response(iceId, 'Attendee', [{ entityId: requestorId }]);
    }

    // Abort if requestor already has max confirmed
    const requestorConfirmed = isSponsor ? confirmedCountBySponsor : (confirmedCounts[requestorId] || 0);
    const maxAllowed = requestorLimit - requestorConfirmed;
    if (maxAllowed <= 0) {
        return { status: 0, message: "You already have maximum confirmed meetings." };
    }

    // Determine match type and their limit
    const matchType = isSponsor ? "Attendee" : "Sponsor";
    const matchLimit = matchType === "Sponsor" ? (eventInfo.SPONSOR_MAX_CONFIRM_REQUEST || 0) : (eventInfo.ATTENDEE_MAX_CONFIRM_REQUEST || 0);

    // Get potential matches and their QnA responses
    const matchList = await _available_participants(iceId, matchType, matchLimit);
    const matchIds = matchList.map(p => ({ entityId: p.entityId }));
    const matchQnA = await _qna_response(iceId, matchType, matchIds);


    const requestorEntitySlots = isSponsor ? sponsorRes : attendeeRes;
    const inviteeEntitySlots = isSponsor ? attendeeRes : sponsorRes;

    //console.log("RequestorEntity slots", requestorEntitySlots);
    //console.log("InviteeEntity slots", inviteeEntitySlots);



    // Get requestor's preferred and confirmed slots
    const requestorEntityId = isSponsor ? sponsorId : requestorId;
    const preferred = requestorEntitySlots.preferredSlotsMap[requestorEntityId] || [];
    const confirmed = requestorEntitySlots.confirmedSlotsMap[requestorEntityId] || [];

    //console.log("confirmed", confirmed);
    //console.log("preferred", preferred);

    const confirmedSet = new Set(confirmed);
    const requestorAvailableSlots = preferred.filter(slot => !confirmedSet.has(slot));


    // Sort matches by fewest confirmed meetings
    const finalConfirmedCounts = matchType === "Sponsor"
        ? await _confirmed_meetings_count_by_sponsors(iceId)
        : confirmedCounts; // Use existing confirmedCounts variable

    matchList.forEach(p => p.confirmed = finalConfirmedCounts?.[p.entityId] || 0);
    matchList.sort((a, b) => a.confirmed - b.confirmed);

    // Render the prompt using Handlebars
    const promptData = {
        requestorQnA: (() => {
            const rawQnA = requestorQnA[isSponsor ? sponsorId : requestorId];
            if (!rawQnA) return '';

            const parsedData = parseQnAData(rawQnA);

            // Format the sponsor data for display
            let formatted = [];
            if (parsedData.investing_next_12_months) {
                formatted.push(`Investing over next 12 months?: ${parsedData.investing_next_12_months}`);
            }
            if (parsedData.interest_areas.length > 0) {
                formatted.push(`Areas of interest: ${parsedData.interest_areas.join(', ')}`);
            }
            if (parsedData.revenue_tier) {
                formatted.push(`Revenue Tier: ${parsedData.revenue_tier}`);
            }
            if (parsedData.revenue) {
                formatted.push(`Revenue: ${parsedData.revenue}`);
            }

            return formatted.join('\n');
        })(),
        invitees: matchList.map(match => {
            const rawQnA = matchQnA[match.entityId];
            if (!rawQnA) {
                return {
                    id: match.entityId,
                    investing_next_12_months: null,
                    interest_areas: [],
                    revenue_tier: null,
                    revenue: null
                };
            }

            const parsedData = parseQnAData(rawQnA);

            return {
                id: match.entityId,
                investing_next_12_months: parsedData.investing_next_12_months,
                interest_areas: parsedData.interest_areas,
                revenue_tier: parsedData.revenue_tier,
                revenue: parsedData.revenue
            };
        })
    };

    //console.log('Prompt Data:', JSON.stringify(promptData, null, 2));
    const renderedPrompt = Handlebars.compile(promptTemplateRaw)(promptData);
    console.log('Rendered Prompt:', renderedPrompt);

    // Get AI matches
    const response = await openai.chat.completions.create({
        model: "gpt-4.1",
        messages: [{ role: "user", content: renderedPrompt }]
    });
    //console.log('AI Response:', response);
    let scoredMatches = [];
    try {
        //console.log('AI response', response.choices[0].message.content);
        const parsedContent = JSON.parse(response.choices[0].message.content);
        scoredMatches = parsedContent.filter(m => m.score >= MATCH_SCORE);
        console.log('Scored Matches:', scoredMatches);
    } catch (err) {
        console.error('Parse error:', err);
        return { status: -1, message: "Failed to parse AI response." };
    }

    // Convert confirmedSlotsMap arrays to Sets for efficient lookup during meeting confirmation
    const confirmedSlotsSets = {};

    // Add requestor entity slots
    for (const [entityId, slots] of Object.entries(requestorEntitySlots.confirmedSlotsMap)) {
        confirmedSlotsSets[entityId] = new Set(Array.isArray(slots) ? slots : []);
    }

    // Add invitee entity slots  
    for (const [entityId, slots] of Object.entries(inviteeEntitySlots.confirmedSlotsMap)) {
        confirmedSlotsSets[entityId] = new Set(Array.isArray(slots) ? slots : []);
    }

    //console.log("matchList", matchList);

    let confirmedCount = 0;
    for (const { id: matchId } of scoredMatches) {
        const match = matchList.find(m => m.entityId === matchId);
        if (!match || (match.confirmed + confirmedCount) >= matchLimit) continue;
        const matchMaxAllowed = matchLimit - (match.confirmed + confirmedCount);
        //console.log("matchMaxAllowed", matchMaxAllowed);
        if (matchMaxAllowed <= 0) continue;
        const inviteeSlots = inviteeEntitySlots.preferredSlotsMap[matchId] || [];
        // const inviteeConfirmedArray = inviteeEntitySlots.preferredSlotsMap[matchId] || [];
        // const requestorConfirmedArray = requestorEntitySlots.confirmedSlotsMap[requestorEntityId] || [];

        // // Ensure we have Sets for efficient lookup
        // // Use the pre-converted and maintained Sets instead of converting arrays each time
        // const inviteeConfirmedSlots = confirmedSlotsSets[matchId] || new Set();
        // const requestorConfirmedSlots = confirmedSlotsSets[requestorEntityId] || new Set();

        console.log("matchId", matchId);
        console.log("inviteeSlots", inviteeSlots);
        //console.log("inviteeConfirmedSlots", Array.from(inviteeConfirmedSlots));
        console.log("requestorAvailableSlots", requestorAvailableSlots);

        const validSlots = requestorAvailableSlots.filter(slot =>
            inviteeSlots.includes(slot)
        );

        console.log("validSlots", validSlots);

        if (!validSlots) continue;

        try {
            const res = await request_meetings({
                key: payload.key,
                data: {
                    InviteeIds: [matchId],
                    RequestorId: requestorId,
                    Slots: validSlots,
                    IsCreatedByAI: 1
                }
            });

            // await confirm_meeting({
            //     key: payload.key,
            //     data: {
            //         MeetingId: res.created[0],
            //         Slot: validSlot
            //     }
            // });

            // Update confirmed slots Sets for efficient operations
            // if (!confirmedSlotsSets[requestorId]) confirmedSlotsSets[requestorId] = new Set();
            // if (!confirmedSlotsSets[matchId]) confirmedSlotsSets[matchId] = new Set();

            // confirmedSlotsSets[requestorId].add(validSlot);
            // confirmedSlotsSets[matchId].add(validSlot);


            confirmedCount++;
        } catch (err) {
            console.error("Matchmaking failed for:", matchId, err);
        }
    }

    return {
        status: 1,
        message: confirmedCount
            ? `AI confirmed ${confirmedCount} meeting${confirmedCount > 1 ? "s" : ""} successfully.`
            : "No matching candidate found."
    };
}


// Helper function to parse revenue tier information
function parseRevenueTier(revenueTierString) {
    if (!revenueTierString) return { tier: null, revenue: null };

    // Extract tier number
    const tierMatch = revenueTierString.match(/Tier\s+(\d+)/i);
    const tier = tierMatch ? `Tier ${tierMatch[1]}` : null;

    // Extract revenue range
    let revenue = null;
    if (revenueTierString.includes('under £30M')) {
        revenue = 'under £30M';
    } else if (revenueTierString.includes('between £30M & £500M')) {
        revenue = '£30M–£500M';
    } else if (revenueTierString.includes('exceeding £500M')) {
        revenue = 'exceeding £500M';
    }

    return { tier, revenue };
}

// Helper function to parse Q&A data
function parseQnAData(qnaData) {
    const parsed = {
        investing_next_12_months: null,
        interest_areas: [],
        revenue_tier: null,
        revenue: null
    };

    if (!qnaData) return parsed;

    // Handle string data (for requestorQnA)
    if (typeof qnaData === 'string') {
        const lines = qnaData.split('\n');
        qnaData = lines;
    }

    // Handle array of strings (for invitee qna)
    if (Array.isArray(qnaData)) {
        qnaData.forEach(item => {
            if (typeof item === 'string') {
                if (item.includes('Are you looking to invest in your tech stack')) {
                    const match = item.match(/Are you looking to invest in your tech stack[^:]*:\s*(.+)/);
                    if (match) parsed.investing_next_12_months = match[1].trim();
                } else if (item.includes('Pick 3 areas of interest')) {
                    const match = item.match(/Pick 3 areas of interest[^:]*:\s*(.+)/);
                    if (match) {
                        parsed.interest_areas = match[1].split(',').map(area => area.trim());
                    }
                } else if (item.includes('What is your company\'s yearly revenue')) {
                    const match = item.match(/What is your company's yearly revenue[^:]*:\s*(.+)/);
                    if (match) {
                        const tierInfo = parseRevenueTier(match[1].trim());
                        parsed.revenue_tier = tierInfo.tier;
                        parsed.revenue = tierInfo.revenue;
                    }
                }
            }
        });
    }

    return parsed;
}

async function send_sms_to_user(payload) {
    let ret_val = { status: -1 }
    try {
        // console.log("payload", payload)

        if (!payload || !payload.data || !(payload.data.mobiles && payload.data.users) || !payload.data.msg) {
            if (payload.data.mobiles && payload.data.mobiles.length === 0) {
                return { status: -1, message: "No mobiles provided" };
            } else if (payload.data.users && payload.data.users.length === 0) {
                return { status: -1, message: "No attendees provided" };
            } else if (!payload.data.mobiles && !payload.data.users) {
                return { status: -1, message: "No mobiles or users provided" };
            } else if (!payload.data.msg) {
                return { status: -1, message: "No message template provided" };
            }
        }
        let list = payload.data.mobiles;
        let inputType = "mobile";
        if (payload.data.users && payload.data.users.length > 0) {
            inputType = "users";
            list = payload.data.users
        }
        const tasks = [];
        let exceptions = [];
        // Loop through the recipients and send email/SMS
        for (let i = 0; i < list.length; i++) {
            const item = list[i];
            let data;
            // console.log("inputType", inputType)
            if (inputType == "users") {
                const userRef = await dbClient.collection("OA_UAT").doc("UserList").collection("Users").doc(item).get();
                if (userRef.exists) {
                    data = userRef.data();
                }
            } else {
                const userRef = await dbClient.collection("OA_UAT").doc("UserList").collection("Users").where("Phone", "==", item).get();
                const userDocs = !userRef.empty ? userRef.docs : [];
                if (userDocs.length > 0) {
                    data = userDocs[0].data();
                }
            }
            // console.log("data", data)
            if (!data || !data.Phone) {
                exceptions.push(`No User found: ${item}`);
                continue;
            } else {
                const mobile = data.Phone;
                if (!mobile || mobile.trim().length == 0) {
                    exceptions.push(mobile);
                } else if (mobile == "447777777777") {
                    exceptions.push(mobile);
                } else if (mobile == "447000000000") {
                    exceptions.push(mobile);
                } else {
                    let mobile_to = mobile;
                    let mobile2 = mobile;
                    let mobile3 = mobile;
                    if (mobile.length == 10) {
                        mobile2 = "+44" + mobile;
                        mobile_to = "+44" + mobile;
                    } else if (!mobile.startsWith("+")) {
                        mobile3 = "+" + mobile;
                        mobile_to = "+" + mobile;
                    }
                    const smsText = payload.data.msg;
                    tasks.push(_send_sms({ to: mobile_to, message: smsText }));
                }
            }
        }
        // Await all tasks (SMS sending)
        await Promise.allSettled(tasks);

        // Respond back with success
        ret_val.status = 0
        ret_val.exceptions = exceptions;
        if (list.length == exceptions.length) {
            ret_val.message = `Unable to sent SMS`
        } else {
            ret_val.message = `SMS sent`
        }
    } catch (err) {
        console.error("Error in send_sms_to_user:", err);
        ret_val.message = "Unknown error";
    }
    return ret_val
}

async function send_sms_to_attendee(payload) {
    let ret_val = { status: -1 }
    try {
        // console.log("payload", payload)
        if (!payload || !payload.key || !payload.data || !payload.key.instanceId || !payload.key.clientId || !payload.key.eventId || !(payload.data.mobiles && payload.data.attendees) || !payload.data.msg) {
            if (!payload.key) {
                return { status: -1, message: "key missing" };
            } else if (!payload.data) {
                return { status: -1, message: "data missing" };
            } else if (!payload.key.instanceId) {
                return { status: -1, message: "instanceId missing" };
            } else if (!payload.key.clientId) {
                return { status: -1, message: "clientId missing" };
            } else if (!payload.key.eventId) {
                return { status: -1, message: "eventId missing" };
            } else if (payload.data.mobiles && payload.data.mobiles.length === 0) {
                return { status: -1, message: "No mobiles provided" };
            } else if (payload.data.users && payload.data.attendees.length === 0) {
                return { status: -1, message: "No attendees provided" };
            } else if (!payload.data.mobiles && !payload.data.attendees) {
                return { status: -1, message: "No mobiles or attendees provided" };
            } else if (!payload.data.msg) {
                return { status: -1, message: "No message template provided" };
            }
        }
        // const instance_base_path = `/${payload.key.instanceId}`;
        const iceId = `${payload.key.instanceId}_${payload.key.clientId}${payload.key.eventId}`;
        const event_base_path = `/${iceId}`;

        const eventInfoDoc = await dbClient.doc(`${event_base_path}/EventInfo`).get();
        if (!eventInfoDoc.exists) {
            return { status: -1, message: "Event not found." };
        }
        const eventInfo = eventInfoDoc.data();

        let list = payload.data.mobiles;
        let inputType = "mobile";
        if (payload.data.attendees && payload.data.attendees.length > 0) {
            inputType = "attendees";
            list = payload.data.attendees
        }
        const tasks = [];
        let exceptions = [];
        // Loop through the recipients and send email/SMS
        for (let i = 0; i < list.length; i++) {
            const item = list[i];
            if (item) {
                let data;
                // console.log("inputType", inputType)
                console.log("AttendeeId", item)
                if (inputType == "attendees") {
                    const attendeeRef = await dbClient.collection(event_base_path).doc("AttendeeList").collection("Attendees").doc(item).get();
                    if (attendeeRef.exists) {
                        data = attendeeRef.data();
                    }
                } else {
                    const attendeeRef = await dbClient.collection(event_base_path).doc("AttendeeList").collection("Attendees").where("Phone", "==", item).get();
                    const attendeeDocs = !attendeeRef.empty ? attendeeRef.docs : [];
                    if (attendeeDocs.length > 0) {
                        data = attendeeDocs[0].data();
                    }
                }
                // console.log("attendeeData", attendeeData)
                if (!data || !data.Phone) {
                    exceptions.push(`No User found: ${item}`);
                    continue;
                } else {
                    const mobile = data.Phone;
                    if (!mobile || mobile.trim().length == 0) {
                        exceptions.push(mobile);
                    } else if (mobile == "447777777777") {
                        exceptions.push(mobile);
                    } else if (mobile == "447000000000") {
                        exceptions.push(mobile);
                    } else {
                        let attendeeNameAndCompany = data.Name;
                        if (data.Company) {
                            attendeeNameAndCompany += " (" + data.Company + ")";
                        }
                        let eventAttendConfirmLink = eventInfo.EventAttendConfirmLink.replace("{{AttendeeId}}", data.AttendeeId)
                        // console.log("eventAttendConfirmLink", eventAttendConfirmLink)
                        const placeholders = {
                            AttendeeId: data.AttendeeId || "",
                            AttendeeName: data.Name || "",
                            AttendeeEmail: data.Email || "",
                            AttendeeCompany: data.Company || "",
                            AttendeeNameAndCompany: attendeeNameAndCompany,
                            EventName: eventInfo.EventShortName || "",
                            EventAttendConfirmLink: eventAttendConfirmLink,
                        };

                        const emailTemplate = Handlebars.compile(payload.data.msg);
                        const smsText = emailTemplate(placeholders);
                        let mobile_to = mobile;
                        let mobile2 = mobile;
                        let mobile3 = mobile;
                        if (mobile.length == 10) {
                            mobile2 = "+44" + mobile;
                            mobile_to = "+44" + mobile;
                        } else if (!mobile.startsWith("+")) {
                            mobile3 = "+" + mobile;
                            mobile_to = "+" + mobile;
                        }
                        tasks.push(_send_sms({ to: mobile_to, message: smsText, attendeeId: data.AttendeeId, smsType: "AttendEvent", iceId: iceId }));
                    }
                }
            }
        }
        // Await all tasks (SMS sending)
        await Promise.allSettled(tasks);



        // Respond back with success
        ret_val.status = 0
        ret_val.exceptions = exceptions;
        if (list.length == exceptions.length) {
            ret_val.message = `Unable to sent SMS`
        } else {
            ret_val.message = `SMS sent`
        }
    } catch (err) {
        console.error("Error in send_sms_to_attendee:", err);
        ret_val.message = "Unknown error";
    }
    return ret_val
}

async function send_sms_to_attendee1(payload) {
    let ret_val = { status: -1 }
    try {
        // console.log("payload", payload)

        if (!payload || !payload.data || !(payload.data.mobiles && payload.data.attendees) || !payload.data.msg) {
            if (payload.data.mobiles && payload.data.mobiles.length === 0) {
                return { status: -1, message: "No mobiles provided" };
            } else if (payload.data.attendees && payload.data.attendees.length === 0) {
                return { status: -1, message: "No attendees provided" };
            } else if (!payload.data.mobiles && !payload.data.attendees) {
                return { status: -1, message: "No mobiles or attendees provided" };
            } else if (!payload.data.msg) {
                return { status: -1, message: "No message template provided" };
            }
        }
        // if (!payload || !payload.data || !payload.data.mobiles || payload.data.mobiles.length === 0 || !payload.data.msg) {
        //     if (!payload.data.mobiles || payload.data.mobiles.length === 0) {
        //         return { status: -1, message: "No mobiles provided" };
        //     } else if (!payload.data.msg) {
        //         return { status: -1, message: "No message template provided" };
        //     }
        // }
        const mobiles = payload.data.mobiles;
        let exceptions = [];
        const instance_base_path = `/${payload.key.instanceId}`;
        const iceId = `${payload.key.instanceId}_${payload.key.clientId}${payload.key.eventId}`;
        const event_base_path = `/${iceId}`;
        const tasks = [];

        // const configDoc = await dbClient.collection(event_base_path).doc("MeetingList").collection("Settings").doc("Config").get();
        // let configData = configDoc.exists ? configDoc.data() : [];


        // // Fetch client and event data
        // const [eventSnap] = await Promise.all([
        //     // dbClient.doc(`${instance_base_path}/ClientList/Clients/${payload.key.clientId}`).get(),
        //     dbClient.doc(`${event_base_path}/EventInfo`).get()
        // ]);
        // // const Client = clientSnap.data();
        // const Event = eventSnap.data();


        // Loop through the recipients and send email/SMS
        for (let i = 0; i < mobiles.length; i++) {
            const mobile = mobiles[i];
            if (!mobile || mobile.trim().length == 0) {
                exceptions.push(mobile);
            } else if (mobile == "447777777777") {
                exceptions.push(mobile);
            } else if (mobile == "447000000000") {
                exceptions.push(mobile);
            } else {
                let mobile_to = mobile;
                let mobile2 = mobile;
                let mobile3 = mobile;
                if (mobile.length == 10) {
                    mobile2 = "+44" + mobile;
                    mobile_to = "+44" + mobile;
                } else if (!mobile.startsWith("+")) {
                    mobile3 = "+" + mobile;
                    mobile_to = "+" + mobile;
                }
                // console.log("mobile", mobile)OA_UAT/UserList/Users
                // let attendeeRef = dbClient.collection(event_base_path).doc("AttendeeList").collection("Attendees").doc(mobile);
                // let attendeeRef = await dbClient.collection(event_base_path).doc("AttendeeList").collection("Attendees")
                let attendeeRef = await dbClient.collection("OA_UAT").doc("UserList").collection("Users")
                    .where("Phone", "==", mobile)
                    // .where("Phone", "==", mobile2)
                    // .where("Phone", "==", mobile3)
                    // .where("Phone", "==", mobile_to)
                    // .where(
                    //     Filter.or(
                    //         Filter.where('Phone', '==', mobile),
                    //         Filter.where('Phone', '==', mobile2),
                    //         Filter.where('Phone', '==', mobile3),
                    //         Filter.where('Phone', '==', mobile_to)
                    //     )
                    // )
                    .get();
                // console.log("attendeeRef", attendeeRef)
                const attendeeDocs = !attendeeRef.empty ? attendeeRef.docs : [];
                // console.log("attendeeDocs", attendeeDocs)
                // const attendeeData = await attendeeRef.get();
                // if (!attendeeData.exists) {
                if (attendeeDocs.length == 0) {
                    exceptions.push(`No Attendee found for mobile: ${mobile}`);
                    continue;
                } else {
                    const attendeeData = attendeeDocs[0].data();
                    // Send SMS if enabled and phone is available
                    // if (configData.SendSMS && attendeeData.Phone && _allow_send_email(attendeeData.Email)) {
                    if (attendeeData.Phone) {
                        // const smsText = `Hi ,\nPlease join the event - ${Event.EventGroupName}.`;
                        // const smsText = `Good morning, we look forward to welcoming you to Retail MediaX on May 13th. Please check your email (spam) with an update on your ticket & 121 meeting process from Laurenc@retailx.net. You can't reply to this message. - ${Event.EventGroupName}.`;
                        const smsText = payload.data.msg;
                        tasks.push(_send_sms({ to: mobile_to, message: smsText }));
                        // console.log("SMS to be sent:", smsText);
                    }
                }
            }
        }
        // Await all tasks (SMS sending)
        await Promise.allSettled(tasks);

        // Respond back with success
        ret_val.status = 0
        ret_val.exceptions = exceptions;
        if (mobiles.length == exceptions.length) {
            ret_val.message = `Unable to sent SMS`
        } else {
            ret_val.message = `SMS sent`
        }
    } catch (err) {
        console.error("Error in send_sms_to_attendee:", err);
        ret_val.message = "Unknown error";
    }
    return ret_val
}

// Supporting methods
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
async function _get_from_cache_or_db(cacheKey, fetchFn) {
    let cached = await cm.getFromCache({ cacheKey: cacheKey });
    if (cached?.cacheValueJson) {
        try {
            return JSON.parse(cached.cacheValueJson); // Always return as array of plain objects
        } catch (e) {
            console.warn("Failed to parse cached data", e);
        }
    }

    // Fetch fresh data from DB
    const freshDocs = await fetchFn();

    // Convert to plain objects if these are DocumentSnapshots
    const serializedDocs = freshDocs.map(doc => ({
        id: doc.id,
        data: doc.data()
    }));

    // Store in cache as stringified JSON
    await cm.storeInCache({
        cacheKey,
        cacheValueJson: JSON.stringify(serializedDocs),
        expirySeconds: 3600 * 3
    });

    return serializedDocs;
}
async function _get_slots(eventPath, sponsorId) {
    let ret_val = []
    const sponsorRef = await dbClient.collection(`${eventPath}/SponsorList/Sponsors`).doc(sponsorId.toString()).get();
    if (sponsorRef.exists) {
        const sponsor = sponsorRef.data();
        ret_val = sponsor.Slots || [];
    }
    return ret_val;
};
async function _save_as_draft(payload) {
    try {
        let iceId = `${payload.key.instanceId}_${payload.key.clientId}${payload.key.eventId}`;
        let eventBasePath = `/${iceId}`;
        let requestorId = payload.auth.data.UserId;//"1324000";//
        let inviteeId = payload.data.inviteeId;
        let RequestorIsSponsor = false;

        let requestorRef = dbClient.collection(eventBasePath).doc("AttendeeList").collection("Attendees").doc(requestorId);
        let requestorDoc = await requestorRef.get();
        let requestorData = requestorDoc.data();
        let registrationType = requestorData?.RegistrationType?.RegistrationType?.toLowerCase();
        let registrationTypeEntityId = requestorData?.RegistrationType?.RegistrationTypeEntityId ?? null;

        if (!requestorDoc.exists || !registrationTypeEntityId) {
            throw new Error("Requestor not found or registrationTypeEntityId is missing");
        }
        if (registrationType === "sponsor") {
            RequestorIsSponsor = true;
        }
        let eventInfo = await dbClient.collection(eventBasePath).doc("EventInfo").get();
        let eventData = eventInfo.exists ? eventInfo.data() : {};
        const MAX_CONFIRM = RequestorIsSponsor ? (eventData.SPONSOR_MAX_CONFIRM_REQUEST || 0) : (eventData.ATTENDEE_MAX_CONFIRM_REQUEST || 0);

        // const [confirmedMeetings, draftedInfo] = await Promise.all([
        // mysql.executeQuery(
        //     `SELECT COUNT(*) AS count FROM meeting 
        //          WHERE iceId= ? AND inviteeId = ? AND requestStatus = 'confirmed'`,
        //     [iceId, inviteeId], true
        // ),
        //     mysql.executeQuery(
        //         `SELECT 
        //             COUNT(DISTINCT inviteeId) AS total, 
        //             SUM(CASE WHEN inviteeId = ? THEN 1 ELSE 0 END) AS alreadyDrafted 
        //          FROM meeting 
        //          WHERE iceId= ? AND requestorTypeEntityId = ?`,
        //         [inviteeId, iceId, registrationTypeEntityId], true
        //     )
        // ]);
        const [confirmedMeetingsR, draftedInfoR] = await Promise.all([
            supabase.rpc('get_confirmed_meeting_count_by_attendee', {
                "p_ice_id": iceId,
                "p_invitee_id": inviteeId
            }),
            // supabase.from('meeting')
            //     .select('COUNT(*) AS count')
            //     .eq('iceId', iceId)
            //     .eq('inviteeId', inviteeId)
            //     .eq('requestStatus', 'confirmed'),

            // supabase.from('COUNT(DISTINCT inviteeId) AS total, SUM(CASE WHEN inviteeId = ' + inviteeId + ' THEN 1 ELSE 0 END) AS alreadyDrafted')
            //     .select('inviteeId')
            //     .eq('iceId', iceId)
            //     .eq('requestorTypeEntityId', registrationTypeEntityId)
            supabase.rpc('get_draft_status_summary_by_attendee', {
                p_ice_id: iceId,
                p_requestor_type_entity_id: registrationTypeEntityId,
                p_invitee_id: inviteeId
            })
        ]);
        // console.log(confirmedMeetingsR)
        const confirmedMeetings = confirmedMeetingsR.data;
        // console.log(confirmedMeetings)
        // console.log(draftedInfoR)
        const draftedInfo = draftedInfoR.data;
        // console.log(draftedInfo)
        if (confirmedMeetings.length > 0 && confirmedMeetings[0].count >= MAX_CONFIRM) {
            throw new Error(`Invitee already has ${MAX_CONFIRM} confirmed meetings`);
        }

        if (draftedInfo.length > 0) {
            const { total, alreadyDrafted } = draftedInfo[0];

            // if (total >= MAX_REQUESTS) {
            //     throw new Error(`Maximum of ${MAX_REQUESTS} drafted attendees reached for this sponsor`);
            // }

            if (alreadyDrafted > 0) {
                throw new Error("Invitee is already saved as a draft");
            }
        }

        // Save as draft
        // await mysql.executeQuery(
        //     `INSERT INTO meeting (iceId, requestorId, inviteeId, requestorType, requestorTypeEntityId,inviteeType, inviteeTypeEntityId,requestStatus) 
        //      VALUES (?, ?, ?, ?, ?, 'attendee','','draft')`,
        //     [iceId, requestorId, inviteeId, registrationType, registrationTypeEntityId]
        // );
        const { data, error } = await supabase
            .from('meeting')
            .insert([
                {
                    meetingCode: new Date().getTime(),
                    iceId: iceId,
                    requestorId: requestorId,
                    inviteeId: inviteeId,
                    requestorType: registrationType,
                    requestorTypeEntityId: registrationTypeEntityId,
                    inviteeType: 'attendee',
                    inviteeTypeEntityId: '',
                    requestStatus: 'draft',
                    requestUpdateDateTime: new Date(),
                    isCreatedByAI: 0,
                    sendEmail: 0,
                }
            ]);
        // await mysql.executeQuery(
        //     `INSERT INTO meeting (iceId, requestorId, inviteeId, requestorType, requestorTypeEntityId,inviteeType, inviteeTypeEntityId,requestStatus) 
        //      VALUES (?, ?, ?, ?, ?, 'attendee','','draft')`,
        //     [iceId, requestorId, inviteeId, registrationType, registrationTypeEntityId]
        // );
        // console.log(error)
        // console.log(data)
        // console.log(registrationType)
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
async function _send_sms({ to, message, attendeeId, smsType, iceId }) {
    try {
        // Replace this with your SMS provider's API logic, example using Twilio
        const accountSid = config.TWILIO.SID;
        const authToken = config.TWILIO.TOKEN;
        const fromPhoneNumber = config.TWILIO.FROM; // Your Twilio phone number

        // Use the twilio client
        const client = twilio(accountSid, authToken);

        // Send the SMS
        const smsResponse = await client.messages.create({
            body: message,         // SMS content
            from: fromPhoneNumber, // From your Twilio number
            to: to                 // Recipient phone number
        });

        // console.log(`SMS sent successfully to ${to}:, ${smsResponse.sid}`);
        if (attendeeId && smsType && iceId) {
            // Log the SMS in your database
            const smsLog = {
                SmsType: smsType,
                Phone: to,
                Message: message,
                SentAt: new Date(),
                Status: 'sent',
                SID: smsResponse.sid // Twilio message SID
            };
            // Save smsLog to your database here
            const attendeeRef = dbClient.collection(iceId).doc("AttendeeList").collection("Attendees").doc(attendeeId);
            await attendeeRef.update({
                "SMS": Firestore.FieldValue.arrayUnion(smsLog)
            });
        }
        return { success: true, message: 'SMS sent successfully' };
    } catch (error) {
        console.error('Error sending SMS:', error);
        return { success: false, error: error.message || 'Failed to send SMS' };
    }
}
// async function _available_participants(iceId, type, limit) {

//     console.log("_available_participants:", iceId, "type:", type, "limit:", limit);
//     // let selectClause;

//     // if (type === "Sponsor") {
//     //     // Sponsor logic: match by requestorType or inviteeType being 'Sponsor'
//     //     selectClause = `
//     //         SELECT sponsorId AS entityId, COUNT(*) AS confirmed FROM(
//     //         SELECT requestorTypeEntityId AS sponsorId
//     //             FROM e2m_o2o_prd.meeting
//     //             WHERE requestStatus = 'confirmed' 
//     //                 AND iceId = ?
//     //         AND requestorType = 'sponsor'
//     //                 AND requestorTypeEntityId IS NOT NULL

//     //             UNION ALL

//     //             SELECT inviteeTypeEntityId AS sponsorId
//     //             FROM e2m_o2o_prd.meeting
//     //             WHERE requestStatus = 'confirmed' 
//     //                 AND iceId = ?
//     //         AND inviteeType = 'sponsor'
//     //                 AND inviteeTypeEntityId IS NOT NULL
//     //     ) AS combined
//     //         `;
//     // } else {
//     //     // Attendee logic: match by requestorType or inviteeType being 'Attendee'
//     //     selectClause = `
//     //         SELECT attendeeId AS entityId, COUNT(*) AS confirmed FROM(
//     //             SELECT requestorId AS attendeeId
//     //             FROM e2m_o2o_prd.meeting
//     //             WHERE requestStatus = 'confirmed' 
//     //                 AND iceId = ?
//     //             AND requestorType = 'attendee'
//     //                 AND requestorId IS NOT NULL

//     //             UNION ALL

//     //             SELECT inviteeId AS attendeeId
//     //             FROM e2m_o2o_prd.meeting
//     //             WHERE requestStatus = 'confirmed' 
//     //                 AND iceId = ?
//     //             AND inviteeType = 'attendee'
//     //                 AND inviteeId IS NOT NULL
//     //         ) AS combined
//     //         `;
//     // }

//     // const finalQuery = `
//     //     ${selectClause}
//     //     GROUP BY entityId
//     //     HAVING confirmed < ?
//     //         LIMIT ?
//     //             `;

//     // const params = [iceId, iceId, limit, limit];

//     // const rows = await mysql.executeQuery(finalQuery, params);

//     const { data: rows, error } = await supabase.rpc('get_underbooked_entities', {
//         ice_id_input: iceId,
//         type_input: type,           // either 'Sponsor' or 'Attendee'
//         limit_input: limit,         // number of entities to return
//         max_confirmed: limit        // same as HAVING confirmed < ?
//     });

//     return rows.map(r => ({
//         entityId: r.entityId,
//         confirmed: Number(r.confirmed),
//     }));
// }

async function _available_participants(iceId, type, limit) {
    //console.log("_available_participants:", iceId, "type:", type, "limit:", limit);

    const { data: rows, error } = await supabase.rpc('get_available_entities', {
        ice_id_input: iceId,
        type_input: type.toLowerCase(),           // either 'Sponsor' or 'Attendee'
        max_confirmed: limit        // entities with >= this many meetings are excluded
    });

    if (error) {
        console.error('Error calling get_available_entities:', error);
        return [];
    }

    return rows.map(r => ({
        entityId: r.entityId,      // Note: now camelCase from Supabase
        confirmed: Number(r.confirmed || 0),
    }));
}
// async function _confirmed_meetings_count(iceId) {
//     // const rows = await mysql.executeQuery(`
//     //     SELECT 
//     //         m.requestorId AS participantId,
//     //         COUNT(m.meetingId) AS confirmed
//     //     FROM e2m_o2o_prd.meeting m
//     //     WHERE m.iceId = ? AND m.requestStatus = 'Confirmed'
//     //     GROUP BY m.requestorId
//     //     UNION
//     //     SELECT 
//     //         m.inviteeId AS participantId,
//     //         COUNT(m.meetingId) AS confirmed
//     //     FROM e2m_o2o_prd.meeting m
//     //     WHERE m.iceId = ? AND m.requestStatus = 'Confirmed'
//     //     GROUP BY m.inviteeId
//     //         `, [iceId, iceId]);

//     // const confirmedCounts = {};
//     // if (rows.length > 0) {
//     //     rows.forEach(row => {
//     //         confirmedCounts[row.participantId] = row.confirmed;
//     //     });
//     // }
//     const { data, error } = await supabase
//         .rpc('get_entity_wise_confirmed_meeting_count', { ice_id: iceId });

//     // if (error) throw error;
//     if (error) return {};

//     // Transform result to object if needed
//     const confirmedCounts = {};
//     data.forEach(row => {
//         confirmedCounts[row.participantId] = row.confirmed;
//     });


//     return confirmedCounts;
// }

async function _confirmed_meetings_count(iceId) {
    const { data, error } = await supabase
        .rpc('get_all_participants_meeting_count', { ice_id: iceId });

    if (error) {
        console.error('Error calling get_all_participants_meeting_count:', error);
        return {};
    }

    // Transform result to object
    const confirmedCounts = {};
    data.forEach(row => {
        confirmedCounts[row.participantId] = Number(row.confirmed || 0); // Note: camelCase from Supabase
    });

    return confirmedCounts;
}

// async function _confirmed_meetings_count_by_sponsors(iceId) {
//     // const rows = await mysql.executeQuery(`
//     //     SELECT sponsorId, COUNT(*) AS confirmed FROM(
//     //             SELECT 
//     //             m.requestorTypeEntityId AS sponsorId
//     //         FROM e2m_o2o_prd.meeting m
//     //         WHERE m.iceId = ? AND m.requestStatus = 'Confirmed' AND m.requestorType = 'Sponsor'
//     //         AND m.requestorTypeEntityId IS NOT NULL

//     //         UNION ALL

//     //         SELECT 
//     //             m.inviteeTypeEntityId AS sponsorId
//     //         FROM e2m_o2o_prd.meeting m
//     //         WHERE m.iceId = ? AND m.requestStatus = 'Confirmed' AND m.inviteeType = 'Sponsor'
//     //         AND m.inviteeTypeEntityId IS NOT NULL
//     //         ) AS combined
//     //     GROUP BY sponsorId
//     //         `, [iceId, iceId]);

//     const { data: rows, error } = await supabase
//         .rpc('get_confirmed_meeting_count_by_sponsor', { ice_id_input: iceId });

//     const confirmedCounts = {};
//     rows.forEach(row => {
//         confirmedCounts[row.sponsorId] = Number(row.confirmed);
//     });

//     return confirmedCounts; // { sponsorId: confirmedCount }
// }
// async function _confirmed_meetings_slots_by_sponsors(iceId) {
//     const configRef = dbClient.doc(`/${iceId}/MeetingList/Settings/Config`);
//     const configSnap = await configRef.get();
//     const config = configSnap.data();
//     const allSlots = config?.Slots || [];

//     // const rows = await mysql.executeQuery(`
//     //     SELECT 
//     //         CASE 
//     //             WHEN requestorType = 'Sponsor' THEN requestorTypeEntityId 
//     //             WHEN inviteeType = 'Sponsor' THEN inviteeTypeEntityId 
//     //         END AS sponsorId,
//     //         m.requestMeetingSlot as slot
//     //     FROM e2m_o2o_prd.meeting m
//     //     WHERE m.iceId = ? AND m.requestStatus = 'Confirmed'
//     //     AND(requestorType = 'Sponsor' OR inviteeType = 'Sponsor')
//     //             `, [iceId]);
//     const { data: rows, error } = await supabase
//         .rpc('get_sponsor_meeting_slots', { ice_id_input: iceId });

//     console.log('Confirmed meeting slots by sponsors:', rows);

//     const confirmedSlotsMap = {};
//     for (const row of rows) {
//         if (!confirmedSlotsMap[row.sponsorid]) confirmedSlotsMap[row.sponsorid] = new Set();
//         confirmedSlotsMap[row.sponsorid].add(row.slot);
//     }

//     // Convert Sets to arrays to match the attendees function format
//     for (const sponsorId in confirmedSlotsMap) {
//         confirmedSlotsMap[sponsorId] = Array.from(confirmedSlotsMap[sponsorId]);
//     }

//     // Create preferredSlotsMap with allSlots for each sponsor (if needed)
//     const preferredSlotsMap = {};
//     const sponsorIds = Object.keys(confirmedSlotsMap);
//     for (const sponsorId of sponsorIds) {
//         preferredSlotsMap[sponsorId] = allSlots; // All sponsors get all available slots
//     }

//     return { preferredSlotsMap, confirmedSlotsMap };
// }

async function _confirmed_meetings_count_by_sponsors(iceId) {
    const { data: rows, error } = await supabase
        .rpc('get_all_sponsors_meeting_count', { ice_id_input: iceId });

    if (error) {
        console.error('Error calling get_all_sponsors_meeting_count:', error);
        return {};
    }

    const confirmedCounts = {};
    rows.forEach(row => {
        confirmedCounts[row.sponsorId] = Number(row.confirmed || 0); // Note: camelCase from Supabase
    });

    return confirmedCounts; // { sponsorId: confirmedCount }
}

async function _confirmed_meetings_slots_by_sponsors(iceId) {
    const configRef = dbClient.doc(`/${iceId}/MeetingList/Settings/Config`);
    const configSnap = await configRef.get();
    const config = configSnap.data();
    const allSlots = config?.Slots || [];

    const { data: rows, error } = await supabase
        .rpc('get_all_sponsor_slots', { ice_id_input: iceId });

    if (error) {
        console.error('Error calling get_all_sponsor_slots:', error);
        return { preferredSlotsMap: {}, confirmedSlotsMap: {} };
    }


    const confirmedSlotsMap = {};
    const preferredSlotsMap = {};

    for (const row of rows) {
        const sponsorId = row.sponsorId; // Note: camelCase from Supabase

        // Initialize if not exists
        if (!confirmedSlotsMap[sponsorId]) {
            confirmedSlotsMap[sponsorId] = [];
            preferredSlotsMap[sponsorId] = allSlots; // All sponsors get all available slots
        }

        // Add confirmed slot if it exists
        if (row.slot) {
            confirmedSlotsMap[sponsorId].push(row.slot);
        }
    }

    return { preferredSlotsMap, confirmedSlotsMap };
}

// async function _confirmed_meetings_slots_by_attendees(iceId) {
//     // const rows = await mysql.executeQuery(`
//     //     SELECT 
//     //         CASE 
//     //             WHEN requestorType = 'Attendee' THEN requestorId 
//     //             WHEN inviteeType = 'Attendee' THEN inviteeId 
//     //         END AS attendeeId,
//     //         m.requestMeetingSlot as slot
//     //     FROM e2m_o2o_prd.meeting m
//     //     WHERE m.iceId = ? AND m.requestStatus = 'Confirmed'
//     //     AND(requestorType = 'Attendee' OR inviteeType = 'Attendee')
//     //             `, [iceId]);
//     const { data: rows, error1 } = await supabase.rpc('get_attendee_meeting_slots', {
//         ice_id_input: iceId
//     });

//     console.log('Confirmed meeting slots by attendees:', rows);

//     const confirmedSlotsMap = {};
//     for (const row of rows) {
//         if (!confirmedSlotsMap[row.attendeeid]) confirmedSlotsMap[row.attendeeid] = new Set();
//         confirmedSlotsMap[row.attendeeid].add(row.slot);
//     }

//     // Convert Sets to arrays to match preferredSlotsMap format
//     for (const attendeeId in confirmedSlotsMap) {
//         confirmedSlotsMap[attendeeId] = Array.from(confirmedSlotsMap[attendeeId]);
//     }

//     // Get preferred slots from Firestore
//     let preferredSlotsMap = {};
//     // const query = `
//     //     SELECT attendeeId, slots 
//     //     FROM e2m_o2o_prd.slots
//     //         `;
//     // const result = await mysql.executeQuery(query);
//     const { data: result, error2 } = await supabase
//         .from('slots')
//         .select('attendeeId, slots');

//     for (const row of result) {
//         preferredSlotsMap[row.attendeeId] = JSON.parse(row.slots || "[]");
//     }

//     return { preferredSlotsMap, confirmedSlotsMap };
// }

async function _confirmed_meetings_slots_by_attendees(iceId) {
    const { data: rows, error1 } = await supabase.rpc('get_all_attendee_slots', {
        ice_id_input: iceId
    });

    if (error1) {
        console.error('Error calling get_all_attendee_slots:', error1);
        return { preferredSlotsMap: {}, confirmedSlotsMap: {} };
    }


    const confirmedSlotsMap = {};

    // Initialize confirmedSlotsMap for all attendees
    for (const row of rows) {
        const attendeeId = row.attendeeId; // Note: camelCase from Supabase

        // Initialize if not exists
        if (!confirmedSlotsMap[attendeeId]) {
            confirmedSlotsMap[attendeeId] = [];
        }

        // Add confirmed slot if it exists
        if (row.slot) {
            confirmedSlotsMap[attendeeId].push(row.slot);
        }
    }

    // Get preferred slots from Firestore/Supabase
    let preferredSlotsMap = {};
    const { data: result, error2 } = await supabase
        .from('slots')
        .select('"attendeeId", slots');

    if (error2) {
        console.error('Error fetching preferred slots:', error2);
    } else {
        for (const row of result) {
            preferredSlotsMap[row.attendeeId] = JSON.parse(row.slots || "[]");
        }
    }

    // Ensure all attendees have entries in preferredSlotsMap (even if empty)
    for (const attendeeId of Object.keys(confirmedSlotsMap)) {
        if (!preferredSlotsMap[attendeeId]) {
            preferredSlotsMap[attendeeId] = [];
        }
    }

    return { preferredSlotsMap, confirmedSlotsMap };
}


async function _qna_response(iceId, type, participants) {
    const ids = participants.map(p => p.entityId);
    console.log('_qna_response called for ', JSON.stringify(ids));
    const placeholders = ids.map(() => "?").join(",");
    // const [rows] = await mysql.executeQuery(`
    //     SELECT entityId, questionLabel, selectedValue
    //     FROM e2m_o2o_prd.qna
    //     WHERE iceId = ? AND entitytype =? AND entityId IN(${placeholders})
    //     ORDER BY questionId
    //         `, [iceId, type, ...ids]);

    const { data: rows, error } = await supabase
        .from('qna')
        .select('"entityId", "questionLabel", "selectedValue"')
        .eq("iceId", iceId)
        .eq("entityType", type.toLowerCase())
        .in("entityId", ids)
        .order("questionId", { ascending: true });

    // console.log('iceId:', iceId);
    // console.log('participants:', ids);
    // console.log('type:', type);
    // console.log('QnA rows:', rows);

    const qnaMap = {};
    if (rows && rows.length > 0) {
        for (const row of rows) {
            if (!qnaMap[row.entityId]) qnaMap[row.entityId] = [];
            qnaMap[row.entityId].push(`${row.questionLabel}: ${row.selectedValue}`);
        }
    }
    return qnaMap;
}
async function _sponsor_representatives(iceId, sponsorId) {
    const snapshot = await dbClient
        .collection(`/${iceId}/AttendeeList/Attendees`)
        .where("RegistrationType.RegistrationType", "==", "Sponsor")
        .where("RegistrationType.RegistrationTypeEntityId", "==", sponsorId)
        .get();

    return snapshot.docs.map(doc => ({ attendeeId: doc.id, ...doc.data() }));
}
async function _sponsor_representatives_map(eventPath, sponsorIds) {
    const sponsorDocs = await Promise.all(
        sponsorIds.map(id => dbClient.doc(`${eventPath}/SponsorList/Sponsors/${id}`).get())
    );

    const sponsorMap = {};
    sponsorDocs.forEach(doc => {
        if (doc.exists) {
            const sponsor = doc.data();
            sponsorMap[sponsor.SponsorId] = sponsor.MappedContacts || [];
        }
    });

    return sponsorMap;
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
    meetingConfig: meeting_config,
    getMeetings: get_meetings,
    getMeetingDetail: get_meeting_detail,
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
    availableSlots: available_slots,
    confirmMeeting: confirm_meeting,
    acceptMeeting: accept_meeting,
    cancelMeeting: cancel_meeting,
    onSmsReplied: on_sms_replied,
    validateMeeting: validate_meeting,
    pubsubRequestMeeting: pubsub_request_meeting,
    pubsubConfirmMeeting: pubsub_confirm_meeting,
    meetingReminder: meeting_reminder,
    aiConfirmMeeting: ai_confirm_meeting,
    consolidatedSendEmail: consolidated_send_email,
    sendSMSToUser: send_sms_to_user,
    sendSMSToAttendee: send_sms_to_attendee,
    markMeetingAttended: mark_meeting_attended,
}