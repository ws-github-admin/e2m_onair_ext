'use strict';

const logger = require('./logger');
const config = require('../config.json');
const { Firestore } = require('@google-cloud/firestore');
const { Storage } = require('@google-cloud/storage');
const cm = require('./cache_manager');
const { ERRCODE } = require('./errcode');
const validate = require("./validator");
const mysql = require('./mysql');
const momentz = require('moment-timezone');
const MomentRange = require('moment-range');
const moment = MomentRange.extendMoment(momentz);
const fs = require('fs');
const path = require('path');
const PDFDocument = require('pdfkit');
const getStream = require('get-stream');
const { PassThrough } = require('stream');

const dbClient = new Firestore({
    projectId: config.GCP.PROJECT_ID,
    keyFilename: (__dirname + config.GCP.KEY_FILE_PATH).replace('/lib/', '/')
});

const bucketName = (config.FIREBASE_CONFIG.storageBucket);
const storageClient = new Storage({
    projectId: config.GCP.PROJECT_ID,
    keyFilename: (__dirname + config.GCP.KEY_FILE_PATH).replace('/lib/', '/')
});

async function user_info(payload) {
    logger.log(payload);
    return new Promise(async (resolve, reject) => {
        let ret_val = { status: -1 };

        try {
            if (!payload.key || !payload.key.instanceId || !payload.key.clientId || !payload.key.eventId) {
                ret_val = ERRCODE.PAYLOAD_ERROR;
                return reject(ERRCODE.PAYLOAD_ERROR);
            }
            if (!payload.data) {
                payload.data = {}
            }
            payload.data.fields = Array.isArray(payload.data.fields) ? (payload.data.fields) : [];
            const { instanceId, clientId, eventId } = payload.key;
            const iceId = `${instanceId}_${clientId}${eventId}`;
            const eventBasePath = `/${iceId}`;
            console.log("eventBasePath", eventBasePath);
            let entityId = (payload.data.attendeeId) ? payload.data.attendeeId : payload.auth?.data?.UserId;
            if (entityId) {
                let entityType = (payload.data.attendeeType) ? payload.data.attendeeType : 'Attendee'
                console.log("entityId", entityId);
                let entityCollectionPath = `${eventBasePath}/${entityType}List/${entityType}s`

                // Generate cache key based on id or email
                const cacheKey = `${iceId}/${entityType}/${entityId}`;
                console.log("cacheKey", cacheKey);

                // Clear cache if requested
                if (payload.data.clearCache) cm.removeFromCache({ cacheKey });

                // Fetch user data from cache or Firestore
                const entityData = await _get_from_cache_or_db(cacheKey, async () => {
                    if (entityId) {
                        const doc = await dbClient.collection(entityCollectionPath).doc(entityId).get();
                        return doc.exists ? doc.data() : null;
                    }
                });

                let sponsorData = null;
                if (entityData) {
                    if (entityType == 'Attendee') {
                        let regType = entityData?.RegistrationType?.RegistrationType;
                        let regTypeEntityId = entityData?.RegistrationType?.RegistrationTypeEntityId;
                        console.log("regType", regType);
                        console.log("regTypeEntityId", regTypeEntityId);
                        
                        if (regType === "Sponsor") {
                            let attendeeEntityCollectionPath = `${eventBasePath}/${regType}List/${regType}s`;
                            // Generate cache key based on id or email
                            const attendeeEntityTypeCacheKey = `${iceId}/${regType}/${regTypeEntityId}`;
                            console.log("attendeeEntityTypeCacheKey", attendeeEntityTypeCacheKey);

                            // Clear cache if requested
                            if (payload.data.clearCache) cm.removeFromCache({ cacheKey:attendeeEntityTypeCacheKey });

                            sponsorData = await _get_from_cache_or_db(attendeeEntityTypeCacheKey, async () => {
                                if (regTypeEntityId) {
                                    const doc = await dbClient.collection(attendeeEntityCollectionPath).doc(regTypeEntityId).get();
                                    return doc.exists ? doc.data() : null;
                                }
                            });

                            // let doc = await dbClient.collection(attendeeEntityCollectionPath).doc(regTypeEntityId).get();
                            // sponsorData = doc.data();
                        }
                        // If stats are requested
                        if (payload.data?.includeStat) {

                            // QnA query (safe and parameterized)
                            let qnaCountQuery = `
                                SELECT COUNT(DISTINCT questionId) AS totalQnA
                                FROM e2m_o2o_prd.qna
                                WHERE iceId = ? AND entityId = ? AND entityType = ?`;

                            let qnaQueryParams = [];
                            // Meeting stats query (parameterized based on regType)
                            let meetingStatQuery = '';
                            let meetingParams = [];

                            if (regType === 'Sponsor') {
                                qnaQueryParams = [iceId, regTypeEntityId, regType.toLowerCase()];
                                meetingParams = [iceId, entityType, regTypeEntityId, entityType, regTypeEntityId];
                                meetingStatQuery = `
                                SELECT
                                    SUM(CASE WHEN requestStatus = 'draft' THEN 1 ELSE 0 END) AS draftCount,
                                    SUM(CASE WHEN requestStatus = 'requested' THEN 1 ELSE 0 END) AS requestedCount,
                                    SUM(CASE WHEN requestStatus = 'confirmed' THEN 1 ELSE 0 END) AS confirmedCount,
                                    SUM(CASE WHEN isCreatedByAI = 1 THEN 1 ELSE 0 END) AS aiMatchCount
                                FROM e2m_o2o_prd.meeting
                                WHERE iceId = ?
                                    AND (
                                    (requestorType = ? AND requestorTypeEntityId = ?) OR
                                    (inviteeType = ? AND inviteeTypeEntityId = ?)
                                    )`;
                            } else {
                                qnaQueryParams = [iceId, entityId, regType.toLowerCase()];
                                meetingParams = [iceId, entityType, entityId, entityType, entityId];
                                meetingStatQuery = `
                                SELECT
                                    SUM(CASE WHEN requestStatus = 'draft' THEN 1 ELSE 0 END) AS draftCount,
                                    SUM(CASE WHEN requestStatus = 'requested' THEN 1 ELSE 0 END) AS requestedCount,
                                    SUM(CASE WHEN requestStatus = 'confirmed' THEN 1 ELSE 0 END) AS confirmedCount,
                                    SUM(CASE WHEN isCreatedByAI = 1 THEN 1 ELSE 0 END) AS aiMatchCount
                                FROM e2m_o2o_prd.meeting
                                WHERE iceId = ?
                                    AND (
                                    (requestorType = ? AND requestorId = ?) OR
                                    (inviteeType = ? AND inviteeId = ?)
                                    )`;
                            }

                            // Fetch both in parallel
                            const [qnaResult, meetingResult] = await Promise.all([
                                mysql.executeQuery(qnaCountQuery, qnaQueryParams),
                                mysql.executeQuery(meetingStatQuery, meetingParams),
                            ]);

                            // Populate stats in response
                            ret_val.stat = {
                                QnA: qnaResult[0]?.totalQnA || 0,
                                Meetings: {
                                    Draft: meetingResult[0]?.draftCount || 0,
                                    Requested: meetingResult[0]?.requestedCount || 0,
                                    confirmedCount: meetingResult[0]?.confirmedCount || 0,
                                    AIMatched: meetingResult[0]?.aiMatchCount || 0
                                },
                                AIMatched: meetingResult[0]?.aiMatchCount || 0,
                            };
                        }
                        ret_val.status = 0;
                        ret_val.result = _fields(entityData, payload.data.fields);
                        if (sponsorData) {
                            ret_val.sponsor = sponsorData
                        }
                    }
                    else if (entityType == 'Sponsor') {
                        if (payload.data?.includeStat) {
                            // QnA query (safe and parameterized)
                            const qnaCountQuery = `
                        SELECT COUNT(DISTINCT questionId) AS totalQnA
                        FROM e2m_o2o_prd.qna
                        WHERE iceId = ? AND entityId = ? AND entityType = ?`;

                            const qnaQueryParams = [iceId, entityId, entityType.toLowerCase()];

                            // Meeting stats query (parameterized based on regType)
                            let meetingStatQuery = '';
                            let meetingParams = [iceId, entityType, entityId, entityType, entityId];

                            meetingStatQuery = `
                        SELECT
                            SUM(CASE WHEN requestStatus = 'draft' THEN 1 ELSE 0 END) AS draftCount,
                            SUM(CASE WHEN requestStatus = 'requested' THEN 1 ELSE 0 END) AS requestedCount,
                            SUM(CASE WHEN requestStatus = 'confirmed' THEN 1 ELSE 0 END) AS confirmedCount,
                            SUM(CASE WHEN isCreatedByAI = 1 THEN 1 ELSE 0 END) AS aiMatchCount
                        FROM e2m_o2o_prd.meeting
                        WHERE iceId = ?
                            AND (
                            (requestorType = ? AND requestorTypeEntityId = ?) OR
                            (inviteeType = ? AND inviteeTypeEntityId = ?)
                            )`;


                            // Fetch both in parallel
                            const [qnaResult, meetingResult] = await Promise.all([
                                mysql.executeQuery(qnaCountQuery, qnaQueryParams),
                                mysql.executeQuery(meetingStatQuery, meetingParams),
                            ]);

                            // Populate stats in response
                            ret_val.stat = {
                                QnA: qnaResult[0]?.totalQnA || 0,
                                Meetings: {
                                    Draft: meetingResult[0]?.draftCount || 0,
                                    Requested: meetingResult[0]?.requestedCount || 0,
                                    confirmedCount: meetingResult[0]?.confirmedCount || 0,
                                    AIMatched: meetingResult[0]?.aiMatchCount || 0
                                },
                                AIMatched: meetingResult[0]?.aiMatchCount || 0,
                            };
                        }
                        ret_val.status = 0;
                        ret_val.result = _fields(entityData, payload.data.fields);
                    }
                    else if (entityType == 'Speaker') {
                        ret_val.status = 0;
                        ret_val.result = _fields(entityData, payload.data.fields);
                    }
                    else if (entityType == 'Session') {
                        ret_val.status = 0;
                        ret_val.result = _fields(entityData, payload.data.fields);
                    }
                }
            }

            // Final response
            resolve(ret_val);
        } catch (err) {
            console.error(err);
            reject(ERRCODE.UNKNOWN_ERROR);
        }
    });
}
async function user_update(payload) {
    return new Promise(async (resolve, reject) => {
        let ret_val = { status: -1 };
        try {
            logger.log(payload);
            const pk = payload.key;
            const pd = payload.data || {};
            let pa = payload.auth.data;
            const userUpdates = pd.updates || {};
            const sponsorUpdates = pd.sponsor || {};
            const instance_base_path = "/" + pk.instanceId;
            const eventBasePath = pk.instanceId + "_" + pk.clientId + pk.eventId;

            const allowedUserFields = ["FirstName", "LastName", "Name", "Address", "Company", "Designation", "Phone", "isHiddenFromChat", "preferredSlots","ProfilePictureURL"];
            const allowedSponsorFields = ["Name", "Company", "Logo", "Category", "Profile", "Website", "isMeetingEnabled"];

            if (!pa.UserId && typeof userUpdates !== 'object') {
                reject(ERRCODE.PAYLOAD_ERROR);
                return;
            }

            const userPath = `${eventBasePath}/AttendeeList/Attendees/${pa.UserId}`;
            const userDocRef = dbClient.doc(userPath);
            const userDoc = await userDocRef.get();

            if (!userDoc.exists) {
                reject(ERRCODE.DATA_NOT_FOUND);
                return;
            }

            const userData = userDoc.data();
            const userRegData = userData.RegistrationType;
            const userType = userRegData.RegistrationType;

            const sanitizedUserUpdates = {};
            for (const key of Object.keys(userUpdates)) {
                if (allowedUserFields.includes(key)) {
                    sanitizedUserUpdates[key] = userUpdates[key];
                }
            }

            if (Object.keys(sanitizedUserUpdates).length === 0 && !pd.sponsor) {
                reject(ERRCODE.PAYLOAD_ERROR); // nothing to update
                return;
            }

            const batch = dbClient.batch();

            // Update Firestore user record
            if (Object.keys(sanitizedUserUpdates).length > 0) {
                batch.update(userDocRef, sanitizedUserUpdates);
            }

            // Update Firestore sponsor record if applicable
            if (pd.sponsor && typeof sponsorUpdates === 'object') {
                const sponsorId = userRegData.RegistrationTypeEntityId;
                if (!sponsorId) {
                    reject(ERRCODE.PAYLOAD_ERROR);
                    return;
                }

                const sponsorPath = `${eventBasePath}/SponsorList/Sponsors/${sponsorId}`;
                const sponsorDocRef = dbClient.doc(sponsorPath);
                const sponsorDoc = await sponsorDocRef.get();

                if (sponsorDoc.exists) {
                    const sanitizedSponsorUpdates = {};
                    for (const key of Object.keys(sponsorUpdates)) {
                        if (allowedSponsorFields.includes(key)) {
                            sanitizedSponsorUpdates[key] = sponsorUpdates[key];
                        }
                    }

                    if (Object.keys(sanitizedSponsorUpdates).length > 0) {
                        batch.update(sponsorDocRef, sanitizedSponsorUpdates);
                    }
                }
            }

            await batch.commit();

            // If preferredSlots was updated, sync with MySQL
            if ('preferredSlots' in sanitizedUserUpdates) {
                const preferredSlots = sanitizedUserUpdates.preferredSlots || [];
                const updateSql = `
                    INSERT INTO e2m_o2o_prd.slots (attendeeId, slots)
                    VALUES (?, ?)
                    ON DUPLICATE KEY UPDATE slots = ?
                `;
                await mysql.execute(updateSql, [pa.UserId, JSON.stringify(preferredSlots), , JSON.stringify(preferredSlots)]);
            }

            ret_val.status = 0;
            ret_val.message = "Update successful";
            resolve(ret_val);

        } catch (err) {
            console.error(err);
            reject(ERRCODE.UNKNOWN_ERROR);
        }
    });
}
function upload_files(payload) {
    return new Promise(async (resolve, reject) => {
        let ret_val = { status: -1 }
        let tasks = []
        try {
            let bucket = storageClient.bucket(bucketName);
            for (var i = 0; i < payload.files.length; i++) {
                let fileObj = payload.files[i];
                let fileData = fileObj.blob;

                let fileName = fileObj.name.replace(/\s+/g, '-');
                fileName = fileName.replace(/[)]/g, '')
                fileName = fileName.replace(/[(]/g, '-')
                fileName.replace(/[^0-9a-zA-Z_.-]/g, '');
                fileName = fileName.toLowerCase();
                fileName = moment.utc().valueOf() + '-' + fileName;
                //logger.log(fileName)
                let target = fileObj.target + fileName;
                let fileRef = bucket.file(target);
                let fileOptions = {
                    public: true,
                    resumable: false,
                    metadata: { contentType: _base64MimeType(fileData) || fileObj.type },
                    validation: false
                }
                if (typeof fileData === 'string') {
                    let base64EncodedString = fileData.replace(/^data:\w+\/\w+;base64,/, '');
                    //logger.log(base64EncodedString)
                    let fileBuffer = Buffer.from(base64EncodedString, 'base64');
                    //console.log(fileBuffer)
                    tasks.push(_save_file(fileObj.name, fileRef, fileBuffer, fileOptions));
                } else {
                    let fileBuffer = get(fileData, 'buffer', fileData);
                    tasks.push(_save_file(fileObj.name, fileRef, fileBuffer, fileOptions));
                }
            }
            Promise.allSettled(tasks)
                .then((res) => {
                    ret_val.status = 0
                    ret_val.data = res
                    logger.log(ret_val)
                    resolve(ret_val)
                    return;
                })
                .catch(err => {
                    console.log(err)
                    ret_val = ERRCODE.UNKNOWN_ERROR
                    reject(ret_val)
                    return;
                })
        } catch (err) {
            console.log(err)
            ret_val = ERRCODE.UNKNOWN_ERROR
            reject(ret_val)
            return;
        }

    })
}
function _save_file(fileName, fileRef, fileBuffer, fileOptions) {
    return new Promise((resolve, reject) => {
        let ret_val = {}
        fileRef.save(fileBuffer, fileOptions)
            .then(() => {
                ret_val.name = fileName
                ret_val.url = fileRef.publicUrl()
                ret_val.type = fileOptions.metadata.contentType
                resolve(ret_val)
            })
            .catch(err => {
                logger.log(err)
                ret_val.name = fileName
                ret_val.url = ""
                ret_val.type = fileOptions.metadata.contentType
                reject(ret_val)
            })
    })
}
function _fields(obj, fields) {
    if (!Array.isArray(fields) || fields.length === 0) {
        return obj; // return all fields
    }

    let ret_obj = {};
    fields.forEach(field => {
        if (obj.hasOwnProperty(field)) {
            ret_obj[field] = obj[field];
        }
    });

    return ret_obj;
}
async function _get_from_cache_or_db(cacheKey, fallbackFn) {
    const cachedValue = await cm.getFromCache({ cacheKey });
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
function _base64MimeType(encoded) {
    var result = null;
    if (typeof encoded !== 'string') {
        return result;
    }
    var mime = encoded.match(/data:([a-zA-Z0-9]+\/[a-zA-Z0-9-.+]+).*,.*/);
    if (mime && mime.length) {
        result = mime[1];
    }
    return result;
}

async function get_sponsors_meeting(payload) {
    const { key, data } = payload;
    const { instanceId, clientId, eventId } = key;

    const allSponsors = [];
    const confirmedMeetings = [];
    const groupedData = {};
    const pathPrefix = `${instanceId}_${clientId}${eventId}`;
    const sponsorsRef = dbClient.collection(pathPrefix).doc("SponsorList").collection("Sponsors");
    const meetingsRef = dbClient.collection(pathPrefix).doc("MeetingList").collection("Meetings");

    try {
        // 1️⃣ Fetch sponsor(s)
        let sponsorSnapshot;

        if (data?.sponsor) {
            const docRef = sponsorsRef.doc(data.sponsor);
            const doc = await docRef.get();
            if (doc.exists) {
                sponsorSnapshot = [doc]; // Mimic forEach compatibility
            } else {
                sponsorSnapshot = [];
            }
        } else {
            const querySnapshot = await sponsorsRef.where("IsPublished", "==", true).get();
            sponsorSnapshot = querySnapshot.empty ? [] : querySnapshot.docs;
        }

        if (sponsorSnapshot.length > 0) {
            sponsorSnapshot.forEach(doc => {
                const sponsorData = {
                    eventId: eventId,
                    sponsorId: doc.id,
                    ...doc.data(),
                    requestData: {
                        RequestorId: data.RequestorId,
                        InviteeIds: data.InviteeIds,
                        Message: data.Message,
                        Timezone: data.Timezone
                    }
                };
                allSponsors.push(sponsorData);
                groupedData[sponsorData.sponsorId] = {
                    sponsor: sponsorData,
                    meetings: []
                };
            });
        }

        // 2️⃣ Fetch confirmed meetings
        const meetingSnapshot = await meetingsRef.where("Status", "==", "Confirmed").get();

        if (!meetingSnapshot.empty) {
            meetingSnapshot.forEach(doc => {
                const meetingData = {
                    eventId: eventId,
                    meetingId: doc.id,
                    ...doc.data()
                };
                confirmedMeetings.push(meetingData);

                const sponsorId = meetingData.SponsorId;
                if (groupedData[sponsorId]) {
                    groupedData[sponsorId].meetings.push(meetingData);
                }
            });

            // 3️⃣ Generate PDF
            const doc = new PDFDocument({ margin: 50 });

            const pdfDir = path.join(process.cwd(), 'pdfs');
            if (!fs.existsSync(pdfDir)) fs.mkdirSync(pdfDir);
            const filePath = path.join(`${pathPrefix}_meetings.pdf`);
            const fileStream = fs.createWriteStream(filePath);
            const passStream = new PassThrough();

            doc.pipe(fileStream);
            doc.pipe(passStream);

            doc.fontSize(18).text(`Sponsor Wise Confirmed Meetings`, { align: 'center' });
            doc.moveDown();

            let hasData = false;

            for (const sponsorId in groupedData) {
                const { sponsor, meetings } = groupedData[sponsorId];
                if (meetings.length > 0) {
                    hasData = true;
                    doc.fontSize(14).font('Helvetica-Bold').fillColor('black').text(`${sponsor.Name}`);
                    doc.moveDown(0.5);

                     // Sort meetings by time slot before displaying
                        meetings.sort((a, b) => {
                            const timeA = a.Slots && a.Slots.length > 0 ? new Date(a.Slots[0]) : new Date(0);
                            const timeB = b.Slots && b.Slots.length > 0 ? new Date(b.Slots[0]) : new Date(0);
                            return timeA - timeB;
                        });

                    meetings.forEach(meeting => {
                        const slot = meeting.Slots?.[0]
                            ? moment.tz(meeting.Slots[0], 'UTC').tz("Europe/London").format("MMM DD, YYYY hh:mm A")
                            : "N/A";

                        const requestor = meeting.Requestor || {};
                        const invitee = meeting.Invitee || {};
                        doc.x = 50;
                        doc.fontSize(12).fillColor('blue').text(`Time Slot: ${slot}`);
                        doc.moveDown(0.3);

                        const boxWidth = 250, boxHeight = 100, startX = doc.x, startY = doc.y;

                        doc.rect(startX, startY, boxWidth, boxHeight).stroke();
                        doc.fontSize(10).font('Helvetica-Bold').fillColor('black');
                        doc.text('Requestor', startX + (boxWidth - doc.widthOfString('Requestor')) / 2, startY + 5);
                        doc.font('Helvetica');
                        doc.text(`Name: ${requestor.Name || 'Unknown'}`, startX + 5, startY + 20);
                        doc.text(`Company: ${requestor.Company || 'N/A'}`, startX + 5, startY + 35);
                        doc.text(`Designation: ${requestor.Designation || 'N/A'}`, startX + 5, startY + 50);
                        doc.text(`Email: ${requestor.Email || 'N/A'}`, startX + 5, startY + 65);
                        doc.text(`Phone: ${requestor.Phone || 'N/A'}`, startX + 5, startY + 80);

                        const inviteeX = startX + boxWidth + 20;
                        doc.rect(inviteeX, startY, boxWidth, boxHeight).stroke();
                        doc.fontSize(10).font('Helvetica-Bold').fillColor('black');
                        doc.text('Invitee', inviteeX + (boxWidth - doc.widthOfString('Invitee')) / 2, startY + 5);
                        doc.font('Helvetica');
                        doc.text(`Name: ${invitee.Name || 'Unknown'}`, inviteeX + 5, startY + 20);
                        doc.text(`Company: ${invitee.Company || 'N/A'}`, inviteeX + 5, startY + 35);
                        doc.text(`Designation: ${invitee.Designation || 'N/A'}`, inviteeX + 5, startY + 50);
                        doc.text(`Email: ${invitee.Email || 'N/A'}`, inviteeX + 5, startY + 65);
                        doc.text(`Phone: ${invitee.Phone || 'N/A'}`, inviteeX + 5, startY + 80);

                        doc.moveDown(7);
                    });

                    const sponsorIds = Object.keys(groupedData).filter(id => groupedData[id].meetings.length > 0);
                    if (sponsorId !== sponsorIds[sponsorIds.length - 1]) {
                        doc.addPage();
                    }

                }
            }

            if (!hasData) {
                doc.fontSize(14).text("No sponsors with confirmed meetings available.");
            }

            doc.end();

            const buffer = await getStream.buffer(passStream);
            const base64PDF = buffer.toString('base64');

            return {
                allSponsors,
                confirmedMeetings,
                groupedData,
                pdfPath: filePath,
                pdfBuffer: buffer,
                pdfBase64: base64PDF
            };
        }

        return {
            allSponsors,
            confirmedMeetings,
            groupedData,
            message: "No confirmed meetings. No PDF generated."
        };

    } catch (error) {
        console.error(`Error fetching data for ${eventId}:`, error.message);
        throw error;
    }
}

module.exports = {
    userInfo: user_info,
    userUpdate: user_update,
    uploadFiles: upload_files,
    getSponsorsMeeting : get_sponsors_meeting
}