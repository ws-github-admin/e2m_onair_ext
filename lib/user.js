'use strict';

const logger = require('./logger');
const config = require('../config.json');
const { Firestore } = require('@google-cloud/firestore');
const { Storage } = require('@google-cloud/storage');
const cm = require('./cache_manager');
const { ERRCODE } = require('./errcode');
const validate = require("./validator");
// const mysql = require('./mysql');
const momentz = require('moment-timezone');
const MomentRange = require('moment-range');
const moment = MomentRange.extendMoment(momentz);
const fs = require('fs');
const path = require('path');
const PDFDocument = require('pdfkit');
const getStream = require('get-stream');
const { PassThrough } = require('stream');

const { createClient } = require('@supabase/supabase-js')

const supabase = createClient(config.SUPABASE.DATABASE, config.SUPABASE.KEY)

const dbClient = new Firestore({
    projectId: config.GCP.PROJECT_ID,
    keyFilename: (__dirname + config.GCP.KEY_FILE_PATH).replace('/lib/', '/')
    // keyFilename: ('E:\\Debashis\\OnAir\\E2M_AI\\OnAirEXT_Dev\\creds\\prd_key.json')
});

const bucketName = (config.FIREBASE_CONFIG.storageBucket);
const storageClient = new Storage({
    projectId: config.GCP.PROJECT_ID,
    keyFilename: (__dirname + config.GCP.KEY_FILE_PATH).replace('/lib/', '/')
});

async function attendee_list(payload) {
    // logger.log(payload);
    return new Promise(async (resolve, reject) => {
        let ret_val = { status: -1 };
        try {
            if (!payload.key || !payload.key.instanceId || !payload.key.clientId || !payload.key.eventId || !payload.data.fields || payload.data.fields.length == 0) {
                ret_val = ERRCODE.PAYLOAD_ERROR;
                return resolve(ERRCODE.PAYLOAD_ERROR);
            }
            if (!payload.data) {
                payload.data = {}
            }
            payload.data.fields = Array.isArray(payload.data.fields) ? (payload.data.fields) : [];
            const allowedFields = ["FirstName", "LastName", "Email", "AttendeeId", "VCard"];

            const sanitizedFields = [];
            for (let i = 0; i < payload.data.fields.length; i++) {
                const key = payload.data.fields[i];
                if (allowedFields.includes(key)) {
                    sanitizedFields.push(key);
                }
            }

            if (sanitizedFields.length === 0) {
                resolve(ERRCODE.PAYLOAD_ERROR); // nothing to update
                return;
            }

            const { instanceId, clientId, eventId } = payload.key;
            const iceId = `${instanceId}_${clientId}${eventId}`;
            const eventBasePath = `/${iceId}`;
            // console.log("eventBasePath", eventBasePath);
            let entityCollectionPath = `${eventBasePath}/AttendeeList/Attendees`
            // console.log("entityCollectionPath", entityCollectionPath);

            let entitySnap = dbClient.collection(entityCollectionPath);
            let entityId = (payload.data.attendeeId) ? payload.data.attendeeId : "";
            if (entityId) {
                entitySnap = entitySnap.where("AttendeeId", "==", entityId);
            }
            entitySnap = await entitySnap.get();
            const entityDocs = entitySnap.empty ? [] : entitySnap.docs;
            let allEntityData = [];
            let attendeeList = [];
            if (entityDocs.length > 0) {
                entityDocs.forEach(doc => {
                    const docData = doc.data();
                    const data = _fields(docData, sanitizedFields);
                    allEntityData.push(data);
                    attendeeList.push(data.AttendeeId);
                });
            }

            // console.log("attendeeList: ", JSON.stringify(attendeeList));

            // console.log("attendeeList: ", JSON.stringify(attendeeList));
            // console.log("allEntityData.length: ", allEntityData.length);
            // console.log("allEntityData: ", allEntityData);
            ret_val.status = 0;
            ret_val.result = allEntityData;

            // Final response
            resolve(ret_val);
        } catch (err) {
            console.error(err);
            resolve(ERRCODE.UNKNOWN_ERROR);
        }
    });
}
// async function user_info(payload) {
//     // logger.log(payload);
//     return new Promise(async (resolve, reject) => {
//         let ret_val = { status: -1 };

//         try {
//             if (!payload.key || !payload.key.instanceId || !payload.key.clientId || !payload.key.eventId) {
//                 ret_val = ERRCODE.PAYLOAD_ERROR;
//                 return reject(ERRCODE.PAYLOAD_ERROR);
//             }
//             if (!payload.data) {
//                 payload.data = {}
//             }
//             payload.data.fields = Array.isArray(payload.data.fields) ? (payload.data.fields) : [];
//             const { instanceId, clientId, eventId } = payload.key;
//             const iceId = `${instanceId}_${clientId}${eventId}`;
//             const eventBasePath = `/${iceId}`;
//             console.log("eventBasePath", eventBasePath);
//             let entityId = (payload.data.attendeeId) ? payload.data.attendeeId : payload.auth?.data?.UserId;
//             if (entityId) {
//                 let entityType = (payload.data.attendeeType) ? payload.data.attendeeType : 'Attendee'
//                 console.log("entityId", entityId);
//                 let entityCollectionPath = `${eventBasePath}/${entityType}List/${entityType}s`

//                 // Generate cache key based on id or email
//                 const cacheKey = `${iceId}/${entityType}/${entityId}`;
//                 console.log("cacheKey", cacheKey);


//                 //let clearCache = payload.data?.clearCache === true; // ensure it's explicitly true
//                 let clearCache = payload.data?.clearCache !== false; // ensure it implicitly true
//                 if (clearCache) {
//                     cm.removeFromCache({ cacheKey: cacheKey });
//                 }

//                 // Fetch user data from cache or Firestore
//                 const entityData = await _get_from_cache_or_db(cacheKey, async () => {
//                     if (entityId) {
//                         const doc = await dbClient.collection(entityCollectionPath).doc(entityId).get();
//                         return doc.exists ? doc.data() : null;
//                     }
//                 });

//                 let sponsorData = null;
//                 if (entityData) {
//                     if (entityType == 'Attendee') {
//                         let regType = entityData?.RegistrationType?.RegistrationType;
//                         let regTypeEntityId = entityData?.RegistrationType?.RegistrationTypeEntityId;
//                         console.log("regType", regType);
//                         console.log("regTypeEntityId", regTypeEntityId);

//                         if (regType === "Sponsor") {
//                             let attendeeEntityCollectionPath = `${eventBasePath}/${regType}List/${regType}s`;
//                             // Generate cache key based on id or email
//                             const attendeeEntityTypeCacheKey = `${iceId}/${regType}/${regTypeEntityId}`;
//                             console.log("attendeeEntityTypeCacheKey", attendeeEntityTypeCacheKey);


//                             //let clearCache = payload.data?.clearCache === true; // ensure it's explicitly true
//                             let clearCache = payload.data?.clearCache !== false; // ensure it implicitly true
//                             if (clearCache) {
//                                 cm.removeFromCache({ cacheKey: attendeeEntityTypeCacheKey });
//                             }


//                             sponsorData = await _get_from_cache_or_db(attendeeEntityTypeCacheKey, async () => {
//                                 if (regTypeEntityId) {
//                                     const doc = await dbClient.collection(attendeeEntityCollectionPath).doc(regTypeEntityId).get();
//                                     return doc.exists ? doc.data() : null;
//                                 }
//                             });

//                             // let doc = await dbClient.collection(attendeeEntityCollectionPath).doc(regTypeEntityId).get();
//                             // sponsorData = doc.data();
//                         }
//                         // If stats are requested
//                         if (payload.data?.includeStat) {

//                             // // QnA query (safe and parameterized)
//                             // let qnaCountQuery = `
//                             //     SELECT COUNT(DISTINCT questionId) AS totalQnA
//                             //     FROM e2m_o2o_prd.qna
//                             //     WHERE iceId = ? AND entityId = ? AND entityType = ?`;

//                             // let qnaQueryParams = [];
//                             // // Meeting stats query (parameterized based on regType)
//                             // let meetingStatQuery = '';
//                             // let meetingParams = [];

//                             // if (regType === 'Sponsor') {
//                             //     qnaQueryParams = [iceId, regTypeEntityId, regType.toLowerCase()];
//                             //     meetingParams = [iceId, entityType, regTypeEntityId, entityType, regTypeEntityId];
//                             //     meetingStatQuery = `
//                             //     SELECT
//                             //         SUM(CASE WHEN requestStatus = 'draft' THEN 1 ELSE 0 END) AS draftCount,
//                             //         SUM(CASE WHEN requestStatus = 'requested' THEN 1 ELSE 0 END) AS requestedCount,
//                             //         SUM(CASE WHEN requestStatus = 'confirmed' THEN 1 ELSE 0 END) AS confirmedCount,
//                             //         SUM(CASE WHEN isCreatedByAI = 1 THEN 1 ELSE 0 END) AS aiMatchCount
//                             //     FROM e2m_o2o_prd.meeting
//                             //     WHERE iceId = ?
//                             //         AND (
//                             //         (requestorType = ? AND requestorTypeEntityId = ?) OR
//                             //         (inviteeType = ? AND inviteeTypeEntityId = ?)
//                             //         )`;
//                             // } else {
//                             //     qnaQueryParams = [iceId, entityId, regType.toLowerCase()];
//                             //     meetingParams = [iceId, entityType, entityId, entityType, entityId];
//                             //     meetingStatQuery = `
//                             //     SELECT
//                             //         SUM(CASE WHEN requestStatus = 'draft' THEN 1 ELSE 0 END) AS draftCount,
//                             //         SUM(CASE WHEN requestStatus = 'requested' THEN 1 ELSE 0 END) AS requestedCount,
//                             //         SUM(CASE WHEN requestStatus = 'confirmed' THEN 1 ELSE 0 END) AS confirmedCount,
//                             //         SUM(CASE WHEN isCreatedByAI = 1 THEN 1 ELSE 0 END) AS aiMatchCount
//                             //     FROM e2m_o2o_prd.meeting
//                             //     WHERE iceId = ?
//                             //         AND (
//                             //         (requestorType = ? AND requestorId = ?) OR
//                             //         (inviteeType = ? AND inviteeId = ?)
//                             //         )`;
//                             // }

//                             // // Fetch both in parallel
//                             // const [qnaResult, meetingResult] = await Promise.all([
//                             //     mysql.executeQuery(qnaCountQuery, qnaQueryParams),
//                             //     mysql.executeQuery(meetingStatQuery, meetingParams),
//                             // ]);

//                             // // Populate stats in response
//                             // ret_val.stat = {
//                             //     QnA: qnaResult[0]?.totalQnA || 0,
//                             //     Meetings: {
//                             //         Draft: meetingResult[0]?.draftCount || 0,
//                             //         Requested: meetingResult[0]?.requestedCount || 0,
//                             //         confirmedCount: meetingResult[0]?.confirmedCount || 0,
//                             //         AIMatched: meetingResult[0]?.aiMatchCount || 0
//                             //     },
//                             //     AIMatched: meetingResult[0]?.aiMatchCount || 0,
//                             // };

//                             // ret_val.stat = {
//                             //     QnA: 0,
//                             //     Meetings: {
//                             //         Draft: 0,
//                             //         Requested: 0,
//                             //         confirmedCount: 0,
//                             //         AIMatched: 0
//                             //     },
//                             //     AIMatched: 0,
//                             // };
//                             // --- Prepare filters based on regType ---
//                             let qnaFilter = {};
//                             let meetingOrFilter = '';

//                             if (regType === 'Sponsor') {
//                                 qnaFilter = {
//                                     "iceId": iceId,
//                                     "entityId": regTypeEntityId,
//                                     "entityType": regType.toLowerCase()
//                                 };

//                                 meetingOrFilter = `
//                                                 or=(
//                                                 and("requestorType".eq.${entityType},"requestorTypeEntityId".eq.${regTypeEntityId}),
//                                                 and("inviteeType".eq.${entityType},"inviteeTypeEntityId".eq.${regTypeEntityId})
//                                                 )
//                                             `;
//                             } else {
//                                 qnaFilter = {
//                                     "iceId": iceId,
//                                     "entityId": entityId,
//                                     "entityType": regType.toLowerCase()
//                                 };

//                                 meetingOrFilter = `
//                                             or=(
//                                             and("requestorType".eq.${entityType},"requestorId".eq.${entityId}),
//                                             and("inviteeType".eq.${entityType},"inviteeId".eq.${entityId})
//                                             )
//                                         `;
//                             }

//                             // --- QnA Count Query (Distinct Question IDs) ---
//                             const { data: qnaData, error: qnaError } = await supabase
//                                 .from('qna')
//                                 .select('questionId')
//                                 .match(qnaFilter);

//                             const qnaSet = new Set((qnaData || []).map(q => q.questionId));
//                             const totalQnA = qnaSet.size;

//                             // --- Meeting Stats Query ---
//                             const { data: meetingData, error: meetingError } = await supabase
//                                 .from('meeting')
//                                 .select('"requestStatus", "isCreatedByAI"')
//                                 .eq('iceId', iceId)
//                                 .filter(meetingOrFilter);

//                             // --- Aggregate Meeting Stats ---
//                             const meetingStats = {
//                                 draftCount: 0,
//                                 requestedCount: 0,
//                                 confirmedCount: 0,
//                                 aiMatchCount: 0,
//                             };

//                             for (const row of meetingData || []) {
//                                 if (row.requestStatus === 'draft') meetingStats.draftCount++;
//                                 if (row.requestStatus === 'requested') meetingStats.requestedCount++;
//                                 if (row.requestStatus === 'confirmed') meetingStats.confirmedCount++;
//                                 if (row.isCreatedByAI) meetingStats.aiMatchCount++;
//                             }

//                             // --- Final Output ---
//                             ret_val.stat = {
//                                 QnA: totalQnA,
//                                 Meetings: {
//                                     Draft: meetingStats.draftCount,
//                                     Requested: meetingStats.requestedCount,
//                                     confirmedCount: meetingStats.confirmedCount,
//                                     AIMatched: meetingStats.aiMatchCount,
//                                 },
//                                 AIMatched: meetingStats.aiMatchCount
//                             };
//                         }
//                         ret_val.status = 0;
//                         ret_val.result = _fields(entityData, payload.data.fields);
//                         if (sponsorData) {
//                             ret_val.sponsor = sponsorData
//                             ret_val.MIN_REQUESTS_BATCH1 = config.SPONSOR_MIN_REQUESTS_BATCH1 || 20;
//                             ret_val.MIN_REQUESTS_BATCH2 = config.SPONSOR_MIN_REQUESTS_BATCH2 || 0;
//                             ret_val.BATCH_MEETING_REQUESTS = config.BATCH_MEETING_REQUESTS_SUBMIT_ENABLED || false;
//                         }
//                     } else if (entityType == 'Sponsor') {
//                         if (payload.data?.includeStat) {
//                             //     // QnA query (safe and parameterized)
//                             //     const qnaCountQuery = `
//                             // SELECT COUNT(DISTINCT questionId) AS totalQnA
//                             // FROM e2m_o2o_prd.qna
//                             // WHERE iceId = ? AND entityId = ? AND entityType = ?`;

//                             //     const qnaQueryParams = [iceId, entityId, entityType.toLowerCase()];

//                             //     // Meeting stats query (parameterized based on regType)
//                             //     let meetingStatQuery = '';
//                             //     let meetingParams = [iceId, entityType, entityId, entityType, entityId];

//                             //     meetingStatQuery = `
//                             // SELECT
//                             //     SUM(CASE WHEN requestStatus = 'draft' THEN 1 ELSE 0 END) AS draftCount,
//                             //     SUM(CASE WHEN requestStatus = 'requested' THEN 1 ELSE 0 END) AS requestedCount,
//                             //     SUM(CASE WHEN requestStatus = 'confirmed' THEN 1 ELSE 0 END) AS confirmedCount,
//                             //     SUM(CASE WHEN isCreatedByAI = 1 THEN 1 ELSE 0 END) AS aiMatchCount
//                             // FROM e2m_o2o_prd.meeting
//                             // WHERE iceId = ?
//                             //     AND (
//                             //     (requestorType = ? AND requestorTypeEntityId = ?) OR
//                             //     (inviteeType = ? AND inviteeTypeEntityId = ?)
//                             //     )`;


//                             //     // Fetch both in parallel
//                             //     const [qnaResult, meetingResult] = await Promise.all([
//                             //         mysql.executeQuery(qnaCountQuery, qnaQueryParams),
//                             //         mysql.executeQuery(meetingStatQuery, meetingParams),
//                             //     ]);

//                             //     // Populate stats in response
//                             //     ret_val.stat = {
//                             //         QnA: qnaResult[0]?.totalQnA || 0,
//                             //         Meetings: {
//                             //             Draft: meetingResult[0]?.draftCount || 0,
//                             //             Requested: meetingResult[0]?.requestedCount || 0,
//                             //             confirmedCount: meetingResult[0]?.confirmedCount || 0,
//                             //             AIMatched: meetingResult[0]?.aiMatchCount || 0
//                             //         },
//                             //         AIMatched: meetingResult[0]?.aiMatchCount || 0,
//                             //     };

//                             //     ret_val.stat = {
//                             //         QnA: 0,
//                             //         Meetings: {
//                             //             Draft: 0,
//                             //             Requested: 0,
//                             //             confirmedCount: 0,
//                             //             AIMatched: 0
//                             //         },
//                             //         AIMatched: 0,
//                             //     };
//                             // Fetch QnA count (DISTINCT questionId)
//                             const { data: qnaData, error: qnaError } = await supabase
//                                 .from('qna')
//                                 .select('questionId', { count: 'exact', head: false }) // `head: false` returns rows
//                                 .match({
//                                     "iceId": iceId,
//                                     "entityId": entityId,
//                                     "entityType": entityType.toLowerCase()
//                                 });

//                             const uniqueQnAs = new Set((qnaData || []).map(q => q.questionId)).size;

//                             // Fetch meeting data
//                             const { data: meetingData, error: meetingError } = await supabase
//                                 .from('meeting')
//                                 .select('"requestStatus", "isCreatedByAI"')
//                                 .eq('iceId', iceId)
//                                 .or(`and("requestorType".eq.${entityType},"requestorTypeEntityId".eq.${entityId}),
//                                 and("inviteeType".eq.${entityType},"inviteeTypeEntityId".eq.${entityId})`);

//                             // Aggregate meeting stats
//                             const meetingStats = {
//                                 draftCount: 0,
//                                 requestedCount: 0,
//                                 confirmedCount: 0,
//                                 aiMatchCount: 0,
//                             };

//                             for (const meeting of meetingData || []) {
//                                 if (meeting.requestStatus === 'draft') meetingStats.draftCount++;
//                                 if (meeting.requestStatus === 'requested') meetingStats.requestedCount++;
//                                 if (meeting.requestStatus === 'confirmed') meetingStats.confirmedCount++;
//                                 if (meeting.isCreatedByAI) meetingStats.aiMatchCount++;
//                             }

//                             // Populate stats in response
//                             ret_val.stat = {
//                                 QnA: uniqueQnAs,
//                                 Meetings: {
//                                     Draft: meetingStats.draftCount,
//                                     Requested: meetingStats.requestedCount,
//                                     confirmedCount: meetingStats.confirmedCount,
//                                     AIMatched: meetingStats.aiMatchCount
//                                 },
//                                 AIMatched: meetingStats.aiMatchCount
//                             };
//                         }
//                         ret_val.status = 0;
//                         ret_val.result = _fields(entityData, payload.data.fields);
//                         ret_val.MIN_REQUESTS_BATCH1 = config.SPONSOR_MIN_REQUESTS_BATCH1 || 20;
//                         ret_val.MIN_REQUESTS_BATCH2 = config.SPONSOR_MIN_REQUESTS_BATCH2 || 0;
//                         ret_val.BATCH_MEETING_REQUESTS = config.BATCH_MEETING_REQUESTS_SUBMIT_ENABLED || false;
//                     }
//                     else if (entityType == 'Speaker') {
//                         ret_val.status = 0;
//                         ret_val.result = _fields(entityData, payload.data.fields);
//                     }
//                     else if (entityType == 'Session') {
//                         ret_val.status = 0;
//                         ret_val.result = _fields(entityData, payload.data.fields);
//                     }
//                 }
//             }
//             // console.log(ret_val)
//             // Final response
//             resolve(ret_val);
//         } catch (err) {
//             console.error(err);
//             reject(ERRCODE.UNKNOWN_ERROR);
//         }
//     });
// }

async function user_info(payload) {
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


            // Helper function to handle cache operations safely
            const safeRemoveFromCache = (cacheKey) => {
                if (payload.data.isLocalEnvironment) {
                    console.log(`[LOCAL] Skipping cache removal for: ${cacheKey}`);
                    return;
                }
                try {
                    if (typeof cm !== 'undefined' && cm.removeFromCache) {
                        cm.removeFromCache({ cacheKey: cacheKey });
                    }
                } catch (error) {
                    console.log(`[ERROR] Cache removal failed for ${cacheKey}:`, error.message);
                }
            };

            // Helper function to handle cache-or-db operations safely
            const safeGetFromCacheOrDb = async (cacheKey, dbCallback) => {
                if (payload.data.isLocalEnvironment) {
                    console.log(`[LOCAL] Bypassing cache, fetching directly from DB for: ${cacheKey}`);
                    return await dbCallback();
                }

                try {
                    if (typeof _get_from_cache_or_db !== 'undefined') {
                        return await _get_from_cache_or_db(cacheKey, dbCallback);
                    } else {
                        console.log(`[FALLBACK] Cache function not available, fetching directly from DB for: ${cacheKey}`);
                        return await dbCallback();
                    }
                } catch (error) {
                    console.log(`[ERROR] Cache operation failed for ${cacheKey}:`, error.message);
                    console.log(`[FALLBACK] Fetching directly from DB`);
                    return await dbCallback();
                }
            };

            if (entityId) {
                let entityType = (payload.data.attendeeType) ? payload.data.attendeeType : 'Attendee'
                console.log("entityId", entityId);
                let entityCollectionPath = `${eventBasePath}/${entityType}List/${entityType}s`

                // Generate cache key based on id or email
                const cacheKey = `${iceId}/${entityType}/${entityId}`;
                console.log("cacheKey", cacheKey);

                let clearCache = payload.data?.clearCache !== false; // ensure it implicitly true
                if (clearCache) {
                    safeRemoveFromCache(cacheKey);
                }

                // Fetch user data from cache or Firestore
                const entityData = await safeGetFromCacheOrDb(cacheKey, async () => {
                    if (entityId) {
                        console.log(`[DB] Fetching entity data from: ${entityCollectionPath}/${entityId}`);
                        const doc = await dbClient.collection(entityCollectionPath).doc(entityId).get();
                        return doc.exists ? doc.data() : null;
                    }
                });

                let sponsorData = null;
                if (entityData) {
                    let eventInfo = await dbClient.collection(eventBasePath).doc("EventInfo").get();
                    let eventData = eventInfo.exists ? eventInfo.data() : {};
                    if (entityType == 'Attendee') {
                        let regType = entityData?.RegistrationType?.RegistrationType;
                        let regTypeEntityId = entityData?.RegistrationType?.RegistrationTypeEntityId;
                        console.log("regType", regType);
                        console.log("regTypeEntityId", regTypeEntityId);

                        if (regType === "Sponsor") {
                            let attendeeEntityCollectionPath = `${eventBasePath}/${regType}List/${regType}s`;
                            const attendeeEntityTypeCacheKey = `${iceId}/${regType}/${regTypeEntityId}`;
                            console.log("attendeeEntityTypeCacheKey", attendeeEntityTypeCacheKey);

                            if (clearCache) {
                                safeRemoveFromCache(attendeeEntityTypeCacheKey);
                            }

                            sponsorData = await safeGetFromCacheOrDb(attendeeEntityTypeCacheKey, async () => {
                                if (regTypeEntityId) {
                                    console.log(`[DB] Fetching sponsor data from: ${attendeeEntityCollectionPath}/${regTypeEntityId}`);
                                    const doc = await dbClient.collection(attendeeEntityCollectionPath).doc(regTypeEntityId).get();
                                    return doc.exists ? doc.data() : null;
                                }
                            });
                        }

                        // If stats are requested
                        if (payload.data?.includeStat) {
                            // CREATE CACHE KEY FOR MEETING STATS
                            const statsEntityId = regType === 'Sponsor' ? regTypeEntityId : entityId;
                            const statsEntityType = regType === 'Sponsor' ? regType : entityType;
                            const statsCacheKey = `${iceId}/stats/${statsEntityType}/${statsEntityId}`;

                            if (clearCache) {
                                safeRemoveFromCache(statsCacheKey);
                            }

                            // WRAP STATS QUERIES IN CACHE
                            const statsData = await safeGetFromCacheOrDb(statsCacheKey, async () => {
                                console.log(`[DB] Fetching stats data for: ${statsEntityType}/${statsEntityId}`);
                                console.log(`[DEBUG] regType: ${regType}, entityType: ${entityType}, entityId: ${entityId}, regTypeEntityId: ${regTypeEntityId}`);

                                // --- Prepare filters based on regType ---
                                let qnaFilter = {};
                                let meetingOrFilter = '';

                                if (regType === 'Sponsor') {
                                    qnaFilter = {
                                        "iceId": iceId,
                                        "entityId": regTypeEntityId,
                                        "entityType": regType.toLowerCase()
                                    };

                                    meetingOrFilter = `or=(and("requestorType".eq."${entityType}","requestorTypeEntityId".eq."${regTypeEntityId}"),and("inviteeType".eq."${entityType}","inviteeTypeEntityId".eq."${regTypeEntityId}"))`;
                                } else {
                                    // Default to 'attendee' if regType is undefined or null
                                    const actualRegType = regType || 'attendee';
                                    qnaFilter = {
                                        "iceId": iceId,
                                        "entityId": entityId,
                                        "entityType": actualRegType.toLowerCase()
                                    };

                                    meetingOrFilter = `or=(and("requestorType".eq."${entityType}","requestorId".eq."${entityId}"),and("inviteeType".eq."${entityType}","inviteeId".eq."${entityId}"))`;
                                }

                                console.log(`[DEBUG] meetingOrFilter: ${meetingOrFilter}`);
                                console.log(`[DEBUG] qnaFilter:`, qnaFilter);

                                // --- QnA Count Query (Distinct Question IDs) ---
                                const { data: qnaData, error: qnaError } = await supabase
                                    .from('qna')
                                    .select('questionId')
                                    .match(qnaFilter);

                                if (qnaError) {
                                    console.error('QnA query error:', qnaError);
                                }

                                const qnaSet = new Set((qnaData || []).map(q => q.questionId));
                                const totalQnA = qnaSet.size;

                                // --- Meeting Stats Query ---
                                let meetingQuery;
                                if (regType === 'Sponsor') {
                                    meetingQuery = supabase
                                        .from('meeting')
                                        .select('"requestStatus", "isCreatedByAI"')
                                        .eq('iceId', iceId)
                                        .or(`and("requestorType".eq."${entityType.toLowerCase()}","requestorTypeEntityId".eq."${regTypeEntityId}"),and("inviteeType".eq."${entityType.toLowerCase()}","inviteeTypeEntityId".eq."${regTypeEntityId}")`);
                                } else {
                                    meetingQuery = supabase
                                        .from('meeting')
                                        .select('"requestStatus", "isCreatedByAI"')
                                        .eq('iceId', iceId)
                                        .or(`and("requestorType".eq."${entityType.toLowerCase()}","requestorId".eq."${entityId}"),and("inviteeType".eq."${entityType.toLowerCase()}","inviteeId".eq."${entityId}")`);
                                }

                                const { data: meetingData, error: meetingError } = await meetingQuery;

                                if (meetingError) {
                                    console.error('Meeting query error:', meetingError);
                                }

                                console.log(`[DEBUG] Meeting data:`, meetingData);
                                console.log(`[DEBUG] Meeting count:`, meetingData?.length || 0);

                                // --- Aggregate Meeting Stats ---
                                const meetingStats = {
                                    draftCount: 0,
                                    requestedCount: 0,
                                    confirmedCount: 0,
                                    aiMatchCount: 0,
                                };

                                for (const row of meetingData || []) {
                                    console.log(`[DEBUG] Processing meeting:`, row);
                                    const status = (row.requestStatus || '').toLowerCase();
                                    if (status === 'draft') meetingStats.draftCount++;
                                    if (status === 'requested') meetingStats.requestedCount++;
                                    if (status === 'confirmed') meetingStats.confirmedCount++;
                                    if (row.isCreatedByAI) meetingStats.aiMatchCount++;
                                }

                                console.log(`[DEBUG] Final meeting stats:`, meetingStats);

                                // Return the stats data to be cached
                                return {
                                    QnA: totalQnA,
                                    Meetings: {
                                        Draft: meetingStats.draftCount,
                                        Requested: meetingStats.requestedCount,
                                        confirmedCount: meetingStats.confirmedCount,
                                        AIMatched: meetingStats.aiMatchCount,
                                    },
                                    AIMatched: meetingStats.aiMatchCount
                                };
                            });

                            // Use the cached/fresh stats data
                            ret_val.stat = statsData;
                        }

                        ret_val.status = 0;
                        ret_val.result = _fields(entityData, payload.data.fields);
                        // let SkipQNA = eventInfo.exists ? (eventInfo.data().SkipQNA ? eventInfo.data().SkipQNA : false) : false;
                        ret_val.result.SkipQNA = (entityData.SkipQNA !== undefined) ? entityData.SkipQNA : (eventData.SkipQNA || false);
                        if (sponsorData) {
                            ret_val.sponsor = sponsorData                            
                        }
                        ret_val.ATTENDEE_MIN_REQUESTS = eventData.ATTENDEE_MIN_REQUESTS || 0;
                            ret_val.MIN_REQUESTS_BATCH1 = eventData.SPONSOR_MIN_REQUESTS_BATCH1 || 0;
                            ret_val.MIN_REQUESTS_BATCH2 = eventData.SPONSOR_MIN_REQUESTS_BATCH2 || 0;
                            ret_val.BATCH_MEETING_REQUESTS = eventData.BATCH_MEETING_REQUESTS_SUBMIT_ENABLED == true;
                    } else if (entityType == 'Sponsor') {
                        if (payload.data?.includeStat) {
                            // CREATE CACHE KEY FOR SPONSOR STATS
                            const statsCacheKey = `${iceId}/stats/${entityType}/${entityId}`;

                            if (clearCache) {
                                safeRemoveFromCache(statsCacheKey);
                            }

                            // WRAP SPONSOR STATS QUERIES IN CACHE
                            const statsData = await safeGetFromCacheOrDb(statsCacheKey, async () => {
                                console.log(`[DB] Fetching sponsor stats for: ${entityType}/${entityId}`);

                                // Fetch QnA count (DISTINCT questionId)
                                const { data: qnaData, error: qnaError } = await supabase
                                    .from('qna')
                                    .select('questionId', { count: 'exact', head: false })
                                    .match({
                                        "iceId": iceId,
                                        "entityId": entityId,
                                        "entityType": entityType.toLowerCase()
                                    });

                                if (qnaError) {
                                    console.error('Sponsor QnA query error:', qnaError);
                                }

                                const uniqueQnAs = new Set((qnaData || []).map(q => q.questionId)).size;

                                // Fetch meeting data
                                const { data: meetingData, error: meetingError } = await supabase
                                    .from('meeting')
                                    .select('"requestStatus", "isCreatedByAI"')
                                    .eq('iceId', iceId)
                                    .or(`and("requestorType".eq.${entityType},"requestorTypeEntityId".eq.${entityId}),and("inviteeType".eq.${entityType},"inviteeTypeEntityId".eq.${entityId})`);

                                if (meetingError) {
                                    console.error('Sponsor meeting query error:', meetingError);
                                }

                                // Aggregate meeting stats
                                const meetingStats = {
                                    draftCount: 0,
                                    requestedCount: 0,
                                    confirmedCount: 0,
                                    aiMatchCount: 0,
                                };

                                for (const meeting of meetingData || []) {
                                    const status = (meeting.requestStatus || '').toLowerCase();
                                    if (status === 'draft') meetingStats.draftCount++;
                                    if (status === 'requested') meetingStats.requestedCount++;
                                    if (status === 'confirmed') meetingStats.confirmedCount++;
                                    if (meeting.isCreatedByAI) meetingStats.aiMatchCount++;
                                }

                                // Return the stats data to be cached
                                return {
                                    QnA: uniqueQnAs,
                                    Meetings: {
                                        Draft: meetingStats.draftCount,
                                        Requested: meetingStats.requestedCount,
                                        confirmedCount: meetingStats.confirmedCount,
                                        AIMatched: meetingStats.aiMatchCount
                                    },
                                    AIMatched: meetingStats.aiMatchCount
                                };
                            });

                            // Use the cached/fresh stats data
                            ret_val.stat = statsData;
                        }

                        ret_val.status = 0;
                        ret_val.result = _fields(entityData, payload.data.fields);
                        ret_val.result.SkipQNA = (entityData.SkipQNA !== undefined) ? entityData.SkipQNA : (eventData.SkipQNA || false);
                        ret_val.ATTENDEE_MIN_REQUESTS = eventData.ATTENDEE_MIN_REQUESTS || 0;
                        ret_val.MIN_REQUESTS_BATCH1 = eventData.SPONSOR_MIN_REQUESTS_BATCH1 || 0;
                        ret_val.MIN_REQUESTS_BATCH2 = eventData.SPONSOR_MIN_REQUESTS_BATCH2 || 0;
                        ret_val.BATCH_MEETING_REQUESTS = eventData.BATCH_MEETING_REQUESTS_SUBMIT_ENABLED == true;
                    } else if (entityType == 'Speaker') {
                        ret_val.status = 0;
                        ret_val.result = _fields(entityData, payload.data.fields);
                        ret_val.result.SkipQNA = (entityData.SkipQNA !== undefined) ? entityData.SkipQNA : (eventData.SkipQNA || false);
                        ret_val.ATTENDEE_MIN_REQUESTS = eventData.ATTENDEE_MIN_REQUESTS || 0;
                        ret_val.MIN_REQUESTS_BATCH1 = eventData.SPONSOR_MIN_REQUESTS_BATCH1 || 0;
                        ret_val.MIN_REQUESTS_BATCH2 = eventData.SPONSOR_MIN_REQUESTS_BATCH2 || 0;
                        ret_val.BATCH_MEETING_REQUESTS = eventData.BATCH_MEETING_REQUESTS_SUBMIT_ENABLED == true;
                    }
                    else if (entityType == 'Session') {
                        ret_val.status = 0;
                        ret_val.result = _fields(entityData, payload.data.fields);
                    }
                } else {
                    console.log('[WARNING] No entity data found');
                }
            } else {
                console.log('[WARNING] No entityId provided');
            }

            // Final response
            resolve(ret_val);
        } catch (err) {
            console.error('[ERROR] Function execution failed:', err);
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

            const allowedUserFields = ["FirstName", "LastName", "Name", "Address", "Company", "Designation", "Phone", "isHiddenFromChat", "preferredSlots", "ProfilePictureURL"];
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

            // // If preferredSlots was updated, sync with MySQL
            // if ('preferredSlots' in sanitizedUserUpdates) {
            //     const preferredSlots = sanitizedUserUpdates.preferredSlots || [];
            //     const updateSql = `
            //         INSERT INTO e2m_o2o_prd.slots (attendeeId, slots)
            //         VALUES (?, ?)
            //         ON DUPLICATE KEY UPDATE slots = ?
            //     `;
            //     await mysql.execute(updateSql, [pa.UserId, JSON.stringify(preferredSlots), , JSON.stringify(preferredSlots)]);
            // }

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

// async function get_sponsors_meeting(payload) {
//     const { key, data } = payload;
//     const { instanceId, clientId, eventId } = key;

//     const allSponsors = [];
//     const confirmedMeetings = [];
//     const groupedData = {};
//     const pathPrefix = `${instanceId}_${clientId}${eventId}`;
//     const sponsorsRef = dbClient.collection(pathPrefix).doc("SponsorList").collection("Sponsors");
//     const meetingsRef = dbClient.collection(pathPrefix).doc("MeetingList").collection("Meetings");

//     try {
//         // 1️⃣ Fetch sponsor(s)
//         let sponsorSnapshot;

//         if (data?.sponsor) {
//             const docRef = sponsorsRef.doc(data.sponsor);
//             const doc = await docRef.get();
//             if (doc.exists) {
//                 sponsorSnapshot = [doc]; // Mimic forEach compatibility
//             } else {
//                 sponsorSnapshot = [];
//             }
//         } else {
//             const querySnapshot = await sponsorsRef.where("IsPublished", "==", true).get();
//             sponsorSnapshot = querySnapshot.empty ? [] : querySnapshot.docs;
//         }

//         if (sponsorSnapshot.length > 0) {
//             sponsorSnapshot.forEach(doc => {
//                 const sponsorData = {
//                     eventId: eventId,
//                     sponsorId: doc.id,
//                     ...doc.data(),
//                     requestData: {
//                         RequestorId: data.RequestorId,
//                         InviteeIds: data.InviteeIds,
//                         Message: data.Message,
//                         Timezone: data.Timezone
//                     }
//                 };
//                 allSponsors.push(sponsorData);
//                 groupedData[sponsorData.sponsorId] = {
//                     sponsor: sponsorData,
//                     meetings: []
//                 };
//             });
//         }

//         // 2️⃣ Fetch confirmed meetings
//         const meetingSnapshot = await meetingsRef.where("Status", "==", "Confirmed").get();

//         if (!meetingSnapshot.empty) {
//             meetingSnapshot.forEach(doc => {
//                 const meetingData = {
//                     eventId: eventId,
//                     meetingId: doc.id,
//                     ...doc.data()
//                 };
//                 confirmedMeetings.push(meetingData);

//                 const sponsorId = meetingData.SponsorId;
//                 if (groupedData[sponsorId]) {
//                     groupedData[sponsorId].meetings.push(meetingData);
//                 }
//             });

//             // 3️⃣ Generate PDF
//             const doc = new PDFDocument({ margin: 50 });
//             const passStream = new PassThrough();
//             doc.pipe(passStream);

//             doc.fontSize(18).text(`Sponsor Wise Confirmed Meetings 1`, { align: 'center' });
//             doc.moveDown();

//             let hasData = false;

//             for (const sponsorId in groupedData) {
//                 const { sponsor, meetings } = groupedData[sponsorId];
//                 if (meetings.length > 0) {
//                     hasData = true;
//                     doc.fontSize(14).font('Helvetica-Bold').fillColor('black').text(`${sponsor.Name}`);
//                     doc.moveDown(0.5);

//                     // Sort meetings by time slot before displaying
//                     meetings.sort((a, b) => {
//                         const timeA = a.Slots && a.Slots.length > 0 ? new Date(a.Slots[0]) : new Date(0);
//                         const timeB = b.Slots && b.Slots.length > 0 ? new Date(b.Slots[0]) : new Date(0);
//                         return timeA - timeB;
//                     });

//                     meetings.forEach(meeting => {
//                         const slot = meeting.Slots?.[0]
//                             ? moment.tz(meeting.Slots[0], 'UTC').tz("Europe/London").format("MMM DD, YYYY hh:mm A")
//                             : "N/A";

//                         const requestor = meeting.Requestor || {};
//                         const invitee = meeting.Invitee || {};
//                         doc.x = 50;
//                         doc.fontSize(12).fillColor('blue').text(`Time Slot: ${slot}`);
//                         doc.moveDown(0.3);

//                         const boxWidth = 250, boxHeight = 100, startX = doc.x, startY = doc.y;

//                         doc.rect(startX, startY, boxWidth, boxHeight).stroke();
//                         doc.fontSize(10).font('Helvetica-Bold').fillColor('black');
//                         doc.text('Requestor', startX + (boxWidth - doc.widthOfString('Requestor')) / 2, startY + 5);
//                         doc.font('Helvetica');
//                         doc.text(`Name: ${requestor.Name || 'Unknown'}`, startX + 5, startY + 20);
//                         doc.text(`Company: ${requestor.Company || 'N/A'}`, startX + 5, startY + 35);
//                         doc.text(`Designation: ${requestor.Designation || 'N/A'}`, startX + 5, startY + 50);
//                         doc.text(`Email: ${requestor.Email || 'N/A'}`, startX + 5, startY + 65);
//                         doc.text(`Phone: ${requestor.Phone || 'N/A'}`, startX + 5, startY + 80);

//                         const inviteeX = startX + boxWidth + 20;
//                         doc.rect(inviteeX, startY, boxWidth, boxHeight).stroke();
//                         doc.fontSize(10).font('Helvetica-Bold').fillColor('black');
//                         doc.text('Invitee', inviteeX + (boxWidth - doc.widthOfString('Invitee')) / 2, startY + 5);
//                         doc.font('Helvetica');
//                         doc.text(`Name: ${invitee.Name || 'Unknown'}`, inviteeX + 5, startY + 20);
//                         doc.text(`Company: ${invitee.Company || 'N/A'}`, inviteeX + 5, startY + 35);
//                         doc.text(`Designation: ${invitee.Designation || 'N/A'}`, inviteeX + 5, startY + 50);
//                         doc.text(`Email: ${invitee.Email || 'N/A'}`, inviteeX + 5, startY + 65);
//                         doc.text(`Phone: ${invitee.Phone || 'N/A'}`, inviteeX + 5, startY + 80);

//                         doc.moveDown(7);
//                     });

//                     const sponsorIds = Object.keys(groupedData).filter(id => groupedData[id].meetings.length > 0);
//                     if (sponsorId !== sponsorIds[sponsorIds.length - 1]) {
//                         doc.addPage();
//                     }
//                 }
//             }

//             if (!hasData) {
//                 doc.fontSize(14).text("No sponsors with confirmed meetings available.");
//             }

//             doc.end();

//             // 4️⃣ Get buffer from passThrough stream
//             const buffer = await getStream.buffer(passStream);

//             // 5️⃣ Upload PDF to Firebase Storage
//             const bucket = storageClient.bucket(bucketName);
//             const fileName = `meeting-reports/${pathPrefix}_${Date.now()}.pdf`;
//             const file = bucket.file(fileName);

//             await file.save(buffer, {
//                 metadata: {
//                     contentType: 'application/pdf'
//                 },
//                 public: true // Set to public to overwrite the existing file
//             });

//             // 6️⃣ Get public URL
//             const [url] = await file.getSignedUrl({
//                 action: 'read',
//                 expires: '03-09-2491' // Far future date
//             });

//             return {
//                 pdfUrl: url,
//                 status: 0,
//                 message: "PDF generated and uploaded successfully"
//             };
//         }

//         return {
//             allSponsors,
//             confirmedMeetings,
//             groupedData,
//             status: 1,
//             message: "No confirmed meetings. No PDF generated."
//         };

//     } catch (error) {
//         console.error(`Error in get_sponsors_meeting:`, error);
//         return {
//             status: -1,
//             message: "Failed to generate meeting report",
//             error: error.message
//         };
//     }
// }

async function get_sponsors_meeting(payload) {
    const { key, data } = payload;
    const { instanceId, clientId, eventId } = key;

    const allSponsors = [];
    const confirmedMeetings = [];
    const groupedData = {};
    const pathPrefix = `${instanceId}_${clientId}${eventId}`;
    const sponsorsRef = dbClient.collection(pathPrefix).doc("SponsorList").collection("Sponsors");
    const meetingsRef = dbClient.collection(pathPrefix).doc("MeetingList").collection("Meetings");
    const eventInfoRef = dbClient.collection(pathPrefix).doc("EventInfo");
    const eventDoc = await eventInfoRef.get();
    const eventName = eventDoc.data().EventFullName;
    console.log("eventName", eventName);
    const now = moment().tz("Europe/London");
    const formattedDateTime = now.format("MMM DD, YYYY hh:mm A");

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
                        RequestorId: data?.RequestorId,
                        InviteeIds: data?.InviteeIds,
                        Message: data?.Message,
                        Timezone: data?.Timezone
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

        // if (!meetingSnapshot.empty) {
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
        const passStream = new PassThrough();
        doc.pipe(passStream);

        // Event name - Darker gray with slightly increased size
        doc.fontSize(20)  // Increased from 10 to 12
            .font('Helvetica-Bold')
            .fillColor('#000000')  // Darker gray (75% black instead of 60%)
            .text(`${eventName}`, { align: 'center' });
        doc.moveDown(0.3);

        // Main title - Bold and prominent
        doc.fontSize(16)
            .font('Helvetica')
            .fillColor('#444444')  // Pure black
            .text('Sponsor Wise Confirmed Meetings', { align: 'center' });

        // Add small space between titles
        doc.moveDown(2);  // Reduced from 1 to bring them slightly closer

        // doc.fontSize(10)
        //     .font('Helvetica-Oblique')  // Italic
        //     .fillColor('#666666')  // Medium gray
        //     .text(`Time: ${formattedDateTime}`, { align: 'center' });

        // // Space after header section
        // doc.moveDown(6);  // Increased from 1 to 1.2 for better separation

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

                // Inside the meetings.forEach loop, replace the current implementation with:

                meetings.forEach((meeting, index) => {
                    const slot = meeting.Slots?.[0]
                        ? moment.tz(meeting.Slots[0], 'UTC').tz("Europe/London").format("MMM DD, YYYY hh:mm A")
                        : "N/A";

                    const requestor = meeting.Requestor || {};
                    const invitee = meeting.Invitee || {};

                    // Check if we need a new page before adding this meeting
                    if (doc.y + 150 > doc.page.height - 50) {
                        doc.addPage();
                    }

                    doc.x = 50;

                    // Time Slot - improved styling
                    doc.font('Helvetica')
                        .fontSize(12)
                        .fillColor('#1565C0')
                        .text(`Time Slot: ${slot}`);
                    doc.moveDown(0.5);

                    const boxWidth = 250, boxHeight = 110, startX = doc.x, startY = doc.y;

                    // Requestor Box
                    doc.rect(startX, startY, boxWidth, boxHeight)
                        .lineWidth(0.5)
                        .stroke('#333333');

                    doc.fontSize(10)
                        .font('Helvetica-Bold')
                        .fillColor('#333333')
                        .text('REQUESTOR', startX + (boxWidth - doc.widthOfString('REQUESTOR')) / 2, startY + 5);

                    let companyRequester = requestor.Company || 'N/A';
                    if (companyRequester.length > 30) {
                        companyRequester = companyRequester.substring(0, 30) + '...';
                    } else {
                        companyRequester = companyRequester;
                    }
                    // Content - Smaller font (8pt) for Designation only
                    doc.font('Helvetica')
                        .fillColor('#444444')
                        .text(`Name: ${requestor.Name || 'N/A'}`, startX + 10, startY + 25)
                        .text(`Company: ${companyRequester || 'N/A'}`, startX + 10, startY + 40);

                    let designationRequestor;

                    if (requestor.Designation.length > 30) {
                        designationRequestor = requestor.Designation.substring(0, 30) + '...';
                    } else {
                        designationRequestor = requestor.Designation;
                    }
                    // Smaller font for Designation
                    doc.fontSize(10)  // Reduced from 10 to 8
                        .text(`Designation: ${designationRequestor || 'N/A'}`, startX + 10, startY + 55);

                    // Back to normal font size
                    doc.fontSize(10)
                        .text(`Email: ${requestor.Email || 'N/A'}`, startX + 10, startY + 70)
                        .text(`Phone: ${requestor.Phone || 'N/A'}`, startX + 10, startY + 85);

                    // Invitee Box
                    const inviteeX = startX + boxWidth + 20;
                    doc.rect(inviteeX, startY, boxWidth, boxHeight)
                        .lineWidth(0.5)
                        .stroke('#333333');

                    doc.fontSize(10)
                        .font('Helvetica-Bold')
                        .fillColor('#333333')
                        .text('INVITEE', inviteeX + (boxWidth - doc.widthOfString('INVITEE')) / 2, startY + 5);

                    let companyInvitee = invitee.Company || 'N/A';
                    if (companyInvitee.length > 30) {
                        companyInvitee = companyInvitee.substring(0, 30) + '...';
                    } else {
                        companyInvitee = companyInvitee;
                    }
                    // Content - Smaller font (8pt) for Designation only
                    doc.font('Helvetica')
                        .fillColor('#444444')
                        .text(`Name: ${invitee.Name || 'N/A'}`, inviteeX + 10, startY + 25)
                        .text(`Company: ${companyInvitee || 'N/A'}`, inviteeX + 10, startY + 40);

                    let designationInvitee;

                    if (invitee.Designation.length > 30) {
                        designationInvitee = invitee.Designation.substring(0, 30) + '...';
                    } else {
                        designationInvitee = invitee.Designation;
                    }
                    // Smaller font for Designation
                    doc.fontSize(10)  // Reduced from 10 to 8
                        .text(`Designation: ${designationInvitee || 'N/A'}`, inviteeX + 10, startY + 55);

                    // Back to normal font size
                    doc.fontSize(10)
                        .text(`Email: ${invitee.Email || 'N/A'}`, inviteeX + 10, startY + 70)
                        .text(`Phone: ${invitee.Phone || 'N/A'}`, inviteeX + 10, startY + 85);

                    doc.moveDown(2);
                });

                // Remove the current page break logic and replace with:
                if (doc.y + 100 > doc.page.height - 50) {
                    doc.addPage();
                }

                const sponsorIds = Object.keys(groupedData).filter(id => groupedData[id].meetings.length > 0);
                if (sponsorId !== sponsorIds[sponsorIds.length - 1]) {
                    doc.addPage();
                }
            }
        }

        if (!hasData) {
            doc.fontSize(14).text("No confirmed meetings available.", { align: 'center' });
        }

        doc.end();

        // 4️⃣ Get buffer from passThrough stream
        const buffer = await getStream.buffer(passStream);

        // 5️⃣ Upload PDF to Firebase Storage
        const bucket = storageClient.bucket(bucketName);

        const sponsorId = Object.keys(groupedData)[0]; // Assuming you want to create a report for the first sponsor
        const sponsorName = groupedData[sponsorId]?.sponsor?.Name || 'UnknownSponsor';
        const sponsorWithoutSpace = sponsorName.replace(/\s+/g, '_');
        const fileName = `${pathPrefix}/MeetingReports/SponsorMeetingReport.pdf`;
        const file = bucket.file(fileName);

        try {
            await file.delete();
            console.log(`Deleted existing file: ${fileName}`);
        } catch (error) {
            if (error.code !== 404) { // Ignore "not found" errors
                console.error(`Error deleting existing file:`, error);
                throw error;
            }
        }

        // Upload the new file
        await file.save(buffer, {
            metadata: {
                contentType: 'application/pdf',
                cacheControl: 'no-cache, max-age=0' // Prevent caching
            }
        });

        // 6️⃣ Get public URL
        const [url] = await file.getSignedUrl({
            action: 'read',
            expires: '03-09-2491' // Far future date
        });

        const freshUrl = `${url}&t=${Date.now()}`;

        return {
            pdfUrl: freshUrl,
            status: 0,
            message: "PDF generated and uploaded successfully"
        };
        // }

        // return {
        //     allSponsors,
        //     confirmedMeetings,
        //     groupedData,
        //     status: 1,
        //     message: "No confirmed meetings. No PDF generated."
        // };

    } catch (error) {
        console.error(`Error in get_sponsors_meeting:`, error);
        return {
            status: -1,
            message: "Failed to generate meeting report",
            error: error.message
        };
    }
}

async function scanned_dashboard_analysis(payload) {
    try {
        const { key } = payload;
        const { instanceId, clientId, eventId } = key;
        const pathPrefix = `${instanceId}_${clientId}${eventId}`;

        const attendeeRef = dbClient
            .collection(pathPrefix)
            .doc('AttendeeList')
            .collection('Attendees');

        const snapshot = await attendeeRef.get();

        if (snapshot.empty) {
            console.log("No Documents Found in this Event", pathPrefix);
            return {
                analytics: {
                    total_scan: 0,
                    active_sponsor_by_scan_count: 0,
                    unique_visitors: 0
                },
                top_sponsors: [],
                data: []
            };
        }

        const allAttendeeData = [];
        let vCardScannedCount = 0;

        snapshot.forEach((doc) => {
            const data = doc.data();
            const scannedList = data?.VCard?.Scanned || [];

            scannedList.forEach(scanEntry => {
                const scannedAttendees = scanEntry?.Attendee || [];
                vCardScannedCount += scannedAttendees.length;
            });

            allAttendeeData.push({
                id: doc.id,
                ...data
            });
        });

        const sponsorRef = dbClient
            .collection(pathPrefix)
            .doc('SponsorList')
            .collection('Sponsors');
        const sponsorSnapshot = await sponsorRef.get();

        if (sponsorSnapshot.empty) {
            console.log("No Sponsors Found in this Event", pathPrefix);
            return {
                analytics: {
                    total_scan: vCardScannedCount,
                    active_sponsor_by_scan_count: 0,
                    unique_visitors: 0
                },
                top_sponsors: [],
                data: []
            };
        }

        const active_sponsors = [];
        const top_sponsors = [];
        const data = [];

        for (const sponsorDoc of sponsorSnapshot.docs) {
            const sponsorData = sponsorDoc.data();
            const sponsorId = sponsorData?.SponsorId;
            const sponsorName = sponsorData?.Name || 'Unknown Sponsor';

            if (!sponsorId) {
                console.log(`Sponsor doc ${sponsorDoc.id} missing SponsorId, skipping.`);
                continue;
            }

            const matchingAttendees = allAttendeeData.filter(attendee => {
                const regType = attendee?.RegistrationType;
                const scannedList = attendee?.VCard?.Scanned || [];

                return (
                    regType?.RegistrationType === "Sponsor" &&
                    regType?.RegistrationTypeEntityId === sponsorId &&
                    scannedList.length > 0
                );
            });

            if (matchingAttendees.length > 0) {
                const totalScansBySponsor = matchingAttendees.reduce((sum, att) => {
                    const scannedList = att?.VCard?.Scanned || [];
                    return sum + scannedList.reduce((innerSum, entry) => {
                        return innerSum + 1;
                    }, 0);
                }, 0);

                active_sponsors.push({
                    sponsorId,
                    sponsorName,
                    scannedCount: totalScansBySponsor
                });

                top_sponsors.push({
                    [sponsorName]: totalScansBySponsor
                });

                matchingAttendees.forEach(att => {
                    const scannedList = att?.VCard?.Scanned || [];
                    scannedList.forEach(scannedPerson => {
                        data.push({
                            Name: scannedPerson.Name || null,
                            Designation: scannedPerson.Designation || null,
                            Email: scannedPerson.Email || null,
                            Phone: scannedPerson.Phone || null,
                            Company: scannedPerson.Company || null,
                            ScannedSponsorId: sponsorId,
                            ScannedSponsorName: sponsorName
                        });
                    });
                });
            }
        }

        // ✅ Calculate unique visitors based on unique emails in final `data` array
        const uniqueEmails = new Set(data.map(d => d.Email).filter(Boolean));

        return {
            analytics: {
                // total_scan: vCardScannedCount,
                total_scan: data.length,
                active_sponsor_by_scan_count: active_sponsors.length,
                unique_visitors: uniqueEmails.size
            },
            top_sponsors,
            data
        };

    } catch (error) {
        console.error("Error fetching scanned dashboard data:", error);
        throw error;
    }
}

module.exports = {
    attendeeList: attendee_list,
    userInfo: user_info,
    userUpdate: user_update,
    uploadFiles: upload_files,
    getSponsorsMeeting: get_sponsors_meeting,
    scannedDashboardAnalysis: scanned_dashboard_analysis
}