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

module.exports = {
    userInfo: user_info,
    userUpdate: user_update,
    uploadFiles: upload_files,
}