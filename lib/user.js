'use strict';

const logger = require('./logger');
const config = require('../config.json');
const { Firestore } = require('@google-cloud/firestore');
const { Storage } = require('@google-cloud/storage');
const cm = require('./cache_manager');
const { ERRCODE } = require('./errcode');
const validate = require("./validator");

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
    return new Promise(async (resolve, reject) => {
        let ret_val = { status: -1 };
        try {
            logger.log(payload);
            let pk = payload.key;
            let pd = payload.data || {};
            let userType = payload.data?.type || "Attendee";
            let instance_base_path = "/" + pk.instanceId;
            let event_base_path = pk.instanceId + "_" + pk.clientId + pk.eventId;
            let userCollectionPath = `${event_base_path}/${userType}List/${userType}s`;

            pd.fields = Array.isArray(pd.fields) ? pd.fields : [];

            if (!pd.id && !pd.Email) {
                reject(ERRCODE.PAYLOAD_ERROR);
                return;
            }

            let userData;

            if (pd.id) {
                const cacheKey = `${config.INSTANCE}/${event_base_path}/${userType}/${pd.id}`;
                userData = await _get_from_cache_or_db(cacheKey, async () => {
                    const doc = await dbClient.collection(userCollectionPath).doc(pd.id).get();
                    return doc.exists ? doc.data() : null;
                });
            } else if (pd.Email) {
                const cacheKey = `${config.INSTANCE}/${event_base_path}/${userType}/${pd.Email}`;
                userData = await _get_from_cache_or_db(cacheKey, async () => {
                    const userSnap = await dbClient
                        .collection(userCollectionPath)
                        .where("Email", "==", pd.Email)
                        .get();
                    return !userSnap.empty ? userSnap.docs[0].data() : null;
                });
            }

            if (!userData) {
                reject(ERRCODE.DATA_NOT_FOUND);
                return;
            }

            ret_val.status = 0;
            ret_val.result = _fields(userData, pd.fields);
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
            const event_base_path = pk.instanceId + "_" + pk.clientId + pk.eventId;

            const allowedUserFields = ["FirstName", "LastName", "Name", "Address", "Company", "Designation", "Phone", "isHiddenFromChat"];
            const allowedSponsorFields = ["Name", "Company", "Logo", "Category", "Profile", "Website", "isMeetingEnabled"];

            if (!pa.UserId && typeof userUpdates !== 'object') {
                reject(ERRCODE.PAYLOAD_ERROR);
                return;
            }

            const userPath = `${event_base_path}/AttendeeList/Attendees/${pa.UserId}`;
            const userDocRef = dbClient.doc(userPath);
            const userDoc = await userDocRef.get();

            if (!userDoc.exists) {
                reject(ERRCODE.DATA_NOT_FOUND);
                return;
            }

            const userData = userDoc.data();
            const userRegData = userData.RegistrationType;
            const userType = userRegData.RegistrationType;

            // Prepare filtered user update data
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

            // Update the user's own record
            if (Object.keys(sanitizedUserUpdates).length > 0) {
                batch.update(userDocRef, sanitizedUserUpdates);
            }

            // If sponsor updates are provided, update the sponsor record
            if (pd.sponsor && typeof sponsorUpdates === 'object') {
                const sponsorId = userRegData.RegistrationTypeEntityId;
                if (!sponsorId) {
                    reject(ERRCODE.PAYLOAD_ERROR);
                    return;
                }

                const sponsorPath = `${event_base_path}/SponsorList/Sponsors/${sponsorId}`;
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
            let bucketName = config.FIREBASE_CONFIG.storageBucket;
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

module.exports = {
    userInfo: user_info,
    userUpdate: user_update,
    uploadFiles: upload_files,
}