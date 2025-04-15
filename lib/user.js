'use strict';

const logger = require('./logger');
const config = require('../config.json');
const { Firestore } = require('@google-cloud/firestore');
const cm = require('./cache_manager');
const { ERRCODE } = require('./errcode');

const dbClient = new Firestore({
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
    userInfo: user_info
}