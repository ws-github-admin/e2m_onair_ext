'use strict';

const Redis = require('ioredis'),
    logger = require('./logger'),
    config = require('../config.json');

const DEFAULT_EXPIRY_SECONDS = 10800;

function store_in_cache(payload) {
    return new Promise(async(resolve, reject) => {
        let redis;
        let ret_val = {
            status: -1
        };

        try {
            const expirySeconds = (payload.expirySeconds || DEFAULT_EXPIRY_SECONDS);

            redis = _get_redis();
            await redis.set(payload.cacheKey, JSON.stringify(payload.cacheValueJson), 'EX', expirySeconds);
            redis.quit();

            ret_val.status = 0;
        } catch (err) {
            logger.logError('cache_manager.store_in_cache()', err);
            if (redis) {
                redis.quit();
            }
        }

        resolve(ret_val);
    });
}

function get_from_cache(payload) {
    return new Promise(async(resolve, reject) => {
        let redis;
        let ret_val = {};
        console.log(payload.cacheKey)
        try {
            redis = _get_redis();
            const result = await redis.get(payload.cacheKey);
            redis.quit();

            if (result) {
                console.log('cache hit ...');
                ret_val.cacheValueJson = JSON.parse(result);
            } else {
                console.log('cache miss ...');
            }
        } catch (err) {
            console.log('cache miss ...');
            logger.logError('cache_manager.get_from_cache()', err);
            if (redis) {
                redis.quit();
            }
        }

        resolve(ret_val);
    });
}

function remove_from_cache(payload) {
    return new Promise(async(resolve, reject) => {
        let redis;
        try {
            redis = _get_redis();
            const result = await redis.del(payload.cacheKey);
            redis.quit();

            resolve(result);
        } catch (err) {
            logger.logError('cache_manager.remove_from_cache()', err);
            if (redis) {
                redis.quit();
            }
            reject(err);
        }
    });
}
//
function _get_redis() {
    return new Redis({
        port: config.GCP.REDIS.PORT,
        host: config.GCP.REDIS.IP
    });
}
//--

module.exports = {
    storeInCache: store_in_cache,
    getFromCache: get_from_cache,
    removeFromCache: remove_from_cache
};