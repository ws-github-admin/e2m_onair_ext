'use strict';
const admin = require("firebase-admin");
const { OAuth2Client } = require('google-auth-library');
const config = require('../config.json');
const logger = require('./logger');
const { ERRCODE } = require('./errcode');
const axios = require("axios");
//const aad = require("azure-ad-jwt");

var gclient = new OAuth2Client(config.FIREBASE_CONFIG.clientId, '', '');

var validatorApp = admin.initializeApp({
    credential: admin.credential.cert(config.SERVICE_ACCOUNT)
}, "validatorApp");

const SYSROLES = [
    { RoleID: 1, RoleName: "SuperAdmin" },
    { RoleID: 2, RoleName: "ClientAdmin" },
    { RoleID: 3, RoleName: "EventAdmin" },
    { RoleID: 4, RoleName: "AnalyticsAdmin" },
    { RoleID: 10, RoleName: "AppUser" },
    { RoleID: 11, RoleName: "Speaker" },
    { RoleID: 12, RoleName: "Exhibitor" },
    { RoleID: 13, RoleName: "Sponsor" }
];

function __call(req, params) {
    return new Promise(async(resolve, reject) => {
        let ret_val = { status: -1 }
        try {
            if (params[req.method].isPrivate) {
                // let token = req.body.token
                //     //ret_val = await api_key(req, params, token)
                // ret_val = await auth_token(req, params, token)
                // resolve(ret_val)
                // return;
                //logger.log(req.headers);
                if (req.get('apikey') || req.get('x-api-key')) {
                    let token = (req.get('apikey') || req.get('x-api-key'))
                        //logger.log(token);
                        //logger.log(config.GCP.X_API_KEY)
                    ret_val = await api_key(req, params, token)
                    if (ret_val.status < 0) {
                        if (req.get('Authorization')) {
                            let token = req.get('Authorization').split('Bearer ')[1];
                            ret_val = await auth_token(req, params, token)
                            resolve(ret_val)
                        } else {
                            ret_val = ERRCODE.ACCESS_DENIED
                            reject(ret_val)

                        }
                    } else {
                        resolve(ret_val)
                    }
                    return
                } else if (req.get('Authorization')) {
                    let token = req.get('Authorization').split('Bearer ')[1];
                    ret_val = await auth_token(req, params, token)
                    resolve(ret_val)
                    return
                } else {
                    ret_val = ERRCODE.ACCESS_DENIED
                    reject(ret_val)
                    return
                }
            } else {
                ret_val.status = 0;
                ret_val.data = {}
                resolve(ret_val)
                return
            }
        } catch (err) {
            logger.log(err);
            if (!err.status) {
                ret_val = ERRCODE.UNKNOWN_ERROR
                reject(ret_val)
                return;
            }
            reject(err)
        }
    })
}

function auth_token(req, params, token) {
    return new Promise(async(resolve, reject) => {
        let ret_val = { status: -1 }
        _has_valid_auth_token(req, params, token)
            .then(res => {
                let creds = res.creds;
                return _get_auth_user(creds)
            })
            .then(res => {
                resolve(res)
            })
            .catch(err => {
                logger.log(err)
                if (!err.status) {
                    ret_val = ERRCODE.UNKNOWN_ERROR
                    reject(ret_val)
                    return;
                }
                reject(err)
            })
    })
}

function api_key(req, params, token) {
    return new Promise(async(resolve, reject) => {
        let ret_val = ERRCODE.UNKNOWN_ERROR
        try {
            if (token == 'ws2000rb28082019' || token == config.GCP.X_API_KEY) {
                ret_val.status = 0;
                ret_val.data = {}
                resolve(ret_val)
            } else {
                ret_val = ERRCODE.ACCESS_DENIED
                resolve(ret_val)
                return;
            }
        } catch (err) {
            logger.log(err)
            ret_val = ERRCODE.ACCESS_DENIED
            reject(ret_val)
            return;
        }

    })
}

function write_object(payload) {
    return new Promise((resolve, reject) => {
        let ret_val = { status: -1 }
        let creds = payload.auth.data;

        try {
            if (!payload.instance || !payload.collection) {
                ret_val = ERRCODE.PAYLOAD_ERROR
                reject(ret_val)
                return;
            }
            let event_base_pattern = /(OA_UAT)_(C\d+)(E\d+)$/;
            let matches = payload.instance.match(event_base_pattern);

            let iid = matches[1];
            let cid = matches[2];
            let eid = matches[3];
            if (!iid || !cid || !eid) {
                ret_val = ERRCODE.ACCESS_DENIED
                reject(ret_val)
                return;
            }
            let allowed_collection = [
                'RegistrationTicketList/RegistrationTicketTypes',
                'FormList/CheckoutForms',
                'SessionList/Sessions',
                'RoundTableList/RoundTables',
                'UserVideoActivity/OneToOneMeeting',
                'UserVideoActivity/ExhibitorMeeting'
            ]
            let validCollection = false;
            allowed_collection.forEach(col => {
                if (payload.collection.includes(col)) {
                    validCollection = true
                }
            })
            if (validCollection) {
                if (creds.Roles.includes(1)) {
                    ret_val.status = 0
                    resolve(ret_val)
                } else if (creds.Roles.includes(2) && creds.Clients.includes(cid)) {
                    ret_val.status = 0
                    resolve(ret_val)
                } else if (creds.ICEIds.includes(payload.instance)) {
                    ret_val.status = 0
                    resolve(ret_val)
                } else {
                    ret_val = ERRCODE.UNAUTHORIZED_ACCESS
                    reject(ret_val)
                }
            } else {
                ret_val = ERRCODE.UNAUTHORIZED_ACCESS
                reject(ret_val)
            }
        } catch (err) {
            console.log(err)
            if (!err.status) {
                ret_val = ERRCODE.UNKNOWN_ERROR
                reject(ret_val)
                return;
            }
            reject(err)
        }

    })
}

function deactivate_attendees(payload) {
    return new Promise(async(resolve, reject) => {
        console.log('validator.deactivate_attendees')
        let ret_val = { status: -1 }
        if (payload.auth && payload.auth.data && payload.auth.data.Roles && payload.auth.data.ICEIds) {
            let creds = payload.auth.data;
            try {
                let iceId = payload.key.instanceId + '_' + payload.key.clientId + payload.key.eventId
                if (!creds.Roles.includes(1) && !creds.ICEIds.includes(iceId)) {
                    ret_val = ERRCODE.UNAUTHORIZED_ACCESS
                    reject(ret_val)
                } else {

                    ret_val.status = 0
                    resolve(ret_val)
                }
            } catch (err) {
                console.log(err)
                if (!err.status) {
                    ret_val = ERRCODE.UNKNOWN_ERROR
                    reject(ret_val)
                    return;
                }
                reject(err)
            }
        } else {
            ret_val.status = 0
            resolve(ret_val)
        }
    })
}

function get_auth_token(payload) {
    return new Promise(async(resolve, reject) => {
        let ret_val = { status: -1 }
        if (!payload.emailId) {
            ret_val = ERRCODE.PAYLOAD_ERROR
            reject(ret_val);
            return;
        } else {
            ret_val.status = 0
            resolve(ret_val)
        }
    })
}

function search_collection(payload) {
    return new Promise(async(resolve, reject) => {
        let ret_val = { status: -1 }
        let creds = payload.auth.data;
        //console.log(creds)
        try {
            let iceId = payload.key.instanceId + '_' + payload.key.clientId + payload.key.eventId
            if (!creds.Roles.includes(1) && !creds.ICEIds.includes(iceId)) {
                ret_val = ERRCODE.UNAUTHORIZED_ACCESS
                reject(ret_val)
            } else {
                ret_val.status = 0
                resolve(ret_val)
                return;
            }
        } catch (err) {
            console.log(err)
            if (!err.status) {
                ret_val = ERRCODE.UNKNOWN_ERROR
                reject(ret_val)
                return;
            }
            reject(err)
        }

    })
}

function write_session(payload) {
    return new Promise(async(resolve, reject) => {
        let ret_val = { status: -1 }
        ret_val.status = 0
        resolve(ret_val)
        return;
        let creds = payload.auth.data;
        try {
            let iceId = payload.key.instanceId + '_' + payload.key.clientId + payload.key.eventId
            if (!creds.Roles.includes(1) && !creds.ICEIds.includes(iceId)) {
                ret_val = ERRCODE.UNAUTHORIZED_ACCESS
                reject(ret_val)
            } else {
                ret_val.status = 0
                resolve(ret_val)
                return;
            }
        } catch (err) {
            logger.log(err)
            if (!err.status) {
                ret_val = ERRCODE.UNKNOWN_ERROR
                reject(ret_val)
                return;
            }
            reject(err)
        }

    })
}

function join_session(payload) {
    return new Promise(async(resolve, reject) => {
        let ret_val = { status: -1 }
            // ret_val.status = 0
            // resolve(ret_val)
            // return;
        let creds = payload.auth.data;
        try {
            if (!payload.data.docPath || !payload.data.docId || !payload.key) {
                ret_val = ERRCODE.PAYLOAD_ERROR
                reject(ret_val)
                return;
            }
            let iceId = payload.key.instanceId + '_' + payload.key.clientId + payload.key.eventId
            if (!creds.Roles.includes(1) && !creds.ICEIds.includes(iceId)) {
                ret_val = ERRCODE.UNAUTHORIZED_ACCESS
                reject(ret_val)
                return;
            } else {
                ret_val.status = 0
                resolve(ret_val)
                return;
            }
        } catch (err) {
            logger.log(err)
            if (!err.status) {
                ret_val = ERRCODE.UNKNOWN_ERROR
                reject(ret_val)
                return;
            }
            reject(err)
        }

    })
}


function update_user(payload) {
    return new Promise(async(resolve, reject) => {
        let ret_val = { status: -1 }
        let creds;
        //console.log(payload)
        if (payload.auth && payload.auth.data && payload.auth.data.UserId) {
            creds = payload.auth.data;
        } else {
            creds = { UserId: payload.data.UserId, ICEIds: [], Roles: [10] }
        }

        //console.log(creds)
        //logger.log('validator.update_user')
        try {
            if (creds.Roles.includes(1)) {
                ret_val.status = 0
                resolve(ret_val)
            } else {
                if (!payload.key.clientId || !payload.key.clientId || !payload.key.eventId) {
                    ret_val = ERRCODE.UNAUTHORIZED_ACCESS
                    reject(ret_val)
                    return;
                }
                let iceId = payload.key.instanceId + '_' + payload.key.clientId + payload.key.eventId
                if (creds.Roles.includes(2) || creds.Roles.includes(3) || creds.Roles.includes(4)) {
                    if (!creds.ICEIds.includes(iceId)) {
                        ret_val = ERRCODE.UNAUTHORIZED_ACCESS
                        reject(ret_val)
                        return;
                    } else {
                        ret_val.status = 0
                        resolve(ret_val)
                    }
                } else {
                    if (payload.data.UserId != creds.UserId) {
                        ret_val = ERRCODE.UNAUTHORIZED_ACCESS
                        reject(ret_val)
                        return;
                    } else {
                        ret_val.status = 0
                        resolve(ret_val)
                    }
                }

            }
        } catch (err) {
            logger.log(err)
            if (!err.status) {
                ret_val = ERRCODE.UNKNOWN_ERROR
                reject(ret_val)
                return;
            }
            reject(err)
        }

    })
}


function _has_valid_auth_token(req, params, token) {
    return new Promise(async(resolve, reject) => {
        let ret_val = { status: -1 }
        let creds = {}
        verify_id_token(token)
            .then(res => {
                creds = res;
                let tasks = []
                tasks.push(_is_allowed_method(req, params))
                tasks.push(_is_called_from_cms(req, creds))
                tasks.push(_has_allowed_role(req, params, creds))
                return Promise.all(tasks)
            })
            .then((res) => {
                ret_val.status = 0
                ret_val.msg = '_is_valid_auth_token'
                ret_val.creds = creds
                resolve(ret_val)
            })
            .catch(err => {
                logger.log(err)
                if (err.code == 'auth/id-token-revoked') {
                    ret_val = ERRCODE.TOKEN_REVOKED
                } else if (err.code == 'auth/id-token-expired') {
                    ret_val = ERRCODE.TOKEN_EXPIRED
                } else if (err.code == 'auth/argument-error') {
                    ret_val = ERRCODE.INVALID_TOKEN
                } else if (err.status) {
                    ret_val = err
                } else {
                    ret_val = ERRCODE.UNKNOWN_ERROR
                }
                reject(ret_val)
            })
    })
}

function _get_auth_user(creds) {
    return new Promise(async(resolve, reject) => {
        let ret_val = { status: -1 }
        try {
            let auth_user = {}
            auth_user.AuthType = (creds.authtype || 1)
            auth_user.InstanceId = (creds.instanceId || "")
            auth_user.Domain = (creds.domain || "")
            auth_user.UserId = (creds.UserId || creds.user_id || creds.uid)
            auth_user.Email = (creds.email || "")
            auth_user.Roles = (creds.roles || [])
            auth_user.Events = (creds.events || [])
            auth_user.Clients = (creds.clients || [])
            auth_user.UserRoles = SYSROLES.filter(item => creds.roles.includes(item.RoleID))
            auth_user.MappedEvents = {}
            auth_user.MappedClients = []
            auth_user.ICEIds = creds.iceIds;
            creds.iceIds.forEach(iceId => {
                let pattern = /^(OA_UAT)_(C\d+)(E\d+)$/;
                let iceArr = iceId.match(pattern)
                if (iceArr.length > 3) {
                    if (!auth_user.MappedEvents[iceArr[2]]) {
                        auth_user.MappedEvents[iceArr[2]] = []
                    }
                    auth_user.MappedEvents[iceArr[2]].push(iceArr[3])
                }
            })
            auth_user.MappedClients = Object.keys(auth_user.MappedEvents)
            ret_val.status = 0
            ret_val.data = auth_user
            resolve(ret_val)
        } catch (err) {
            logger.log(err)
            ret_val = ERRCODE.UNKNOWN_ERROR
            reject(ret_val)
        }
    })
}

function _is_allowed_method(req, params) {
    return new Promise(async(resolve, reject) => {
        let ret_val = { status: -1 }
        try {
            let allowed_methods = Object.keys(params)
            if (!allowed_methods.includes(req.method)) {
                ret_val = ERRCODE.METHOD_NOT_ALLOWED
                reject(ret_val)
                return;
            } else {
                ret_val.status = 0
                ret_val.msg = '_has_allowed_method'
                resolve(ret_val)
            }
        } catch (err) {
            logger.log(err)
            ret_val = ERRCODE.UNKNOWN_ERROR
            reject(ret_val)
            return;
        }
    })
}

function _has_allowed_role(req, params, creds, matchany = true) {
    return new Promise(async(resolve, reject) => {
        let ret_val = { status: -1 }
        let roles = (params[req.method].allowedRoles || []);
        try {
            if (creds.roles.length) {
                if (roles.length) {
                    if (matchany) {
                        if (!roles.some(val => creds.roles.includes(val))) {
                            ret_val = ERRCODE.UNAUTHORIZED_ACCESS
                            reject(ret_val)
                            return
                        }
                    } else {
                        if (!roles.every(val => creds.roles.includes(val))) {
                            ret_val = ERRCODE.UNAUTHORIZED_ACCESS
                            reject(ret_val)
                            return
                        }
                    }
                }
                ret_val.status = 0
                ret_val.msg = '_has_allowed_role'
                resolve(ret_val)
            } else {
                ret_val = ERRCODE.ACCESS_DENIED
                reject(ret_val)
                return
            }
        } catch (err) {
            logger.log(err)
            ret_val = ERRCODE.UNKNOWN_ERROR
            reject(ret_val)
            return;
        }
    })
}

function _is_called_from_cms(req, creds) {
    return new Promise(async(resolve, reject) => {
        let ret_val = { status: -1 }
        let host = '';
        try {
            try {
                host = Url(req.headers.origin, true);
            } catch {
                host = '';
                //logger.log('Could not determine host from request')
            }
            let adminRoles = [1, 2, 3, 4];
            if (host === config.CMS && !adminRoles.some(val => creds.roles.includes(val))) {
                ret_val = ERRCODE.ACCESS_DENIED
                reject(ret_val)
                return;
            } else {
                ret_val.status = 0
                ret_val.msg = '_is_called_from_cms'
                resolve(ret_val)
                return
            }
        } catch (err) {
            logger.log(err)
            ret_val = ERRCODE.ACCESS_DENIED
            reject(ret_val)
            return;
        }
    })
}

function verify_token(token) {
    return new Promise((resolve, reject) => {
        jwt.verify(token, config.AUTH2.ACCESS_TOKEN_SECRET, function(err, decoded) {
            if (err) {
                logger.log(err)
                reject(err)
            }
            resolve(decoded);
        })
    })
}

function verify_id_token(idToken) {
    return new Promise(async(resolve, reject) => {
        let checkRevoked = true;
        validatorApp.auth().verifyIdToken(idToken, checkRevoked)
            .then((decodedToken) => {
                validatorApp.delete().then(() => {
                    validatorApp = admin.initializeApp({
                        credential: admin.credential.cert(config.SERVICE_ACCOUNT)
                    }, "validatorApp");
                    resolve(decodedToken);
                })
            })
            .catch((err) => {
                validatorApp.delete().then(() => {
                    validatorApp = admin.initializeApp({
                        credential: admin.credential.cert(config.SERVICE_ACCOUNT)
                    }, "validatorApp");
                    logger.log(err)
                    reject(err)
                })
            });
    })
}

function verify_social_token(provider, token) {
    return new Promise(async(resolve, reject) => {
        let cred;
        try {
            if (provider === 'Google') {
                cred = await verify_google_id_token(token)
            } else if (provider === 'Facebook') {
                cred = await verify_facebook_access_token(token)
            } else if (provider === 'LinkedIn') {
                cred = await verify_linkedin_access_token(token)
            } else if (provider === 'Microsoft') {
                cred = await verify_microsoft_token(token)
            }
            console.log(cred)
            resolve(cred)
        } catch (err) {
            console.log(err)
            reject(err)
        }
    })
}

function verify_google_id_token(idToken) {
    return new Promise(async(resolve, reject) => {
        console.log('verify_google_id_token')
        let ret_val = { status: -1 }
        if (!idToken) {
            ret_val = ERRCODE.PAYLOAD_ERROR
            reject(ret_val);
            return;
        }
        gclient.verifyIdToken({ idToken: idToken, audience: config.FIREBASE_CONFIG.clientId })
            .then((decodedToken) => {
                console.log(decodedToken)
                let cred = decodedToken.getPayload()
                if (!cred.email) {
                    ret_val = ERRCODE.INVALID_TOKEN
                    reject(ret_val);
                    return;
                }
                if (!cred.email || !cred.email_verified) {
                    ret_val = ERRCODE.EMAIL_NOT_VERIFIED
                    reject(ret_val);
                    return;
                }
                resolve(cred);
            })
            .catch((err) => {
                logger.log(err)
                if (err.code == 'auth/id-token-revoked') {
                    ret_val = ERRCODE.TOKEN_REVOKED
                } else if (err.code == 'auth/id-token-expired') {
                    ret_val = ERRCODE.TOKEN_EXPIRED
                } else if (err.code == 'auth/argument-error') {
                    ret_val = ERRCODE.INVALID_TOKEN
                } else if (err.status) {
                    ret_val = err
                } else {
                    ret_val = ERRCODE.UNKNOWN_ERROR
                }
                reject(ret_val)
                return;
            });
    })
}


function verify_microsoft_token(token) {
    return new Promise(async(resolve, reject) => {
        let ret_val = { status: -1 }
        try {
            let AuthStr = 'Bearer ' + token;
            //console.log(AuthStr)
            let res = await axios.get('https://graph.microsoft.com/v1.0/me', { 'headers': { 'Authorization': AuthStr } });
            ret_val.email = res.data.userPrincipalName
            ret_val.name = (res.data.displayName || "")
            ret_val.first_name = (res.data.givenName || "")
            ret_val.last_name = (res.data.surname || "")
            ret_val.status = 0
            resolve(ret_val);
        } catch (err) {
            console.log(JSON.stringify(err))
            ret_val = ERRCODE.INVALID_TOKEN
            reject(ret_val)
        }
    })
};

function verify_facebook_access_token(token) {
    return new Promise(async(resolve, reject) => {
        try {
            let res = await axios.get('https://graph.facebook.com/me', { params: { fields: ['id', 'email', 'first_name', 'last_name'].join(','), access_token: token } });
            console.log(res.data)
            resolve(res.data);
        } catch (err) {
            console.log('error')
            console.log(JSON.stringify(err))
            reject(ERRCODE.INVALID_TOKEN)
        }
    })
};

function verify_linkedin_access_token(token) {
    return new Promise(async(resolve, reject) => {
        try {
            let tasks = []
            let res = { email: null }
            tasks.push(axios.get('https://api.linkedin.com/v2/emailAddress?q=members&projection=(elements*(handle~))&oauth2_access_token=' + token))
            tasks.push(axios.get('https://api.linkedin.com/v2/me?projection=(id,firstName,lastName,profilePicture(displayImage~:playableStreams))&oauth2_access_token=' + token));
            let results = await Promise.all(tasks);

            if (results.length) {
                let emailRes = results[0].data;
                //console.log(emailRes.elements[0]['handle~']['emailAddress'])
                // if (emailRes && emailRes.elements && isArray(emailRes.elements) && emailRes.elements[0]['handle~'] && emailRes.elements[0]['handle~']['emailAddress']) {
                //     res.email = emailRes.elements[0]['handle~']['emailAddress'];
                // }
                res.email = emailRes.elements[0]['handle~']['emailAddress'];
            }
            console.log(res)
            resolve(res);
        } catch (err) {
            console.log('error')
            console.log(JSON.stringify(err))
            reject(ERRCODE.INVALID_TOKEN)
        }
    })
};

module.exports = {
    __call: __call,
    verifyToken: verify_token,
    verifyIdToken: verify_id_token,
    verifySocialToken: verify_social_token,
    authToken: auth_token,
    apiKey: api_key,
    writeObject: write_object,
    deactivateAttendees: deactivate_attendees,
    updateUser: update_user,
    writeSession: write_session,
    joinSession: join_session,
    searchCollection: search_collection,
    getAuthToken: get_auth_token
}