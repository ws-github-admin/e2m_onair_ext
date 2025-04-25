'use strict';

const admin = require("firebase-admin");
const config = require('../config.json');
const moment = require('moment');
const puppeteer = require('puppeteer');
const Handlebars = require("handlebars");
const { Firestore } = require('@google-cloud/firestore');
const { Storage, } = require('@google-cloud/storage');
const { PubSub } = require('@google-cloud/pubsub');
//const FCM = require('promise-fcm');
const axios = require('axios');
const bcrypt = require('bcryptjs');
const path = require('path');
const mailer = require('@sendgrid/mail');
const mailgun = require('mailgun-js');
const nmailer = require('nodemailer-promise');
const { htmlToText } = require('html-to-text');
const { get, reject } = require('lodash');
const logger = require('./logger');
const { ERRCODE } = require('./errcode');
const { createEvent } = require('ics');
const fs = require('fs');

const CRYPTO_KEY = "~{ry*I)==yU/]2<9WSg!Hi@R:#-/E7(hTBnjAC=3Q%ZE$";

var utilsApp = admin.initializeApp({
    credential: admin.credential.cert(config.SERVICE_ACCOUNT),
    storageBucket: config.FS_STORAGE_BUCKET
}, "utilsApp");


// Instantiates clients
const pubSubClient = new PubSub({
    projectId: config.GCP.PROJECT_ID,
    keyFilename: (__dirname + config.GCP.KEY_FILE_PATH).replace('/lib/', '/')
});

const dbClient = new Firestore({
    projectId: config.GCP.PROJECT_ID,
    keyFilename: (__dirname + config.GCP.KEY_FILE_PATH).replace('/lib/', '/')
});

const bucketName = (config.FIREBASE_CONFIG.storageBucket);
const storageClient = new Storage({
    projectId: config.GCP.PROJECT_ID,
    keyFilename: (__dirname + config.GCP.KEY_FILE_PATH).replace('/lib/', '/')
});
//mailer.setApiKey(config.SENDGRID_API_KEY);


function get_email_template(payload, path) {
    return new Promise((resolve, reject) => {
        let ret_val = { status: -1 }
        let tasks = []
        let instance_base_path = '/' + payload.key.instanceId;
        let eti_doc_path = instance_base_path + path;

        //logger.log(eti_doc_path)
        tasks.push(dbClient.doc(eti_doc_path).get());
        if (payload.key.clientId && payload.key.eventId) {
            let event_base_path = "/" + payload.key.instanceId + "_" + payload.key.clientId + payload.key.eventId;
            let ete_doc_path = event_base_path;
            ete_doc_path += path;
            //logger.log(ete_doc_path)
            tasks.push(dbClient.doc(ete_doc_path).get())
        } else if (payload.key.clientId) {
            let client_base_path = "/" + payload.key.instanceId + "/ClientList/Clients/" + payload.key.clientId;
            let etc_doc_path = client_base_path + path;
            tasks.push(dbClient.doc(etc_doc_path).get())
        }

        Promise.all(tasks)
            .then(result => {

                if (result.length > 1) {
                    if (result[1].exists) {
                        ret_val.status = 0
                        ret_val.email_template = result[1].data();
                    } else {
                        if (result[0].exists) {
                            ret_val.status = 0
                            ret_val.email_template = result[0].data();
                        }
                    }
                } else if (result[0].exists) {
                    ret_val.status = 0
                    ret_val.email_template = result[0].data();
                } else {
                    ret_val = ERRCODE.PAYLOAD_ERROR
                }
                //logger.log(ret_val)
                resolve(ret_val)
            })
            .catch(err => {
                logger.log(err)
                ret_val = ERRCODE.UNKNOWN_ERROR
                resolve(ret_val);
            });
    });
}

function send_push(payload) {
    return new Promise(async (resolve, reject) => {
        logger.log("send_push")
        logger.log(payload)
        let ret_val = { status: -1 };
        if (!payload.key || !payload.data) {
            ret_val = ERRCODE.PAYLOAD_ERROR
            reject(ret_val);
            return;
        }
        if (!payload.key.instanceId || !payload.key.clientId || !payload.key.eventId) {
            ret_val = ERRCODE.PAYLOAD_ERROR
            reject(ret_val);
            return;
        }

        const event_base_path = "/" + payload.key.instanceId + "_" + payload.key.clientId + payload.key.eventId;
        var NotificationId = (payload.data.NotificationId || "0");
        dbClient.doc(event_base_path + '/NotificationList/Notifications/' + NotificationId).get()
            .then(async (notification) => {
                let tasks = [];
                let data = {};
                let tempData = {};
                if (notification.exists) {
                    data = notification.data();
                    tempData = JSON.parse(JSON.stringify(notification.data()));
                    tempData.NotificationId = NotificationId;
                } else if (payload.data.Notification) {
                    data = payload.data.Notification
                    tempData = JSON.parse(JSON.stringify(payload.data.Notification));
                } else {
                    ret_val = ERRCODE.PAYLOAD_ERROR
                    reject(ret_val);
                    return;
                }

                if (data.Attendees) {

                    if (tempData.Attendees) {
                        delete tempData.Attendees;
                    }
                    console.log(data)
                    for (let attendee of data.Attendees) {
                        tasks.push(_send_push_attendee(payload, event_base_path, attendee.AttendeeID, tempData));
                    }
                }
                if (data.Groups) {
                    if (tempData.Groups) {
                        delete tempData.Groups;
                    }
                    tasks.push(_send_push_groups(payload, event_base_path, data.Groups, tempData));
                }
                if (data.TopicConvention) {
                    tasks.push(_send_push_topic(payload, event_base_path, data.TopicConvention, tempData));
                }

                return Promise.all(tasks)
            })
            .then((res) => {
                utilsApp.delete()
                utilsApp = admin.initializeApp({
                    credential: admin.credential.cert(config.SERVICE_ACCOUNT),
                    storageBucket: config.FS_STORAGE_BUCKET
                }, "utilsApp");
                //logger.log(res)
                ret_val.status = 0;
                ret_val.result = res;
                resolve(ret_val)
            })
            .catch((err) => {
                //console.log(err);
                utilsApp.delete()
                utilsApp = admin.initializeApp({
                    credential: admin.credential.cert(config.SERVICE_ACCOUNT),
                    storageBucket: config.FS_STORAGE_BUCKET
                }, "utilsApp");
                ret_val = ERRCODE.UNKNOWN_ERROR
                reject(ret_val);
                return;
            })

    })
}

function _get_group_attendees(payload, event_base_path, groupId) {
    return new Promise((resolve, reject) => {
        let ret_val = { status: -1 };
        dbClient.doc(event_base_path + '/GroupList/Groups/' + groupId).get()
            .then((doc) => {
                let attendees = []
                if (doc.exists) {
                    if (doc.data().Attendees) {
                        doc.data().Attendees.forEach(attendee => {
                            if (attendee.AttendeeId) {
                                attendees.push(attendee.AttendeeId)
                            }
                        })
                    }
                }
                resolve(attendees)
            })
            .catch((err) => {
                logger.log(err);
                ret_val = ERRCODE.UNKNOWN_ERROR
                reject(ret_val);
                return;
            })
    })
}

function _send_push_groups(payload, event_base_path, groups, data) {
    return new Promise(async (resolve, reject) => {
        //logger.log("_send_push_groups")
        let tasks = [];
        let ret_val = { status: -1 }
        for (let group of groups) {
            tasks.push(_get_group_attendees(payload, event_base_path, group.GroupId));
        }
        Promise.all(tasks)
            .then((results) => {
                let attendees = [];
                for (let result of results) {
                    attendees = attendees.concat(result)
                }
                let unique_attendees = [...new Set(attendees)];
                return _send_push_group(payload, event_base_path, unique_attendees, data)
            })
            .then((res) => {
                ret_val.status = 0
                ret_val.result = res
                resolve(ret_val);
            })
            .catch((err) => {
                logger.log(err);
                ret_val = ERRCODE.UNKNOWN_ERROR
                reject(ret_val);
                return;
            })

    })
}

function _send_push_group(payload, event_base_path, attendees, data) {
    return new Promise(async (resolve, reject) => {
        //logger.log("_send_push_group")
        let tokens = []
        let ret_val = { status: -1 }
        dbClient.collection(event_base_path + '/SubscribersList/Subscriptions').get()
            .then(async (querySnapshot) => {
                let tasks = [];
                let subscribers = [];
                querySnapshot.docs.forEach(doc => {
                    if (doc.data().AttendeeID) {
                        subscribers.push(doc.data().AttendeeID);
                    }
                })
                //// Save push to attendee (changed on 05/13/2021)
                if (attendees.length) {
                    for (let attendeeId of attendees) {
                        tasks.push(save_push_attendee(payload, event_base_path, attendeeId, data));
                    }
                }
                let matched = attendees.filter(e => subscribers.includes(e));
                if (matched.length) {
                    for (let attendeeId of matched) {
                        let subscription = await dbClient.doc(event_base_path + '/SubscribersList/Subscriptions/' + attendeeId).get();
                        if (subscription.exists) {
                            //tasks.push(save_push_attendee(payload, event_base_path, attendeeId, data));
                            let subscriptionData = subscription.data();
                            for (let device of subscriptionData.Devices) {
                                tokens.push(device.DeviceToken)
                            }
                        }
                    }
                }
                return Promise.all(tasks)
            })
            .then((res) => {
                let tempToken = tokens
                if (tokens.length > 500) {
                    tempToken = tokens.slice(0, 500)
                }
                return _send_push_multi(tempToken, data)
            })
            .then((res) => {
                //logger.log(res)
                ret_val.status = 0
                ret_val.result = res
                resolve(ret_val);
            })
            .catch((err) => {
                logger.log(err);
                ret_val = ERRCODE.UNKNOWN_ERROR
                reject(ret_val);
                return;
            })
    })
}

function _send_push_topic(payload, event_base_path, topicConvention, data) {
    return new Promise(async (resolve, reject) => {
        //logger.log("_send_push_topic")
        let tasks = [];
        let tokens = [];
        let tempToken = [];
        let ret_val = { status: -1 }
        tasks.push(dbClient.collection('/' + topicConvention + '/SubscribersList/Subscriptions').get());
        tasks.push(dbClient.collection('/' + topicConvention + '/AttendeeList/Attendees').get());
        Promise.all(tasks)
            .then(async (res) => {
                let tasks = [];
                let subscribers = [];
                let attendees = [];
                res[0].docs.forEach(doc => {
                    if (doc.data().AttendeeID) {
                        subscribers.push(doc.data().AttendeeID);
                    }
                })
                res[1].docs.forEach(doc => {
                    if (doc.data().AttendeeId) {
                        attendees.push(doc.data().AttendeeId);
                    }
                })


                //// Save push for subscribers only
                // let matched = subscribers.filter(e => attendees.includes(e));
                // if (matched.length) {
                //     logger.log(matched.length)
                //     for (let attendeeId of matched) {
                //         tasks.push(save_push_attendee(payload, event_base_path, attendeeId, data));
                //     }
                // }

                //// Save push to attendee (changed on 05/13/2021)
                if (attendees.length) {
                    for (let attendeeId of attendees) {
                        tasks.push(save_push_attendee(payload, event_base_path, attendeeId, data));
                    }
                }
                return Promise.all(tasks);
            })
            .then((result) => {
                return _send_push_all(topicConvention, tokens, data)
            })
            .then((res) => {
                //logger.log(res)
                ret_val.status = 0
                ret_val.result = res
                resolve(ret_val);
            })
            .catch((err) => {
                logger.log(err);
                ret_val = ERRCODE.UNKNOWN_ERROR
                reject(ret_val);
                return;
            })
    })
}

function send_meeting_push(payload) {
    return new Promise(async (resolve, reject) => {
        //logger.log("send_push_attendee")
        logger.log(payload)
        let pk = payload.key;
        let pd = payload.data;
        let event_base_path = pk.instanceId + '_' + pk.clientId + pk.eventId;
        let ret_val = { status: -1 }
        let tasks = [];
        for (let i = 0; i < pd.length; i++) {
            let data = {
                NotificationId: (pd[i].NotificationId || ""),
                Initials: (pd[i].Tags || ""),
                Name: (pd[i].Name || ""),
                ProfilePictureURL: (pd[[i]].ProfilePictureURL || ""),
                SenderAttendeeId: (pd[i].SenderAttendeeId || ""),
                SenderName: (pd[i].SenderName || ""),
                SenderProfilePictureURL: (pd[[i]].SenderProfilePictureURL || ""),
                EventLogo: (pd[i].EventLogo || ""),
                NotifyTypeIndex: (pd[i].Type || 2),
                MeetingId: (pk.meetingId || ""),
                ChannelId: (pk.channelId || ""),
                MeetingType: (pk.meetingType || ""),
                NotificationMessage: (pd[i].NotificationMessage || ""),
                NotificationTitle: (pd[i].NotificationTitle || ""),
                isSeen: "0"
            }
            tasks.push(_send_push_attendee(payload, event_base_path, pd[i].AttendeeId, data))
        }
        Promise.allSettled(tasks)
            .then(res => {
                utilsApp.delete()
                utilsApp = admin.initializeApp({
                    credential: admin.credential.cert(config.SERVICE_ACCOUNT),
                    storageBucket: config.FS_STORAGE_BUCKET
                }, "utilsApp");
                ret_val.status = 0
                ret_val.results = res;
                resolve(ret_val)
            })
            .catch(err => {
                utilsApp.delete()
                utilsApp = admin.initializeApp({
                    credential: admin.credential.cert(config.SERVICE_ACCOUNT),
                    storageBucket: config.FS_STORAGE_BUCKET
                }, "utilsApp");
                logger.log(err);
                ret_val = ERRCODE.UNKNOWN_ERROR
                reject(et_val)
            })
    })
}

function send_push_attendee(payload, event_base_path, attendeeId, data) {
    return new Promise(async (resolve, reject) => {
        //logger.log("send_push_attendee")
        logger.log(payload)
        let subscriptionData;
        let ret_val = { status: -1 }

        if (data.MeetingType && payload.auth) {
            let NotificationTemplatePath = '/OA_UAT/NotificationTPL/' + data.MeetingType;
            let TempRef = await dbClient.doc(NotificationTemplatePath).get()
            if (TempRef.exists && payload.auth) {
                let senderId = payload.auth.data.UserId
                let senderRef = await dbClient.doc(event_base_path + '/AttendeeList/Attendees/' + senderId).get();
                if (senderRef.exists) {
                    let senderData = senderRef.data()
                    let senderObj = {
                        AttendeeId: senderData.AttendeeId,
                        Name: senderData.Name,
                        ProfilePictureURL: senderData.ProfilePictureURL
                    }
                    data.ProfilePictureURL = senderData.ProfilePictureURL
                    data.Name = senderData.Name
                    data.Sender = senderObj
                    let NotificationTemplate = TempRef.data()
                    let notificationMessageTemplate = Handlebars.compile(NotificationTemplate.Message);
                    data.NotificationMessage = notificationMessageTemplate(senderObj);
                    let notificationTitleTemplate = Handlebars.compile(NotificationTemplate.Title);
                    data.NotificationTitle = notificationTitleTemplate(senderObj);
                }
            }
        }

        dbClient.doc(event_base_path + '/SubscribersList/Subscriptions/' + attendeeId).get()
            .then((subscription) => {
                let tasks = [];
                if (subscription.exists) {
                    subscriptionData = subscription.data();
                    // for (let device of subscriptionData.Devices) {
                    //     let instance = { id: attendeeId, token: device.DeviceToken }
                    //     tasks.push(_save_push_instance(payload, event_base_path, instance, data))
                    // }
                }
                //return save_push_attendee(payload, event_base_path, attendeeId, data);
                return Promise.all(tasks)
            })
            .then((res) => {
                let tasks = [];
                if (subscriptionData) {
                    for (let device of subscriptionData.Devices) {
                        if (device.IsOptIn) {
                            let instance = { id: attendeeId, token: device.DeviceToken }
                            tasks.push(_send_push_single(payload, event_base_path, instance, data))
                        }
                    }
                }
                return Promise.all(tasks)
            })
            .then((res) => {
                utilsApp.delete()
                utilsApp = admin.initializeApp({
                    credential: admin.credential.cert(config.SERVICE_ACCOUNT),
                    storageBucket: config.FS_STORAGE_BUCKET
                }, "utilsApp");
                ret_val.status = 0;
                ret_val.result = res;
                resolve(ret_val)
            })
            .catch((err) => {
                utilsApp.delete()
                utilsApp = admin.initializeApp({
                    credential: admin.credential.cert(config.SERVICE_ACCOUNT),
                    storageBucket: config.FS_STORAGE_BUCKET
                }, "utilsApp");
                logger.log(err);
                ret_val = ERRCODE.UNKNOWN_ERROR
                reject(ret_val);
                return;
            });
    })
}

function _send_push_attendee(payload, event_base_path, attendeeId, data) {
    return new Promise(async (resolve, reject) => {
        logger.log("_send_push_attendee")
        let subscriptionData;
        let ret_val = { status: -1 }

        if (data.MeetingType && payload.auth) {
            let NotificationTemplatePath = '/OA_UAT/NotificationTPL/' + data.MeetingType;
            let TempRef = await dbClient.doc(NotificationTemplatePath).get()
            if (TempRef.exists && payload.auth) {
                let senderId = payload.auth.data.UserId
                let senderRef = await dbClient.doc(event_base_path + '/AttendeeList/Attendees/' + senderId).get();
                if (senderRef.exists) {
                    let senderData = senderRef.data()
                    let senderObj = {
                        AttendeeId: senderData.AttendeeId,
                        Name: senderData.Name,
                        ProfilePictureURL: senderData.ProfilePictureURL
                    }
                    data.ProfilePictureURL = senderData.ProfilePictureURL
                    data.Name = senderData.Name
                    data.Sender = senderObj
                    let NotificationTemplate = TempRef.data()
                    let notificationMessageTemplate = Handlebars.compile(NotificationTemplate.Message);
                    data.NotificationMessage = notificationMessageTemplate(senderObj);
                    let notificationTitleTemplate = Handlebars.compile(NotificationTemplate.Title);
                    data.NotificationTitle = notificationTitleTemplate(senderObj);
                }
            }
        }

        dbClient.doc(event_base_path + '/SubscribersList/Subscriptions/' + attendeeId).get()
            .then((subscription) => {
                let tasks = [];
                if (subscription.exists) {
                    subscriptionData = subscription.data();
                    // for (let device of subscriptionData.Devices) {
                    //     let instance = { id: attendeeId, token: device.DeviceToken }
                    //     tasks.push(_save_push_instance(payload, event_base_path, instance, data))
                    // }
                }
                return save_push_attendee(payload, event_base_path, attendeeId, data);
                //return Promise.all(tasks)
            })
            .then((res) => {
                let tasks = [];
                if (subscriptionData) {
                    for (let device of subscriptionData.Devices) {
                        if (device.IsOptIn) {
                            let instance = { id: attendeeId, token: device.DeviceToken }
                            tasks.push(_send_push_single(payload, event_base_path, instance, data))
                        }
                    }
                }
                return Promise.all(tasks)
            })
            .then((res) => {
                ret_val.status = 0;
                ret_val.result = res;
                resolve(ret_val)
            })
            .catch((err) => {
                logger.log(err);
                ret_val = ERRCODE.UNKNOWN_ERROR
                reject(ret_val);
                return;
            });
    })
}

function save_push_attendee(payload, event_base_path, attendeeId, data) {
    return new Promise(async (resolve, reject) => {
        logger.log("save_push_attendee")
        let ret_val = { status: -1 }
        // let subscriptionData;
        // dbClient.doc(event_base_path + '/SubscribersList/Subscriptions/' + attendeeId).get()
        //     .then((subscription) => {
        //         let tasks = [];
        //         if (subscription.exists) {
        //             subscriptionData = subscription.data();
        //             for (let device of subscriptionData.Devices) {
        //                 let instance = { id: attendeeId, token: device.DeviceToken }
        //                 tasks.push(_save_push_instance(payload, event_base_path, instance, data))
        //             }
        //         }
        //         return Promise.all(tasks)
        //     })
        ////    (changed on 05 / 13 / 2021)
        let instance = { id: attendeeId }
        _save_push_instance(payload, event_base_path, instance, data)
            .then((res) => {
                ret_val.status = 0
                ret_val.result = res
                resolve(ret_val)
            })
            .catch((err) => {
                logger.log(err);
                ret_val = ERRCODE.UNKNOWN_ERROR
                reject(ret_val);
                return;
            });
    })
}

function _save_push_instance(payload, event_base_path, instance, data) {
    return new Promise(async (resolve, reject) => {
        logger.log("_save_push_instance")
        let ret_val = { status: -1 }
        let tasks = [];
        let eventDocPath = event_base_path + "/EventInfo";
        tasks.push(dbClient.doc(event_base_path + '/AttendeeList/Attendees/' + instance.id).get())
        tasks.push(dbClient.doc(eventDocPath).get())
        Promise.all(tasks)
            .then(async (res) => {
                let tasks = []
                let ret;
                let attendeeData = res[0];
                let attendee = attendeeData.data();
                let eventData = res[1];
                let event = eventData.data();
                let tempData = {
                    Timestamp: new Date(),
                    NotificationId: (data.NotificationId || ""),
                    ExpireInMiniutes: 30,
                    Module: 'Notification',
                    Initials: (data.Tags || ""),
                    Name: (data.Name || ""),
                    ProfilePictureURL: (data.ProfilePictureURL || ""),
                    EventLogo: (event.EventLogo || ""),
                    NotifyTypeIndex: (data.Type || 2),
                    NotificationMessage: (data.NotificationMessage || ""),
                    NotificationTitle: (data.NotificationTitle || ""),
                    CreatedDate: (data.CreatedDate || new Date()),
                    LastModifiedDate: (data.LastModifiedDate || new Date()),
                    isSeen: "0"
                }

                if (data.MeetingId) {
                    tempData.MeetingId = data.MeetingId;
                }
                if (data.MeetingType) {
                    tempData.MeetingType = data.MeetingType;
                }
                if (data.ChannelId) {
                    tempData.ChannelId = data.ChannelId;
                }
                if (data.SenderAttendeeId) {
                    tempData.SenderAttendeeId = data.SenderAttendeeId;
                }
                if (data.SenderName) {
                    tempData.SenderName = data.SenderName;
                }
                if (data.SenderProfilePictureURL) {
                    tempData.SenderProfilePictureURL = data.SenderProfilePictureURL;
                }
                if (data.Sender) {
                    tempData.Sender = data.Sender;
                }

                if (data.NotificationId) {
                    ret = await dbClient.doc(event_base_path + '/AttendeeList/Attendees/' + instance.id + '/Notifications/' + data.NotificationId).set(tempData, { merge: true });
                } else {

                    ret = await dbClient.collection(event_base_path + '/AttendeeList/Attendees/' + instance.id + '/Notifications').add(tempData);
                    // console.log(event_base_path + '/AttendeeList/Attendees/' + instance.id + '/Notifications/' + ret.id)
                }
                return ret
            })
            .then((res) => {
                ret_val.status = 0;
                ret_val.result = res
                resolve(ret_val)
            })
            .catch((err) => {
                ret_val = ERRCODE.UNKNOWN_ERROR
                resolve(ret_val);
                return;
            });
    })
}

function _send_push_single(payload, event_base_path, instance, data) {
    return new Promise(async (resolve, reject) => {
        //logger.log("_send_push_single")
        let ret_val = { status: -1 }
        let message = {
            token: instance.token,
            notification: {
                title: data.NotificationTitle,
                body: data.NotificationMessage,
            },
            data: {
                instanceId: payload.key.instanceId,
                eventId: payload.key.eventId,
                clientId: payload.key.clientId
            }
        };
        if (data.MeetingId) {
            message.data.meetingId = data.MeetingId.toString();
        }
        if (data.MeetingType) {
            message.data.meetingType = data.MeetingType.toString();
        }
        if (data.ChannelId) {
            message.data.ChannelId = data.ChannelId.toString();
        }
        if (data.SenderAttendeeId) {
            message.data.SenderAttendeeId = data.SenderAttendeeId.toString();
        }
        if (data.SenderName) {
            message.data.SenderName = data.SenderName;
        }
        if (data.SenderProfilePictureURL) {
            message.data.SenderProfilePictureURL = data.SenderProfilePictureURL;
        }
        // if (data.Sender) {
        //     message.data.Sender = data.Sender;
        // }

        //let sender = new FCM(config.SERVER_KEY);
        // sender.sendTo(instance.token)
        //     .withNotification({
        //         title: data.NotificationTitle,
        //         body: data.NotificationMessage
        //     })
        //     .now()
        //logger.log(message)
        utilsApp.messaging().send(message)
            .then((res) => {
                //logger.log(res)
                ret_val.status = 0;
                ret_val.result = res
                resolve(ret_val)
            })
            .catch((err) => {
                logger.log(err);
                ret_val = ERRCODE.UNKNOWN_ERROR
                resolve(ret_val);
                return;
            });
    })
}

function _send_push_multi(tokens, data) {
    return new Promise(async (resolve, reject) => {
        //logger.log("_send_push_multi")
        let ret_val = { status: -1 }
        let payload = {
            tokens: tokens,
            notification: {
                title: data.NotificationTitle,
                body: data.NotificationMessage
            },
            webpush: {
                fcmOptions: {
                    link: 'https://google.com/'
                }
            },
            // android: {
            //     notification:{
            //         click_action: "OPEN_ACTIVITY_1"
            //     }
            // },
            // apns: {
            //     payload: {
            //         aps: {
            //             category: "OPEN_ACTIVITY_1"
            //         }
            //     }
            // },
        };
        utilsApp.messaging().sendMulticast(payload)
            .then((res) => {
                ret_val.status = 0;
                ret_val.result = {
                    successCount: res.successCount,
                    failureCount: res.failureCount
                }
                resolve(ret_val)
            })
            .catch((err) => {
                logger.log(err)
                ret_val = ERRCODE.UNKNOWN_ERROR
                reject(ret_val);
                return;
            })
    })
}

async function _send_push_all(topic, tokens, data) {
    return new Promise(async (resolve, reject) => {
        //logger.log("_send_push_all")
        let ret_val = { status: -1 }
        let payload = {
            topic: topic,
            notification: {
                title: data.NotificationTitle,
                body: data.NotificationMessage
            },
            webpush: {
                fcmOptions: {
                    link: 'https://google.com/'
                }
            },
            // android: {
            //     notification:{
            //         click_action: "OPEN_ACTIVITY_1"
            //     }
            // },
            // apns: {
            //     payload: {
            //         aps: {
            //             category: "OPEN_ACTIVITY_1"
            //         }
            //     }
            // },
        };
        //if (tokens.length) {
        // utilsApp.messaging().subscribeToTopic(tokens, topic)
        //     .then(async(response) => {
        //         // See the MessagingTopicManagementResponse reference documentation
        //         // for the contents of response.
        //         //console.log('Successfully subscribed to topic:');
        //         return utilsApp.messaging().send(payload)
        //     })
        utilsApp.messaging().send(payload)
            .then((res) => {
                ret_val.status = 0;
                ret_val.result = res
                resolve(ret_val)
            })
            .catch((err) => {
                logger.log(err)
                ret_val = ERRCODE.UNKNOWN_ERROR
                reject(ret_val);
                return;
            })
        //}
    })
}

function send_email_via_sendgrid(message, SENDGRIDApiKey = null) {
    return new Promise((resolve, reject) => {
        let ret_val = { status: -1 }
        try {
            if (config.MAILGUN_API_KEY) {
                let tasks = [];
                tasks.push(send_email_via_mailgun(message));
                Promise.all(tasks)
                    .then((res) => {
                        //console.log('_send_email end: ' + moment().format())
                        resolve(res)
                    })
                    .catch(err => {
                        logger.log(err)
                        ret_val = ERRCODE.UNKNOWN_ERROR
                        reject(ret_val);
                        return;
                    })
            } else {
                console.log("Sending Email via SENDGRID")
                let from = _format_email_address(message.from);
                message.from = from.name + "<" + from.email + ">";
                // message.from = _format_email_address(message.from);
                // //console.log(message.from)
                // if (message.from) {
                //     emailStr = (message.from.email || message.from);
                // }
                // console.log(emailStr)
                SENDGRIDApiKey = (SENDGRIDApiKey || config.SENDGRID_API_KEY)
                console.log(SENDGRIDApiKey)
                mailer.setApiKey(SENDGRIDApiKey)
                // if (emailStr.includes('edulife')) {
                //     mailer.setApiKey(config.SENDGRID_API_KEY_EDU);
                // } else if (emailStr.includes('mastry')) {
                //     mailer.setApiKey(config.SENDGRID_API_KEY_MASTRY);
                // }

                if (config.Email.Stage == "PRD") {
                    message.to = _format_email_address(message.to);
                } else {
                    message.to = config.Email.TestTo;
                    //message.subject = ("[UAT] " + message.subject);
                    message.subject = ("[" + config.Email.Stage + "] " + message.subject);
                }

                if (message.cc) {
                    message.ccArray = [];
                    let ccArray = message.cc.split(",");
                    for (let cc of ccArray) {
                        message.ccArray.push(_format_email_address(cc));
                    }
                } else {
                    delete message.cc;
                }

                if (message.bcc) {
                    message.bccArray = [];
                    let bccArray = message.bcc.split(",");
                    for (let bcc of bccArray) {
                        message.bccArray.push(_format_email_address(bcc));
                    }
                } else {
                    delete message.bcc;
                }

                // if (message.cc) {
                //     message.cc = _format_email_address(message.cc);
                // }
                // if (message.bcc) {
                //     message.bcc = _format_email_address(message.bcc);
                // }

                if (message.reply_to) {
                    message.replyTo = _format_email_address(message.reply_to);
                }

                if (!message.text) {
                    message.text = htmlToText(message.html, { wordwrap: 130 });
                }


                if (message.hasOwnProperty('reply_to')) {
                    delete message.reply_to;
                }

                console.log("message")
                console.log(JSON.stringify(message))
                //console.log('request to sendgrid : ' + moment().format())
                mailer.send(message)
                    .then(result => {
                        //console.log('response from sendgrid : ' + moment().format())
                        resolve(result);
                    })
                    .catch(err => {
                        console.log("ERROR IN")
                        logger.log(err)
                        ret_val = ERRCODE.UNKNOWN_ERROR
                        reject(ret_val);
                        return;
                    });
            }
        } catch (err) {
            console.log("ERROR OUT")
            logger.log(err)
            ret_val = ERRCODE.UNKNOWN_ERROR
            reject(ret_val);
            return;
        }
    });
}

function send_test_email_via_sendgrid(message, SENDGRIDApiKey = null) {
    console.log("send_test_email_via_sendgrid")
    return new Promise((resolve, reject) => {
        let ret_val = { status: -1 }
        try {
            // console.log(apiKey)
            let message = {};
            message.from = "On Air<noreply@mail.e2m.live>";
            message.to = "debashis.giri@webspiders.com";
            message.subject = "test";
            message.text = "test email";

            mailer.setApiKey(config.SENDGRID_API_KEY)

            console.log("send_test_email_via_sendgrid before send")
            mailer.send(message)
                .then(result => {
                    console.log("send_test_email_via_sendgrid inside send")
                    console.log(result)
                    //console.log('response from sendgrid : ' + moment().format())
                    resolve(result);
                })
                .catch(err => {
                    console.log("send_test_email_via_sendgrid ERROR")
                    logger.log(err)
                    ret_val = ERRCODE.UNKNOWN_ERROR
                    reject(ret_val);
                    return;
                });
        } catch (err) {
            console.log("send_test_email_via_sendgrid ERROR OUT")
            logger.log(err)
            ret_val = ERRCODE.UNKNOWN_ERROR
            reject(ret_val);
            return;
        }
    });
}

function send_email_via_mailgun(message, SENDGRIDApiKey = null) {
    return new Promise( async(resolve, reject) => {
        let ret_val = { status: -1 }
        try {
            let emailStr = '';
            console.log("message: ", message)
            console.log("message: ", JSON.stringify(message))
            let from = _format_email_address(message.from);
            if (from.email) {
                message.from = from.name + "<" + from.email + ">";
            } else {
                message.from = from;
            }
            //console.log(message.from)
            // if (message.from) {
            //     emailStr = (message.from.email || message.from);
            // }
            // console.log(emailStr)
            let mg = mailgun({ apiKey: config.MAILGUN_API_KEY, domain: "e2m.live" });

            if (config.Email.Stage == "PRD") {
                // message.to = _format_email_address(message.to);
                let to = _format_email_address(message.to);
                if (to.email) {
                    message.to = to.name + "<" + to.email + ">";
                } else {
                    message.to = to;
                }
            } else {
                message.to = config.Email.TestTo;
                //message.subject = ("[UAT] " + message.subject);
                message.subject = ("[" + config.Email.Stage + "] " + message.subject);
            }

            if (message.cc) {
                message.ccArray = [];
                let ccArray = message.cc.split(",");
                for (let cc of ccArray) {
                    message.ccArray.push(_format_email_address(cc));
                }
            } else {
                delete message.cc;
            }

            if (message.bcc) {
                message.bccArray = [];
                let bccArray = message.bcc.split(",");
                for (let bcc of bccArray) {
                    message.bccArray.push(_format_email_address(bcc));
                }
            } else {
                delete message.bcc;
            }

            // if (message.cc) {
            //     message.cc = _format_email_address(message.cc);
            // }
            // if (message.bcc) {
            //     message.bcc = _format_email_address(message.bcc);
            // }

            if (message.reply_to) {
                message.replyTo = _format_email_address(message.reply_to);
            }

            if (!message.text) {
                message.text = htmlToText(message.html, { wordwrap: 130 });
            }

            if (message.hasOwnProperty('reply_to')) {
                delete message.reply_to;
            }

            if (message.attachmentICS) {
                const attachment = new mg.Attachment(message.attachmentICS);
                message.attachment = attachment;
                delete message.attachmentICS;
            }
            //console.log('request to mailgun : ' + moment().format())
            console.log("send_email_via_mailgun before send")
            console.log('request to mailgun message: ', message)
            console.log('request to mailgun message: ', JSON.stringify(message))
            // mg.messages().send(message, function async(error, body) {
            //     console.log("111111111111111111")
            //     console.log("send_email_via_mailgun inside send")
            //     console.log(error)
            //     console.log(body)
            //     resolve(body);
            // })
            await mg.messages().send(message);
            resolve({status: 0, result: "Email sent successfully"});
            // .catch(err => {
            //     console.log("22222222222222")
            //     console.log("send_email_via_mailgun ERROR")
            //     logger.log(err)
            //     ret_val = ERRCODE.UNKNOWN_ERROR
            //     reject(ret_val);
            //     return;
            // });
        }
        catch (err1) {
            console.log("3333333333333")
            console.log(err1)
            ret_val = ERRCODE.UNKNOWN_ERROR
            reject(ret_val);
            return;
        }
    });
}

function send_test_email_via_mailgun(message, apiKey = null) {
    console.log("send_test_email_via_mailgun")
    return new Promise((resolve, reject) => {
        let ret_val = { status: -1 }
        try {
            let mg = mailgun({ apiKey: config.MAILGUN_API_KEY, domain: "e2m.live" });
            // console.log(apiKey)
            let data = {
                from: "On Air<noreply@mail.e2m.live>",
                to: "debashis.giri@webspiders.com",
                subject: "test",
                html: "test email"
            };
            console.log("send_test_email_via_mailgun before send")
            //console.log('request to sendgrid : ' + moment().format())
            mg.messages().send(data, function async(error, body) {
                console.log("send_test_email_via_mailgun inside send")
                console.log(error)
                console.log(body)
                resolve(body);
            })
                .catch(err => {
                    console.log("send_test_email_via_mailgun ERROR")
                    logger.log(err)
                    ret_val = ERRCODE.UNKNOWN_ERROR
                    reject(ret_val);
                    return;
                });
        } catch (err) {
            console.log("send_test_email_via_mailgun ERROR OUT")
            logger.log(err)
            ret_val = ERRCODE.UNKNOWN_ERROR
            reject(ret_val);
            return;
        }
    });
}

function send_email_via_smtp(smtp, message) {
    let ret_val = { status: -1 }
    return new Promise((resolve, reject) => {
        try {
            var sendEmail = nmailer.config(smtp);
            message.from = _format_email_address_smtp(message.from);

            if (config.Email.Stage == "PRD") {
                message.to = _format_email_address_smtp(message.to);
            } else {
                message.to = config.Email.TestTo;
                message.subject = ("[UAT] " + message.subject);
            }

            if (message.cc) {
                message.ccArray = [];
                let ccArray = message.cc.split(",");
                for (let cc of ccArray) {
                    message.ccArray.push(_format_email_address_smtp(cc));
                }
            } else {
                delete message.cc;
            }

            if (message.bcc) {
                message.bccArray = [];
                let bccArray = message.bcc.split(",");
                for (let bcc of bccArray) {
                    message.bccArray.push(_format_email_address_smtp(bcc));
                }
            } else {
                delete message.bcc;
            }

            // if (message.cc) {
            //     message.cc = _format_email_address(message.cc);
            // }

            // if (message.bcc) {
            //     message.bcc = _format_email_address(message.bcc);
            // }

            if (message.reply_to) {
                message.replyTo = _format_email_address_smtp(message.reply_to);
            }

            if (!message.text) {
                message.text = htmlToText(message.html, { wordwrap: 130 });
            }

            if (message.hasOwnProperty('reply_to')) {
                delete message.reply_to;
            }
            //console.log('request to SMTP : ' + moment().format())
            sendEmail(message)
                .then(result => {
                    //console.log('response to SMTP : ' + moment().format())
                    resolve(result);
                })
                .catch(err => {
                    logger.log(err)
                    ret_val = ERRCODE.UNKNOWN_ERROR
                    reject(ret_val);
                    return;
                });
        } catch (err) {
            logger.log(err)
            ret_val = ERRCODE.UNKNOWN_ERROR
            reject(ret_val);
            return;
        }
    });
}



function send_scheduled_Notification(payload) {
    return new Promise(async (resolve, reject) => {
        logger.log(payload)
        try {
            let pk = payload.key;
            let pd = payload.data;
            let tasks = [];
            let clientDoc = pk.instanceId + '/ClientList/Clients/' + pk.clientId;
            let clientRef = await dbClient.doc(clientDoc).get();
            let event_base_path = pk.instanceId + '_' + pk.clientId + pk.eventId;
            let sessionDocPath = event_base_path + '/SessionList/Sessions/' + pk.sessionId;
            let emailTemplatePath = pk.instanceId + '/ClientList/Clients/' + pk.clientId + '/mailtpl/sessionReminder';
            let emailRef = await dbClient.doc(emailTemplatePath).get();
            if (emailRef.exists) {
                let notificationMessage = payload.data.message;
                let emailSubject = emailRef.data().subject;
                let emailBody = emailRef.data().html;
                let type = payload.data.type;
                let clientName = payload.data.ClientName;
                let countDownTime = payload.data.CountDownTime;
                let eventName = payload.data.EventName;
                let sessionName = payload.data.SessionName;
                let clientDomain = payload.data.ClientDomain;
                let attendeeDetails = [];
                dbClient.doc(sessionDocPath).get()
                    .then(async (res) => {
                        let sessionDetails = res.data();
                        if (sessionDetails && sessionDetails.MappedHosts) {
                            sessionDetails.MappedHosts.forEach((element) => {
                                attendeeDetails.push(element);
                            });
                        }
                        if (sessionDetails && sessionDetails.IncludedAttendees) {
                            sessionDetails.IncludedAttendees.forEach((element) => {
                                attendeeDetails.push(element);
                            });
                        }
                        if (attendeeDetails.length) {
                            attendeeDetails.forEach((attendee) => {


                                let replacements = {
                                    CountDownTime: countDownTime,
                                    Name: attendee.Name,
                                    FirstName: attendee.FirstName,
                                    LastName: attendee.LastName,
                                    EventName: eventName,
                                    SessionName: sessionName,
                                }


                                /////SEND EMAIL
                                let TemplateSubject = Handlebars.compile(emailSubject);
                                let subject = TemplateSubject(replacements);
                                let TemplateBody = Handlebars.compile(emailBody);
                                let body = TemplateBody(replacements);
                                let email_payload = {
                                    to: attendee.Email,
                                    subject: subject,
                                    html: body,
                                    from: {
                                        email: (emailRef.data().from || "support@edulife.sg"),
                                        name: (emailRef.data().fromName || "EduLife SG")
                                    }
                                };

                                if (emailRef.data().cc) {
                                    email_payload.cc = emailRef.data().cc;
                                }
                                if (emailRef.data().bcc) {
                                    email_payload.bcc = emailRef.data().bcc;
                                }
                                if (emailRef.data().replyTo) {
                                    email_payload.replyTo = emailRef.data().replyTo;
                                }
                                //let tmppayload = JSON.parse(JSON.stringify(email_payload))
                                tasks.push(_send_email(clientRef.data(), email_payload));

                                if (type !== 'Email') {
                                    /////SEND NOTIFICATION
                                    let TemplateMessage = Handlebars.compile(notificationMessage);
                                    let message = TemplateMessage(replacements);
                                    message = message + "\n" + "Team " + clientName;
                                    let messagePayload = {
                                        userId: [attendee.AttendeeId],
                                        body: message,
                                        messagetype: "session joined",
                                    };
                                    tasks.push(send_sms(messagePayload));
                                }
                            });
                        }
                        await Promise.allSettled(tasks)
                        resolve("Notification Alert Set");
                    });
            } else {
                reject("Notification Template Missing");
            }
        } catch (e) {
            logger.log(e)
            reject(e);
        }
    });
}

function send_sms(payload) {
    return new Promise(async (resolve, reject) => {
        logger.log(payload)
        try {
            let to = "";
            if (!payload.body) {
                reject({ status: 0, err: "body cannot be empty" });
            }
            if (!payload.messagetype) {
                reject({ status: 0, err: "messagetype cannot be empty" });
            }

            if (payload.messagetype == "session joined") {
                let userId = payload.userId[0];
                let path = `/OA_UAT/UserList/Users/${userId}`;
                let userref = await dbClient.doc(path).get();
                let data = userref.data();

                if (data.Phone) {
                    to = data.Phone;
                } else {
                    resolve({ status: 0, err: "phone no. not found" });
                }
            }
            if (payload.messagetype == "standalone") {
                if (!payload.to) {
                    reject({
                        status: 0,
                        err: "For standalone the 'to' field cannot be empty",
                    });
                } else {
                    to = payload.to;
                }
            }

            let smsObject = {
                body: payload.body,
                messagingServiceSid: config.TWILIOSERVICEID,
                to: to,
            };
            if (to.includes("+65")) {
                smsObject.from = "EduLife";
            }
            client.messages
                .create(smsObject)
                .then((message) => {
                    resolve("message sent successfully")
                })
                .catch((e) => {
                    logger.log(e)
                    reject(e);
                });
        } catch (e) {
            logger.log(e)
            reject(e);
        }
    });
}

function send_email(Client, EmailPayload) {
    return new Promise(async (resolve, reject) => {
        let ret_val = { status: -1 }
        let tasks = [];
        //console.log(Client)
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
            tasks.push(send_email_via_smtp(smtp_config, EmailPayload))

        } else {
            let SENDGRIDApiKey = (Client.SENDGRIDApiKey || config.SENDGRID_API_KEY)
            tasks.push(send_email_via_sendgrid(EmailPayload, SENDGRIDApiKey))
        }
        //console.log('_send_email start: ' + moment().format())
        Promise.all(tasks)
            .then((res) => {
                //console.log('_send_email end: ' + moment().format())
                resolve(res)
            })
            .catch(err => {
                logger.log(err)
                ret_val = ERRCODE.UNKNOWN_ERROR
                reject(ret_val);
                return;
            })
    })
}


function _format_email_address(emailAddress) {
    let ret_val;

    if (emailAddress.name && (emailAddress.name.length > 0)) {
        if (emailAddress.company && (emailAddress.company.length > 0)) {
            ret_val = {
                name: (emailAddress.name + " - " + emailAddress.company),
                email: emailAddress.email
            }
        } else {
            ret_val = {
                name: emailAddress.name,
                email: emailAddress.email
            }
        };
    } else {
        if (emailAddress.email) {
            ret_val = emailAddress.email;
        } else {
            ret_val = emailAddress;
        }
    }

    return ret_val;
}

function _format_email_address_smtp(emailAddress) {
    let ret_val;

    if (emailAddress.name && (emailAddress.name.length > 0)) {
        if (emailAddress.company && (emailAddress.company.length > 0)) {
            ret_val = {
                name: (emailAddress.name + " - " + emailAddress.company),
                address: emailAddress.email
            }
        } else {
            ret_val = {
                name: emailAddress.name,
                address: emailAddress.email
            }
        };
    } else {
        if (emailAddress.email) {
            ret_val = emailAddress.email;
        } else {
            ret_val = emailAddress;
        }
    }

    return ret_val;
}

function _send_email(Client, EmailPayload) {
    return new Promise(async (resolve, reject) => {
        let ret_val = { status: -1 }
        let tasks = [];
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
            tasks.push(send_email_via_smtp(smtp_config, EmailPayload))

        } else {
            let SENDGRIDApiKey = (Client.SENDGRIDApiKey || config.SENDGRID_API_KEY)
            tasks.push(send_email_via_sendgrid(EmailPayload, SENDGRIDApiKey))
        }
        //console.log('_send_email start: ' + moment().format())
        Promise.all(tasks)
            .then((res) => {
                //console.log('_send_email end: ' + moment().format())
                resolve(res)
            })
            .catch(err => {
                logger.log(err)
                ret_val = ERRCODE.UNKNOWN_ERROR
                reject(ret_val);
                return;
            })
    })
}

// async function send_invite1(payload) {
//     return new Promise(async (resolve, reject) => {
//         const { subject, description, location, start, end, organizer, attendees } = payload;

//         // 1. Generate ICS content
//         const { error, value } = createEvent({
//             start: start,
//             end: end,
//             title: subject,
//             description,
//             location,
//             organizer: { name: organizer.name, email: organizer.email },
//             attendees: attendees.map(a => ({
//                 name: a.name,
//                 email: a.email,
//                 rsvp: a.rsvp,
//                 partstat: 'NEEDS-ACTION',
//                 role: 'REQ-PARTICIPANT'
//             })),
//             method: 'REQUEST',
//             productId: 'MyCompanySchedulingAPI',
//             uid: Date.now() + '@mycompany.com'
//         });

//         if (error) {
//             console.error('Error generating ICS:', error);
//             return reject(error);
//         }
//         // 2. Send ICS content via email
//         const icsContent = value;
//         let mg = mailgun({ apiKey: config.MAILGUN_API_KEY, domain: "e2m.live" });
//         const attachment = new mg.Attachment({
//             //   data: Buffer.from(calendarContent, 'utf-8'),
//             data: Buffer.from(icsContent, 'utf-8'),
//             filename: 'invite.ics',
//             contentType: 'text/calendar',
//         });
//         const emailPayload = {
//             from: "noreply@mail.e2m.live",
//             to: "debashis.giri@webspiders.com",//attendees.map(a => a.email),
//             subject: `Invitation: ${subject}`,
//             // text: `You are invited to the event "${subject}".`,
//             html: `<p>You are invited to the event ${subject}.</p>`,
//             attachments: attachment
//             // attachments: [
//             //     {
//             //         filename: 'invite.ics',
//             //         content: icsContent,
//             //         contentType: 'text/calendar'
//             //     }
//             // ]
//         };
//         try {
//             const result = await send_email_via_mailgun(emailPayload)
//             resolve(result);
//         } catch (error) {
//             console.error('Error sending email:', error);
//             reject(error);
//         }
//     })
// }

// async function send_invite2(payload) {
//     return new Promise(async (resolve, reject) => {
//         const { subject, description, location, start, end, duration_m, organizer, attendees } = payload;
//         const event = {
//             start: [2025, 4, 30, 15, 0],
//             duration: { minutes: duration_m },
//             title: 'One 2 One Meeting',
//             description: '121 Event Meeting - 30 April 2025',
//             location: 'UK',
//             status: 'CONFIRMED',
//             organizer: { name: 'Debashis Giri', email: 'debashis.giri@webspiders.com' },
//         };

//         // 1. Generate ICS content
//         const { error, value } = createEvent(event);
//         if (error) {
//             console.error('Error generating ICS:', error);
//             return reject(error);
//         }
//         const icsContent = value;

//         // // const filePath = `${__dirname}/event.ics`;
//         // const filePath = path.join(__dirname, 'event.ics');
//         // fs.writeFileSync(filePath, value);
//         let mg = mailgun({ apiKey: config.MAILGUN_API_KEY, domain: "e2m.live" });

//         // 2. Send ICS content via email
//         const attachment = new mg.Attachment({
//             //   data: Buffer.from(calendarContent, 'utf-8'),
//             data: Buffer.from(icsContent, 'utf-8'),
//             filename: 'invite.ics',
//             contentType: 'text/calendar',
//         });
//         const data = {
//             from: 'noreply@mail.e2m.live',
//             to: 'debashis.giri@webspiders.com',
//             subject: 'Meeting Invite',
//             html: '<p>You are invited to the event.</p>',
//             attachment: attachment
//             // {
//             //     filename: 'invite.ics',
//             //     data: icsContent,
//             //     contentType: 'text/calendar'
//             // }

//             // attachments: [
//             //     {
//             //         filename: 'event.ics',
//             //         path: filePath,
//             //         contentType: 'text/calendar',
//             //     },
//             // ],
//         };
//         console.log("send_invite data: ", data);
//         mg.messages().send(data, function (error, body) {
//             if (error) {
//                 console.error('Mailgun send error:', error);
//             } else {
//                 console.log('Email sent:', body);
//             }
//         });
//     })
// }

// async function send_invite3(payload) {
//     return new Promise(async (resolve, reject) => {
//         const attachmentICS = await createICS(payload);
//         let mg = mailgun({ apiKey: config.MAILGUN_API_KEY, domain: "e2m.live" });
//         const attachment = new mg.Attachment(attachmentICS);
//         const data = {
//             from: 'noreply@mail.e2m.live',
//             to: 'debashis.giri@webspiders.com',
//             subject: 'Meeting Invite',
//             html: '<p>You are invited to the event.</p>',
//             attachment: attachment
//         };
//         console.log("send_invite data: ", data);
//         mg.messages().send(data, function (error, body) {
//             if (error) {
//                 console.error('Mailgun send error:', error);
//             } else {
//                 console.log('Email sent:', body);
//             }
//         });
//     })
// }

// async function send_invite(payload) {
//     return new Promise(async (resolve, reject) => {
//         const attachmentICS = await createICS(payload);

//         const data = {
//             attachmentICS: attachmentICS
//         };

//         // // const result = await send_email_via_sendgrid(data)
//         const result = await send_email_via_mailgun(data)
//         // let mg = mailgun({ apiKey: config.MAILGUN_API_KEY, domain: "e2m.live" });
//         // const attachment = new mg.Attachment(attachmentICS);
//         // console.log("send_invite data: ", data);
//         // mg.messages().send(data, function (error, body) {
//         //     if (error) {
//         //         console.error('Mailgun send error:', error);
//         //     } else {
//         //         console.log('Email sent:', body);
//         //     }
//         // });
//     })
// }

async function createICS(payload) {
    return new Promise(async (resolve, reject) => {
        let attachment = null;
        const { start, duration_m, title, description, event_location, geo_location, status, organizer, event_url, attendees } = payload;

        const event = {
            start: start,//[2025, 4, 30, 15, 0],
            duration: { minutes: duration_m },
            title: title,
            description: description,
            location: event_location,
            status: status,
            organizer: organizer,
            // url: event_url || "",
            // geo: geo_location,--
            // alarms: alerms,--
            // attendees: attendees,
        };

        // 1. Generate ICS content
        const { error, value } = createEvent(event);
        if (error) {
            console.error('Error generating ICS:', error);
            // return reject(error);
        }
        const icsContent = value;

        // const filePath = path.join(__dirname, 'event.ics');
        // fs.writeFileSync(filePath, value);
        attachment = {
            //   data: Buffer.from(calendarContent, 'utf-8'),
            data: Buffer.from(icsContent, 'utf-8'),
            filename: 'invite.ics',
            contentType: 'text/calendar',
        };
        resolve(attachment)
    })
}

module.exports = {
    createICS: createICS,
    // sendInvite: send_invite,
    getEmailTemplate: get_email_template,
    sendEmail_: send_email,
    sendPush: send_push,
    sendMeetingPush: send_meeting_push,
    sendPushAttendee: send_push_attendee,
    savePushAttendee: save_push_attendee,
    sendEmail: send_email_via_sendgrid,
    sendEmailSMTP: send_email_via_smtp,
    sendScheduledNotification: send_scheduled_Notification,
    send_email_via_mailgun: send_email_via_mailgun,
    send_test_email_via_mailgun: send_test_email_via_mailgun,
    send_test_email_via_sendgrid: send_test_email_via_sendgrid
}