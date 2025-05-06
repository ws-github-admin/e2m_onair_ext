"use strict";

const meeting = require("./lib/meeting");
const vcard = require("./lib/vcard");
const user = require("./lib/user");
const logger = require("./lib/logger");
const mysql = require("./lib/mysql");
const validate = require("./lib/validator");
const { ERRCODE } = require('./lib/errcode');

/* M E E T I N G  H A N D L E R S */
function mysql_connection(req, res) {
    let params = {
        GET: {
            methodToCall: mysql.mysqlConnection,
            methodNameText: "mysql.mysqlConnection",
            allowedRoles: [],
            isPrivate: false,
        }
    };
    let allowed_methods = Object.keys(params);
    res = _set_cors(req, res, allowed_methods);
    _handle_request(req, params)
        .then((result) => {
            if (result.status == 204) {
                return res.status(204).send("");
            }
            return res.status(200).send(result);
        })
        .catch((err) => {
            return res.status(200).send(err);
        });
}
function meeting_qna(req, res) {
    let payload = {}
    if (req.method == 'GET') {
        payload = {
            key: {
                instanceId: (req.query && req.query.iid) ? req.query.iid : 'OA_UAT',
                clientId: (req.query && req.query.cid) ? req.query.cid : 'C1742212403583',
                eventId: (req.query && req.query.eid) ? req.query.eid : 'E1742214690559'
            }
        };
    }
    let params = {
        GET: {
            methodToCall: meeting.getMeetingQnA,
            methodNameText: "meeting.getMeetingQnA",
            methodPayload: payload,
            allowedRoles: [],
            isPrivate: true,
        },
        POST: {
            methodToCall: meeting.getMeetingQnA,
            methodNameText: "meeting.getMeetingQnA",
            allowedRoles: [],
            isPrivate: true,
        },
        PUT: {
            methodToCall: meeting.setMeetingQnA,
            methodNameText: "meeting.setMeetingQnA",
            allowedRoles: [],
            isPrivate: true,
        },
    };
    let allowed_methods = Object.keys(params);
    res = _set_cors(req, res, allowed_methods);
    _handle_request(req, params)
        .then((result) => {
            if (result.status == 204) {
                return res.status(204).send("");
            }
            return res.status(200).send(result);
        })
        .catch((err) => {
            return res.status(200).send(err);
        });
}
function available_attendees(req, res) {
    let params = {
        POST: {
            methodToCall: meeting.availableAttendees,
            methodNameText: "meeting.availableAttendees",
            allowedRoles: [],
            isPrivate: true,
        }
    };
    let allowed_methods = Object.keys(params);
    res = _set_cors(req, res, allowed_methods);
    _handle_request(req, params)
        .then((result) => {
            if (result.status == 204) {
                return res.status(204).send("");
            }
            return res.status(200).send(result);
        })
        .catch((err) => {
            return res.status(200).send(err);
        });
}
function available_speakers(req, res) {
    let params = {
        POST: {
            methodToCall: meeting.availableSpeakers,
            methodNameText: "meeting.availableSpeakers",
            allowedRoles: [],
            isPrivate: true,
        }
    };
    let allowed_methods = Object.keys(params);
    res = _set_cors(req, res, allowed_methods);
    _handle_request(req, params)
        .then((result) => {
            if (result.status == 204) {
                return res.status(204).send("");
            }
            return res.status(200).send(result);
        })
        .catch((err) => {
            return res.status(200).send(err);
        });
}
function available_sponsors(req, res) {
    let params = {
        POST: {
            methodToCall: meeting.availableSponsors,
            methodNameText: "meeting.availableSponsors",
            allowedRoles: [],
            isPrivate: true,
        }
    };
    let allowed_methods = Object.keys(params);
    res = _set_cors(req, res, allowed_methods);
    _handle_request(req, params)
        .then((result) => {
            if (result.status == 204) {
                return res.status(204).send("");
            }
            return res.status(200).send(result);
        })
        .catch((err) => {
            return res.status(200).send(err);
        });
}
function user_info(req, res) {
    let params = {
        POST: {
            methodToCall: user.userInfo,
            methodNameText: "user.userInfo",
            allowedRoles: [],
            isPrivate: false,
        },
        PUT: {
            isPrivate: true,
            methodToCall: user.userUpdate,
            methodNameText: "user.userUpdate",
            allowedRoles: [],
        }
    };
    let allowed_methods = Object.keys(params);
    res = _set_cors(req, res, allowed_methods);
    _handle_request(req, params)
        .then((result) => {
            if (result.status == 204) {
                return res.status(204).send("");
            }
            return res.status(200).send(result);
        })
        .catch((err) => {
            return res.status(200).send(err);
        });
}
function upload_files(req, res) {
    let params = {
        POST: {
            methodToCall: user.uploadFiles,
            methodNameText: "user.uploadFiles",
            allowedRoles: [],
            isPrivate: true,
        },
    };
    let allowed_methods = Object.keys(params);
    res = _set_cors(req, res, allowed_methods);
    _handle_request(req, params)
        .then((result) => {
            if (result.status == 204) {
                return res.status(204).send("");
            }
            return res.status(200).send(result);
        })
        .catch((err) => {
            return res.status(200).send(err);
        });
}
function draft_attendees(req, res) {
    let payload = {}
    console.log("req.method====", req.method);
    if (req.method == 'GET') {
        payload = {
            key: {
                instanceId: (req.query && req.query.iid) ? req.query.iid : 'OA_UAT',
                clientId: (req.query && req.query.cid) ? req.query.cid : 'C1742212403583',
                eventId: (req.query && req.query.eid) ? req.query.eid : 'E1742214690559'
            }, data: {
                sponsorId: (req.query && req.query.sid) ? req.query.sid : '',
            }
        };
    }

    let params = {
        GET: {
            methodToCall: meeting.draftAttendees,
            methodNameText: "meeting.draftAttendees",
            methodPayload: payload,
            allowedRoles: [],
            isPrivate: true,
        },
        POST: {
            methodToCall: meeting.saveAsDraft,
            methodNameText: "meeting.saveAsDraft",
            allowedRoles: [],
            isPrivate: true,
        },
        DELETE: {
            methodToCall: meeting.removeFromDraft,
            methodNameText: "meeting.removeFromDraft",
            allowedRoles: [],
            isPrivate: true,
        }
    };
    let allowed_methods = Object.keys(params);
    res = _set_cors(req, res, allowed_methods);
    _handle_request(req, params)
        .then((result) => {
            if (result.status == 204) {
                return res.status(204).send("");
            }
            return res.status(200).send(result);
        })
        .catch((err) => {
            return res.status(200).send(err);
        });
}
function meeting_attendees(req, res) {
    let params = {
        POST: {
            methodToCall: meeting.meetingAttendees,
            methodNameText: "meeting.meetingAttendees",
            allowedRoles: [],
            isPrivate: true,
        }
    };
    let allowed_methods = Object.keys(params);
    res = _set_cors(req, res, allowed_methods);
    _handle_request(req, params)
        .then((result) => {
            if (result.status == 204) {
                return res.status(204).send("");
            }
            return res.status(200).send(result);
        })
        .catch((err) => {
            return res.status(200).send(err);
        });
}
function attendee_meetings(req, res) {
    let params = {
        POST: {
            methodToCall: meeting.attendeeMeetings,
            methodNameText: "meeting.attendeeMeetings",
            allowedRoles: [],
            isPrivate: true,
        }
    };
    let allowed_methods = Object.keys(params);
    res = _set_cors(req, res, allowed_methods);
    _handle_request(req, params)
        .then((result) => {
            if (result.status == 204) {
                return res.status(204).send("");
            }
            return res.status(200).send(result);
        })
        .catch((err) => {
            return res.status(200).send(err);
        });
}
function available_slots(req, res) {
    let params = {
        POST: {
            methodToCall: meeting.availableSlots,
            methodNameText: "meeting.availableSlots",
            allowedRoles: [],
            isPrivate: false,
        }
    };
    let allowed_methods = Object.keys(params);
    res = _set_cors(req, res, allowed_methods);
    _handle_request(req, params)
        .then((result) => {
            if (result.status == 204) {
                return res.status(204).send("");
            }
            return res.status(200).send(result);
        })
        .catch((err) => {
            return res.status(200).send(err);
        });
}
function meeting_config(req, res) {
    let params = {
        POST: {
            methodToCall: meeting.meetingConfig,
            methodNameText: "meeting.meetingConfig",
            allowedRoles: [],
            isPrivate: false,
        }
    };
    let allowed_methods = Object.keys(params);
    res = _set_cors(req, res, allowed_methods);
    _handle_request(req, params)
        .then((result) => {
            if (result.status == 204) {
                return res.status(204).send("");
            }
            return res.status(200).send(result);
        })
        .catch((err) => {
            return res.status(200).send(err);
        });
}
function get_meetings(req, res) {
    let params = {
        POST: {
            methodToCall: meeting.getMeetings,
            methodNameText: "meeting.getMeetings",
            allowedRoles: [],
            isPrivate: true,
        }
    };
    let allowed_methods = Object.keys(params);
    res = _set_cors(req, res, allowed_methods);
    _handle_request(req, params)
        .then((result) => {
            if (result.status == 204) {
                return res.status(204).send("");
            }
            return res.status(200).send(result);
        })
        .catch((err) => {
            return res.status(200).send(err);
        });
}
function get_meeting_detail(req, res) {
    let params = {
        POST: {
            methodToCall: meeting.getMeetingDetail,
            methodNameText: "meeting.getMeetingDetail",
            allowedRoles: [],
            isPrivate: false,
        },
    };
    let allowed_methods = Object.keys(params);
    res = _set_cors(req, res, allowed_methods);
    _handle_request(req, params)
        .then((result) => {
            if (result.status == 204) {
                return res.status(204).send("");
            }
            return res.status(200).send(result);
        })
        .catch((err) => {
            return res.status(200).send(err);
        });
}

function consolidated_send_email(req, res) {
    let params = {
        POST: {
            methodToCall: meeting.consolidatedSendEmail,
            methodNameText: "meeting.consolidatedSendEmail",
            allowedRoles: [],
            isPrivate: false,
        },
    };
    let allowed_methods = Object.keys(params);
    res = _set_cors(req, res, allowed_methods);
    _handle_request(req, params)
        .then((result) => {
            if (result.status == 204) {
                return res.status(204).send("");
            }
            return res.status(200).send(result);
        })
        .catch((err) => {
            return res.status(200).send(err);
        });
}
function request_meetings(req, res) {
    let params = {
        POST: {
            methodToCall: meeting.requestMeetings,
            methodNameText: "meeting.requestMeetings",
            allowedRoles: [],
            isPrivate: true,
        },
    };
    let allowed_methods = Object.keys(params);
    res = _set_cors(req, res, allowed_methods);
    _handle_request(req, params)
        .then((result) => {
            if (result.status == 204) {
                return res.status(204).send("");
            }
            return res.status(200).send(result);
        })
        .catch((err) => {
            return res.status(200).send(err);
        });
}
function confirm_meeting(req, res) {
    let params = {
        POST: {
            methodToCall: meeting.confirmMeeting,
            methodNameText: "meeting.confirmMeeting",
            allowedRoles: [],
            isPrivate: true,
        },
    };
    let allowed_methods = Object.keys(params);
    res = _set_cors(req, res, allowed_methods);
    _handle_request(req, params)
        .then((result) => {
            if (result.status == 204) {
                return res.status(204).send("");
            }
            return res.status(200).send(result);
        })
        .catch((err) => {
            return res.status(200).send(err);
        });
}
function accept_meeting(req, res) {
    let params = {
        POST: {
            methodToCall: meeting.acceptMeeting,
            methodNameText: "meeting.acceptMeeting",
            allowedRoles: [],
            isPrivate: false,
        },
    };
    let allowed_methods = Object.keys(params);
    res = _set_cors(req, res, allowed_methods);
    _handle_request(req, params)
        .then((result) => {
            if (result.status == 204) {
                return res.status(204).send("");
            }
            return res.status(200).send(result);
        })
        .catch((err) => {
            return res.status(200).send(err);
        });
}
function cancel_meeting(req, res) {
    let params = {
        POST: {
            methodToCall: meeting.cancelMeeting,
            methodNameText: "meeting.cancelMeeting",
            allowedRoles: [],
            isPrivate: false,
        },
    };
    let allowed_methods = Object.keys(params);
    res = _set_cors(req, res, allowed_methods);
    _handle_request(req, params)
        .then((result) => {
            if (result.status == 204) {
                return res.status(204).send("");
            }
            return res.status(200).send(result);
        })
        .catch((err) => {
            return res.status(200).send(err);
        });
}
function validate_meeting(req, res) {
    let params = {
        POST: {
            methodToCall: meeting.validateMeeting,
            methodNameText: "meeting.validateMeeting",
            allowedRoles: [],
            isPrivate: false,
        },
    };
    let allowed_methods = Object.keys(params);
    res = _set_cors(req, res, allowed_methods);
    _handle_request(req, params)
        .then((result) => {
            if (result.status == 204) {
                return res.status(204).send("");
            }
            return res.status(200).send(result);
        })
        .catch((err) => {
            return res.status(200).send(err);
        });
}
function ai_confirm_meeting(req, res) {
    let params = {
        POST: {
            methodToCall: meeting.aiConfirmMeeting,
            methodNameText: "meeting.aiConfirmMeeting",
            allowedRoles: [],
            isPrivate: true,
        },
    };
    let allowed_methods = Object.keys(params);
    res = _set_cors(req, res, allowed_methods);
    _handle_request(req, params)
        .then((result) => {
            if (result.status == 204) {
                return res.status(204).send("");
            }
            return res.status(200).send(result);
        })
        .catch((err) => {
            return res.status(200).send(err);
        });
}
function meeting_reminder(req, res) {
    let params = {
        POST: {
            methodToCall: meeting.meetingReminder,
            methodNameText: "meeting.meetingReminder",
            allowedRoles: [],
            isPrivate: true,
        },
    };
    let allowed_methods = Object.keys(params);
    res = _set_cors(req, res, allowed_methods);
    _handle_request(req, params)
        .then((result) => {
            if (result.status == 204) {
                return res.status(204).send("");
            }
            return res.status(200).send(result);
        })
        .catch((err) => {
            return res.status(200).send(err);
        });
}
function scan_vcard(req, res) {
    let params = {
        POST: {
            methodToCall: vcard.scanVCard,
            methodNameText: "meeting.scanVCard",
            allowedRoles: [],
            isPrivate: true,
        },
    };
    let allowed_methods = Object.keys(params);
    res = _set_cors(req, res, allowed_methods);
    _handle_request(req, params)
        .then((result) => {
            if (result.status == 204) {
                return res.status(204).send("");
            }
            return res.status(200).send(result);
        })
        .catch((err) => {
            return res.status(200).send(err);
        });
}
function scan_rating(req, res) {
    let params = {
        POST: {
            methodToCall: vcard.scanRating,
            methodNameText: "meeting.scanRating",
            allowedRoles: [],
            isPrivate: true,
        },
    };
    let allowed_methods = Object.keys(params);
    res = _set_cors(req, res, allowed_methods);
    _handle_request(req, params)
        .then((result) => {
            if (result.status == 204) {
                return res.status(204).send("");
            }
            return res.status(200).send(result);
        })
        .catch((err) => {
            return res.status(200).send(err);
        });
}
function share_vcard(req, res) {
    let params = {
        POST: {
            methodToCall: vcard.shareVCard,
            methodNameText: "meeting.shareVCard",
            allowedRoles: [],
            isPrivate: true,
        },
    };
    let allowed_methods = Object.keys(params);
    res = _set_cors(req, res, allowed_methods);
    _handle_request(req, params)
        .then((result) => {
            if (result.status == 204) {
                return res.status(204).send("");
            }
            return res.status(200).send(result);
        })
        .catch((err) => {
            return res.status(200).send(err);
        });
}
async function on_sms_received(req, res) {
    let twiml = `<?xml version="1.0" encoding="UTF-8"?>
<Response>
    <Message>We got your message, thank you!</Message>
</Response>`;

    try {
        await meeting.onSmsReplied(req.body);
    } catch (ex) {
        console.log(ex);
    }

    res.set('Content-Type', 'text/xml');
    res.status(200).send(twiml);
}


async function pubsub_request_meeting(message, context) {
    try {
        if (message.data) {
            let payload = JSON.parse(Buffer.from(message.data, "base64").toString());
            await meeting.pubsubRequestMeeting(payload)
        }
    } catch (err) {
        logger.log(err);
    }
    return;
}
async function pubsub_confirm_meeting(message, context) {
    try {
        if (message.data) {
            let payload = JSON.parse(Buffer.from(message.data, "base64").toString());
            await meeting.pubsubConfirmMeeting(payload)
        }
    } catch (err) {
        logger.log(err);
    }
    return;
}
async function pubsub_share_vcard(message, context) {
    try {
        if (message.data) {
            let payload = JSON.parse(Buffer.from(message.data, "base64").toString());
            await vcard.pubsubShareVCard(payload)
        }
    } catch (err) {
        logger.log(err);
    }
    return;
}
async function pubsub_scan_vcard(message, context) {
    try {
        if (message.data) {
            let payload = JSON.parse(Buffer.from(message.data, "base64").toString());
            await vcard.pubsubScanVCard(payload)
        }
    } catch (err) {
        logger.log(err);
    }
    return;
}

function send_sms_to_user(req, res) {
    let params = {
        POST: {
            methodToCall: meeting.sendSMSToUser,
            methodNameText: "meeting.sendSMSToUser",
            allowedRoles: [],
            isPrivate: true,
        },
    };
    let allowed_methods = Object.keys(params);
    res = _set_cors(req, res, allowed_methods);
    _handle_request(req, params)
        .then((result) => {
            if (result.status == 204) {
                return res.status(204).send("");
            }
            return res.status(200).send(result);
        })
        .catch((err) => {
            return res.status(200).send(err);
        });
}

function send_sms_to_attendee(req, res) {
    let params = {
        POST: {
            methodToCall: meeting.sendSMSToAttendee,
            methodNameText: "meeting.sendSMSToAttendee",
            allowedRoles: [],
            isPrivate: true,
        },
    };
    let allowed_methods = Object.keys(params);
    res = _set_cors(req, res, allowed_methods);
    _handle_request(req, params)
        .then((result) => {
            if (result.status == 204) {
                return res.status(204).send("");
            }
            return res.status(200).send(result);
        })
        .catch((err) => {
            return res.status(200).send(err);
        });
}

/* S U P P O R T I N G  M E T H O D S */
function _set_cors(req, res, allowed_methods) {
    res.set("Access-Control-Allow-Origin", "*");
    res.set("Access-Control-Allow-Methods", allowed_methods.toString());
    res.set(
        "Access-Control-Allow-Headers",
        "Authorization, Origin, X-Requested-With, Content-Type, Accept, apikey, x-api-key,instanceId,eventId,clientId"
    );
    res.set("Access-Control-Max-Age", "3600");
    res.set("Access-Control-Allow-Credentials", "true");
    return res;
}

function _handle_request(req, params) {
    return new Promise(async (resolve, reject) => {
        let ret_val = { status: -1 };
        let payload = { data: {} };
        console.log("_handle_request");
        //logger.log(params);
        logger.log(params[req.method]);
        //logger.log(req.headers);
        if (req.method == "OPTIONS") {
            ret_val = ERRCODE.PREFLIGHT;
            resolve(ret_val);
            return;
        }

        validate
            .__call(req, params)
            .then((res) => {
                //console.log(res)
                payload = params[req.method].methodPayload || req.body || {};
                payload.method = req.method;
                try {
                    payload.domain = Url(req.headers.origin, true).host || "";
                } catch (err) {
                    payload.domain = payload.domain || "";
                    //logger.log('Could not determine domain from request')
                }

                try {
                    payload.user_agent = req.get("User-Agent") || "";
                } catch (err) {
                    payload.user_agent = payload.user_agent || "";
                    //logger.log('Could not determine User-Agent from request')
                }
                try {
                    payload.ip =
                        req.headers["x-forwarded-for"] ||
                        req.connection.remoteAddress ||
                        req.socket.remoteAddress ||
                        (req.connection.socket ? req.connection.socket.remoteAddress : "");
                } catch (err) {
                    payload.ip = payload.ip || "";
                    //logger.log('Could not determine IP Address from request')
                }

                payload.data = payload.data || {};
                payload.auth = res;
                //payload.req={headers:req.headers}
                //logger.log(params[req.method].methodNameText);
                return params[req.method].methodToCall(payload);
            })
            .then((res) => {
                resolve(res);
            })
            .catch((err) => {
                console.log(err);
                if (!err.status) {
                    ret_val = ERRCODE.UNKNOWN_ERROR;
                    reject(ret_val);
                    return;
                }
                reject(err);
            });
    });
}

module.exports = {
    meetingConfig: meeting_config,
    getMeetings: get_meetings,
    getMeetingDetail: get_meeting_detail,
    meetingQnA: meeting_qna,
    mysqlConnection: mysql_connection,
    availableAttendees: available_attendees,
    availableSpeakers: available_speakers,
    availableSponsors: available_sponsors,
    userInfo: user_info,
    uploadFiles: upload_files,
    draftAttendees: draft_attendees,
    requestMeetings: request_meetings,
    meetingAttendees: meeting_attendees,
    attendeeMeetings: attendee_meetings,
    availableSlots: available_slots,
    confirmMeeting: confirm_meeting,
    cancelMeeting: cancel_meeting,
    validateMeeting: validate_meeting,
    acceptMeeting: accept_meeting,
    aiConfirmMeeting: ai_confirm_meeting,
    meetingReminder: meeting_reminder,
    scanVCard: scan_vcard,
    scanRating: scan_rating,
    shareVCard: share_vcard,
    onSmsReceived: on_sms_received,
    pubsubRequestMeeting: pubsub_request_meeting,
    pubsubConfirmMeeting: pubsub_confirm_meeting,
    pubsubScanVCard: pubsub_scan_vcard,
    pubsubShareVCard: pubsub_share_vcard,
    consolidatedSendEmail: consolidated_send_email,
    sendSMSToUser: send_sms_to_user,
    sendSMSToAttendee: send_sms_to_attendee,
};
