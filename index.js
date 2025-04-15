"use strict";

const meeting = require("./lib/meeting");
const vcard = require("./lib/vcard");
const user = require("./lib/user");
const logger = require("./lib/logger");
const mysql = require("./lib/mysql");
const validator = require("./lib/validator");
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



function draft_attendees(req, res) {
    let payload = {}
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

function get_meetings(req, res) {
    let params = {
        POST: {
            methodToCall: meeting.getMeetings,
            methodNameText: "meeting.getMeetings",
            allowedRoles: [],
            isPrivate: true,
        },
        PUT: {
            methodToCall: meeting.getAllMeetings,
            methodNameText: "meeting.getAllMeetings",
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

function get_meeting_detail(req, res) {
    let params = {
        POST: {
            methodToCall: meeting.getMeetingDetail,
            methodNameText: "meeting.getMeetingDetail",
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

function get_meeting_slots(req, res) {
    let params = {
        POST: {
            methodToCall: meeting.getMeetingSlots,
            methodNameText: "meeting.getMeetingSlots",
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

function create_meetings(req, res) {
    let params = {
        POST: {
            methodToCall: meeting.createMeetings,
            methodNameText: "meeting.createMeetings",
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

function create_meeting(req, res) {
    let params = {
        POST: {
            methodToCall: meeting.createMeeting,
            methodNameText: "meeting.createMeeting",
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

function store_meetings(req, res) {
    let params = {
        POST: {
            methodToCall: meeting.storeMeetings,
            methodNameText: "meeting.storeMeetings",
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

function request_meeting(req, res) {
    let params = {
        POST: {
            methodToCall: meeting.requestMeeting,
            methodNameText: "meeting.requestMeeting",
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






function cancel_meeting(req, res) {
    let params = {
        POST: {
            methodToCall: meeting.cancelMeeting,
            methodNameText: "meeting.cancelMeeting",
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

function reject_meeting(req, res) {
    let params = {
        POST: {
            methodToCall: meeting.rejectMeeting,
            methodNameText: "meeting.rejectMeeting",
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

function delete_meeting(req, res) {
    let params = {
        POST: {
            methodToCall: meeting.deleteMeetings,
            methodNameText: "meeting.deleteMeetings",
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

function meeting_config(req, res) {
    let params = {
        POST: {
            methodToCall: meeting.getMeetingConfig,
            methodNameText: "meeting.getMeetingConfig",
            allowedRoles: [],
            isPrivate: true,
        },
        PUT: {
            methodToCall: meeting.setMeetingConfig,
            methodNameText: "meeting.setMeetingConfig",
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

function download_meetings(req, res) {
    let payload = { key: {} };

    // Handle GET request and URL decode the email
    if (req.method == 'GET' && req.query) {
        payload.key.instanceId = (req.query.iid || 'OA_UAT');
        payload.key.clientId = (req.query.cid || '');
        payload.key.eventId = (req.query.eid || '');

        // Decode the email if present
        payload.data = {
            AttendeeId: req.query.uid || '',
            Email: req.query.email ? decodeURIComponent(req.query.email) : '',
        };
    }

    // Parameters for handling GET and POST requests
    let params = {
        GET: {
            methodToCall: meeting.downloadMeetings,
            methodPayload: payload,
            methodNameText: "meeting.downloadMeetings",
            allowedRoles: [],
            isPrivate: false
        },
        POST: {
            methodToCall: meeting.downloadMeetings,
            methodNameText: "meeting.downloadMeetings",
            allowedRoles: [],
            isPrivate: false,
        }
    };

    let allowed_methods = Object.keys(params);
    res = _set_cors(req, res, allowed_methods);

    // Handle request
    _handle_request(req, params)
        .then((result) => {
            if (result.status == 204) {
                return res.status(204).send("");
            }

            // Convert base64 to Buffer
            const buffer = Buffer.from(result.file_data, 'base64');

            // If it's a GET request, return the file for download
            if (req.method === 'GET') {
                // Set response headers for file download
                res.setHeader('Content-Disposition', `attachment; filename="${result.file_name}.xlsx"`);
                res.setHeader('Content-Type', 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet');

                // Send the file buffer for download
                return res.status(200).send(buffer);
            }

            // If it's a POST request, return the base64 data
            if (req.method === 'POST') {
                // Return base64 data directly in response
                return res.status(200).json({ file_data: result.file_data });
            }
        })
        .catch((err) => {
            return res.status(500).send(err);
        });
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

async function pubsub_cancel_meeting(message, context) {
    try {
        if (message.data) {
            let payload = JSON.parse(Buffer.from(message.data, "base64").toString());
            await meeting.pubsubCancelMeeting(payload)
        }
    } catch (err) {
        logger.log(err);
    }
    return;
}

async function pubsub_reject_meeting(message, context) {
    try {
        if (message.data) {
            let payload = JSON.parse(Buffer.from(message.data, "base64").toString());
            await meeting.pubsubRejectMeeting(payload)
        }
    } catch (err) {
        logger.log(err);
    }
    return;
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
        //logger.log(params[req.method]);
        //logger.log(req.headers);
        if (req.method == "OPTIONS") {
            ret_val = ERRCODE.PREFLIGHT;
            resolve(ret_val);
            return;
        }

        validator
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
    createMeetings: create_meetings,
    createMeeting: create_meeting,
    storeMeetings: store_meetings,
    getMeetings: get_meetings,
    getMeetingSlots: get_meeting_slots,
    getMeetingDetail: get_meeting_detail,
    requestMeeting: request_meeting,
    pubsubRequestMeeting: pubsub_request_meeting,
    cancelMeeting: cancel_meeting,
    pubsubCancelMeeting: pubsub_cancel_meeting,
    rejectMeeting: reject_meeting,
    pubsubRejectMeeting: pubsub_reject_meeting,
    deleteMeeting: delete_meeting,
    meetingConfig: meeting_config,
    downloadMeetings: download_meetings,
    meetingQnA: meeting_qna,
    mysqlConnection: mysql_connection,
    availableAttendees: available_attendees,
    availableSpeakers: available_speakers,
    availableSponsors: available_sponsors,
    userInfo: user_info,
    draftAttendees: draft_attendees,
    requestMeetings: request_meetings,
    meetingAttendees: meeting_attendees,
    attendeeMeetings: attendee_meetings,
    confirmMeeting: confirm_meeting,
    pubsubConfirmMeeting: pubsub_confirm_meeting,
    meetingReminder: meeting_reminder,
    scanVCard: scan_vcard,
    pubsubScanVCard: pubsub_scan_vcard,
    scanRating: scan_rating,
    shareVCard: share_vcard,
    pubsubShareVCard: pubsub_share_vcard
};