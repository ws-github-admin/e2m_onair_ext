const Handlebars = require("handlebars");
const moment = require('moment');
const { Firestore } = require('@google-cloud/firestore');
const { Storage } = require('@google-cloud/storage');
const { PubSub } = require('@google-cloud/pubsub');
const utils = require('./utils');
const config = require('../config.json');
const logger = require('./logger');
const { ERRCODE } = require('./errcode');


const pubSubClient = new PubSub({
    projectId: config.GCP.PROJECT_ID,
    keyFilename: (__dirname + config.GCP.KEY_FILE_PATH).replace('/lib/', '/')
});

const dbClient = new Firestore({
    projectId: config.GCP.PROJECT_ID,
    keyFilename: (__dirname + config.GCP.KEY_FILE_PATH).replace('/lib/', '/')
});

//const bucketName = (config.GCP.PROJECT_ID + '.appspot.com');
const bucketName = (config.FIREBASE_CONFIG.storageBucket);
const storageClient = new Storage({
    projectId: config.GCP.PROJECT_ID,
    keyFilename: (__dirname + config.GCP.KEY_FILE_PATH).replace('/lib/', '/')
});

async function scan_vcard(payload) {
    return new Promise(async (resolve, reject) => {
        let ret_val = { status: -1 };
        try {
            logger.log(payload);
            const pk = payload.key;
            const pd = payload.data;
            const pa = payload.auth.data;

            const event_base_path = `${pk.instanceId}_${pk.clientId}${pk.eventId}`;
            const scannedByAttendeeDocPath = `${event_base_path}/AttendeeList/Attendees/${pa.UserId}`;
            const scannedByAttendeeRef = dbClient.doc(scannedByAttendeeDocPath);

            // Retrieve the attendee associated with the provided QR code
            const scannedAttendeeSnap = await dbClient
                .collection(`${event_base_path}/AttendeeList/Attendees`)
                .where("VCard.EXT_QRCODE", "==", pd.qrCode)
                .limit(1)
                .get();

            if (scannedAttendeeSnap.empty) {
                throw new Error(`No attendee found with QR Code: ${pd.qrCode}`);
            }

            const scannedAttendee = scannedAttendeeSnap.docs[0].data();

            const scannedData = {
                AttendeeId: scannedAttendee.AttendeeId,
                Name: scannedAttendee.Name,
                Email: scannedAttendee.Email,
                Designation: scannedAttendee.Designation,
                Company: scannedAttendee.Company,
                Phone: scannedAttendee.Phone,
                Rating: pd.Rating || 0,
                Comment: pd.Comment || '',
                CreatedDateTime: new Date(),
            };
            const scannedSponsorId = scannedAttendee?.RegistrationType?.RegistrationType?.toLowerCase() === 'sponsor'
                ? scannedAttendee?.RegistrationType?.RegistrationTypeEntityId
                : null;
            if (scannedSponsorId) {
                scannedData.SponsorId = scannedSponsorId;
            }
            const tasks = [];

            // Add scanned attendee to the scanner's VCard.Scanned list
            tasks.push(scannedByAttendeeRef.update({
                'VCard.Scanned': Firestore.FieldValue.arrayUnion(scannedData)
            }, { merge: true }));

            // Check if the scanner is a sponsor representative
            const scannerDoc = await scannedByAttendeeRef.get();
            const scannerData = scannerDoc.exists ? scannerDoc.data() : null;
            const sponsorId = scannerData?.RegistrationType?.RegistrationType?.toLowerCase() === 'sponsor'
                ? scannerData?.RegistrationType?.RegistrationTypeEntityId
                : null;

            // If the scanner is a sponsor representative, add the scanned attendee to the sponsor's VCard.Scanned list
            if (sponsorId) {
                const sponsorDocRef = dbClient.doc(`${event_base_path}/SponsorList/Sponsors/${sponsorId}`);
                const sponsorDoc = await sponsorDocRef.get();
                if (sponsorDoc.exists) {
                    tasks.push(sponsorDocRef.update({
                        'VCard.Scanned': Firestore.FieldValue.arrayUnion(scannedData)
                    }, { merge: true }));
                }
            }

            await Promise.all(tasks);

            ret_val.status = 0;
            ret_val.data = scannedData;
            resolve(ret_val);
        } catch (err) {
            console.error(err);
            ret_val = ERRCODE.UNKNOWN_ERROR;
            reject(ret_val);
        }
    });
}

function scan_vcard_2(payload) {
    return new Promise(async (resolve, reject) => {
        let ret_val = { status: -1 };
        let topicName = "ext-scan-vcard"
        try {
            logger.log(payload)
            let payloadBuffer = Buffer.from(JSON.stringify(payload))
            let message = { data: payloadBuffer }
            let messageId = await pubSubClient.topic(topicName).publishMessage(message)

            ret_val.status = 0
            ret_val.result = `Message ${messageId} published.`
            resolve(ret_val)
        } catch (err) {
            console.log(err)
            ret_val = ERRCODE.UNKNOWN_ERROR
            reject(ret_val);
            return;
        }
    })
}

function pubsub_scan_vcard(payload) {
    return new Promise(async (resolve, reject) => {
        let ret_val = { status: -1 };
        try {
            logger.log(payload);
            let tasks = [];
            let pk = payload.key;
            let pd = payload.data;
            let pa = payload.auth.data;

            let event_base_path = `${pk.instanceId}_${pk.clientId}${pk.eventId}`;
            let scannedByAttendeeDocPath = `${event_base_path}/AttendeeList/Attendees/${pa.UserId}`;
            let scannedByAttendeeRef = dbClient.doc(scannedByAttendeeDocPath);

            let scannedAttendeeSnap = await dbClient
                .collection(`${event_base_path}/AttendeeList/Attendees`)
                .where("VCard.EXT_QRCODE", "==", pd.qrCode)
                .limit(1)
                .get();

            if (scannedAttendeeSnap.empty) {
                throw new Error(`No attendee found with QR Code: ${pd.qrCode}`);
            }
            let scannedAttendee = scannedAttendeeSnap.docs[0].data();

            const scannedData = {
                AttendeeId: scannedAttendee.AttendeeId,
                Name: scannedAttendee.Name,
                Email: scannedAttendee.Email,
                Designation: scannedAttendee.Designation,
                Company: scannedAttendee.Company,
                Phone: scannedAttendee.Phone,
                Rating: pd.Rating || 0,
                Comment: pd.Comment || '',
                CreatedDateTime: new Date(),
            };
            const scannedSponsorId = scannedAttendee?.RegistrationType?.RegistrationType?.toLowerCase() === 'sponsor'
                ? scannedAttendee?.RegistrationType?.RegistrationTypeEntityId
                : null;
            if (scannedSponsorId) {
                scannedData.SponsorId = scannedSponsorId;
            }

            // Add scanned attendee to sponsor rep's VCard.Scanned list
            tasks.push(scannedByAttendeeRef.update({
                'VCard.Scanned': Firestore.FieldValue.arrayUnion(scannedData)
            }, { merge: true }));

            // Check if scanner is a sponsor rep, add to their sponsor too
            const scannerDoc = await scannedByAttendeeRef.get();
            const scannerData = scannerDoc.exists ? scannerDoc.data() : null;
            const sponsorId = scannerData?.RegistrationType?.RegistrationType?.toLowerCase() === 'sponsor'
                ? scannerData?.RegistrationType?.RegistrationTypeEntityId
                : null;

            if (sponsorId) {
                const sponsorDocRef = dbClient.doc(`${event_base_path}/SponsorList/Sponsors/${sponsorId}`);
                const sponsorDoc = await sponsorDocRef.get();
                if (sponsorDoc.exists) {
                    tasks.push(sponsorDocRef.update({
                        'VCard.Scanned': Firestore.FieldValue.arrayUnion(scannedData)
                    }, { merge: true }));
                }
            }

            await Promise.all(tasks);

            let res = await scannedByAttendeeRef.get();
            ret_val.status = 0;
            ret_val.data = res.data().VCard;
            resolve(ret_val);
        } catch (err) {
            console.log(err);
            ret_val = ERRCODE.UNKNOWN_ERROR;
            reject(ret_val);
        }
    });
}

async function scan_rating(payload) {
    try {

        const { instanceId, clientId, eventId } = payload.key;
        const items = payload.data;
        let scannedByAttendeeId = payload.auth.data.UserId;

        if (!Array.isArray(items) || items.length === 0) {
            return res.status(400).json({ status: -1, message: "No items provided" });
        }

        if (!scannedByAttendeeId) {
            return res.status(400).json({ status: -1, message: "Missing scannedByAttendeeId in key" });
        }

        const iceId = `${instanceId}_${clientId}${eventId}`;
        const event_base_path = `/${iceId}`;

        const scannedByRef = dbClient.doc(`${event_base_path}/AttendeeList/Attendees/${scannedByAttendeeId}`);

        const scannedSnapshots = await scannedByRef.get();
        if (!scannedSnapshots.exists) {
            throw new Error(`Attendee ${scannedByAttendeeId} not found`);
        }

        const existingScanned = scannedSnapshots.data().VCard?.Scanned || [];

        const updatedScanned = [...existingScanned];

        for (const { attendeeId, rating, comment } of items) {
            if (!attendeeId) continue;

            const scannedAttendeeRef = dbClient.doc(`${event_base_path}/AttendeeList/Attendees/${attendeeId}`);
            const scannedAttendeeDoc = await scannedAttendeeRef.get();
            if (!scannedAttendeeDoc.exists) {
                throw new Error(`Attendee ${attendeeId} not found`);
            }
            const scannedAttendee = scannedAttendeeDoc.data();

            const existingIndex = updatedScanned.findIndex(item => item.AttendeeId === attendeeId);

            const newEntry = {
                AttendeeId: attendeeId,
                Name: scannedAttendee.Name,
                Email: scannedAttendee.Email,
                Designation: scannedAttendee.Designation,
                Company: scannedAttendee.Company,
                Phone: scannedAttendee.Phone,
                Rating: rating || '',
                Comment: comment || ''
            };

            if (existingIndex !== -1) {
                updatedScanned[existingIndex] = newEntry; // overwrite existing
            } else {
                updatedScanned.push(newEntry); // add new
            }
        }

        await scannedByRef.update({
            'VCard.Scanned': updatedScanned
        });

        res.status(200).json({ status: 0, message: "Ratings and comments updated successfully" });
    } catch (err) {
        console.error("Error in ratings_and_comments:", err);
        res.status(500).json({ status: -1, error: err.message || "Unknown error" });
    }
}





function share_vcard(payload) {
    return new Promise(async (resolve, reject) => {
        let ret_val = { status: -1 };
        let topicName = "ext-share-vcard"
        try {
            logger.log(payload)
            let payloadBuffer = Buffer.from(JSON.stringify(payload));
            let message = { data: payloadBuffer }
            let messageId = await pubSubClient.topic(topicName).publishMessage(message)

            ret_val.status = 0
            ret_val.result = `Message ${messageId} published.`
            resolve(ret_val)
        } catch (err) {
            console.log(err)
            ret_val = ERRCODE.UNKNOWN_ERROR
            reject(ret_val);
            return;
        }
    })
}

function pubsub_share_vcard(payload) {
    return new Promise(async (resolve, reject) => {
        let ret_val = { status: -1 }
        try {
            logger.log(payload)
            let pk = payload.key;
            let pd = payload.data;
            let event_base_path = pk.instanceId + "_" + pk.clientId + pk.eventId;
            let share_media = (pk.shareMedia || ["email"])
            let tasks = [];
            share_media.forEach(media => {
                if (media === 'email') {
                    tasks.push(_share_vcard_email(payload))
                }
            })
            ret_val.result = await Promise.all(tasks)
            ret_val.status = 0;
        } catch (err) {
            console.log(err);
            ret_val = ERRCODE.UNKNOWN_ERROR
            reject(err)
        }
    })
}

function _share_vcard_email(payload) {
    return new Promise(async (resolve, reject) => {
        let ret_val = { status: -1 }
        try {
            logger.log(payload)
            let tasks = []
            let pk = payload.key;
            let pd = payload.data;
            let pa = payload.auth.data;
            // let pa = {
            //     UserId: "221000",
            //     AttendeeId: "221000",
            //     FirstName: "Debashis",
            //     LastName: "Giri",
            //     Email: "debashis.giri@webspiders.com",
            //     Name: "Debashis Giri"
            // }

            let event_base_path = pk.instanceId + "_" + pk.clientId + pk.eventId;
            let attendeeDocPath = event_base_path + '/AttendeeList/Attendees/' + pd.attendeeId;
            let client_doc_path = pk.instanceId + "/ClientList/Clients/" + pk.clientId;
            tasks.push(dbClient.doc(attendeeDocPath).get())
            tasks.push(utils.getEmailTemplate(payload, '/mailtpl/Vcard/share'));
            tasks.push(dbClient.doc(client_doc_path).get())


            let sharedByAttendeeDocPath = event_base_path + '/AttendeeList/Attendees/' + pa.UserId
            let sharedByAttendeeRef = dbClient.doc(sharedByAttendeeDocPath)
            tasks.push(sharedByAttendeeRef.update({ 'VCard.Shared': Firestore.FieldValue.arrayUnion(pd.attendeeId) }, { merge: true }))


            let res = await Promise.all(tasks)
            //console.log(res)
            let attendeeRef = res[0]
            let templateRef = res[1]
            let clientRef = res[2]
            if (attendeeRef.exists && templateRef.email_template && clientRef.exists) {
                let attendee = attendeeRef.data();
                if (!attendee.Name) {
                    if (attendee.FirstName && attendee.LastName) {
                        attendee.Name = attendee.FirstName + " " + attendee.LastName
                    } else if (!attendee.LastName) {
                        attendee.Name = attendee.FirstName
                    } else if (!attendee.FirstName) {
                        attendee.Name = attendee.LastName
                    }
                }
                if (attendee.Name && attendee.Salutation) {
                    attendee.Name = attendee.Salutation + " " + attendee.Name
                }
                let vcard = "BEGIN:VCARD\r\nVERSION:3.0\r\n";
                vcard += "ID:" + attendee.AttendeeId + "\r\n";
                if (attendee.Name) {
                    vcard += "FN:" + attendee.Name + "\r\n";
                    vcard += "N:" + attendee.LastName + ";" + attendee.FirstName + "\r\n";
                }
                if (attendee.Company) {
                    vcard += "ORG:" + attendee.Company + "\r\n";
                }
                if (attendee.Email) {
                    vcard += "EMAIL;TYPE=INTERNET:" + attendee.Email + "\r\n";
                }
                if (attendee.Designation) {
                    vcard += "TITLE:" + attendee.Designation + "\r\n";
                }
                if (attendee.Phone || attendee.Mobile) {
                    vcard += "TEL;TYPE= WORK,VOICE:" + (attendee.Phone || attendee.Mobile) + "\r\n";
                }
                vcard += "REV:" + moment().format() + "\r\n";
                vcard += "END:VCARD\r\n";
                //console.log(vcard)
                let email_template = templateRef.email_template
                const emailTemplate = Handlebars.compile(email_template.html);

                let email_body_obj = {
                    fromTitle: (pa.Title || ""),
                    fromFirstName: pa.FirstName,
                    fromLastName: pa.LastName,
                    fromName: pa.Name,
                    fromEmail: pa.Email,
                    fromCompany: (pa.Company || ""),
                    fromPhone: (pa.Phone || ""),
                    toFirstName: attendee.FirstName,
                    toLastName: attendee.LastName,
                    toName: attendee.Name,
                    toEmail: pd.Email

                }

                let email_body_html = emailTemplate(email_body_obj);

                let attachments = [{
                    content: Buffer.from(vcard).toString('base64'),
                    filename: "vc" + pd.attendeeId + ".vcf",
                    type: "text/x-vcard",
                    disposition: "attachment"
                }];

                let email_payload = {
                    // to: pd.email,
                    to: {
                        name: attendee.Name,
                        email: attendee.Email
                    },
                    subject: email_template.subject,
                    html: email_body_html,
                    attachments: attachments
                };
                email_payload.from = {
                    email: email_template.from,
                    name: email_template.fromName
                };
                if (email_template.cc) {
                    email_payload.cc = email_template.cc
                }
                if (email_template.bcc) {
                    email_payload.bcc = email_template.bcc
                }
                //console.log(email_payload)
                let Client = clientRef.data()
                let res = await utils.sendEmail(Client, email_payload)

                ret_val.result = res
                ret_val.status = 0;
            } else {
                ret_val = ERRCODE.DATA_NOT_FOUND
                reject(ret_val)
            }
        } catch (err) {
            console.log(err);
            ret_val = ERRCODE.UNKNOWN_ERROR
            reject(err)
        }
    })
}


async function update_vcard_in_batches(payload) {
    const { key, data } = payload;
    if (!Array.isArray(data) || data.length === 0) {
        throw new Error("Invalid data array");
    }

    const event_base_path = `${key.instanceId}_${key.clientId}${key.eventId}`;
    const batchSize = 100; // Batch size

    for (let i = 0; i < data.length; i += batchSize) {
        const batchItems = data.slice(i, i + batchSize);

        const tasks = batchItems.map(async (item) => {
            const attendeeRef = dbClient.doc(`${event_base_path}/AttendeeList/Attendees/${item.AttendeeId}`);
            const updatedVCard = {
                EXT_QR: item.VCard.EXT_QR,
                EXT_QRCODE: item.VCard.EXT_QRCODE,
                QR: item.VCard.QR
            };

            await attendeeRef.set({ VCard: updatedVCard }, { merge: true });
        });

        await Promise.all(tasks);
    }

    return { status: 0, message: "VCard update completed." };
}


//--

module.exports = {
    scanVCard: scan_vcard,
    pubsubScanVCard: pubsub_scan_vcard,
    scanRating: scan_rating,
    shareVCard: share_vcard,
    pubsubShareVCard: pubsub_share_vcard,
    updateVCardInBatches: update_vcard_in_batches
}