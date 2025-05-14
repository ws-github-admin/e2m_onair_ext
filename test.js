// async function _handle_request() {
//     const meeting = require("./lib/meeting");

const user = require("./lib/user");

//     let payload = {
//         "Invitee": {
//           "Address": "kolkata",
//           "AttendeeId": "1324000",
//           "Company": "web spiders",
//           "CreatedDate": {},
//           "Designation": "software engineer",
//           "DynamicFields": [],
//           "Email": "debayan.ghosh@webspiders.com",
//           "FirstName": "Debayan",
//           "FormType": "FREE",
//           "IsPublished": true,
//           "LastModifiedDate": {},
//           "LastName": "Ghosh",
//           "Meetings": [],
//           "Name": "Debayan Ghosh",
//           "Phone": "90623 01394",
//           "PhoneCountryCode": "IN",
//           "RegistrationType": {},
//           "ShowInCMSAttendeeList": 1,
//           "Slots": [],
//           "UserId": "1324000",
//           "UserRoles": [],
//           "VCard": {},
//           "Zip": "700054",
//           "isComplete": true,
//           "sendMail": 1
//         },
//         "Meeting": {
//           "CreateDateTime": {
//             "_nanoseconds": 346000000,
//             "_seconds": 1744313698
//           },
//           "Invitee": {
//             "AttendeeId": "1324000",
//             "Company": "web spiders",
//             "Designation": "software engineer",
//             "Name": "Debayan Ghosh"
//           },
//           "LastModifiedDate": {
//             "_nanoseconds": 999000000,
//             "_seconds": 1744624563
//           },
//           "LastUpdatedDateTime": {
//             "_nanoseconds": 65000000,
//             "_seconds": 1744626979
//           },
//           "MeetingId": "MbmhJkLfDq4Pmyn4sbdK",
//           "Requestor": {
//             "AttendeeId": "99934194",
//             "Company": "StackAdapt",
//             "Designation": "Business Development Lead, Strategic Partnerships",
//             "Name": "Wev Castro",
//             "Phone": "+91983022166"
//           },
//           "Slots": [
//             "2025-05-13T03:30:00Z"
//           ],
//           "SponsorId": "34057000",
//           "Status": "Requested"
//         },
//         "Payload": {
//           "data": {
//             "MeetingId": "MbmhJkLfDq4Pmyn4sbdK",
//             "Slot": "2025-05-13T03:30:00Z"
//           },
//           "key": {
//             "clientId": "C1742212403583",
//             "eventId": "E1742214690559",
//             "instanceId": "OA_UAT"
//           }
//         },
//         "Requestor": {
//           "Address": "Flat 307 Botanical Court",
//           "AttendeeId": "99934194",
//           "Company": "StackAdapt",
//           "CreatedDate": {},
//           "Designation": "Business Development Lead, Strategic Partnerships",
//           "DynamicFields": [],
//           "Email": "debseyana@gmail.com",
//           "FirstName": "Wev",
//           "FormType": "FREE",
//           "IsPublished": true,
//           "LastModifiedDate": {},
//           "LastName": "Castro",
//           "Meetings": [],
//           "Name": "Wev Castro",
//           "Phone": "+91983022166",
//           "RegistrationType": {},
//           "ShowInCMSAttendeeList": 1,
//           "Slots": [
//             "2025-05-13T03:30:00Z"
//           ],
//           "UserId": "99934194",
//           "UserRoles": [],
//           "VCard": {},
//           "Zip": "E1 3FU",
//           "isComplete": true,
//           "sendMail": 0
//         }
//       }
//       ;

//     try {
//         let res = await meeting.pubsubConfirmMeeting(payload);
//         console.log("Success:", res);
//         //process.exit(0);
//     } catch (err) {
//         console.log("Error:", err);
//         process.exit(0);
//     }
// }


// async function _handle_request() {
//   const meeting = require("./lib/meeting");

//   let payload = {
//     "key": {
//           "instanceId": "OA_UAT",
//           "clientId": "C1742212403583",
//           "eventId": "E1742214690559"
//       },
//     "data": {
//       "RequestorId": "99915239",
//       "InviteeIds": [ "34053000", "34057000"
//          ]
//   ,
//       "Slots": [],
//       "Message": "Let'\''s connect and discuss potential collaboration opportunities.",
//       "Timezone": "Asia/Kolkata"
//     }
//   };

//   try {
//     let res = await meeting.requestMeetings(payload);
//     console.log("Success:", res);
//     //process.exit(0);
//   } catch (err) {
//     console.log("Error:", err);
//     process.exit(0);
//   }
// }

async function _handle_request() {
  const meeting = require("./lib/meeting");

  let payload ={
    "key": {
          "instanceId": "OA_UAT",
          "clientId": "C1742212403583",
          "eventId": "E1743163201304"
      } ,
      "data" : {
        "UserId" : "99934511"
      }
  };

  try {
    let res = await meeting.getMeetings(payload);
    console.log("Success:", res);
    //process.exit(0);
  } catch (err) {
    console.log("Error:", err);
    process.exit(0);
  }
}

_handle_request();
