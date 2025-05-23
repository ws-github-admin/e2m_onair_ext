const { createClient } = require('@supabase/supabase-js')
const config = require('../config.json');

const supabase = createClient(config.SUPABASE.DATABASE, config.SUPABASE.KEY)

//function to check supabase connection

async function checkSupabaseConnection() {
  try {
    const { data, error } = await supabase.auth.getSession()

    if (error) {
      console.error('❌ Supabase auth API error:', error.message)
    } else {
      console.log('✅ Supabase is reachable and credentials are valid.')
    }
  } catch (err) {
    console.error('❌ Network or configuration error:', err.message)
  }
}

// Example function to insert dummy data into the meeting table
async function insertDummyMeeting() {
  const { data, error } = await supabase
    .from('meeting')
    .insert([
      {
        meetingCode: 1001,
        iceId: 'ICE12345',
        requestorId: 'REQ123',
        requestorType: 'user',
        requestorTypeEntityId: 'ENT001',
        inviteeId: 'INV456',
        inviteeType: 'partner',
        inviteeTypeEntityId: 'ENT002',
        requestStatus: 'pending',
        requestUpdateDateTime: new Date().toISOString(),
        isCreatedByAI: true,
        sendEmail: 1,
        remarks: 'Initial dummy entry',
        requestMeetingSlot: '2025-06-01T14:00:00Z'
      }
    ]);

  if (error) {
    console.error('Error inserting dummy data:', error.message);
  } else {
    console.log('Inserted dummy meeting:', data);
  }
}

// Example function to insert dummy data into the QnA table
async function insertDummyQna() {
  const { data, error } = await supabase
    .from('qna')
    .insert([
      {
        iceId: 'ICE12345',
        entityId: 'ENT001',
        entityType: 'user',
        questionId: 'Q001',
        questionLabel: 'What is your primary goal?',
        selectedValue: 'Improve engagement',
        updateBy: 'admin_user',
        insertDateTime: new Date().toISOString(),
        updateDateTime: new Date().toISOString()
      }
    ]);

  if (error) {
    console.error('Error inserting dummy QnA:', error.message);
  } else {
    console.log('Inserted dummy QnA:', data);
  }
}

// Example function to insert dummy data into the slots table
async function insertDummySlots() {
  const { data, error } = await supabase
    .from('slots')
    .insert([
      {
        attendeeId: 'ATTENDEE001',
        slots: JSON.stringify([
          { start: '2025-06-01T09:00:00Z', end: '2025-06-01T10:00:00Z' },
          { start: '2025-06-01T11:00:00Z', end: '2025-06-01T12:00:00Z' }
        ])
      },
      {
        attendeeId: 'ATTENDEE002',
        slots: JSON.stringify([
          { start: '2025-06-02T14:00:00Z', end: '2025-06-02T15:30:00Z' }
        ])
      }
    ]);

  if (error) {
    console.error('Error inserting dummy slots:', error.message);
  } else {
    console.log('Inserted dummy slots:', data);
  }
}


// Example function to update meeting status
async function updateMeetingStatus() {
  const { data, error } = await supabase
    .from('meeting')
    .update({
      requestStatus: 'skipped',
      requestUpdateDateTime: new Date().toISOString(),
      remarks: 'Invitee not available'
    })
    .match({
      iceId: 'ICE12345',
      requestorId: 'REQ123',
      inviteeId: 'INV456',
      requestStatus: 'pending'
    });

  if (error) {
    console.error('Update failed:', error.message);
  } else {
    console.log('Meeting updated:', data);
  }
}


// Example function to update sendEmail status
async function updateSendEmailStatus(meetingIds) {
  // meetingIds should be an array of meetingCode values, e.g. [1001, 1002]

  const { data, error } = await supabase
    .from('meeting')
    .update({ sendEmail: 3 })
    .in('meetingCode', meetingIds);

  if (error) {
    console.error('Error updating sendEmail:', error.message);
  } else {
    console.log('sendEmail updated for meetings:', data);
  }
}


module.exports = {
    checkSupabaseConnection : checkSupabaseConnection ,
    insertDummyMeeting : insertDummyMeeting ,
    insertDummyQna : insertDummyQna ,
    insertDummySlots : insertDummySlots ,
    updateMeetingStatus : updateMeetingStatus ,
    updateSendEmailStatus : updateSendEmailStatus ,
}



