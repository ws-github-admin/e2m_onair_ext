const ERRCODE = {
    UNKNOWN_ERROR: { status: -1, msg: "Application error" },
    PAYLOAD_ERROR: { status: -2, msg: "Incorrect parameters " },
    ACCESS_DENIED: { status: -3, msg: "Access denied" },
    UNAUTHORIZED_ACCESS: { status: -4, msg: "Unauthorized access" },
    METHOD_NOT_ALLOWED: { status: -5, msg: "Metod not allowed" },
    PASSWORD_MISMATCH: { status: -6, msg: "Password mismatch" },
    WEAK_PASSWORD: { status: -7, msg: "Weak password" },
    EXT_API_ERROR: { status: -8, msg: "External api error" },
    EMAIL_NOT_VERIFIED: { status: -9, msg: "Email not verfied" },
    TOKEN_EXPIRED: { status: -10, msg: "Token expired" },
    INVALID_TOKEN: { status: -11, msg: "Invalid token" },
    TOKEN_REVOKED: { status: -12, msg: "Token revoked" },
    DUPLICATE_OPERATION: { status: -13, msg: "Duplicate operation" },
    CONFIRM_PASS_MISMATCH: { status: -14, msg: "Confirm password does not match" },
    DATA_NOT_FOUND: { status: -15, msg: "Data not found" },
    FIREBASE_AUTH_ERROR: { status: -16, msg: "Firebase auth error" },
    OCCUPIED: { status: -17, msg: "Fully Occupied" },
    TRIGGER_BLANK_FIRE: { status: -18, msg: "Nothing to update" },
    EVENT_EXPIRED: { status: -19, msg: "Event expired" },
    SERVICE_NOT_AVAILABLE: { status: -20, msg: "Service not available" },
    MALFORMATTED_DOC: { status: -21, msg: "Malformatted document" },
    WORKFLOW_ERROR: { status: -22, msg: "Cloud Workflow error" },
    ZOOM_AUTH_CODE_ERROR: { status: -30, msg: "Zoom authorization code error" },
    ZOOM_AUTH_CODE_GRANT_ERROR: { status: -31, msg: "Zoom authorization code request error" },
    ZOOM_TOKEN_REVOKED: { status: -32, msg: "Zoom token revoked" },
    ZOOM_HOST_ERROR: { status: -33, msg: "Zoom host not found" },
    MAX_COUNT_ERROR: { status: -34, msg: "Max count reached" },
    MAX_COUNT_ERROR: { status: -35, msg: "Meeting confirmation disabled" },

    PREFLIGHT: { status: 204 }


};
module.exports = {
    ERRCODE: ERRCODE
};