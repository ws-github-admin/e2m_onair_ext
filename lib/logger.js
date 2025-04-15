'use strict';

module.exports = {
    log: function(log_message) {
        try {
            if (log_message) {
                if (typeof log_message == 'object') {
                    console.log(JSON.stringify(log_message));
                } else {
                    console.log(log_message);
                }
            }
        } catch (err) {}
    },
    logError: function(source, err) {
        try {
            var err_msg;
            if (err.stack) {
                err_msg = err.stack;
            } else if (err.message) {
                err_msg = err.message;
            } else {
                err_msg = err;
            }
            err_msg = ("ERROR!! in " + source + ": => [" + err_msg + "].");
            console.error(err_msg);
        } catch (err) {}
    },
    logCrash: function(log_message) {
        try {
            console.error("CRASH!!!: " + log_message);
        } catch (err) {}
    }
}