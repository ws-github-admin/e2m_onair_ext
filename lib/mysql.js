const mysql = require('mysql2/promise');
const config = require('../config.json');

// Function to create a MySQL connection
async function mysql_create_connection() {
    let ret_val = { status: -1 };
    let connection;

    try {
        connection = await mysql.createConnection({
            host: config.MYSQL.HOST,
            user: config.MYSQL.USER,
            database: config.MYSQL.DATABASE,
            port: config.MYSQL.PORT,
            password: config.MYSQL.PASS,
        });

        console.log('Connected to MySQL successfully!');
        ret_val.status = 0;
        return ret_val;

    } catch (err) {
        console.error('Failed to connect to MySQL:', err);
        return ret_val;

    } finally {
        if (connection) {
            try {
                await connection.end(); // Clean up connection
            } catch (closeErr) {
                console.warn('Error closing MySQL connection:', closeErr);
            }
        }
    }
}

// Helper to interpolate parameters into the query for logging
function interpolateQuery(query, params) {
    let i = 0;
    return query.replace(/\?/g, () => {
        const param = params[i++];
        if (param === null || param === undefined) return 'NULL';
        if (typeof param === 'number') return param;
        return `'${String(param).replace(/'/g, "''")}'`; // escape single quotes
    });
}

async function execute_query(query, params = [],logQuery = false) {
    let connection;

    try {
        if(logQuery) {
            const interpolatedQuery = interpolateQuery(query, params);
            console.log("Executing SQL:", interpolatedQuery);
        }        

        connection = await mysql.createConnection({
            host: config.MYSQL.HOST,
            user: config.MYSQL.USER,
            database: config.MYSQL.DATABASE,
            port: config.MYSQL.PORT,
            password: config.MYSQL.PASS,
        });

        const [results] = await connection.execute(query, params);
        return results;

    } catch (error) {
        console.error("Database Query Error:", error);
        throw error;

    } finally {
        if (connection) {
            try {
                await connection.end();
            } catch (closeErr) {
                console.warn("Error closing MySQL connection:", closeErr);
            }
        }
    }
}
module.exports = {
    mysqlConnection: mysql_create_connection,
    executeQuery: execute_query,
};
