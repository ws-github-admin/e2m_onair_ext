const mysql = require('mysql2/promise');
const config = require('../config.json');

// Create a MySQL connection pool (Best for Serverless Functions)
const pool = mysql.createPool({
    host: config.MYSQL.HOST,
    user: config.MYSQL.USER,
    database: config.MYSQL.DATABASE,
    port: config.MYSQL.PORT,
    password: config.MYSQL.PASS,
    waitForConnections: true,
    connectionLimit: 10,  // Adjust as per workload
    queueLimit: 0
});

// Function to check MySQL connection
async function mysql_create_connection() {
    let ret_val = { status: -1 };
    let connection;
    try {
        connection = await pool.getConnection();
        console.log('Connected to MySQL successfully!');
        ret_val.status = 0;
        connection.release();        
        return ret_val; // Success
    } catch (err) {
        console.error('Failed to connect to MySQL:', err);
        return ret_val; // Failure
    } finally {
        if (connection) {
            connection.release(); // Always release the connection
        }
    }
}

// Function to Execute Queries
async function execute_query(query, params = []) {
    let connection;
    try {
        connection = await pool.getConnection();
        const [results] = await connection.execute(query, params);
        connection.release();
        return results;
    } catch (error) {
        console.error("Database Query Error:", error);
        throw error;
    } finally {
        if (connection) {
            connection.release(); // Always release the connection
        }
    }
}

module.exports = {
    mysqlConnection: mysql_create_connection,
    executeQuery: execute_query
};
