import pg, { Pool } from "pg";
import Cursor from "pg-cursor";

const query = "SELECT 1; SELECT 2;";

const host = "localhost";
//const host = "localhost";

const pool = new Pool({
    user: "postgres",
    host,
    database: "postgres",
    password: "postgres",
    port: host === "192.168.1.216" ? 6161 : 5432,
});

const cursor = await pool.query(`
SELECT 1;
SELECT 2;
`);

console.log(cursor);
