import pg from "pg";
import Cursor from "pg-cursor";

const query = "SELECT * FROM t1;";

const host = "192.168.1.216";
//const host = "localhost";

const client = new pg.Client({
    user: "postgres",
    host,
    database: "postgres",
    password: "postgres",
    port: host === "192.168.1.216" ? 6161 : 5432,
});

client.connect();

const cursor = client.query(new Cursor(query, []));

const rows = await cursor.read(100);

await cursor.close();

console.log(rows);
