import fs from "node:fs";
import pg from "pg";
import { drizzle } from "drizzle-orm/node-postgres";
import { pushSchema } from "drizzle-kit/api";
import { schema } from "./drizzle-schema.js";

const [url, options, mode, output] = process.argv.slice(2);
const imports = schema(JSON.parse(options));
const pool = new pg.Pool({ connectionString: url });
try {
  const pushed = await pushSchema(imports, drizzle({ client: pool, schema: imports }));
  if (mode === "apply") await pushed.apply();
  fs.writeFileSync(
    output,
    JSON.stringify({
      hasDataLoss: pushed.hasDataLoss,
      statements: pushed.statementsToExecute,
    }),
  );
} finally {
  await pool.end();
}
