/**
 * Generates an INSERT SQL statement from an object.
 *
 * @param tableName - The name of the table to insert into
 * @param data - The data object with keys representing column names
 * @param options - Optional configuration
 * @returns An object containing the SQL statement and values array
 *
 * @example
 * const { sql, values } = buildInsert('users', { name: 'John', age: 30 });
 * // sql: "INSERT INTO users (name, age) VALUES (?, ?)"
 * // values: ['John', 30]
 */
export function buildInsert<T extends Record<string, any>>(
    tableName: string,
    data: T,
    options?: {
        exclude?: (keyof T)[];
        ignoreId?: boolean;
    },
): { sql: string; values: any[] } {
    const { exclude = [], ignoreId = true } = options || {};

    const entries = Object.entries(data).filter(([key, value]) => {
        if (exclude.includes(key as keyof T)) return false;
        if (ignoreId && key === "id") return false;
        return value !== undefined;
    });

    const columns = entries.map(([key]) => key);
    const values = entries.map(([, value]) => value);
    const placeholders = columns.map(() => "?").join(", ");

    const sql = `INSERT INTO ${tableName} (${columns.join(", ")}) VALUES (${placeholders})`;

    return { sql, values };
}

/**
 * Generates an UPDATE SQL statement from an object.
 *
 * @param tableName - The name of the table to update
 * @param data - The data object with keys representing column names
 * @param options - Optional configuration
 * @returns An object containing the SQL statement and values array
 *
 * @example
 * const { sql, values } = buildUpdate('users', { name: 'John', age: 30 }, { where: { id: 5 } });
 * // sql: "UPDATE users SET name = ?, age = ? WHERE id = ?"
 * // values: ['John', 30, 5]
 */
export function buildUpdate<T extends Record<string, any>>(
    tableName: string,
    data: T,
    options?: {
        exclude?: (keyof T)[];
        where?: Record<string, any>;
    },
): { sql: string; values: any[] } {
    const { exclude = [], where } = options || {};

    const entries = Object.entries(data).filter(([key, value]) => {
        if (exclude.includes(key as keyof T)) return false;
        return value !== undefined;
    });

    if (entries.length === 0) {
        throw new Error("No fields to update");
    }

    const setClause = entries.map(([key]) => `${key} = ?`).join(", ");
    const values = entries.map(([, value]) => value);

    let sql = `UPDATE ${tableName} SET ${setClause}`;

    if (where) {
        const whereEntries = Object.entries(where).filter(
            ([, value]) => value !== undefined,
        );
        if (whereEntries.length > 0) {
            const whereClause = whereEntries
                .map(([key]) => `${key} = ?`)
                .join(" AND ");
            sql += ` WHERE ${whereClause}`;
            values.push(...whereEntries.map(([, value]) => value));
        }
    }

    return { sql, values };
}

/**
 * Generates a SELECT SQL statement with optional WHERE clause.
 *
 * @param tableName - The name of the table to select from
 * @param options - Optional configuration
 * @returns An object containing the SQL statement and values array
 *
 * @example
 * const { sql, values } = buildSelect('users', { where: { active: true, role: 'admin' } });
 * // sql: "SELECT * FROM users WHERE active = ? AND role = ?"
 * // values: [true, 'admin']
 */
export function buildSelect(
    tableName: string,
    options?: {
        columns?: string[];
        where?: Record<string, any>;
        orderBy?: string;
        limit?: number;
    },
): { sql: string; values: any[] } {
    const { columns = ["*"], where, orderBy, limit } = options || {};

    let sql = `SELECT ${columns.join(", ")} FROM ${tableName}`;
    const values: any[] = [];

    if (where) {
        const entries = Object.entries(where).filter(
            ([, value]) => value !== undefined,
        );
        if (entries.length > 0) {
            const whereClause = entries
                .map(([key]) => `${key} = ?`)
                .join(" AND ");
            sql += ` WHERE ${whereClause}`;
            values.push(...entries.map(([, value]) => value));
        }
    }

    if (orderBy) {
        sql += ` ORDER BY ${orderBy}`;
    }

    if (limit !== undefined) {
        sql += ` LIMIT ${limit}`;
    }

    return { sql, values };
}

/**
 * Generates a DELETE SQL statement with optional WHERE clause.
 *
 * @param tableName - The name of the table to delete from
 * @param options - Optional configuration
 * @returns An object containing the SQL statement and values array
 *
 * @example
 * const { sql, values } = buildDelete('users', { where: { id: 5 } });
 * // sql: "DELETE FROM users WHERE id = ?"
 * // values: [5]
 */
export function buildDelete(
    tableName: string,
    options?: {
        where?: Record<string, any>;
    },
): { sql: string; values: any[] } {
    const { where } = options || {};

    let sql = `DELETE FROM ${tableName}`;
    const values: any[] = [];

    if (where) {
        const entries = Object.entries(where).filter(
            ([, value]) => value !== undefined,
        );
        if (entries.length > 0) {
            const whereClause = entries
                .map(([key]) => `${key} = ?`)
                .join(" AND ");
            sql += ` WHERE ${whereClause}`;
            values.push(...entries.map(([, value]) => value));
        }
    }

    return { sql, values };
}
