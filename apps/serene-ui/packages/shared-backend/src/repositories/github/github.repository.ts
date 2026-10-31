import { DBClient } from "../../database";
import {
    buildDelete,
    buildInsert,
    buildSelect,
    buildUpdate,
} from "../../utils/request-builder";

export const GithubRepository = {
    getAccessToken: (): string | null => {
        const { sql, values } = buildSelect("github", {
            columns: ["access_token"],
            orderBy: "id DESC",
            limit: 1,
        });
        const row = DBClient.prepare(sql).get(...values);

        if (!row) {
            return null;
        }

        const { access_token } = row as { access_token?: string };
        return access_token ?? null;
    },

    setAccessToken: (accessToken: string): void => {
        const { sql: selectSql, values: selectValues } = buildSelect("github", {
            columns: ["id"],
            orderBy: "id DESC",
            limit: 1,
        });
        const existing = DBClient.prepare(selectSql).get(...selectValues) as
            | { id?: number }
            | undefined;

        if (existing?.id != null) {
            const { sql, values } = buildUpdate(
                "github",
                {
                    access_token: accessToken,
                },
                {
                    where: { id: existing.id },
                },
            );
            DBClient.prepare(sql).run(...values);
            return;
        }

        const { sql, values } = buildInsert("github", {
            access_token: accessToken,
        });
        DBClient.prepare(sql).run(...values);
    },

    deleteAccessToken: (): boolean => {
        const { sql: selectSql, values: selectValues } = buildSelect("github", {
            columns: ["id"],
            orderBy: "id DESC",
            limit: 1,
        });
        const existing = DBClient.prepare(selectSql).get(...selectValues) as
            | { id?: number }
            | undefined;

        if (!existing?.id) {
            return false;
        }

        const { sql, values } = buildDelete("github", {
            where: { id: existing.id },
        });
        DBClient.prepare(sql).run(...values);
        return true;
    },
};
