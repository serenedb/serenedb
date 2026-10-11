package com.serenedb.drivers.flyway;

import org.flywaydb.core.api.migration.BaseJavaMigration;
import org.flywaydb.core.api.migration.Context;

import java.sql.PreparedStatement;

public class V3__Promote extends BaseJavaMigration {

    @Override
    public void migrate(Context context) throws Exception {
        try (PreparedStatement st = context.getConnection().prepareStatement(
                "UPDATE hero SET level = level + 1 WHERE guild_id = ?")) {
            st.setInt(1, 2);
            st.executeUpdate();
        }
    }
}
