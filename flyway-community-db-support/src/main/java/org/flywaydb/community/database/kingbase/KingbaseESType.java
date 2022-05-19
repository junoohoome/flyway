package org.flywaydb.community.database.kingbase;

import org.flywaydb.core.internal.database.base.Type;
import org.flywaydb.core.internal.jdbc.JdbcTemplate;

import java.sql.SQLException;

public class KingbaseESType extends Type<KingbaseESDatabase, KingbaseESSchema> {
    public KingbaseESType(JdbcTemplate jdbcTemplate, KingbaseESDatabase database, KingbaseESSchema schema, String name) {
        super(jdbcTemplate, database, schema, name);
    }

    @Override
    protected void doDrop() throws SQLException {
        jdbcTemplate.execute("DROP TYPE " + database.quote(schema.getName(), name));
    }
}

