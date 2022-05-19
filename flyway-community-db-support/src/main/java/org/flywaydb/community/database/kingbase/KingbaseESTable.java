//
// Source code recreated from a .class file by IntelliJ IDEA
// (powered by FernFlower decompiler)
//

package org.flywaydb.community.database.kingbase;

import org.flywaydb.core.internal.database.base.Table;
import org.flywaydb.core.internal.jdbc.JdbcTemplate;

import java.sql.SQLException;

public class KingbaseESTable extends Table<KingbaseESDatabase, KingbaseESSchema> {
    KingbaseESTable(JdbcTemplate jdbcTemplate, KingbaseESDatabase database, KingbaseESSchema schema, String name) {
        super(jdbcTemplate, database, schema, name);
    }

    @Override
    protected void doDrop() throws SQLException {
        this.jdbcTemplate.execute("DROP TABLE " + ((KingbaseESDatabase)this.database).quote(new String[]{((KingbaseESSchema)this.schema).getName(), this.name}) + " CASCADE", new Object[0]);
    }

    @Override
    protected boolean doExists() throws SQLException {
        return this.jdbcTemplate.queryForBoolean("SELECT EXISTS (\n  SELECT 1\n  FROM   sys_catalog.sys_class c\n  JOIN   sys_catalog.sys_namespace n ON n.oid = c.relnamespace\n  WHERE  n.nspname = ?\n  AND    c.relname = ?\n  AND    c.relkind = 'r'\n)", new String[]{((KingbaseESSchema)this.schema).getName(), this.name});
    }

    @Override
    protected void doLock() throws SQLException {
        this.jdbcTemplate.execute("SELECT * FROM " + this + " FOR UPDATE", new Object[0]);
    }
}
