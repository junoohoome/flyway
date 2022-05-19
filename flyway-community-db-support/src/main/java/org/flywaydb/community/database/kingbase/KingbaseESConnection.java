//
// Source code recreated from a .class file by IntelliJ IDEA
// (powered by FernFlower decompiler)
//

package org.flywaydb.community.database.kingbase;

import org.flywaydb.core.api.FlywayException;
import org.flywaydb.core.internal.database.base.Connection;
import org.flywaydb.core.internal.database.base.Schema;
import org.flywaydb.core.internal.database.base.Table;
import org.flywaydb.core.internal.exception.FlywaySqlException;
import org.flywaydb.core.internal.util.StringUtils;

import java.sql.SQLException;
import java.util.concurrent.Callable;

public class KingbaseESConnection extends Connection<KingbaseESDatabase> {
    private final String originalRole;

    KingbaseESConnection(KingbaseESDatabase database, java.sql.Connection connection) {
        super(database, connection);

        try {
            this.originalRole = this.jdbcTemplate.queryForString("SELECT CURRENT_USER", new String[0]);
        } catch (SQLException var4) {
            throw new FlywaySqlException("Unable to determine current user", var4);
        }
    }

    @Override
    protected void doRestoreOriginalState() throws SQLException {
        this.jdbcTemplate.execute("SET ROLE '" + this.originalRole + "'", new Object[0]);
    }

    @Override
    public Schema doGetCurrentSchema() throws SQLException {
        String currentSchema = this.jdbcTemplate.queryForString("SELECT current_schema", new String[0]);
        if (!StringUtils.hasText(currentSchema)) {
            throw new FlywayException("Unable to determine current schema as search_path is empty. Set the current schema in currentSchema parameter of the JDBC URL or in Flyway's schemas property.");
        } else {
            return this.getSchema(currentSchema);
        }
    }

    @Override
    protected String getCurrentSchemaNameOrSearchPath() throws SQLException {
        return this.jdbcTemplate.queryForString("SHOW search_path", new String[0]);
    }

    @Override
    public void changeCurrentSchemaTo(Schema schema) {
        try {
            if (!schema.getName().equals(this.originalSchemaNameOrSearchPath) && !this.originalSchemaNameOrSearchPath.startsWith(schema.getName() + ",") && schema.exists()) {
                if (StringUtils.hasText(this.originalSchemaNameOrSearchPath)) {
                    this.doChangeCurrentSchemaOrSearchPathTo(schema.toString() + "," + this.originalSchemaNameOrSearchPath);
                } else {
                    this.doChangeCurrentSchemaOrSearchPathTo(schema.toString());
                }

            }
        } catch (SQLException var3) {
            throw new FlywaySqlException("Error setting current schema to " + schema, var3);
        }
    }

    @Override
    public void doChangeCurrentSchemaOrSearchPathTo(String schema) throws SQLException {
        this.jdbcTemplate.execute("SELECT set_config('search_path', ?, false)", new Object[]{schema});
    }

    @Override
    public Schema getSchema(String name) {
        return new KingbaseESSchema(this.jdbcTemplate, (KingbaseESDatabase)this.database, name);
    }

    @Override
    public <T> T lock(Table table, Callable<T> callable) {
        return (new KingbaseESAdvisoryLockTemplate(this.jdbcTemplate, table.toString().hashCode())).execute(callable);
    }
}
