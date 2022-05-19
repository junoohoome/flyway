//
// Source code recreated from a .class file by IntelliJ IDEA
// (powered by FernFlower decompiler)
//

package org.flywaydb.community.database.kingbase;

import org.flywaydb.core.internal.database.base.Schema;
import org.flywaydb.core.internal.database.base.Table;
import org.flywaydb.core.internal.database.base.Type;
import org.flywaydb.core.internal.jdbc.JdbcTemplate;

import java.sql.SQLException;
import java.util.*;

public class KingbaseESSchema extends Schema<KingbaseESDatabase, KingbaseESTable> {
    KingbaseESSchema(JdbcTemplate jdbcTemplate, KingbaseESDatabase database, String name) {
        super(jdbcTemplate, database, name);
    }

    @Override
    protected boolean doExists() throws SQLException {
        return this.jdbcTemplate.queryForInt("SELECT COUNT(*) FROM sys_namespace WHERE nspname=?", new String[]{this.name}) > 0;
    }

    @Override
    protected boolean doEmpty() throws SQLException {
        return !this.jdbcTemplate.queryForBoolean("SELECT EXISTS (\n    SELECT c.oid FROM sys_catalog.sys_class c\n    JOIN sys_catalog.sys_namespace n ON n.oid = c.relnamespace\n    LEFT JOIN sys_catalog.sys_depend d ON d.objid = c.oid AND d.deptype = 'e'\n    WHERE  n.nspname = ? AND d.objid IS NULL AND c.relkind IN ('r', 'v', 'S', 't')\n  UNION ALL\n    SELECT t.oid FROM sys_catalog.sys_type t\n    JOIN sys_catalog.sys_namespace n ON n.oid = t.typnamespace\n    LEFT JOIN sys_catalog.sys_depend d ON d.objid = t.oid AND d.deptype = 'e'\n    WHERE n.nspname = ? AND d.objid IS NULL AND t.typcategory NOT IN ('A', 'C')\n  UNION ALL\n    SELECT p.oid FROM sys_catalog.sys_proc p\n    JOIN sys_catalog.sys_namespace n ON n.oid = p.pronamespace\n    LEFT JOIN sys_catalog.sys_depend d ON d.objid = p.oid AND d.deptype = 'e'\n    WHERE n.nspname = ? AND d.objid IS NULL\n)", new String[]{this.name, this.name, this.name});
    }

    @Override
    protected void doCreate() throws SQLException {
        this.jdbcTemplate.execute("CREATE SCHEMA " + ((KingbaseESDatabase)this.database).quote(new String[]{this.name}), new Object[0]);
    }

    @Override
    protected void doDrop() throws SQLException {
        this.jdbcTemplate.execute("DROP SCHEMA " + ((KingbaseESDatabase)this.database).quote(new String[]{this.name}) + " CASCADE", new Object[0]);
    }

    @Override
    protected void doClean() throws SQLException {
        Iterator<String> var1 = this.generateDropStatementsForMaterializedViews().iterator();

        String statement;
        while(var1.hasNext()) {
            statement = (String)var1.next();
            this.jdbcTemplate.execute(statement, new Object[0]);
        }

        var1 = this.generateDropStatementsForViews().iterator();

        while(var1.hasNext()) {
            statement = (String)var1.next();
            this.jdbcTemplate.execute(statement, new Object[0]);
        }

        KingbaseESTable[] var5 = (KingbaseESTable[])this.allTables();
        int var6 = var5.length;

        for(int var3 = 0; var3 < var6; ++var3) {
            Table table = var5[var3];
            table.drop();
        }

        var1 = this.generateDropStatementsForBaseTypes(true).iterator();

        while(var1.hasNext()) {
            statement = (String)var1.next();
            this.jdbcTemplate.execute(statement, new Object[0]);
        }

        var1 = this.generateDropStatementsForRoutines().iterator();

        while(var1.hasNext()) {
            statement = (String)var1.next();
            this.jdbcTemplate.execute(statement, new Object[0]);
        }

        var1 = this.generateDropStatementsForEnums().iterator();

        while(var1.hasNext()) {
            statement = (String)var1.next();
            this.jdbcTemplate.execute(statement, new Object[0]);
        }

        var1 = this.generateDropStatementsForDomains().iterator();

        while(var1.hasNext()) {
            statement = (String)var1.next();
            this.jdbcTemplate.execute(statement, new Object[0]);
        }

        var1 = this.generateDropStatementsForSequences().iterator();

        while(var1.hasNext()) {
            statement = (String)var1.next();
            this.jdbcTemplate.execute(statement, new Object[0]);
        }

        var1 = this.generateDropStatementsForBaseTypes(false).iterator();

        while(var1.hasNext()) {
            statement = (String)var1.next();
            this.jdbcTemplate.execute(statement, new Object[0]);
        }

    }

    private List<String> generateDropStatementsForSequences() throws SQLException {
        List<String> sequenceNames = this.jdbcTemplate.queryForStringList("SELECT sequence_name FROM information_schema.sequences WHERE sequence_schema=?", new String[]{this.name});
        List<String> statements = new ArrayList();
        Iterator var3 = sequenceNames.iterator();

        while(var3.hasNext()) {
            String sequenceName = (String)var3.next();
            statements.add("DROP SEQUENCE IF EXISTS " + ((KingbaseESDatabase)this.database).quote(new String[]{this.name, sequenceName}));
        }

        return statements;
    }

    private List<String> generateDropStatementsForBaseTypes(boolean recreate) throws SQLException {
        List<Map<String, String>> rows = this.jdbcTemplate.queryForList("select typname, typcategory from sys_catalog.sys_type t left join sys_depend dep on dep.objid = t.oid and dep.deptype = 'e' where (t.typrelid = 0 OR (SELECT c.relkind = 'c' FROM sys_catalog.sys_class c WHERE c.oid = t.typrelid)) and NOT EXISTS(SELECT 1 FROM sys_catalog.sys_type el WHERE el.oid = t.typelem AND el.typarray = t.oid) and t.typnamespace in (select oid from sys_catalog.sys_namespace where nspname = ?) and dep.objid is null and t.typtype != 'd'", new Object[]{this.name});
        List<String> statements = new ArrayList();
        Iterator var4 = rows.iterator();

        Map row;
        while(var4.hasNext()) {
            row = (Map)var4.next();
            statements.add("DROP TYPE IF EXISTS " + ((KingbaseESDatabase)this.database).quote(new String[]{this.name, (String)row.get("typname")}) + " CASCADE");
        }

        if (recreate) {
            var4 = rows.iterator();

            while(var4.hasNext()) {
                row = (Map)var4.next();
                if (Arrays.asList("P", "U").contains(row.get("typcategory"))) {
                    statements.add("CREATE TYPE " + ((KingbaseESDatabase)this.database).quote(new String[]{this.name, (String)row.get("typname")}));
                }
            }
        }

        return statements;
    }

    private List<String> generateDropStatementsForRoutines() throws SQLException {
        String isAggregate = ((KingbaseESDatabase)this.database).getVersion().isAtLeast("11") ? "sys_proc.prokind = 'a'" : "sys_proc.proisagg";
        String isProcedure = ((KingbaseESDatabase)this.database).getVersion().isAtLeast("11") ? "sys_proc.prokind = 'p'" : "FALSE";
        List<Map<String, String>> rows = this.jdbcTemplate.queryForList("SELECT proname, oidvectortypes(proargtypes) AS args, " + isAggregate + " as agg, " + isProcedure + " as proc " + "FROM sys_proc INNER JOIN sys_namespace ns ON (sys_proc.pronamespace = ns.oid) " + "LEFT JOIN sys_depend dep ON dep.objid = sys_proc.oid AND dep.deptype = 'e' " + "WHERE ns.nspname = ? AND dep.objid IS NULL", new Object[]{this.name});
        List<String> statements = new ArrayList();

        Map row;
        String type;
        for(Iterator var5 = rows.iterator(); var5.hasNext(); statements.add("DROP " + type + " IF EXISTS " + ((KingbaseESDatabase)this.database).quote(new String[]{this.name, (String)row.get("proname")}) + "(" + (String)row.get("args") + ") CASCADE")) {
            row = (Map)var5.next();
            type = "FUNCTION";
            if (this.isTrue((String)row.get("agg"))) {
                type = "AGGREGATE";
            } else if (this.isTrue((String)row.get("proc"))) {
                type = "PROCEDURE";
            }
        }

        return statements;
    }

    private boolean isTrue(String agg) {
        return agg != null && agg.toLowerCase(Locale.ENGLISH).startsWith("t");
    }

    private List<String> generateDropStatementsForEnums() throws SQLException {
        List<String> enumNames = this.jdbcTemplate.queryForStringList("SELECT t.typname FROM sys_catalog.sys_type t INNER JOIN sys_catalog.sys_namespace n ON n.oid = t.typnamespace WHERE n.nspname = ? and t.typtype = 'e'", new String[]{this.name});
        List<String> statements = new ArrayList();
        Iterator var3 = enumNames.iterator();

        while(var3.hasNext()) {
            String enumName = (String)var3.next();
            statements.add("DROP TYPE " + ((KingbaseESDatabase)this.database).quote(new String[]{this.name, enumName}));
        }

        return statements;
    }

    private List<String> generateDropStatementsForDomains() throws SQLException {
        List<String> domainNames = this.jdbcTemplate.queryForStringList("SELECT t.typname as domain_name\nFROM sys_catalog.sys_type t\n       LEFT JOIN sys_catalog.sys_namespace n ON n.oid = t.typnamespace\n       LEFT JOIN sys_depend dep ON dep.objid = t.oid AND dep.deptype = 'e'\nWHERE t.typtype = 'd'\n  AND n.nspname = ?\n  AND dep.objid IS NULL", new String[]{this.name});
        List<String> statements = new ArrayList();
        Iterator var3 = domainNames.iterator();

        while(var3.hasNext()) {
            String domainName = (String)var3.next();
            statements.add("DROP DOMAIN " + ((KingbaseESDatabase)this.database).quote(new String[]{this.name, domainName}));
        }

        return statements;
    }

    private List<String> generateDropStatementsForMaterializedViews() throws SQLException {
        List<String> viewNames = this.jdbcTemplate.queryForStringList("SELECT relname FROM sys_catalog.sys_class c JOIN sys_namespace n ON n.oid = c.relnamespace WHERE c.relkind = 'm' AND n.nspname = ?", new String[]{this.name});
        List<String> statements = new ArrayList();
        Iterator var3 = viewNames.iterator();

        while(var3.hasNext()) {
            String domainName = (String)var3.next();
            statements.add("DROP MATERIALIZED VIEW IF EXISTS " + ((KingbaseESDatabase)this.database).quote(new String[]{this.name, domainName}) + " CASCADE");
        }

        return statements;
    }

    private List<String> generateDropStatementsForViews() throws SQLException {
        List<String> viewNames = this.jdbcTemplate.queryForStringList("SELECT relname FROM sys_catalog.sys_class c JOIN sys_namespace n ON n.oid = c.relnamespace LEFT JOIN sys_depend dep ON dep.objid = c.oid AND dep.deptype = 'e' WHERE c.relkind = 'v' AND  n.nspname = ? AND dep.objid IS NULL", new String[]{this.name});
        List<String> statements = new ArrayList();
        Iterator var3 = viewNames.iterator();

        while(var3.hasNext()) {
            String domainName = (String)var3.next();
            statements.add("DROP VIEW IF EXISTS " + ((KingbaseESDatabase)this.database).quote(new String[]{this.name, domainName}) + " CASCADE");
        }

        return statements;
    }

    @Override
    protected KingbaseESTable[] doAllTables() throws SQLException {
        List<String> tableNames = this.jdbcTemplate.queryForStringList("SELECT t.table_name FROM information_schema.tables t LEFT JOIN sys_depend dep ON dep.objid = (quote_ident(t.table_schema)||'.'||quote_ident(t.table_name))::regclass::oid AND dep.deptype = 'e' WHERE table_schema=? AND table_type='BASE TABLE' AND dep.objid IS NULL AND NOT (SELECT EXISTS (SELECT inhrelid FROM sys_catalog.sys_inherits WHERE inhrelid = (quote_ident(t.table_schema)||'.'||quote_ident(t.table_name))::regclass::oid))", new String[]{this.name});
        KingbaseESTable[] tables = new KingbaseESTable[tableNames.size()];

        for(int i = 0; i < tableNames.size(); ++i) {
            tables[i] = new KingbaseESTable(this.jdbcTemplate, (KingbaseESDatabase)this.database, this, (String)tableNames.get(i));
        }

        return tables;
    }

    @Override
    public Table getTable(String tableName) {
        return new KingbaseESTable(this.jdbcTemplate, (KingbaseESDatabase)this.database, this, tableName);
    }

    @Override
    protected Type getType(String typeName) {
        return new KingbaseESType(this.jdbcTemplate, (KingbaseESDatabase)this.database, this, typeName);
    }
}
