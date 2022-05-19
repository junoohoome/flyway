/*
 * Copyright (C) Red Gate Software Ltd 2010-2022
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.flywaydb.community.database.kingbase;

import lombok.CustomLog;
import org.flywaydb.core.api.ResourceProvider;
import org.flywaydb.core.api.configuration.Configuration;
import org.flywaydb.core.internal.authentication.postgres.PgpassFileReader;
import org.flywaydb.core.internal.database.base.BaseDatabaseType;
import org.flywaydb.core.internal.database.base.Database;
import org.flywaydb.core.internal.jdbc.JdbcConnectionFactory;
import org.flywaydb.core.internal.jdbc.StatementInterceptor;
import org.flywaydb.core.internal.parser.Parser;
import org.flywaydb.core.internal.parser.ParsingContext;

import java.sql.Connection;
import java.sql.Types;
import java.util.Properties;

@CustomLog
public class KingbaseESDatabaseType extends BaseDatabaseType {


    @Override
    public String getName() {
        return "KingbaseES";
    }

    @Override
    public int getNullType() {
        return Types.NULL;
    }

    @Override
    public boolean handlesJDBCUrl(String url) {
        if (url.startsWith("jdbc-secretsmanager:kingbasees:")) {
            throw new org.flywaydb.core.internal.license.FlywayTeamsUpgradeRequiredException("jdbc-secretsmanager");
        }
        return url.startsWith("jdbc:kingbase8:") ;
    }

    @Override
    public String getDriverClass(String url, ClassLoader classLoader) {
        if (url.startsWith("jdbc:p6spy:postgresql:")) {
            return "com.p6spy.engine.spy.P6SpyDriver";
        }
        return "com.kingbase8.Driver";
    }

    @Override
    public boolean handlesDatabaseProductNameAndVersion(String databaseProductName, String databaseProductVersion, Connection connection) {
        return databaseProductName.startsWith("KingbaseES");
    }

    @Override
    public Database createDatabase(Configuration configuration, JdbcConnectionFactory jdbcConnectionFactory, StatementInterceptor statementInterceptor) {
        return new KingbaseESDatabase(configuration, jdbcConnectionFactory, statementInterceptor);
    }

    @Override
    public Parser createParser(Configuration configuration, ResourceProvider resourceProvider, ParsingContext parsingContext) {
        return new KingbaseESParser(configuration, parsingContext);
    }

    @Override
    public void setDefaultConnectionProps(String url, Properties props, ClassLoader classLoader) {
        props.put("applicationName", APPLICATION_NAME);
    }

    @Override
    public boolean detectUserRequiredByUrl(String url) {
        return !url.contains("user=");
    }

    @Override
    public boolean detectPasswordRequiredByUrl(String url) {


        // Postgres supports password in URL
        return !url.contains("password=");
    }

    @Override
    public boolean externalAuthPropertiesRequired(String url, String username, String password) {

        return super.externalAuthPropertiesRequired(url, username, password);


    }

    @Override
    public Properties getExternalAuthProperties(String url, String username) {
        PgpassFileReader pgpassFileReader = new PgpassFileReader();

        if (pgpassFileReader.getPgpassFilePath() != null) {
            LOG.info(org.flywaydb.core.internal.license.FlywayTeamsUpgradeMessage.generate(
                    "pgpass file '" + pgpassFileReader.getPgpassFilePath() + "'",
                    "use this for database authentication"));
        }
        return super.getExternalAuthProperties(url, username);


    }
}
