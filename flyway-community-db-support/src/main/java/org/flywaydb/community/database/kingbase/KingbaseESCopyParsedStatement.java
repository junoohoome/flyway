//
// Source code recreated from a .class file by IntelliJ IDEA
// (powered by FernFlower decompiler)
//

package org.flywaydb.community.database.kingbase;

import org.flywaydb.core.api.FlywayException;
import org.flywaydb.core.internal.jdbc.JdbcTemplate;
import org.flywaydb.core.internal.jdbc.Result;
import org.flywaydb.core.internal.jdbc.Results;
import org.flywaydb.core.internal.sqlscript.Delimiter;
import org.flywaydb.core.internal.sqlscript.ParsedSqlStatement;

import java.io.Reader;
import java.io.StringReader;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.List;

public class KingbaseESCopyParsedStatement extends ParsedSqlStatement {
    private static final Delimiter COPY_DELIMITER = new Delimiter("\\.", true);
    private final String copyData;

    public KingbaseESCopyParsedStatement(int pos, int line, int col, String sql, String copyData) {
        super(pos, line, col, sql, COPY_DELIMITER, true);
        this.copyData = copyData;
    }

    @Override
    public Results execute(JdbcTemplate jdbcTemplate) {
        Object copyManager;
        Method copyManagerCopyInMethod;
        try {
            Connection connection = jdbcTemplate.getConnection();
            ClassLoader classLoader = connection.getClass().getClassLoader();
            Class<?> baseConnectionClass = classLoader.loadClass("com.kingbase8.core.BaseConnection");
            Object baseConnection = connection.unwrap(baseConnectionClass);
            Class<?> copyManagerClass = classLoader.loadClass("com.kingbase8.copy.CopyManager");
            Constructor<?> copyManagerConstructor = copyManagerClass.getConstructor(baseConnectionClass);
            copyManagerCopyInMethod = copyManagerClass.getMethod("copyIn", String.class, Reader.class);
            copyManager = copyManagerConstructor.newInstance(baseConnection);
        } catch (Exception var12) {
            throw new FlywayException("Unable to find KingbaseES CopyManager class", var12);
        }

        Results results = new Results();

        try {
            try {
                Long updateCount = (Long)copyManagerCopyInMethod.invoke(copyManager, this.getSql(), new StringReader(this.copyData));
                results.addResult(new Result(updateCount, (List)null, (List)null, null));
            } catch (InvocationTargetException | IllegalAccessException var10) {
                throw new SQLException("Unable to execute COPY operation", var10);
            }
        } catch (SQLException var11) {
            jdbcTemplate.extractErrors(results, var11);
        }

        return results;
    }
}
