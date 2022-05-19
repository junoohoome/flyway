//
// Source code recreated from a .class file by IntelliJ IDEA
// (powered by FernFlower decompiler)
//

package org.flywaydb.community.database.kingbase;

import org.flywaydb.core.api.FlywayException;
import org.flywaydb.core.api.logging.Log;
import org.flywaydb.core.api.logging.LogFactory;
import org.flywaydb.core.internal.exception.FlywaySqlException;
import org.flywaydb.core.internal.jdbc.JdbcTemplate;
import org.flywaydb.core.internal.jdbc.RowMapper;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;
import java.util.concurrent.Callable;

public class KingbaseESAdvisoryLockTemplate {
    private static final Log LOG = LogFactory.getLog(KingbaseESAdvisoryLockTemplate.class);
    private static final long LOCK_MAGIC_NUM = 77431708279161L;
    private final JdbcTemplate jdbcTemplate;
    private final long lockNum;

    KingbaseESAdvisoryLockTemplate(JdbcTemplate jdbcTemplate, int discriminator) {
        this.jdbcTemplate = jdbcTemplate;
        this.lockNum = 77431708279161L + (long)discriminator;
    }

    public <T> T execute(Callable<T> callable) {

        try {
            this.lock();
           return callable.call();
        } catch (SQLException var12) {
            throw new FlywaySqlException("Unable to acquire KingbaseES advisory lock", var12);
        } catch (Exception var13) {
            if (var13 instanceof RuntimeException) {
                throw  (RuntimeException)var13;
            } else {
                throw  new FlywayException(var13);
            }

        } finally {
            try {
                this.jdbcTemplate.execute("SELECT sys_advisory_unlock(" + this.lockNum + ")", new Object[0]);
            } catch (SQLException var11) {
                throw new FlywayException("Unable to release KingbaseES advisory lock", var11);
            }
        }


    }

    private void lock() throws SQLException {
        int retries = 0;

        while(!this.tryLock()) {
            try {
                Thread.sleep(100L);
            } catch (InterruptedException var3) {
                throw new FlywayException("Interrupted while attempting to acquire KingbaseES advisory lock", var3);
            }

            ++retries;
            if (retries >= 50) {
                throw new FlywayException("Number of retries exceeded while attempting to acquire KingbaseES advisory lock");
            }
        }

    }

    private boolean tryLock() throws SQLException {
        List<Boolean> results = this.jdbcTemplate.query("SELECT sys_try_advisory_lock(" + this.lockNum + ")", new RowMapper<Boolean>() {
            @Override
            public Boolean mapRow(ResultSet rs) throws SQLException {
                return rs.getBoolean("sys_try_advisory_lock");
            }
        }, new Object[0]);
        return results.size() == 1 && (Boolean)results.get(0);
    }
}
