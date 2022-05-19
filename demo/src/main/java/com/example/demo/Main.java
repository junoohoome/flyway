package com.example.demo;


import org.flywaydb.core.Flyway;
import org.flywaydb.core.api.configuration.FluentConfiguration;
import org.junit.Test;


public class Main {

    @Test
    public void kingbase() {
        String url = "jdbc:kingbase8://192.168.101.61:54321/testflyway";
        String user = "testflyway";
        String password = "11";
        Flyway flyway = Flyway.configure().dataSource(url, user, password).locations("db/migration/kingbase").load();
        flyway.migrate();
    }

    @Test
    public void dm() {
        String url = "jdbc:dm://192.168.0.108:5236";
        String user = "TESTFLYWAY";
        String password = "minstone@123";
        FluentConfiguration configure = Flyway.configure();
        //   #当迁移时发现目标schema非空，而且带有没有元数据的表时，是否自动执行基准迁移，默认false.
        //   spring.flyway.baseline-on-migrate=true
        configure.baselineOnMigrate(true);
        Flyway flyway = configure.dataSource(url, user, password).locations("db/migration/dm").load();
        flyway.repair();
        flyway.migrate();
    }

    @Test
    public void mysql() {
        String url = "jdbc:mysql://192.168.101.68:13306/testflyway";
        String user = "minstone";
        String password = "minstone@123";
        Flyway flyway = Flyway.configure().dataSource(url, user, password).locations("db/migration/mysql").load();
        flyway.migrate();
    }

}
