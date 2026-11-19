package com.flashorder;

import org.springframework.test.context.DynamicPropertyRegistry;
import org.testcontainers.mysql.MySQLContainer;

final class MySqlTestDatabase {

    private MySqlTestDatabase() {
    }

    static void configure(DynamicPropertyRegistry registry) {
        String externalUrl = System.getenv("FLASHORDER_TEST_JDBC_URL");
        if (externalUrl != null && !externalUrl.isBlank()) {
            registry.add("spring.datasource.url", () -> externalUrl);
            registry.add("spring.datasource.username",
                    () -> System.getenv().getOrDefault("FLASHORDER_TEST_DB_USER", "flashorder"));
            registry.add("spring.datasource.password",
                    () -> System.getenv().getOrDefault("FLASHORDER_TEST_DB_PASSWORD", "flashorder"));
            return;
        }

        MySQLContainer mysql = LocalMysql.INSTANCE;
        registry.add("spring.datasource.url", mysql::getJdbcUrl);
        registry.add("spring.datasource.username", mysql::getUsername);
        registry.add("spring.datasource.password", mysql::getPassword);
    }

    private static final class LocalMysql {
        private static final MySQLContainer INSTANCE = startMysql();

        private static MySQLContainer startMysql() {
            var mysql = new MySQLContainer("mysql:8.4")
                    .withDatabaseName("flashorder_test")
                    .withUsername("flashorder")
                    .withPassword("flashorder")
                    .withCommand("--transaction-isolation=READ-COMMITTED", "--default-time-zone=+00:00");
            mysql.start();
            return mysql;
        }
    }
}
