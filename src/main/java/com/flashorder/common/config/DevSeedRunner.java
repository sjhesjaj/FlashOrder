package com.flashorder.common.config;

import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.boot.sql.init.dependency.DependsOnDatabaseInitialization;
import org.springframework.context.annotation.Profile;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

@Component
@Profile("dev")
@DependsOnDatabaseInitialization
public class DevSeedRunner implements ApplicationRunner {

    private final JdbcTemplate jdbc;

    public DevSeedRunner(JdbcTemplate jdbc) {
        this.jdbc = jdbc;
    }

    @Override
    @Transactional
    public void run(ApplicationArguments args) {
        jdbc.update("""
                INSERT INTO campaign (id, sku, starts_at, ends_at)
                VALUES (1, 'FLASH-M1', '2000-01-01 00:00:00', '2099-01-01 00:00:00')
                ON DUPLICATE KEY UPDATE id = campaign.id
                """);
        jdbc.update("""
                INSERT INTO inventory (campaign_id, total, available)
                SELECT id, 50, 50 FROM campaign
                WHERE id = 1 AND sku = 'FLASH-M1'
                ON DUPLICATE KEY UPDATE campaign_id = inventory.campaign_id
                """);
    }
}
