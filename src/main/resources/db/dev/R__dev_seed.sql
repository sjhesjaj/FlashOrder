-- Simulated users use X-User-Id values 1 through 200.
INSERT INTO campaign (id, sku, starts_at, ends_at)
VALUES (1, 'FLASH-M1', '2000-01-01 00:00:00', '2099-01-01 00:00:00')
ON DUPLICATE KEY UPDATE id = campaign.id;

INSERT INTO inventory (campaign_id, total, available)
VALUES (1, 50, 50)
ON DUPLICATE KEY UPDATE campaign_id = inventory.campaign_id;
