# FlashOrder

FlashOrder 是限量抢购后端。M1 交付同步建单基线，用 MySQL 条件更新、事务保存点和唯一索引保证库存与幂等。订单和 `ORDER_CREATED` 事件一起落库，消息发布在 M3 接入。

## Windows 本地运行

需要 Java 21 和 Docker Desktop。用 PowerShell 从项目根目录执行。

```powershell
java -version
docker compose up -d --wait
.\mvnw.cmd spring-boot:run '-Dspring-boot.run.profiles=dev'
```

应用绑定 `127.0.0.1:8080`，MySQL 绑定 `127.0.0.1:13306`。如果 PowerShell 里的 Java 版本不对，把当前终端的 `JAVA_HOME` 指到 JDK 21。开发配置自动建表，预置活动 `1` 和 50 件库存。调用时用 `X-User-Id` 模拟身份，例如 `1` 到 `200`。这些接口用于本地开发。

```powershell
curl.exe -i -X POST http://127.0.0.1:8080/campaigns/1/purchase-requests -H 'X-User-Id: 1' -H 'Idempotency-Key: example-m1'
```

成功返回 `201` 和 `requestUid`、`orderUid`。重复执行同一条命令会返回相同结果，并带上 `Idempotent-Replayed: true`。库存不足和已持有订单返回 `409`，已持久化的拒绝同样可重放。可以用同一个 `X-User-Id` 请求 `GET /purchase-requests/{requestUid}` 和 `GET /orders/{orderUid}`。

`/actuator/health` 提供健康状态。`/actuator/prometheus` 输出 HTTP 延迟直方图、连接池指标与 `flashorder_purchase_submissions_total`，可查看成功、拒绝与重放。

## M1 集成测试

```powershell
.\mvnw.cmd test
```

测试默认使用 Testcontainers 启动独立的 MySQL 8.4，检查 200 人争 50 件、同键有货及零库存的并发重放、同用户换键、库存扣减后的故障回滚。也可用 `FLASHORDER_TEST_JDBC_URL`、`FLASHORDER_TEST_DB_USER` 和 `FLASHORDER_TEST_DB_PASSWORD` 指向专用测试库。测试会在该库中新建并保留测试数据。

## 设计

[现行设计](docs/design-v1.md) · [决策记录](docs/decisions.md)

并发测试写法参考 [burst-safe-inventory](https://github.com/AkshajM16/burst-safe-inventory/tree/801b6953e4cb8bc822bfec77cf0d7c7ff782f804)（MIT）。
