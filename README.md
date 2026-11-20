# FlashOrder

FlashOrder 是限量抢购后端。M1 实现同步建单，M2 加入模拟支付、超时归还库存和对账。M3 通过 MySQL Outbox 和 Kafka 实现异步建单、失败恢复和死信。两种受理模式共用建单事务逻辑。

## Windows 本地运行

需要 Java 21 和 Docker Desktop。用 PowerShell 从项目根目录执行。

```powershell
java -version
docker compose up -d --wait
.\mvnw.cmd spring-boot:run '-Dspring-boot.run.profiles=dev'
```

应用绑定 `127.0.0.1:8080`，MySQL 绑定 `127.0.0.1:13306`。如果 PowerShell 里的 Java 版本不对，把当前终端的 `JAVA_HOME` 指到 JDK 21。Flyway 负责建表，`dev` 启动任务预置活动 `1` 和 50 件库存，重复启动保留已扣减库存。调用时用 `X-User-Id` 模拟身份，例如 `1` 到 `200`。这些接口用于本地开发。

```powershell
curl.exe -i -X POST http://127.0.0.1:8080/campaigns/1/purchase-requests -H 'X-User-Id: 1' -H 'Idempotency-Key: example-m1'
```

成功返回 `201` 和 `requestUid`、`orderUid`。重复执行同一条命令会返回相同结果，并带上 `Idempotent-Replayed: true`。库存不足和已持有订单返回 `409`，已持久化的拒绝同样可重放。入口拒绝返回 `NOT_ACCEPTED`，活动不存在返回 `404`。锁等待超时、死锁或连接池暂时无法分配连接时返回 `503` 和 `Retry-After`，按原幂等键重试。可以用同一个 `X-User-Id` 请求 `GET /purchase-requests/{requestUid}` 和 `GET /orders/{orderUid}`，订单到期时间 `expireAt` 以 UTC 返回。

将下单响应中的 `orderUid` 替换到支付地址。

```powershell
$orderUid = '<下单响应中的 orderUid>'
curl.exe -i -X POST "http://127.0.0.1:8080/orders/$orderUid/payments" -H 'X-User-Id: 1' -H 'Idempotency-Key: example-payment'
curl.exe -sS http://127.0.0.1:8080/admin/campaigns/1/reconciliation
```

支付成功返回 `200 PAID` 和 UTC `paidAt`。相同支付键重放原结果。过期订单由 `scheduler` 取消并归还库存，原建单键仍重放原订单，换键可重新下单。对账接口报告库存差额和请求与订单的关联差异。

本地 `dev` 启用 `api`、`scheduler` 和 `consumer`，默认为同步受理。超时取消每批默认处理至多 200 条，查不到可取消订单时结束本轮，默认等 1 秒开启下一轮。分别用 `flashorder.order-expiry.batch-size` 和 `flashorder.order-expiry.fixed-delay-ms` 调整。

`/actuator/health` 提供健康状态。`/actuator/prometheus` 输出 HTTP 延迟、连接池、建单、支付、取消及最近一次对账的库存差额。

## M3 异步受理

```powershell
docker compose --profile async up -d --wait
$env:FLASHORDER_INTAKE_MODE = 'async'
.\mvnw.cmd spring-boot:run '-Dspring-boot.run.profiles=dev'
```

Kafka 默认绑定 `127.0.0.1:19092`。下单接口受理成功返回 `202`，用同一个 `X-User-Id` 轮询响应的 `Location`。查询到 `ORDER_CREATED` 后，可以按上面的示例支付。若返回 `SYSTEM_FAILED`，原幂等键重放返回 `409 REQUEST_FAILED`，换键可以重新提交。

使用 `api`、`scheduler`、`consumer` profile 可分别启动三个进程，后台角色不开 HTTP。API 在 Kafka 暂时不可用时仍能持久化受理。改用外部 Kafka 时设置 `FLASHORDER_KAFKA_BOOTSTRAP`。若外部集群禁用客户端建主题，请预建 `flashorder.order-commands`、`flashorder.order-events` 和 `flashorder.order-commands.DLT`，各 6 个分区。

Relay 默认每批领取 100 条。满批全部确认后立即处理下一批，直到取到短批、批次未全部确认，或本轮累计达到 1 秒。已领取的批次按 30 秒租约处理，轮次结束后等待 250 ms 再检查。分别用 `flashorder.outbox-relay.batch-size`、`flashorder.outbox-relay.max-round-ms`、`flashorder.outbox-relay.lease-ms` 和 `flashorder.outbox-relay.fixed-delay-ms` 调整。

对生产者已报告失败的消息及本批未尝试发送的消息，Relay 会将租约缩短至约 1 秒后可重领。通过 `flashorder.outbox-relay.retry-delay-ms` 调整这个间隔。等待时尚无发送结果或进程中断的消息按原租约恢复。

每个消费者进程默认启动 1 个监听线程。需要同时处理多个活动时，可以设 `FLASHORDER_CONSUMER_CONCURRENCY=2` 或调整 `spring.kafka.listener.concurrency`。消费组最多同时使用 6 个命令分区，同一活动的命令仍进同一个分区。

## M4 入口保护和监控

```powershell
docker compose --profile async --profile protection --profile monitoring up -d --wait
$env:FLASHORDER_INTAKE_MODE = 'async'
$env:FLASHORDER_PROTECTION_ENABLED = 'true'
.\mvnw.cmd spring-boot:run '-Dspring-boot.run.profiles=dev'
```

保护启用后按顺序检查用户配额、原幂等请求、活动窗口与售罄快照、活动配额和异步在途门槛。`429` 和在途门槛的 `503 IN_FLIGHT_FULL` 都带 `Retry-After`。售罄快速拒绝返回 `409 NOT_ACCEPTED`，已持久化的业务拒绝仍可重放。

- Redis 默认使用 `127.0.0.1:16379`。用 `flashorder.protection.rate-limit.*` 设置用户和活动的 `*-rate-per-second`、`*-burst`。Redis 降级时会按 `flashorder.protection.rate-limit.instances` 分摊本地配额。
- `flashorder.protection.backlog-limit` 默认 500，异步 API 每 1 秒采样各活动的 `ACCEPTED` 数。售罄快照每 200 ms 刷新，超过 600 ms 走数据库校验。
- Prometheus 在 `http://127.0.0.1:19090`，可查询 `mysql_global_status_innodb_row_lock_waits` 和 `mysql_global_status_innodb_row_lock_time`。Hikari 上限默认 10，可通过 `FLASHORDER_DB_POOL_SIZE` 调整。
- 分三进程抓取时分别启用 `api`、`scheduler,ops` 和 `consumer,ops`。后两者只有管理端口，默认分别为 18081 和 18082。用 `FLASHORDER_MANAGEMENT_PORT` 和对应的 `FLASHORDER_*_SCRAPE_PORT` 配置自定义端口。

压测器只需 Python 3.10 或更新版本。按固定速率提交示例。

```powershell
py -3 .\scripts\benchmark.py --campaign 1 --requests 600 --rate 100 --out .\target\benchmark-example
```

省略 `--rate` 会立即提交一批请求。脚本保存每次请求的 HTTP 结果，并默认每 10 笔抽一笔查询终态。正式对比为两种受理模式分别准备活动和库存，记录相同的提供负载、抽样频率与运行环境。

[M4 对比实验和原始数据](docs/experiments/m4-comparison.md) · [M4 独立验收](docs/reviews/2026-11-20-m4-acceptance.md)

## 升级用过旧种子迁移的 M1 数据库

从旧版本更新代码后，首次启动前先执行 `.\mvnw.cmd clean`，清理旧构建产物里的迁移脚本。

如果旧库执行过 `R__dev_seed.sql`（包括 `DO 0` 占位版本），先停掉连接该库的 FlashOrder 实例，再用 Flyway 做一次校验和修复。下列命令使用 Compose 的默认连接参数。自定义数据库请改用实际连接参数。

```powershell
$flywayLocal = @(
  '-Dflyway.url=jdbc:mysql://127.0.0.1:13306/flashorder'
  '-Dflyway.user=flashorder'
  '-Dflyway.password=flashorder'
)
# 预检允许尚未应用的版本迁移，应用启动时会正常执行它们。
.\mvnw.cmd org.flywaydb:flyway-maven-plugin:validate @flywayLocal '-Dflyway.ignoreMigrationPatterns=*:future,versioned:pending'
# 只有当唯一的校验错误是找不到 dev seed 时，才运行下一条。
.\mvnw.cmd org.flywaydb:flyway-maven-plugin:repair @flywayLocal
```

`repair` 会为 `dev seed` 追加一条删除标记，保留已有订单和库存。修复成功后直接启动应用，全新数据库可跳过这一步。

## 集成测试

```powershell
.\mvnw.cmd test
```

测试默认通过 Testcontainers 启动独立的 MySQL 8.4 和 Kafka 4.2.2。覆盖抢购、支付与取消竞争、受理回包丢失、Kafka ACK 后 Relay 中断、提交订单后 offset 前失败和死信恢复。同步与订单闭环测试可用 `FLASHORDER_TEST_JDBC_URL`、`FLASHORDER_TEST_DB_USER` 和 `FLASHORDER_TEST_DB_PASSWORD` 指向空的专用测试库，异步测试单独启动一套隔离环境。

## 设计

[现行设计](docs/design-v1.md) · [决策记录](docs/decisions.md)

并发测试写法参考 [burst-safe-inventory](https://github.com/AkshajM16/burst-safe-inventory/tree/801b6953e4cb8bc822bfec77cf0d7c7ff782f804)（MIT）。
