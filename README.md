# FlashOrder

FlashOrder 是限量抢购后端。M1 用 MySQL 条件更新、事务保存点和唯一索引实现同步建单。M2 加入模拟支付、超时归还库存和对账。建单、支付和取消的事件随各自的事务写入 Outbox，消息发布在 M3 接入。

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

本地 `dev` 同时启用 `api` 和 `scheduler`。单独运行 `scheduler` 时只启动后台任务。每批默认处理至多 200 条到期订单，查不到可取消订单时结束本轮，默认等 1 秒开启下一轮。分别用 `flashorder.order-expiry.batch-size` 和 `flashorder.order-expiry.fixed-delay-ms` 调整。

`/actuator/health` 提供健康状态。`/actuator/prometheus` 输出 HTTP 延迟、连接池、建单、支付、取消及最近一次对账的库存差额。

## 升级用过旧种子迁移的 M1 数据库

从旧版本更新代码后，首次启动前先执行 `.\mvnw.cmd clean`，清理旧构建产物里的迁移脚本。

如果旧库执行过 `R__dev_seed.sql`（包括 `DO 0` 占位版本），先停掉连接该库的 FlashOrder 实例，再用 Flyway 做一次校验和修复。下列命令使用 Compose 的默认连接参数。自定义数据库请改用实际连接参数。

```powershell
$flywayLocal = @(
  '-Dflyway.url=jdbc:mysql://127.0.0.1:13306/flashorder'
  '-Dflyway.user=flashorder'
  '-Dflyway.password=flashorder'
)
.\mvnw.cmd org.flywaydb:flyway-maven-plugin:validate @flywayLocal
# 只有当唯一的校验错误是找不到 dev seed 时，才运行下一条。
.\mvnw.cmd org.flywaydb:flyway-maven-plugin:repair @flywayLocal
```

`repair` 会为 `dev seed` 追加一条删除标记，保留已有订单和库存。修复成功后直接启动应用，全新数据库可跳过这一步。

## 集成测试

```powershell
.\mvnw.cmd test
```

测试默认使用 Testcontainers 启动独立的 MySQL 8.4，覆盖抢购、幂等、故障回滚、支付等锁期间到期、多个取消任务并行和支付与取消竞争。也可用 `FLASHORDER_TEST_JDBC_URL`、`FLASHORDER_TEST_DB_USER` 和 `FLASHORDER_TEST_DB_PASSWORD` 指向空的专用测试库。测试会在该库写入数据，超时扫描也会处理该库的所有到期订单。

## 设计

[现行设计](docs/design-v1.md) · [决策记录](docs/decisions.md)

并发测试写法参考 [burst-safe-inventory](https://github.com/AkshajM16/burst-safe-inventory/tree/801b6953e4cb8bc822bfec77cf0d7c7ff782f804)（MIT）。
