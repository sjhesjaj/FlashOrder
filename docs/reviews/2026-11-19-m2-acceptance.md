# M2 验收记录

> 验收日期：2026-11-19。依据是 [M2 任务单](../tasks/2026-11-19-m2-order-lifecycle.md)和 [design-v1.md](../design-v1.md) 第五节的 M2 过关条件。验收对象是分支 `feat/m2-order-lifecycle` 上的 `633aaef`、`2b8a11f`、`91a4bf5`、`f29bf1d` 四个提交，基线为 `bd48ccd`。

## 结论：通过，可以合并进 `main`

没有阻塞问题。文末记下了 4 条不阻塞的观察。

## 测试复跑（JDK 21，Testcontainers MySQL 8.4）

- `.\mvnw.cmd clean test`：25/25 通过，其中 M2 12 项、M1 13 项。
- T6、T7、T8 用 `-Dtest` 单独又跑了 3 轮，每轮 3/3 通过。

## 三条加锁约束

| 约束 | 结论 | 依据 |
|---|---|---|
| 支付不碰 `inventory` | 满足 | `PaymentService` 不依赖任何库存 Mapper。T1 在库存行被另一个连接 `FOR UPDATE` 锁住时支付成功，证明支付不会等库存锁。 |
| 取消最后按 `campaign_id` 升序更新库存 | 满足 | `OrderExpiryService` 先改订单、写事件，最后用 `TreeMap` 按活动 ID 升序归还库存。 |
| 到期判断只用数据库时间 | 满足 | 支付在拿到订单锁之后，用 `UPDATE ... AND NOW(6) < expire_at` 判断；取消用 `expire_at <= NOW(6)` 领取。Java 里没有任何时间比较。 |

## 逐项核对

- **支付**：
  - 步骤和任务单一致：先锁订单，再查重放，然后按状态分支，最后写支付记录和事件。
  - 同一个支付键重放时返回原状态码和原响应体，带重放头。
  - 订单行锁把同一订单上的并发支付串行化，所以 T2、T3 不会撞 `payment_attempt` 的唯一键。
- **取消**：
  - 领取用 `FOR UPDATE SKIP LOCKED`，强制走 `(status, expire_at)` 索引。
  - 领取语句里取 `request_uid` 的子查询不加锁。这一点和 MySQL 文档一致：外层的锁定读不会锁子查询里的表。
  - 改单后核对影响行数。归还库存带"不超过总量"的条件，不满足时整批回滚。
  - 取消计数在事务提交之后才增加。
- **对账**：
  - 库存差额用一条语句读出。
  - 请求与订单的对应关系用另一条查询统计，响应里标明 `sameSnapshot: false`。
  - 只读，不改数据。gauge 由一个 Map 持有强引用，不会被垃圾回收掉。
- **测试强度高于任务单**：
  - T7 证明了两个取消任务同时停在同一活动的库存更新上，而且各自锁住的订单互不重叠。
  - T8 先占住一张已到期订单，证明并发取消会跳过它。最后的结果是确定的：25 单支付成功，24 单被取消，1 单因过期被拒后被取消。
- **文档**：设计稿 2.3–2.5 节、接口表、指标和 README 都已同步，和代码一致。

## 手工验证（临时 MySQL 8.4，M2 打包出的 jar）

**`dev` 配置（`api` + `scheduler`）：**

1. 用户 7 下单，返回 `201`。
2. 用 SQL 把 `expire_at` 改到 1 秒前。3 秒内定时任务把订单取消：状态为 `CANCELLED`，有 `cancelled_at`，库存从 49 回到 50，写了 1 条 `ORDER_CANCELLED`。
3. 支付这张已取消的订单，返回 `409 ORDER_CANCELLED`。
4. 对账结果一致：库存 50，待支付 0，已支付 0，差额 0。
5. 同一用户换新键下单，返回 `201` 和一个新订单。用旧键重放，返回原订单，状态码 `201`，带 `Idempotent-Replayed: true`。
6. 支付新订单，返回 `200 PAID`，`paidAt` 是 UTC 时间。
7. 再次对账一致：库存 49，已支付 1，差额 0。
8. Prometheus 能看到 `flashorder_orders_cancelled_total`、`flashorder_payments_total{result=...}` 和 `flashorder_reconciliation_gap{campaign="1"}`。

**单独运行 `scheduler`：**

- 先造 5 张已到期订单，再用 `batch-size=2` 启动 `scheduler`，调度间隔设为 10 分钟。启动后 3 秒内，5 张订单全部被取消，库存从 44 回到 49。
- 这说明一轮调度会连续处理多批，直到取不到订单为止。
- 进程里没有启动 Tomcat，端口也没有监听。

## 不阻塞的观察

| # | 观察 | 建议时间 |
|---|---|---|
| N1 | `restoreStock` 的"不超过总量"条件一旦失败，整批回滚，下一轮还会领到同一批订单，后面所有到期订单都会被卡住。正常情况下不会触发，但一旦库存数据出错，取消任务会整体停摆，只能从日志里发现。建议加一个失败计数指标，或者把出问题的活动单独隔离出来。 | **已关闭**（`f5b1ea8`，失败计数），见 [M4 验收记录](2026-11-20-m4-acceptance.md) |
| N2 | 每次支付成功都要多查一次 `findPaidOrderDetails`（JOIN 请求表），只为了给事件取 `request_uid`。可以像取消任务那样，在锁定订单的那条查询里用子查询一并取出。 | M4 压测前 |
| N3 | 单独运行 `scheduler` 时不开 Web 服务，`/actuator/health` 和 `/actuator/prometheus` 都访问不到，`flashorder_orders_cancelled_total` 也采集不到。这一条和 F7 的监控栈一起处理，比如给它单独开一个 management 端口。 | **已关闭**（`f5b1ea8`，管理端口），见 [M4 验收记录](2026-11-20-m4-acceptance.md) |
| N4 | 对账的 gauge 每个活动一条序列，从不删除。第一版的活动数量少，可以接受。 | 不处理 |
