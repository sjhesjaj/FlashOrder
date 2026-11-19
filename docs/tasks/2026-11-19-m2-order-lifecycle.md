# M2 任务单：订单闭环

> 依据 [design-v1.md](../design-v1.md) 的 2.3–2.5 节、第三节接口和第五节 M2 过关条件。基线是 `main` 上的 `bd48ccd`。实现方写代码，验收方按本文最后一节复核。

## 范围

| 做 | 不做 |
|---|---|
| 模拟支付接口 | 真实扣款、退款 |
| 超时取消定时任务（`scheduler` 角色） | Outbox Relay、Kafka（M3） |
| 对账接口 | 限流、售罄缓存、监控栈（M4，含 F7） |
| 支付、取消事件写入 outbox | 事件发布 |

## 一、模拟支付

`POST /orders/{orderUid}/payments`，请求头 `X-User-Id`、`Idempotency-Key`（规则同建单：1–128 个可打印 ASCII 字符）。属于 `api` 角色，和 `intake-mode` 无关。

**一个事务内按顺序执行：**

1. `SELECT ... FROM purchase_order WHERE order_uid = ? AND user_id = ? FOR UPDATE`。没有这一行就返回 `404`。
2. 查 `(order_id, idempotency_key)` 的支付记录，有就按原结果返回，带 `Idempotent-Replayed: true`。
3. 订单已是 `PAID`：记一条 `ALREADY_PAID`，返回 `200`。
4. 订单已是 `CANCELLED`：记一条 `REJECTED_CANCELLED`，返回 `409 ORDER_CANCELLED`。
5. 订单是 `PENDING`：执行 `UPDATE purchase_order SET status = 'PAID', paid_at = NOW(6) WHERE id = ? AND status = 'PENDING' AND NOW(6) < expire_at`。
   - 影响 1 行：记一条 `SUCCEEDED`，写 `ORDER_PAID` 事件，返回 `200`。
   - 影响 0 行：说明已到期，记一条 `REJECTED_EXPIRED`，返回 `409 ORDER_EXPIRED`。订单保持 `PENDING`，留给超时任务取消。

**约束：**

- 到期判断只用数据库时间，写在 `UPDATE` 条件里，不在 Java 里比较时间。
- 整个过程不读、不写 `inventory`。
- 响应体：`200` 返回 `orderUid`、`status`、`paidAt`（UTC）；`409` 返回 `orderUid`、`status`、`reason`。
- 第 2 步的重放结果要和首次完全一致，包括状态码。

## 二、超时取消

`scheduler` 角色下的定时任务，间隔可配置（默认 1 秒）。把单批逻辑放在一个可直接调用的服务方法里，测试直接调这个方法，不依赖定时触发。

**每批一个事务：**

1. `SELECT id, campaign_id FROM purchase_order WHERE status = 'PENDING' AND expire_at <= NOW(6) ORDER BY expire_at LIMIT ? FOR UPDATE SKIP LOCKED`，批大小可配置（默认 200）。
2. 把选中的订单改为 `CANCELLED`，写 `cancelled_at`。`active_flag` 随之变成 NULL，购买资格自动释放。
3. 每单写一条 `ORDER_CANCELLED` 事件。
4. 按 `campaign_id` 分组，**按 `campaign_id` 升序**执行 `UPDATE inventory SET available = available + n WHERE campaign_id = ?`。
5. 提交。一批取不到行就结束本轮。

**约束：**

- 库存行永远是事务里最后更新的，和建单保持同一加锁顺序。
- 多个 `scheduler` 实例同时运行必须安全，靠 `SKIP LOCKED`，不引入分布式锁。
- `dev` 配置组加上 `scheduler`，本地一个进程同时跑 `api` 和 `scheduler`。测试里关掉定时触发。

## 三、对账

`GET /admin/campaigns/{campaignId}/reconciliation`，第一版不鉴权，依赖服务只绑定 `127.0.0.1`。活动不存在返回 `404`。

- 用设计稿 2.5 节那条单语句读出 `total`、`available`、`pending`、`paid`，再算 `gap = total - available - pending - paid`。
- 另用一条查询统计请求与订单的对应关系：`ORDER_CREATED` 但没有订单的请求数，`ACCEPTED` 或 `REJECTED` 却有订单的请求数。这条和上一条不在同一快照里，响应里注明。
- 返回 `consistent`：所有差异都为 0 时为 `true`。
- 只报告，不修正任何数据。

## 四、指标

- `flashorder.payments`，标签 `result`（`SUCCEEDED` / `ALREADY_PAID` / `REJECTED_EXPIRED` / `REJECTED_CANCELLED`）、`replayed`。
- `flashorder.orders.cancelled`：超时取消的订单数。
- `flashorder.reconciliation.gap`：最近一次对账的 `gap`，按活动打标签。

## 五、过关条件（集成测试，真实 MySQL 8.4）

测试里需要"过期"时，直接用 SQL 把 `expire_at` 改到过去，不等 15 分钟。

| # | 场景 | 断言 |
|---|---|---|
| T1 | 截止前支付 | `200 PAID`；订单 `PAID`、`paid_at` 非空；1 条 `SUCCEEDED`、1 条 `ORDER_PAID`；`available` 不变；对账一致 |
| T2 | 同一支付键 40 份并发 | 40 个响应状态码和内容相同，39 个带重放头；1 条支付记录，1 条 `ORDER_PAID` |
| T3 | 20 个不同支付键并发 | 全部 `200 PAID`；恰好 1 条 `SUCCEEDED`、19 条 `ALREADY_PAID`；1 条 `ORDER_PAID` |
| T4 | 已到期、尚未取消时支付 | `409 ORDER_EXPIRED`；订单仍 `PENDING`；1 条 `REJECTED_EXPIRED`。随后跑一批取消：订单 `CANCELLED`，`available` +1，1 条 `ORDER_CANCELLED` |
| T5 | 已取消订单再支付 | `409 ORDER_CANCELLED`；订单和库存不变 |
| T6 | **支付等锁一直等到过期** | 用另一个连接 `FOR UPDATE` 锁住订单，`expire_at` 设为 2 秒后；发起支付（会阻塞）；等过了截止时间再释放锁；支付返回 `409 ORDER_EXPIRED`，订单不是 `PAID` |
| T7 | **两个取消任务同时跑** | 2 个活动共 100 个已到期订单；两个线程同时反复调用单批方法直到取不到行；每单恰好 `CANCELLED` 一次、恰好 1 条 `ORDER_CANCELLED`；两个活动的 `available` 各自恰好恢复对应数量 |
| T8 | 支付与取消并发竞争 | 50 个订单的 `expire_at` 设在同一时刻附近，到点时并发发起支付和取消；每单最终只能是 `PAID`（支付 `200`、无取消事件、库存未归还）或 `CANCELLED`（支付 `409`、无 `ORDER_PAID`、库存归还 1），不能两者都有；对账一致 |
| T9 | 超时用户换键重购 | 订单取消后，同一用户换新幂等键建单得到 `201` 和新订单；用旧键重放仍返回原来的 `201` 结果 |
| T10 | 取消任务不误伤 | 未到期的 `PENDING` 和已 `PAID` 的订单在取消后保持不变 |
| T11 | 对账能发现差异 | 正常混合场景后 `consistent = true`；手动把 `available` 改错 1 后，`gap = ±1`、`consistent = false`，数据不被改回 |
| T12 | 权限与输入 | 别人的订单支付返回 `404`；非法支付键返回 `400`；不存在的活动对账返回 `404` |

M1 的 13 项测试必须继续通过。

## 交付要求

1. 在新分支上开发，比如 `feat/m2-order-lifecycle`，不直接提交到 `main`。
2. 按"支付""超时取消""对账"拆成几个提交，方便按 diff 验收。
3. 设计稿里和实现有出入的地方（比如支付响应体、`ALREADY_PAID`），同步改进 `design-v1.md`。
4. 完成后报告：改了哪些文件、新增哪些测试、`.\mvnw.cmd clean test` 的结果。

## 验收方式

- 用 JDK 21 在新分支上跑 `clean test`，逐行读 diff。
- 检查三条加锁约束：支付不碰 `inventory`；取消按 `campaign_id` 升序最后更新库存；到期判断只用数据库时间。
- 在临时 MySQL 上起 `dev` 配置（含 `scheduler`），手工走一遍：下单 → 把 `expire_at` 改到过去 → 等定时任务取消 → 对账一致 → 同用户换键重购成功。
- T6、T7、T8 各重复跑 3 次，确认不是偶然通过。
