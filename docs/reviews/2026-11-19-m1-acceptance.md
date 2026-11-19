# M1 验收记录

> 验收日期：2026-11-19。验收依据是 [design-v1.md](../design-v1.md) 第五节 M1 的过关条件，以及 [decisions.md](../decisions.md) 中的 G1–G5。

## 结论：有条件通过

- **功能验收全部通过。** 我用 JDK 21（JBR 21.0.8）复跑了 `.\mvnw.cmd test`，9 项全部通过。设计稿 M1 的过关条件逐条都有对应的测试。
- **设计一致性没有问题。** 已核对的项目：
  - 建单顺序：请求行 → 保存点 → 候选订单 → 事件 → 请求状态 → 最后扣库存。
  - 售罄时只回滚到保存点，技术异常时整笔回滚。
  - `READ COMMITTED` 在服务端、Hikari、注解三处都有保证。
  - MyBatis 一级缓存设为 `STATEMENT`。
  - 建了 `UNIQUE(order_uid)`。
  - 没有任何表引用 `inventory`。
  - 有 CHECK 约束。
  - 重放响应带 `Idempotent-Replayed` 头。
- **还要修 3 项（F1–F3）。** 修完后我按下面每一项的验收方式复核，全部过了，M1 才正式通过。

## 修复清单

| # | 级别 | 阻塞 M1 | 问题 | 状态 |
|---|---|---|---|---|
| F1 | 中 | 是 | dev 种子是 Flyway 可重复迁移，导致非 dev 配置在同一个库上无法启动 | 待修复 |
| F2 | 中 | 是 | Connector/J 每个请求多出 6 次会话状态查询 | 待修复 |
| F3 | 低 | 是 | 锁冲突和连接池超时统一返回 500，客户端分不出能否重试 | 待修复 |
| F4 | 低 | 否，M3 前 | 靠解析报错文本判断是哪个唯一键冲突 | 待修复 |
| F5 | 低 | 否，M4 前 | 插入后多查一次 `SELECT id ... FOR UPDATE` | 待修复 |
| F6 | 低 | 否，M3 前 | 入口拒绝的响应体和已落库的拒绝分不清；活动不存在时返回 409 | 待修复 |
| F7 | 低 | 否，M4 前 | 采不到 InnoDB 行锁等待，连接池大小没有显式配置 | 待修复 |
| F8 | 低 | 否，M2 前 | `expire_at` 不带时区；Controller 的包位置不符合约定 | 待修复 |

### F1. dev 种子不要写成 Flyway 迁移

**现象（已复现）**：先用 `dev` 启动，再用默认配置连同一个库启动，Flyway 校验失败，应用起不来：

```
Detected applied migration not resolved locally: dev seed.
```

`R__dev_seed.sql` 只在 `dev` 配置下被加载，但它的执行记录写进了 `flyway_schema_history`。M3 用 `consumer`、`scheduler` 配置连本地库时一定会遇到。

**要求**：种子数据不写进 Flyway 的历史表，比如改成只在 `dev` 配置下运行的启动任务。种子写入要幂等，库里已有数据时不能覆盖。

**验收方式**：在一个空库上依次执行下面四步：

1. 用 `dev` 启动，确认活动 1 和 50 件库存已经预置好。
2. 下一单。
3. 用默认配置启动，必须启动成功。
4. 再用 `dev` 启动，库存仍是 49，没有被改回 50。

### F2. 去掉多余的会话状态查询

**现象（已实测）**：发 10 个成功请求，用 `performance_schema.events_statements_summary_by_digest` 统计语句次数：

| 语句 | 次数 | 来源 |
|---|---|---|
| `SELECT @@session.transaction_read_only` | 50 | Connector/J 默认 `useLocalSessionState=false`，每条写语句执行前都查一次 |
| `SELECT @@session.transaction_isolation` | 10 | `@Transactional(isolation = READ_COMMITTED)` 让 Spring 每个事务都读一次当前隔离级别 |

每个请求约 18 条语句，其中 6 条是这类查询。它们会拉长连接占用时间，影响同步基线测出的 TPS。

**要求**：在 `spring.datasource.hikari.data-source-properties` 里设 `useLocalSessionState: true`，测试也要一起生效；去掉注解上多余的 `isolation`。以后不要用原始 SQL 修改会话状态，比如直接执行 `SET autocommit`。

**验收方式**：

- 发 10 个成功请求，上面两种查询的次数都是 0。
- 每个请求的语句数从约 18 条降到约 12 条。
- `databaseSessionUsesReadCommitted` 测试仍然通过，会话隔离级别仍是 `READ-COMMITTED`。

### F3. 可重试的数据库异常返回 503

**现象（按 MySQL 文档推断，未复现）**：如果第一笔请求因技术异常回滚，而同一个键还有两个以上的请求排队等这条唯一键，InnoDB 会让这些排队的请求互相死锁，被选中回滚的一方收到 1213 错误。锁等待超时（1205）和连接池取不到连接，现在也都返回 500。数据不会出错，但客户端分不出这次能不能重试。

**要求**：`PessimisticLockingFailureException`（包括死锁和锁等待超时）和取不到连接的异常，统一映射成 `503`，带 `Retry-After`，错误码可以用 `RETRY_LATER`。其他异常仍然返回 500。

**验收方式**：

- 新增一个测试：把这类异常映射到 `503` + `Retry-After`。
- 新增一个集成测试：注入故障后，用同一个键同时发 40 个请求。断言如下：
  - 恰好 1 个响应是注入故障造成的 500。
  - 其余响应只能是 `201` 或 `503`。
  - 数据库里恰好 1 条请求、1 笔订单。
  - 每个返回 `503` 的请求用同一个键重试，都拿到同一个 `requestUid`。

### F4–F8（不阻塞 M1）

- **F4**：请求插入撞上唯一键时，不再按报错文本区分是哪个键，直接按幂等键重新读一次。服务端改了 `lc_messages`（报错语言）之后，按文本解析就会失效。订单插入那一处如果还需要区分是哪个键冲突，改用查询来确认。
- **F5**：插入请求和订单时用 `useGeneratedKeys` 拿主键，去掉 `lockIdByUid`；`markCreated` 也不再需要 JOIN。
- **F6**：入口拒绝（`CAMPAIGN_CLOSED`、`IDEMPOTENCY_CONFLICT` 等）换成单独的错误响应体，不再用 `status: REJECTED`；活动不存在时返回 `404`。
- **F7**：compose 加上 mysqld-exporter 和 Prometheus，能采到 `Innodb_row_lock_waits` 和 `Innodb_row_lock_time`；Hikari 的 `maximum-pool-size` 显式写进配置。
- **F8**：`expire_at` 返回时带时区，比如用 `Instant` 或带偏移量的时间；Controller 移到 `request` 和 `order` 包里。

## 交付要求

1. 修复前，先把当前 M1 代码作为基线提交一次。目前仓库里还没有任何提交。
2. F1–F3 放在单独的提交里，我按 diff 验收。
3. 修完后报告：改了哪些文件、新增了哪些测试，以及 `.\mvnw.cmd test` 的结果。

验收时我会做三件事：用 JDK 21 复跑全部测试；在临时 MySQL 上重做 F1、F2 的验证步骤；逐行读 diff。结果会更新到上面表格的"状态"一列。
