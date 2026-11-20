# M1 验收记录

> 验收日期：2026-11-19。验收依据是 [design-v1.md](../design-v1.md) 第五节 M1 的过关条件，以及 [decisions.md](../decisions.md) 中的 G1–G5。

## 复核结论：通过，M1 验收完成，可以进入 M2

复核对象是三个提交：`9bd91bb`（基线）、`bc3c352`（F1–F3）、`63bc9f5`（F4–F6、F8）。

- F1–F6、F8 已关闭。F7 按计划留到 M4 前处理，仍处于打开状态。
- `bc3c352` 和 `63bc9f5` 分别用 JDK 21 复跑：`bc3c352` 12/12 通过，`63bc9f5` 13/13 通过。
- F1、F2 在临时 MySQL 8.4 上按验收方式重做了一遍，结果见文末的"复核记录"。

## 首次验收：有条件通过

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
| F1 | 中 | 是 | dev 种子是 Flyway 可重复迁移，导致非 dev 配置在同一个库上无法启动 | 已关闭（`bc3c352`） |
| F2 | 中 | 是 | Connector/J 每个请求多出 6 次会话状态查询 | 已关闭（`bc3c352`） |
| F3 | 低 | 是 | 锁冲突和连接池超时统一返回 500，客户端分不出能否重试 | 已关闭（`bc3c352`） |
| F4 | 低 | 否，M3 前 | 靠解析报错文本判断是哪个唯一键冲突 | 已关闭（`63bc9f5`） |
| F5 | 低 | 否，M4 前 | 插入后多查一次 `SELECT id ... FOR UPDATE` | 已关闭（`63bc9f5`） |
| F6 | 低 | 否，M3 前 | 入口拒绝的响应体和已落库的拒绝分不清；活动不存在时返回 409 | 已关闭（`63bc9f5`） |
| F7 | 低 | 否，M4 前 | 采不到 InnoDB 行锁等待，连接池大小没有显式配置 | **已关闭**（`f5b1ea8`），见 [M4 验收记录](2026-11-20-m4-acceptance.md) |
| F8 | 低 | 否，M2 前 | `expire_at` 不带时区；Controller 的包位置不符合约定 | 已关闭（`63bc9f5`） |

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

## 复核记录（2026-11-19）

**测试**：用 JDK 21（JBR 21.0.8）分别检出两个提交复跑。`bc3c352` 12/12 通过，`63bc9f5` 13/13 通过。

**F1**：用 `63bc9f5` 打出的 jar，在临时 MySQL 8.4 上验证了两种库。

- **新库**：
  1. 用 `dev` 启动后，活动 1 已预置，库存 50/50。
  2. 下 11 单，库存剩 39。
  3. 改用默认配置启动，Flyway 校验通过，应用正常启动。
  4. 再用 `dev` 启动，库存仍是 39，没有改回 50；活动仍只有 1 个。
- **旧库**：先用 `9bd91bb` 的 jar 以 `dev` 启动，历史表里写下原来那条 `dev seed` 记录。再用 `63bc9f5` 的 jar 以默认配置启动，应用正常启动。占位迁移以新的 checksum 重新执行了一次（第 3 条记录）。

**F2**：在新库上发 10 个成功请求，统计 `events_statements_summary_by_digest`：

- 总共 100 条语句，每个请求 10 条。修复前是 170 条，每个请求 17 条。
- `transaction_read_only` 和 `transaction_isolation` 两种查询都是 0 次。
- `SELECT id ... FOR UPDATE` 也是 0 次。
- 剩下的语句是：`SET autocommit` 2 条、幂等查询、插入请求、保存点、插入订单、插入事件、更新请求、扣库存、`COMMIT`。

实现方报告的是 102 条，比我测的多 2 条，应该是统计范围不同，不影响结论。

**F3**：逐行读了代码和测试。

- `DatabaseExceptionHandler` 把 `PessimisticLockingFailureException`、取不到连接，以及由连接问题导致的 `CannotCreateTransactionException` 映射成 `503` + `Retry-After: 1`，其他异常照旧返回 500。
- 锁等待超时和连接池耗尽各有一个确定会触发的集成测试。测试里用真实 `FOR UPDATE` 占住库存行，或者占满连接池。
- 40 个同键并发请求加故障注入的测试，覆盖了首次验收时定的全部断言。

**F4–F6、F8**：逐行读了代码和测试。

- 请求插入撞键时，直接按幂等键回查。
- 订单插入撞键时，用 `hasActiveOrderConflict` 区分是"已持有资格"还是其他唯一键冲突。判断逻辑正确：同时排除了本请求已有订单和 `order_uid` 碰撞这两种情况。
- 两处插入都改用生成键，不再用 JOIN。
- `NOT_ACCEPTED` 和 `404 CAMPAIGN_NOT_FOUND` 都有测试覆盖。
- `expireAt` 返回 UTC 时间，测试里和库里存的值做了对比。
- 两个 Controller 已分别移到 `request` 和 `order` 包。

**附带观察（不阻塞）**：`db/migration/R__dev_seed.sql` 这个占位迁移（内容是 `DO 0`）现在会在每个库上执行一次。以后如果删掉它，所有已经执行过它的库都会再次校验失败，所以它实际上需要永久保留。它不影响功能，但每个库的历史表里都会多出一条 `dev seed`。

## 第二次复核（2026-11-19）：占位迁移已删除，F1 仍然通过

**改动**：基于 `63bc9f5` 的工作区，尚未提交。

- 删除了 `db/migration/R__dev_seed.sql`。
- README 新增"升级用过旧种子迁移的 M1 数据库"一节，包括三点：
  - 更新代码后先执行 `mvnw clean`。
  - 停掉连接该库的实例后，先 `validate`；确认唯一的错误是找不到 `dev seed`，再执行一次 `repair`。
  - 全新的库可以跳过这一步。

上一节附带观察里说的"需要永久保留"不再成立。代价是每个旧库要手动修复一次。

**测试**：`.\mvnw.cmd clean test` 13/13 通过（JDK 21）。

**新库**（用工作区打出的 jar，临时 MySQL 8.4）：

1. 用 `dev` 启动后，Flyway 历史只有 `V1` 一条，库存 50/50。
2. 下 1 单，库存剩 49。
3. 改用默认配置启动，正常启动。
4. 再用 `dev` 启动，库存仍是 49，活动只有 1 个。

**三种旧库**：分别用 `9bd91bb` 和 `63bc9f5` 的 jar 造出来，每个库都先下单作为已有数据。

| 旧库历史 | 修复前用默认配置启动 | README 的 `validate` | `repair` 后 `validate` | 修复后启动并下 1 单 |
|---|---|---|---|---|
| A：只有原始种子 | 失败：`Detected applied migration not resolved locally: dev seed` | 失败，同样的唯一错误 | 通过（3 条） | 正常；订单 1→2，库存 49→48 |
| B：原始种子 + `DO 0` | 失败，同上 | 失败，同上 | 通过（4 条） | 正常；订单 2→3，库存 48→47 |
| C：只有 `DO 0` | 失败，同上 | 失败，同上 | 通过（3 条） | 正常；订单 1→2，库存 49→48 |

- `repair` 只在历史表里追加一条 `DELETE` 类型的 `dev seed` 记录，不动已有的订单和库存。
- 修复后的库 A 再用 `dev` 启动，库存仍是 48，没有被重置。
- Maven 插件解析到的是 `flyway-maven-plugin:12.4.0`，由 Spring Boot 的插件管理统一定版，和应用里用的 Flyway 是同一版本。

**提醒**：M1 报告里提到验证过开发配置启动。如果用的是本地 compose 库（端口 13306），它就属于旧库 A、B 或 C 中的一种，用新代码启动它之前要先按 README 修复一次。
