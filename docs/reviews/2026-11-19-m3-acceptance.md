# M3 验收记录

> 验收日期：2026-11-19。依据是 [design-v1.md](../design-v1.md) 第五节的 M3 过关条件，以及决策 [G6、G7](../decisions.md#m3-和-m4-修订)。验收对象是分支 `feat/m3-async-outbox` 上从 `f605961` 到 `171ce59` 的 6 个提交，基线为 `main` 的 `30746a5`。

## 结论：通过，可以合并进 `main`

没有阻塞 M3 的问题。M3 的正确性要求都满足了：受理持久化、租约 Relay、幂等消费、失败标记和死信同事务。

文末有 3 条不阻塞的观察，接着 M2 编号为 N5–N7。**N5 必须在 M4 的对比实验之前处理**，否则实验测出来的是 Relay 的轮询间隔，不是热点行。

## 测试复跑（JDK 21，Testcontainers MySQL 8.4 + Kafka 4.2.2）

- `.\mvnw.cmd clean test`：34/34 通过，其中 M3 9 项、M2 12 项、M1 13 项。
- 日志里的 `ERROR` 都来自测试故意注入的故障：死信写入失败、库存扣减后抛异常。

## 过关条件逐条对应

| 过关条件 | 结论 | 依据 |
|---|---|---|
| 受理后立刻能查到 | 满足 | 受理事务同时写入请求行和 outbox 命令，提交后返回 `202`，带 `Location` 和 `Retry-After: 1`。手工验证中 Kafka 离线时照样能查到 `ACCEPTED`。 |
| HTTP 回包丢失 | 满足 | `lostHttpResponseReplaysDurableAdmissionAndClosedCampaignStillFinishes`：活动关闭后，已受理的请求照样完成。手工压测中客户端漏读了 28 笔响应，重放后都返回原结果（见下文）。 |
| Relay 收到 ACK 后、标记前崩溃 | 满足 | `kafkaAckBeforeRelayCrashRedeliversExpiredLeaseWithoutDuplicateOrder`：租约过期后重新投递，不产生第二单。`staleRelayTokenCannotConfirmAnotherWorkersLease`：持有过期令牌的 Relay 不能确认别人的租约。 |
| 消费者提交数据库后、提交 offset 前崩溃 | 满足 | `listenerFailsAfterMysqlCommitBeforeOffsetAndReplaysCompletedRequest`：重投时请求已是终态，直接结束，仍然只有一单。 |
| 毒消息变成 `SYSTEM_FAILED` 并进入死信 | 满足 | `retryExhaustionCommitsFailedStatusAndDurableDeadLetter`：`attemptOrder` 共调用 4 次（首次加 3 次重试），然后标记失败并写死信。`malformedCommandReachesDeadLetterWithoutChangingAnUnrelatedRequest`：格式错误的命令不重试，直接进死信。 |
| G7：失败标记和死信同事务 | 满足 | `failedDeadLetterInsertRollsBackFailureStatusAndDoesNotAdvanceOffset`：死信写入失败时，`SYSTEM_FAILED` 一起回滚，offset 也不前进。 |

另外两项测试：同一个键 40 份并发受理，只写一条命令，售罄后进入终态；outbox 清理只删除超过保留期、且已发布的行。

## 逐项核对

- **Relay 租约（G6）**：
  - `publishBatch` 声明为 `Propagation.NEVER`，发送期间不占数据库事务。
  - 领取走一个短事务：`FOR UPDATE SKIP LOCKED` 选出一批，写入令牌和 `lease_until`，然后提交。
  - 回写时 `markPublished` 的条件带 `lease_token`，相当于一次比较并设置。
  - 构造函数会拒绝不大于 `max.block.ms + delivery.timeout.ms + 1000` 的租约。按当前配置，租约 30 s，下限是 1 + 5 + 1 = 7 s。
  - 发送循环在剩余时间不够一次完整发送时停下，所以 ACK 不会晚于租约到期。截止时间在领取之前就开始计算，而且用的是相对时长（`nanoTime`），应用和数据库之间的时钟偏差不会影响它。
- **Producer**：`acks=all`，开启幂等，`max.block.ms` 1000，`delivery.timeout.ms` 5000，和 G6 要求一致。
- **消费者（G7）**：
  - `process` 先锁请求行，核对活动和请求号，只有 `ACCEPTED` 才建单，所以重投天然幂等。
  - `recover` 在 `REQUIRES_NEW` 事务里重新锁请求。如果请求已是终态就直接返回；否则把请求标成 `SYSTEM_FAILED`，同时写死信 outbox 行。死信的 `event_id` 是确定的，重复写入时 `ON DUPLICATE KEY` 不做任何事。
  - 对不上任何请求的命令只进死信，不改任何请求。这种命令本来就没有可以标记的请求，这样处理是对的。
- **角色拆分**：`(scheduler | consumer) & !api` 时 `web-application-type: none`。`OrderTopics` 只在后台角色里声明主题，命令、事件、死信三个主题各 6 个分区。
- **文档**：设计稿 1.1–1.4 节、outbox 表结构、响应规则、README 的异步一节和升级命令都已同步，`decisions.md` 里 G6、G7 已标为采纳。

## 手工验证（临时 compose：MySQL 8.4 + Kafka 4.2.2，M3 打包出的 jar）

三个角色各起一个进程，都用 `--flashorder.intake-mode=async`。只有 api 进程监听端口，scheduler 和 consumer 不开 Web 服务。一笔请求返回 `202`，约 1.5 s 后查到 `ORDER_CREATED`。

**Kafka 离线：**

1. 停掉 Kafka 后提交 30 笔请求，用时 967 ms，全部返回 `202`，30 行请求都是 `ACCEPTED`，outbox 积压 30 条。
2. 离线约 60 s。Relay 按租约轮转：每批租约到期后被重新领取，共用了 3 个令牌，没有丢消息，scheduler 进程也一直在运行。
3. Kafka 恢复健康后 6 s 内积压清空。31 笔都变成 `ORDER_CREATED`，库存 50 → 19，对账差额为 0。
4. 剩下的 20 条一直等到上一批租约到期（约恢复后 5 s）才被重新领取，见 N6。

**吞吐（活动 2，库存 1000）：**

1. 停掉 scheduler，通过 API 并发提交 600 笔，全部受理。
2. 重启 scheduler，600 条命令从首条发布到末条发布用时 9.0 s，约 67 条/s。按秒统计的发布量一直是 60–80 条，即每秒 3–4 批、每批 20 条，见 N5。
3. 消费者完成速率是 61.5 笔/s，和 Relay 同步，瓶颈在 Relay。
4. 每建一单还会写一条 `ORDER_CREATED` 事件，这 600 条事件在命令之后又用了约 9 s 才发完。
5. Kafka 上命令主题 p2 的 offset 正好是 600，p3 正好是 31，与提交数一致。离线期间租约反复重领，也没有重复投递。消费组 lag 为 0。

**幂等重放：**

- 压测脚本的配置文件结尾多了一个 `next`，curl 中止了还在途的 28 笔传输。服务端已经受理了这 28 笔，客户端没读到响应，正好构成一次真实的"回包丢失"。
- 600 个键全部重放一遍：600 笔都返回 `201`，带 `Idempotent-Replayed: true`。请求、订单、命令都没有新增。对账一致：库存 400，待支付 600，差额 0。

**V1 → V2 升级：**

- 先用 `--spring.flyway.target=1` 建一个只到 V1 的库，用 sync 模式下 3 单，留下 3 条未发布的订单事件，模拟 M2 的旧库。
- 再用 async 模式的 `api,scheduler` 启动：Flyway 迁移到 V2，3 条旧事件全部发出，租约列已清空。
- 真实的 M2 库里，所有历史事件都没有发布过。第一次启动 Relay 时会按 N5 的速率一次性补发这些事件。

## 不阻塞的观察

| # | 观察 | 建议时间 |
|---|---|---|
| N5 | **Relay 每次调度只处理一批。** [OutboxRelayScheduler.java:23](../../src/main/java/com/flashorder/outbox/OutboxRelayScheduler.java) 每 250 ms 调用一次 `publishBatch`，每批默认 20 条，即使这批是满的也不接着取。实测每个 Relay 每秒 60–80 条。每笔请求要发两条消息（命令和事件），所以持续进单时，端到端只有大约 35 笔/s。设计稿第五节的持续写入实验要比较 sync 和 async 的库存 TPS，按现在的写法，async 一侧测到的是这个轮询上限。建议：批是满的就继续领取，直到取不满或超出单轮时限（M2 的取消任务就是这样做的）；`publishBatch` 改为返回领取到的条数；批大小调到 100–200；改完重测。 | **M4 对比实验前，必须处理** |
| N6 | **发送失败的消息要等租约到期才能重试。** [OutboxRelay.java:80](../../src/main/java/com/flashorder/outbox/OutboxRelay.java) 只记录一条警告，失败的消息继续持有 30 s 租约。实测 Kafka 恢复后有 20 条多等了约 5 s，最坏情况接近 30 s。M4 要记录"恢复后积压清空时间"，这段等待会算进去。另外，即使只有一个 Relay，重试也会让后面的批先发出去；设计稿第 123 行只提到多实例交错，可以补一句。建议：对已经明确失败的 future，按令牌提前释放租约；超时或结果未知的消息保持现状。重复投递有消费者幂等兜底。 | M4 前 |
| N7 | **消费者并发度是 1。** `kafka-consumer-groups` 显示 6 个分区都在同一个 consumer 上，不同活动的命令也被串行处理了。单个活动单线程是设计如此（第 119 行），但多活动压测和多实例部署需要设置 `spring.kafka.listener.concurrency`，或者起多个 consumer 进程。现在瓶颈在 Relay，N5 修好之后才会暴露这个问题。 | M4，N5 之后 |

M2 的 N3 仍然有效，范围扩大到了 `consumer`：两个后台角色都没有 actuator，Relay 和消费者的指标都采集不到。这一条和 M1 的 F7 一起在 M4 处理。
