# 在线分区压实协议（设计说明）

本文只做协议设计与评审，不修改 `lake/`、既有测试和样例数据。文中所有关键判断都引用当前源码的 `文件:行号`；基线证据来自实际运行 `python3 -m lake.evidence all`，不是未来协议的模拟实现。

## 0. 结论摘要

选定协议是 **“选中输入存活检查 + 追加兼容清单拼接（manifest splice）+ 现有 CAS/回执”**：

1. 维护 worker 只读取 `selected segment IDs` 的载荷，写出内容寻址的压实输出；输出写入与 Catalog 提交仍是两个独立事务。
2. 发布前只读元数据：检查 current manifest 中每个选中 ID 仍各出现一次；若申请回收墓碑，还要求 base manifest 是 current manifest 的**精确前缀**，且 checkpoint 已由 ingest 前进。满足时直接复用已写输出，按 ID 拼接目标清单，绝不重新 `get`/归并选中载荷。
3. 用 `current.version` 作为 CAS base，在同一个 `BEGIN IMMEDIATE` 内写入 snapshot、head 与 receipt。head 前进只导致 `HEAD_CONFLICT`，维护端重新读元数据、重算目标后重试；选中段被其他维护替换时返回 `INPUT_REPLACED`，必须按当前清单重规划或重新排队。
4. 普通局部压实保留选中集合内每个 key 的最大 seq 事件（包括 delete）；只有 `selected == base 全清单` 的显式全覆盖申请才丢弃 delete 回收墓碑。部分选择申请回收会被直接拒绝。
5. 维护提交只复制 `current.checkpoint`，从不消费或推进源位点；ingest 是唯一 checkpoint 推进者。ingest 拿着压实前准备的旧清单提交时，在 `HEAD_CONFLICT` 后以 **current manifest + 自己已写好的 added 段** 重建，而不是使用旧清单。
6. 现有 `token → signature → version` 回执语义不变：成功请求的重试只返回其 receipt.version，绝不回退 head；同 token 换内容返回 `TOKEN_REUSE`，必须使用新的 attempt/token。
7. 新增的持久状态只有一张极小的维护计划日志（每个 job/attempt 一行）和只读的 `receipt(token)` 查询；不引入 segment 租约、按 key 墓碑表、外部服务或物理删除。

---

## 1. 基线、真实调用链与事务边界

### 1.1 输入、读者语义与基线事实

- `seq` 是单个逻辑源的全局连续位点，`key` 是全局主键，`day` 只是物理分区。`winners` 只按 `event["key"]` 聚合，day 只在物理写入分组时使用：`lake/segments.py:36`、`lake/segments.py:43`、`lake/segments.py:48`、`lake/segments.py:49`。
- 固定快照的读取结果 = 所有段中按 key 的最大 seq 事件，再过滤 delete：`lake/segments.py:40`-`lake/segments.py:51`、`lake/segments.py:54`-`lake/segments.py:57`。
- 实际基线（见 `docs/review-evidence.json` 的 `baseline`）：
  - 六条源事件回放后 `checkpoint=6`，live rows 为 `A/2026-10-02/15`、`C/2026-10-02/7`；
  - 当前离线压实 version `3 → 4`，段数 `4 → 2`，B 的墓碑仍保留；
  - 固定 `version=1` 仍读到 `A=10`、`B=20`。这些断言由 `lake/evidence.py:19`-`lake/evidence.py:28` 产生，并由 `tests/test_lake.py` 的七项测试固定。
- 全部基线是实际运行时现场生成的，不能把结构检查通过当成协议正确；A—F 的协议正确性必须按第 7 节逐条论证。

### 1.2 ingest 调用链

`cdc.ingest` → `cdc.prepare` → `catalog.snapshot` → 连续 seq 校验 → `write_partitioned`/`store.put` → `PreparedBatch` → `cdc.commit` → `catalog.publish`：

- `lake/cdc.py:13`-`lake/cdc.py:19`：读 base snapshot，并要求事件 seq 恰为 `base.checkpoint+1 .. +n`。
- `lake/cdc.py:27`：先写 SegmentStore 对象；`lake/cdc.py:28`：清单是 `base.segments + added`，checkpoint 是最后一条 seq。
- `lake/cdc.py:31`-`lake/cdc.py:33`：提交时才调用 `catalog.publish`。
- 因此 ingest 的对象写入与 Catalog 提交之间没有事务；`PreparedBatch` 可以跨维护发布而变旧。

### 1.3 压实调用链

当前离线压实：`compact.plan` → `catalog.snapshot` → `winners(store, base.segments)` → `write_partitioned` → `Plan` → `compact.publish` → `catalog.publish`：

- `lake/compact.py:14`-`lake/compact.py:18`：一次读取**全部** base segments 的载荷并写输出。
- `lake/compact.py:21`-`lake/compact.py:23`：发布仍走同一个 `catalog.publish`。
- 当前实现没有“选中段”“只读选中载荷”“回收墓碑”或“复用已写输出”的概念；这正是本轮要设计的部分。

读者链：`catalog.snapshot(version)` → `read_rows(store, snapshot)`，即 `lake/catalog.py:35`-`lake/catalog.py:44` 与 `lake/segments.py:54`-`lake/segments.py:57`。固定 version 的读取与 head 无关。

### 1.4 事务边界（现有代码的事实）

- **SegmentStore 是独立适配器**：`SegmentStore` 使用自己的 SQLite 连接，`put` 以内容哈希为 ID 并 `INSERT OR IGNORE`，`get` 校验哈希：`lake/segments.py:9`-`lake/segments.py:19`、`lake/segments.py:21`-`lake/segments.py:29`。它不参与 Catalog 事务。
- **Catalog 是唯一原子提交点**：`BEGIN IMMEDIATE` 在 `lake/catalog.py:52`，snapshot 插入在 `lake/catalog.py:68`-`lake/catalog.py:69`，head 更新在 `lake/catalog.py:70`-`lake/catalog.py:71`，receipt 插入在 `lake/catalog.py:72`-`lake/catalog.py:73`，`COMMIT` 在 `lake/catalog.py:75`，异常 `ROLLBACK` 在 `lake/catalog.py:77`-`lake/catalog.py:79`。
- **没有跨两个数据库的事务**；Catalog 提交成功后，SegmentStore 对象可能早已存在；Catalog 回滚后，已写对象成为孤儿但不可变，不参与任何 head。历史 snapshots 没有删除路径，`lake/catalog.py:22`-`lake/catalog.py:24` 只有 INSERT/SELECT。
- **现有 publish 的语义**：token 已存在时校验 signature，匹配就返回 receipt 对应版本（`lake/catalog.py:54`-`lake/catalog.py:60`），不修改 head；不存在时才 CAS base（`lake/catalog.py:62`-`lake/catalog.py:64`）、拒绝 checkpoint 回退（`lake/catalog.py:65`-`lake/catalog.py:66`）、递增 version 并原子写三张表（`lake/catalog.py:67`-`lake/catalog.py:74`）。
- 当前 Catalog **不能区分 ingest 与 maintenance**：两者都调用同一个 `publish`（`lake/cdc.py:31`-`lake/cdc.py:33`、`lake/compact.py:21`-`lake/compact.py:23`），checkpoint 只做下界检查（`lake/catalog.py:65`-`lake/catalog.py:66`），因此维护侧“绝不推进 checkpoint”必须由新协议的调用方式和（建议的）`checkpoint_mode` 前置条件共同保证。

---

## 2. 记号与不变量

对一个 manifest `M`，记 `E(M)` 为所有段载荷事件的多重集合；对 key `k`，读取结果是 `max_seq(E(M), k)` 的 upsert 或空。

- **f(S)**：对选中集合 `S` 的每个 key 保留最大 seq 事件，包括 delete。这就是 `winners(store, S)`（`lake/segments.py:40`-`lake/segments.py:51`）后由 `write_partitioned` 重分区（`lake/segments.py:32`-`lake/segments.py:37`）。
- **g(S)**：在 f(S) 的基础上，若某 key 的最大事件是 delete，则省略它（墓碑回收）。仅在 `S` 覆盖完整 base manifest 时允许。
- **base manifest**：调度方给出的 `base snapshot version` 对应的 `snapshot.segments`；历史版本永久可读（`lake/catalog.py:35`-`lake/catalog.py:44`）。
- **current manifest**：head 当前指向的 snapshot。

协议必须逐条维持：

- **I1（全 key 身份）**：任何发布后的读取都按 key 聚合，不能按 day 拆分身份；day 只是输出重分区。
- **I2（已提交源前缀）**：当前结果的业务状态等价于已提交 seq 前缀的按 key 最新 upsert/delete。
- **I3（不丢新增）**：head 前进后由 ingest 追加的新段必须原样留在新 manifest。
- **I4（不复活删除）**：局部压实必须保留必要 delete；全覆盖回收只能在证明“没有更老事件位于选中集合之外”时发生。
- **I5（维护不碰 checkpoint）**：维护发布会话只读取并复制 current checkpoint；ingest 才推进它。
- **I6（历史不可变）**：旧 snapshot 与旧 segment 对象永久保留；压实只新增对象。
- **I7（回执只确认，不改 head）**：任何 receipt 命中路径不得写 head；旧回执只代表它自己的精确请求。

---

## 3. 发布策略比较与最终选择

### P1：每次从头重算 + 整清单 CAS

做法：维护在每次发布失败后，重新读取选中段载荷、重新 `winners`/`write_partitioned`，再从当前 head 重建完整清单，以 `current.version` 为 base 调用 `publish`。

- 正确性可以做到，但**在固定时序 A 明确不满足**：维护在 `v1` 已经写好输出，ingest 提交 `v2` 后，P1 必须重新读取并归并选中载荷，违反“变化仅来自合法源追加且选中输入仍有效时，必须复用已写输出，不能每次重读、归并全部选中载荷”的要求。
- 在持续 ingest 下，每次 head 前进都让重算成果作废，可能反复重算（题目不要求无限冲突有界完成，但显式禁止每次重读）。
- 它也没有回答重叠 worker 的输出归属；仅换 job ID 不能避免重新引入旧段。

### P2：跨对象写入与发布的维护锁 / 两阶段意图

做法：维护先取得 Catalog 级排他锁或写一条“意图”记录，在持有期间写 SegmentStore 输出，再发布。

- **在固定时序 B/C 不满足**：ingest 会被维护锁阻塞，违背“ingest 持续运行”；C 中两个维护 worker 也不能各自准备输出。
- 锁无法跨越两个数据库形成分布式事务：写完对象后崩溃会留下锁/意图和孤儿对象，恢复语义复杂；若锁自动释放，ingest 仍可能前进，复用问题原样存在。
- 因此不以锁/租约作为正确性机制。

### P3（选定）：选中输入存活 + 追加兼容清单拼接 + 现有 CAS/回执

做法：元数据核对（选中 ID 存活；回收时 base 前缀 + checkpoint 前进）→ 按 ID 拼接目标清单 → 以 current.version 为 base 的现有 `publish` CAS；不读未选载荷、不重读已写输出。

| 维度 | P1 重算 | P2 锁/意图 | **P3 清单拼接（选）** |
|---|---|---|---|
| A：head 因合法追加前进 | 重读、重算选中载荷，违反复用要求 | ingest 被阻塞 | 元数据核对后复用输出 |
| B：ingest 持旧清单 | 需要重算整批 | 锁顺序导致等待 | 以 current 清单 + 已写 added 重建 |
| C：重叠维护 | 可能重新引入旧段 | 两 worker 不能并行准备 | 先发布者替换 ID，后者 `INPUT_REPLACED` |
| 崩溃/响应丢失 | 重算成本高 | 锁/意图恢复不明确 | 对象孤儿可重算；receipt 精确确认 |
| 事务边界 | 不跨库 | 试图跨库 | 维持两库独立，Catalog 单点原子 |

替代方案失败的“具体时序”已经写在各节：P1 失败于 A 的 ingest 追加；P2 失败于 B/C 的并行与持续 ingest。

---

## 4. 选定协议

### 4.1 维护计划（只读选中载荷）

维护调度方给出：`base_version`、`selected segment IDs`、`reclaim_tombstones`、`logical job_id`。worker 增加本地 `attempt` 与 `token`。

计划阶段：

1. `base = catalog.snapshot(base_version)`（历史版本永久可读，`lake/catalog.py:35`-`lake/catalog.py:44`）。
2. 校验 `selected` 非空、无重复、且 `set(selected) ⊆ set(base.segments)`。
3. 若 `reclaim`：**必须** `set(selected) == set(base.segments)`，否则在读取任何载荷前拒绝 `TOMBSTONE_RECLAIM_REQUIRES_FULL_COVERAGE`。理由是局部选择无法在不扫描未选载荷的情况下证明没有更老的 upsert 会被复活。
4. 只对 `selected` 调 `store.get`，计算 `winners(store, selected)`；不调用 `winners(store, current.segments)`，不读未选载荷。
5. 非回收：保留 f(S)（包括 delete）；回收：过滤掉 delete，得到 g(S)。
6. `outputs = write_partitioned(store, kept_events)`（`lake/segments.py:32`-`lake/segments.py:37`）；`store.put` 的内容寻址使重试幂等（`lake/segments.py:14`-`lake/segments.py:19`）。
7. 所有输出写完后，把计划写入最小日志（见 4.5）；日志写入失败/崩溃只会留下孤儿对象，不会改变 head。

`Plan` 至少含：`job_id, attempt, token, base_version, base_segments, selected, outputs, reclaim`。其中 `base_segments` 可从历史 snapshot 再取，不必重复持久化；计划日志只需保存恢复所需的最小字段。

### 4.2 复用与重叠判定（只读元数据）

发布前读取 `current = catalog.snapshot()`，按以下顺序判定：

1. **输入存活（所有场景）**：对每个 `s ∈ selected`，要求 `current.segments.count(s) == 1`。否则说明该输入段已被其他维护替换/移除，抛 `Conflict("INPUT_REPLACED")`，必须按 current 重新规划或重新排队。**job_id 不同不等于互不冲突；冲突只看段 provenance**。
2. **追加兼容（reclaim 场景）**：取 `base = catalog.snapshot(plan.base_version)`，要求 `current.segments[:len(base.segments)] == base.segments`（base manifest 是精确前缀），且若 `len(current.segments) > len(base.segments)` 则必须有 `current.checkpoint > base.checkpoint`。否则抛 `Conflict("BASE_NOT_APPEND_ONLY")`。
   - 这条元数据证明：base 的段一个都没被替换；尾段的源事件只可能来自 base checkpoint 之后的合法 ingest 追加。其他维护若只对尾段做局部压实，按 f 保持等价；若其做全覆盖回收，必然替换至少一个 base 段，从而使前缀检查失败。而 ingest 的连续 seq 校验（`lake/cdc.py:16`-`lake/cdc.py:19`）与 checkpoint=最后 seq（`lake/cdc.py:27`-`lake/cdc.py:28`）保证追加事件 seq 全部大于 base checkpoint。维护从不推进 checkpoint，因此 checkpoint 前进本身就标记了“合法源追加”。
   - 非回收的局部压实不需要该前缀检查：部分压实保留 f(S)，对任意 outside `T` 都等价（见第 8 节引理），所以即使其他维护替换了不相交段，只要选中 ID 仍存活，也可安全拼接复用。
3. **不满足存活/前缀**：拒绝，不发布，不拼接旧输出。C 中 worker 2 就是这种情况：它选的最早段已被 worker 1 的全覆盖输出替换，必须新 attempt 按 current 重规划；绝不能重新引入旧段。
4. 判定只使用 manifest/checkpoint/version 元数据，不调用 `store.get`，因此 A 的复用不重新归并载荷。

### 4.3 目标清单拼接

在 current manifest 上按 ID 操作：

1. 找到 `selected` 中段在 current 的最早位置 `insert_at`；若已按 4.2 保证每个 ID 存在，该位置必存在。
2. `kept = [id for id in current.segments if id not in selected]`。
3. `extra = [id for id in outputs if id not in kept]`（若输出 ID 已作为相同内容存在，去重以免 `DUPLICATE_SEGMENT`；相同内容重复不会改变 `winners`）。
4. `target = kept[:insert_at] + extra + kept[insert_at:]`；再次检查 `len(set(target)) == len(target)`。段顺序不影响读取语义；它只影响下一次前缀/位置元数据。
5. `checkpoint = current.checkpoint`（复制，不推进、不回退）。
6. 以 `base = current.version`、`segments = target`、`checkpoint`、`token` 调用 `catalog.publish`。CAS 保证：验证时读到 version `v`，若提交时 head 已变为 `v+1`，则 `HEAD_CONFLICT`（`lake/catalog.py:62`-`lake/catalog.py:64`），不会把基于旧 `v` 的 target 写入。version 严格递增（`lake/catalog.py:67`），因此 version 相同即 snapshot 相同。
7. 若 `HEAD_CONFLICT`：只重读元数据、重新做 4.2 和拼接，再次 CAS；不重读载荷。若重试次数超限（第 9 节）则重新排队；若 `INPUT_REPLACED` 则结束本 attempt。

### 4.4 墓碑回收的充分条件

**局部（非全覆盖）**：永远保留 f(S) 中每个 key 的最大事件；若最大是 delete，它就是必要墓碑，用来压制未选段中更老的 upsert。`lake/segments.py:55`-`lake/segments.py:57` 在读取时才过滤 delete，因此墓碑必须留在段里。

**全覆盖回收**：`selected` 恰为 base 的全部段。此时 base 中所有事件都在 S 内：
- 对每个 key，f(S) 的最大事件若是 delete，则 base 中没有更老的 outside 事件可被它压制；
- 丢弃它后，g(S) 在该 key 为空，base 的读取结果同样为空；
- 对 plan 之后合法的源追加 `T`，每个追加事件 seq 都大于 base checkpoint，也大于被丢弃 delete 的 seq。因此原始 f(S)∪T 与 g(S)∪T 对每个 key 都取 T 的最大事件（或为空），结果等价。若 T 对某 key 是 upsert，则它本来就是 delete 之后的新值；若是 delete，则仍为空。
- 若任何事件 seq ≤ base checkpoint 却位于选中集合之外，则上述证明不成立；这正是“必须全覆盖”的原因。

**不能证明时**：申请回收但 `selected != base.segments` → 拒绝（D 的第一种申请）；base 被替换/前缀不成立 → `BASE_NOT_APPEND_ONLY` 或 `INPUT_REPLACED`，重新规划。历史 snapshot 中的旧墓碑照常保留；回收只是新 manifest 不再携带它，不做物理删除。

### 4.5 最少持久状态与恢复字段

现有持久状态已足够表达的结果：

- `snapshots(version, segments, checkpoint)`：每个版本的不可变清单与源 checkpoint（`lake/catalog.py:22`-`lake/catalog.py:24`）。
- `head(singleton, version)`：唯一当前指针（`lake/catalog.py:25`-`lake/catalog.py:27`）。
- `receipts(token, signature, version)`：精确请求回执（`lake/catalog.py:28`-`lake/catalog.py:30`）。
- SegmentStore 的内容寻址对象（`lake/segments.py:11`-`lake/segments.py:19`）。

新增的**唯一**持久元数据是每个未完成维护 attempt 一行的小日志，用于“输出已写完但尚未发布就崩溃”的恢复复用：

```sql
CREATE TABLE IF NOT EXISTS maintenance_plans(
  job_id      TEXT    NOT NULL,
  attempt     INTEGER NOT NULL,
  token       TEXT    NOT NULL UNIQUE,
  base_version INTEGER NOT NULL,
  selected    TEXT    NOT NULL,   -- JSON array of segment ids
  outputs     TEXT    NOT NULL,   -- JSON array; 行存在即表示对象已写完
  reclaim     INTEGER NOT NULL,
  PRIMARY KEY(job_id, attempt)
);
```

- 不保存 per-key 状态、不保存锁/租约、不保存墓碑表。
- 是否已发布不重复存：用只读 `receipt(token)` 查询即可；receipt 是唯一成功凭据。
- `attempt` 是逻辑 job 的重试代次；计划内容（base/selected/outputs/reclaim）变化时必须新 attempt，token 由 `(job_id, attempt)` 确定性派生，便于崩溃后按 token 确认原请求。
- 计划行可在发布成功后继续保留；本题不做物理清理。

**新增 Catalog 只读接口**：`receipt(token)` 执行 `SELECT version FROM receipts WHERE token=?`，命中返回 `snapshot(version)`，未命中返回 `None`。它绝不 `UPDATE head`，因此旧回执只用于确认，不回退 head。

**建议的最小 publish 扩展**：给 `catalog.publish` 增加可选关键字 `checkpoint_mode="ingest"|"maintenance"`。在无 receipt 的分支、version CAS 通过之后，若为 `maintenance` 且 `checkpoint != current.checkpoint`，抛 `CHECKPOINT_NOT_OWNED`。receipt 命中分支完全不变，因此不影响重试确认。默认 `"ingest"` 保持现有所有调用和测试行为（`lake/catalog.py:46`）。

---

## 5. 关键接口、错误与事务前置条件

| 接口 | 输入 | 输出 | 错误 | 事务前置条件 |
|---|---|---|---|---|
| `Catalog.snapshot(version=None)`（现有） | 可选历史 version | `Snapshot(version, segments, checkpoint)` | `KeyError(version)`（`lake/catalog.py:42`-`lake/catalog.py:43`） | 只读；不能删除历史行 |
| `Catalog.publish(base, segments, checkpoint, token, checkpoint_mode="ingest")`（扩展自 `lake/catalog.py:46`） | base、目标清单、checkpoint、token、角色 | 已提交或回执对应的 `Snapshot` | `DUPLICATE_SEGMENT`（47-48）、`HEAD_CONFLICT`（63-64）、`CHECKPOINT_REGRESSION`（65-66）、`TOKEN_REUSE`（58-59）、`CHECKPOINT_NOT_OWNED`（维护模式新增） | `BEGIN IMMEDIATE` 内完成 receipt 检查、CAS、snapshot/head/receipt 原子写；维护调用 checkpoint 必须等于 current |
| `Catalog.receipt(token)`（新增只读） | token | `Snapshot` 或 `None` | 无（未命中 None） | 只读；不得写 head |
| `Catalog.record_plan/load_plan`（新增） | 计划字段 / job+attempt | 计划行 | `JOB_ATTEMPT_CONFLICT` | 独立于发布事务；行存在 ⇒ outputs 已写完 |
| `compact.plan_subset(base_version, selected, reclaim, job_id, attempt, token)`（新增） | 上述维护输入 | `Plan`（含 outputs） | `TOMBSTONE_RECLAIM_REQUIRES_FULL_COVERAGE`、`SELECTED_NOT_IN_BASE`、`DUPLICATE_SELECTED` | 只 `store.get(selected)`；base 取自历史 snapshot |
| `compact.publish_plan(plan)`（新增） | `Plan` | 发布后的 `Snapshot` | `INPUT_REPLACED`、`BASE_NOT_APPEND_ONLY`、`RETRY_EXHAUSTED`、`HEAD_CONFLICT`、`TOKEN_REUSE` | 先读 current 元数据，再以 current.version 为 CAS base；复用时不读载荷 |
| `cdc.prepare(catalog, store, events, token)`（现有） | base snapshot、连续事件 | `PreparedBatch`（建议加 `added`、`batch_id`） | `EMPTY_BATCH`、`NONCONTIGUOUS_SOURCE`、`BAD_AMOUNT`、`BAD_KEY_OR_PARTITION`（15-26） | 对象先写；不提交 |
| `cdc.commit/prepare` 的 rebase 路径（新增） | 旧 `PreparedBatch`、events | 发布后的 `Snapshot` | `HEAD_CONFLICT`、`SOURCE_ALREADY_CONSUMED_OR_REPLAY`、`NONCONTIGUOUS_SOURCE` | 用 current manifest + `prepared.added`，不得用旧 `prepared.manifest`；新 attempt token |

所有错误都不写入新 snapshot：`publish` 异常路径 `ROLLBACK`（`lake/catalog.py:77`-`lake/catalog.py:79`）；`HEAD_CONFLICT` 不消费源数据，也不产生 receipt。

---

## 6. 原子提交、回执与崩溃窗口

### 6.1 原子关系

`publish` 的单个 `BEGIN IMMEDIATE`（`lake/catalog.py:52`）内：

1. 先查 receipt；命中则只返回 `snapshot(receipt.version)`，**不 UPDATE head**（`lake/catalog.py:54`-`lake/catalog.py:60`）。
2. 未命中则读 head（`lake/catalog.py:62`），CAS base（`lake/catalog.py:63`-`lake/catalog.py:64`），检查 checkpoint（`lake/catalog.py:65`-`lake/catalog.py:66`）。
3. 同一事务写 `snapshots`（version/segments/checkpoint，`lake/catalog.py:68`-`lake/catalog.py:69`）、更新 `head`（`lake/catalog.py:70`-`lake/catalog.py:71`）、写 `receipts(token, signature, version)`（`lake/catalog.py:72`-`lake/catalog.py:73`）。

因此：**head.version → snapshot.version → (segments, checkpoint)** 是同一次原子提交；receipt.version 指向那个 snapshot。SegmentStore 对象是提交的输入前提，不在该事务内。

### 6.2 崩溃/丢失窗口

| 窗口 | 状态 | 恢复动作 | 安全性 |
|---|---|---|---|
| 输出已写完、未发布即崩溃 | head 不变；可能有孤儿 outputs；计划日志可能存在 | 日志存在：直接复用 ID；日志不存在：按同一 job/base/selected 重算，内容寻址 `put` 幂等（`lake/segments.py:17`） | 无快照/回执变化；孤儿对象不参与读取 |
| 发布成功、响应丢失 | snapshot/head/receipt 已原子提交（`lake/catalog.py:68`-`lake/catalog.py:75`） | `receipt(token)` 命中；或重放完全相同的 `(base,manifest,checkpoint,token)` 得到 receipt 分支 | 返回原 receipt.version；不更新 head |
| 响应丢失后 head 又前进 | 旧 receipt.version = r，current head = r+1 或更高 | 继续用旧 token/精确旧请求确认 r；若要作用于新 head，使用新 attempt/token | receipt 分支不做 `UPDATE head`（`lake/catalog.py:60` 与 `lake/catalog.py:70` 分离），旧回执不能回退 head |
| 恢复时请求内容改变 | token 相同但 signature 不同 | `TOKEN_REUSE`（`lake/catalog.py:58`-`lake/catalog.py:59`） | 失败关闭；换新 token/attempt 后重新发布 |
| 写对象中途崩溃 | 部分 outputs；未执行发布 | 重算缺失/全部输出；不会出现引用不完整清单的成功提交 | Catalog 只在输出完整后调用 |

关键区分是 **“确认原请求” vs “提交变化后的请求”**：token 在第一次成功提交时被 receipt 绑定到 `(base, manifest, checkpoint)` 的 signature（`lake/catalog.py:49`-`lake/catalog.py:51`）；因此旧 token 只能确认旧请求。内容变化必须新 token；head 变化不改变旧 receipt 的意义，也不能让旧 receipt 回退 head。

---

## 7. A—F 固定时序走通

记号：`X1={seq1 A=10, seq2 B=20}`（day1 一段），`X2={seq4 B delete}`（day1），`X3={seq3 A=12}`（day2），`X4={seq5 C=7, seq6 A=15}`（day2）。`O(S)` 表示对 S 的 f(S) 输出。

### A. `seq1..2` → 维护准备 → `seq3..4` → 维护发布（最终 cp=4，只有 A=12）

| 步骤 | 动作 | version | checkpoint | manifest / 段数 | 读取结果 |
|---|---|---|---|---|---|
| A0 | 初始 | 0 | 0 | `[]` / 0 | — |
| A1 | ingest `X1` | 1 | 2 | `[X1]` / 1 | A=10, B=20 |
| A2 | 维护 plan（base v1，selected `{X1}`，写 `O1=X1`） | 1 | 2 | 不变 | — |
| A3 | ingest `seq3,4` 追加 `X2,X3` | 2 | 4 | `[X1,X2,X3]` / 3 | A=12（B delete 胜出） |
| A4 | 维护用 v1 发布：base 不是 head → `HEAD_CONFLICT` | 2 | 4 | 不变 | — |
| A5 | 重读元数据：current 前缀含 `X1`、selected 存活、追加来自 checkpoint 2→4；拼 `target=[O1,X2,X3]`，以 base=2 发布 | 3 | 4 | `[O1,X2,X3]` / 3 | A=12 |

A 的复用条件：只读 current 的 manifest/checkpoint/version；选中 `X1` 仍存在；追加段 `X2,X3` 从 checkpoint 2 之后合法追加；已写输出 `O1` 直接拼接。**没有重新 `get`/归并 `X1`**。这里 `O1` 内容恰好等于 `X1`（同 ID），但协议执行的是“复用已写输出”的路径，而非重算路径。最终 checkpoint=4，只有 A=12。

### B. ingest 持旧清单；维护先发布；该批 ingest 再提交（最终 cp=4，A=12）

| 步骤 | 动作 | version | checkpoint | manifest / 段数 | 读取结果 / 结果 |
|---|---|---|---|---|---|
| B1 | ingest `X1` | 1 | 2 | `[X1]` / 1 | A=10, B=20 |
| B2 | 下一批 ingest prepare：base v1，写 `X2,X3`，`PreparedBatch=[X1,X2,X3] cp4`，不提交 | 1 | 2 | 未发布 | — |
| B3 | 维护 exact base v1 发布 `O1` | 2 | 2 | `[O1]` / 1 | A=10, B=20 |
| B4 | ingest commit 旧 `base=1` → `HEAD_CONFLICT`（`lake/catalog.py:63`-`lake/catalog.py:64`） | 2 | 2 | 不变 | 失败，无 snapshot/receipt |
| B5 | ingest 恢复：读 current v2 `[O1]`，校验 `seq3,4` 仍从 checkpoint 2+1 连续；用 `current.segments + prepared.added = [O1,X2,X3]`，新 token 发布 | 3 | 4 | `[O1,X2,X3]` / 3 | A=12 |

**关键**：B5 不能用旧 `prepared.manifest=[X1,X2,X3]` 作为新 target；那会重新引入维护已替换的 `X1`（若 `X1` 中有被回收的墓碑，则违反回收/复用决定）。应使用 current manifest 作为左半部分。checkpoint 从 2 前进到 4 仍由 ingest 完成，维护没有碰 checkpoint。最终 cp=4，A=12。

### C. 两个维护 worker 选择重叠输入，worker 1 先发布

| 步骤 | 动作 | version | checkpoint | manifest | 说明 |
|---|---|---|---|---|---|
| C1 | ingest `X1`，再 `X2,X3` | 2 | 4 | `[X1,X2,X3]` | A=12 |
| C2 | worker 1（base v2，selected 全部 3）写 `O123=f(all)` | 2 | 4 | 准备中 | 输出按 ID 复用 |
| C3 | worker 2（base v2，selected 最早 `X1`）写 `O1` | 2 | 4 | 准备中 | job ID 不同 |
| C4 | worker 1 发布 | 3 | 4 | `[X2,X3]` | `X1,X2,X3` 被 `O123={X2,X3}` 替换 |
| C5 | worker 2 以 base v2 尝试：`X1` 已不在 current `[X2,X3]` → `INPUT_REPLACED` | 3 | 4 | 不变 | 不能拼接 `O1` |
| C6 | worker 2 重新排队/按 current 重规划（新 attempt/token），输出 `O={X2,X3}`（或确认已满足后不写） | 3 或 4 | 4 | 若发布则为 `[X2,X3]` | 绝不重新引入 `X1` |

C5 的判定依据是段 provenance，不是 job ID：即使两个 job 逻辑名不同，重叠段已被替换就必须拒绝。若错误地“current 去掉 X1 再加 O1”，会把 `[X2,X3,X1]` 写回，重新引入更老版本（C 中 `X1` 的 seq1/2 会在某些键上复活旧状态；在回收场景还可能破坏墓碑语义），因此不允许。

### D. projection 两段的两种回收申请

fixture：`U={seq1 B upsert 20}`，`D={seq2 B delete}`，base v1=`[U,D]`，checkpoint=2；读取结果为 `[]`。

| 申请 | 判定 | 结果 |
|---|---|---|
| 仅选 `D`，`reclaim=true` | `set(selected) != set(base.segments)` → `TOMBSTONE_RECLAIM_REQUIRES_FULL_COVERAGE`，在读取载荷前拒绝 | 不发布，无 B=20 |
| 仅选 `D`，`reclaim=false`（备选合法路径） | 局部保留 f(`{D}`) 的 delete（必要时墓碑）；拼回 `[U,D]` | rows `[]`，无 B=20（delete 压制 U） |
| 选全部 `U,D`，`reclaim=true` | 全覆盖；f(base) 的 B 最大事件是 delete，可丢弃；额外检查 base 前缀（此时 current=base） | outputs 为空，发布空 manifest；rows `[]`，无 B=20 |

第二种（仅 delete + reclaim）之所以拒绝：不扫描 `U` 就无法知道 `U` 中是否有 seq=1 的 upsert；若丢弃 delete，则 `U` 的 B=20 会复活（现有 projection 的 `projected_manifest` 正是这种危险结果的证据）。全覆盖时所有 base 事件都在选中集合内，不存在可被压制的老事件，才可丢弃。

**全覆盖计划形成后又发生合法源追加时，需重新核验**：
1. current manifest 仍以 base manifest 为精确前缀（base 全部段都还活着）；
2. current checkpoint 因 ingest 前进；尾段事件源自 seq 大于 base checkpoint 的合法追加（尾段若又被其他维护局部压实，f 保持等价；若被全覆盖回收则必然替换某个 base 段，前缀检查失败）；维护没有推进 checkpoint；
3. selected/base 覆盖关系仍成立（若被其他维护替换则重新规划）；
4. target 应拼成 `g(base) + 追加段`，checkpoint 复制 current；
5. 追加段若包含新 upsert（seq 更大），出现新值是**源的新语义**，不是复活；若包含新 delete，仍为空。任何 seq ≤ base checkpoint 的 outside 事件都会使证明失效，必须重新规划。

### E. 崩溃与回执

| 场景 | 已发生 | 正确恢复 | 不得发生 |
|---|---|---|---|
| 输出落盘、未发布崩溃 | SegmentStore 对象存在，Catalog head/snapshot/receipt 未变 | 计划日志存在 → 复用 outputs，`receipt(token)` 无命中 → 重做 4.2/4.3 后发布；日志不存在 → 重算输出（内容寻址同 ID） | 不能声称已发布；不能仅凭孤儿对象推进 head |
| 发布成功、响应丢失 | snapshot/head/receipt 已原子提交（`lake/catalog.py:68`-`lake/catalog.py:75`） | `receipt(token)` 返回 receipt.version；或重放完全相同的旧请求 → receipt 分支（`lake/catalog.py:54`-`lake/catalog.py:60`） | 不能因为看不见响应就重复写新 snapshot |
| 响应丢失后 head 又前进 | 旧 receipt.version=r，head=r+1/更高 | 旧回执确认 r；当前 head 用 `catalog.snapshot()` 另行读取；若要发布到新 head，使用新 attempt/token 重建 target | 不能用旧 receipt 的 version 执行 `UPDATE head`；代码的 receipt 分支没有 head 写（`lake/catalog.py:60` vs `lake/catalog.py:70`） |
| 恢复时请求内容变化 | 同 token，signature 不同（`lake/catalog.py:49`-`lake/catalog.py:51`） | `TOKEN_REUSE`；新 attempt/token 重新记录并发布 | 不能让旧 token 静默绑定新内容 |

旧回执“不能回退 head”在此协议中是结构性保证：`publish` 的 receipt 分支只 `result = snapshot(receipt[1])` 并 `COMMIT`；只有无 receipt 的 else 分支才有 `UPDATE head`（`lake/catalog.py:70`-`lake/catalog.py:71`）。新增的 `receipt(token)` 同样是只读。

### F. 固定 version=1 的历史读者

F1：`seq1..2` 提交后产生 `version=1`；读者在后续 ingest/压实之前 `snapshot(version=1)`（`lake/catalog.py:35`-`lake/catalog.py:44`），固定 `segments=(X1,)`。
F2：之后所有 ingest/维护只新增 snapshot 行和 SegmentStore 对象；没有物理删除或 vacuum，`SegmentStore.put` 只 `INSERT OR IGNORE`（`lake/segments.py:17`）。
F3：再次 `read_rows(store, pinned_v1)` 仍取 `X1` 的 `seq1 A=10`、`seq2 B=20`，输出 `A/2026-10-01/10`、`B/2026-10-01/20`；与当前 head 的 A/C 行无关。基线 `pinned_v1` 已证明这一点（`lake/evidence.py:19`-`lake/evidence.py:28`）。

---

## 8. 正确性论证

**引理 1（局部压实的等价性）**：设 `O=f(S)`，`T` 为任意其他事件集合，则对每个 key，`max_seq(O ∪ T)` 与 `max_seq(S ∪ T)` 相同。证明：若 S 在 key 上无事件，O 无事件；否则 O 保留 S 内最大 seq 事件 m，S 中其余事件的 seq 都小于 m，不会改变并集最大值；T 的事件无论新旧，取 max 后与保留 m 的结果一致。因此局部压实保留必要 delete 是安全的，且不需要扫描 T。对应 `winners` 的按 key 取最大 seq（`lake/segments.py:48`-`lake/segments.py:49`）。

**引理 2（全覆盖回收在合法追加下成立）**：`S=base.segments`，`g(S)` 丢弃最大事件为 delete 的 key。对每个 key，base 内所有事件都在 S；丢弃 delete 后 base 内无该 key 事件。对 plan 后合法追加 `T`，所有 T 事件的 seq > base checkpoint ≥ S 内最大 seq，故 `max(T)` 严格大于被丢弃 delete；原始 `f(S)∪T` 与 `g(S)∪T` 都由 `max(T)` 决定（或为空）。若存在 seq ≤ base checkpoint 的 outside 事件，则可能由 g(S) 复活老 upsert，因此必须重新规划，不能回收。证据：追加 seq 的连续性由 `lake/cdc.py:16`-`lake/cdc.py:19` 校验，checkpoint=最后 seq 由 `lake/cdc.py:27`-`lake/cdc.py:28` 记录；维护只复制 current checkpoint。

**引理 3（ingest 追加）**：新事件 seq 连续且大于 current checkpoint；对未被新事件触及的 key，旧状态不变；被触及的 key，新事件 seq 更大，取最大后即为新 upsert/delete。`read_rows` 只在最后过滤 delete（`lake/segments.py:54`-`lake/segments.py:57`），所以不会按 day 拆分身份。

**引理 4（ingest rebase 不重引入旧段）**：B 中旧 `prepared.manifest` 的前半部分是旧 base segments。维护可能已替换它们（甚至回收墓碑）。rebase 只用 `current.segments + prepared.added`，保留 current 的最新语义与 ingest 自己新增的事件，因此不会把维护已删除的旧版本写回。新增 added 段已在 prepare 时写为内容寻址对象（`lake/cdc.py:27`），不需要重读。

**守恒结论**：A/B/C 的每一条成功发布都满足 I1—I7；D 的回收只在全覆盖证明下发生；E 的回执路径不写 head；F 的历史 snapshot/对象不变。

---

## 9. 重试边界与重新排队条件

- **单个 publish 调用**：一次 `BEGIN IMMEDIATE` 内最多一次 CAS 尝试。`HEAD_CONFLICT` 表示 base version 已过期（`lake/catalog.py:63`-`lake/catalog.py:64`）；调用方可以重读元数据并**重新拼接 target**，再试一次。
- **元数据重试次数**：每个维护 attempt 最多对 head 的连续变化重试 `MAX_METADATA_RETRIES`（建议 3）次；每次失败都重新读 current、按 4.2 重新判定。超过则抛 `RETRY_EXHAUSTED`，由调度方以同一 `job_id`、新 `attempt` 重新排队。题目不要求对抗无限冲突时有界完成，但不允许无限忙等或无限重读载荷。
- **必须重新排队（不能在同一 attempt 内继续）**：
  - `INPUT_REPLACED`：选中段已被替换，必须重新选段/重规划（新 attempt/token）；
  - `BASE_NOT_APPEND_ONLY`（回收场景）：base 前缀被破坏，必须按 current 重新做全覆盖计划；
  - `TOKEN_REUSE`：同 token 的请求内容改变，必须新 attempt/token；
  - 计划输出与当前 selected 的 provenance 不再对应（例如崩溃后重新选段）。
- **ingest 重试**：`HEAD_CONFLICT` 后若 `prepared.checkpoint > current.checkpoint` 且事件 seq 仍为 `current.checkpoint+1 .. prepared.checkpoint`，用 current manifest + `prepared.added` 重建，新 token 重试；若 `prepared.checkpoint <= current.checkpoint` 且无本请求 receipt，则 `SOURCE_ALREADY_CONSUMED_OR_REPLAY`，不得重复消费源前缀。单逻辑 ingest writer 仍由 `lake/cdc.py:16`-`lake/cdc.py:19` 保证连续。
- **恢复优先查 receipt**：任何重启后的 attempt 先 `receipt(token)`；命中即确认原请求，不再计算 target。未命中才重新判定/发布。这样“响应丢失后 head 又前进”不会把旧回执当成新 head，也不会无谓重算。

---

## 10. 语句级伪代码

### 10.1 计划阶段（只读 selected 载荷）

```python
def plan_subset(catalog, store, base_version, selected, reclaim,
                job_id, attempt, token):
    # 历史版本可读: catalog.py:35-44
    base = catalog.snapshot(base_version)
    if len(selected) == 0:
        raise ValueError("EMPTY_SELECTION")
    if len(set(selected)) != len(selected):
        raise ValueError("DUPLICATE_SELECTED")
    if not set(selected) <= set(base.segments):
        raise Conflict("SELECTED_NOT_IN_BASE")
    if reclaim and set(selected) != set(base.segments):
        # 不扫描未选段就无法证明安全: 在读取载荷之前拒绝
        raise ValueError("TOMBSTONE_RECLAIM_REQUIRES_FULL_COVERAGE")

    # 只读 selected 的载荷; 绝不 winners(current.segments)
    latest = winners(store, tuple(selected))       # segments.py:40-51
    kept = latest
    if reclaim:                                    # 全覆盖语义
        kept = [e for e in latest if e["op"] != "delete"]
    outputs = write_partitioned(store, kept)       # segments.py:32-37

    plan = Plan(job_id, attempt, token, base_version,
                tuple(base.segments), tuple(selected),
                tuple(outputs), bool(reclaim))
    # 对象已写完; 单独事务记录恢复日志
    # BEGIN IMMEDIATE; INSERT OR IGNORE ...
    catalog.record_plan(plan)
    return plan
```

`Plan` 日志字段：`job_id, attempt, token, base_version, selected, outputs, reclaim`。`base_segments` 可由历史 snapshot 重取，不写入日志。

### 10.2 发布阶段（复用输出；CAS；不读载荷）

```python
def publish_plan(catalog, plan, max_metadata_retries=3):
    retries = 0
    while True:
        current = catalog.snapshot()               # head 的 version/manifest/checkpoint
        # --- 4.2 输入存活 ---
        for sid in plan.selected:
            if current.segments.count(sid) != 1:
                raise Conflict("INPUT_REPLACED")

        # --- 4.2 回收的追加证明 ---
        if plan.reclaim:
            base = catalog.snapshot(plan.base_version)
            if set(plan.selected) != set(base.segments):
                raise ValueError("TOMBSTONE_RECLAIM_REQUIRES_FULL_COVERAGE")
            if current.segments[:len(base.segments)] != base.segments:
                raise Conflict("BASE_NOT_APPEND_ONLY")
            if len(current.segments) > len(base.segments) \
                    and current.checkpoint <= base.checkpoint:
                raise Conflict("BASE_NOT_APPEND_ONLY")

        # --- 4.3 按 ID 拼接; 不 store.get, 不 winners ---
        positions = [i for i, sid in enumerate(current.segments)
                     if sid in plan.selected]
        insert_at = min(positions)                 # 存活检查保证非空
        kept = [sid for sid in current.segments if sid not in plan.selected]
        extra = [o for o in plan.outputs if o not in kept]
        target = tuple(kept[:insert_at] + extra + kept[insert_at:])
        if len(set(target)) != len(target):
            raise ValueError("DUPLICATE_SEGMENT")

        checkpoint = current.checkpoint            # 维护只复制, 不推进/回退
        try:
            # 现有 publish 的 BEGIN IMMEDIATE + CAS + receipt:
            # catalog.py:46-79
            return catalog.publish(current.version, target, checkpoint,
                                   plan.token, checkpoint_mode="maintenance")
        except Conflict as exc:
            if str(exc) != "HEAD_CONFLICT":
                raise
            retries += 1
            if retries > max_metadata_retries:
                raise Conflict("RETRY_EXHAUSTED")
            # 循环: 只重读元数据, 不重读载荷
```

### 10.3 Catalog 扩展（保持现有 receipt 语义）

```python
def publish(self, base, segments, checkpoint, token,
            checkpoint_mode="ingest"):
    if len(set(segments)) != len(segments):
        raise ValueError("DUPLICATE_SEGMENT")          # catalog.py:47-48
    signature = sha256(json([base, list(segments), checkpoint]))  # 49-51
    self.db.execute("BEGIN IMMEDIATE")                  # 52
    try:
        receipt = SELECT signature, version FROM receipts WHERE token=?  # 54-56
        if receipt:
            if receipt.signature != signature:
                raise ValueError("TOKEN_REUSE")         # 58-59
            result = self.snapshot(receipt.version)     # 60; 不 UPDATE head
        else:
            current = self.snapshot()                   # 62
            if current.version != base:
                raise Conflict("HEAD_CONFLICT")         # 63-64
            if checkpoint < current.checkpoint:
                raise ValueError("CHECKPOINT_REGRESSION") # 65-66
            if checkpoint_mode == "maintenance" and checkpoint != current.checkpoint:
                raise ValueError("CHECKPOINT_NOT_OWNED")  # 新增前置条件
            version = current.version + 1               # 67
            INSERT INTO snapshots VALUES(version, segments, checkpoint)  # 68-69
            UPDATE head SET version=version WHERE singleton=1            # 70-71
            INSERT INTO receipts VALUES(token, signature, version)       # 72-73
            result = self.snapshot(version)             # 74
        self.db.execute("COMMIT")                       # 75
        return result
    except BaseException:
        self.db.execute("ROLLBACK")                     # 77-79
        raise


def receipt(self, token):
    # 新增只读确认; 不写 head
    row = self.db.execute(
        "SELECT version FROM receipts WHERE token=?", (token,)).fetchone()
    return None if row is None else self.snapshot(row[0])
```

### 10.4 ingest 旧清单 rebase

```python
def commit_prepared(catalog, prepared, events):
    try:
        return catalog.publish(prepared.base, prepared.manifest,
                               prepared.checkpoint, prepared.token)
    except Conflict:
        while True:
            current = catalog.snapshot()
            if prepared.checkpoint <= current.checkpoint:
                raise ValueError("SOURCE_ALREADY_CONSUMED_OR_REPLAY")
            expected = list(range(current.checkpoint + 1,
                                  prepared.checkpoint + 1))
            if [e["seq"] for e in events] != expected:
                raise ValueError("NONCONTIGUOUS_SOURCE")
            # 关键: 用 current, 而不是旧 prepared.manifest
            target = current.segments + prepared.added
            token = derived_token(prepared.batch_id, current.version)
            try:
                return catalog.publish(current.version, target,
                                       prepared.checkpoint, token)
            except Conflict as exc:
                if str(exc) != "HEAD_CONFLICT":
                    raise
                # 有界元数据重试后交回调度方
```

`prepared.added` 是从 `write_partitioned` 得到的、已经写好的新增段；建议在 `PreparedBatch` 中新增该字段（`lake/cdc.py:5`-`lake/cdc.py:10`）。

---

## 11. 后续开发步骤（按现有文件拆分）与验证计划

1. **`lake/catalog.py`**
   - 增加 `maintenance_plans` 表（4.5 的 SQL）与 `record_plan/load_plan`；独立事务，不动 head/checkpoint。
   - 增加只读 `receipt(token)`；保持 `publish` 的 `BEGIN IMMEDIATE`、CAS、receipt 三分支结构（`lake/catalog.py:46`-`lake/catalog.py:79`）。
   - 给 `publish` 增加可选 `checkpoint_mode`；维护模式在无 receipt 分支强制 `checkpoint == current.checkpoint`（紧接 `lake/catalog.py:65`-`lake/catalog.py:66` 之后）。
   - 为 `Checkpoint`/role 错误补测试；不改变七项基线测试期望。
2. **`lake/compact.py`**
   - 新增 `Plan` 字段（`job_id/attempt/token/base_version/selected/outputs/reclaim`），保留现有离线 `plan`（`lake/compact.py:14`-`lake/compact.py:18`）以兼容基线。
   - 新增 `plan_subset`：只 `winners(store, selected)`，回收时过滤 delete；在调用载荷读取前校验全覆盖。
   - 新增 `publish_plan`：4.2 存活/前缀判定 + 4.3 splice + 以 `current.version` 调用现有 `publish`；不读未选载荷。
3. **`lake/cdc.py`**
   - `PreparedBatch` 增加 `added` 与 `batch_id`（`lake/cdc.py:5`-`lake/cdc.py:10`、`lake/cdc.py:27`-`lake/cdc.py:28`）。
   - 新增 `commit_prepared`/rebase 路径：`HEAD_CONFLICT` 时用 `current.segments + prepared.added`，新 token；`SOURCE_ALREADY_CONSUMED_OR_REPLAY` 防止重复消费。
   - 保留 `prepare` 的连续 seq/字段校验（`lake/cdc.py:13`-`lake/cdc.py:28`）。
4. **`lake/segments.py`**
   - 复用现有 `winners`/`write_partitioned`/`put`/`get`；如需把“过滤 delete”做成显式 `compact_events(events, reclaim)`（纯函数），可放在此处，但读取语义不变（`lake/segments.py:40`-`lake/segments.py:57`）。
5. **`lake/evidence.py` / `lake/review.py`**
   - 基线四场景保持不变；未来可新增独立协议 harness（A—F、崩溃窗口、双 worker），但不修改现有测试期望。
   - `review` 的 verify 已改为 `python3 -m lake.review`，demo 保持 `python3 -m lake.evidence all`。
6. **验证计划**
   - `python3 -m lake.review`：七项基线断言 + 证据可复现 + 文档/decision 引用结构检查。
   - `python3 -m lake.evidence all`：实际基线四种轨迹。
   - 未来协议测试至少覆盖：A 的元数据复用（断言未调用 `store.get` 于未选段）、B 的旧清单 rebase、C 的重叠拒绝、D 两种回收判定、E 四个崩溃窗口、F 固定 version=1。
   - **结构检查通过不等于协议正确**：A—F 的安全判定必须依据第 4、8、9 节的证明与第 7 节的状态转换逐条评审；基线证据仅证明讨论所依据的现状。

---

## 12. 五项 decision 的源码定位（与 `docs/review-evidence.json` 对应）

- **publication**：`lake/catalog.py:52`、`lake/catalog.py:63`、`lake/catalog.py:67`、`lake/catalog.py:70`、`lake/compact.py:17`、`lake/cdc.py:28`。
- **checkpoint**：`lake/catalog.py:65`、`lake/catalog.py:66`、`lake/catalog.py:70`、`lake/cdc.py:17`、`lake/cdc.py:28`。
- **tombstones**：`lake/segments.py:48`、`lake/segments.py:49`、`lake/segments.py:55`、`lake/segments.py:57`、`lake/compact.py:17`。
- **retry-receipt**：`lake/catalog.py:49`、`lake/catalog.py:51`、`lake/catalog.py:55`、`lake/catalog.py:58`、`lake/catalog.py:59`、`lake/catalog.py:60`、`lake/catalog.py:75`。
- **overlap**：`lake/catalog.py:47`、`lake/catalog.py:63`、`lake/catalog.py:70`、`lake/cdc.py:28`、`lake/compact.py:22`。

## 13. 基线证据说明

`docs/review-evidence.json` 的 `baseline` 直接取实际运行 `python3 -m lake.evidence all` 的结构化结果（含 `regression`、`interleavings`、`receipt`、`projection`）。它只证明现有调用链、CAS 拒绝、回执精确绑定、projection 全局 key/delete 语义等讨论前提；未来在线协议的 A—F 正确性不由该基线模拟，也不以 `lake.review` 的结构检查代替。
