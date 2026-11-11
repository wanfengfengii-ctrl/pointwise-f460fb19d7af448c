二、真实业务语义与输入输出
输入 data/cdc.json 为 JSON 数组。每条记录含 seq、key、op、day；upsert 另含整数 amount，delete 不含 amount。seq 是单个逻辑源的全局唯一、连续递增位点。一个逻辑 ingest writer 按顺序消费，允许进程重启。key 是全局主键，day 只是物理路由分区，不能作为主键的一部分；更新或删除可以落入与旧值不同的 day。

读者固定一个 Snapshot，再从其全部数据段中按 key 选择最大 seq，最后过滤 delete。read_rows 输出按 key 排序的 key/day/amount 数组。固定六条输入处理完成后，结果必须为 A/2026-10-02/15 与 C/2026-10-02/7，checkpoint=6。当前离线压实将 version 从 3 变为 4，数据段数从 4 变为 2，逻辑结果和 checkpoint 不变；固定的 version=1 仍读到 A=10、B=20。

可直接运行的命令：
- python3 -m lake.review --baseline：执行业务断言并输出证据 JSON。
- python3 -m lake.evidence all：输出全部固定场景。
- python3 -m lake.evidence interleavings：展示 ingest 与压实双方旧计划的拒绝及重新计算。
- python3 -m lake.evidence receipt：展示成功请求重试与改变请求后复用 token 的区别。
- python3 -m lake.evidence projection：展示给定清单下的全局删除语义。

三、本轮需要讨论的目标
我们准备允许 ingest 持续运行，同时由最多两个维护 worker 对选定的完整数据段做在线压实。请基于这个仓库提出一个可以直接指导后续开发的最小完整协议。

维护调度方提供 base snapshot version、selected segment IDs、是否申请回收墓碑以及逻辑 job ID；不要求设计调度器或自动挑选分区。普通压实只能读取选中数据段的载荷，可以读取清单和必要元数据，不能为了每次局部压实扫描未选数据段。允许一次明确申请、选中整个基准快照的全覆盖压实。新方案必须同时支持：局部压实安全保留必要删除语义，以及在能证明安全的全覆盖情形下回收墓碑；永久保留所有墓碑不满足目标。

持续写入导致 head 前进时，如果变化仅来自合法源追加且选中输入仍然有效，必须能复用已经写好的压实输出，通过必要的元数据检查完成重新发布；不能每次都重新读取、归并全部选中载荷。对于输入已被其他维护操作替换的情况，可以明确拒绝或重新规划。无需保证对抗无限冲突时仍有有界完成时间，但必须定义重试边界和重新排队条件。

需要共同满足的约束：
1. 任意成功发布前后，当前快照的业务结果等价于已提交源前缀的按 key 最新状态，不丢新增、不复活删除、不把 key 按 day 拆成多个身份。
2. 维护提交不能消费源数据、推进或回退源 checkpoint。head、清单、checkpoint 与发布回执之间的原子关系必须写清。ingest 也可能拿着压实前准备的旧清单，方案不能只修改维护侧而忽略该调用链。
3. 两个维护 worker 可以选择重叠输入。说明何时能复用输出，何时必须拒绝，不能仅因逻辑 job ID 不同就认为互不冲突。
4. 数据段写入和 Catalog 提交之间没有分布式事务。考虑写完输出后崩溃、发布成功但响应丢失、响应丢失后 head 又前进，以及恢复时请求内容发生变化。现有 receipt 的精确请求绑定语义必须保留或给出明确兼容方案。
5. 历史快照永久保留，本题不做物理对象删除、vacuum 或读者租约。新方案不能破坏旧快照，也不能借物理删除解决逻辑墓碑问题。
6. 可以提议增加最少的持久元数据、Catalog 接口和事务条件，但应继续使用当前两个适配器的职责边界。不要引入外部消息系统、服务账号或更换整套湖表框架；不讨论 schema 演进、多源排序、权限平台或监控平台。

四、必须在报告中逐项走通的固定时序
A. ingest 提交 seq=1..2；维护准备计划；ingest 提交 seq=3..4；维护尝试发布。说明已有输出如何在条件成立时复用。最终 checkpoint=4，当前只剩 A=12。
B. ingest 提交 seq=1..2；下一批 ingest 已准备但未提交；维护先成功发布；该批 ingest 再提交。说明失败尝试、恢复及最终成功时清单和 checkpoint 的变化，最终仍为 checkpoint=4、A=12。
C. 在提交 seq=1..4 后，worker 1 选全部三个输入段，worker 2 选其中最早的一个输入段；二者均已准备输出，worker 1 先发布。说明 worker 2 的处理，不能重新引入已被替换的旧版本。
D. 使用 projection 的两个数据段：仅选择删除段时申请回收墓碑，以及选择全部两个段时申请回收墓碑。解释两种申请的判定依据；正确结果都不能出现 B=20。说明全覆盖计划形成后又发生合法源追加时，需要重新核验什么。
E. 分别讨论输出已落盘但尚未发布就崩溃、发布成功但响应丢失、响应丢失后 head 又前进。用 receipt 场景说明如何区分确认原请求与提交变化后的请求，不能将历史回执对应的版本重新设为 head。
F. 一个读者在上述过程前固定 version=1，之后仍须读到 A=10、B=20。

五、交付格式
只创建 docs/online-compaction.md、docs/review-evidence.json，并更新 .pointwise-runtime.json；不要修改 lake 下的实现、既有测试或样例数据。

方案文档应包含：现有调用链及事务边界；至少两种候选发布策略的比较和最终选择；计划与恢复状态所需字段；关键接口的输入、输出、错误和事务前置条件；可执行到语句级别的伪代码或步骤；上述 A—F 的状态转换表；墓碑回收的充分条件和不能证明时的处理；按现有文件拆分的后续开发步骤及验证计划。所有关键判断引用当前源码的 文件:行号，不能只引用 README。讨论替代方案时应明确它在哪个具体时序下不满足约束。

review-evidence.json 格式为：顶层 baseline 必须等于 python3 -m lake.evidence all 的实际 JSON 输出；decisions 为五个对象，id 分别是 publication、checkpoint、tombstones、retry-receipt、overlap，每项包含非空 choice 字符串和非空 code 数组，code 元素为 lake/catalog.py:46 这样的真实源码定位，且须在报告中出现。choice 写本次选择及依据，不要填状态口号。基线证据用于证明讨论依据，不要求实现未来协议或为未来协议开发模拟引擎。

保留运行配置 version=1、kind=cli、setup=[]；最终 verify 改为 ["python3","-m","lake.review"]，demo 保留 ["python3","-m","lake.evidence","all"]。verify 会重跑业务断言、比对证据并检查源码引用；协议正确性还按上述固定时序评审，不能将结构检查通过等同于设计正确。所有命令须可在 macOS 使用 Python3 直接运行，不要求截图或录像。