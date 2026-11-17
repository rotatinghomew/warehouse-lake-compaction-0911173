# 按日期追加观看事件的本地列式湖

这是一个处理本地数据交换的仓湖项目。示例使用虚构机构和匿名合成记录，不连接业务平台。

## 数据契约

观看事件 event_id 全表唯一，每个上传独立写不可变 Parquet 文件。快照显式固定上传文件集合，同一文件集合在同一有效期口径下复用快照。查询可以固定快照并按日期过滤，导出使用该固定集合。目录中未被清单引用的文件不参与查询。

## 在线整理与空间回收

写入不暂停、读者不中断的前提下，库支持把同一业务日期的小 Parquet 整理成较大的文件，并在确认没有任何表版本引用后回收物理空间。

- `viewsegments/compact.py` 的 `compact(lake)`：按业务日期分组整理。整理是 `planning → writing → ready → published` 的可恢复状态机，输出文件在写入前先登记到 `physical_files`。切换上传清单与发布新快照在同一个 SQLite 事务内完成，并以事件多重集校验保证业务记录不增不减。
- 全局整理锁与 fencing 令牌：两个整理者同时碰到同一批文件时只有一个生效；持锁者崩溃后，锁超过心跳有效期可被接管，旧令牌随后的任何写入或切换都会被拒绝。重试同一批输入会复用已发布任务，不会产生第二份数据。
- `reclaim_space(lake)`：只根据真实的引用关系决定删除。可删文件 = 已登记但不在「当前上传清单 ∪ 未到期快照 ∪ 未完成整理输出」中的文件；删除前再核对一次内容 SHA256。磁盘上未登记的 Parquet 只在结果中报告，绝不按修改时间删目录。

## 快照有效期

每个快照带调用者可观察的 `expires_at`（默认 24 小时）。`renew(snapshot, ttl=...)` 从当前时刻续期；有效期内即使物理布局已被整理改变，导出仍能读完固定集合。到期读取抛出 `SnapshotExpired`，引用不存在抛出 `SnapshotMissing`，二者都与「查询结果为空」（正常返回 `[]`）严格区分。到期判断只看时钟，不把运行进程是否存在当作占用依据；到期后其文件才可能被回收，空间不会被无限拖住。

## 代码职责

- `viewsegments/append.py`：观看事件规范化与批次落盘。
- `viewsegments/catalog.py`：上传键、物理文件登记、带有效期的快照集合与引用核算。
- `viewsegments/compact.py`：在线整理状态机、整理锁与空间回收。
- `viewsegments/query.py`：日期查询、时长汇总与导出。
- `viewsegments/storage.py`：本地事务、内容摘要和查询工具。
- `viewsegments/clock.py`：统一可注入时钟，有效期判断的唯一时间来源。
- `viewsegments/errors.py`：到期、缺失、整理冲突等可区分错误。

## 安装与运行

使用 Python 3.11（兼容该系列补丁版本，独立容器镜像以 SHA256 摘要固定）。构建工具和运行依赖均按精确版本登记。先执行 `python3 -m pip install -r requirements-build.txt -r requirements.txt`；依赖仅在准备环境时安装，运行与测试均可断网。

```sh
python3 -m compileall -q viewsegments tests
python3 -m build --wheel --no-isolation
python3 -m unittest discover -s tests -v
python3 -m viewsegments
```

默认示例在独立临时目录执行，结束后删除目录。需要检查持久文件时可执行 `python3 -m viewsegments --root state`。测试自己创建临时目录，不依赖固定端口、外部服务或其他用例顺序。

## 容器

```sh
docker build -t lake-compaction:local .
docker run --rm --network none lake-compaction:local
docker run --rm --network none lake-compaction:local python3 -m viewsegments
```

容器构建负责预装锁定依赖，启动不下载包。示例输出是真实接入、查询、整理、续期、到期和回收的数据，不读取预制输出。测试及示例可分别运行，退出码非零表示失败。

## 交换口径

进一步的字段含义、范围及兼容边界见 `docs/data-contract.md`。
