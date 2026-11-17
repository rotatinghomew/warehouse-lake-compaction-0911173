# 按日期追加观看事件的本地列式湖

这是一个处理本地数据交换的仓湖项目。示例使用虚构机构和匿名合成记录，不连接业务平台。

## 数据契约

观看事件event_id全表唯一，每个上传独立写不可变Parquet文件。快照显式固定上传文件集合，同一文件集合复用快照。查询可以固定快照并按日期过滤，导出使用该固定集合。目录中未被清单引用的文件不参与查询；当前产品不删除数据文件。

## 代码职责

- `viewsegments/append.py`：观看事件规范化与批次落盘。
- `viewsegments/catalog.py`：上传键、文件引用和快照集合。
- `viewsegments/query.py`：日期查询、时长汇总与导出。
- `viewsegments/storage.py`：本地事务、内容摘要和查询工具。

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

容器构建负责预装锁定依赖，启动不下载包。示例输出是真实接入、查询和发布的数据，不读取预制输出。测试及示例可分别运行，退出码非零表示失败。

## 交换口径

进一步的字段含义、范围及兼容边界见 `docs/data-contract.md`。
