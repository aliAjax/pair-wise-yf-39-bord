# 野生动物疫病监测与离线同步

这是一个只使用Python标准库和SQLite的模块化项目，默认端口为`8305`。所有业务规则集中在`src/rules.py`，`app.py`只负责组装依赖和启动服务。

## 模块结构

- `app.py`：命令行参数、依赖组装、启动和信号处理。
- `src/domain.py`：角色、数据结构、领域异常和基础校验。
- `src/rules.py`：状态机、权限、领域计算、冲突和跨对象校验。
- `src/repository.py`：SQLite建表、查询、事务和乐观锁。
- `src/service.py`：用例编排、幂等处理、版本控制和审计写入。
- `src/http_api.py`：HTTP路由、请求解析和统一错误响应。
- `src/audit.py`：实体操作审计时间线。
- `static/index.html`：最小演示页面。
- `tests/`：完整流程、规则和失败场景测试。

## 初始化与启动

```bash
python3 app.py --db ./data.db --port 8305
```

服务启动时会自动建表。`--host`可修改监听地址，`--db`可指定其他SQLite文件。

## 核心对象

- `observation`：现场观察；`sample`：样本与实验室结果；`cluster`：异常聚集事件。

## 主要接口

- `GET /health`：健康检查。
- `GET /api/<kind>`：按对象类型查询，可用`?status=`过滤。
- `POST /api/<kind>`：创建对象；请求体为JSON。
- `GET /api/entities/<id>`：读取对象当前版本。
- `POST /api/entities/<id>/actions`：提交`{"action":"动作名","data":{...},"expected_version":数字}`。
- `GET /api/audit`：读取审计记录。
- `POST /api/sync`：离线批次回网合并，请求体为
  `{"batch_id":"批次号","device_id":"设备号","changes":[{"seq":序号,"op":"create|update|transition","kind":"observation|sample|cluster","client_id":"端临时ID","entity_id":"服务端ID","baseline_version":基线版本,"data":{...}}]}`。
  每条改动携带设备号、批次内序号和基线版本；同一批次重传只返回第一次结果，不重复应用。
- `GET /api/adjudications?status=pending|decided`：读取待站里裁定的冲突。
- `POST /api/adjudications/<id>/decide`：站里对观察的物种和地点裁定，请求体为
  `{"candidate_device":"设备号"}` 或 `{"species":"...","location":"..."}`。

请求身份通过`X-User-Id`和`X-Role`请求头传入。创建和动作的可执行角色由规则引擎控制。

## 离线合并与冲突裁定

- 断网期间各设备在本地改动观察、样本和聚集事件，回网时以批次形式提交。
- 批次按`seq`顺序应用；`op=create`可用`client_id`作为端临时ID，批次内样本对观察、聚集对观察的引用会自动解析为服务端ID。
- 改动携带`baseline_version`（设备端最后见到的版本）。若服务端当前版本与基线不一致，说明该观察被其他设备改过：物种和地点各留一版候选（先到设备的版本保留在canonical，后到设备的版本进入裁定候选），不互相覆盖，等站里裁定。
- 裁定后按选定版本更新观察，并重新核验关联样本状态与聚集成员。
- 已确认的聚集事件只要成员或坐标改变就重算质心并退回待确认（`draft`）；野外角色不能替站里确认聚集（`confirm_cluster`仅`admin`/`epidemiologist`可执行）。

## 测试

```bash
python3 -m unittest discover -s tests -v
```

## 局限

离线同步使用批次和幂等键演示，不包含真实野外通信协议、地图底图或完整空间索引。
