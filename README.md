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
- `POST /api/sync/batch`：离线批次回传（见下）。
- `GET /api/sync/changes?device_id=&batch_id=`：查询批次内每条改动的溯源记录。
- `GET /api/audit`：读取审计记录。

请求身份通过`X-User-Id`和`X-Role`请求头传入。创建和动作的可执行角色由规则引擎控制。

## 离线批次同步

野外设备断网期间把改动累积成本地批次，回网后整体提交：

```json
POST /api/sync/batch
{
  "device_id": "trapcam-7",
  "batch_id": "2026-04-01-01",
  "changes": [
    {"seq": 1, "op": "create", "kind": "observation", "data": {...}},
    {"seq": 2, "op": "action", "entity_id": "...", "action": "revise",
     "data": {"species": "elk"}, "baseline_version": 3}
  ]
}
```

- 每条改动都带设备号（`device_id`）、批次内序号（`seq`）和基线版本（`baseline_version`，action 必填）。
  这些信息写入审计明细、`sync_changes` 表，以及观察数据内每个字段的 `_field_source` 溯源戳。
- 同一 `(device_id, batch_id)` 重传时，服务直接返回**第一次**的完整结果，批次内改动不会重复执行，因此不会多建记录。
- 批次内单条改动失败只影响自身（结果中带 `ok/error/type`），不阻断同批其他改动。

### 两台设备同改一条观察

- 野外角色通过 `observation.revise` 离线修订 `species` 与地点（`location/lat/lon`，地点三者作为一个整体）。
- 若改动基于过期基线、且对应字段已被另一台设备改成不同值，观察进入 `disputed`：
  两个版本都作为候选保留（`_species_candidates` / `_location_candidates`，含来源设备与批次），
  当前值不被覆盖；该观察上仍在流转的样本（collected/in_lab/resulted）一律挂起为 `held`。
- 物种与地点可以分别裁定。站里角色（`admin`/`epidemiologist`，野外角色无权）调用
  `observation.adjudicate`，只能从候选值中选定；全部待裁定字段处理完后，观察恢复争议前状态。
- 裁定后按新版本重新核验：挂起样本恢复原状态，观察若被驳回则样本置为 `invalidated`；
  聚集事件成员与质心也按裁定后的观察重新计算。

### 聚集事件重算

- 已确认（`confirmed`）的聚集事件，只要成员集合或质心坐标发生变化，就按 14 天窗口 / 10 公里
  半径重新核验成员与质心，并退回 `draft` 待确认，同时记录 `_reopen_reason`。
- 移出的成员保留在候选池 `_candidate_ids` 中，坐标回到窗口内可被重新接纳，但事件仍需站里重新确认。
- 聚集事件的确认权限始终只有 `admin`/`epidemiologist`，野外角色无法代为确认。


## 测试

```bash
python3 -m unittest discover -s tests -v
```

## 局限

离线同步提供批次幂等、字段级冲突留痕和站里裁定闭环，但不包含真实野外通信协议、地图底图或完整空间索引；聚集重算使用基于质心的近似成员核验。
