from uuid import uuid4

from .audit import AuditTrail
from .domain import ConflictError, NotFoundError, ValidationError
from .repository import utcnow
from .rules import RuleEngine, compute_centroid


class DomainService:
    def __init__(self, repository, rules=None):
        self.repository = repository
        self.rules = rules or RuleEngine()
        self.audit = AuditTrail(repository)

    def _lookup(self, kind, field, value):
        return self.repository.find_entities(self.rules.normalize_kind(kind), field, value)

    def health(self):
        return {"status": "ok" if self.repository.ping() else "error"}

    def create(self, actor, kind, data, idempotency_key=None):
        kind = self.rules.normalize_kind(kind)
        payload = dict(data or {})
        if idempotency_key:
            existing = self.repository.get_idempotency(actor.user_id, idempotency_key)
            if existing:
                entity = self.repository.get_entity(existing)
                if entity:
                    return entity
        self.rules.validate_create(actor, kind, payload, self._lookup)
        entity_id = str(payload.pop("id", "") or uuid4())
        if self.repository.get_entity(entity_id):
            raise ConflictError("entity already exists: " + entity_id)
        status = self.rules.initial_status(kind)
        entity = self.repository.create_entity(entity_id, kind, status, payload, actor.user_id)
        self.audit.record(entity_id, actor, "create", None, status, {"kind": kind})
        if idempotency_key:
            self.repository.save_idempotency(actor.user_id, idempotency_key, entity_id)
        return entity

    def transition(self, actor, entity_id, action, data=None, expected_version=None):
        entity = self.repository.get_entity(entity_id)
        if not entity:
            raise NotFoundError("entity not found: " + entity_id)
        expected = int(expected_version) if expected_version is not None else entity["version"]
        next_status, patch = self.rules.validate_transition(
            actor, entity, action, dict(data or {}), self._lookup
        )
        merged = dict(entity["data"])
        merged.update(patch)
        updated = self.repository.update_entity(entity_id, expected, next_status, merged)
        self.audit.record(
            entity_id,
            actor,
            action,
            entity["status"],
            updated["status"],
            {"patch": patch},
        )
        return updated

    def get(self, entity_id):
        entity = self.repository.get_entity(entity_id)
        if not entity:
            raise NotFoundError("entity not found: " + entity_id)
        return entity

    def list(self, kind=None, status=None):
        if kind:
            kind = self.rules.normalize_kind(kind)
        return self.repository.list_entities(kind=kind, status=status)

    def audit_log(self, entity_id=None):
        return self.repository.list_audit(entity_id=entity_id)

    # ------------------------------------------------------------------
    # 离线批次同步
    # ------------------------------------------------------------------
    def sync_batch(self, actor, batch_id, device_id, changes):
        batch_id = str(batch_id)
        existing = self.repository.get_batch(batch_id)
        if existing:
            # 重传只返回第一次结果，不重复应用任何改动
            return existing["result"]
        client_map = {}
        results = []
        adjudications = []
        has_conflict = False
        ordered = sorted(changes or [], key=lambda item: item.get("seq", 0))
        for change in ordered:
            result = self._apply_change(actor, device_id, batch_id, change, client_map)
            results.append(result)
            if result.get("status") == "conflict":
                has_conflict = True
                if result.get("adjudication"):
                    adjudications.append(result["adjudication"])
        status = "conflict" if has_conflict else "processed"
        result = {
            "batch_id": batch_id,
            "device_id": device_id,
            "status": status,
            "results": results,
            "adjudications": adjudications,
        }
        self.repository.save_batch(batch_id, device_id, actor.user_id, status, result)
        return result

    def _apply_change(self, actor, device_id, batch_id, change, client_map):
        seq = change.get("seq")
        op = change.get("op")
        kind = self.rules.normalize_kind(change.get("kind"))
        data = self._resolve_refs(dict(change.get("data") or {}), client_map)
        baseline = change.get("baseline_version")
        if op == "create":
            return self._apply_create(actor, device_id, change, client_map, kind, data, seq)
        entity_id = change.get("entity_id")
        entity_id = client_map.get(entity_id, entity_id)
        entity = self.repository.get_entity(entity_id) if entity_id else None
        if not entity:
            raise NotFoundError("entity not found: " + str(entity_id))
        if baseline is not None and int(baseline) != entity["version"]:
            return self._conflict(actor, device_id, batch_id, seq, op, entity, data)
        if op == "transition":
            action = change.get("action")
            next_status, patch = self.rules.validate_transition(
                actor, entity, action, data, self._lookup
            )
            merged = dict(entity["data"])
            merged.update(patch)
            updated = self.repository.update_entity(
                entity_id, entity["version"], next_status, merged, device_id=device_id
            )
            self.audit.record(
                entity_id, actor, action, entity["status"], updated["status"],
                {"batch_id": batch_id, "seq": seq, "device_id": device_id, "patch": patch},
            )
            self._post_change_reverify(actor, device_id, updated)
            return {"seq": seq, "op": op, "kind": kind, "status": "applied", "entity_id": entity_id}
        if op == "update":
            merged = dict(entity["data"])
            merged.update(data)
            self.rules.validate_update(actor, kind, merged, self._lookup)
            updated = self.repository.update_entity(
                entity_id, entity["version"], entity["status"], merged, device_id=device_id
            )
            self.audit.record(
                entity_id, actor, "update", entity["status"], updated["status"],
                {"batch_id": batch_id, "seq": seq, "device_id": device_id, "patch": data},
            )
            self._post_change_reverify(actor, device_id, updated)
            return {"seq": seq, "op": op, "kind": kind, "status": "applied", "entity_id": entity_id}
        raise ValidationError("unknown op: " + str(op))

    def _apply_create(self, actor, device_id, change, client_map, kind, data, seq):
        self.rules.validate_create(actor, kind, data, self._lookup)
        client_id = change.get("client_id")
        if client_id and not self.repository.get_entity(client_id):
            entity_id = client_id
        else:
            entity_id = str(uuid4())
        status = self.rules.initial_status(kind)
        entity = self.repository.create_entity(
            entity_id, kind, status, data, actor.user_id, device_id=device_id
        )
        if client_id:
            client_map[client_id] = entity_id
        self.audit.record(
            entity_id, actor, "create", None, status,
            {"batch_id": change.get("batch_id"), "seq": seq, "device_id": device_id},
        )
        return {"seq": seq, "op": "create", "kind": kind, "status": "applied", "entity_id": entity_id}

    def _conflict(self, actor, device_id, batch_id, seq, op, entity, data):
        if entity["kind"] == "observation":
            adjudication = self.repository.get_adjudication(entity["id"])
            if not adjudication:
                adjudication = {
                    "entity_id": entity["id"],
                    "kind": entity["kind"],
                    "status": "pending",
                    "candidates": [],
                    "decision": None,
                    "decided_by": None,
                    "created_at": utcnow(),
                    "decided_at": None,
                }
            # 把当前 canonical（先到设备的版本）也留一版，避免被后到设备覆盖
            canonical_device = entity.get("device_id") or "canonical"
            known = {item["device_id"] for item in adjudication["candidates"]}
            if canonical_device not in known:
                adjudication["candidates"].append({
                    "device_id": canonical_device,
                    "batch_id": None,
                    "seq": None,
                    "species": entity["data"].get("species"),
                    "location": entity["data"].get("location"),
                    "data": dict(entity["data"]),
                    "canonical": True,
                })
            candidate = {
                "device_id": device_id,
                "batch_id": batch_id,
                "seq": seq,
                "species": data.get("species"),
                "location": data.get("location"),
                "data": data,
            }
            adjudication["candidates"].append(candidate)
            self.repository.save_adjudication(adjudication)
            self.audit.record(
                entity["id"], actor, "conflict", entity["status"], entity["status"],
                {"batch_id": batch_id, "seq": seq, "device_id": device_id},
            )
            return {
                "seq": seq,
                "op": op,
                "kind": entity["kind"],
                "status": "conflict",
                "entity_id": entity["id"],
                "adjudication": adjudication,
            }
        # 样本或聚集的冲突不覆盖 canonical 数据，仅标记冲突
        self.audit.record(
            entity["id"], actor, "conflict", entity["status"], entity["status"],
            {"batch_id": batch_id, "seq": seq, "device_id": device_id, "kind": entity["kind"]},
        )
        return {
            "seq": seq,
            "op": op,
            "kind": entity["kind"],
            "status": "conflict",
            "entity_id": entity["id"],
        }

    def _resolve_refs(self, data, client_map):
        if not client_map:
            return data
        data = dict(data)
        if data.get("observation_id") in client_map:
            data["observation_id"] = client_map[data["observation_id"]]
        if isinstance(data.get("observation_ids"), list):
            data["observation_ids"] = [
                client_map.get(item, item) for item in data["observation_ids"]
            ]
        return data

    # ------------------------------------------------------------------
    # 裁定与重新核验
    # ------------------------------------------------------------------
    def list_adjudications(self, status=None):
        return self.repository.list_adjudications(status=status)

    def decide_adjudication(self, actor, entity_id, decision):
        self.rules.validate_adjudicate(actor)
        entity = self.repository.get_entity(entity_id)
        if not entity:
            raise NotFoundError("entity not found: " + entity_id)
        adjudication = self.repository.get_adjudication(entity_id)
        if not adjudication or adjudication["status"] != "pending":
            raise ConflictError("no pending adjudication for entity: " + entity_id)
        species = decision.get("species")
        location = decision.get("location")
        candidate_device = decision.get("candidate_device")
        if candidate_device:
            candidate = next(
                (item for item in adjudication["candidates"] if item["device_id"] == candidate_device),
                None,
            )
            if not candidate:
                raise ValidationError("unknown candidate device: " + str(candidate_device))
            species = candidate["species"]
            location = candidate["location"]
        if species is None or location is None:
            raise ValidationError("species and location are required")
        data = dict(entity["data"])
        data["species"] = species
        data["location"] = location
        updated = self.repository.update_entity(
            entity_id, entity["version"], entity["status"], data, device_id=entity.get("device_id")
        )
        adjudication["status"] = "decided"
        adjudication["decision"] = {
            "species": species,
            "location": location,
            "candidate_device": candidate_device,
        }
        adjudication["decided_by"] = actor.user_id
        adjudication["decided_at"] = utcnow()
        self.repository.save_adjudication(adjudication)
        self.audit.record(
            entity_id, actor, "adjudicate", entity["status"], entity["status"],
            {"species": species, "location": location, "candidate_device": candidate_device},
        )
        self._reverify_samples(actor, updated)
        self._reverify_clusters_for_observation(actor, updated)
        return updated

    def _post_change_reverify(self, actor, device_id, entity):
        if entity["kind"] == "observation":
            self._reverify_clusters_for_observation(actor, entity)
        elif entity["kind"] == "cluster":
            self._recompute_cluster(actor, device_id, entity)

    def _reverify_samples(self, actor, observation):
        samples = self.repository.list_entities(kind="sample")
        for sample in samples:
            if sample["data"].get("observation_id") != observation["id"]:
                continue
            linked = self.repository.get_entity(observation["id"])
            precondition_ok = linked and linked["status"] in ("submitted", "sampled")
            if precondition_ok:
                continue
            updated = self.repository.update_entity(
                sample["id"], sample["version"], "collected", sample["data"],
                device_id=sample.get("device_id"),
            )
            self.audit.record(
                sample["id"], actor, "reverify", sample["status"], "collected",
                {"observation_id": observation["id"], "reason": "observation no longer valid"},
            )
            return updated
        return None

    def _reverify_clusters_for_observation(self, actor, observation):
        clusters = self.repository.list_entities(kind="cluster")
        for cluster in clusters:
            if observation["id"] in (cluster["data"].get("observation_ids") or []):
                self._recompute_cluster(actor, cluster.get("device_id"), cluster)

    def _recompute_cluster(self, actor, device_id, cluster):
        data = dict(cluster["data"])
        obs_ids = list(data.get("observation_ids") or [])
        members = []
        for oid in obs_ids:
            member = self.repository.get_entity(oid)
            if member and member["kind"] == "observation":
                members.append(member["data"])
        new_centroid = compute_centroid(members)
        old_centroid = data.get("centroid")
        old_members = data.get("_last_observation_ids", obs_ids)
        members_changed = set(obs_ids) != set(old_members)
        coords_changed = new_centroid != old_centroid
        data["centroid"] = new_centroid
        data["_last_observation_ids"] = obs_ids
        next_status = cluster["status"]
        if cluster["status"] == "confirmed" and (members_changed or coords_changed):
            next_status = "draft"
        updated = self.repository.update_entity(
            cluster["id"], cluster["version"], next_status, data, device_id=device_id
        )
        if next_status != cluster["status"]:
            self.audit.record(
                cluster["id"], actor, "recompute", cluster["status"], next_status,
                {"centroid": new_centroid, "members_changed": members_changed, "coords_changed": coords_changed},
            )
        return updated
