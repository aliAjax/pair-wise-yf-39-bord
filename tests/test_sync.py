import tempfile
import unittest
from pathlib import Path

from src.domain import Actor, PermissionDenied
from src.repository import SQLiteRepository
from src.rules import RuleEngine
from src.service import DomainService


def _resolve(value, created):
    if isinstance(value, str):
        for key, item in created.items():
            value = value.replace("{" + key + "}", str(item))
        return value
    if isinstance(value, list):
        return [_resolve(item, created) for item in value]
    if isinstance(value, dict):
        return {key: _resolve(item, created) for key, item in value.items()}
    return value


class OfflineSyncTest(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.repo = SQLiteRepository(Path(self.tmp.name) / "test.db")
        self.service = DomainService(self.repo, RuleEngine())
        self.field = Actor("field-1", "field")
        self.field2 = Actor("field-2", "field")
        self.admin = Actor("admin", "admin")
        self.epi = Actor("epi", "epidemiologist")

    def tearDown(self):
        self.tmp.cleanup()

    def _field_batch(self):
        return [
            {"seq": 1, "op": "create", "kind": "observation", "client_id": "o1",
             "data": {"event_id": "E-1", "species": "deer", "location": "North",
                      "observed_at": "2026-04-01", "lat": 40.0, "lon": 116.0}},
            {"seq": 2, "op": "transition", "kind": "observation", "entity_id": "o1",
             "action": "submit", "baseline_version": 1,
             "data": {"location": "North", "observed_at": "2026-04-01"}},
            {"seq": 3, "op": "create", "kind": "observation", "client_id": "o2",
             "data": {"event_id": "E-2", "species": "deer", "location": "North",
                      "observed_at": "2026-04-03", "lat": 40.01, "lon": 116.01}},
            {"seq": 4, "op": "transition", "kind": "observation", "entity_id": "o2",
             "action": "submit", "baseline_version": 1,
             "data": {"location": "North", "observed_at": "2026-04-03"}},
            {"seq": 5, "op": "create", "kind": "observation", "client_id": "o3",
             "data": {"event_id": "E-3", "species": "deer", "location": "North",
                      "observed_at": "2026-04-05", "lat": 40.02, "lon": 116.02}},
            {"seq": 6, "op": "transition", "kind": "observation", "entity_id": "o3",
             "action": "submit", "baseline_version": 1,
             "data": {"location": "North", "observed_at": "2026-04-05"}},
            {"seq": 7, "op": "create", "kind": "sample", "client_id": "s1",
             "data": {"observation_id": "o1", "sample_code": "W-1"}},
            {"seq": 8, "op": "transition", "kind": "sample", "entity_id": "s1",
             "action": "send_lab", "baseline_version": 1, "data": {"lab_id": "LAB-1"}},
        ]

    def test_batch_sync_applies_changes_with_device_seq_baseline(self):
        result = self.service.sync_batch(self.field, "batch-1", "dev-1", self._field_batch())
        self.assertEqual(result["status"], "processed")
        self.assertEqual([r["status"] for r in result["results"]], ["applied"] * 8)
        observations = self.repo.list_entities(kind="observation")
        self.assertEqual(len(observations), 3)
        self.assertTrue(all(o["status"] == "submitted" for o in observations))
        sample = self.repo.list_entities(kind="sample")[0]
        self.assertEqual(sample["status"], "in_lab")
        self.assertEqual(sample["device_id"], "dev-1")

    def test_retransmission_returns_first_result(self):
        first = self.service.sync_batch(self.field, "batch-1", "dev-1", self._field_batch())
        second = self.service.sync_batch(self.field, "batch-1", "dev-1", self._field_batch())
        self.assertEqual(first["results"], second["results"])
        self.assertEqual(first["status"], second["status"])
        # 重传不会多建记录
        self.assertEqual(len(self.repo.list_entities(kind="observation")), 3)

    def test_concurrent_observation_edits_create_adjudication(self):
        self.service.sync_batch(self.field, "batch-1", "dev-1", self._field_batch())
        obs_id = self.repo.list_entities(kind="observation")[0]["id"]
        current = self.service.get(obs_id)
        # 先到设备基于当前版本编辑，正常应用
        self.service.sync_batch(
            self.field, "batch-dev1", "dev-1",
            [{"seq": 1, "op": "update", "kind": "observation", "entity_id": obs_id,
              "baseline_version": current["version"],
              "data": {"species": "deer", "location": "North"}}],
        )
        # 后到设备基于旧基线编辑，触发冲突
        result = self.service.sync_batch(
            self.field2, "batch-dev2", "dev-2",
            [{"seq": 1, "op": "update", "kind": "observation", "entity_id": obs_id,
              "baseline_version": 1,
              "data": {"species": "elk", "location": "South"}}],
        )
        self.assertEqual(result["status"], "conflict")
        self.assertEqual(result["results"][0]["status"], "conflict")
        pending = self.service.list_adjudications(status="pending")
        self.assertEqual(len(pending), 1)
        devices = {c["device_id"] for c in pending[0]["candidates"]}
        self.assertIn("dev-1", devices)
        self.assertIn("dev-2", devices)
        # canonical 未被覆盖
        canonical = self.service.get(obs_id)
        self.assertEqual(canonical["data"]["species"], "deer")

    def test_field_cannot_adjudicate_but_admin_can(self):
        self.service.sync_batch(self.field, "batch-1", "dev-1", self._field_batch())
        obs_id = self.repo.list_entities(kind="observation")[0]["id"]
        current = self.service.get(obs_id)
        self.service.sync_batch(
            self.field, "b1", "dev-1",
            [{"seq": 1, "op": "update", "kind": "observation", "entity_id": obs_id,
              "baseline_version": current["version"],
              "data": {"species": "deer", "location": "North"}}],
        )
        self.service.sync_batch(
            self.field2, "b2", "dev-2",
            [{"seq": 1, "op": "update", "kind": "observation", "entity_id": obs_id,
              "baseline_version": 1,
              "data": {"species": "elk", "location": "South"}}],
        )
        with self.assertRaises(PermissionDenied):
            self.service.decide_adjudication(self.field, obs_id, {"candidate_device": "dev-2"})
        decided = self.service.decide_adjudication(
            self.admin, obs_id, {"candidate_device": "dev-2"}
        )
        self.assertEqual(decided["data"]["species"], "elk")
        self.assertEqual(decided["data"]["location"], "South")
        self.assertEqual(len(self.service.list_adjudications(status="decided")), 1)

    def test_confirmed_cluster_reverts_when_member_coordinates_change(self):
        cluster = self.service.create(self.admin, "cluster", {"region": "South"})
        oa = self.service.create(self.admin, "observation", {"event_id": "A", "species": "deer", "location": "S", "observed_at": "2026-05-01", "lat": 10.0, "lon": 10.0})
        ob = self.service.create(self.admin, "observation", {"event_id": "B", "species": "deer", "location": "S", "observed_at": "2026-05-02", "lat": 10.01, "lon": 10.01})
        oc = self.service.create(self.admin, "observation", {"event_id": "C", "species": "deer", "location": "S", "observed_at": "2026-05-03", "lat": 10.02, "lon": 10.02})
        for item in (oa, ob, oc):
            self.service.transition(self.admin, item["id"], "submit", {"location": "S", "observed_at": item["data"]["observed_at"]})
        self.service.transition(
            self.admin, cluster["id"], "confirm_cluster",
            {"observation_ids": [oa["id"], ob["id"], oc["id"]], "centroid": [10.01, 10.01]},
        )
        cluster = self.service.get(cluster["id"])
        self.assertEqual(cluster["status"], "confirmed")
        current = self.service.get(oa["id"])
        self.service.sync_batch(
            self.field, "b-edit", "dev-1",
            [{"seq": 1, "op": "update", "kind": "observation", "entity_id": oa["id"],
              "baseline_version": current["version"],
              "data": {"lat": 11.0, "lon": 11.0}}],
        )
        cluster = self.service.get(cluster["id"])
        self.assertEqual(cluster["status"], "draft")
        self.assertNotEqual(cluster["data"]["centroid"], [10.01, 10.01])

    def test_field_cannot_confirm_cluster(self):
        cluster = self.service.create(self.admin, "cluster", {"region": "East"})
        oa = self.service.create(self.admin, "observation", {"event_id": "A", "species": "deer", "location": "S", "observed_at": "2026-05-01", "lat": 10.0, "lon": 10.0})
        ob = self.service.create(self.admin, "observation", {"event_id": "B", "species": "deer", "location": "S", "observed_at": "2026-05-02", "lat": 10.01, "lon": 10.01})
        oc = self.service.create(self.admin, "observation", {"event_id": "C", "species": "deer", "location": "S", "observed_at": "2026-05-03", "lat": 10.02, "lon": 10.02})
        with self.assertRaises(PermissionDenied):
            self.service.transition(
                self.field, cluster["id"], "confirm_cluster",
                {"observation_ids": [oa["id"], ob["id"], oc["id"]], "centroid": [10.01, 10.01]},
            )

    def test_epidemiologist_batch_can_confirm_cluster(self):
        self.service.sync_batch(self.field, "b1", "dev-1", self._field_batch())
        obs_ids = [o["id"] for o in self.repo.list_entities(kind="observation")]
        changes = [
            {"seq": 1, "op": "create", "kind": "cluster", "client_id": "c1", "data": {"region": "North"}},
            {"seq": 2, "op": "transition", "kind": "cluster", "entity_id": "c1",
             "action": "confirm_cluster", "baseline_version": 1,
             "data": {"observation_ids": obs_ids, "centroid": [40.01, 116.01]}},
        ]
        result = self.service.sync_batch(self.epi, "b2", "dev-epi", changes)
        self.assertEqual(result["status"], "processed")
        cluster = self.repo.list_entities(kind="cluster")[0]
        self.assertEqual(cluster["status"], "confirmed")
        self.assertEqual(cluster["data"]["centroid"], [40.01, 116.01])


if __name__ == "__main__":
    unittest.main()
