import json
import tempfile
import unittest
from http.client import HTTPConnection
from pathlib import Path
from threading import Thread

from src.domain import Actor, ConflictError, PermissionDenied, ValidationError
from src.http_api import create_server
from src.repository import SQLiteRepository
from src.rules import RuleEngine, recompute_membership
from src.service import DomainService


OBS = {
    "event_id": "E-1",
    "species": "deer",
    "location": "North",
    "observed_at": "2026-04-01",
    "lat": 40.0,
    "lon": 116.0,
}


class OfflineSyncTest(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.repo = SQLiteRepository(Path(self.tmp.name) / "test.db")
        self.service = DomainService(self.repo, RuleEngine())
        self.field = Actor("field-1", "field")
        self.field_a = Actor("fa", "field")
        self.field_b = Actor("fb", "field")
        self.epi = Actor("epi-1", "epidemiologist")

    def tearDown(self):
        self.tmp.cleanup()

    def _observation(self, actor=None, device=None, batch=None, **overrides):
        actor = actor or self.field
        data = dict(OBS)
        data.update(overrides)
        if device:
            response = self.service.sync_batch(
                actor,
                device,
                batch,
                [{"seq": 1, "op": "create", "kind": "observation", "data": data}],
            )
            result = response["results"][0]
            self.assertTrue(result["ok"], result)
            return result["entity_id"]
        return self.service.create(actor, "observation", data)["id"]

    def _submit(self, obs_id, version=1, actor=None, observed_at="2026-04-01"):
        return self.service.transition(
            actor or self.field,
            obs_id,
            "submit",
            {"location": "North", "observed_at": observed_at},
            version,
        )

    # ------------------------------------------------------------------
    # Batch idempotency and provenance
    # ------------------------------------------------------------------
    def test_batch_retransmission_returns_first_result_without_duplicates(self):
        first = self.service.sync_batch(
            self.field,
            "device-A",
            "batch-1",
            [{"seq": 1, "op": "create", "kind": "observation", "data": dict(OBS)}],
        )
        self.assertEqual(first["replayed"], False)
        obs_id = first["results"][0]["entity_id"]

        # Device resends after connectivity returns; the second payload is
        # never executed.
        resent = self.service.sync_batch(
            self.field,
            "device-A",
            "batch-1",
            [
                {"seq": 1, "op": "create", "kind": "observation", "data": dict(OBS)},
                {"seq": 2, "op": "create", "kind": "observation", "data": dict(OBS, event_id="E-OTHER")},
            ],
        )
        self.assertTrue(resent["replayed"])
        self.assertEqual(len(resent["results"]), 1)
        self.assertEqual(resent["results"][0]["entity_id"], obs_id)
        self.assertEqual(len(self.service.list("observation")), 1)

    def test_every_change_carries_device_seq_and_baseline(self):
        obs_id = self._observation(device="device-A", batch="b1")
        self.service.sync_batch(
            self.field_a,
            "device-A",
            "b2",
            [
                {
                    "seq": 7,
                    "op": "action",
                    "entity_id": obs_id,
                    "action": "submit",
                    "data": {"location": "North", "observed_at": "2026-04-01"},
                    "baseline_version": 1,
                }
            ],
        )
        changes = self.repo.list_sync_changes("device-A")
        self.assertEqual([(c["batch_id"], c["seq"]) for c in changes], [("b1", 1), ("b2", 7)])
        self.assertEqual(changes[1]["baseline_version"], 1)
        self.assertEqual(changes[1]["applied_version"], 2)

        audit = self.repo.list_audit(obs_id)
        self.assertEqual(audit[1]["detail"]["provenance"]["device_id"], "device-A")
        self.assertEqual(audit[1]["detail"]["provenance"]["seq"], 7)
        self.assertEqual(audit[1]["detail"]["provenance"]["baseline_version"], 1)

        field_source = self.service.get(obs_id)["data"]["_field_source"]["species"]
        self.assertEqual(field_source["device_id"], "device-A")
        self.assertEqual(field_source["batch_id"], "b1")
        self.assertEqual(field_source["seq"], 1)

    def test_batch_change_failure_does_not_abort_batch(self):
        response = self.service.sync_batch(
            self.field,
            "device-A",
            "batch-1",
            [
                {"seq": 1, "op": "create", "kind": "sample",
                 "data": {"observation_id": "missing", "sample_code": "W-X"}},
                {"seq": 2, "op": "create", "kind": "observation", "data": dict(OBS)},
            ],
        )
        self.assertFalse(response["results"][0]["ok"])
        self.assertEqual(response["results"][0]["type"], "ValidationError")
        self.assertTrue(response["results"][1]["ok"])

    def test_batch_validates_identity_and_seq(self):
        with self.assertRaises(ValidationError):
            self.service.sync_batch(self.field, "", "b", [])
        with self.assertRaises(ValidationError):
            self.service.sync_batch(self.field, "d", "b", [])
        with self.assertRaises(ValidationError):
            self.service.sync_batch(self.field, "d", "b", [{"seq": 0, "op": "create"}])
        with self.assertRaises(ValidationError):
            self.service.sync_batch(
                self.field, "d", "b", [{"seq": 1, "op": "create"}, {"seq": 1, "op": "create"}]
            )

    def test_batch_action_requires_baseline_version(self):
        obs_id = self._observation(device="device-A", batch="b1")
        response = self.service.sync_batch(
            self.field,
            "device-A",
            "b2",
            [
                {
                    "seq": 1,
                    "op": "action",
                    "entity_id": obs_id,
                    "action": "submit",
                    "data": {"location": "North", "observed_at": "2026-04-01"},
                }
            ],
        )
        self.assertFalse(response["results"][0]["ok"])
        self.assertEqual(response["results"][0]["type"], "ValidationError")

    # ------------------------------------------------------------------
    # Two devices editing one observation
    # ------------------------------------------------------------------
    def _two_device_conflict(self, species=("elk", "boar"), place_b=None):
        obs_id = self._observation()
        self._submit(obs_id, version=1)
        baseline = self.service.get(obs_id)["version"]
        self.service.sync_batch(
            self.field_a,
            "device-A",
            "b3",
            [{"seq": 1, "op": "action", "entity_id": obs_id, "action": "revise",
              "data": {"species": species[0]}, "baseline_version": baseline}],
        )
        patch_b = {"species": species[1]}
        if place_b:
            patch_b.update(place_b)
        response = self.service.sync_batch(
            self.field_b,
            "device-B",
            "b4",
            [{"seq": 1, "op": "action", "entity_id": obs_id, "action": "revise",
              "data": patch_b, "baseline_version": baseline}],
        )
        return obs_id, baseline, response

    def test_divergent_species_and_location_become_candidates(self):
        obs_id, _, response = self._two_device_conflict(
            place_b={"location": "South", "lat": 40.05, "lon": 116.05}
        )
        self.assertTrue(response["results"][0]["disputed"])
        obs = self.service.get(obs_id)
        self.assertEqual(obs["status"], "disputed")
        species_values = {c["value"] for c in obs["data"]["_species_candidates"]}
        self.assertEqual(species_values, {"elk", "boar"})
        places = {
            (c["value"]["location"], c["value"]["lat"], c["value"]["lon"])
            for c in obs["data"]["_location_candidates"]
        }
        self.assertIn(("North", 40.0, 116.0), places)
        self.assertIn(("South", 40.05, 116.05), places)
        # Every non-current candidate names the device that proposed it; the
        # current winners keep their original field provenance.
        proposed = {
            (c["origin"].get("device_id"), c["value"])
            for c in obs["data"]["_species_candidates"]
            if not c.get("baseline")
        }
        self.assertIn(("device-B", "boar"), proposed)
        self.assertEqual(
            obs["data"]["_field_source"]["species"]["device_id"], "device-A"
        )
        place_proposed = {
            (c["origin"].get("device_id"), (c["value"]["lat"], c["value"]["lon"]))
            for c in obs["data"]["_location_candidates"]
            if not c.get("baseline")
        }
        self.assertIn(("device-B", (40.05, 116.05)), place_proposed)

    def test_same_value_from_stale_baseline_is_version_conflict_not_overwrite(self):
        obs_id, baseline, _ = self._two_device_conflict(species=("elk", "elk"))
        # Both asked for the same value; the second one retries against the
        # current version instead of silently forking the record.
        obs = self.service.get(obs_id)
        self.assertEqual(obs["status"], "submitted")
        self.assertEqual(obs["data"]["species"], "elk")

    def test_linked_samples_are_held_and_reverified_after_adjudication(self):
        obs_id = self._observation()
        self._submit(obs_id, version=1)
        sample_id = self.service.create(
            self.field, "sample", {"observation_id": obs_id, "sample_code": "W-1"}
        )["id"]
        self.service.transition(self.field, sample_id, "send_lab", {"lab_id": "LAB-1"}, 1)

        baseline = self.service.get(obs_id)["version"]
        self.service.sync_batch(
            self.field_a,
            "device-A",
            "b3",
            [{"seq": 1, "op": "action", "entity_id": obs_id, "action": "revise",
              "data": {"species": "elk"}, "baseline_version": baseline}],
        )
        self.service.sync_batch(
            self.field_b,
            "device-B",
            "b4",
            [{"seq": 1, "op": "action", "entity_id": obs_id, "action": "revise",
              "data": {"species": "boar"}, "baseline_version": baseline}],
        )
        self.assertEqual(self.service.get(obs_id)["status"], "disputed")
        self.assertEqual(self.service.get(sample_id)["status"], "held")
        self.assertEqual(self.service.get(sample_id)["data"]["_held_status"], "in_lab")

        obs = self.service.get(obs_id)
        self.service.transition(
            self.epi,
            obs_id,
            "adjudicate",
            {"species": "boar"},
            obs["version"],
        )
        obs = self.service.get(obs_id)
        self.assertEqual(obs["status"], "submitted")
        self.assertEqual(obs["data"]["species"], "boar")
        self.assertNotIn("_species_candidates", obs["data"])
        sample = self.service.get(sample_id)
        self.assertEqual(sample["status"], "in_lab")
        self.assertNotIn("_held_status", sample["data"])

    def test_field_role_cannot_adjudicate(self):
        obs_id, _, _ = self._two_device_conflict()
        version = self.service.get(obs_id)["version"]
        with self.assertRaises(PermissionDenied):
            self.service.transition(
                self.field, obs_id, "adjudicate", {"species": "boar"}, version
            )

    def test_species_and_location_adjudicated_independently(self):
        obs_id = self._observation(event_id="E-2", lat=41.0, lon=117.0, location="East")
        self._submit(obs_id, version=1, observed_at="2026-05-01")
        baseline = self.service.get(obs_id)["version"]
        self.service.transition(
            self.field_a, obs_id, "revise",
            {"species": "wolf", "location": "West", "lat": 41.2, "lon": 117.2}, baseline,
        )
        self.service.transition(
            self.field_b, obs_id, "revise",
            {"species": "bear", "location": "East", "lat": 41.0, "lon": 117.0}, baseline,
        )
        obs = self.service.get(obs_id)
        self.assertEqual(obs["status"], "disputed")

        self.service.transition(self.epi, obs_id, "adjudicate", {"species": "wolf"}, obs["version"])
        obs = self.service.get(obs_id)
        self.assertEqual(obs["status"], "disputed")
        self.assertEqual(obs["data"]["species"], "wolf")
        self.assertNotIn("_species_candidates", obs["data"])
        self.assertTrue(obs["data"]["_location_candidates"])

        self.service.transition(
            self.epi, obs_id, "adjudicate",
            {"location": "West", "lat": 41.2, "lon": 117.2}, obs["version"],
        )
        obs = self.service.get(obs_id)
        self.assertEqual(obs["status"], "submitted")
        self.assertEqual((obs["data"]["lat"], obs["data"]["lon"]), (41.2, 117.2))

    def test_adjudication_choice_must_match_candidate(self):
        obs_id, _, _ = self._two_device_conflict()
        version = self.service.get(obs_id)["version"]
        with self.assertRaises(ValidationError):
            self.service.transition(
                self.epi, obs_id, "adjudicate", {"species": "dragon"}, version
            )

    def test_rejecting_disputed_observation_invalidates_held_samples(self):
        obs_id = self._observation(event_id="E-9")
        self._submit(obs_id, version=1)
        sample_id = self.service.create(
            self.field, "sample", {"observation_id": obs_id, "sample_code": "W-9"}
        )["id"]
        baseline = self.service.get(obs_id)["version"]
        self.service.transition(self.field_a, obs_id, "revise", {"species": "wolf"}, baseline)
        self.service.transition(self.field_b, obs_id, "revise", {"species": "hare-x"}, baseline)
        self.assertEqual(self.service.get(sample_id)["status"], "held")
        version = self.service.get(obs_id)["version"]
        self.service.transition(self.epi, obs_id, "reject", {"reason": "bogus"}, version)
        self.assertEqual(self.service.get(obs_id)["status"], "rejected")
        self.assertEqual(self.service.get(sample_id)["status"], "invalidated")

    # ------------------------------------------------------------------
    # Cluster re-verification
    # ------------------------------------------------------------------
    def test_confirmed_cluster_reopens_when_member_coordinates_change(self):
        ids = []
        for index, (lat, lon) in enumerate(
            [(40.0, 116.0), (40.01, 116.01), (40.02, 116.02)]
        ):
            obs_id = self._observation(
                actor=self.field,
                event_id="C-%d" % index,
                lat=lat,
                lon=lon,
                observed_at="2026-06-01",
                location="N",
            )
            self._submit(obs_id, version=1, observed_at="2026-06-01")
            ids.append(obs_id)

        cluster = self.service.create(self.epi, "cluster", {"region": "North"})
        self.service.transition(
            self.epi,
            cluster["id"],
            "confirm_cluster",
            {"observation_ids": ids, "centroid": [40.01, 116.01]},
            1,
        )
        # Field devices cannot confirm clusters.
        with self.assertRaises(PermissionDenied):
            self.service.create(self.field, "cluster", {"region": "X"})

        # One member moves ~100 km away: membership is recomputed and the
        # confirmed cluster falls back to draft.
        version = self.service.get(ids[2])["version"]
        self.service.transition(
            self.field, ids[2], "revise", {"lat": 41.0, "lon": 117.0}, version
        )
        cluster = self.service.get(cluster["id"])
        self.assertEqual(cluster["status"], "draft")
        self.assertNotIn(ids[2], cluster["data"]["observation_ids"])
        self.assertFalse(cluster["data"]["qualifies"])

        # Move the member back: it is re-admitted from the candidate pool, but
        # the cluster stays draft until the station re-confirms it.
        version = self.service.get(ids[2])["version"]
        self.service.transition(
            self.field, ids[2], "revise", {"lat": 40.02, "lon": 116.02}, version
        )
        cluster = self.service.get(cluster["id"])
        self.assertEqual(cluster["status"], "draft")
        self.assertEqual(set(cluster["data"]["observation_ids"]), set(ids))
        self.assertTrue(cluster["data"]["qualifies"])
        with self.assertRaises(PermissionDenied):
            self.service.transition(
                self.field,
                cluster["id"],
                "confirm_cluster",
                {"observation_ids": ids, "centroid": cluster["data"]["centroid"]},
                cluster["version"],
            )
        self.service.transition(
            self.epi,
            cluster["id"],
            "confirm_cluster",
            {"observation_ids": ids, "centroid": cluster["data"]["centroid"]},
            cluster["version"],
        )
        self.assertEqual(self.service.get(cluster["id"])["status"], "confirmed")

    def test_recompute_membership_helper(self):
        close = [
            {"id": "a", "observed_at": "2026-06-01", "lat": 40.0, "lon": 116.0},
            {"id": "b", "observed_at": "2026-06-01", "lat": 40.01, "lon": 116.01},
            {"id": "c", "observed_at": "2026-06-01", "lat": 40.02, "lon": 116.02},
        ]
        ids, centroid, qualifies = recompute_membership(close)
        self.assertEqual(set(ids), {"a", "b", "c"})
        self.assertTrue(qualifies)
        self.assertEqual(centroid, (40.01, 116.01))

        far = close + [
            {"id": "d", "observed_at": "2026-06-01", "lat": 41.0, "lon": 117.0}
        ]
        ids, _, qualifies = recompute_membership(far)
        self.assertEqual(set(ids), {"a", "b", "c"})

    # ------------------------------------------------------------------
    # HTTP surface
    # ------------------------------------------------------------------
    def test_http_batch_endpoint_and_replay(self):
        server = create_server(
            "127.0.0.1", 0, self.service, RuleEngine(), str(Path(__file__).resolve().parent.parent / "static")
        )
        thread = Thread(target=server.serve_forever, daemon=True)
        thread.start()
        try:
            host, port = server.server_address

            def post(path, payload, role="field", user="dev-1"):
                conn = HTTPConnection(host, port, timeout=5)
                body = json.dumps(payload).encode("utf-8")
                conn.request(
                    "POST",
                    path,
                    body=body,
                    headers={
                        "Content-Type": "application/json",
                        "X-User-Id": user,
                        "X-Role": role,
                    },
                )
                response = conn.getresponse()
                payload = json.loads(response.read().decode("utf-8"))
                conn.close()
                return response.status, payload

            status, payload = post(
                "/api/sync/batch",
                {
                    "device_id": "dev-http",
                    "batch_id": "http-1",
                    "changes": [
                        {"seq": 1, "op": "create", "kind": "observation", "data": dict(OBS)}
                    ],
                },
            )
            self.assertEqual(status, 200, payload)
            self.assertFalse(payload["replayed"])
            obs_id = payload["results"][0]["entity_id"]

            status, replay = post(
                "/api/sync/batch",
                {"device_id": "dev-http", "batch_id": "http-1", "changes": []},
            )
            self.assertEqual(status, 200)
            self.assertTrue(replay["replayed"])
            self.assertEqual(replay["results"][0]["entity_id"], obs_id)

            conn = HTTPConnection(host, port, timeout=5)
            conn.request("GET", "/api/sync/changes?device_id=dev-http")
            listing = json.loads(conn.getresponse().read().decode("utf-8"))
            conn.close()
            self.assertEqual(len(listing["items"]), 1)
            self.assertEqual(listing["items"][0]["batch_id"], "http-1")
        finally:
            server.shutdown()
            server.server_close()


if __name__ == "__main__":
    unittest.main()
