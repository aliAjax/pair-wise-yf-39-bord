from uuid import uuid4

from .audit import AuditTrail
from .domain import (
    ConflictError,
    NotFoundError,
    ValidationError,
)
from .rules import (
    PLACE_FIELDS,
    REVISE_FIELDS,
    RuleEngine,
    recompute_membership,
)

# Lifecycle statuses of a sample that still follow the observation.
ACTIVE_SAMPLE_STATUSES = ("collected", "in_lab", "resulted")


class DomainService:
    def __init__(self, repository, rules=None):
        self.repository = repository
        self.rules = rules or RuleEngine()
        self.audit = AuditTrail(repository)

    def _lookup(self, kind, field, value):
        return self.repository.find_entities(self.rules.normalize_kind(kind), field, value)

    def health(self):
        return {"status": "ok" if self.repository.ping() else "error"}

    # ------------------------------------------------------------------
    # Single-operation entry points (online or one-at-a-time devices)
    # ------------------------------------------------------------------
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
        return self._apply_action(
            actor, entity_id, action, dict(data or {}), expected_version, None
        )

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
        return self.repository.list_audit(entity_id)

    # ------------------------------------------------------------------
    # Offline batch synchronization
    # ------------------------------------------------------------------
    def sync_batch(self, actor, device_id, batch_id, changes):
        if not device_id:
            raise ValidationError("device_id is required")
        if not batch_id:
            raise ValidationError("batch_id is required")
        # Retransmitting the same batch returns the first run's exact result,
        # regardless of the resent payload (devices may retry after a timeout).
        previous = self.repository.get_sync_batch(device_id, batch_id)
        if previous is not None:
            previous["replayed"] = True
            return previous
        if not isinstance(changes, list) or not changes:
            raise ValidationError("changes must be a non-empty list")

        seen_seqs = set()
        results = []
        for raw in changes:
            if not isinstance(raw, dict):
                raise ValidationError("each change must be an object")
            seq = raw.get("seq")
            if not isinstance(seq, int) or isinstance(seq, bool) or seq < 1:
                raise ValidationError("change seq must be a positive integer")
            if seq in seen_seqs:
                raise ValidationError("duplicate seq in batch: %s" % seq)
            seen_seqs.add(seq)
            provenance = {
                "device_id": device_id,
                "batch_id": batch_id,
                "seq": seq,
                "baseline_version": raw.get("baseline_version"),
            }
            results.append(self._apply_change(actor, raw, provenance))
        response = {
            "device_id": device_id,
            "batch_id": batch_id,
            "replayed": False,
            "results": results,
        }
        self.repository.save_sync_batch(device_id, batch_id, actor.user_id, response)
        return response

    def sync_changes(self, device_id=None, batch_id=None):
        return self.repository.list_sync_changes(device_id, batch_id)

    def _apply_change(self, actor, change, provenance):
        seq = change["seq"]
        op = change.get("op")
        kind = change.get("kind")
        result = {"seq": seq, "op": op}
        try:
            if op == "create":
                entity = self._apply_create(
                    actor, kind, dict(change.get("data") or {}), provenance
                )
                result.update(
                    {
                        "ok": True,
                        "entity_id": entity["id"],
                        "kind": entity["kind"],
                        "status": entity["status"],
                        "version": entity["version"],
                        "entity": entity,
                    }
                )
            elif op == "action":
                if change.get("baseline_version") is None:
                    raise ValidationError("action change requires baseline_version")
                entity = self._apply_action(
                    actor,
                    change.get("entity_id"),
                    change.get("action"),
                    dict(change.get("data") or {}),
                    change.get("baseline_version"),
                    provenance,
                )
                result.update(
                    {
                        "ok": True,
                        "entity_id": entity["id"],
                        "kind": entity["kind"],
                        "status": entity["status"],
                        "version": entity["version"],
                        "disputed": entity["status"] == "disputed",
                        "entity": entity,
                    }
                )
            else:
                raise ValidationError("unknown op: %s" % op)
        except Exception as exc:  # one bad change does not abort the batch
            result.update({"ok": False, "error": str(exc), "type": type(exc).__name__})
        return result

    def _apply_create(self, actor, kind, payload, provenance):
        kind = self.rules.normalize_kind(kind)
        self.rules.validate_create(actor, kind, payload, self._lookup)
        entity_id = str(payload.pop("id", "") or uuid4())
        if self.repository.get_entity(entity_id):
            raise ConflictError("entity already exists: " + entity_id)
        status = self.rules.initial_status(kind)
        if kind == "observation":
            initial_fields = [field for field in REVISE_FIELDS if field in payload]
            data = self._with_initial_field_sources(payload, initial_fields, provenance)
        else:
            data = payload
        entity = self.repository.create_entity(entity_id, kind, status, data, actor.user_id)
        detail = {"kind": kind}
        if provenance:
            detail["provenance"] = dict(provenance)
        self.audit.record(entity_id, actor, "create", None, status, detail)
        self._record_change(entity, "create", provenance, None, entity["version"])
        return entity

    # ------------------------------------------------------------------
    # Action application with offline conflict branching
    # ------------------------------------------------------------------
    def _apply_action(self, actor, entity_id, action, data, expected_version, provenance):
        entity = self.repository.get_entity(entity_id)
        if not entity:
            raise NotFoundError("entity not found: " + entity_id)
        from_status = entity["status"]
        before_point = (
            entity["data"].get("lat"),
            entity["data"].get("lon"),
            entity["data"].get("observed_at"),
        )
        next_status, patch = self.rules.validate_transition(
            actor, entity, action, data, self._lookup
        )

        if (
            entity["kind"] == "observation"
            and action == "revise"
            and expected_version is not None
            and int(expected_version) < entity["version"]
        ):
            contested = self._contested_fields(entity, patch, int(expected_version))
            if contested:
                return self._branch_disputed(
                    actor, entity, patch, contested, provenance
                )

        merged = dict(entity["data"])
        merged.update(patch)

        if entity["kind"] == "observation" and action == "adjudicate":
            # The station rules on the pending candidates; either all pending
            # fields are resolved now or the observation stays disputed until
            # the remaining fields are ruled on.
            merged, next_status = self._apply_adjudication(entity, merged, patch)

        if entity["kind"] == "observation" and action == "reject":
            merged = self._clear_dispute_markers(merged)

        if entity["kind"] == "observation":
            # Every write advances field version stamps so an offline device's
            # stale baseline can see exactly which values moved.
            merged = self._restamp_tracked_fields(
                merged,
                patch if action == "revise" else {},
                provenance or self._actor_provenance(actor),
                entity["version"] + 1,
            )

        expected = (
            int(expected_version)
            if expected_version is not None
            else entity["version"]
        )
        updated = self.repository.update_entity(
            entity_id, expected, next_status, merged
        )
        detail = {"patch": patch}
        if provenance:
            detail["provenance"] = dict(provenance)
        self.audit.record(
            entity_id, actor, action, from_status, updated["status"], detail
        )
        self._record_change(updated, action, provenance, expected_version, updated["version"])

        if entity["kind"] == "observation" and action in ("adjudicate", "reject"):
            resolved = from_status == "disputed" and updated["status"] != "disputed"
            if resolved:
                self._reverify_samples(actor, updated, provenance)
            self._sync_observation_clusters(actor, entity, updated)
        elif entity["kind"] == "observation":
            after_point = (
                updated["data"].get("lat"),
                updated["data"].get("lon"),
                updated["data"].get("observed_at"),
            )
            coords_changed = before_point != after_point
            if coords_changed and updated["status"] in ("submitted", "sampled", "rejected"):
                self._sync_observation_clusters(actor, entity, updated)
        return updated

    # ------------------------------------------------------------------
    # Field-level offline conflict handling
    # ------------------------------------------------------------------
    @staticmethod
    def _revised_fields(patch):
        fields = [field for field in REVISE_FIELDS if field in patch]
        if any(field in patch for field in PLACE_FIELDS):
            return list(dict.fromkeys(fields + list(PLACE_FIELDS)))
        return fields

    @staticmethod
    def _field_source_map(entity):
        return dict(entity["data"].get("_field_source") or {})

    def _contested_fields(self, entity, patch, baseline_version):
        """Fields changed away from the patcher's baseline by another device."""
        data = entity["data"]
        sources = self._field_source_map(entity)
        contested = []
        if "species" in patch:
            last_version = int(sources.get("species", {}).get("version", 1))
            if last_version > baseline_version and patch["species"] != data.get("species"):
                contested.append("species")
        if any(field in patch for field in PLACE_FIELDS):
            incoming = tuple(
                patch.get(field, data.get(field)) for field in PLACE_FIELDS
            )
            current = tuple(data.get(field) for field in PLACE_FIELDS)
            place_source = sources.get("location") or sources.get("lat") or {}
            last_version = int(place_source.get("version", 1))
            if last_version > baseline_version and incoming != current:
                contested.append("place")
        return contested

    def _restamp_tracked_fields(self, entity_data, patch, provenance, next_version):
        """Move every tracked field's version stamp to the new entity version.

        Patched fields carry the change provenance; untouched fields keep their
        device/batch origin so a later device can tell whether the value moved
        since its baseline.
        """
        old_sources = dict(entity_data.get("_field_source") or {})
        stamped = self._revised_fields(patch)
        origin = {
            "device_id": provenance.get("device_id"),
            "batch_id": provenance.get("batch_id"),
            "seq": provenance.get("seq"),
            "baseline_version": provenance.get("baseline_version"),
        }
        if provenance.get("user_id"):
            origin["user_id"] = provenance["user_id"]
        sources = {}
        for field in REVISE_FIELDS:
            if field in stamped:
                source = dict(origin)
            else:
                # Keep the device/batch that established the current value;
                # only the version moves forward.
                source = dict(old_sources.get(field) or {})
            source["version"] = next_version
            sources[field] = source
        data = dict(entity_data)
        data["_field_source"] = sources
        return data

    def _with_initial_field_sources(self, data, fields, provenance):
        origin = {
            "device_id": provenance.get("device_id") if provenance else None,
            "batch_id": provenance.get("batch_id") if provenance else None,
            "seq": provenance.get("seq") if provenance else None,
            "baseline_version": provenance.get("baseline_version") if provenance else None,
        }
        data = dict(data)
        data["_field_source"] = {field: dict(origin, version=1) for field in fields}
        return data

    @staticmethod
    def _actor_provenance(actor):
        """Provenance for online (non-batch) changes, attributed to the user."""
        return {"user_id": actor.user_id}

    @staticmethod
    def _provenance_stamp(provenance, applied_version):
        stamp = {"version": applied_version}
        if provenance:
            stamp.update(
                device_id=provenance.get("device_id"),
                batch_id=provenance.get("batch_id"),
                seq=provenance.get("seq"),
                baseline_version=provenance.get("baseline_version"),
            )
        return stamp

    def _branch_disputed(self, actor, entity, patch, contested, provenance):
        """Keep both devices' versions of the divergent fields and park the
        observation in 'disputed' for the station to adjudicate."""
        data = dict(entity["data"])
        applied_version = entity["version"] + 1
        prior_status = entity["status"]
        stamp = self._provenance_stamp(provenance, applied_version)

        sources = self._field_source_map(entity)
        if "species" in contested:
            candidates = list(data.get("_species_candidates") or [])
            candidate = {"value": patch["species"], "origin": stamp}
            if not self._candidate_seen(candidates, candidate):
                candidates.append(candidate)
            data["_species_candidates"] = candidates
            self._seed_current_candidate(data, sources, "species")
        if "place" in contested:
            candidates = list(data.get("_location_candidates") or [])
            candidate = {
                "value": {field: patch.get(field, data.get(field)) for field in PLACE_FIELDS},
                "origin": stamp,
            }
            if not self._candidate_seen(candidates, candidate):
                candidates.append(candidate)
            data["_location_candidates"] = candidates
            self._seed_current_place_candidate(data, sources)

        # Non-contested revised fields still merge; contested fields keep
        # their current value and source until the station adjudicates.
        new_sources = dict(sources)
        for field in REVISE_FIELDS:
            if field in patch and not self._is_contested(field, contested):
                data[field] = patch[field]
                new_sources[field] = dict(stamp)
            elif field in new_sources:
                new_sources[field] = dict(new_sources[field])
                new_sources[field]["version"] = applied_version

        data["_field_source"] = new_sources
        if prior_status != "disputed":
            data["_status_before_dispute"] = prior_status
        data["_dispute_source"] = stamp
        updated = self.repository.force_update_entity(entity["id"], "disputed", data)
        detail = {"contested": contested, "patch": patch}
        if provenance:
            detail["provenance"] = dict(provenance)
        self.audit.record(
            entity["id"], actor, "revise_conflict", prior_status, "disputed", detail
        )
        self._record_change(
            updated,
            "revise",
            provenance,
            provenance.get("baseline_version") if provenance else None,
            updated["version"],
        )
        self._hold_observation_samples(actor, updated, provenance)
        return updated

    @staticmethod
    def _is_contested(field, contested):
        if field == "species":
            return "species" in contested
        return field in PLACE_FIELDS and "place" in contested

    @staticmethod
    def _candidate_seen(candidates, candidate):
        return any(item["value"] == candidate["value"] for item in candidates)

    def _seed_current_candidate(self, data, sources, field):
        candidates_key = "_%s_candidates" % field
        existing = data.get(candidates_key) or []
        if any(candidate["value"] == data.get(field) for candidate in existing):
            return
        origin_source = sources.get(field) or {"version": 1}
        seeded = {"value": data.get(field), "origin": dict(origin_source), "baseline": True}
        data[candidates_key] = [seeded] + existing

    def _seed_current_place_candidate(self, data, sources):
        existing = data.get("_location_candidates") or []
        current = {field: data.get(field) for field in PLACE_FIELDS}
        if any(candidate["value"] == current for candidate in existing):
            return
        origin_source = sources.get("location") or sources.get("lat") or {"version": 1}
        seeded = {"value": current, "origin": dict(origin_source), "baseline": True}
        data["_location_candidates"] = [seeded] + existing

    def _apply_adjudication(self, entity, merged, patch):
        data = dict(merged)
        pending_species = list(data.get("_species_candidates") or [])
        pending_places = list(data.get("_location_candidates") or [])
        if "species" in patch and pending_species:
            pending_species = []
        if any(field in patch for field in PLACE_FIELDS) and pending_places:
            pending_places = []
        if pending_species:
            data["_species_candidates"] = pending_species
        else:
            data.pop("_species_candidates", None)
        if pending_places:
            data["_location_candidates"] = pending_places
        else:
            data.pop("_location_candidates", None)
        if not pending_species and not pending_places:
            prior = data.pop("_status_before_dispute", None)
            data.pop("_dispute_source", None)
            next_status = prior if prior in ("captured", "submitted", "sampled") else "submitted"
        else:
            next_status = "disputed"
        return data, next_status

    @staticmethod
    def _clear_dispute_markers(data):
        data = dict(data)
        data.pop("_species_candidates", None)
        data.pop("_location_candidates", None)
        data.pop("_status_before_dispute", None)
        data.pop("_dispute_source", None)
        return data

    # ------------------------------------------------------------------
    # Sample re-verification after adjudication
    # ------------------------------------------------------------------
    def _hold_observation_samples(self, actor, observation, provenance):
        samples = self._lookup("sample", "observation_id", observation["id"])
        for sample in samples:
            if sample["status"] in ACTIVE_SAMPLE_STATUSES:
                data = dict(sample["data"])
                data["_held_status"] = sample["status"]
                data["_hold_reason"] = "observation awaiting station adjudication"
                updated = self.repository.force_update_entity(sample["id"], "held", data)
                detail = {"observation_id": observation["id"]}
                if provenance:
                    detail["provenance"] = dict(provenance)
                self.audit.record(
                    sample["id"], actor, "hold_for_adjudication", sample["status"], "held", detail
                )

    def _reverify_samples(self, actor, observation, provenance):
        """Re-check every held sample against the adjudicated observation."""
        samples = self._lookup("sample", "observation_id", observation["id"])
        for sample in samples:
            if sample["status"] != "held":
                continue
            data = dict(sample["data"])
            prior = data.pop("_held_status", None)
            data.pop("_hold_reason", None)
            try:
                self.rules.validate_business_create(
                    "sample",
                    {
                        "observation_id": observation["id"],
                        "sample_code": data.get("sample_code"),
                    },
                    self._lookup,
                )
            except Exception as exc:
                data["_invalidation_reason"] = str(exc)
                self.repository.force_update_entity(sample["id"], "invalidated", data)
                self.audit.record(
                    sample["id"],
                    actor,
                    "invalidate_after_adjudication",
                    "held",
                    "invalidated",
                    {"observation_id": observation["id"], "reason": str(exc)},
                )
                continue
            restored = prior if prior in ACTIVE_SAMPLE_STATUSES else "collected"
            updated = self.repository.force_update_entity(sample["id"], restored, data)
            detail = {"observation_id": observation["id"], "restored": restored}
            if provenance:
                detail["provenance"] = dict(provenance)
            self.audit.record(
                sample["id"],
                actor,
                "restore_after_adjudication",
                "held",
                updated["status"],
                detail,
            )

    # ------------------------------------------------------------------
    # Cluster re-verification
    # ------------------------------------------------------------------
    def _sync_observation_clusters(self, actor, before, after):
        clusters = self.repository.list_entities(kind="cluster")
        for cluster in clusters:
            data = cluster["data"]
            attached = set(data.get("observation_ids") or []) | set(
                data.get("_candidate_ids") or []
            )
            if before["id"] not in attached and after["id"] not in attached:
                continue
            self._reverify_cluster(actor, cluster)

    def _reverify_cluster(self, actor, cluster):
        member_ids = list(cluster["data"].get("observation_ids") or [])
        # The candidate pool keeps every member ever attached (as long as it is
        # still an active observation) so a point that drifted out can be
        # re-admitted when it moves back; confirmed membership is observation_ids.
        pool_ids = list(cluster["data"].get("_candidate_ids") or member_ids)
        observations = [self.repository.get_entity(member_id) for member_id in pool_ids]
        active = [
            item for item in observations if item and item["status"] in ("submitted", "sampled")
        ]
        active_ids = {item["id"] for item in active}
        pool_ids = [member_id for member_id in pool_ids if member_id in active_ids]
        points = []
        for observation in active:
            points.append(
                {
                    "id": observation["id"],
                    "observed_at": observation["data"].get("observed_at"),
                    "lat": observation["data"].get("lat"),
                    "lon": observation["data"].get("lon"),
                }
            )
        verified_ids, centroid, qualifies = recompute_membership(points)
        data = dict(cluster["data"])
        data["_candidate_ids"] = pool_ids
        data["observation_ids"] = verified_ids
        data["qualifies"] = qualifies
        data["centroid"] = list(centroid) if centroid else None

        old_ids = set(member_ids)
        members_changed = set(verified_ids) != old_ids
        old_centroid = cluster["data"].get("centroid")
        coords_changed = bool(centroid) and (
            not old_centroid
            or tuple(round(float(v), 6) for v in old_centroid[:2]) != centroid
        )
        if not members_changed and not coords_changed and cluster["data"].get("qualifies") == qualifies:
            return
        if cluster["status"] == "confirmed":
            next_status = "draft"
            data["_reopen_reason"] = "members or coordinates changed; pending re-confirmation"
        else:
            next_status = cluster["status"]
        updated = self.repository.force_update_entity(cluster["id"], next_status, data)
        self.audit.record(
            cluster["id"],
            actor,
            "recompute_membership",
            cluster["status"],
            next_status,
            {
                "members_changed": members_changed,
                "coords_changed": coords_changed,
                "qualifies": qualifies,
                "observation_ids": verified_ids,
                "centroid": list(centroid) if centroid else None,
            },
        )

    # ------------------------------------------------------------------
    def _record_change(self, entity, op, provenance, baseline_version, applied_version):
        if not provenance:
            return
        self.repository.append_sync_change(
            provenance["device_id"],
            provenance["batch_id"],
            provenance["seq"],
            entity["id"],
            op,
            baseline_version if baseline_version is not None else provenance.get("baseline_version"),
            applied_version,
        )
