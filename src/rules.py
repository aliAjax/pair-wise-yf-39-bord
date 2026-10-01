from datetime import datetime, timedelta

from .domain import (
    ConflictError,
    InvalidTransition,
    PermissionDenied,
    ValidationError,
)


def _validate_observation(actor, data, lookup):
    rows = lookup("observation", "event_id", data.get("event_id")) or [] if lookup else []
    for row in rows:
        if row["data"].get("observed_at") == data.get("observed_at"):
            raise ConflictError("duplicate observation event")
    if not data.get("species"):
        raise ValidationError("species is required")


def _validate_sample(actor, data, lookup):
    observation = _find_one(lookup, "observation", "id", data.get("observation_id"))
    if not observation or observation["status"] not in ("submitted", "sampled"):
        raise ValidationError("sample requires a submitted observation")
    if observation["status"] == "disputed":
        raise ValidationError("observation is awaiting station adjudication")


def _validate_lab_result(actor, entity, data, lookup):
    if data.get("result", "").lower() not in ("positive", "negative"):
        raise ValidationError("lab result must be positive or negative")


# Fields a field device may revise offline. location/lat/lon are one logical
# "place" and are adjudicated together.
REVISE_FIELDS = ("species", "location", "lat", "lon", "observed_at")
PLACE_FIELDS = ("location", "lat", "lon")
ADJUDICATE_FIELDS = ("species",) + PLACE_FIELDS


def _validate_revise(actor, entity, data, lookup):
    if not data:
        raise ValidationError("revise requires at least one field")
    unknown = set(data) - set(REVISE_FIELDS)
    if unknown:
        raise ValidationError("cannot revise field: " + ",".join(sorted(unknown)))
    if "lat" in data and not -90 <= float(data["lat"]) <= 90:
        raise ValidationError("lat out of range")
    if "lon" in data and not -180 <= float(data["lon"]) <= 180:
        raise ValidationError("lon out of range")
    return {}


def _validate_adjudicate(actor, entity, data, lookup):
    """Station picks the winning value among the candidate versions."""
    stored = entity["data"]
    pending_species = stored.get("_species_candidates") or []
    pending_places = stored.get("_location_candidates") or []
    species_provided = data.get("species") is not None
    place_provided = [key for key in PLACE_FIELDS if data.get(key) is not None]
    if not species_provided and not place_provided:
        raise ValidationError("adjudication requires a species or location choice")
    unknown = set(data) - set(ADJUDICATE_FIELDS)
    if unknown:
        raise ValidationError("cannot adjudicate field: " + ",".join(sorted(unknown)))
    if species_provided:
        if not pending_species:
            raise ValidationError("no pending species decision")
        options = [candidate["value"] for candidate in pending_species]
        if data["species"] not in options:
            raise ValidationError("species choice must match a pending candidate")
    if place_provided:
        if not pending_places:
            raise ValidationError("no pending location decision")
        if set(place_provided) != set(PLACE_FIELDS):
            raise ValidationError("location adjudication requires location, lat and lon")
        chosen = (data["location"], data["lat"], data["lon"])
        options = [
            (
                candidate["value"]["location"],
                candidate["value"]["lat"],
                candidate["value"]["lon"],
            )
            for candidate in pending_places
        ]
        if chosen not in options:
            raise ValidationError("location choice must match a pending candidate")
    return {}


def _haversine_km(lat1, lon1, lat2, lon2):
    from math import asin, cos, radians, sin, sqrt
    dlat = radians(lat2 - lat1)
    dlon = radians(lon2 - lon1)
    a = sin(dlat / 2) ** 2 + cos(radians(lat1)) * cos(radians(lat2)) * sin(dlon / 2) ** 2
    return 6371.0 * 2 * asin(sqrt(a))


def is_cluster(observations, max_days=14, radius_km=10):
    if len(observations) < 3:
        return False
    points = observations[:3]
    same_window = all(
        abs(_date_ordinal(points[0].get("observed_at")) - _date_ordinal(item.get("observed_at"))) <= max_days
        for item in points[1:]
    )
    close = all(
        _haversine_km(points[0]["lat"], points[0]["lon"], item["lat"], item["lon"]) <= radius_km
        for item in points[1:]
    )
    return same_window and close


def recompute_membership(observations, max_days=14, radius_km=10):
    """Re-verify a cluster against the current observation versions.

    Every observation is tried as the earliest anchor of a 14-day window;
    within each window the largest subset whose centroid is within the radius
    of every member is kept. Returns (member_ids, centroid, qualifies).
    """
    points = [
        item
        for item in observations
        if item.get("lat") is not None and item.get("lon") is not None and item.get("observed_at")
    ]
    points.sort(key=lambda item: _date_ordinal(item.get("observed_at")))
    if not points:
        return [], None, False

    best = []
    for index, anchor_point in enumerate(points):
        anchor = _date_ordinal(anchor_point.get("observed_at"))
        window = [
            item
            for item in points[index:]
            if _date_ordinal(item.get("observed_at")) <= anchor + max_days
        ]
        members = _fit_centroid(window, radius_km)
        if len(members) > len(best):
            best = members

    if not best:
        return [], None, False
    centroid_lat = sum(item["lat"] for item in best) / len(best)
    centroid_lon = sum(item["lon"] for item in best) / len(best)
    centroid = (round(centroid_lat, 6), round(centroid_lon, 6))
    return [item["id"] for item in best], centroid, len(best) >= 3


def _fit_centroid(points, radius_km):
    """Largest subset of points whose centroid sits within the radius of all.

    Candidates are removed one at a time (the point farthest from the current
    centroid), but each fit restarts from the full candidate set so a point
    removed in an earlier configuration can be admitted again later.
    """
    excluded = set()
    while len(excluded) < len(points):
        members = [item for item in points if item["id"] not in excluded]
        if not members:
            return []
        centroid_lat = sum(item["lat"] for item in members) / len(members)
        centroid_lon = sum(item["lon"] for item in members) / len(members)
        distances = [
            (
                _haversine_km(centroid_lat, centroid_lon, item["lat"], item["lon"]),
                item,
            )
            for item in members
        ]
        farthest_distance, farthest = max(distances, key=lambda pair: pair[0])
        if farthest_distance <= radius_km:
            return members
        excluded.add(farthest["id"])
    return []


CUSTOM_CREATE = {'observation': _validate_observation, 'sample': _validate_sample}
CUSTOM_TRANSITIONS = {
    ('sample', 'lab_result'): _validate_lab_result,
    ('observation', 'revise'): _validate_revise,
    ('observation', 'adjudicate'): _validate_adjudicate,
}


class RuleEngine:
    ALIASES = {'observations': 'observation', 'samples': 'sample', 'clusters': 'cluster'}
    INITIAL_STATUS = {'observation': 'captured', 'sample': 'collected', 'cluster': 'draft'}
    TRANSITIONS = {'observation': {'submit': (('captured',), 'submitted'), 'reject': (('submitted', 'disputed'), 'rejected'), 'link_sample': (('submitted',), 'sampled'), 'revise': (('captured', 'submitted', 'sampled', 'disputed'), None), 'adjudicate': (('disputed',), 'disputed')}, 'sample': {'send_lab': (('collected',), 'in_lab'), 'lab_result': (('in_lab',), 'resulted'), 'retest': (('resulted',), 'in_lab'), 'close': (('resulted',), 'closed')}, 'cluster': {'confirm_cluster': (('draft',), 'confirmed'), 'dismiss': (('draft',), 'dismissed')}}
    CREATE_REQUIRED = {'observation': ('event_id', 'species', 'location', 'observed_at', 'lat', 'lon'), 'sample': ('observation_id', 'sample_code'), 'cluster': ('region',)}
    ACTION_REQUIRED = {('observation', 'submit'): ('location', 'observed_at'), ('observation', 'reject'): ('reason',), ('observation', 'link_sample'): ('sample_id',), ('sample', 'send_lab'): ('lab_id',), ('sample', 'lab_result'): ('result', 'result_at'), ('sample', 'retest'): ('reason',), ('sample', 'close'): ('outcome',), ('cluster', 'confirm_cluster'): ('observation_ids', 'centroid'), ('cluster', 'dismiss'): ('reason',)}
    CREATE_ROLES = {'observation': ('admin', 'field'), 'sample': ('admin', 'field'), 'cluster': ('admin', 'epidemiologist')}
    ROLE_ACTIONS = {'submit': ('admin', 'field'), 'reject': ('admin', 'epidemiologist'), 'link_sample': ('admin', 'field'), 'revise': ('admin', 'field'), 'adjudicate': ('admin', 'epidemiologist'), 'send_lab': ('admin', 'field'), 'lab_result': ('admin', 'lab'), 'retest': ('admin', 'lab'), 'close': ('admin', 'epidemiologist'), 'confirm_cluster': ('admin', 'epidemiologist'), 'dismiss': ('admin', 'epidemiologist')}

    def normalize_kind(self, kind):
        return self.ALIASES.get(kind, kind)

    def initial_status(self, kind):
        kind = self.normalize_kind(kind)
        if kind not in self.INITIAL_STATUS:
            raise ValidationError("unknown kind: " + str(kind))
        return self.INITIAL_STATUS[kind]

    @staticmethod
    def _ensure_role(actor, allowed):
        if "*" not in allowed and actor.role not in allowed:
            raise PermissionDenied("role %s is not allowed here" % actor.role)

    @staticmethod
    def _require(data, fields):
        for field in fields:
            value = data.get(field)
            if value is None or value == "" or value == [] or value == {}:
                raise ValidationError("missing required field: " + field)

    def validate_create(self, actor, kind, data, lookup=None):
        kind = self.normalize_kind(kind)
        if kind not in self.INITIAL_STATUS:
            raise ValidationError("unknown kind: " + str(kind))
        self._ensure_role(actor, self.CREATE_ROLES.get(kind, ("admin",)))
        self._require(data, self.CREATE_REQUIRED.get(kind, ()))
        custom = CUSTOM_CREATE.get(kind)
        if custom:
            custom(actor, data, lookup)
        return dict(data)

    def validate_business_create(self, kind, data, lookup=None):
        """Business rules without the actor role check (system re-verification)."""
        kind = self.normalize_kind(kind)
        if kind not in self.INITIAL_STATUS:
            raise ValidationError("unknown kind: " + str(kind))
        self._require(data, self.CREATE_REQUIRED.get(kind, ()))
        custom = CUSTOM_CREATE.get(kind)
        if custom:
            custom(None, data, lookup)
        return dict(data)

    def validate_transition(self, actor, entity, action, data, lookup=None):
        kind = self.normalize_kind(entity["kind"])
        transition = self.TRANSITIONS.get(kind, {}).get(action)
        if not transition:
            raise InvalidTransition("unknown action %s for %s" % (action, kind))
        allowed_statuses, next_status = transition
        if entity["status"] not in allowed_statuses:
            raise InvalidTransition(
                "cannot %s from status %s" % (action, entity["status"])
            )
        allowed_roles = self.ROLE_ACTIONS.get(
            (kind, action), self.ROLE_ACTIONS.get(action, ("admin",))
        )
        self._ensure_role(actor, allowed_roles)
        self._require(data, self.ACTION_REQUIRED.get((kind, action), ()))
        custom = CUSTOM_TRANSITIONS.get((kind, action))
        extra = custom(actor, entity, data, lookup) if custom else {}
        patch = dict(data)
        if extra:
            patch.update(extra)
        if next_status is None:
            # Same-state revision (observation.revise keeps the lifecycle status).
            next_status = entity["status"]
        return next_status, patch


def _find_one(lookup, kind, field, value):
    if lookup is None:
        return None
    rows = lookup(kind, field, value) or []
    return rows[0] if rows else None


def _date_ordinal(value):
    return datetime.fromisoformat(str(value)[:10]).date().toordinal()
