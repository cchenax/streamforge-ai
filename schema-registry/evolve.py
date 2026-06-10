"""
CompatibilityChecker — offline Avro schema compatibility analysis.

Checks whether a proposed schema change is compatible with an existing schema
according to the specified compatibility level, **without** connecting to any
registry server.  Useful in CI/CD pipelines and pre-flight validation.

Compatibility levels (mirrors Confluent Schema Registry semantics)
------------------------------------------------------------------
    BACKWARD            New schema can read data written with the old schema.
                        Safe to deploy consumers before producers.
    BACKWARD_TRANSITIVE New schema is BACKWARD-compatible with ALL previous versions.
    FORWARD             Old schema can read data written with the new schema.
                        Safe to deploy producers before consumers.
    FORWARD_TRANSITIVE  Old schema is FORWARD-compatible with ALL previous versions.
    FULL                Both BACKWARD and FORWARD.
    FULL_TRANSITIVE     Both BACKWARD and FORWARD with ALL previous versions.
    NONE                No compatibility check.

Usage
-----
    from schema_registry.evolve import CompatibilityChecker, EvolutionReport

    report = CompatibilityChecker.analyze(new_schema_str, old_schema_str)
    print(report.is_backward)   # True — new can read old data
    print(report.changes)       # list of FieldChange

    # Quick boolean check
    ok = CompatibilityChecker.is_compatible(new_schema_str, old_schema_str, "FULL")
"""
from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Dict, List, Optional
import json


# ---------------------------------------------------------------------------
# Value types
# ---------------------------------------------------------------------------

class FieldChangeKind(str, Enum):
    FIELD_ADDED         = "field_added"          # new field with default → FULL
    FIELD_ADDED_NO_DEF  = "field_added_no_default"  # new required field → BACKWARD-breaking
    FIELD_REMOVED       = "field_removed"        # field dropped → FORWARD-breaking
    TYPE_WIDENED        = "type_widened"          # int→long, float→double → BACKWARD
    TYPE_CHANGED        = "type_changed"          # incompatible type change → BREAKING
    DEFAULT_CHANGED     = "default_changed"       # default value changed (non-breaking)
    DOC_CHANGED         = "doc_changed"           # doc-only change (non-breaking)
    NO_CHANGE           = "no_change"


@dataclass
class FieldChange:
    """Describes a single detected difference between two Avro schemas."""
    field_name: str
    kind: FieldChangeKind
    old_type: Optional[str] = None
    new_type: Optional[str] = None
    detail: str = ""

    @property
    def is_backward_breaking(self) -> bool:
        """True if this change breaks BACKWARD compatibility."""
        return self.kind == FieldChangeKind.FIELD_ADDED_NO_DEF

    @property
    def is_forward_breaking(self) -> bool:
        """True if this change breaks FORWARD compatibility."""
        return self.kind == FieldChangeKind.FIELD_REMOVED

    @property
    def is_breaking(self) -> bool:
        return self.kind == FieldChangeKind.TYPE_CHANGED


@dataclass
class EvolutionReport:
    """
    Full compatibility analysis between two Avro record schemas.

    Attributes
    ----------
    changes         : All detected field-level differences.
    is_backward     : New schema can read data written with the old schema.
    is_forward      : Old schema can read data written with the new schema.
    is_full         : Both backward and forward.
    breaking_fields : Fields that cause hard breaks.
    """
    changes: List[FieldChange] = field(default_factory=list)

    @property
    def is_backward(self) -> bool:
        return not any(c.is_backward_breaking or c.is_breaking for c in self.changes)

    @property
    def is_forward(self) -> bool:
        return not any(c.is_forward_breaking or c.is_breaking for c in self.changes)

    @property
    def is_full(self) -> bool:
        return self.is_backward and self.is_forward

    @property
    def breaking_fields(self) -> List[FieldChange]:
        return [c for c in self.changes if c.is_breaking or c.is_backward_breaking or c.is_forward_breaking]

    def summary(self) -> str:
        parts = []
        if self.is_full:
            parts.append("FULL")
        elif self.is_backward:
            parts.append("BACKWARD")
        elif self.is_forward:
            parts.append("FORWARD")
        else:
            parts.append("BREAKING")
        if self.changes:
            kinds = ", ".join(sorted({c.kind.value for c in self.changes}))
            parts.append(f"({len(self.changes)} changes: {kinds})")
        return " ".join(parts)

    def __repr__(self) -> str:
        return f"EvolutionReport({self.summary()})"


# ---------------------------------------------------------------------------
# CompatibilityChecker
# ---------------------------------------------------------------------------

_NUMERIC_WIDENING = {
    ("int",    "long"),
    ("int",    "float"),
    ("int",    "double"),
    ("long",   "float"),
    ("long",   "double"),
    ("float",  "double"),
}


class CompatibilityChecker:
    """
    Stateless utility for offline Avro schema compatibility analysis.

    All methods are classmethods — no instantiation needed.
    """

    @classmethod
    def analyze(cls, new_schema_str: str, old_schema_str: str) -> EvolutionReport:
        """
        Compare *new_schema_str* against *old_schema_str* and return a full
        :class:`EvolutionReport`.

        Both schemas must be JSON strings of Avro ``record`` type.
        Primitive schemas (``"string"``, ``"int"`` etc.) are compared directly.

        Raises
        ------
        ValueError  if either schema string is not valid JSON.
        """
        new_def = json.loads(new_schema_str)
        old_def = json.loads(old_schema_str)
        changes: List[FieldChange] = []
        cls._compare(new_def, old_def, changes, prefix="")
        return EvolutionReport(changes=changes)

    @classmethod
    def is_compatible(
        cls,
        new_schema_str: str,
        old_schema_str: str,
        level: str = "BACKWARD",
    ) -> bool:
        """
        Return ``True`` if *new_schema_str* satisfies *level* relative to
        *old_schema_str*.

        Parameters
        ----------
        level : ``"BACKWARD"``, ``"FORWARD"``, ``"FULL"``, ``"NONE"``, or
                the ``_TRANSITIVE`` variants (treated identically here since
                only two versions are compared).
        """
        level = level.upper().replace("_TRANSITIVE", "")
        if level == "NONE":
            return True
        report = cls.analyze(new_schema_str, old_schema_str)
        if level == "BACKWARD":
            return report.is_backward
        if level == "FORWARD":
            return report.is_forward
        if level == "FULL":
            return report.is_full
        # Unknown level — be conservative
        return report.is_full

    # ------------------------------------------------------------------
    # Internal comparison helpers
    # ------------------------------------------------------------------

    @classmethod
    def _compare(
        cls,
        new_def: Any,
        old_def: Any,
        changes: List[FieldChange],
        prefix: str,
    ) -> None:
        """Recursively compare two schema definitions."""
        # Both are record objects
        if isinstance(new_def, dict) and isinstance(old_def, dict):
            if new_def.get("type") == "record" and old_def.get("type") == "record":
                cls._compare_records(new_def, old_def, changes, prefix)
                return
            # Compare union / type field for non-record objects
            cls._compare_type_annotations(
                new_def.get("type"), old_def.get("type"), changes, prefix
            )
            return

        # Both are unions (list)
        if isinstance(new_def, list) and isinstance(old_def, list):
            cls._compare_unions(new_def, old_def, changes, prefix)
            return

        # Scalar type names
        if isinstance(new_def, str) and isinstance(old_def, str):
            cls._compare_scalar_types(new_def, old_def, changes, prefix)
            return

        # Mixed (one is dict/list, other is str) — type change
        if new_def != old_def:
            changes.append(FieldChange(
                field_name=prefix or "root",
                kind=FieldChangeKind.TYPE_CHANGED,
                old_type=_type_label(old_def),
                new_type=_type_label(new_def),
                detail="structural type mismatch",
            ))

    @classmethod
    def _compare_records(
        cls,
        new_rec: dict,
        old_rec: dict,
        changes: List[FieldChange],
        prefix: str,
    ) -> None:
        old_fields: Dict[str, dict] = {f["name"]: f for f in old_rec.get("fields", [])}
        new_fields: Dict[str, dict] = {f["name"]: f for f in new_rec.get("fields", [])}

        # Check fields present in old schema
        for fname, old_f in old_fields.items():
            fpath = f"{prefix}.{fname}" if prefix else fname
            if fname not in new_fields:
                changes.append(FieldChange(
                    field_name=fpath,
                    kind=FieldChangeKind.FIELD_REMOVED,
                    old_type=_type_label(old_f.get("type")),
                    detail="field removed from new schema",
                ))
            else:
                new_f = new_fields[fname]
                cls._compare_field(new_f, old_f, changes, fpath)

        # Check fields added in new schema
        for fname, new_f in new_fields.items():
            fpath = f"{prefix}.{fname}" if prefix else fname
            if fname not in old_fields:
                has_default = "default" in new_f
                nullable = _is_nullable(new_f.get("type"))
                if has_default or nullable:
                    changes.append(FieldChange(
                        field_name=fpath,
                        kind=FieldChangeKind.FIELD_ADDED,
                        new_type=_type_label(new_f.get("type")),
                        detail="new field with default/nullable — backward-compatible",
                    ))
                else:
                    changes.append(FieldChange(
                        field_name=fpath,
                        kind=FieldChangeKind.FIELD_ADDED_NO_DEF,
                        new_type=_type_label(new_f.get("type")),
                        detail="new required field without default — breaks backward compat",
                    ))

    @classmethod
    def _compare_field(
        cls,
        new_f: dict,
        old_f: dict,
        changes: List[FieldChange],
        fpath: str,
    ) -> None:
        cls._compare(new_f.get("type"), old_f.get("type"), changes, fpath)

        # Default changed (non-breaking meta change)
        new_default = new_f.get("default", _SENTINEL)
        old_default = old_f.get("default", _SENTINEL)
        if new_default is not _SENTINEL and old_default is not _SENTINEL:
            if new_default != old_default:
                changes.append(FieldChange(
                    field_name=fpath,
                    kind=FieldChangeKind.DEFAULT_CHANGED,
                    detail=f"{old_default!r} → {new_default!r}",
                ))

    @classmethod
    def _compare_unions(
        cls,
        new_union: list,
        old_union: list,
        changes: List[FieldChange],
        prefix: str,
    ) -> None:
        new_set = {_type_label(t) for t in new_union}
        old_set = {_type_label(t) for t in old_union}
        added   = new_set - old_set
        removed = old_set - new_set
        if added:
            changes.append(FieldChange(
                field_name=prefix,
                kind=FieldChangeKind.FIELD_ADDED,
                detail=f"union expanded with: {added}",
            ))
        if removed:
            changes.append(FieldChange(
                field_name=prefix,
                kind=FieldChangeKind.FIELD_REMOVED,
                detail=f"union shrunk, removed: {removed}",
            ))

    @classmethod
    def _compare_scalar_types(
        cls,
        new_t: str,
        old_t: str,
        changes: List[FieldChange],
        prefix: str,
    ) -> None:
        if new_t == old_t:
            return
        if (old_t, new_t) in _NUMERIC_WIDENING:
            changes.append(FieldChange(
                field_name=prefix,
                kind=FieldChangeKind.TYPE_WIDENED,
                old_type=old_t,
                new_type=new_t,
                detail=f"numeric widening {old_t}→{new_t} — backward-compatible",
            ))
        else:
            changes.append(FieldChange(
                field_name=prefix,
                kind=FieldChangeKind.TYPE_CHANGED,
                old_type=old_t,
                new_type=new_t,
                detail=f"incompatible type change {old_t}→{new_t}",
            ))

    @classmethod
    def _compare_type_annotations(
        cls,
        new_t: Any,
        old_t: Any,
        changes: List[FieldChange],
        prefix: str,
    ) -> None:
        if new_t != old_t:
            changes.append(FieldChange(
                field_name=prefix,
                kind=FieldChangeKind.TYPE_CHANGED,
                old_type=_type_label(old_t),
                new_type=_type_label(new_t),
            ))


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------

_SENTINEL = object()


def _type_label(t: Any) -> str:
    if isinstance(t, str):
        return t
    if isinstance(t, list):
        return f"union[{', '.join(_type_label(x) for x in t)}]"
    if isinstance(t, dict):
        return t.get("name", t.get("type", "?"))
    return str(t)


def _is_nullable(t: Any) -> bool:
    """Return True if the type accepts null (i.e. is a union containing 'null')."""
    if isinstance(t, list):
        return any(x == "null" for x in t)
    return t == "null"
