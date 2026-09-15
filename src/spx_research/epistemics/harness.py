"""Packet compiler and proposal validator for the temporal knowledge boundary.

Ported from spx_ai_handover_v2/reference/temporal_harness.py. The compiler
builds the blinded model-visible packet from delivered, available, in-scope
evidence only; the validator checks a structured proposal against that packet
and emits a private structural witness (never proof of model ignorance).
"""

from __future__ import annotations

import hashlib
import hmac
import json
from collections.abc import Mapping, Sequence
from datetime import datetime
from decimal import Decimal, InvalidOperation
from typing import Any

from spx_research.epistemics.types import (
    ACTIONS,
    CONFIDENCE,
    KINDS,
    REASONS,
    TOPICS,
    UNKNOWNS,
    VOCAB,
    Atom,
    Compiled,
    Context,
    Delivery,
    HarnessError,
    MenuChoice,
    aware_check,
)


def canonical(value: Any) -> bytes:
    return json.dumps(
        value, sort_keys=True, separators=(",", ":"), ensure_ascii=True, allow_nan=False
    ).encode("utf-8")


def digest(value: Any) -> str:
    return hashlib.sha256(canonical(value)).hexdigest()


def aware(t: datetime) -> None:
    aware_check(t)


class Harness:
    def __init__(self, alias_secret: bytes) -> None:
        if len(alias_secret) < 16:
            raise HarnessError("ALIAS_KEY_TOO_SHORT")
        self._key = alias_secret

    def token(self, namespace: str, kind: str, value: Any) -> str:
        message = canonical([namespace, kind, value])
        return kind + "_" + hmac.new(self._key, message, hashlib.sha256).hexdigest()[:24]

    def check_atom(
        self, atom_id: str, atoms: Mapping[str, Atom], ctx: Context, path: tuple[str, ...] = ()
    ) -> None:
        if atom_id in path:
            raise HarnessError("DEPENDENCY_CYCLE")
        if atom_id not in atoms:
            raise HarnessError("MISSING_DEPENDENCY")
        a = atoms[atom_id]
        for t in (a.published_at, a.available_at, a.subject_at, ctx.as_of):
            aware(t)
        if a.kind not in KINDS or not a.source_checked:
            raise HarnessError("UNVERIFIED_SOURCE")
        if a.published_at > a.available_at:
            raise HarnessError("BACKDATED_AVAILABILITY")
        if a.available_at > ctx.as_of:
            raise HarnessError("FUTURE_DEPENDENCY")
        if "PUBLIC" not in a.recipients and ctx.actor_id not in a.recipients:
            raise HarnessError("WRONG_RECIPIENT")
        if a.kind == "OBSERVATION" and a.subject_at > ctx.as_of:
            raise HarnessError("FUTURE_REALIZATION")
        if a.metric not in VOCAB:
            raise HarnessError("UNAPPROVED_VOCABULARY")
        unit, allowed = VOCAB[a.metric]
        if a.unit != unit:
            raise HarnessError("UNAPPROVED_VOCABULARY")
        if not isinstance(a.value, str):
            raise HarnessError("INVALID_VALUE")
        if allowed is not None:
            if a.value not in allowed:
                raise HarnessError("UNAPPROVED_TEXT")
        else:
            try:
                if not Decimal(a.value).is_finite():
                    raise HarnessError("NONFINITE_VALUE")
            except InvalidOperation as exc:
                raise HarnessError("UNAPPROVED_TEXT") from exc
        for p in a.dependencies:
            self.check_atom(p, atoms, ctx, (*path, atom_id))
            if atoms[p].available_at > a.available_at:
                raise HarnessError("BACKDATED_DERIVATION")
        if a.kind == "DERIVED":
            if a.transform not in {"SUBTRACT", "SUM", "COPY"} or not a.dependencies:
                raise HarnessError("UNREGISTERED_DERIVATION")
            parents = [atoms[p] for p in a.dependencies]
            if any(p.unit != a.unit for p in parents):
                raise HarnessError("UNIT_MISMATCH")
            try:
                values = [Decimal(p.value) for p in parents]
                if a.transform == "SUBTRACT" and len(values) == 2:
                    expected = values[0] - values[1]
                elif a.transform == "SUM":
                    expected = sum(values, Decimal(0))
                elif a.transform == "COPY" and len(values) == 1:
                    expected = values[0]
                else:
                    raise HarnessError("INVALID_DERIVATION_ARITY")
                if Decimal(a.value) != expected:
                    raise HarnessError("WRONG_DERIVED_VALUE")
            except InvalidOperation as exc:
                raise HarnessError("NONNUMERIC_DERIVATION") from exc
        elif a.transform is not None or a.dependencies:
            raise HarnessError("UNDECLARED_DERIVATION")

    def compile(
        self,
        atoms: Mapping[str, Atom],
        deliveries: Sequence[Delivery],
        ctx: Context,
        menu: Sequence[MenuChoice],
        assessments: Sequence[Any] = (),
        unknowns: Sequence[str] = (
            "UNKNOWN_FUTURE_POLICY_PATH",
            "UNKNOWN_FUTURE_PRICE_PATH",
        ),
        premise_ids: set[str] | None = None,
    ) -> Compiled:
        aware(ctx.as_of)
        if ctx.actor_role not in ACTIONS:
            raise HarnessError("INVALID_ROLE")
        if ctx.session_index < 0 or not 0 <= ctx.minute_from_open <= 1440:
            raise HarnessError("INVALID_RELATIVE_CLOCK")
        observed: set[str] = set()
        for d in deliveries:
            aware(d.delivered_at)
            if (d.run_id, d.branch_id, d.actor_id) != (ctx.run_id, ctx.branch_id, ctx.actor_id):
                continue
            if d.delivered_at > ctx.as_of:
                continue
            if d.atom_id not in atoms:
                raise HarnessError("MISSING_OBSERVATION")
            a = atoms[d.atom_id]
            aware(a.available_at)
            if d.delivered_at < a.available_at:
                raise HarnessError("DELIVERY_BEFORE_AVAILABILITY")
            self.check_atom(a.atom_id, atoms, ctx)
            observed.add(a.atom_id)
        premise_map: dict[str, Atom] = {}
        premises = []
        for atom_id in sorted(observed):
            a = atoms[atom_id]
            token = self.token(ctx.alias_namespace, "ev", atom_id)
            premise_map[token] = a
            if premise_ids is not None and atom_id not in premise_ids:
                continue
            premises.append(
                {
                    "token": token,
                    "metric": a.metric,
                    "value": a.value,
                    "unit": a.unit,
                    "kind": a.kind,
                    "age_minutes": int((ctx.as_of - a.available_at).total_seconds() // 60),
                }
            )
        action_map: dict[str, MenuChoice] = {}
        public_menu = []
        for choice in sorted(menu, key=lambda x: x.internal_id):
            if choice.kind not in ACTIONS[ctx.actor_role]:
                raise HarnessError("ACTION_WRONG_ROLE")
            if not choice.required_atoms or not set(choice.required_atoms) <= observed:
                raise HarnessError("MENU_UNKNOWN_PREMISE")
            token = self.token(ctx.alias_namespace, "act", choice.internal_id)
            if token in action_map:
                raise HarnessError("DUPLICATE_ACTION")
            action_map[token] = choice
            target_token = (
                self.token(ctx.alias_namespace, "target", choice.target_internal_id)
                if choice.target_internal_id
                else None
            )
            limit_token = (
                self.token(ctx.alias_namespace, "limit", choice.limit_internal_id)
                if choice.limit_internal_id
                else None
            )
            public_menu.append(
                {
                    "action_id": token,
                    "kind": choice.kind,
                    "target_token": target_token,
                    "limit_option_token": limit_token,
                    "attributes": [],
                    "required_premise_tokens": [
                        self.token(ctx.alias_namespace, "ev", x) for x in choice.required_atoms
                    ],
                }
            )
        if not public_menu:
            raise HarnessError("EMPTY_ACTION_MENU")
        public_assessments = []
        for rec in assessments:
            premise_tokens = [
                self.token(ctx.alias_namespace, "ev", aid) for aid in rec.premise_atom_ids
            ]
            public_assessments.append(
                {
                    "token": self.token(ctx.alias_namespace, "as", rec.decision_token),
                    "topic": rec.topic,
                    "assessment": rec.assessment,
                    "confidence_label": rec.confidence_label,
                    "premise_tokens": premise_tokens,
                }
            )
        for u in unknowns:
            if u not in UNKNOWNS:
                raise HarnessError("UNAPPROVED_TEXT")
        packet = {
            "schema_version": "2.0",
            "actor_role": ctx.actor_role,
            "episode_token": self.token(ctx.alias_namespace, "ep", "episode"),
            "decision_token": self.token(
                ctx.alias_namespace,
                "dec",
                [ctx.actor_id, ctx.session_index, ctx.minute_from_open],
            ),
            "packet_token": "",
            "prior_belief_token": self.token(
                ctx.alias_namespace,
                "bel",
                ctx.prior_visible_belief_hash,
            ),
            "clock": {
                "session_index": ctx.session_index,
                "minute_from_open": ctx.minute_from_open,
            },
            "premises": premises,
            "assessments": public_assessments,
            "unknowns": sorted(set(unknowns)),
            "action_menu": public_menu,
        }
        packet["packet_token"] = "pkt_" + digest(packet)[:24]
        return Compiled(packet, ctx, premise_map, action_map)

    def validate(self, proposal: Mapping[str, Any], compiled: Compiled) -> dict[str, Any]:
        keys = {
            "schema_version",
            "actor_role",
            "decision_token",
            "packet_token",
            "prior_belief_token",
            "action_id",
            "premise_tokens",
            "assessment_updates",
            "reason_codes",
            "uncertainty_codes",
            "confidence_label",
        }
        if set(proposal) != keys:
            raise HarnessError("OUTPUT_KEYS")
        for k in (
            "schema_version",
            "actor_role",
            "decision_token",
            "packet_token",
            "prior_belief_token",
        ):
            if proposal[k] != compiled.public[k]:
                raise HarnessError("STALE_OR_WRONG_CONTEXT")
        action = proposal["action_id"]
        if not isinstance(action, str) or action not in compiled.action_map:
            raise HarnessError("UNKNOWN_ACTION")

        def strings(v: Any, allowed: set[str]) -> set[str]:
            if not isinstance(v, list) or not all(isinstance(x, str) for x in v):
                raise HarnessError("INVALID_ARRAY")
            if len(set(v)) != len(v) or not set(v) <= allowed:
                raise HarnessError("UNKNOWN_OR_DUPLICATE_TOKEN")
            return set(v)

        premises = strings(proposal["premise_tokens"], set(compiled.premise_map))
        selected = next(x for x in compiled.public["action_menu"] if x["action_id"] == action)
        if not set(selected["required_premise_tokens"]) <= premises:
            raise HarnessError("REQUIRED_PREMISE_MISSING")
        reasons = strings(proposal["reason_codes"], REASONS)
        unknowns = strings(proposal["uncertainty_codes"], UNKNOWNS)
        if not reasons or not unknowns or ("NONE_IDENTIFIED" in unknowns and len(unknowns) > 1):
            raise HarnessError("INVALID_CODES")
        if proposal["confidence_label"] not in CONFIDENCE:
            raise HarnessError("INVALID_CONFIDENCE")
        if not isinstance(proposal["assessment_updates"], list):
            raise HarnessError("INVALID_ASSESSMENTS")
        seen_topics: set[str] = set()
        for update in proposal["assessment_updates"]:
            update_keys = {"topic", "assessment", "premise_tokens", "confidence_label"}
            if not isinstance(update, dict) or set(update) != update_keys:
                raise HarnessError("ASSESSMENT_KEYS")
            topic = update["topic"]
            if not isinstance(topic, str) or topic not in TOPICS or topic in seen_topics:
                raise HarnessError("INVALID_ASSESSMENT_TOPIC")
            seen_topics.add(topic)
            if (
                update["assessment"] not in TOPICS[topic]
                or update["confidence_label"] not in CONFIDENCE
            ):
                raise HarnessError("INVALID_ASSESSMENT_VALUE")
            refs = strings(update["premise_tokens"], set(compiled.premise_map))
            if not refs:
                raise HarnessError("UNSUPPORTED_ASSESSMENT")
            premises |= refs
        # This witness checks only the reference's structural subset, not meaning/optimality.
        accepted_atoms = [compiled.premise_map[p] for p in sorted(premises)]
        ctx = compiled.context
        return {
            "schema_version": "2.0",
            "private_decision_id": proposal["decision_token"],
            "run_id": ctx.run_id,
            "branch_id": ctx.branch_id,
            "actor_id": ctx.actor_id,
            "as_of_utc": ctx.as_of.isoformat(),
            "packet_hash": digest(compiled.public),
            "prior_belief_hash": ctx.prior_visible_belief_hash,
            "proposal_hash": digest(dict(proposal)),
            "accepted_premise_ids": [a.atom_id for a in accepted_atoms],
            "max_evidence_available_at_utc": max(
                a.available_at for a in accepted_atoms
            ).isoformat(),
            "checks": [{"name": "reference_structural_subset", "status": "PASS"}],
            "status": "ACCEPTED",
            "rejection_codes": [],
            "validator_version": "reference-0.1",
            "parametric_ignorance_proven": False,
        }
