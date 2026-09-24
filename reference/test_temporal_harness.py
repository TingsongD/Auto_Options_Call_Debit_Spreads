"""Synthetic boundary examples. Does not test a real model or financial engine."""
from dataclasses import replace
from datetime import datetime, timedelta, timezone
import copy
import unittest
from temporal_harness import Atom, Context, Delivery, MenuChoice, Harness, HarnessError, canonical

T = datetime(2019, 1, 2, 15, 0, tzinfo=timezone.utc)

class HarnessReferenceTests(unittest.TestCase):
    def setUp(self):
        self.h = Harness(b"synthetic-alias-secret-32bytes")
        self.ctx = Context("private-2019-run", "branch-one", "worker", "SPREAD", T,
                           12, 30, "opaque-episode-seed", "whole-archive-v1")
        self.rule = Atom("rule", "advisory_rule", "DISCRETIONARY", "policy", "APPROVED_RULE",
                         T-timedelta(days=2),T-timedelta(days=2),T-timedelta(days=2))
        self.rate = Atom("rate", "policy_rate_bps", "300", "basis_points", "OBSERVATION",
                         T-timedelta(minutes=15),T-timedelta(minutes=15),T-timedelta(minutes=15))
        self.atoms={"rule":self.rule,"rate":self.rate}
        self.deliveries=[self.delivery("rule"),self.delivery("rate")]
        self.menu=[MenuChoice("hold-own-position","HOLD",("rule","rate"),"SPXW-PRIVATE-20190102")]

    def delivery(self, atom_id, **kwargs):
        return replace(Delivery(self.ctx.run_id,self.ctx.branch_id,self.ctx.actor_id,atom_id,T),**kwargs)

    def compile(self, atoms=None, deliveries=None, ctx=None, menu=None):
        return self.h.compile(self.atoms if atoms is None else atoms,
            self.deliveries if deliveries is None else deliveries,
            self.ctx if ctx is None else ctx,self.menu if menu is None else menu)

    def proposal(self, compiled):
        p=compiled.public
        return {"schema_version":"2.0","actor_role":p["actor_role"],
                "decision_token":p["decision_token"],"packet_token":p["packet_token"],
                "prior_belief_token":p["prior_belief_token"],
                "action_id":p["action_menu"][0]["action_id"],
                "premise_tokens":p["action_menu"][0]["required_premise_tokens"],
                "assessment_updates":[],"reason_codes":["MAINTAIN_THESIS"],
                "uncertainty_codes":["UNKNOWN_FUTURE_POLICY_PATH"],"confidence_label":"LOW"}

    def assert_code(self, code, fn):
        with self.assertRaisesRegex(HarnessError,"^"+code+"$"):
            fn()

    def test_future_suffix_invariant(self):
        future=replace(self.rate,atom_id="future",value="999",published_at=T+timedelta(days=2),
                       available_at=T+timedelta(days=2),subject_at=T+timedelta(days=2))
        a=dict(self.atoms,future=future)
        d=self.deliveries+[self.delivery("future",delivered_at=T+timedelta(days=2))]
        first=self.compile(a,d).public
        a["future"]=replace(future,value="1")
        second=self.compile(a,d).public
        self.assertEqual(canonical(first),canonical(second))

    def test_future_record_insertion_does_not_change_packet(self):
        future=replace(self.rate,atom_id="inserted",available_at=T+timedelta(seconds=1),
                       published_at=T+timedelta(seconds=1))
        self.assertEqual(self.compile().public,self.compile(dict(self.atoms,inserted=future)).public)

    def test_visible_change_positive_control(self):
        self.assertNotEqual(self.compile().public,self.compile(dict(self.atoms,rate=replace(self.rate,value="350"))).public)

    def test_hidden_manifest_change_invariant(self):
        self.assertEqual(self.compile().public,self.compile(ctx=replace(self.ctx,private_manifest_id="different-future-v2")).public)

    def test_direct_calendar_and_contract_ids_absent(self):
        raw=canonical(self.compile().public).decode()
        for text in ("2019-01-02","20190102","SPXW","private-2019-run","whole-archive-v1"):
            self.assertNotIn(text,raw)

    def test_known_future_schedule_permitted(self):
        a=replace(self.rate,atom_id="schedule",kind="SCHEDULE",metric="minutes_to_meeting",
                  value="45",unit="minutes",subject_at=T+timedelta(minutes=45))
        c=self.compile(dict(self.atoms,schedule=a),self.deliveries+[self.delivery("schedule")])
        self.assertIn("SCHEDULE",[x["kind"] for x in c.public["premises"]])

    def test_published_forecast_permitted(self):
        a=replace(self.rate,atom_id="forecast",kind="SOURCE_FORECAST",metric="expected_rate_bps",
                  subject_at=T+timedelta(days=45))
        c=self.compile(dict(self.atoms,forecast=a),self.deliveries+[self.delivery("forecast")])
        self.assertIn("SOURCE_FORECAST",[x["kind"] for x in c.public["premises"]])

    def test_future_realization_rejected(self):
        bad=replace(self.rate,subject_at=T+timedelta(days=1))
        self.assert_code("FUTURE_REALIZATION",lambda:self.compile(dict(self.atoms,rate=bad)))

    def test_backdated_availability_rejected(self):
        bad=replace(self.rate,published_at=T+timedelta(seconds=1))
        self.assert_code("BACKDATED_AVAILABILITY",lambda:self.compile(dict(self.atoms,rate=bad)))

    def test_delivery_before_availability_rejected(self):
        d=[self.delivery("rule"),self.delivery("rate",delivered_at=T-timedelta(hours=1))]
        self.assert_code("DELIVERY_BEFORE_AVAILABILITY",lambda:self.compile(deliveries=d))

    def test_delivery_recorded_for_not_yet_available_atom_rejected(self):
        future=replace(self.rate,atom_id="fut",value="999",
                       published_at=T+timedelta(days=1),available_at=T+timedelta(days=1),
                       subject_at=T+timedelta(days=1))
        d=self.deliveries+[self.delivery("fut")]
        self.assert_code("DELIVERY_BEFORE_AVAILABILITY",
                         lambda:self.compile(dict(self.atoms,fut=future),d))

    def test_later_actor_delivery_not_observed(self):
        d=[self.delivery("rule"),self.delivery("rate",delivered_at=T+timedelta(minutes=15))]
        m=[replace(self.menu[0],required_atoms=("rule",))]
        self.assertEqual(len(self.compile(deliveries=d,menu=m).public["premises"]),1)

    def test_other_run_delivery_not_observed(self):
        d=[self.delivery("rule"),self.delivery("rate",run_id="other-run")]
        m=[replace(self.menu[0],required_atoms=("rule",))]
        self.assertEqual(len(self.compile(deliveries=d,menu=m).public["premises"]),1)

    def test_other_branch_delivery_not_observed(self):
        d=[self.delivery("rule"),self.delivery("rate",branch_id="future-branch")]
        m=[replace(self.menu[0],required_atoms=("rule",))]
        self.assertEqual(len(self.compile(deliveries=d,menu=m).public["premises"]),1)

    def test_wrong_recipient_rejected(self):
        self.assert_code("WRONG_RECIPIENT",lambda:self.compile(dict(self.atoms,rate=replace(self.rate,recipients=("other-worker",)))))

    def test_unverified_source_rejected(self):
        self.assert_code("UNVERIFIED_SOURCE",lambda:self.compile(dict(self.atoms,rate=replace(self.rate,source_checked=False))))

    def test_missing_dependency_rejected(self):
        a=replace(self.rate,kind="DERIVED",dependencies=("absent",),transform="COPY")
        self.assert_code("MISSING_DEPENDENCY",lambda:self.compile(dict(self.atoms,rate=a)))

    def test_future_dependency_rejected(self):
        parent=replace(self.rate,atom_id="parent",published_at=T+timedelta(days=1),available_at=T+timedelta(days=1))
        child=replace(self.rate,kind="DERIVED",dependencies=("parent",),transform="COPY")
        self.assert_code("FUTURE_DEPENDENCY",lambda:self.compile(dict(self.atoms,rate=child,parent=parent)))

    def test_dependency_cycle_rejected(self):
        a=replace(self.rate,kind="DERIVED",dependencies=("rate",),transform="COPY")
        self.assert_code("DEPENDENCY_CYCLE",lambda:self.compile(dict(self.atoms,rate=a)))

    def test_derived_number_recomputed(self):
        p=replace(self.rate,atom_id="old",value="275")
        q=replace(self.rate,atom_id="change",metric="policy_delta_bps",value="25",kind="DERIVED",dependencies=("rate","old"),transform="SUBTRACT")
        c=self.compile(dict(self.atoms,old=p,change=q),self.deliveries+[self.delivery("change")])
        self.assertIn("25",[x["value"] for x in c.public["premises"]])

    def test_wrong_derived_number_rejected(self):
        p=replace(self.rate,atom_id="old",value="275")
        q=replace(self.rate,atom_id="change",metric="policy_delta_bps",value="99",kind="DERIVED",dependencies=("rate","old"),transform="SUBTRACT")
        self.assert_code("WRONG_DERIVED_VALUE",lambda:self.compile(dict(self.atoms,old=p,change=q),self.deliveries+[self.delivery("change")]))

    def test_unknown_transform_rejected(self):
        a=replace(self.rate,kind="DERIVED",dependencies=("rule",),transform="LLM_OPINION")
        self.assert_code("UNREGISTERED_DERIVATION",lambda:self.compile(dict(self.atoms,rate=a)))

    def test_prose_cannot_become_numeric_fact(self):
        a=replace(self.rate,value="The market crashes next month")
        self.assert_code("UNAPPROVED_TEXT",lambda:self.compile(dict(self.atoms,rate=a)))

    def test_nonfinite_value_rejected(self):
        self.assert_code("NONFINITE_VALUE",lambda:self.compile(dict(self.atoms,rate=replace(self.rate,value="NaN"))))

    def test_enum_value_outside_metric_registry_rejected(self):
        bad=replace(self.rule,value="BULL_PUT_CREDIT")
        self.assert_code("UNAPPROVED_TEXT",lambda:self.compile(dict(self.atoms,rule=bad)))

    def test_wrong_unit_for_registered_metric_rejected(self):
        bad=replace(self.rate,unit="slots")
        self.assert_code("UNAPPROVED_VOCABULARY",lambda:self.compile(dict(self.atoms,rate=bad)))

    def test_naive_time_rejected(self):
        self.assert_code("NAIVE_TIME",lambda:self.compile(ctx=replace(self.ctx,as_of=T.replace(tzinfo=None))))

    def test_accepted_witness_never_proves_model_ignorance(self):
        c=self.compile();w=self.h.validate(self.proposal(c),c)
        self.assertEqual(w["status"],"ACCEPTED")
        self.assertFalse(w["parametric_ignorance_proven"])

    def test_unknown_evidence_token_rejected(self):
        c=self.compile();p=self.proposal(c);p["premise_tokens"].append("ev_invented")
        self.assert_code("UNKNOWN_OR_DUPLICATE_TOKEN",lambda:self.h.validate(p,c))

    def test_missing_required_premise_rejected(self):
        c=self.compile();p=self.proposal(c);p["premise_tokens"]=[]
        self.assert_code("REQUIRED_PREMISE_MISSING",lambda:self.h.validate(p,c))

    def test_unknown_action_rejected(self):
        c=self.compile();p=self.proposal(c);p["action_id"]="free-trade"
        self.assert_code("UNKNOWN_ACTION",lambda:self.h.validate(p,c))

    def test_stale_packet_rejected(self):
        c=self.compile();p=self.proposal(c);p["packet_token"]="old-packet"
        self.assert_code("STALE_OR_WRONG_CONTEXT",lambda:self.h.validate(p,c))

    def test_free_explanation_rejected(self):
        c=self.compile();p=self.proposal(c);p["explanation"]="I know the future"
        self.assert_code("OUTPUT_KEYS",lambda:self.h.validate(p,c))

    def test_model_fact_promotion_rejected(self):
        c=self.compile();p=self.proposal(c)
        p["assessment_updates"]=[{"topic":"REALIZED_FUTURE","assessment":"MAINTAIN","premise_tokens":p["premise_tokens"],"confidence_label":"HIGH"}]
        self.assert_code("INVALID_ASSESSMENT_TOPIC",lambda:self.h.validate(p,c))

    def test_cross_topic_assessment_rejected(self):
        c=self.compile();p=self.proposal(c)
        p["assessment_updates"]=[{"topic":"RATE_OUTLOOK","assessment":"MAINTAIN","premise_tokens":p["premise_tokens"],"confidence_label":"LOW"}]
        self.assert_code("INVALID_ASSESSMENT_VALUE",lambda:self.h.validate(p,c))

    def test_valid_subjective_assessment_is_not_a_fact_write(self):
        c=self.compile();p=self.proposal(c)
        p["assessment_updates"]=[{"topic":"RATE_OUTLOOK","assessment":"UNCERTAIN","premise_tokens":p["premise_tokens"],"confidence_label":"LOW"}]
        before=copy.deepcopy(self.atoms);self.h.validate(p,c)
        self.assertEqual(before,self.atoms)

    def test_manager_has_same_prefix_boundary(self):
        ctx=replace(self.ctx,actor_role="MANAGER",actor_id="manager")
        ds=[replace(d,actor_id="manager") for d in self.deliveries]
        menu=[MenuChoice("no-change","NO_CHANGE",("rule","rate"))]
        first=self.compile(deliveries=ds,ctx=ctx,menu=menu)
        second=self.compile(deliveries=ds,ctx=replace(ctx,private_manifest_id="another-future"),menu=menu)
        self.assertEqual(first.public,second.public)
        self.assertEqual(self.h.validate(self.proposal(first),first)["status"],"ACCEPTED")

    def test_role_inappropriate_menu_rejected(self):
        self.assert_code("ACTION_WRONG_ROLE",lambda:self.compile(menu=[MenuChoice("allocate","ALLOCATE",("rule",))]))

if __name__ == "__main__":
    unittest.main(verbosity=2)
