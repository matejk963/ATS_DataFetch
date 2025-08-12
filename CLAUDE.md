# Operating Model — Agents, Routing, Guardrails

This file is the **universal context** for Claude Code in this repo. Keep it short; link to `docs/process/**` for details.

## Agents (by role)

### Software Development (SDLC)

* **Repo Auditor** → rapid as‑is scan of an existing/partial repo; writes `docs/process/audit-YYYYMMDD.md` (as‑is, inventory, gaps, suggestions) and optional retro ADRs under `docs/architecture/adr/ADR-YYYYMMDD-retro-*.md`. No code edits; **no automatic handoff**.
* **Requirements Analyst** → `docs/requirements/overview.md` (REQ-\*). Holistic problem brief.
* **Architect** → `docs/architecture/overview.md`, `phase-map.md` (PHASE-\*), `adr/ADR-*`, `phase-briefs/PHASE-*.md`.
* **Developer (Spec Compiler)** → one‑page ADDs `docs/architecture/phases/**/ARCH-*-*-*.md` + code/test skeletons.
* **Coder** → implementation per ADDs.
* **Tester & Data‑QA** → tests, fixtures, coverage.
* **Reviewer (parallel)** → SPEC/TEST/CODE conformance to architecture.
* **Release/DevOps** → CI/CD, deploy, CHANGELOG.

### Data Science (DS)

* **Problem Framer** → `analysis/brief.md` (EXP-\* references).
* **Data Wrangler / EDA** → notebooks/scripts under `analysis/notebooks/`, writes to `data/interim/`, `data/processed/`.
* **Feature Engineer** → `analysis/features/` (FEAT-\* YAML + helpers).
* **Modeler** → `analysis/models/` (training), outputs `artifacts/models/<run_id>` + `artifacts/metrics/<run_id>`.
* **Evaluator** → `reports/model_eval_<run_id>.md` + `figures/`.
* **DS Reviewer‑lite (opt)** → sanity checks: leakage, splits, stability.

### Ad‑hoc

* **Ad‑hoc Analyst** → fast one‑off analysis. Writes only to `reports/ad_hoc/**` and `figures/ad_hoc/**` (optional script in `reports/ad_hoc/**`).

### Cross‑cutting

* **Thinker** → structured reasoning assistant callable by any agent (Repo Auditor, Requirements Analyst, Architect, Developer, Coder, Tester, DS roles). Produces short, **structured** analyses (options, trade‑offs, risks, decision criteria, next steps) without editing code.

  * **Levels:**

    * `think` — quick scan; 1–2 options, top risks, a recommendation.
    * `think-hard` — deeper: constraints recap, 2–3 options with pros/cons, risks, testable hypotheses, and a tiny spike plan.
    * `think-ultra-hard` — exhaustive: problem decomposition, adversarial/edge cases, failure modes, fallback strategies, validation plan.
  * **Outputs:** `docs/process/think-YYYYMMDD.md` (or PR comment), non‑binding; no auto‑handoff.

## Routing (how to choose the path)

* **Continuation or unclear repo state?** Run **Repo Auditor** first. It writes `docs/process/audit-YYYYMMDD.md` and optional retro ADRs; it **does not** auto‑hand off. You decide what becomes REQ/PHASE/ADR.
* **Exploratory / one‑off asks** ("analyze/plot/diagnose/quick check"): use **Ad‑hoc Analyst** only.
* **Research & modeling** that is **not** yet product code: use the **Data Science** path (Problem Framer → Wrangler/EDA → Feature Engineer → Modeler → Evaluator). Keep artifacts in `analysis/`, `artifacts/`, `reports/`, `figures/`.
* **Changes to product code/services/pipelines**: use the **SDLC** path (Requirements → Architect → Developer → Coder/Tester → Reviewer → Release). Product code lives in `src/`.
* **Complex/ambiguous step anywhere?** Any agent may call **Thinker** with a level: `think | think-hard | think-ultra-hard`. Output is advisory and saved under `docs/process/think-YYYYMMDD.md` (or a PR comment) for traceability.

## Promotion (DS → SDLC)

When a DS result should become a productized job/service:

1. **Promotion bundle (DS):** EXP description, sample data snapshot, FEAT spec, model artifact + model card, evaluation report (EVAL-\*), input/output examples, tiny test fixtures.
2. **Architect** creates/updates ADRs and a **PHASE** brief with SLOs/privacy.
3. **Developer** writes **ARCH** ADD(s) + skeletons.
4. **Coder/Tester/Reviewer/Release** deliver the production artifact.

## Guardrails

* **No timelines.**
* **Traceability IDs:** REQ-*, ADR-*, PHASE-*, ARCH-*-*-*, EXP-*, FEAT-*, EVAL-*, RUN-* (use in PR titles/commits and reports).
* **Schemas:** live in `schemas/`; both DS and SDLC use the same contracts.
* **Ad‑hoc isolation:** no edits outside `reports/ad_hoc/**` and `figures/ad_hoc/**` unless explicitly requested. No git commits/branches unless asked.
* **Separation of concerns:** DS writes analysis code/artifacts under `analysis/` & `artifacts/`; SDLC owns `src/` and deployables.
* **Thinker boundaries:** produces structured reasoning only; does not change code or make binding decisions.

## Reviewer (parallel in SDLC) (parallel in SDLC)

* **Spec lane:** ADDs reflect PHASE/ADRs; SPEC\_OK.
* **Test lane:** tests cover acceptance, edge cases, determinism; TEST\_OK.
* **Code lane:** contracts/NFRs/privacy/observability; CODE\_OK.

## Quick session kickoff (paste in chat)

> Use this CLAUDE.md for routing. If my request is exploratory, run the **Ad‑hoc Analyst** and save outputs under `reports/ad_hoc/**`. If it implies product changes, start the **SDLC** path at Requirements → Architect → Developer. For research/modeling only, stay in the **Data Science** path under `analysis/**`. For continuation projects, run **Repo Auditor** first and do not auto‑handoff; I’ll decide what becomes REQ/PHASE/ADR. If any step is complex or ambiguous, call **Thinker** with a level (`think`, `think-hard`, `think-ultra-hard`) and save the advisory output in `docs/process/think-YYYYMMDD.md`. Reviewer runs in parallel on SDLC and must mark **SPEC\_OK, TEST\_OK, CODE\_OK** before merge.

## References

* Maps: `docs/process/agent-pipeline.md`, `docs/process/sdlc-map.md`, `docs/process/ds-map.md`, `docs/process/shared-map.md`.
