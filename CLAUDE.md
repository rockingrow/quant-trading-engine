@AGENTS.md

# Claude Code

The line above imports [AGENTS.md](AGENTS.md), the shared instruction file every
coding agent in this repository follows — project layout, the navigation table,
architecture invariants, verification, and the [AUDIT.md](AUDIT.md) rules
digest. Claude Code does not read `AGENTS.md` on its own; that import is what
puts it in context.

**Do not copy shared content into this file.** Anything both Codex and Claude
need goes in `AGENTS.md`; this file holds only what is specific to Claude Code.
Two files describing the same repository is how they start contradicting each
other.

## Before handing work back

- `/code-review` on the diff before the branch is proposed for a PR into `dev`.
- `/security-review` when the change touches broker delivery, credentials,
  `.env` handling, or anything published to NATS.
- Reports and audits are deliverables, not chat output: write them to
  `data/reports/` and `data/audits/` per AUDIT.md rule 5.

## Where to slow down

Use plan mode, and confirm the approach, before editing:

- `engines/strategy_engine/src/qte_strategy_engine/{broker_sink,runner}.py` —
  the live delivery path; a mistake here sends or loses real orders.
- `migrations/versions/` — one Alembic chain for every engine.
- `engines/shared/src/qte_shared/{models,strategy_base}.py` — a contract change
  for every private strategy repo, which this repository cannot see.

## Session hygiene

- Durable project facts belong in `AGENTS.md`, not in auto memory: Codex has to
  see them too.
- If `/context` does not list both `CLAUDE.md` and `AGENTS.md` under **Memory
  files**, the import broke — check that the first line of this file is
  `@AGENTS.md` outside any code fence.
