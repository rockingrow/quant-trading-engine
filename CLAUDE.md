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

## Working in a worktree

A worktree session (`.claude/worktrees/*`) has its own git-ignored `data/`, and
it disappears with the worktree. Backtest output must not end up there.

- Every backtest report — JSON, Markdown, HTML dashboard, chart assets — is
  written under the **main checkout's**
  `data\reports\YYYY-MM-DD-<topic>\`
  (AUDIT.md rule 5 still applies: one session, one subfolder).
- Pass that absolute path to the run (`qte-backtest run --report <DIR>`, or set
  `QTE_ENGINE__REPORTS_DIR` when going through `make backtest`), so the process
  writes there itself. A bare `--report` falls back to the worktree's own
  `data/reports/`, which is the case to avoid. The
  Write/Edit hook that refuses edits into the main checkout does not apply to
  files a backtest process produces.
- Never leave a report in the worktree's own `data/reports/`. If one was
  produced there by mistake, move it to the main checkout before handing back.
- Audits (`data/audits/`) are not covered by this rule.

## Where to slow down

Use plan mode, and confirm the approach, before editing:

- `src/qte_strategy_engine/{broker_sink,runner}.py` —
  the live delivery path; a mistake here sends or loses real orders.
- `migrations/versions/` — one Alembic chain for every engine.
- `src/qte_shared/models.py` and
  `src/qte_shared/strategies/strategy_base.py` — a contract change
  for every private strategy repo, which this repository cannot see.
- `src/qte_shared/providers/mt5/{protocol,history}.py` — the wire contract with
  `algo-trading-ingester`: the subjects, the request/reply shape and the
  accepted `schema_version`. Both sides have to change together, and this
  repository cannot see the other one either.

## Session hygiene

- Durable project facts belong in `AGENTS.md`, not in auto memory: Codex has to
  see them too.
- If `/context` does not list both `CLAUDE.md` and `AGENTS.md` under **Memory
  files**, the import broke — check that the first line of this file is
  `@AGENTS.md` outside any code fence.
