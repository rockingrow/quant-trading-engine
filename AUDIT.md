# AUDIT.md — Engineering rules

Binding on every contributor, human or agent. [CLAUDE.md](CLAUDE.md) carries a
digest of these rules into every session; this file is the authority. Read the
relevant rule before you commit, open a PR, or write out a report.

---

## 1. Commit messages

- **Commit only when asked.** Do not create commits, push branches or open a
  pull request unless the user requests that action. Leave the work in the
  tree and say what you would commit.
- **English only.** No exceptions, including WIP and fixup commits.
- Imperative mood, describing what the commit makes the system do:
  `Make live delivery and startup recoverable`, not `fixed stuff` or
  `updated files`. Match the existing history — `git log --oneline -20`.
- Summary line ≤ 72 characters, no trailing period. Body wrapped at 72, and
  only when the *why* is not obvious from the diff.
- **No generated-by footers of any kind.** Specifically forbidden anywhere in
  the message: `Generated with Claude Code`, `Co-Authored-By: Claude …`,
  `Assisted-by:`, `🤖`, and any URL pointing at a Claude / Codex / agent
  session. The commit is authored by the person who owns the change.

**Why:** this repository is public and its history is the only durable record
of intent. Tooling attribution ages badly, leaks session URLs that outlive
their context, and says nothing a reviewer can use.

**Check before committing:**

```bash
git log -1 --format=%B | grep -Eiq 'claude|anthropic|codex|generated with|co-authored-by|assisted-by' \
  && echo "FORBIDDEN FOOTER — amend the message" || echo "commit message ok"
```

---

## 2. Comments, identifiers and docs are always English

Applies to inline comments, docstrings, `TODO` / `FIXME` notes, identifiers
(variables, functions, classes, modules), log and exception message strings,
and every committed Markdown file (`README.md`, `docs/`, `data/audits/`, PR
bodies).

Vietnamese belongs in chat and in PR *conversation*, never in the tree. If a
comment is worth writing in Vietnamese, it is worth translating before it is
staged.

**Why:** the engine is public and the docstrings are the primary API
documentation — every module here opens with one. A mixed-language tree makes
`grep` unreliable and splits the reader base.

**Check on staged changes** (a heuristic: it looks for Vietnamese tone marks,
and ignores the em dashes and arrows the docstrings already use):

```bash
git diff --cached -U0 | grep '^+' \
  | grep -iq '[ăâđêôơưàáảãạèéẻẽẹìíỉĩịòóỏõọùúủũụỳýỷỹỵ]' \
  && echo "NON-ENGLISH TEXT in the diff" || echo "language ok"
```

---

## 3. Variable names: explicit, and at least 6 characters

Every declared name — local variables, function parameters, instance
attributes, module constants, comprehension and loop targets — must be spelled
out well enough to carry its meaning alone, and must be **≥ 6 characters**.

```python
# no
df   = load(symbol)         # 2
n    = len(bars)            # 1
res  = auditor.run()        # 3
cfg  = settings.runner      # 3, and an abbreviation
for i, b in enumerate(bars) # 1, 1

# yes
candles      = load(symbol)
bar_count    = len(candles)
audit_result = auditor.run()
runner_setup = settings.runner
for bar_index, candle in enumerate(candles):
```

**The floor is not the point — clarity is.** `result`, `values`, `object`,
`data12` all clear six characters and still fail this rule. Name what the value
*is* in this domain: `closed_trades`, `entry_price`, `pending_signals`.

Never abbreviate by deleting vowels (`sgnl`, `stgy`, `msgbuf`).

**Narrow exceptions**, and nothing beyond them:

| Allowed | Because |
| --- | --- |
| `self`, `cls`, `_` (deliberately unused) | Python protocol |
| Names fixed by an external contract | The broker wire format owns `tp1`, `tp2`, `sl`, `r_sl`, `flat`; OHLC columns are `open`/`high`/`low`/`close`; a vendor field keeps the vendor's spelling |
| Standard market vocabulary used as a whole word | `bid`, `ask`, `pnl`, `atr`, `ema`, `rsi` — as the *complete* name of that quantity, never as a prefix hiding a longer one |

**Legacy names are not in scope.** Apply this rule to what you write. Do not
rename existing variables in code the task does not otherwise touch — a
rename-only diff buries the real change and breaks `git blame` for no gain.

**Why:** the same file is read in three modes here — as live trading code, as
backtest replay, and as an audit artefact. A name that needs its declaration
line to be understood costs a reader (and an agent) a file jump every time.

---

## 4. Pull requests always target `dev`

- **Base branch is `dev`.** Every PR, without exception:
  `gh pr create --base dev --head <your-branch>`.
- **Branch off `dev`** as well, so the PR diff contains only your work.
- `master` receives changes only through a release merge from `dev`, performed
  by the maintainer. Never open a PR into `master`.
- Opening the PR is the user's call (rule 1); prepare the branch and wait.
- PR title and body follow rule 1 and rule 2: English, no agent footers, no
  session links. The body states what changed, why, and how it was verified —
  paste the `make check` result.

---

## 5. Where generated artefacts go

| Artefact | Directory | Tracked in git |
| --- | --- | --- |
| Backtest reports — JSON, Markdown, HTML dashboards | `data/reports/` | No, git-ignored: regenerated per run and timestamped |
| Audits, reviews, quality write-ups | `data/audits/` | **Yes, committed** |

- Backtests are written there by the tooling already (`make backtest`,
  `make chart`); do not redirect them elsewhere and do not hand-edit them.
- Audits are named `YYYY-MM-DD-<topic>.md` — for example
  `data/audits/2026-08-26-repository-audit.md`. One file per audit run; update
  the existing file when re-verifying the same scope rather than opening a
  second one.
- `docs/` is for durable design documents that stay true as the code moves
  (`architecture.md`, `broker-contract.md`). An audit is a dated snapshot, so
  it does not belong there. Never write either kind of artefact to the
  repository root.
- Scratch and intermediate files stay outside the repository entirely.

---

## Before you call the work done

- [ ] `make check` passes (Ruff + pytest)
- [ ] Comments, docstrings and Markdown are English (rule 2)
- [ ] New names are explicit and ≥ 6 characters (rule 3)
- [ ] Reports in `data/reports/`, audits in `data/audits/` (rule 5)
- [ ] Commit message is English with no generated-by footer (rule 1)
- [ ] PR base branch is `dev` (rule 4)
