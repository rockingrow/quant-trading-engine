"""The auto-generated artefact block at the bottom of a concept document.

Three properties matter and are asserted here: the block is found by its
markers and replaced rather than duplicated, nothing outside the markers is
touched, and a document that does not exist is stepped over instead of raising
into the middle of a finished replay.
"""

from __future__ import annotations

import json
from datetime import UTC, datetime
from pathlib import Path

import pytest

from qte_backtest.doc_reference import (
    BEGIN_MARKER,
    END_MARKER,
    ArtefactReference,
    apply_block,
    concept_documents,
    latest_audit,
    reference_from_report_file,
    render_block,
    update_concept_document,
)

ALIAS = "MOUNTED_EXAMPLE_M15_V1"

HAND_WRITTEN = f"""# {ALIAS}

| | |
| --- | --- |
| Instrument | XAUUSD |

## The idea

Prose nobody but the author may touch.
"""


@pytest.fixture
def mount(tmp_path: Path) -> Path:
    concepts = tmp_path / "repository" / "docs" / "concepts"
    concepts.mkdir(parents=True)
    (concepts / "README.md").write_text("# Index\n", encoding="utf-8")
    (concepts / "mounted_example_m15.md").write_text(HAND_WRITTEN, encoding="utf-8")
    return tmp_path


def _reference(tmp_path: Path) -> ArtefactReference:
    reports = tmp_path / "data" / "reports" / "2026-11-03-example"
    reports.mkdir(parents=True)
    written = {}
    for suffix in ("json", "md", "html"):
        path = reports / f"run.{suffix}"
        path.write_text("{}", encoding="utf-8")
        written[suffix] = path
    return ArtefactReference(
        strategy=ALIAS,
        symbol="XAUUSD",
        timeframe="M15",
        generated_at=datetime(2026, 11, 3, 14, 2, tzinfo=UTC),
        period_start="2020-01-02T00:00:00+00:00",
        period_end="2026-09-30T21:45:00+00:00",
        reports=written,
        headline="412 trades, 46.10% win rate",
    )


# ── Discovery ─────────────────────────────────────────────────────────


def test_a_document_is_found_by_the_alias_it_opens_with(mount: Path) -> None:
    found = concept_documents(mount)
    assert set(found) == {ALIAS}
    assert found[ALIAS].name == "mounted_example_m15.md"


def test_a_missing_mount_is_not_an_error(tmp_path: Path) -> None:
    assert concept_documents(tmp_path / "nowhere") == {}


def test_the_index_itself_is_never_taken_for_a_strategy(mount: Path) -> None:
    index = mount / "repository" / "docs" / "concepts" / "README.md"
    index.write_text("# SOMETHING_LOUD_V1\n", encoding="utf-8")
    assert concept_documents(mount) == {ALIAS: index.with_name("mounted_example_m15.md")}


def test_the_newest_audit_naming_the_strategy_wins(tmp_path: Path) -> None:
    audits = tmp_path / "audits"
    audits.mkdir()
    (audits / "2026-10-01-older.md").write_text(f"about {ALIAS}\n", encoding="utf-8")
    (audits / "2026-10-28-newer.md").write_text(f"also about {ALIAS}\n", encoding="utf-8")
    (audits / "2026-11-02-unrelated.md").write_text("about something else\n", encoding="utf-8")
    assert latest_audit(ALIAS, audits).name == "2026-10-28-newer.md"
    assert latest_audit("NOBODY_AT_ALL_V1", audits) is None


# ── Rendering ─────────────────────────────────────────────────────────


def test_the_block_says_it_is_generated_and_links_every_format(mount: Path, tmp_path: Path) -> None:
    document = concept_documents(mount)[ALIAS]
    block = render_block(_reference(tmp_path), document)
    assert block.startswith(BEGIN_MARKER)
    assert block.rstrip().endswith(END_MARKER)
    assert "AUTO-GENERATED" in block
    assert "do not edit" in block.lower()
    for suffix in ("json", "md", "html"):
        assert f"run.{suffix}" in block
    assert "2020-01-02 → 2026-09-30" in block


def test_an_empty_block_states_that_no_replay_was_recorded(mount: Path) -> None:
    document = concept_documents(mount)[ALIAS]
    block = render_block(None, document)
    assert "No reporting replay has been recorded" in block
    assert "AUTO-GENERATED" in block


# ── Writing ───────────────────────────────────────────────────────────


def test_the_block_is_appended_once_and_then_replaced(mount: Path, tmp_path: Path) -> None:
    document = concept_documents(mount)[ALIAS]
    update_concept_document(ALIAS, _reference(tmp_path), strategies_dir=mount)
    once = document.read_text(encoding="utf-8")
    assert once.count(BEGIN_MARKER) == 1
    assert once.startswith(HAND_WRITTEN.rstrip() + "\n")

    update_concept_document(ALIAS, None, strategies_dir=mount)
    twice = document.read_text(encoding="utf-8")
    assert twice.count(BEGIN_MARKER) == 1
    assert "run.html" not in twice
    assert "Prose nobody but the author may touch." in twice


def test_a_strategy_without_a_document_is_stepped_over(mount: Path, tmp_path: Path) -> None:
    assert update_concept_document("ABSENT_STRATEGY_V1", None, strategies_dir=mount) is None


def test_text_around_the_markers_survives_a_rewrite() -> None:
    text = f"head\n\n{BEGIN_MARKER}\nstale\n{END_MARKER}\n\ntail\n"
    updated = apply_block(text, f"{BEGIN_MARKER}\nfresh\n{END_MARKER}\n")
    assert updated.startswith("head\n")
    assert updated.endswith("tail\n")
    assert "stale" not in updated
    assert "fresh" in updated


# ── Reading a report back ─────────────────────────────────────────────


def test_a_report_json_describes_itself(tmp_path: Path) -> None:
    report = tmp_path / "run.json"
    report.write_text(
        json.dumps(
            {
                "generated_at": "2026-11-03T14:02:00+00:00",
                "run": {"strategy": ALIAS, "symbol": "XAUUSD", "timeframe": "M15"},
                "data": {
                    "first_bar": "2020-01-02T00:00:00+00:00",
                    "last_bar": "2026-09-30T21:45:00+00:00",
                },
                "metrics": {
                    "trades": 412,
                    "win_rate": 46.1,
                    "max_consecutive_losses": 7,
                    "net_pnl": 3812.44,
                    "max_drawdown_pct": 11.4,
                },
            }
        ),
        encoding="utf-8",
    )
    (tmp_path / "run.html").write_text("<html></html>", encoding="utf-8")

    reference = reference_from_report_file(report, audits_dir=tmp_path / "nowhere")
    assert reference.strategy == ALIAS
    assert set(reference.reports) == {"json", "html"}
    assert reference.headline is not None
    assert "46.10% win rate" in reference.headline
    assert "max losing run 7" in reference.headline


def test_a_file_that_is_not_a_report_is_refused(tmp_path: Path) -> None:
    path = tmp_path / "notes.json"
    path.write_text(json.dumps({"something": "else"}), encoding="utf-8")
    with pytest.raises(ValueError, match="not a backtest report"):
        reference_from_report_file(path)
