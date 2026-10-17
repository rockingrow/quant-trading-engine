"""Cost profiles: what a replay charges a fill, and what the file refuses.

The real `config/costs.toml` is git-ignored, so these tests and
`config/costs.example.toml` are the only reviewable record of the shape. The
rejections are the interesting half: a file that half-parses prices a fill at a
number nobody chose, and the report would look just as confident about it.
"""

from __future__ import annotations

import textwrap
from pathlib import Path

import pytest

from qte_backtest.costs import (
    FROM_ACCOUNT,
    FROM_DEFAULTS,
    FROM_FLAG,
    FROM_SYMBOL,
    CostOrigin,
    CostProfiles,
    resolve_costs,
)
from qte_shared.config import settings

PROFILES = """
    default_profile = "standard"

    [profiles.standard.defaults]
    spread = 0.04
    slippage = 0.0
    commission_per_unit = 0.0

    [profiles.standard.symbols.XAUUSD]
    spread = 0.25
    slippage = 0.05

    [profiles.zero.defaults]
    spread = 0.0

    [profiles.zero.symbols.XAUUSD]
    spread = 0.05
    commission_per_unit = 0.035
"""

#: What the account-wide variables would price a fill at, stated here rather
#: than read from the environment so a test says what it is testing.
ACCOUNT = {"spread": 1.5, "slippage": 0.5, "commission_per_unit": 0.25}


def write(tmp_path: Path, body: str, name: str = "costs.toml") -> Path:
    path = tmp_path / name
    path.write_text(textwrap.dedent(body).lstrip(), encoding="utf-8")
    return path


@pytest.fixture
def profiles(tmp_path: Path) -> CostProfiles:
    return CostProfiles.load(write(tmp_path, PROFILES))


# ── Resolution ────────────────────────────────────────────────────────


def test_the_symbol_table_prices_the_fill(profiles):
    costs = resolve_costs("XAUUSD", profiles=profiles, fallback=ACCOUNT)

    assert (costs.spread, costs.slippage) == (0.25, 0.05)
    assert costs.origin.profile == "standard"
    assert costs.origin.sources["spread"] == FROM_SYMBOL


def test_each_cost_resolves_on_its_own(profiles):
    """XAUUSD states no commission, so the profile's default stands — not zero."""
    costs = resolve_costs("XAUUSD", profiles=profiles, fallback=ACCOUNT)

    assert costs.commission_per_unit == 0.0
    assert costs.origin.sources["commission_per_unit"] == FROM_DEFAULTS


def test_an_unlisted_symbol_falls_back_to_the_profile_defaults(profiles, caplog):
    with caplog.at_level("WARNING"):
        costs = resolve_costs("USOIL", profiles=profiles, fallback=ACCOUNT)

    assert costs.spread == 0.04
    assert costs.origin.sources["spread"] == FROM_DEFAULTS
    # Silence here is the bug: one number for every symbol is what the file exists
    # to replace, so applying it has to be visible in the run log.
    assert "states nothing for USOIL" in caplog.text


def test_what_no_layer_states_comes_from_the_account(tmp_path):
    """A profile naming only a spread leaves the other two to QTE_ACCOUNT__*."""
    profiles = CostProfiles.load(write(tmp_path, "[profiles.thin.symbols.XAUUSD]\nspread = 0.3\n"))

    costs = resolve_costs("XAUUSD", profiles=profiles, fallback=ACCOUNT)

    assert (costs.spread, costs.slippage) == (0.3, 0.5)
    assert costs.origin.sources == {
        "spread": FROM_SYMBOL,
        "slippage": FROM_ACCOUNT,
        "commission_per_unit": FROM_ACCOUNT,
    }


def test_a_flag_outranks_the_profile(profiles):
    costs = resolve_costs("XAUUSD", profiles=profiles, flags={"spread": 0.5}, fallback=ACCOUNT)

    assert costs.spread == 0.5
    assert costs.origin.sources["spread"] == FROM_FLAG
    # One overridden cost does not drag the others off the profile.
    assert costs.slippage == 0.05
    assert costs.origin.sources["slippage"] == FROM_SYMBOL


def test_a_flag_that_was_not_passed_is_not_an_override(profiles):
    """``None`` is "not stated" — the whole reason the CLI defaults are None."""
    costs = resolve_costs(
        "XAUUSD",
        profiles=profiles,
        flags={"spread": None, "slippage": None, "commission_per_unit": None},
        fallback=ACCOUNT,
    )

    assert costs.spread == 0.25
    assert costs.origin.sources["spread"] == FROM_SYMBOL


def test_the_profile_is_switchable(profiles):
    costs = resolve_costs("XAUUSD", profiles=profiles, profile="zero", fallback=ACCOUNT)

    assert (costs.spread, costs.commission_per_unit) == (0.05, 0.035)
    assert costs.origin.profile == "zero"


def test_a_symbol_is_matched_whatever_its_case(tmp_path):
    profiles = CostProfiles.load(
        write(tmp_path, "[profiles.standard.symbols.xauusd]\nspread = 0.25\n")
    )

    assert resolve_costs("xauusd", profiles=profiles, fallback=ACCOUNT).spread == 0.25


def test_no_file_prices_everything_from_the_account(tmp_path):
    profiles = CostProfiles.load(tmp_path / "absent.toml")

    costs = resolve_costs("XAUUSD", profiles=profiles, fallback=ACCOUNT)

    assert not profiles
    assert (costs.spread, costs.slippage, costs.commission_per_unit) == (1.5, 0.5, 0.25)
    assert costs.origin.profile == ""
    assert set(costs.origin.sources.values()) == {FROM_ACCOUNT}


def test_the_account_fallback_is_the_configured_one(tmp_path):
    """Omitting *fallback* reads QTE_ACCOUNT__*, which is what the CLI relies on."""
    costs = resolve_costs("XAUUSD", profiles=CostProfiles.load(tmp_path / "absent.toml"))

    assert costs.spread == settings.account.spread
    assert costs.commission_per_unit == settings.account.commission_per_unit


# ── Refusals ──────────────────────────────────────────────────────────


def test_an_unknown_profile_is_refused_with_the_names_it_has(profiles):
    with pytest.raises(ValueError, match="standard, zero"):
        resolve_costs("XAUUSD", profiles=profiles, profile="exness_pro", fallback=ACCOUNT)


def test_asking_for_a_profile_without_a_file_is_refused(tmp_path):
    profiles = CostProfiles.load(tmp_path / "absent.toml")

    with pytest.raises(ValueError, match="no cost-profile file"):
        resolve_costs("XAUUSD", profiles=profiles, profile="standard", fallback=ACCOUNT)


def test_several_profiles_and_no_default_is_refused(tmp_path):
    """Picking the first table in the file is not a choice anyone asked for."""
    body = "[profiles.standard.defaults]\nspread = 0.04\n\n[profiles.zero.defaults]\nspread = 0.0\n"
    profiles = CostProfiles.load(write(tmp_path, body))

    with pytest.raises(ValueError, match="no default_profile"):
        resolve_costs("XAUUSD", profiles=profiles, fallback=ACCOUNT)


def test_one_profile_needs_no_default(tmp_path):
    profiles = CostProfiles.load(
        write(tmp_path, "[profiles.standard.symbols.XAUUSD]\nspread = 0.25\n")
    )

    assert resolve_costs("XAUUSD", profiles=profiles, fallback=ACCOUNT).spread == 0.25


@pytest.mark.parametrize(
    ("body", "message"),
    [
        (
            'default_profile = "missing"\n[profiles.standard.defaults]\nspread = 0.04\n',
            "names no profile",
        ),
        ("[profiles.standard.defaults]\nspred = 0.04\n", "has no key 'spred'"),
        ("[profiles.standard.symbols.XAUUSD]\nspread = -0.1\n", "negative"),
        ('[profiles.standard.symbols.XAUUSD]\nspread = "wide"\n', "must be a number"),
        ("[profiles.standard.symbols.XAUUSD]\nspread = true\n", "must be a number"),
        ("spred = 0.04\n", "unknown top-level key"),
        ("[profiles.standard]\ncosts = 0.04\n", "unknown key"),
        ("default_profile = 3\n", "must be a profile name"),
        ("profiles = 3\n", "must be a table of profiles"),
        ("[profiles.standard]\nsymbols = 3\n", "must be a table keyed by symbol"),
        ("[profiles.standard]\ndefaults = 3\n", "must be a table of costs"),
    ],
)
def test_a_malformed_file_stops_the_run(tmp_path, body, message):
    with pytest.raises(ValueError, match=message):
        CostProfiles.load(write(tmp_path, body))


def test_a_negative_flag_is_refused(profiles):
    with pytest.raises(ValueError, match="--spread is negative"):
        resolve_costs("XAUUSD", profiles=profiles, flags={"spread": -0.1}, fallback=ACCOUNT)


# ── What the report says about it ─────────────────────────────────────


def test_the_origin_is_reportable(profiles):
    origin = resolve_costs("XAUUSD", profiles=profiles, flags={"spread": 0.5}).origin
    payload = origin.to_dict()

    assert payload["profile"] == "standard"
    assert payload["profile_file"].endswith("costs.toml")
    assert payload["sources"]["spread"] == FROM_FLAG
    assert "spread overridden on the command line" in origin.summary()


def test_an_unpriced_report_says_so_rather_than_implying_a_profile():
    """The keys are written either way, so a consumer reads one shape."""
    assert CostOrigin().to_dict() == {"profile": None, "profile_file": None, "sources": {}}
    assert "no cost profile" in CostOrigin().summary()
