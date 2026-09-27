"""The runner turning a mapping table into slots.

`_build_slots` is the only place the table has any effect, and it is the place
where getting it wrong is expensive: a missing pair trades nothing, a spurious
one trades something nobody asked for, and a shared instance across symbols
lets gold's last bar decide bitcoin's next one.

No NATS, no Redis, no Postgres — the runner is constructed but never started,
and only the pairing is exercised.
"""

from __future__ import annotations

import textwrap
from pathlib import Path

import pytest

from qte_shared.strategies.mapping import SymbolMapping
from qte_shared.strategies.strategy_base import SignalStrategy
from qte_strategy_engine.runner import StrategyRunner


class Edge(SignalStrategy):
    name = "GOLD_M15"
    symbols = ("XAUUSD",)
    timeframe = "M15"
    warmup = 10

    def long(self, df, context):
        return None

    def short(self, df, context):
        return None

    def tp1(self, df, context):
        return None

    def tp2(self, df, context):
        return None

    def sl(self, df, context):
        return None


class Scalp(Edge):
    name = "GOLD_SCALP"
    symbols = ("XAUUSD",)


def table(tmp_path: Path, body: str) -> Path:
    path = tmp_path / "strategies_mapping.toml"
    path.write_text(textwrap.dedent(body).lstrip(), encoding="utf-8")
    return path


@pytest.fixture
def runner(monkeypatch, tmp_path):
    """A runner whose loader returns our two strategies and nothing else."""
    from qte_shared.strategies.plugin_loader import LoadedStrategy

    discovered = [
        LoadedStrategy(name="GOLD_M15", cls=Edge, source=tmp_path / "edge.py"),
        LoadedStrategy(name="GOLD_SCALP", cls=Scalp, source=tmp_path / "scalp.py"),
    ]
    monkeypatch.setattr(
        "qte_strategy_engine.runner.load_strategies", lambda *a, **k: list(discovered)
    )
    return StrategyRunner()


def build(runner, monkeypatch, mapping_file: Path | None):
    from qte_shared.config import settings

    monkeypatch.setattr(
        settings.engine, "mapping_file", mapping_file or Path("does-not-exist.toml")
    )
    runner._build_slots()
    return {(slot.strategy.name, slot.symbol) for slot in runner.slots}


def test_the_table_decides_the_pairs(runner, monkeypatch, tmp_path):
    """Including a symbol the strategy never declared on itself."""
    mapping = table(
        tmp_path,
        """
        [symbols.XAUUSD]
        strategies = ["GOLD_M15", "GOLD_SCALP"]

        [symbols.BTCUSDT]
        strategies = ["GOLD_M15"]
        """,
    )
    assert build(runner, monkeypatch, mapping) == {
        ("GOLD_M15", "XAUUSD"),
        ("GOLD_SCALP", "XAUUSD"),
        ("GOLD_M15", "BTCUSDT"),
    }


def test_a_strategy_the_table_never_mentions_does_not_run(runner, monkeypatch, tmp_path):
    """Loaded is not the same as deployed, once there is a table saying so."""
    mapping = table(tmp_path, '[symbols.XAUUSD]\nstrategies = ["GOLD_M15"]\n')
    assert build(runner, monkeypatch, mapping) == {("GOLD_M15", "XAUUSD")}


def test_without_a_table_each_strategy_keeps_its_own_symbols(runner, monkeypatch):
    """The behaviour from before the table existed, unchanged."""
    assert build(runner, monkeypatch, None) == {
        ("GOLD_M15", "XAUUSD"),
        ("GOLD_SCALP", "XAUUSD"),
    }


def test_each_pair_gets_its_own_instance(runner, monkeypatch, tmp_path):
    """A strategy carries state between bars; sharing it across symbols leaks it."""
    mapping = table(
        tmp_path,
        """
        [symbols.XAUUSD]
        strategies = ["GOLD_M15"]

        [symbols.BTCUSDT]
        strategies = ["GOLD_M15"]
        """,
    )
    build(runner, monkeypatch, mapping)
    instances = [slot.strategy for slot in runner.slots]
    assert len(instances) == 2
    assert instances[0] is not instances[1]


def test_per_pair_params_beat_the_per_strategy_default(runner, monkeypatch, tmp_path):
    """One strategy running tighter on gold than on bitcoin is the point."""
    mapping = table(
        tmp_path,
        """
        [strategies.GOLD_M15]
        risk_percent = 2.0

        [symbols.XAUUSD]
        strategies = ["GOLD_M15"]

        [symbols.XAUUSD.params.GOLD_M15]
        risk_percent = 0.5

        [symbols.BTCUSDT]
        strategies = ["GOLD_M15"]
        """,
    )
    build(runner, monkeypatch, mapping)
    by_symbol = {slot.symbol: slot.strategy.params for slot in runner.slots}

    assert by_symbol["XAUUSD"]["risk_percent"] == 0.5
    assert by_symbol["BTCUSDT"]["risk_percent"] == 2.0, "the default still applies elsewhere"


def test_a_name_nobody_publishes_is_logged_as_an_error(runner, monkeypatch, tmp_path, caplog):
    """Otherwise the symbol trades nothing and it reads like patience."""
    mapping = table(tmp_path, '[symbols.XAUUSD]\nstrategies = ["TYPOD_NAME"]\n')
    with caplog.at_level("ERROR"):
        build(runner, monkeypatch, mapping)

    assert "TYPOD_NAME" in caplog.text
    assert not runner.slots


def test_strategy_defaults_for_an_unknown_name_are_warned_about(
    runner, monkeypatch, tmp_path, caplog
):
    """A typo in a [strategies.<name>] key means an override silently never applies."""
    mapping = table(
        tmp_path,
        """
        [strategies.TYPOD_NAME]
        risk_percent = 2.0

        [symbols.XAUUSD]
        strategies = ["GOLD_M15"]
        """,
    )
    with caplog.at_level("WARNING"):
        pairs = build(runner, monkeypatch, mapping)

    assert "TYPOD_NAME" in caplog.text
    assert pairs == {("GOLD_M15", "XAUUSD")}, "the typo'd default does not stop the real pairing"


def test_a_table_that_maps_nothing_trades_nothing(runner, monkeypatch, tmp_path):
    """Not the same as no table at all — those two differ by a deploy."""
    mapping = table(
        tmp_path,
        """
        [symbols.XAUUSD]
        enabled = false
        strategies = ["GOLD_M15"]
        """,
    )
    assert build(runner, monkeypatch, mapping) == set()


def test_the_mapped_symbols_reach_the_signal_factory(runner, monkeypatch, tmp_path):
    """A slot is only useful if it is subscribed and named correctly."""
    mapping = table(tmp_path, '[symbols.btcusdt]\nstrategies = ["GOLD_SCALP"]\n')
    build(runner, monkeypatch, mapping)

    slot = runner.slots[0]
    assert slot.symbol == "BTCUSDT", "the table may be lower case; the wire is not"
    assert slot.key == ("GOLD_SCALP", "BTCUSDT")
    assert runner._by_subject[("BTCUSDT", "M15")] == [slot]


def test_an_empty_mapping_object_is_falsy_but_a_read_one_is_not():
    """The distinction `_build_slots` branches on."""
    assert not SymbolMapping()
    assert SymbolMapping(source=Path("strategies_mapping.toml"))


# ── The pair's weekend-flat switch ───────────────────────────────────────


@pytest.fixture
def switchable_runner(monkeypatch, tmp_path):
    """One strategy whose repository declares a weekend window that is off by default."""
    from qte_shared.strategies.plugin_loader import LoadedStrategy
    from qte_shared.strategies.strategy_settings import StrategySettings, WeekendFlatPolicy

    declared = StrategySettings(
        weekend_flat=WeekendFlatPolicy.parse(
            {"enabled": False, "flat_from": "FRI 20:00", "flat_until": "SUN 23:00"}
        )
    )
    discovered = [
        LoadedStrategy(name="GOLD_M15", cls=Edge, source=tmp_path / "edge.py", settings=declared)
    ]
    monkeypatch.setattr(
        "qte_strategy_engine.runner.load_strategies", lambda *a, **k: list(discovered)
    )
    return StrategyRunner()


def weekend_policies(runner) -> dict[str, str]:
    return {slot.symbol: slot.weekend_flat.describe() for slot in runner.slots}


def test_a_declared_window_stays_off_until_the_table_switches_it_on(
    switchable_runner, monkeypatch, tmp_path
):
    mapping = table(tmp_path, '[symbols.XAUUSD]\nstrategies = ["GOLD_M15"]\n')
    build(switchable_runner, monkeypatch, mapping)

    assert weekend_policies(switchable_runner) == {
        "XAUUSD": "off (declared: FRI 20:00 -> SUN 23:00)"
    }


def test_the_pair_switch_beats_the_per_strategy_default(switchable_runner, monkeypatch, tmp_path):
    """The same two layers as every other param: the pair's own value wins."""
    mapping = table(
        tmp_path,
        """
        [strategies.GOLD_M15]
        use_weekend_flat = true

        [symbols.XAUUSD]
        strategies = ["GOLD_M15"]

        [symbols.BTCUSDT]
        strategies = ["GOLD_M15"]

        [symbols.BTCUSDT.params.GOLD_M15]
        use_weekend_flat = false
        """,
    )
    build(switchable_runner, monkeypatch, mapping)

    by_symbol = {slot.symbol: slot.weekend_flat for slot in switchable_runner.slots}
    assert by_symbol["XAUUSD"].enabled
    assert by_symbol["XAUUSD"].describe() == "FRI 20:00 -> SUN 23:00"
    assert not by_symbol["BTCUSDT"].enabled


def test_a_switch_that_cannot_be_honoured_stops_the_build(switchable_runner, monkeypatch, tmp_path):
    """A pair configured to be flat must never quietly trade through the weekend."""
    mapping = table(
        tmp_path,
        """
        [symbols.XAUUSD]
        strategies = ["GOLD_M15"]

        [symbols.XAUUSD.params.GOLD_M15]
        use_weekend_flat = "yes"
        """,
    )
    with pytest.raises(ValueError, match="GOLD_M15 on XAUUSD: use_weekend_flat must be true"):
        build(switchable_runner, monkeypatch, mapping)


def test_switching_on_a_window_nobody_declared_stops_the_build(runner, monkeypatch, tmp_path):
    mapping = table(
        tmp_path,
        """
        [symbols.XAUUSD]
        strategies = ["GOLD_M15"]

        [symbols.XAUUSD.params.GOLD_M15]
        use_weekend_flat = true
        """,
    )
    with pytest.raises(ValueError, match="declares no weekend window"):
        build(runner, monkeypatch, mapping)
