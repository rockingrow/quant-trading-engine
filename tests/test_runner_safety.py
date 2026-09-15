"""Restart, ownership and durable delivery failure paths from the production audit."""

import asyncio
from copy import copy
from datetime import UTC, datetime, timedelta
from types import SimpleNamespace
from uuid import uuid4

import pytest
from test_runner_catch_up import CandleStore, build_runner, completed_bars
from test_runner_delivery import (
    FailedSink,
    RecordingSignals,
    _close,
    _enter,
    _runner,
    _slot,
)

from qte_shared.config import settings
from qte_shared.models import SignalAction
from qte_shared.strategies.strategy_base import SignalIntent
from qte_strategy_engine.settings import runner_settings


def pending_signal(strategy_slot, *, status="prepared", shadow=False, created_at=None):
    signal = strategy_slot.factory.build(
        SignalIntent(action=SignalAction.LONG, price=2334.5, sl=2329.5),
        symbol=strategy_slot.symbol,
        moment=datetime.now(UTC),
        commit=False,
    )
    return SimpleNamespace(
        namespace=settings.state_scope.namespace,
        id=uuid4(),
        created_at=created_at or datetime.now(UTC),
        strategy=signal.strategy,
        symbol=signal.symbol,
        payload=signal.to_envelope(),
        inputs={"__qte_outbox__": strategy_slot.factory.pending_delivery_context(signal.symbol)},
        delivery_status=status,
        transport="nats",
        shadow=shadow,
    )


@pytest.mark.parametrize("stored_mode", [True, False, None])
async def test_start_restores_shadow_before_outbox_recovery(monkeypatch, stored_mode):
    candle_store = CandleStore(completed_bars(2))
    if stored_mode is not None:
        candle_store.flags["shadow_mode"] = stored_mode
    runner, _, _ = build_runner(monkeypatch, candle_store)
    observed = []

    async def inspect_recovery():
        observed.append(runner.sink.shadow_mode)

    monkeypatch.setattr(runner, "_recover_pending_deliveries", inspect_recovery)
    await runner.start()
    try:
        assert observed == [True if stored_mode is None else stored_mode]
    finally:
        await runner.stop()


@pytest.mark.parametrize("failure", ["read_error", "invalid_flag"])
async def test_unreadable_shadow_mode_refuses_start_and_releases_ownership(monkeypatch, failure):
    candle_store = CandleStore([])
    runner, _, _ = build_runner(monkeypatch, candle_store)
    runner.sink.shadow_mode = False

    async def broken_flag(flag_name, default=None):
        if failure == "read_error":
            raise ConnectionError("control state unavailable")
        return "false"

    monkeypatch.setattr(candle_store, "get_flag", broken_flag)
    with pytest.raises((ConnectionError, ValueError)):
        await runner.start()
    assert runner.sink.shadow_mode is True
    assert candle_store.owner_id is None


async def test_failed_control_broadcast_is_observed_before_next_delivery():
    runner, strategy_slot = _runner(), _slot()
    runner.state.flags["shadow_mode"] = True
    await _enter(runner, strategy_slot)
    assert runner.sink.delivery_ids == []
    assert runner.signals.pending == []
    assert runner.state.held == {}


async def test_forced_shadow_overrides_a_persisted_live_switch(monkeypatch):
    monkeypatch.setattr(settings.broker, "force_shadow_mode", True)
    runner, strategy_slot = _runner(), _slot()
    runner.state.flags["shadow_mode"] = False
    await _enter(runner, strategy_slot)
    assert runner.sink.shadow_mode is True
    assert runner.sink.delivery_ids == []


async def test_delayed_control_broadcast_cannot_overwrite_newer_durable_mode():
    runner = _runner()
    runner.state.flags["shadow_mode"] = True

    class SilentEvents:
        async def record_event(self, **event_values):
            return None

    runner.events = SilentEvents()
    await runner._on_control_message(
        SimpleNamespace(data=b'{"action":"set_shadow_mode","enabled":false}')
    )
    assert runner.sink.shadow_mode is True
    assert runner.state.flags["shadow_mode"] is True


async def test_a_second_runner_cannot_restore_or_subscribe_to_the_same_book(monkeypatch):
    candle_store = CandleStore(completed_bars(2))
    first_runner, _, _ = build_runner(monkeypatch, candle_store)
    second_runner, _, _ = build_runner(monkeypatch, candle_store)
    await first_runner.start()
    try:
        with pytest.raises(RuntimeError, match="ownership is already held"):
            await second_runner.start()
        assert second_runner.bus.handlers == {}
        assert candle_store.owner_id == first_runner._owner_id
    finally:
        await first_runner.stop()
    assert candle_store.owner_id is None
    next_runner, _, _ = build_runner(monkeypatch, candle_store)
    await next_runner.start()
    await next_runner.stop()


async def test_ownership_loss_blocks_the_next_order_and_requests_shutdown():
    runner, strategy_slot = _runner(), _slot()
    runner.state.owner_id = "OTHER_RUNNER"
    with pytest.raises(RuntimeError, match="no longer owns"):
        await _enter(runner, strategy_slot)
    assert runner.signals.rows == []
    assert runner.sink.delivery_ids == []
    assert runner._stopping.is_set()


async def test_position_failure_recovers_without_sending_a_confirmed_order_again(monkeypatch):
    runner, strategy_slot = _runner(), _slot()
    runner.slots = [strategy_slot]
    original_upsert = runner.positions.upsert

    async def fail_upsert(position):
        return False

    monkeypatch.setattr(runner.positions, "upsert", fail_upsert)
    await _enter(runner, strategy_slot)
    assert runner.signals.pending[0].delivery_status == "sent_pending"
    assert strategy_slot.key in runner._uncertain_pairs
    monkeypatch.setattr(runner.positions, "upsert", original_upsert)
    await runner._recover_pending_deliveries()
    assert len(runner.sink.delivery_ids) == 1
    assert runner.signals.pending[0].delivery_status == "sent"
    assert strategy_slot.key not in runner._uncertain_pairs
    assert runner.positions.held[strategy_slot.key].remaining == 6.0


@pytest.mark.parametrize("failed_status", ["sent_pending", "sent", "failed"])
@pytest.mark.parametrize("commit_before_failure", [False, True])
async def test_failed_checkpoints_remain_blocked_and_retry_local_work_only(
    monkeypatch, failed_status, commit_before_failure
):
    runner, strategy_slot = (
        _runner(sink=FailedSink() if failed_status == "failed" else None),
        _slot(),
    )
    runner.slots = [strategy_slot]
    original_mark = runner.signals.mark_delivery

    async def fail_mark(delivery_id, *, status, **delivery_fields):
        if status == failed_status:
            if commit_before_failure:
                await original_mark(delivery_id, status=status, **delivery_fields)
            return False
        return await original_mark(delivery_id, status=status, **delivery_fields)

    monkeypatch.setattr(runner.signals, "mark_delivery", fail_mark)
    await _enter(runner, strategy_slot)
    assert strategy_slot.key in runner._uncertain_pairs
    monkeypatch.setattr(runner.signals, "mark_delivery", original_mark)
    await runner._recover_pending_deliveries()
    assert strategy_slot.key not in runner._uncertain_pairs
    assert runner.signals.pending[0].delivery_status == (
        "failed" if failed_status == "failed" else "sent"
    )
    if failed_status != "failed":
        assert len(runner.sink.delivery_ids) == 1


async def test_pre_send_checkpoint_failure_prevents_any_broker_call(monkeypatch):
    runner, strategy_slot = _runner(), _slot()

    async def fail_mark(*arguments, **keywords):
        return False

    monkeypatch.setattr(runner.signals, "mark_delivery", fail_mark)
    await _enter(runner, strategy_slot)
    assert runner.sink.delivery_ids == []
    assert strategy_slot.key in runner._uncertain_pairs


async def test_restart_reconciles_a_confirmed_partial_exactly_once(monkeypatch):
    runner, strategy_slot = _runner(), _slot()
    await _enter(runner, strategy_slot)
    original_mark = runner.signals.mark_delivery

    async def fail_final(delivery_id, *, status, **delivery_fields):
        if status == "sent":
            return False
        return await original_mark(delivery_id, status=status, **delivery_fields)

    monkeypatch.setattr(runner.signals, "mark_delivery", fail_final)
    await _close(runner, strategy_slot, SignalAction.TP1, 2345, quantity=1.8)
    replacement = _runner(state=runner.state, positions=runner.positions)
    restored_slot = _slot()
    replacement.slots = [restored_slot]
    replacement.signals = RecordingSignals(runner.signals.pending)
    await replacement._restore_position(restored_slot)
    await replacement._recover_pending_deliveries()
    assert restored_slot.factory.open_position("XAUUSD").remaining == pytest.approx(4.2)
    assert replacement.sink.delivery_ids == []


@pytest.mark.parametrize("status", ["pending", "unknown"])
async def test_legacy_or_unknown_live_delivery_is_not_blindly_resent(status):
    runner, strategy_slot = _runner(), _slot()
    runner.slots = [strategy_slot]
    runner.signals = RecordingSignals([pending_signal(strategy_slot, status=status)])
    await runner._recover_pending_deliveries()
    assert runner.sink.delivery_ids == []
    assert strategy_slot.key in runner._uncertain_pairs


@pytest.mark.parametrize("age_seconds,expected_sends", [(10, 1), (300, 0), (-10, 0)])
async def test_explicit_retry_horizon_bounds_ambiguous_resends(
    monkeypatch, age_seconds, expected_sends
):
    monkeypatch.setattr(runner_settings, "delivery_retry_max_age", 60)
    runner, strategy_slot = _runner(), _slot()
    runner.slots = [strategy_slot]
    runner.signals = RecordingSignals(
        [
            pending_signal(
                strategy_slot,
                status="unknown",
                created_at=datetime.now(UTC) - timedelta(seconds=age_seconds),
            )
        ]
    )
    await runner._recover_pending_deliveries()
    assert len(runner.sink.delivery_ids) == expected_sends


async def test_a_rejected_retry_does_not_disprove_the_original_ambiguous_send(monkeypatch):
    monkeypatch.setattr(runner_settings, "delivery_retry_max_age", 60)
    runner, strategy_slot = _runner(sink=FailedSink()), _slot()
    runner.slots = [strategy_slot]
    runner.signals = RecordingSignals([pending_signal(strategy_slot, status="unknown")])
    await runner._recover_pending_deliveries()
    assert runner.signals.pending[0].delivery_status == "unknown"
    assert strategy_slot.key in runner._uncertain_pairs


async def test_blocked_first_page_does_not_starve_later_pairs():
    runner, blocked_slot = _runner(), _slot()
    blocked_rows = [pending_signal(blocked_slot, status="unknown") for _ in range(105)]
    later_slot = _slot()
    later_slot.symbol = "EURUSD"
    later_row = pending_signal(later_slot, status="sent_pending")
    later_row.created_at = max(record.created_at for record in blocked_rows) + timedelta(seconds=1)
    runner.slots = [blocked_slot, later_slot]
    runner.signals = RecordingSignals([*blocked_rows, later_row])
    await runner._recover_pending_deliveries()
    assert blocked_slot.key in runner._uncertain_pairs
    assert later_slot.key not in runner._uncertain_pairs
    assert later_row.delivery_status == "sent"
    assert runner.sink.delivery_ids == []


async def test_unfinished_older_row_blocks_later_row_of_the_same_pair():
    runner, strategy_slot = _runner(), _slot()
    earlier = pending_signal(strategy_slot, status="unknown")
    later_row = pending_signal(strategy_slot, shadow=True)
    later_row.created_at = earlier.created_at + timedelta(seconds=1)
    runner.slots = [strategy_slot]
    runner.signals = RecordingSignals([earlier, later_row])
    await runner._recover_pending_deliveries()
    assert later_row.delivery_status == "prepared"
    assert strategy_slot.factory.open_position("XAUUSD") is None


async def test_periodic_retry_revisits_pending_local_persistence(monkeypatch):
    monkeypatch.setattr(runner_settings, "delivery_retry_interval", 0.001)
    runner, strategy_slot = _runner(), _slot()
    runner.slots = [strategy_slot]
    runner.signals = RecordingSignals([pending_signal(strategy_slot, status="sent_pending")])
    original_track = runner._track_cycle

    async def stop_after_tracking(selected_slot):
        persisted = await original_track(selected_slot)
        runner.request_stop()
        return persisted

    monkeypatch.setattr(runner, "_track_cycle", stop_after_tracking)
    await asyncio.wait_for(runner._delivery_retry_loop(), timeout=1)
    assert runner.signals.pending[0].delivery_status == "sent"
    assert runner.sink.delivery_ids == []


async def test_recovery_refreshes_a_stale_scan_after_acquiring_the_pair_lock():
    runner, strategy_slot = _runner(), _slot()
    await _enter(runner, strategy_slot)
    stale_snapshot = copy(runner.signals.pending[0])
    stale_snapshot.delivery_status = "prepared"
    assert await runner._recover_delivery(strategy_slot, stale_snapshot)
    assert len(runner.sink.delivery_ids) == 1


@pytest.mark.parametrize("committed", [False, True])
async def test_failed_stage_is_reconciled_before_the_pair_can_emit_again(monkeypatch, committed):
    runner, strategy_slot = _runner(), _slot()
    runner.slots = [strategy_slot]
    original_stage = runner.signals.stage_signal

    async def lose_stage_response(signal, **delivery):
        if committed:
            await original_stage(signal, **delivery)
        return None

    monkeypatch.setattr(runner.signals, "stage_signal", lose_stage_response)
    await _enter(runner, strategy_slot)
    assert strategy_slot.key in runner._uncertain_pairs
    await _enter(runner, strategy_slot)
    assert len(runner.signals.pending) == int(committed)
    await runner._recover_pending_deliveries()
    assert strategy_slot.key not in runner._uncertain_pairs
    assert len(runner.sink.delivery_ids) == int(committed)


@pytest.fixture(autouse=True)
def live_state_scope(monkeypatch):
    """Select a live book; all broker transports in this suite are test doubles."""
    monkeypatch.setattr(settings, "env", "prod")
    monkeypatch.setattr(settings.state_config, "execution_mode", "live")
