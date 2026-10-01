"""The container identity that decides whether a stale claim is provably dead."""

from __future__ import annotations

from qte_strategy_engine import instance
from qte_strategy_engine.instance import container_instance_token


def as_container_main_process(monkeypatch) -> None:
    monkeypatch.setattr(instance.os, "getpid", lambda: 1)


def test_a_process_that_is_not_pid_one_has_no_token(tmp_path):
    # A host process or `docker exec` can run beside the claim's holder.
    instance_file = tmp_path / "runner.instance"
    assert container_instance_token(str(instance_file)) is None
    assert not instance_file.exists()


def test_the_token_survives_a_restart_of_the_same_container(tmp_path, monkeypatch):
    as_container_main_process(monkeypatch)
    instance_file = tmp_path / "runner.instance"
    first_token = container_instance_token(str(instance_file))
    assert first_token
    assert container_instance_token(str(instance_file)) == first_token


def test_a_recreated_container_gets_a_different_token(tmp_path, monkeypatch):
    as_container_main_process(monkeypatch)
    first_token = container_instance_token(str(tmp_path / "first.instance"))
    second_token = container_instance_token(str(tmp_path / "second.instance"))
    assert first_token != second_token


def test_an_unwritable_path_yields_no_token(tmp_path, monkeypatch):
    as_container_main_process(monkeypatch)
    assert container_instance_token(str(tmp_path / "missing" / "runner.instance")) is None


def test_an_empty_token_file_yields_no_token(tmp_path, monkeypatch):
    # A crash between creating the file and writing it must not become the
    # empty prefix that matches every holder.
    as_container_main_process(monkeypatch)
    instance_file = tmp_path / "runner.instance"
    instance_file.write_text("", encoding="utf-8")
    assert container_instance_token(str(instance_file)) is None
