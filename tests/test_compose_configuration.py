"""Render Compose with synthetic configuration; no daemon or live services needed."""

import json
import os
import shutil
import subprocess

import pytest
from sqlalchemy.engine import make_url

from qte_shared.config import REPO_ROOT, PostgresSettings

APPLICATIONS = ("data-ingestion", "strategy-runner", "db-migrate")


@pytest.fixture(scope="module")
def docker_command():
    executable = shutil.which("docker")
    if executable is None:
        pytest.skip("Docker CLI is unavailable for Compose rendering")
    version = subprocess.run(
        [executable, "compose", "version"], capture_output=True, text=True, timeout=15
    )
    if version.returncode:
        pytest.skip("Docker Compose plugin is unavailable")
    return executable


@pytest.fixture
def render_compose(tmp_path, docker_command):
    def render_document(environment_values=None, *, overlay=None, profiles=(), services_only=False):
        environment_file = tmp_path / ".env"
        environment_file.write_text(
            "\n".join(
                f"{variable}='{setting}'"
                for variable, setting in (environment_values or {}).items()
            )
            + "\n",
            encoding="utf-8",
        )
        process_environment = {
            variable: setting
            for variable, setting in os.environ.items()
            if not variable.startswith(("QTE_", "POSTGRES_", "COMPOSE_", "DOCKER_"))
        }
        command = [
            docker_command,
            "compose",
            "--project-directory",
            str(tmp_path),
            "--env-file",
            str(environment_file),
            "-f",
            str(REPO_ROOT / "docker-compose.yml"),
        ]
        if overlay:
            command.extend(["-f", str(REPO_ROOT / overlay)])
        for profile in profiles:
            command.extend(["--profile", profile])
        command.extend(
            ["config", "--services"] if services_only else ["config", "--format", "json"]
        )
        completed = subprocess.run(
            command, env=process_environment, capture_output=True, text=True, timeout=20
        )
        assert completed.returncode == 0, completed.stderr
        return completed.stdout.splitlines() if services_only else json.loads(completed.stdout)

    return render_document


@pytest.mark.parametrize(
    "credentials",
    [
        {},
        {
            "POSTGRES_USER": "reader@desk",
            "POSTGRES_PASSWORD": 'synthetic@:/?#%$&" value',
            "POSTGRES_DB": "custom_audit",
            "QTE_POSTGRES__DSN": "postgresql+asyncpg://ignored:ignored@localhost/host_only",
        },
    ],
)
def test_application_and_migration_urls_match_postgres_credentials(
    render_compose, monkeypatch, credentials
):
    document = render_compose(credentials)
    database_environment = document["services"]["postgres-audit"]["environment"]
    for application in APPLICATIONS:
        application_environment = document["services"][application]["environment"]
        for variable, setting in application_environment.items():
            monkeypatch.setenv(variable, setting)
        database_url = make_url(PostgresSettings().dsn)
        assert database_url.username == database_environment["POSTGRES_USER"]
        assert database_url.password == database_environment["POSTGRES_PASSWORD"]
        assert database_url.database == database_environment["POSTGRES_DB"]
        assert database_url.host == "postgres-audit"
        assert database_url.port == 5432


def test_explicit_container_dsn_reaches_every_application(render_compose, monkeypatch):
    explicit_dsn = "postgresql+asyncpg://external:synthetic%40pass@database.example:6432/audit"
    document = render_compose({"QTE_CONTAINER_POSTGRES_DSN": explicit_dsn})
    for application in APPLICATIONS:
        for variable, setting in document["services"][application]["environment"].items():
            monkeypatch.setenv(variable, setting)
        assert PostgresSettings().dsn == explicit_dsn


def test_default_startup_excludes_simulator_and_production_overlay_sets_mode(render_compose):
    services = render_compose({"QTE_ENV": "prod"}, services_only=True)
    assert "market-simulator" not in services
    assert "strategy-audit" not in services
    assert set(APPLICATIONS).issubset(services)
    document = render_compose({"QTE_ENV": "dev"}, overlay="docker-compose.prod.yml")
    for application in APPLICATIONS:
        assert document["services"][application]["environment"]["QTE_ENV"] == "prod"


def test_dev_profile_starts_simulator_in_dev_even_with_a_production_dotenv(render_compose):
    document = render_compose(
        {"QTE_ENV": "prod"}, overlay="docker-compose.dev.yml", profiles=("dev",)
    )
    for application in (*APPLICATIONS, "market-simulator"):
        assert document["services"][application]["environment"]["QTE_ENV"] == "dev"


def test_infrastructure_recovers_with_apps_and_migration_remains_one_shot(render_compose):
    document = render_compose()
    for service_name in ("redis-cache", "postgres-audit", "nats", *APPLICATIONS[:2]):
        assert document["services"][service_name]["restart"] == "unless-stopped"
    assert document["services"]["db-migrate"]["restart"] == "no"
