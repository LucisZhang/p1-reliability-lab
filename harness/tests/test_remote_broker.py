from __future__ import annotations

from harness.config import REPO_ROOT


def test_remote_sync_excludes_credentials_git_and_existing_results() -> None:
    script = (REPO_ROOT / "scripts" / "remote" / "broker-remote.sh").read_text(encoding="utf-8")
    assert "--exclude=/.git/" in script
    assert "--exclude=/.env" in script
    assert "--exclude='/showcase/results/*.json'" in script
    assert "--exclude=/infra/debezium/target/" in script
    assert "broker_parity.json" in script
    assert "schema_contract_drill.json" in script
    for artifact in (
        "broker_restart_drill.json",
        "duplicate_redelivery_drill.json",
        "ordering_miskey_drill.json",
        "poison_dlq_drill.json",
        "offset_replay_drill.json",
        "broker_slo.json",
    ):
        assert artifact in script
    assert "phase-b3-*.log" in script
    assert "phase-b4-*.log" in script
    assert "cmp -s" in script
    assert "will not be overwritten" in script


def test_remote_launcher_is_disconnect_safe_and_guarded() -> None:
    launcher = (REPO_ROOT / "scripts" / "remote" / "launch-broker-target.sh").read_text(
        encoding="utf-8"
    )
    guard = (REPO_ROOT / "scripts" / "remote" / "shared-host-guard.sh").read_text(encoding="utf-8")
    assert "tmux new-session -d" in launcher
    assert 'tmux has-session -t "${session}"' in launcher
    assert "remote target session disappeared before writing status" in launcher
    assert "shared-host-guard.sh" in launcher
    assert "remote completed:" in launcher
    assert "remote target provenance: git_sha=${git_sha}" in launcher
    assert 'phase_tag="b2"' in launcher
    assert "schema_contract_drill.json" in launcher
    assert 'phase_tag="b3-broker-restart"' in launcher
    assert 'phase_tag="b3-duplicate-redelivery"' in launcher
    assert 'phase_tag="b3-ordering-miskey"' in launcher
    assert 'phase_tag="b3-poison-dlq"' in launcher
    assert 'phase_tag="b3-offset-replay"' in launcher
    assert 'phase_tag="b4"' in launcher
    assert "broker_slo.json" in launcher
    assert "if command -v nvidia-smi" in guard
    assert "nvidia-smi unavailable; dedicated CPU-only VM, GPU check skipped" in guard
    assert "nvidia-smi is required" not in guard
    assert "/proc/loadavg" in guard
    assert "load_value <= cpu_count / 2.0" in guard
    assert "-v load=" not in guard


def test_makefile_exposes_b1_remote_and_fresh_environment_targets() -> None:
    makefile = (REPO_ROOT / "Makefile").read_text(encoding="utf-8")
    for target in (
        "broker-up:",
        "broker-verify:",
        "sync-up:",
        "sync-down:",
        "remote-broker-up:",
        "remote-broker-verify:",
    ):
        assert target in makefile
    assert "ENV_FILE=.env.example" not in makefile
    assert "BROKER_LONG_RUNNING_SERVICES :=" in makefile
    assert "build-debezium-avro-plugin:" in makefile
    assert "build-debezium-jmx-agent:" in makefile
    assert (
        "broker-up: ensure-env preflight-broker build-debezium-avro-plugin "
        "build-debezium-jmx-agent"
    ) in makefile
    assert "--profile broker run --rm minio-init" in makefile


def test_remote_runner_supports_guarded_fresh_b2_bringup() -> None:
    runner = (REPO_ROOT / "scripts" / "remote" / "run-broker-target.sh").read_text(encoding="utf-8")
    assert '"--phase contracts --fresh"' in runner
    assert "make down ENV_FILE=.env.example" in runner
    assert "make broker-up ENV_FILE=.env.example" in runner


def test_remote_runner_supports_guarded_fresh_b3_bringup() -> None:
    runner = (REPO_ROOT / "scripts" / "remote" / "run-broker-target.sh").read_text(encoding="utf-8")
    assert '"--phase failures --fresh"' in runner
    assert "make down ENV_FILE=.env.example" in runner


def test_remote_runner_supports_guarded_fresh_b4_bringup() -> None:
    runner = (REPO_ROOT / "scripts" / "remote" / "run-broker-target.sh").read_text(encoding="utf-8")
    assert '"--phase slo --fresh"' in runner
    assert "make down ENV_FILE=.env.example" in runner
    assert "python3.11 -m venv .venv" in runner
    assert "--requirement harness/requirements.txt" in runner
    assert 'pyiceberg.__version__ == "0.9.1"' in runner
    assert 'python_override=("PYTHON=${PWD}/.venv/bin/python")' in runner
