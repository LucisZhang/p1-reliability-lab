from __future__ import annotations

from harness.config import REPO_ROOT


def test_remote_sync_excludes_credentials_git_and_existing_results() -> None:
    script = (REPO_ROOT / "scripts" / "remote" / "broker-remote.sh").read_text(encoding="utf-8")
    assert "--exclude=/.git/" in script
    assert "--exclude=/.env" in script
    assert "--exclude='/showcase/results/*.json'" in script
    assert "broker_parity.json" in script


def test_remote_launcher_is_disconnect_safe_and_guarded() -> None:
    launcher = (REPO_ROOT / "scripts" / "remote" / "launch-broker-target.sh").read_text(
        encoding="utf-8"
    )
    guard = (REPO_ROOT / "scripts" / "remote" / "shared-host-guard.sh").read_text(encoding="utf-8")
    assert "tmux new-session -d" in launcher
    assert "shared-host-guard.sh" in launcher
    assert "if command -v nvidia-smi" in guard
    assert "nvidia-smi unavailable; dedicated CPU-only VM, GPU check skipped" in guard
    assert "nvidia-smi is required" not in guard
    assert "/proc/loadavg" in guard
    assert "load <= cpus / 2.0" in guard


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
