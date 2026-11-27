"""Exercise branch publication in disposable local Git repositories only.

Record signing requests while disabling real signing; cryptographic GPG
validation and live GitHub behavior are deliberately outside these tests.
"""

import json
import os
from pathlib import Path
import shutil
import subprocess

import pytest


SCRIPT = Path(__file__).resolve().parents[1] / "commitupdate_branch_auto.ps1"
GIT = shutil.which("git")
POWERSHELL = shutil.which("powershell") or shutil.which("pwsh")
pytestmark = pytest.mark.skipif(
    not GIT or not POWERSHELL, reason="Git and PowerShell are required"
)


# The script runs unchanged. Only signing, bump2version, and GitHub are replaced;
# every history, staging, fetch, and push operation uses real Git and local refs.
RUNNER = r"""
$ErrorActionPreference = 'Stop'
$global:QueueChecks = 0
function Invoke-RestMethod {
    [CmdletBinding()]
    param([string]$Uri, [object]$Headers)
    if ($env:TEST_SCENARIO -ne 'rest-pagination') { throw 'Network access is forbidden in this test.' }
    Write-Trace -Name rest -Arguments @($Uri)
    if ($Uri -match '/pulls/42/files') {
        $items = @([pscustomobject]@{filename = '.bumpversion.cfg'; patch = '+current_version = 0.8.0'})
    } elseif ($Uri -match '/pulls/\d+/files') {
        $items = @([pscustomobject]@{filename = 'README.md'; patch = '+Documentation.'})
    } elseif ($Uri -match '[?&]page=1$') {
        $items = @(100..199 | ForEach-Object {
            [pscustomobject]@{number = $_; html_url = "https://github.com/test-owner/test-repo/pull/$_"}
        })
    } elseif ($Uri -match '[?&]page=2$') {
        $items = @([pscustomobject]@{number = 42; html_url = 'https://github.com/test-owner/test-repo/pull/42'})
    } else {
        throw "Unexpected mocked REST endpoint: $Uri"
    }
    # Invoke-RestMethod on Windows PowerShell emits the JSON array as one item.
    Write-Output -NoEnumerate $items
}
function Write-Trace {
    param([string]$Name, [object[]]$Arguments)
    $record = @{command = $Name; arguments = @($Arguments)}
    Add-Content -LiteralPath $env:TEST_TRACE -Value ($record | ConvertTo-Json -Compress) -Encoding UTF8
}
function git {
    Write-Trace -Name git -Arguments @($args)
    if ($args.Count -ge 2 -and $args[0] -eq 'remote' -and $args[1] -eq 'get-url') {
        $global:LASTEXITCODE = 0
        'https://github.com/test-owner/test-repo.git'
        return
    }
    if ($env:TEST_SCENARIO -eq 'first-commit-fails' -and $args[0] -eq 'commit' -and $args -contains '-S') {
        $global:LASTEXITCODE = 1
        return
    }
    $forwarded = @($args | Where-Object { $_ -ne '-S' })
    & $env:TEST_GIT -c commit.gpgsign=false @forwarded
    if ($LASTEXITCODE -eq 0 -and $env:TEST_SCENARIO -eq 'changelog-during-signing' -and
        $args[0] -eq 'commit' -and $args -contains '-S') {
        $changelog = Join-Path (Get-Location) 'CHANGELOG.md'
        [IO.File]::AppendAllText($changelog, "- Added while signing waited.`r`n", (New-Object Text.UTF8Encoding($false)))
    }
    $global:LASTEXITCODE = $LASTEXITCODE
}
function bump2version {
    Write-Trace -Name bump2version -Arguments @($args)
    if ($args -contains '--dry-run') {
        'current_version=0.7.0'
        'new_version=0.8.0'
        $global:LASTEXITCODE = 0
        return
    }
    if ($env:TEST_SCENARIO -eq 'bump-fails') {
        $global:LASTEXITCODE = 1
        return
    }
    $path = Join-Path (Get-Location) '.bumpversion.cfg'
    $content = [IO.File]::ReadAllText($path).Replace('current_version = 0.7.0', 'current_version = 0.8.0')
    [IO.File]::WriteAllText($path, $content, (New-Object Text.UTF8Encoding($false)))
    git add .bumpversion.cfg
    git commit -m 'Bump version: 0.7.0 to 0.8.0'
}
function gh {
    Write-Trace -Name gh -Arguments @($args)
    $global:LASTEXITCODE = 0
    if ($args[0] -eq '--version') { 'gh version test'; return }
    if ($args[0] -eq 'auth') { return }
    if ($args[0] -eq 'api') {
        if ($env:TEST_SCENARIO -eq 'rest-pagination') {
            $global:LASTEXITCODE = 1
            return
        }
        $endpoint = @($args | Where-Object { $_ -match 'repos/' }) | Select-Object -First 1
        if ($endpoint -match '/pulls\?' -and $endpoint -notmatch 'head=') { $global:QueueChecks++ }
        if ($endpoint -match '/pulls/42/files') {
            '[{"filename":".bumpversion.cfg","patch":"-current_version = 0.7.0\n+current_version = 0.8.0"}]'
        } elseif (($env:TEST_SCENARIO -eq 'competing-pr' -or
                  ($env:TEST_SCENARIO -eq 'pr-race' -and $global:QueueChecks -gt 1)) -and
                  $endpoint -match '/pulls') {
            '[{"number":42,"html_url":"https://github.com/test-owner/test-repo/pull/42","head":{"ref":"update/pending","sha":"pending"},"base":{"ref":"master"}}]'
        } else {
            '[]'
        }
        return
    }
    if ($args[0] -eq 'pr' -and $args[1] -eq 'create') {
        if ($env:TEST_SCENARIO -eq 'pr-fails') {
            $global:LASTEXITCODE = 1
        } else {
            'https://github.com/test-owner/test-repo/pull/43'
        }
        return
    }
    throw "Unexpected mocked gh command: $args"
}
Set-Location -LiteralPath $env:TEST_REPO
try {
    & $env:TEST_SCRIPT -BranchName $env:TEST_BRANCH -Part minor -MessageFile $env:TEST_MESSAGE
    exit 0
} catch {
    Write-Error -ErrorRecord $_ -ErrorAction Continue
    exit 1
}
"""


class BranchRun:
    def __init__(self, tmp_path):
        self.root = tmp_path
        self.repo = tmp_path / "working"
        self.remote = tmp_path / "remote.git"
        self.repo.mkdir()
        self.env = {key: value for key, value in os.environ.items() if not key.startswith("GIT_")}
        self.env.update(
            GIT_CONFIG_GLOBAL=str(tmp_path / "empty-gitconfig"),
            GIT_CONFIG_NOSYSTEM="1",
            GIT_TERMINAL_PROMPT="0",
        )
        (tmp_path / "empty-gitconfig").write_text("", encoding="utf-8")
        self.git("init", "--bare", str(self.remote))
        self.git("init", "--initial-branch=master")
        self.git("config", "user.name", "Branch script test")
        self.git("config", "user.email", "branch-test@example.invalid")
        self.git("config", "commit.gpgsign", "false")
        self.git("config", "core.autocrlf", "false")
        self.git("config", "core.whitespace", "cr-at-eol")
        self.git("config", "core.hooksPath", str(tmp_path / "empty-hooks"))
        self.git("--git-dir", str(self.remote), "symbolic-ref", "HEAD", "refs/heads/master")
        self.initial_changelog = (
            "##### CHANGELOG\r\n\r\n"
            "## [0.7.0] - 2026-11-07\r\n- Existing release.\r\n"
        )
        (self.repo / "CHANGELOG.md").write_bytes(self.initial_changelog.encode())
        (self.repo / ".bumpversion.cfg").write_text(
            "[bumpversion]\ncurrent_version = 0.7.0\ncommit = true\ntag = false\n",
            encoding="utf-8",
        )
        (self.repo / "feature.py").write_text("value = 1\n", encoding="utf-8")
        shutil.copy2(SCRIPT, self.repo / SCRIPT.name)
        self.git("add", ".")
        self.git("commit", "-m", "Initial release")
        self.base = self.git("rev-parse", "HEAD")
        self.git("remote", "add", "origin", str(self.remote))
        self.git("push", "--set-upstream", "origin", "master")
        (self.repo / "feature.py").write_text("value = 2\n", encoding="utf-8")
        self.message = '- Preserve $1, $&, and "quoted" text.\n    > Keep the literal detail.'
        (tmp_path / "message.txt").write_text(self.message, encoding="utf-8")
        (tmp_path / "runner.ps1").write_text(RUNNER, encoding="utf-8-sig")
        self.trace_path = tmp_path / "trace.jsonl"
        self.branch = "update/0.8.0-test-change"

    def git(self, *args):
        result = subprocess.run(
            [GIT, "-c", "commit.gpgsign=false", *args], cwd=self.repo, env=self.env, text=True,
            encoding="utf-8", errors="replace", capture_output=True, check=True,
        )
        return result.stdout.strip()

    def run(self, scenario="success"):
        env = self.env | {
            "TEST_REPO": str(self.repo),
            "TEST_SCRIPT": str(self.repo / SCRIPT.name),
            "TEST_GIT": GIT,
            "TEST_MESSAGE": str(self.root / "message.txt"),
            "TEST_TRACE": str(self.trace_path),
            "TEST_BRANCH": self.branch,
            "TEST_SCENARIO": scenario,
        }
        self.result = subprocess.run(
            [POWERSHELL, "-NoProfile", "-NonInteractive", "-File", str(self.root / "runner.ps1")],
            cwd=self.repo, env=env, text=True, encoding="utf-8", errors="replace",
            capture_output=True, timeout=60,
        )
        assert self.trace_path.exists(), self.output
        self.trace = [
            json.loads(line) for line in self.trace_path.read_text(encoding="utf-8-sig").splitlines()
        ]
        return self.result

    def calls(self, command):
        return [record["arguments"] for record in self.trace if record["command"] == command]

    @property
    def output(self):
        return self.result.stdout + self.result.stderr


@pytest.fixture
def branch_run(tmp_path):
    return BranchRun(tmp_path)


def test_publish_squashes_signed_requests_into_one_branch_commit(branch_run):
    result = branch_run.run()
    assert result.returncode == 0, branch_run.output
    head = branch_run.git("rev-parse", "HEAD")
    assert branch_run.git("log", "-1", "--format=%P") == branch_run.base
    assert branch_run.git("rev-parse", "master") == branch_run.base
    assert branch_run.git("--git-dir", str(branch_run.remote), "rev-parse", "master") == branch_run.base
    assert branch_run.git("--git-dir", str(branch_run.remote), "rev-parse", branch_run.branch) == head
    assert branch_run.git("tag", "--list") == ""
    assert branch_run.git("--git-dir", str(branch_run.remote), "tag", "--list") == ""
    assert branch_run.git("status", "--porcelain") == ""
    assert branch_run.git("log", "-1", "--format=%B") == branch_run.message
    cfg = (branch_run.repo / ".bumpversion.cfg").read_text(encoding="utf-8")
    assert "current_version = 0.8.0" in cfg
    changelog = (branch_run.repo / "CHANGELOG.md").read_bytes().decode("utf-8-sig")
    assert changelog.startswith("##### CHANGELOG\r\n\r\n## [0.8.0] - ")
    assert branch_run.message.replace("\n", "\r\n") in changelog
    assert changelog.endswith(branch_run.initial_changelog.split("\r\n\r\n", 1)[1])
    git_calls = branch_run.calls("git")
    signed = [call for call in git_calls if call[0] == "commit" and "-S" in call]
    assert len(signed) == 2
    bumps = [call for call in branch_run.calls("bump2version") if "--dry-run" not in call]
    assert len(bumps) == 1
    assert "--commit" in bumps[0] and "--no-tag" in bumps[0]
    pushes = [call for call in git_calls if call[0] == "push"]
    assert len(pushes) == 1
    assert "--no-follow-tags" in pushes[0]
    assert f"HEAD:refs/heads/{branch_run.branch}" in pushes[0]
    creations = [call for call in branch_run.calls("gh") if call[:2] == ["pr", "create"]]
    assert len(creations) == 1
    assert "--base" in creations[0] and "master" in creations[0]
    assert "--head" in creations[0] and branch_run.branch in creations[0]


def test_competing_version_pr_stops_before_branch_or_bump(branch_run):
    result = branch_run.run("competing-pr")
    assert result.returncode != 0, branch_run.output
    assert branch_run.git("branch", "--show-current") == "master"
    assert branch_run.git("rev-parse", "HEAD") == branch_run.base
    assert "current_version = 0.7.0" in (branch_run.repo / ".bumpversion.cfg").read_text()
    assert not any("--dry-run" not in call for call in branch_run.calls("bump2version"))
    assert not any(call[0] == "push" for call in branch_run.calls("git"))
    assert "https://github.com/test-owner/test-repo/pull/42" in branch_run.output
    assert (branch_run.repo / "feature.py").read_text() == "value = 2\n"


def test_rest_fallback_checks_later_pages_for_competing_version_pr(branch_run):
    result = branch_run.run("rest-pagination")
    assert result.returncode != 0, branch_run.output
    assert branch_run.git("branch", "--show-current") == "master"
    assert branch_run.git("rev-parse", "HEAD") == branch_run.base
    assert not any(call[0] == "push" for call in branch_run.calls("git"))
    assert "https://github.com/test-owner/test-repo/pull/42" in branch_run.output
    assert any("page=2" in call[0] for call in branch_run.calls("rest"))


def test_failed_feature_commit_stops_before_bump_and_push(branch_run):
    result = branch_run.run("first-commit-fails")
    assert result.returncode != 0, branch_run.output
    assert branch_run.git("rev-parse", "HEAD") == branch_run.base
    assert not any("--dry-run" not in call for call in branch_run.calls("bump2version"))
    assert not any(call[0] == "push" for call in branch_run.calls("git"))
    assert (branch_run.repo / "feature.py").read_text() == "value = 2\n"


def test_failed_bump_preserves_feature_commit_without_reset_or_push(branch_run):
    result = branch_run.run("bump-fails")
    assert result.returncode != 0, branch_run.output
    assert branch_run.git("log", "-1", "--format=%P") == branch_run.base
    assert branch_run.git("show", "HEAD:feature.py") == "value = 2"
    assert "current_version = 0.7.0" in (branch_run.repo / ".bumpversion.cfg").read_text()
    assert not any(call[0] in {"reset", "push"} for call in branch_run.calls("git"))


def test_changelog_edits_during_signing_are_preserved_without_reset_or_push(branch_run):
    result = branch_run.run("changelog-during-signing")
    assert result.returncode != 0, branch_run.output
    changelog = (branch_run.repo / "CHANGELOG.md").read_bytes().decode("utf-8")
    assert changelog == branch_run.initial_changelog + "- Added while signing waited.\r\n"
    assert "CHANGELOG.md changed during preparation" in branch_run.output
    assert not any(call[0] in {"reset", "push"} for call in branch_run.calls("git"))
    assert branch_run.git("--git-dir", str(branch_run.remote), "rev-parse", "master") == branch_run.base


def test_new_version_pr_before_push_preserves_local_commit_without_publication(branch_run):
    result = branch_run.run("pr-race")
    assert result.returncode != 0, branch_run.output
    assert branch_run.git("log", "-1", "--format=%P") == branch_run.base
    assert "current_version = 0.8.0" in (branch_run.repo / ".bumpversion.cfg").read_text()
    assert not any(call[0] == "push" for call in branch_run.calls("git"))
    assert branch_run.git("--git-dir", str(branch_run.remote), "branch", "--list", branch_run.branch) == ""
    assert "https://github.com/test-owner/test-repo/pull/42" in branch_run.output


def test_blank_branch_name_generates_relevant_versioned_branch(branch_run):
    branch_run.branch = ""
    result = branch_run.run()
    assert result.returncode == 0, branch_run.output
    generated = branch_run.git("branch", "--show-current")
    assert generated == "update/0.8.0-preserve-1-and-quoted-text"
    assert branch_run.git("--git-dir", str(branch_run.remote), "rev-parse", generated) == branch_run.git("rev-parse", "HEAD")


def test_pr_failure_preserves_published_branch_and_reports_follow_up(branch_run):
    branch_run.run("pr-fails")
    head = branch_run.git("rev-parse", "HEAD")
    assert head != branch_run.base, branch_run.output
    assert branch_run.git("--git-dir", str(branch_run.remote), "rev-parse", branch_run.branch) == head
    assert branch_run.git("--git-dir", str(branch_run.remote), "rev-parse", "master") == branch_run.base
    assert branch_run.git("tag", "--list") == ""
    assert "compare/" in branch_run.output
    assert not any(call[:2] == ["pr", "merge"] for call in branch_run.calls("gh"))
