---
name: commit-update-branch-auto
description: Automatically publish a named ArcaneQuant branch with a version bump and open a pull request into mainline, using one version-changing PR at a time and deferring release tags until after merge.
compatibility: Requires PowerShell, Git, bump2version, configured GPG signing, and GitHub API access. Authenticated GitHub CLI (gh) enables automatic PR creation. Network access is required.
---

# Automatic branch update and pull request

Accept a new branch name, bump type (`patch`, `minor`, or `major`), and release message derived from the reviewed changes and user instructions. Accept an explicit remote/base when supplied; otherwise resolve the repository's remote default branch. Treat "mainline" as that actual branch, which may be named `master`; do not create or rename a base branch implicitly.

Invoke [commitupdate_branch_auto.ps1](../../../commitupdate_branch_auto.ps1) from the repository root. It follows the direct-release auto script's feature commit, version bump, changelog commit, and guarded squash sequence, then publishes only the new branch and tries to create its PR. It never merges or creates a release tag. Keep that implementation in the script rather than recreating its Git sequence in the skill.

## Visible execution setup

Prepare temporary logs outside the repository and a visible PowerShell output window before the workflow's shell checks, tests, and Git operations. Keep a tool-controlled terminal for inputs; mirror output through `Tee-Object` and display it using `Get-Content -Wait` in a `Start-Process -WindowStyle Normal` window. Codex handles ordinary inputs; the user handles required approvals and GPG passwords. Preserve actual exit statuses and treat slow signing as a wait.

## Mandatory preparation

1. Read the script, project guidance, [.bumpversion.cfg](../../../.bumpversion.cfg), and recent [CHANGELOG.md](../../../CHANGELOG.md) entries. Review staged, unstaged, and untracked work and record the original HEAD/branch, intended scope, remote, base, bump type, and expected version. The script stages all pending changes; isolate unrelated work rather than including it accidentally.
2. Establish authorization to create the branch, stage the reviewed work, make signed commits and version/changelog edits, squash only the script's three temporary commits, publish the named branch, and create its PR. Reuse existing authorization; obtain any missing authorization and execution approvals before launch. Merging and release tagging remain separate actions.
3. Check PowerShell, Git, bump2version, and GPG. Prefer authenticated `gh` for the script's GitHub reads and automatic PR creation. Without it, the script can check public PRs through the read-only API and publish a branch with a compare URL, but that is partial completion. Private repository/API failures stop preparation. Report missing access rather than installing tools or starting login without permission.
4. Require HEAD to match the freshly fetched remote base, with no merge/rebase in progress and no existing requested branch. The script enforces this before creating the branch and previews the version without changing files. If the source checkout is stale or includes unrelated commits, prepare an authorized isolated checkout with only the reviewed changes before invoking the script; do not reset or discard the originals.
5. Apply [write-changelog](../write-changelog/SKILL.md) to the reviewed source diff and user instructions. Request body-only notes in the existing format, preserving verbatim wording. Save that exact body to a temporary UTF-8 message file outside the repository. The script supplies the version/date heading and writes CHANGELOG.md; do not pre-bump or pre-insert its entry.
6. Run `pytest` from the publishing checkout's root unless waived or covered by unchanged valid results. Use `pytest --color=yes` when piping to the visible display. Always run `git diff --check`; report limitations before launch. Enforce the sequential version policy below.

## Mandatory sequential version policy

- Permit only one open version-changing PR against the selected base at a time. List all open PRs for that base, following pagination, and inspect their changed files for `.bumpversion.cfg`, configured version files, and release-version changelog entries. Do not rely on titles, labels, or the first page alone.
- If a different version-changing PR is open, stop before bumping or publishing and report its URL. Wait for it to be merged or otherwise resolved; do not reserve a later version from an unmerged branch. On the next run, fetch the base again and derive the bump from the newly merged version.
- Resume an existing branch/PR only when it is the same authorized operation. Inspect its commit, version, changelog, and publication state first. Do not rerun the branch script to finish a partial operation: its existing-branch/base guards deliberately refuse that route.
- Create no release tag during branch publication. Release tags belong to the actual merged mainline commit, which can differ after a squash/rebase merge. Tagging after merge is a separate authorized action and must not bump the already merged version again.

## Mandatory execution

1. Invoke the existing script in the controlled terminal with the prepared live-output capture. Supply `-BranchName`, `-Part`, and `-MessageFile`; optionally supply `-BaseBranch` and `-Remote`. A direct invocation is `powershell.exe -NoProfile -File .\commitupdate_branch_auto.ps1 -BranchName <branch> -Part <part> -MessageFile <path>`. Pass parameters as arguments with correct quoting. Do not launch the direct-to-mainline auto script instead.
2. Supply the user's branch name, or let the script generate a relevant `update/<version>-<message-slug>` name when automatic naming is requested. Missing part/name/message inputs have interactive prompts; Codex handles them in the connected terminal. Message-file invocation avoids the multiline EOF interaction.
3. Let the script create the branch, make its three temporary commits, verify the reset target, squash to the signed final commit, recheck base/PR ordering, push only that branch without tags, and attempt PR creation. Do not repeat those operations manually. Wait for GPG passwords in GPG itself, never chat; quiet signing alone is not failure.
4. Capture the script's actual exit status and result. Check whether it printed a PR URL or only a compare URL. A compare link means the branch was published but PR creation remains unfinished; do not report a completed request or rerun the version bump. If authorized GitHub tools are available, finish only the missing PR creation with explicit repository/head/base/title/body and check for an existing request first.
5. Do not merge, enable auto-merge, or create a release tag. Leave the request for the user to# Mandatory verification and failure handling

- Verify the PR URL, open state, correct base/head repository and branches, head SHA, and actual changed files/version/changelog. Recheck the base tip and competing version-changing PRs after creation. If a race introduces a version conflict, leave/report the PR as needing refresh; do not claim it is ready to merge or invent a higher version from pending PRs.
- Confirm branch publication output and remote SHA, the signed commit's base parent and approved content, the proposed version, no new release tag, and a clean worktree in the publishing checkout. Report the PR URL, branch/base, version transition, test result or waiver, and any work preserved elsewhere.
- On a failed command, inspect actual state and stop dependent actions. If push fails, retry only branch publication after verification. If PR creation fails after a successful push, retain the branch, check for an already-created PR, and retry only creation. Do not rerun the bump or duplicate the PR.
- Resolve failed validation or a changed base before publishing. If a published PR needs refresh, inspect the latest base/version and obtain any missing authorization for its update; do not amend/force-push, replace tags, reset mainline, or auto-merge as recovery.
- Remove temporary message/body files after successful use; retain useful failure logs. Do not remove original work or worktrees containing pending changes. Leave merging and any post-merge tagging to their separate authorized workflows.

Use the explicit refspec and tag suppression described in the [Git push documentation](https://git-scm.com/docs/git-push); do not inherit the direct-release workflow's `--follow-tags` push.
