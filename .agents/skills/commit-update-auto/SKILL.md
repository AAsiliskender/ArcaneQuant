---
name: commit-update-auto
description: Run ArcaneQuant's commitupdate_auto.ps1 for an automatic versioned commit and push, with Codex supplying script inputs and the user handling GPG signing prompts.
compatibility: Requires PowerShell, Git, bump2version, configured GPG signing, and network access for remote Git operations.
---

# Automatic commit update

Invoke [commitupdate_auto.ps1](../../../commitupdate_auto.ps1) from the repository root. Use the existing implementation; do not generate a replacement release script. Use [commit-update-manual](../commit-update-manual/SKILL.md) when manual squash/reset confirmations are wanted. To publish a new branch and open a pull request instead, use [commit-update-branch-auto](../commit-update-branch-auto/SKILL.md).

## Mandatory preparation

1. Read the auto script, [.bumpversion.cfg](../../../.bumpversion.cfg), and recent [CHANGELOG.md](../../../CHANGELOG.md) entries. Check PowerShell, Git, bump2version, and configured GPG signing are available. Confirm the saved script announces pushing with `Write-Host`, not `Read-Host`; resolve a saved-file mismatch before execution.
2. Inspect staged, unstaged, and untracked changes. Record the starting HEAD, branch, upstream, remote URL, and intended file scope. Resolve the requested bump type and exact resulting version from the current configuration. Check local and remote state for an existing release tag or partial prior run; do not bump an already prepared release again.
3. Establish authorization for staging all intended changes, creating signed commits and a signed tag, squashing temporary history with a soft reset, and pushing to the intended destination. Reuse existing authorization; obtain any missing permission before launch. The auto script does not pause before its reset or push, so complete the scope, destination, and authorization checks before starting. Account for `git add .` and other reachable unpublished annotated tags included by `git push --follow-tags`.
4. Run `pytest` from the repository root using the visible-output setup below, unless the user has waived the tests or an unchanged valid result already covers the release. Run `git diff --check` regardless of a test waiver. Report skipped checks or environment limitations.
5. Apply [write-changelog](../write-changelog/SKILL.md) to the reviewed diff and user instructions to prepare the exact changelog body in the repository's existing format. Request body-only output and use it as the commit message; the script adds the version/date heading and writes CHANGELOG.md. Ask for a missing release choice only when it cannot be resolved from the request or existing authorization.

## Mandatory execution

1. Before launching a command, prepare a temporary log outside the repository and a separate visible PowerShell window displaying it with `Get-Content -Wait`. Arrange output capture through `Tee-Object` while preserving stdin in the controlled terminal. Use `Start-Process -WindowStyle Normal` for the display; explain that Codex handles inputs and the visible window displays progress. Apply this setup before the test run too; use `pytest --color=yes` when piping and preserve ANSI colour sequences in a compatible display.
2. Run the auto script in a persistent, input-controllable terminal with `tty: true` and the prepared output capture; use `powershell.exe -NoProfile -File .\commitupdate_auto.ps1` for a direct invocation. Obtain required execution escalation before launch. Propagate script exceptions and capture the underlying exit status when using wrappers.
3. Enter the agreed `patch`, `minor`, or `major`, then Enter. Enter the prepared multiline message, ending with Enter, Ctrl+Z, Enter. With `write_stdin`, send `\r` for Enter and `\u001a\r` for the final EOF sequence. Preserve the session.
4. Wait for signing and monitor output. Let the user enter passwords directly in GPG, never in chat. Treat quiet or slow signing as a wait; allow further GPG prompts for the final commit or tag. After the message/EOF, supply no squash Enter, reset `Y`, or push Enter: these stages run automatically, retaining the script's three-commit ancestry guard. `Pushing to Git.` is an announcement, not an input prompt.
5. Monitor through the automatic push and capture the actual Git output and exit status. Perform the completion checks after the script finishes; do not rely on a pre-push pause or infer success from the final banner or window state.

## Conditional failure handling

- On an actual command failure, stop sending inputs and inspect HEAD, status, version, changelog, and tags. Use exit status and Git state; red text or native stderr alone does not establish failure.
- If the initial commit or staging fails, inspect for subsequent version/changelog commits: those early commands lack exit-code guards. Preserve partial work and respect the history guard; do not blindly rerun the script or version bump.
- If completion verification fails, determine what was published before proposing a repair. Do not amend a published commit, overwrite/recreate its tag, or roll back remotely automatically. Report the state and obtain any missing authorization for a concrete repair.
- If only the push fails, verify the existing release and retry only the push within the same authorization. Do not reset, bump again, or force-push. If any other recovery target or scope is uncertain, stop and report the unfinished stage.

## Mandatory completion checks

- Require the final commit's sole parent to equal the recorded starting HEAD and its exact message and file changes to match the intended release.
- Check `.bumpversion.cfg` and the single new changelog entry agree with the target version and approved message, including any verbatim user text.
- Check the commit contains a GPG signature, the version tag is annotated and signed, and the tag resolves to the final commit. Distinguish signature presence from independent cryptographic verification.
- Confirm the push published both the intended branch and version tag. Compare HEAD with its upstream tracking ref; use `git ls-remote` when the remote outcome is ambiguous. Check `git status --short --branch` for a clean worktree and the expected upstream relationship.
- Report the commit link, version tag, test result or waiver, and any pending work. Keep results visible for inspection and remove temporary message files when no longer needed. Require human input only for unresolved choices/authorization, environment approval dialogs, and GPG passwords; Codex supplies only the initial bump type and message/EOF to the script.
