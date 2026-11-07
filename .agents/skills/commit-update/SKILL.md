---
name: commit-update
description: Commit, version, sign, and push an ArcaneQuant update using the repository's existing commitupdate.ps1, including interactive prompts and verification.
---

# Commit update

Invoke [commitupdate.ps1](../../../commitupdate.ps1) from the repository root. Keep its implementation in that file; do not generate a replacement release script.

## Mandatory preparation

1. Read the script, [.bumpversion.cfg](../../../.bumpversion.cfg), and recent [CHANGELOG.md](../../../CHANGELOG.md) entries. Check that PowerShell, Git, bump2version, and the configured signing program are available.
2. Inspect staged, unstaged, and untracked changes. Record the starting HEAD, branch, configured upstream, and remote URL. Establish the intended change scope, version increment, resulting version, and commit text. Check that the release tag does not already exist.
3. Confirm authorization for committing all intended changes, squashing the script's temporary commits, creating the signed tag, and pushing to the intended destination. Reuse existing authorization; obtain missing authorization before launching. Invoking this skill alone grants no reset or push permission. Account for `git add .` staging all changes and `git push --follow-tags` publishing other reachable unpublished annotated tags.
4. Run `pytest` directly from the repository root, then `git diff --check`, using the visible-console approach below. The user's current environment supports this invocation; do not select individual files or cases by default. If an environment limitation prevents the full run, report it and identify any omitted checks.
5. Prepare the exact multiline message as changelog body text: use `- ` for changes and four spaces followed by `> ` for supporting details. Omit the version/date heading; the script supplies it. Use the same body for the final Git commit. Derive its contents from the actual diff.

## Visible consoles: local preference

- Run tests, the release script, and verification commands in a user-visible Command Prompt or PowerShell window by default. Reuse the same window and batch related checks where practical. Keep the results visible for the user to inspect.
- Prefer a visible terminal that tools can also control. Otherwise launch a desktop console with `Start-Process -WindowStyle Normal`, the repository root as `-WorkingDirectory`, and any required execution approval. A tool session's `tty: true` alone does not establish desktop visibility.
- To automate inputs while showing progress, run the existing script in a controlled `tty: true` session and mirror its output through `Tee-Object` to a temporary log displayed by a visible console with `Get-Content -Wait`. Keep the script's stdin connected to the controlled terminal. Explain that the visible window displays output; Codex enters the script inputs. Prefer this arrangement over a detached interactive console when supported.
- Capture output and explicit command completion/exit status in temporary logs outside the repository so Codex can monitor the visible run. Do not infer completion from the window closing or remaining open. Read summaries and relevant errors rather than repeatedly importing the entire log.
- Before launching a detached console that tools cannot control, tell the user which script prompts they must answer and provide the agreed message. Monitor its transcript and repository state; do not attempt `write_stdin` against an unrelated tool session. If visibility is unavailable, explain the limitation before using a tool-only terminal.

## Mandatory interactive execution

1. Invoke the existing `commitupdate.ps1` from the repository root using the visible-console arrangement above; use `powershell.exe -NoProfile -File .\commitupdate.ps1` for a direct console launch. Preserve the input-controlling session for the remaining prompts. Request execution escalation when required for opening the console, Git metadata writes, signing, or network access; do not bypass a denied permission.
2. Answer the update-type prompt with the agreed `patch`, `minor`, or `major`, then Enter.
3. Enter the prepared message with real newlines. End its final line with Enter, then send Ctrl+Z and Enter to complete `[Console]::In.ReadToEnd()`. Use `write_stdin` only for an actually connected, controllable terminal; use `\r` for Enter and `\u001a\r` for the final EOF sequence. In a detached console, have the user enter these inputs.
4. Monitor output while the script creates the initial commit, bumps the version, and updates the changelog. Treat quiet or slow GPG signing as a wait, not a failure. Tell the user when signing is awaiting their input; let them enter the password in the GPG prompt, never in chat. Signing may prompt again for the final commit or tag.
5. At `Starting commit squashing...`, check that the version and new changelog entry match the agreed release before pressing Enter. For the current script, verify exactly three temporary commits above the recorded starting HEAD and verify `HEAD~3` equals that HEAD. Stop on a mismatch.
6. Review the displayed reset target and temporary commits. Answer the reset prompt with `Y` and Enter only within the established authorization. Wait for the final signed commit and signed version tag to succeed.
7. At `Pushing to Git...`, verify that the intended commit/tag are ready and the push remains authorized, then press Enter. The push prompt takes Enter, not `Y`. Inspect actual Git push output; the script's final `Pushed` message does not establish success.

## Conditional failure handling

- On an explicit command error, stop advancing prompts and inspect HEAD, status, version, changelog, and tags. Preserve the work. Do not relaunch the script blindly: a partial run may already have bumped the version or created commits.
- Distinguish an explicit GPG error from a password wait. Do not cancel signing or alter GPG configuration merely because output is delayed. After a reported signing error, wait for the user to resolve signing access before retrying.
- If the first commit fails, inspect for later version/changelog commits: the current script does not stop after that first failure. Its three-commit history guard can then reject the reset. Never bypass the guard with an unchecked reset.
- If only tagging or pushing fails, verify the completed signed commit and retry only the unfinished action within its authorized scope. Do not bump again, overwrite an existing tag, force-push, or use a hard reset.
- If the history, worktree, authorization, or recovery target is uncertain, stop with the exact state and missing decision. Do not announce completion.

### Conditional recovery from temporary commits

Use this only when the normal script has stopped and authorization covers the recovery's branch rewrite, tag creation, and push. Reuse authorization that already covers these actions; otherwise obtain it before recovery.

1. Verify the captured branch and temporary tip, a clean worktree, the ancestry back to the recorded starting HEAD, the intended release tree, the exact version/message in the changelog, and an absent release tag. Account for every temporary commit; retain their IDs for recovery.
2. Create a signed replacement commit from that release tree with the recorded starting HEAD as its sole parent using `git commit-tree -S`. Supply the agreed body through a temporary UTF-8 message file with `-F`. Check signing success before moving the branch.
3. Move the captured branch with `git update-ref`, supplying the captured temporary tip as the expected old value. Abort if that comparison fails. This ordering leaves the branch unchanged when signing fails.
4. Complete only the missing signed tag and `git push --follow-tags` steps, checking each exit code and existing authorization. Remove the temporary message file. Do not rerun the version bump.

## Mandatory post-execution verification

- Check the final commit's full ID, sole parent equal to the recorded starting HEAD, exact message, and intended file changes. Check its embedded `gpgsig` and the annotated version tag's PGP signature; do not describe signature presence as independent cryptographic verification.
- Confirm the version tag resolves to that final commit and `.bumpversion.cfg` agrees with the new changelog heading. Check that the approved message appears once in that release entry.
- Confirm successful push output for both the branch and version tag. Compare the final HEAD with its upstream tracking ref; use `git ls-remote` if the remote outcome is ambiguous. Do not infer publication from local tag existence or the script's success banner.
- Confirm `git status --short --branch` shows a clean worktree and the expected upstream relationship. Report the commit link, version tag, validation result, and any unfinished action.

## User input points

- **Release choices or authorization, when missing:** obtain the version increment, message/scope, and permission for the temporary-history squash and remote push. Do not ask again when already authorized.
- **PowerShell/tool execution permission, when requested by the environment:** wait for the user to approve launching the script with the required access. Recovery or restricted diagnostics may require a separate tool approval; explain the specific action.
- **GPG password prompt, when shown:** wait for the user to enter their password directly in GPG. Allow time for signing to finish; repeat this wait if another signing prompt appears.
- **Script confirmations:** Codex may enter the update type, message/EOF, squash Enter, reset `Y`, and push Enter when the terminal is controllable and those actions are already authorized. In a detached visible console, the user enters them; tell the user when the required checks are complete before they advance the squash, reset, and push prompts. Obtain any missing authorization first.
- **Live-output mirror:** let Codex enter script inputs in the connected terminal; the user only handles required approvals and GPG password prompts. Do not ask the user to type into the output display.
