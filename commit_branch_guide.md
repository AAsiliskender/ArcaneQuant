[commitupdate_branch_auto.ps1](/C:/Users/a_asi/FinanceProject/ArcaneQuant/commitupdate_branch_auto.ps1:1) takes your **uncommitted changes**, creates a new branch, bumps the version, updates the changelog, and publishes a pull request.

To use it yourself:

1. Start with your changes uncommitted, with `HEAD` matching the latest remote default branch. The script fetches and checks this; it refuses if you already have extra local commits or are behind.
2. Have Git, `bump2version`, and GPG signing configured. Authenticated GitHub CLI (`gh`) enables automatic PR creation.
3. Open PowerShell in the repository root and run:

```powershell
pytest
git diff --check
.\commitupdate_branch_auto.ps1
```

The script itself does **not** run tests, hence the separate `pytest` command.

It prompts you for:

- **Update type:** `patch`, `minor`, or `major`. From the current `0.7.0`, these produce `0.7.1`, `0.8.0`, or `1.0.0`.
- **New branch name:** for example, `fix/reference-resolution`. Leave it blank to generate `update/<version>-<message-slug>`.
- **Multiline message:** write the changelog body, without a version/date heading. Finish with **Enter, Ctrl+Z, Enter**.

That message becomes the final commit message and changelog text. Its first nonempty line supplies the PR title.

The script then:

1. Checks the base, proposed version, branch name, and existing PRs.
2. Creates the branch and stages **all pending changes**, including nonignored untracked files.
3. Makes separate commits for your changes, the version bump, and the changelog.
4. Squashes those three commits into **one signed commit**.
5. Rechecks that the base has not advanced, pushes the branch, and attempts to open its PR.

You may receive GPG password prompts during signing. Afterwards, your checkout remains on the new branch. **It never merges the PR or creates a release tag.**

A few practical details:

- Keep unrelated work out of the checkout: this stages everything.
- Only one version-changing PR against the base is allowed at a time; merge or resolve the previous one first.
- It detects the remote default branch automatically; `-BaseBranch` overrides it, and `-Remote` defaults to `origin`.
- If PR creation fails after pushing, use the printed compare link to open it manually.
- If it stops partway through, inspect the branch and commits before retrying. It preserves the partial state rather than rolling back.

You can also supply the inputs directly, with an existing message file **outside the repository**:

```powershell
.\commitupdate_branch_auto.ps1 `
    -BranchName "fix/reference-resolution" `
    -Part patch `
    -MessageFile "$env:TEMP\release-notes.txt"
```
