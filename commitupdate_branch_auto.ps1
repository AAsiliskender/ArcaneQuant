# commitupdate_branch_auto.ps1
# Prepare and publish a versioned branch, then try to open a PR. Never merge it.
[CmdletBinding()]
param(
    [string]$BranchName,
    [ValidateSet('patch', 'minor', 'major')][string]$Part,
    [string]$MessageFile,
    [string]$BaseBranch,
    [string]$Remote = 'origin'
)

$ErrorActionPreference = 'Stop'

function Invoke-CheckedNative {
    param([string]$Command, [string[]]$Arguments)
    # Windows PowerShell can render native stderr as error records even on success.
    $ErrorActionPreference = 'Continue'
    $output = & $Command @Arguments
    if ($LASTEXITCODE -ne 0) { throw "$Command failed with exit code $LASTEXITCODE. Inspect the current state before retrying." }
    $output
}

function Get-GitHubJson {
    param([string]$Endpoint)
    if ($ghAvailable) {
        try {
            $json = (Invoke-CheckedNative 'gh' @('api', $Endpoint)) -join "`n"
            return ($json | ConvertFrom-Json)
        } catch {
            Write-Verbose 'Authenticated GitHub read failed; trying the public read-only API.'
        }
    }
    try {
        $response = Invoke-RestMethod -Uri "https://api.github.com/$Endpoint" -Headers @{
            Accept = 'application/vnd.github+json'
            'User-Agent' = 'ArcaneQuant-commit-update'
            'X-GitHub-Api-Version' = '2022-11-28'
        } -ErrorAction Stop
        # Emit array members individually so pagination sees the actual page size.
        $response
    } catch {
        throw 'Cannot verify GitHub PR/version ordering. Configure gh authentication or restore API access before retrying.'
    }
}

function Get-GitHubPages {
    param([string]$Endpoint)
    $separator = if ($Endpoint.Contains('?')) { '&' } else { '?' }
    for ($page = 1; ; $page++) {
        $items = @(Get-GitHubJson "$Endpoint${separator}per_page=100&page=$page")
        $items
        if ($items.Count -lt 100) { break }
    }
}

function Assert-NoVersionPullRequest {
    $baseQuery = [uri]::EscapeDataString($BaseBranch)
    $pulls = @(Get-GitHubPages "repos/$repository/pulls?state=open&base=$baseQuery")
    foreach ($pull in $pulls) {
        $files = @(Get-GitHubPages "repos/$repository/pulls/$($pull.number)/files")
        if ($files.Count -ge 3000) { throw "Cannot fully inspect PR $($pull.html_url); resolve its release order first." }
        foreach ($file in $files) {
            $changesVersion = $versionFiles -contains $file.filename -or $versionFiles -contains $file.previous_filename
            $changesReleaseNotes = $file.filename -eq 'CHANGELOG.md' -and (
                -not $file.patch -or $file.patch -match '(?m)^\+## \[\d+\.\d+\.\d+\]'
            )
            if ($changesVersion -or $changesReleaseNotes) {
                throw "Merge or resolve the existing version-changing PR first: $($pull.html_url)"
            }
        }
    }
}

function Get-ExistingPullRequest {
    $headQuery = [uri]::EscapeDataString("${repositoryOwner}:$BranchName")
    $baseQuery = [uri]::EscapeDataString($BaseBranch)
    Get-GitHubPages "repos/$repository/pulls?state=open&head=$headQuery&base=$baseQuery" |
        Where-Object { $_.head.ref -eq $BranchName -and $_.base.ref -eq $BaseBranch } |
        Select-Object -First 1
}

$messagePath = $null
$bodyPath = $null
if ($MessageFile) { $MessageFile = [IO.Path]::GetFullPath($MessageFile) }
Push-Location -LiteralPath $PSScriptRoot
try {
    foreach ($command in @('git', 'bump2version')) {
        if (-not (Get-Command $command -ErrorAction SilentlyContinue)) { throw "$command is required on PATH." }
    }
    $ghAvailable = [bool](Get-Command gh -ErrorAction SilentlyContinue)

    # Get the same update type and multiline changelog/commit message as the auto script.
    if (-not $Part) { $Part = (Read-Host 'Enter the update type (patch/minor/major)').Trim().ToLowerInvariant() }
    if (@('patch', 'minor', 'major') -notcontains $Part) { throw 'Invalid update type.' }
    if (-not $PSBoundParameters.ContainsKey('BranchName')) {
        $BranchName = Read-Host 'Enter a new branch name (blank generates one from the commit message)'
    }
    if ($MessageFile) {
        $msg = [IO.File]::ReadAllText($MessageFile)
    } else {
        Write-Host 'Enter the commit message. Finish with Enter (new line), Ctrl+Z then Enter:'
        $msg = [Console]::In.ReadToEnd()
    }
    if ([string]::IsNullOrWhiteSpace($msg)) { throw 'The commit message cannot be empty.' }
    $msg = $msg.TrimEnd([char[]]"`r`n")

    # Resolve the real mainline name and require a fresh base without unrelated commits.
    $startCommit = Invoke-CheckedNative 'git' @('rev-parse', 'HEAD')
    Invoke-CheckedNative 'git' @('symbolic-ref', '--short', 'HEAD') | Out-Host
    if (@(Invoke-CheckedNative 'git' @('ls-files', '--unmerged')).Count) { throw 'Resolve unmerged files first.' }
    foreach ($stateFile in @('MERGE_HEAD', 'CHERRY_PICK_HEAD', 'REVERT_HEAD', 'rebase-merge', 'rebase-apply')) {
        $statePath = Invoke-CheckedNative 'git' @('rev-parse', '--git-path', $stateFile)
        if (Test-Path -LiteralPath $statePath) { throw 'Finish the current Git operation before publishing a branch.' }
    }
    if (-not $BaseBranch) {
        $remoteHead = (Invoke-CheckedNative 'git' @('ls-remote', '--symref', $Remote, 'HEAD')) -join "`n"
        $baseMatch = [regex]::Match($remoteHead, '(?m)^ref: refs/heads/(.+)\s+HEAD$')
        if (-not $baseMatch.Success) { throw 'Cannot determine the remote default branch; supply -BaseBranch.' }
        $BaseBranch = $baseMatch.Groups[1].Value.Trim()
    }
    Write-Host "Fetching $Remote and checking $BaseBranch before creating a branch"
    Invoke-CheckedNative 'git' @('fetch', '--tags', $Remote) | Out-Host
    $baseRef = "refs/remotes/$Remote/$BaseBranch"
    $baseCommit = Invoke-CheckedNative 'git' @('rev-parse', '--verify', $baseRef)
    if ($startCommit -ne $baseCommit) {
        throw "HEAD must match the fetched $Remote/$BaseBranch. Preserve and reconcile your changes before retrying."
    }
    $remoteUrl = Invoke-CheckedNative 'git' @('remote', 'get-url', $Remote)
    $repositoryMatch = [regex]::Match($remoteUrl, '^(?:https://github\.com/|git@github\.com:)([^/]+)/([^/]+?)(?:\.git)?$')
    if (-not $repositoryMatch.Success) { throw 'This script requires a github.com remote.' }
    $repositoryOwner = $repositoryMatch.Groups[1].Value
    $repository = "$repositoryOwner/$($repositoryMatch.Groups[2].Value)"

    $baseConfig = (Invoke-CheckedNative 'git' @('show', "${baseCommit}:.bumpversion.cfg")) -join "`n"
    $baseVersion = [regex]::Match($baseConfig, '(?m)^current_version\s*=\s*(\d+\.\d+\.\d+)\s*$').Groups[1].Value
    $versionFiles = @('.bumpversion.cfg') + @([regex]::Matches($baseConfig, '(?m)^\[bumpversion:file:(.+)\]\s*$') |
        ForEach-Object { $_.Groups[1].Value.Trim().Replace('\', '/') })
    $preview = (Invoke-CheckedNative 'bump2version' @('--config-file', '.bumpversion.cfg', '--dry-run', '--list', '--no-commit', '--no-tag', '--allow-dirty', $Part)) -join "`n"
    $currentVersion = [regex]::Match($preview, '(?m)^current_version=(\d+\.\d+\.\d+)\s*$').Groups[1].Value
    $vers = [regex]::Match($preview, '(?m)^new_version=(\d+\.\d+\.\d+)\s*$').Groups[1].Value
    if (-not $baseVersion -or $currentVersion -ne $baseVersion -or -not $vers -or $vers -eq $baseVersion) {
        throw 'The pending version must match the fetched base before a single version bump.'
    }
    if (@(Invoke-CheckedNative 'git' @('tag', '--list', $vers)).Count) { throw "Version tag $vers already exists." }
    $content = [IO.File]::ReadAllText((Join-Path $PSScriptRoot 'CHANGELOG.md'))
    $changelogStart = [regex]::Match($content, '\A##### CHANGELOG\r?\n\r?\n')
    if (-not $changelogStart.Success) { throw 'Cannot find the existing CHANGELOG heading.' }
    if ($content -match "(?m)^## \[$([regex]::Escape($vers))\]") { throw "Changelog entry $vers already exists; inspect the prior run." }

    $prTitle = (($msg -split '\r?\n' | Where-Object { $_.Trim() } | Select-Object -First 1) -replace '^\s*-\s*', '').Trim()
    if ([string]::IsNullOrWhiteSpace($prTitle)) { throw 'The commit message needs a descriptive first line.' }
    if ($prTitle.Length -gt 100) { $prTitle = $prTitle.Substring(0, 100).TrimEnd() }
    if ([string]::IsNullOrWhiteSpace($BranchName)) {
        $slug = ($prTitle.ToLowerInvariant() -replace '[^a-z0-9]+', '-').Trim('-')
        if (-not $slug) { $slug = 'changes' }
        if ($slug.Length -gt 60) { $slug = $slug.Substring(0, 60).TrimEnd('-') }
        $BranchName = "update/$vers-$slug"
    }
    $validBranch = Invoke-CheckedNative 'git' @('check-ref-format', '--branch', $BranchName)
    if ($validBranch -ne $BranchName -or $BranchName -eq $BaseBranch) { throw 'Choose a new feature branch name.' }
    if (@(Invoke-CheckedNative 'git' @('branch', '--list', $BranchName)).Count -or
        @(Invoke-CheckedNative 'git' @('ls-remote', '--heads', $Remote, "refs/heads/$BranchName")).Count) {
        throw "Branch $BranchName already exists. Inspect it instead of rerunning the version bump."
    }
    Assert-NoVersionPullRequest

    # 0. Create the branch; the original mainline ref stays at the starting commit.
    Write-Host "Creating $BranchName for $baseVersion -> $vers"
    Invoke-CheckedNative 'git' @('switch', '-c', $BranchName, $baseCommit) | Out-Host
    $messagePath = Join-Path ([IO.Path]::GetTempPath()) ("arcanequant-commit-" + [guid]::NewGuid().ToString('N') + '.txt')
    [IO.File]::WriteAllText($messagePath, $msg + "`n", [Text.UTF8Encoding]::new($false))

    # 1-2. Stage and make the signed feature/fix commit, as in commitupdate_auto.ps1.
    Write-Host 'Staging main changes'
    Invoke-CheckedNative 'git' @('add', '.') | Out-Host
    Write-Host 'Committing main changes with message'
    Invoke-CheckedNative 'git' @('commit', '-S', '-F', $messagePath) | Out-Host

    # 3. Make the normal bump commit, without creating a release tag before merge.
    Write-Host 'Bumping version without a release tag'
    Invoke-CheckedNative 'bump2version' @('--config-file', '.bumpversion.cfg', '--commit', '--no-tag', $Part) | Out-Host
    $actualConfig = [IO.File]::ReadAllText((Join-Path $PSScriptRoot '.bumpversion.cfg'))
    if ($actualConfig -notmatch "(?m)^current_version\s*=\s*$([regex]::Escape($vers))\s*$") { throw 'Unexpected version after bump.' }

    # 4. Insert the same changelog text, preserving literal $, UTF-8 BOM, and EOF.
    Write-Host 'Updating CHANGELOG.md with commit message'
    $changelogPath = Join-Path $PSScriptRoot 'CHANGELOG.md'
    if ([IO.File]::ReadAllText($changelogPath) -cne $content) {
        throw 'CHANGELOG.md changed during preparation. Its edits were preserved; inspect before retrying.'
    }
    $newline = if ($content.Contains("`r`n")) { "`r`n" } else { "`n" }
    $entryMessage = ($msg -replace '\r?\n', $newline)
    $newEntry = "## [$vers] - $(Get-Date -Format yyyy-MM-dd)$newline$entryMessage$newline$newline"
    $updatedContent = $content.Insert($changelogStart.Length, $newEntry)
    $oldBytes = [IO.File]::ReadAllBytes($changelogPath)
    $hasBom = $oldBytes.Length -ge 3 -and $oldBytes[0] -eq 239 -and $oldBytes[1] -eq 187 -and $oldBytes[2] -eq 191
    [IO.File]::WriteAllText($changelogPath, $updatedContent, [Text.UTF8Encoding]::new($hasBom))

    # 5-6. Stage and commit the changelog separately, as in the existing script.
    Write-Host 'Staging CHANGELOG.md changes'
    Invoke-CheckedNative 'git' @('add', '.') | Out-Host
    Invoke-CheckedNative 'git' @('diff', '--cached', '--check') | Out-Host
    Write-Host 'Committing CHANGELOG as empty'
    Invoke-CheckedNative 'git' @('commit', '--allow-empty-message', '-F', 'NUL') | Out-Host

    # 7. Squash only this run's three commits into one final signed commit.
    Write-Host 'Starting commit squashing by making soft reset.'
    $target = Invoke-CheckedNative 'git' @('rev-parse', 'HEAD~3')
    if ($target -ne $startCommit) { throw 'Unexpected commit history. Aborting reset.' }
    Invoke-CheckedNative 'git' @('log', '--oneline', "$target..HEAD") | Out-Host
    Invoke-CheckedNative 'git' @('reset', '--soft', $target) | Out-Host
    Invoke-CheckedNative 'git' @('commit', '-S', '-F', $messagePath) | Out-Host
    Invoke-CheckedNative 'git' @('diff', '--check', $baseCommit, 'HEAD') | Out-Host
    if (@(Invoke-CheckedNative 'git' @('status', '--porcelain')).Count) { throw 'Worktree changed during release preparation.' }

    # 8. Recheck the queue/base, then push this branch alone, never mainline/tags.
    Invoke-CheckedNative 'git' @('fetch', '--no-tags', $Remote) | Out-Host
    $latestBase = Invoke-CheckedNative 'git' @('rev-parse', '--verify', $baseRef)
    if ($latestBase -ne $baseCommit) { throw 'The base advanced. Refresh the proposed version before publishing.' }
    Assert-NoVersionPullRequest
    $commit = Invoke-CheckedNative 'git' @('rev-parse', 'HEAD')
    Write-Host "Publishing $BranchName without release tags"
    Invoke-CheckedNative 'git' @('push', '--no-follow-tags', '--set-upstream', $Remote, "HEAD:refs/heads/$BranchName") | Out-Host
    $published = (Invoke-CheckedNative 'git' @('ls-remote', '--heads', $Remote, "refs/heads/$BranchName")) -join "`n"
    if ($published -notmatch "^$commit\s+") { throw 'Published branch does not match the prepared commit.' }

    # 9. Open the request if possible; merging remains the user's decision.
    $compareUrl = "https://github.com/$repository/compare/$([uri]::EscapeDataString($BaseBranch))...$([uri]::EscapeDataString($BranchName))?expand=1"
    $pullRequestUrl = $null
    try {
        $existing = Get-ExistingPullRequest
        if ($existing) {
            $pullRequestUrl = $existing.html_url
        } elseif ($ghAvailable) {
            $bodyPath = Join-Path ([IO.Path]::GetTempPath()) ("arcanequant-pr-" + [guid]::NewGuid().ToString('N') + '.txt')
            $body = "$msg`n`nVersion: $baseVersion -> $vers.`n`nMerge this version-changing PR before preparing the next one. Release tagging follows the merge."
            [IO.File]::WriteAllText($bodyPath, $body + "`n", [Text.UTF8Encoding]::new($false))
            $created = Invoke-CheckedNative 'gh' @('pr', 'create', '--repo', $repository, '--base', $BaseBranch, '--head', $BranchName, '--title', $prTitle, '--body-file', $bodyPath)
            $pullRequestUrl = $created | Where-Object { $_ -match '^https://github\.com/.+/pull/\d+$' } | Select-Object -Last 1
            if (-not $pullRequestUrl) { throw 'GitHub did not return a pull request URL.' }
        } else {
            Write-Warning 'GitHub CLI (gh) is unavailable; the branch is published, but a PR could not be created automatically.'
        }
    } catch {
        # A failed response can still mean GitHub created the request. Avoid duplicates.
        try { $pullRequestUrl = (Get-ExistingPullRequest).html_url } catch { }
        if (-not $pullRequestUrl) { Write-Warning "Branch published, but PR creation failed: $($_.Exception.Message)" }
    }
    Write-Host "Published $BranchName at $commit (proposed version $vers)."
    if ($pullRequestUrl) { Write-Host "Pull request: $pullRequestUrl" } else { Write-Host "Create the request here: $compareUrl" }
    Write-Host "No merge or release tag was performed. Mainline remains $BaseBranch."
} finally {
    if ($messagePath -and (Test-Path -LiteralPath $messagePath)) { Remove-Item -LiteralPath $messagePath }
    if ($bodyPath -and (Test-Path -LiteralPath $bodyPath)) { Remove-Item -LiteralPath $bodyPath }
    Pop-Location
}
