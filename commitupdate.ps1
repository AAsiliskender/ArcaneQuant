# commitupdate.ps1

# Get part update from user
$part = Read-Host "Enter the update type (patch/minor/major)"

# Prompt for multi-line commit message
Write-Host "Enter the commit message (finish with Ctrl+Z then Enter):"
$msg = [Console]::In.ReadToEnd()

# Get the original commit (will squash commits to this)
$startCommit = git rev-parse HEAD
if ($LASTEXITCODE -ne 0) { throw "Cannot identify starting commit." }

# 1. Stage all changes
Write-Host "Staging main changes"
git add .

# 2. Make your actual feature/fix commit
Write-Host "Committing main changes with message"
git commit -S -m @"
$msg
"@

# 3. Bump the version
Write-Host "Bumping version with tagging"
bump2version --config-file .bumpversion.cfg $part
if ($LASTEXITCODE -ne 0) {
    throw "bump2version failed with exit code $LASTEXITCODE."
}

# 4. Edit the CHANGELOG to include the right message
Write-Host "Updating CHANGELOG.md with commit message"
# Read file content
$versionLine = Select-String -Path ".bumpversion.cfg" -Pattern 'current_version = (\d+\.\d+\.\d+)' | Select-Object -First 1
$vers = $versionLine.Matches[0].Groups[1].Value

$content = Get-Content -Raw -Path "CHANGELOG.md"

# Insert your update after the "# Changelog" line
Write-Host "Inserting changelog update"
$newEntry = "## [$vers] - $(Get-Date -Format yyyy-MM-dd)`n$msg`n"
Write-Host "Generated entry:`n$newEntry"
$updatedContent = $content -replace "(?<=##### CHANGELOG\r\n\r\n)", "$newEntry"

# Write back to the file
Write-Host "Writing update"
Set-Content -Path "CHANGELOG.md" -Value $updatedContent

# 5. Stage addition to Changelog
Write-Host "Staging CHANGELOG.md changes"
git add .

# 6. Commit changelog bump
Write-Host "Committing CHANGELOG as empty"
git commit --allow-empty-message -F NUL

# 7. Squash the version bump + changelog into the main commit
Read-Host "Squashing commits by making soft reset. Press Enter to continue..."
# Identify the target commit before resetting
$target = git rev-parse HEAD~3
if ($LASTEXITCODE -ne 0) {
    throw "Cannot identify reset target."
}
if ($startCommit -ne $target) {
    throw "Unexpected commit history. Aborting reset."
}

Write-Host "`nCurrent HEAD:"
git log -1 --format="%h %s" HEAD

Write-Host "`nReset target:"
git log -1 --format="%h %s" $target

Write-Host "`nCommits that will be squashed:"
git log --oneline "$target..HEAD"

### Squash by soft reset
$confirm = Read-Host "`nReset HEAD to $target while preserving staged changes? (Y/N)"
if ($confirm -ne "Y") { throw "Reset cancelled." }
git reset --soft $target
if ($LASTEXITCODE -ne 0) { throw "Reset failed." }

# Create the final signed commit
git commit -S -m $msg
if ($LASTEXITCODE -ne 0) { throw "Commit failed." }


# Attach a signed tag to the newly created commit
git tag -s "$vers" -m "Version $vers"
if ($LASTEXITCODE -ne 0) { throw "Tag creation failed." }

# 8. Push (no force needed if this is the first push)
Read-Host "Pushing to Git. Press Enter to continue..."
git push --follow-tags
Write-Host "Pushed $vers to Git."

