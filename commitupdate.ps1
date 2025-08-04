# commitupdate.ps1

# Note to user, if update fails past bumping, tag will be updated and must be deleted before being repeated (or may cause a hassle)

# Get part update from user
$part = Read-Host "Enter the update type (patch/minor/major)"

# Prompt for multi-line commit message
Write-Host "Enter the commit message (finish with Ctrl+Z then Enter):"
$msg = [Console]::In.ReadToEnd()

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

# 7. Interactively squash the version bump + changelog into the main commit
Read-Host "Rebasing: Change second and third commits to squash, then save and exit (:wq!). Press Enter to continue..."
git rebase -i HEAD~3  # If you have 3 commits total (feature + bump + changelog)

# In the editor:
# Change the 2nd and 3rd "pick" to "squash"
# Save and exit (:wq or Ctrl+X → Y → Enter)

# 8. Push (no force needed if this is the first push)
Read-Host "Pushing to Git. Press Enter to continue..."
git push --follow-tags
Write-Host "Pushed $vers to Git."

