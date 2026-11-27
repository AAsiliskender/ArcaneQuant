---
name: write-changelog
description: Write ArcaneQuant changelog text from a supplied diff, commit range, or current changes, matching CHANGELOG.md and producing body-only notes for release scripts or a full entry when requested.
---

# Write changelog from a diff

Use recent entries in [CHANGELOG.md](../../../CHANGELOG.md) as the writing and formatting reference. Keep this skill scoped to understanding the selected changes and writing their changelog text; leave version bumps and publication to the release workflow.

## Establish the evidence

1. Read the latest changelog entries and the user's requested scope, destination, and any verbatim wording.
2. Use the supplied diff or exact commit range when given. For a branch comparison, compare from the requested base's merge base. For staged changes, use `git diff --cached`. For all current changes, inspect `git status --short`, `git diff HEAD`, and the contents of intended untracked files; a tracked diff alone omits them. Resolve genuinely ambiguous scope before presenting the notes as complete.
3. Read changed code and enough surrounding context to distinguish new behavior, fixes, refactoring, renames, removals, tests, and documentation. Compare deleted and added files when a rename is not detected automatically. A diffstat or commit title alone is insufficient evidence.
4. Group related edits into material changes. Describe what changed and its resulting behavior; use exact class, function, parameter, and file names where useful. Include limitations or incomplete behavior shown by the diff. Treat a draft changelog or TODO as a claim to verify against the implementation, rather than evidence of completed work.

## Match the existing text format

- Start each main change with `- ` and a concrete action, usually `Added`, `Updated`, `Fixed`, `Removed`, `Refactored`, `Converted`, or `Created`. Follow the concise style of the recent entries.
- End a grouping bullet with `:` when supporting details follow. Put each detail on its own line beginning with exactly four spaces and `> `.
- Keep each bullet and its details contiguous. Use eight spaces followed by `* ` only for a necessary deeper list, as in the existing UI descriptions; otherwise keep details flat.
- Preserve exact identifiers and requested user wording. Use the existing plain-text identifier style rather than imposing blanket backticks or a different terminology.
- Use ordinary sentences and concrete outcomes. Summarize a coordinated change once instead of listing every touched file, assertion, or formatting edit separately.
- Use one blank line between release entries. Avoid new category headings such as `### Added` or `### Fixed`; the recent releases use direct bullets.
- Describe tests added or changed when material. Claim tests passed, performance improved, or a feature is complete only when the available evidence establishes it. Do not run a test suite solely to compose notes.
- Record only changes within the selected scope. Omit unrelated work, speculative benefits, and future plans unless the user specifically requests them and they are labelled accordingly.
- Treat routine version increments and the generated changelog entry as release metadata, rather than additional functionality to advertise. Describe substantive changes to versioning tools or changelog handling when the diff actually makes them.

## Choose the requested output

- **Body text, the default:** return only the `- ` bullets and their indented details. Omit the changelog title, version/date heading, introductory explanation, and closing summary. Existing commit-update scripts add the heading themselves. Provide the text for the calling workflow without independently modifying CHANGELOG.md.
- **Full entry, when requested:** prepend `## [X.Y.Z] - YYYY-MM-DD`, using the target version supplied by the user or calling workflow. Use an explicit supplied date, otherwise the current Europe/London date. Do not invent the next version or use an already released configuration value as a new release. If the version is missing, prepare the body and obtain only that missing value before finalizing the heading.
- **File edit, when requested:** keep the existing `##### CHANGELOG` title and put the new entry immediately below it, newest first. If that target entry already exists, incorporate the selected changes without duplicating the heading or existing bullets. Preserve historical entries, encoding, and line endings; avoid introducing trailing whitespace or extra blank lines at EOF. Do not bump versions, commit, tag, push, or create a PR as part of changelog writing.

## Review before delivery

- Trace each factual claim to the selected diff or supplied verification results. Do not infer completion from a new function name, renamed file, or added test alone.
- Check that every material change is represented once, related details are grouped, identifiers are correct, and any required verbatim wording is retained.
- Check the `- ` bullets, four-space `> ` indentation, optional heading, and absence of duplicated entries. When editing a file, review the resulting diff for changes to historical text and whitespace.
- If the supplied diff is empty or only changes a draft entry, report that limitation rather than inventing release content. Distinguish substantive edits to historical documentation from an unverified draft of new changes.

## Formatting example

The following illustrates the existing format; use its claims only when the selected diff supports them:

```text
- Completed and reorganised Process integration tests:
    > Grouped tests by the components involved.
    > Removed duplicate reference resolution coverage.
- Converted test parameter zip iterators to reusable lists.
```
