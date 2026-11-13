# Changelog entries, one file per pull request

A pull request does not edit `docs/CHANGELOG.md`. It adds its entry here, in a
file named after the pull request or the issue it closes:

```
docs/changelog.d/2740.md
```

The file holds the entry as it will read under `## [Unreleased]`: one bullet,
or a few, written the way the changelog already is.

```markdown
- **The MySQL port without routing answers `LIMIT offset, count`** (#2628). ...
```

Why: every pull request used to add its line at the same place, under
`## [Unreleased]`, so each merge left every other open pull request in
conflict, and each conflict meant another full CI run. Files of their own
never conflict.

When a version is released, the release step moves every file of this folder
(not this README) into `## [Unreleased]` of `docs/CHANGELOG.md`, in pull
request order, removes the files, and only then renames the section to the
version. Between releases, the unreleased changes are this folder plus what
`## [Unreleased]` already holds.
