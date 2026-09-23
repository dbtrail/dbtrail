# Vendored frontend assets

The bintrail console frontend (`index.html`, `app.js`, `style.css`) is written
in vanilla HTML, CSS, and JavaScript with **zero third-party code
dependencies** — no frameworks, no bundler, no Node build step. The files are
embedded directly into the Go binary via `//go:embed` (see `../assets.go`).

This keeps `make build` a pure Go build (CGO for DuckDB, no JS toolchain) and
keeps the supply chain trivial to audit: everything served to the browser lives
in this directory and is reviewed in-tree.

## Fonts (`fonts/`)

The console loads its three brand typefaces from vendored, subset woff2 files —
never from a CDN. The console makes zero external requests by design, so the
`@font-face` rules in `style.css` reference only these embedded files (with the
`system-ui` stack as fallback via `font-display: swap`).

| File | Family | Style | Size |
|---|---|---|---|
| `bricolage-grotesque-latin.woff2` | Bricolage Grotesque v1.001 (Google Fonts v9) | variable: opsz 12–96, wght 200–800 | 71 KB |
| `geist-latin.woff2` | Geist v1.800 (Google Fonts v5) | variable: wght 100–900 | 26 KB |
| `ibm-plex-mono-400-latin.woff2` | IBM Plex Mono v2.3 (Google Fonts v20) | 400 | 9 KB |
| `ibm-plex-mono-500-latin.woff2` | IBM Plex Mono v2.3 (Google Fonts v20) | 500 | 9 KB |
| `ibm-plex-mono-600-latin.woff2` | IBM Plex Mono v2.3 (Google Fonts v20) | 600 | 9 KB |

Provenance: downloaded from Google Fonts' static woff2 endpoints
(`fonts.gstatic.com`, latin unicode-range files), then further subset with
fonttools `pyftsubset` (`--flavor=woff2 --layout-features='*'
--name-IDs='*'`) to basic latin plus common punctuation:

```
U+0020-007E, U+00A0-00FF, U+0131, U+0152-0153, U+2013-2014, U+2018-2019,
U+201C-201D, U+2022, U+2026, U+2039-203A, U+2212, U+2713
```

The same range is declared as `unicode-range` on each `@font-face`; glyphs
outside it (non-latin data values) render in the system fallback stack.
Italic faces are not vendored — the two italic uses in the UI (SQL comments,
NULL placeholders) render as synthesized oblique.

License: all three families are licensed under the **SIL Open Font License 1.1**
(OFL-1.1, <https://openfontlicense.org>), which permits redistribution and
subsetting, including bundling into a binary, provided the fonts are not sold
by themselves. Copyright holders:

- Bricolage Grotesque — © 2022 The Bricolage Grotesque Project Authors
  (<https://github.com/ateliertriay/bricolage>)
- Geist — © 2023 Vercel, in collaboration with basement.studio
  (<https://github.com/vercel/geist-font>)
- IBM Plex Mono — © 2017 IBM Corp. (<https://github.com/IBM/plex>),
  with Reserved Font Name "Plex"

### License metadata (OFL-1.1 §2) — do not regress

OFL-1.1 §2 requires every distributed copy to carry both the copyright notice
and the license, either as accompanying text or in machine-readable font
metadata. Both paths are covered, and each has a CI pin:

- **Accompanying text**: the full OFL-1.1 text plus the three copyright
  notices above ship in the repo-root `THIRD-PARTY-NOTICES` (the manual
  section, maintained in `scripts/notices-header.txt`), which rides in every
  release artifact — tarballs, deb/rpm, and images. `scripts/check-notices.sh`
  fails CI if the section goes missing.
- **Font metadata**: every vendored woff2 carries name IDs 0 (copyright),
  13 (license description) and 14 (license URL). The Google Fonts statics ship
  with IDs 0 and 14 but no ID 13, and `pyftsubset`'s DEFAULT `--name-IDs`
  keeps only IDs 0–6 — which is how the v0.55.0 files lost the license URL.
  The pipeline therefore injects ID 13 before subsetting and subsets with
  `--name-IDs='*'`. `scripts/check-font-licenses.sh` (`make check-fonts`)
  fails CI if any font loses IDs 0/13/14.

The ID-13 injection, run on each downloaded static before `pyftsubset`
(fontTools, same venv):

```python
from fontTools.ttLib import TTFont
f = TTFont("<downloaded-static>.woff2")
url = f["name"].getDebugName(14)  # each family's own license URL
f["name"].setName(
    "This Font Software is licensed under the SIL Open Font License, "
    f"Version 1.1. This license is available with a FAQ at: {url}",
    13, 3, 1, 0x409)
f.save("<downloaded-static>.with-license.woff2")
```

If any other third-party asset is ever added, list it here with its name,
version, license, and source URL, confirm its license permits redistribution,
and extend the notices header + guard scripts above to cover it.

## MySQL logo (`mysql-logo.png`)

The Overview's flow draws the source box with the MySQL logo, downloaded
from `https://www.mysql.com/common/logos/logo-mysql-170x115.png` (176x119
PNG, 3.8 KB) and embedded so the console still makes zero external requests.
MySQL is a trademark of Oracle Corporation; the logo identifies the database
the console reads from and implies no endorsement.

## DBTrail lockup (`dbtrail-lockup-white.png`)

The brand lockup in white (494x188 PNG, 17 KB), from the project's own
logo set, drawn in the Overview's DBTrail box.
