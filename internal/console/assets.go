package console

import (
	"embed"
	"io/fs"
	"mime"
	"net/http"
	"path"
	"strings"
)

// assetsFS embeds the static frontend (HTML/CSS/JS + brand images + the
// vendored, latin-subset brand typefaces under assets/fonts — the console
// makes zero external requests, so the fonts ship in the binary). The
// console has no Node build step — these files are vanilla and, beyond the
// OFL-licensed fonts, dependency-free (see assets/VENDOR.md). Only the
// served files are embedded; VENDOR.md stays as source-tree documentation
// and is not exposed.
//
//go:embed assets/index.html assets/app.js assets/style.css assets/logo.png assets/favicon.png assets/mysql-logo.png assets/dbtrail-lockup-white.png assets/fonts
var assetsFS embed.FS

func init() {
	// Go's built-in MIME table has no ".woff2" entry, so http.FileServer
	// would fall back to the host OS mime.types (or octet-stream). Fonts
	// load either way — nosniff does not apply to font destinations — but
	// registering the type keeps the served Content-Type deterministic
	// across hosts.
	_ = mime.AddExtensionType(".woff2", "font/woff2")
}

// assetHandler serves the embedded frontend rooted at the assets/ directory,
// so "/" resolves to index.html and "/app.js", "/style.css" resolve directly.
//
// The SPA routes with history.pushState ("/events", "/recover", …), so a
// reload or deep link arrives here as a path that is not an embedded file.
// Those get the index.html shell and the frontend router restores the view
// (routeFromLocation in app.js). The fallback is limited to paths whose last
// segment has no extension: a missing real asset ("/favicon.ico", a stale
// "/app.js.map") must stay a 404, not silently become HTML. Treating any
// Stat error as "not a file" is safe only on embed.FS, which has no
// ErrPermission/transient-IO class — revisit if sub ever comes from disk.
func assetHandler() http.Handler {
	sub, err := fs.Sub(assetsFS, "assets")
	if err != nil {
		// Unreachable: the embed directive guarantees the directory exists.
		panic(err)
	}
	files := http.FileServer(http.FS(sub))
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		p := strings.TrimPrefix(path.Clean(r.URL.Path), "/")
		if p != "" && !strings.Contains(path.Base(p), ".") {
			if _, err := fs.Stat(sub, p); err != nil {
				http.ServeFileFS(w, r, sub, "index.html")
				return
			}
		}
		files.ServeHTTP(w, r)
	})
}
