// Package webui embeds the operator terminal so the tracker binary serves the UI
// with no separate frontend service. The terminal is the Qwik app in the repo's
// web/ directory, which pre-renders to static files in dist/ here; run `make ui`
// before building the binary, otherwise only the placeholder in dist/ is embedded.
package webui

import (
	"embed"
	"io/fs"
)

//go:embed all:dist
var files embed.FS

// FS is the embedded terminal, rooted so index.html is served at "/".
var FS fs.FS = mustSub(files, "dist")

func mustSub(f fs.FS, dir string) fs.FS {
	sub, err := fs.Sub(f, dir)
	if err != nil {
		panic(err)
	}
	return sub
}
