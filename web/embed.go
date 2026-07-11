// Package web embeds the static dashboard assets so the tracker binary serves
// the map UI with no separate frontend service and no build step.
package web

import (
	"embed"
	"io/fs"
)

//go:embed index.html app.js style.css
var files embed.FS

// FS is the embedded dashboard, rooted so index.html is served at "/".
var FS fs.FS = files
