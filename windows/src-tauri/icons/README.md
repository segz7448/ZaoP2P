# Icons

`32x32.png` and `128x128.png` are placeholder solid-color PNGs generated for
Milestone 1 so the Tauri bundler config is valid. Replace with real branding
before any public release.

**`icon.ico` is intentionally NOT included.** Generating a correct multi-resolution
.ico by hand is error-prone; instead, generate it properly with:

```
npm install -g @tauri-apps/cli
tauri icon path/to/your/source-icon.png
```

This creates all required sizes (32x32.png, 128x128.png, icon.ico, icon.icns,
etc.) directly into this folder from one source image, and is the supported
way to populate Tauri icons. Run it once you have a real logo, then commit the
output. Until then, remove `"icons/icon.ico"` from `tauri.conf.json`'s bundle
icon list or the Windows build step will fail.
