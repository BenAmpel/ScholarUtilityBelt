#!/usr/bin/env python3
"""capture.py <title substring> <out.png> [toolbar_css_px]: screenshot one Chrome window by title (window only, no
shadow), then crop off the browser toolbar so only the page remains. Never captures the whole screen."""
import sys, subprocess, Quartz
from PIL import Image
title, out = sys.argv[1], sys.argv[2]
toolbar = int(sys.argv[3]) if len(sys.argv) > 3 else 121
wins = [w for w in Quartz.CGWindowListCopyWindowInfo(Quartz.kCGWindowListOptionOnScreenOnly, Quartz.kCGNullWindowID)
        if w.get('kCGWindowOwnerName') == 'Google Chrome' and w.get('kCGWindowLayer') == 0 and title in (w.get('kCGWindowName') or '')]
if len(wins) != 1:
    sys.exit(f"expected one window titled ~{title!r}, found {len(wins)}")
tmp = out + '.full.png'
subprocess.run(['screencapture', '-x', '-o', f"-l{wins[0]['kCGWindowNumber']}", tmp], check=True)
im = Image.open(tmp); scale = im.width / wins[0]['kCGWindowBounds']['Width']
im.crop((0, int(toolbar * scale), im.width, im.height)).save(out)
print(out, im.width, im.height - int(toolbar * scale))
