#!/bin/bash
set -u
# usage: cjs.sh <chrome window id> <js file>: runs the JS in that window's active tab and prints the result
JS="$(cat "$2")"
osascript - "$1" "$JS" <<'OSA'
on run argv
  tell application "Google Chrome" to return execute active tab of window id (item 1 of argv as integer) javascript (item 2 of argv)
end run
OSA
