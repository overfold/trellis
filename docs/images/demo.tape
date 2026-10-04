# Run from the repository root against a disposable cluster. See development.md.
Output docs/images/demo.gif
Require trellisctl
Require bash

Set Shell bash
Set FontFamily "JetBrains Mono"
Set FontSize 22
Set LetterSpacing 0
Set LineHeight 1.15
Set Width 1200
Set Height 600
Set Padding 20
Set Margin 0
Set BorderRadius 0
Set Theme {"background":"#0c1110","foreground":"#d8e2dc","green":"#39ff88","brightGreen":"#39ff88","cursor":"#39ff88","cursorAccent":"#0c1110"}
Set Framerate 12
Set TypingSpeed 30ms
Set CursorBlink false
Env TRELLIS_NAMESPACE default

Hide
Type@0ms `export PS1='\[\e[32m\]❯ '; export PS0=$'\e[0m'; PROMPT_COMMAND='printf "\n"'; set -e; work=$(mktemp -d); cp examples/hello/trellis.yaml "$work/trellis.yaml"; cd "$work"`
Enter
Sleep 200ms
Wait /❯$/
Ctrl+l
Show

Type "trellisctl jobs apply --dry-run --file trellis.yaml"
Enter
Sleep 200ms
Wait /❯$/
Sleep 2500ms

Type "trellisctl jobs apply --file trellis.yaml --wait"
Enter
Sleep 1s
# Omit idle polling, but keep the initial progress and completed Ready output.
Hide
Wait@180s /❯$/
Show
Sleep 2500ms

Hide
Ctrl+l
Show
Type "trellisctl jobs status hello"
Enter
Sleep 200ms
Wait /❯$/
Sleep 4s

Type "trellisctl jobs delete hello --wait"
Enter
Sleep 1s
Hide
Wait@180s /❯$/
Show
Sleep 3s

Hide
Type@0ms `cd /; rm -rf "$work"`
Enter
Sleep 200ms
Wait /❯$/
