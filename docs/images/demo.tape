# Run from the repository root against a disposable three-node cluster.
# See docs/developer/development.md: README terminal demo.
Output docs/images/demo.gif
Require trellisctl
Require jq
Require bash

Set Shell bash
Set FontFamily "DejaVu Sans Mono"
Set FontSize 20
Set Width 1280
Set Height 600
Set Padding 24
Set Theme "Catppuccin Mocha"
Set Framerate 10
Set TypingSpeed 12ms
Set CursorBlink false
Env TRELLIS_NAMESPACE default

# Seed v1 off-camera, then change only the image in a temporary manifest.
Hide
Type@0ms `export PS1='$ '; set -e; work=$(mktemp -d); cp examples/rolling-update/trellis.yaml "$work/trellis.yaml"; cd "$work"; trellisctl jobs apply --file trellis.yaml --wait; sed -i 's/trellis-tutorial:v1/trellis-tutorial:v2/' trellis.yaml`
Enter
Sleep 200ms
Wait@180s /\$$/
Ctrl+l
Show

Type "# Two healthy replicas; rolling updates replace one at a time."
Enter
Type "trellisctl jobs status rolling-update"
Enter
Sleep 200ms
Wait /\$$/
Sleep 3s

Hide
Ctrl+l
Show
Type "# Preview the image change before touching running allocations."
Enter
Type "trellisctl jobs apply --dry-run --file trellis.yaml"
Enter
Sleep 200ms
Wait /\$$/
Sleep 3s

Type "trellisctl jobs apply --file trellis.yaml --wait"
Enter
Sleep 1500ms
# Cut idle polling time, retaining the initial progress and final Ready output.
Hide
Wait@180s /\$$/
Show
Sleep 2s

Hide
Ctrl+l
Show
Type "# New allocation IDs: revision 2 is healthy."
Enter
Type "trellisctl jobs status rolling-update"
Enter
Sleep 200ms
Wait /\$$/
Sleep 3s

# Choose a node that actually hosts one of the new allocations.
Hide
Type@0ms `node=$(trellisctl jobs status rolling-update --output json | jq -r '.allocations[] | select(.phase == "running" and (.draining != true)) | .node_id' | head -1); node=${node:0:8}; history -s "trellisctl nodes drain $node"`
Enter
Sleep 200ms
Wait /\$$/
Ctrl+l
Show
Type "# Drain a hosting node; Trellis reschedules without another apply."
Enter
# Recall the concrete node ID rather than showing an unexplained shell variable.
Up
Sleep 500ms
Enter
Sleep 200ms
Wait /\$$/
Sleep 2s

# Await healthy replacements AND stopped allocations on the drained node.
Hide
Type@0ms `migrated() { trellisctl jobs status rolling-update --output json | jq -e --arg node "$node" '([.allocations[] | select(.phase == "running" and .health == "healthy" and (.draining != true) and (.node_id | startswith($node) | not))] | length == 2) and all(.allocations[]; (.node_id | startswith($node) | not) or .phase == "stopped")' >/dev/null; }; for i in {1..120}; do if migrated; then break; fi; sleep 1; done; migrated; clear`
Enter
Sleep 200ms
Wait@180s /\$$/
Show
Type "# After drain: two healthy allocations on the remaining nodes."
Enter
Type "trellisctl jobs status rolling-update"
Enter
Sleep 200ms
Wait /\$$/
Sleep 5s

# Leave the disposable cluster ready to record again.
Hide
Type@0ms `trellisctl jobs delete rolling-update --wait; trellisctl nodes undrain "$node"; cd /; rm -rf "$work"`
Enter
Sleep 200ms
Wait@180s /\$$/
