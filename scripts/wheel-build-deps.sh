#!/usr/bin/env sh
# Print the container build steps that the beacondb wheel release uses.
#
# The commands live inline in the `before-script-linux:` block of
# .github/workflows/publish-beacondb.yml — that block is the source of truth, because it is
# what actually runs on a release tag. This extracts it verbatim so that
# scripts/build-wheel-docker.sh exercises exactly what the release build does, rather than a
# copy that drifts out of sync with it.
#
# POSIX sh + awk: kept portable so this also runs inside the Alpine musllinux container,
# which has no bash.
set -eu

workflow="$(dirname "$0")/../.github/workflows/publish-beacondb.yml"

if [ ! -f "$workflow" ]; then
    echo "error: cannot find $workflow" >&2
    exit 1
fi

# YAML literal block scalar: the body is every following line indented further than the key,
# and it ends at the first non-blank line that dedents back to the key's level or beyond.
script=$(awk '
    !inside && /^[[:space:]]*before-script-linux:[[:space:]]*\|/ {
        match($0, /^[[:space:]]*/); key_indent = RLENGTH; inside = 1; next
    }
    inside {
        if ($0 ~ /^[[:space:]]*$/) { print ""; next }
        match($0, /^[[:space:]]*/)
        if (RLENGTH <= key_indent) exit
        if (!body_indent) body_indent = RLENGTH
        print substr($0, body_indent + 1)
    }
' "$workflow")

# An empty result means the workflow was reformatted (or the key renamed) and the callers
# would silently build with no toolchain installed at all. Fail loudly instead.
if [ -z "$(printf '%s' "$script" | tr -d '[:space:]')" ]; then
    echo "error: no before-script-linux block found in $workflow" >&2
    exit 1
fi

printf '%s\n' "$script"
