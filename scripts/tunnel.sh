#!/usr/bin/env bash
# HTTPS for phones (CQ-9): the game server behind a Cloudflare quick tunnel.
#
#     scripts/tunnel.sh                 # port 8000
#     scripts/tunnel.sh --port 8767 --room KXQB
#
# Phone browsers only allow the microphone on https:// pages. A quick tunnel
# gives a public, trusted https://<random>.trycloudflare.com URL (wss:// for
# the WebSockets) that works from any network, cellular included, with no
# certificate to install. No Cloudflare account needed.
#
# What it does:
#   1. uses the game server on localhost:PORT if /health answers, else starts
#      one (.venv/bin/uvicorn server.app:app; server env vars such as BRIDGE
#      pass through) and stops it again on exit;
#   2. downloads cloudflared (pinned version, sha256-checked) into tools/ if
#      it isn't there yet (CLOUDFLARED=/path/to/cloudflared overrides);
#   3. starts the quick tunnel, waits until /health answers through it, then
#      prints the phone URL, the host WebSocket URL and a QR code, and writes
#      the URL to .tunnel_url (removed on exit) for other tools / the host screen;
#   4. Ctrl-C stops the tunnel (and the server if it started it).
#
# The URL is PUBLIC: anyone with the link can open the page and join a room.
# Fine for the demo; don't leave it running. Each run gets a new URL.
# QR: qrencode if installed, else the segno package from the venv.
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

CF_VERSION=2026.9.3
CF_SHA256=77e26d8d900e0b8469f416239d14b5f296525fdf79fee6f511ef55609e3fbac2  # cloudflared-linux-amd64, from the release notes
PORT=8000
ROOM=""
URL_FILE="$ROOT/.tunnel_url"

usage() { sed -n '2,25p' "$0" | sed 's/^# \{0,1\}//'; exit "${1:-0}"; }
while [ $# -gt 0 ]; do
    case "$1" in
        --port) PORT="$2"; shift 2 ;;
        --room) ROOM="$(echo "$2" | tr '[:lower:]' '[:upper:]')"; shift 2 ;;
        -h|--help) usage ;;
        *) echo "unknown argument: $1" >&2; usage 2 ;;
    esac
done

LOCAL="http://localhost:$PORT"
CF_LOG="$(mktemp -t cloudflared.XXXXXX.log)"
SERVER_PID="" CF_PID=""

cleanup() {
    trap - EXIT INT TERM
    rm -f "$URL_FILE"
    [ -n "$CF_PID" ] && kill "$CF_PID" 2>/dev/null || true
    [ -n "$SERVER_PID" ] && kill "$SERVER_PID" 2>/dev/null || true
    wait 2>/dev/null || true
    echo "tunnel stopped${SERVER_PID:+, game server stopped}; cloudflared log: $CF_LOG"
}
trap cleanup EXIT
trap 'exit 130' INT TERM

healthy() { curl -fsS --max-time 3 -o /dev/null "$1/health" 2>/dev/null; }

# --- cloudflared -----------------------------------------------------------
CF="${CLOUDFLARED:-$ROOT/tools/cloudflared}"
if [ ! -x "$CF" ]; then
    [ "$(uname -sm)" = "Linux x86_64" ] || { echo "no cloudflared for $(uname -sm); set CLOUDFLARED=" >&2; exit 1; }
    echo "downloading cloudflared $CF_VERSION into tools/ ..."
    mkdir -p "$ROOT/tools"
    curl -fsSL -o "$CF.part" \
        "https://github.com/cloudflare/cloudflared/releases/download/$CF_VERSION/cloudflared-linux-amd64"
    echo "$CF_SHA256  $CF.part" | sha256sum -c --quiet || { rm -f "$CF.part"; echo "checksum mismatch" >&2; exit 1; }
    chmod +x "$CF.part" && mv "$CF.part" "$CF"
fi

# --- game server -----------------------------------------------------------
if healthy "$LOCAL"; then
    echo "using the game server already running on $LOCAL"
else
    echo "starting the game server on port $PORT (BRIDGE=${BRIDGE:-transcribe}) ..."
    .venv/bin/uvicorn server.app:app --host 0.0.0.0 --port "$PORT" &
    SERVER_PID=$!
    for _ in $(seq 1 50); do healthy "$LOCAL" && break; sleep 0.2; done
    healthy "$LOCAL" || { echo "game server didn't start" >&2; exit 1; }
fi

# --- tunnel ----------------------------------------------------------------
echo "starting the quick tunnel (log: $CF_LOG) ..."
# --grace-period: on Ctrl-C don't wait the default 30 s for open WebSockets.
"$CF" tunnel --no-autoupdate --grace-period 2s --url "$LOCAL" >"$CF_LOG" 2>&1 &
CF_PID=$!
URL=""
for _ in $(seq 1 60); do
    URL="$(grep -oE 'https://[a-z0-9-]+\.trycloudflare\.com' "$CF_LOG" | head -1 || true)"
    [ -n "$URL" ] && break
    kill -0 "$CF_PID" 2>/dev/null || break
    sleep 0.5
done
if [ -z "$URL" ]; then
    echo "cloudflared gave no URL. Last log lines:" >&2; tail -20 "$CF_LOG" >&2; exit 1
fi
# The hostname takes a few seconds to exist; don't hand out a dead link. Check
# it through public DNS (DoH): asking this machine's resolver too early can
# cache "no such host" for a minute or more (seen with WSL's DNS proxy).
echo "waiting for $URL to be reachable ..."
for _ in $(seq 1 30); do grep -q "Registered tunnel connection" "$CF_LOG" && break; sleep 0.5; done
DOH=(--doh-url https://cloudflare-dns.com/dns-query)
ok=""
for _ in $(seq 1 60); do
    curl -fsS --max-time 5 "${DOH[@]}" -o /dev/null "$URL/health" 2>/dev/null && { ok=1; break; }
    sleep 1
done
if [ -z "$ok" ]; then
    echo "$URL/health not reachable after 60 s:" >&2
    curl -sS --max-time 5 "${DOH[@]}" -o /dev/null "$URL/health" >&2 || true
    tail -20 "$CF_LOG" >&2; exit 1
fi
healthy "$URL" || echo "note: works via public DNS, but this machine's resolver can't find it yet;" \
    "phones are fine, local tools (simulator) may need a minute." >&2

echo "$URL" >"$URL_FILE"
JOIN="$URL/"
[ -n "$ROOM" ] && JOIN="$URL/play?room=$ROOM"

cat <<EOF

  Phones:       $JOIN
                ($URL/play?room=CODE joins a room directly)
  Host screen:  wss://${URL#https://}/ws/host
  Simulator:    .venv/bin/python scripts/simulate.py --server wss://${URL#https://} ...
  Written to:   .tunnel_url

  PUBLIC URL: anyone with it can join. Ctrl-C to stop.

EOF
if command -v qrencode >/dev/null; then
    qrencode -t ansiutf8 "$JOIN"
elif .venv/bin/python -c "import segno" 2>/dev/null; then
    .venv/bin/python -c "import segno, sys; segno.make(sys.argv[1], error='m').terminal(compact=True)" "$JOIN"
else
    echo "(no QR: install qrencode, or .venv/bin/pip install -r requirements.txt for segno)"
fi

wait "$CF_PID" || true
echo "cloudflared exited. Last log lines:" >&2
tail -20 "$CF_LOG" >&2
exit 1
