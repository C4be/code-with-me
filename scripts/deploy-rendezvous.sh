#!/usr/bin/env bash
set -euo pipefail

DOMAIN="${1:-code-with-me-app.ru}"
SERVER="${2:-c4be@176.123.162.101}"
PUBLIC_IP="${3:-176.123.162.101}"
LEGACY_DOMAIN="176-123-162-101.sslip.io"
KEY="${CODE_WITH_ME_SSH_KEY:-$HOME/.ssh/cloudru}"
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

if [[ ! "$DOMAIN" =~ ^[a-zA-Z0-9.-]+$ ]]; then
  echo "Некорректное доменное имя" >&2
  exit 1
fi
if [[ ! "$PUBLIC_IP" =~ ^[0-9]+\.[0-9]+\.[0-9]+\.[0-9]+$ ]]; then
  echo "Некорректный публичный IP" >&2
  exit 1
fi

mkdir -p "$ROOT/.build"
(
  cd "$ROOT/rendezvous"
  GOOS=linux GOARCH=amd64 CGO_ENABLED=0 go build -o "$ROOT/.build/rendezvous" .
)
(
  cd "$ROOT"
  pnpm build
)

ssh -i "$KEY" -o BatchMode=yes -o ConnectTimeout=10 "$SERVER" 'mkdir -p ~/code-with-me-deploy'
scp -i "$KEY" -o BatchMode=yes -o ConnectTimeout=10 "$ROOT/.build/rendezvous" "$SERVER:code-with-me-deploy/rendezvous"
COPYFILE_DISABLE=1 tar -C "$ROOT" -czf "$ROOT/.build/dist.tgz" dist
scp -i "$KEY" -o BatchMode=yes -o ConnectTimeout=10 "$ROOT/.build/dist.tgz" "$SERVER:code-with-me-deploy/dist.tgz"

ssh -i "$KEY" -o BatchMode=yes -o ConnectTimeout=10 "$SERVER" bash -s -- "$DOMAIN" "$PUBLIC_IP" "$LEGACY_DOMAIN" <<'REMOTE'
set -euo pipefail
domain="$1"
public_ip="$2"
legacy_domain="$3"
sudo install -d -m 755 /opt/code-with-me
sudo install -m 755 "$HOME/code-with-me-deploy/rendezvous" /opt/code-with-me/rendezvous
sudo install -d -m 755 /opt/code-with-me/dist
sudo tar -C /opt/code-with-me --no-same-owner -xzf "$HOME/code-with-me-deploy/dist.tgz"
sudo install -d -m 750 -o root -g c4be /etc/code-with-me
if ! sudo test -f /etc/code-with-me/turn.secret; then
  openssl rand -hex 32 | sudo tee /etc/code-with-me/turn.secret >/dev/null
  sudo chown root:c4be /etc/code-with-me/turn.secret
  sudo chmod 640 /etc/code-with-me/turn.secret
fi
turn_secret="$(sudo cat /etc/code-with-me/turn.secret)"
private_ip="$(hostname -I | awk '{print $1}')"

printf 'PUBLIC_URL=https://%s\nLEGACY_PUBLIC_URL=https://%s\nTURN_HOST=%s\nTURN_LEGACY_HOST=%s\nTURN_SECRET=%s\n' "$domain" "$legacy_domain" "$domain" "$legacy_domain" "$turn_secret" | sudo tee /etc/code-with-me/rendezvous.env >/dev/null
sudo chown root:c4be /etc/code-with-me/rendezvous.env
sudo chmod 640 /etc/code-with-me/rendezvous.env

cat <<SERVICE | sudo tee /etc/systemd/system/code-with-me-rendezvous.service >/dev/null
[Unit]
Description=Code with me rendezvous
After=network-online.target
Wants=network-online.target

[Service]
User=c4be
Group=c4be
EnvironmentFile=/etc/code-with-me/rendezvous.env
ExecStart=/opt/code-with-me/rendezvous
Restart=always
RestartSec=2
NoNewPrivileges=true
ProtectSystem=strict
ProtectHome=true

[Install]
WantedBy=multi-user.target
SERVICE

cat <<CADDY | sudo tee /etc/caddy/Caddyfile >/dev/null
$domain, $legacy_domain {
  handle /api/* {
    reverse_proxy 127.0.0.1:8787
  }
  handle /signal/* {
    reverse_proxy 127.0.0.1:8787
  }
  handle /healthz {
    reverse_proxy 127.0.0.1:8787
  }
  handle {
    root * /opt/code-with-me/dist
    try_files {path} /index.html
    file_server
  }
}
www.$domain {
  redir https://$domain{uri} permanent
}
CADDY

cat <<TURN | sudo tee /etc/turnserver.conf >/dev/null
listening-port=3478
listening-ip=$private_ip
relay-ip=$private_ip
external-ip=$public_ip/$private_ip
realm=$domain
fingerprint
lt-cred-mech
use-auth-secret
static-auth-secret=$turn_secret
min-port=49160
max-port=49200
no-loopback-peers
no-multicast-peers
no-cli
no-tls
no-dtls
TURN
sudo chmod 640 /etc/turnserver.conf
sudo chown root:turnserver /etc/turnserver.conf
echo 'TURNSERVER_ENABLED=1' | sudo tee /etc/default/coturn >/dev/null

sudo caddy validate --config /etc/caddy/Caddyfile
sudo systemctl daemon-reload
sudo systemctl enable --now code-with-me-rendezvous coturn caddy
sudo systemctl restart code-with-me-rendezvous coturn caddy
sudo systemctl --no-pager --full status code-with-me-rendezvous coturn caddy | sed -n '1,65p'
REMOTE
