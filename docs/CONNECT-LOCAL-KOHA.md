# Connecting Cerulean Next to a Local Koha Instance

Connect your cloud-hosted Cerulean Next to a local Koha Test Docker (KTD) instance running on your workstation. This uses a reverse SSH tunnel to securely forward your local Koha port to the Cerulean server.

---

## How It Works

```
Your Mac/PC                          DigitalOcean Server
┌──────────────┐    SSH Tunnel      ┌──────────────────────┐
│  KTD (Koha)  │◄──────────────────►│  Cerulean Next       │
│  port 8081   │   reverse port     │  connects to         │
│  localhost   │   forwarding       │  localhost:8081       │
└──────────────┘                    └──────────────────────┘
```

The SSH tunnel makes your local KTD port appear as `localhost:8081` on the Cerulean server.

---

## Prerequisites

- Koha Test Docker (KTD) running locally
- SSH access to the Cerulean server (ask Brendan for the root password)
- Your KTD Koha staff credentials (for API access)

---

## Mac Instructions

### Step 1: Find Your KTD Port

```bash
docker ps | grep koha
```

Look for the port mapping in the output (e.g., `0.0.0.0:8081->8081/tcp`). Note the **left-side port** (the one on your Mac).

Common KTD ports:
- `8080` — Staff interface
- `8081` — OPAC / API
- `8082` — Intranet (some setups)

### Step 2: Start the SSH Tunnel

```bash
ssh -R 8081:localhost:8081 root@cerulean-next.gallagher-family-hub.com -N
```

Replace `8081` with your KTD port (both occurrences).

You will be prompted for the root password — **ask Brendan if you don't have it**.

The `-N` flag means "don't open a shell, just forward the port." **Keep this terminal open** — the tunnel stays alive as long as the SSH session runs.

### Step 3: Verify the Tunnel (on the server)

In another terminal, SSH to the server and test:

```bash
ssh root@cerulean-next.gallagher-family-hub.com
curl -s http://localhost:8081/api/v1/ | head -5
```

You should see a JSON response from Koha. If you get "Connection refused," check that KTD is running and the port number is correct.

### Step 4: Find the Docker Gateway IP

Cerulean runs inside Docker on the server. Inside a container, `localhost` refers to the container itself, not the server host. You need the Docker gateway IP:

```bash
docker network inspect cerulean_cerulean | grep Gateway
```

This will output something like `"Gateway": "172.19.0.1"`. Note this IP.

### Step 5: Configure Cerulean

1. Open Cerulean in your browser: `https://cerulean-next.gallagher-family-hub.com`
2. Open your project → **Settings**
3. Set **Koha URL** to `http://172.19.0.1:8081` (using the gateway IP from Step 4)
4. Set **Auth Type** to Basic Auth
5. Enter your KTD staff username and password (default: `koha` / `koha`)
6. Click **Save**

### Step 6: Test the Connection

1. Go to **Stage 7 → Setup**
2. Click **Run Preflight**
3. You should see a successful connection with your Koha version displayed

### Making the Tunnel Persistent (Mac)

Instead of keeping a terminal open, use `autossh` which auto-reconnects if the connection drops:

```bash
# Install autossh
brew install autossh

# Start a persistent tunnel (runs in background)
autossh -M 0 -R 8081:localhost:8081 root@cerulean-next.gallagher-family-hub.com -N -f
```

The `-f` flag backgrounds the process. It will keep reconnecting as long as your Mac is awake.

To stop the tunnel:

```bash
pkill -f "autossh.*8081"
```

---

## Windows Instructions

### Step 1: Find Your KTD Port

Open PowerShell or Command Prompt:

```powershell
docker ps | findstr koha
```

Look for the port mapping (e.g., `0.0.0.0:8081->8081/tcp`). Note the left-side port.

### Step 2a: Start the SSH Tunnel (PowerShell / OpenSSH)

Windows 10/11 includes OpenSSH by default:

```powershell
ssh -R 8081:localhost:8081 root@cerulean-next.gallagher-family-hub.com -N
```

Replace `8081` with your KTD port (both occurrences).

You will be prompted for the root password — **ask Brendan if you don't have it**.

**Keep this PowerShell window open.**

### Step 2b: Start the SSH Tunnel (PuTTY)

If you prefer PuTTY:

1. Open PuTTY
2. Enter `root@cerulean-next.gallagher-family-hub.com` in the Host Name field
3. Go to **Connection → SSH → Tunnels**
4. In **Source port**, enter `8081`
5. In **Destination**, enter `localhost:8081`
6. Select **Remote** (not Local)
7. Click **Add**
8. Go back to **Session** and click **Open**
9. Log in — the tunnel is now active

### Step 3: Verify the Tunnel (on the server)

Open a second SSH session to the server (or a second PuTTY window):

```bash
curl -s http://localhost:8081/api/v1/ | head -5
```

You should see a JSON response from Koha.

### Step 4: Find the Docker Gateway IP

On the server:

```bash
docker network inspect cerulean_cerulean | grep Gateway
```

Note the IP (e.g., `172.19.0.1`).

### Step 5: Configure Cerulean

1. Open Cerulean in your browser: `https://cerulean-next.gallagher-family-hub.com`
2. Open your project → **Settings**
3. Set **Koha URL** to `http://172.19.0.1:8081` (gateway IP from Step 4)
4. Set **Auth Type** to Basic Auth
5. Enter your KTD staff username and password (default: `koha` / `koha`)
6. Click **Save**

### Step 6: Test the Connection

1. Go to **Stage 7 → Setup**
2. Click **Run Preflight**
3. You should see a successful connection with your Koha version

### Making the Tunnel Persistent (Windows)

**Option A: Windows Service with NSSM**

1. Download [NSSM](https://nssm.cc/) (Non-Sucking Service Manager)
2. Open an admin Command Prompt:

```cmd
nssm install CeruleanTunnel "C:\Windows\System32\OpenSSH\ssh.exe" "-R 8081:localhost:8081 root@cerulean-next.gallagher-family-hub.com -N -o ServerAliveInterval=60 -o ServerAliveCountMax=3 -i C:\Users\YOU\.ssh\id_rsa"
nssm start CeruleanTunnel
```

**Option B: Startup Script**

Create a `.bat` file in your Startup folder (`Win+R` → `shell:startup`):

```bat
@echo off
:loop
ssh -R 8081:localhost:8081 root@cerulean-next.gallagher-family-hub.com -N -o ServerAliveInterval=60 -o ServerAliveCountMax=3
timeout /t 5
goto loop
```

This reconnects automatically if the connection drops.

---

## Linux Instructions

### Step 1: Find Your KTD Port

```bash
docker ps | grep koha
```

Look for the port mapping (e.g., `0.0.0.0:8081->8081/tcp`). Note the left-side port.

### Step 2: Start the SSH Tunnel

```bash
ssh -R 8081:localhost:8081 root@cerulean-next.gallagher-family-hub.com -N
```

Replace `8081` with your KTD port (both occurrences). You will be prompted for the root password — **ask Brendan if you don't have it**. **Keep this terminal open.**

### Step 3: Verify the Tunnel (on the server)

In another terminal:

```bash
ssh root@cerulean-next.gallagher-family-hub.com
curl -s http://localhost:8081/api/v1/ | head -5
```

You should see a JSON response from Koha.

### Step 4: Find the Docker Gateway IP

On the server:

```bash
docker network inspect cerulean_cerulean | grep Gateway
```

Note the IP (e.g., `172.19.0.1`).

### Step 5: Configure Cerulean

1. Open Cerulean in your browser: `https://cerulean-next.gallagher-family-hub.com`
2. Open your project → **Settings**
3. Set **Koha URL** to `http://172.19.0.1:8081` (gateway IP from Step 4)
4. Set **Auth Type** to Basic Auth
5. Enter your KTD staff username and password (default: `koha` / `koha`)
6. Click **Save**

### Step 6: Test the Connection

1. Go to **Stage 7 → Setup**
2. Click **Run Preflight**
3. You should see a successful connection with your Koha version

### Making the Tunnel Persistent (Linux)

**Option A: autossh (recommended)**

```bash
# Install autossh
sudo apt install autossh    # Debian/Ubuntu
sudo dnf install autossh    # Fedora/RHEL

# Start a persistent tunnel (runs in background)
autossh -M 0 -R 8081:localhost:8081 root@cerulean-next.gallagher-family-hub.com -N -f
```

To stop the tunnel:

```bash
pkill -f "autossh.*8081"
```

**Option B: systemd service**

Create `/etc/systemd/system/cerulean-tunnel.service`:

```ini
[Unit]
Description=Cerulean SSH Tunnel to KTD
After=network-online.target
Wants=network-online.target

[Service]
Type=simple
ExecStart=/usr/bin/ssh -R 8081:localhost:8081 root@cerulean-next.gallagher-family-hub.com -N -o ServerAliveInterval=60 -o ServerAliveCountMax=3 -o ExitOnForwardFailure=yes
Restart=always
RestartSec=10
User=YOUR-USERNAME

[Install]
WantedBy=multi-user.target
```

Then enable and start:

```bash
sudo systemctl daemon-reload
sudo systemctl enable cerulean-tunnel
sudo systemctl start cerulean-tunnel

# Check status
sudo systemctl status cerulean-tunnel
```

This auto-starts on boot and restarts if the connection drops.

---

## Troubleshooting

### "Connection refused" when testing the tunnel

- Verify KTD is running: `docker ps | grep koha`
- Check the port number matches what KTD exposes
- On Mac, check if the SSH tunnel is still running: `ps aux | grep "ssh -R"`

### Cerulean says "connection refused" but `curl` works from the server

- You're probably using `http://localhost:8081` in Cerulean — change it to `http://172.19.0.1:8081` (the Docker gateway IP)
- The Cerulean web container can't reach the server's localhost directly

### "Permission denied" on SSH

- Ensure your SSH key is set up: `ssh-copy-id root@cerulean-next.gallagher-family-hub.com`
- Or use password auth (less secure for persistent tunnels)

### Tunnel drops frequently

- Add keepalive options: `ssh -R 8081:localhost:8081 root@cerulean-next.gallagher-family-hub.com -N -o ServerAliveInterval=60 -o ServerAliveCountMax=3`
- Use `autossh` (Mac) or the reconnecting batch script (Windows)

### Multiple engineers connecting their own KTD

Each engineer uses a **different port** to avoid conflicts:

| Engineer | Local KTD Port | Tunnel Command | Koha URL in Cerulean |
|----------|---------------|----------------|---------------------|
| Alice | 8081 | `ssh -R 9001:localhost:8081 ...` | `http://172.19.0.1:9001` |
| Bob | 8081 | `ssh -R 9002:localhost:8081 ...` | `http://172.19.0.1:9002` |
| Carol | 8081 | `ssh -R 9003:localhost:8081 ...` | `http://172.19.0.1:9003` |

Each person sets their project's Koha URL to their unique forwarded port.

### Security Notes

- The SSH tunnel is encrypted end-to-end
- Only the Cerulean server can access your local KTD through the tunnel
- The tunnel is only active while the SSH session is running
- For production Koha instances, use a direct HTTPS connection instead of a tunnel
