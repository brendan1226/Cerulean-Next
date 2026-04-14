# Deploying Cerulean Next to DigitalOcean

Deploy Cerulean Next to a DigitalOcean Droplet with HTTPS via Let's Encrypt, running the same Docker Compose stack behind an nginx reverse proxy.

**Target URL**: `https://cerulean-next.gallagher-family-hub.com`

---

## 1. Provision a Droplet

If your existing `gallagher-family-hub.com` droplet has capacity, you can run Cerulean on the same one. Otherwise, create a new droplet:

- **Image**: Ubuntu 24.04
- **Size**: 4 GB RAM / 2 vCPUs minimum (8 GB recommended for AI + Celery workers)
- **Region**: same as your existing droplet

Install Docker + Docker Compose if it's a new droplet:

```bash
curl -fsSL https://get.docker.com | sh
sudo usermod -aG docker $USER
```

---

## 2. DNS

In DigitalOcean **Networking > Domains > gallagher-family-hub.com**, add:

| Type | Hostname | Value | TTL |
|------|----------|-------|-----|
| A | cerulean-next | `<droplet IP>` | 3600 |

If using the same droplet as the existing app, point to the same IP.

---

## 3. Clone and Configure

```bash
ssh root@<droplet-ip>
cd /opt
git clone https://github.com/brendan1226/Cerulean-Next.git cerulean
cd cerulean
```

Create the production `.env`:

```bash
cat > .env << 'EOF'
# Database
DATABASE_URL=postgresql+asyncpg://cerulean:STRONG-PASSWORD-HERE@postgres:5432/cerulean

# Redis
REDIS_URL=redis://redis:6379/0
CELERY_RESULT_BACKEND=redis://redis:6379/1

# Security — generate a real key:
#   python3 -c "import secrets; print(secrets.token_urlsafe(64))"
SECRET_KEY=GENERATE-A-REAL-SECRET-HERE
FERNET_KEY=

# JWT
JWT_ALGORITHM=HS256
JWT_EXPIRY_MINUTES=480

# AI
ANTHROPIC_API_KEY=sk-ant-...your-key...
ANTHROPIC_MODEL=claude-sonnet-4-20250514

# Storage
DATA_ROOT=/data/projects

# Google OAuth (can also set via System Settings GUI after deploy)
GOOGLE_CLIENT_ID=
GOOGLE_CLIENT_SECRET=
GOOGLE_ALLOWED_DOMAIN=bywatersolutions.com

# Flower
FLOWER_USER=admin
FLOWER_PASSWORD=CHANGE-THIS

# App
DEBUG=false
LOG_LEVEL=info
EOF
```

> **Important**: Use a strong, unique password for `POSTGRES_PASSWORD` and match it in `DATABASE_URL`. Generate `SECRET_KEY` with `python3 -c "import secrets; print(secrets.token_urlsafe(64))"`.

---

## 4. Production Docker Compose Override

Create `docker-compose.prod.yml`:

```yaml
services:
  web:
    restart: always
    command: uvicorn cerulean.main:app --host 0.0.0.0 --port 8000 --workers 6
    environment:
      - DATABASE_URL=${DATABASE_URL}
      - REDIS_URL=${REDIS_URL}
      - DATA_ROOT=/data/projects
      - ANTHROPIC_API_KEY=${ANTHROPIC_API_KEY}
      - SECRET_KEY=${SECRET_KEY}
      - GOOGLE_CLIENT_ID=${GOOGLE_CLIENT_ID:-}
      - GOOGLE_CLIENT_SECRET=${GOOGLE_CLIENT_SECRET:-}
      - GOOGLE_ALLOWED_DOMAIN=${GOOGLE_ALLOWED_DOMAIN:-bywatersolutions.com}
    # Don't expose port directly — nginx handles this
    ports: !override []
    expose:
      - "8000"
    networks:
      - cerulean

  worker:
    restart: always
    environment:
      - DATABASE_URL=${DATABASE_URL}
      - REDIS_URL=${REDIS_URL}
      - DATA_ROOT=/data/projects
      - ANTHROPIC_API_KEY=${ANTHROPIC_API_KEY}

  beat:
    restart: always

  postgres:
    restart: always
    environment:
      POSTGRES_PASSWORD: STRONG-PASSWORD-HERE

  redis:
    restart: always
    # Don't expose Redis to the host in prod
    ports: !override []

  nginx:
    image: nginx:alpine
    restart: always
    ports:
      - "80:80"
      - "443:443"
    volumes:
      - ./nginx/cerulean.conf:/etc/nginx/conf.d/default.conf:ro
      - certbot-etc:/etc/letsencrypt:ro
      - certbot-var:/var/lib/letsencrypt
    depends_on:
      - web
    networks:
      - cerulean

  certbot:
    image: certbot/certbot
    volumes:
      - certbot-etc:/etc/letsencrypt
      - certbot-var:/var/lib/letsencrypt
    entrypoint: "/bin/sh -c 'trap exit TERM; while :; do sleep 12h; certbot renew; done'"

volumes:
  certbot-etc:
  certbot-var:
```

---

## 5. Nginx Config

```bash
mkdir -p nginx
```

Create `nginx/cerulean.conf`:

```nginx
server {
    listen 80;
    server_name cerulean-next.gallagher-family-hub.com;

    # Let's Encrypt challenge
    location /.well-known/acme-challenge/ {
        root /var/lib/letsencrypt;
    }

    # Redirect everything else to HTTPS
    location / {
        return 301 https://$host$request_uri;
    }
}

server {
    listen 443 ssl http2;
    server_name cerulean-next.gallagher-family-hub.com;

    ssl_certificate /etc/letsencrypt/live/cerulean-next.gallagher-family-hub.com/fullchain.pem;
    ssl_certificate_key /etc/letsencrypt/live/cerulean-next.gallagher-family-hub.com/privkey.pem;

    client_max_body_size 500M;  # large MARC file uploads

    location / {
        proxy_pass http://web:8000;
        proxy_set_header Host $host;
        proxy_set_header X-Real-IP $remote_addr;
        proxy_set_header X-Forwarded-For $proxy_add_x_forwarded_for;
        proxy_set_header X-Forwarded-Proto $scheme;
        proxy_read_timeout 600s;  # long-running API calls
        proxy_send_timeout 600s;
    }
}
```

---

## 6. Get SSL Certificate

First start with a temporary HTTP-only nginx config for cert issuance:

```bash
# Temporary HTTP-only config
cat > nginx/cerulean.conf << 'TEMP'
server {
    listen 80;
    server_name cerulean-next.gallagher-family-hub.com;
    location /.well-known/acme-challenge/ {
        root /var/lib/letsencrypt;
    }
    location / {
        proxy_pass http://web:8000;
    }
}
TEMP

# Start nginx
docker compose -f docker-compose.yml -f docker-compose.prod.yml up -d nginx

# Request the certificate
docker compose -f docker-compose.yml -f docker-compose.prod.yml run --rm certbot \
  certonly --webroot --webroot-path=/var/lib/letsencrypt \
  -d cerulean-next.gallagher-family-hub.com \
  --email your-email@bywatersolutions.com \
  --agree-tos --no-eff-email
```

Then restore the full HTTPS nginx config from Step 5 and restart:

```bash
# Re-create nginx/cerulean.conf with the full config from Step 5, then:
docker compose -f docker-compose.yml -f docker-compose.prod.yml restart nginx
```

---

## 7. Migrate Local Data to the Server

### On your local machine

```bash
cd /Users/brendan/git/Cerulean-Next

# Dump the database
docker compose exec postgres pg_dump -U cerulean cerulean > cerulean_backup.sql

# Export project data files
docker compose exec web tar czf /tmp/projects.tar.gz -C /data projects
docker cp $(docker compose ps -q web):/tmp/projects.tar.gz ./projects.tar.gz

# Upload both to the server
scp cerulean_backup.sql projects.tar.gz root@<droplet-ip>:/opt/cerulean/
```

### On the server

```bash
cd /opt/cerulean

# Start just postgres first
docker compose -f docker-compose.yml -f docker-compose.prod.yml up -d postgres
sleep 5

# Restore the database
docker compose -f docker-compose.yml -f docker-compose.prod.yml exec -T postgres \
  psql -U cerulean cerulean < cerulean_backup.sql

# Copy project files into the web container's data volume
docker cp projects.tar.gz \
  $(docker compose -f docker-compose.yml -f docker-compose.prod.yml ps -q web):/tmp/
docker compose -f docker-compose.yml -f docker-compose.prod.yml exec web \
  tar xzf /tmp/projects.tar.gz -C /data
```

---

## 8. Start Everything

```bash
cd /opt/cerulean
docker compose -f docker-compose.yml -f docker-compose.prod.yml up -d --build
```

Verify:

```bash
# Check all containers are running
docker compose -f docker-compose.yml -f docker-compose.prod.yml ps

# Health check
curl -s https://cerulean-next.gallagher-family-hub.com/api/health
# Expected: {"status":"ok","version":"1.0.0"}
```

---

## 9. Update Google OAuth Callback URI

Once DNS has propagated and HTTPS is working:

1. Go to [Google Cloud Console](https://console.cloud.google.com/) > **APIs & Services** > **Credentials**
2. Click on your OAuth 2.0 Client ID
3. Under **Authorized redirect URIs**, add:
   ```
   https://cerulean-next.gallagher-family-hub.com/api/v1/auth/google/callback
   ```
4. Optionally keep the localhost URI for local development:
   ```
   http://localhost:8000/api/v1/auth/google/callback
   ```
5. Click **Save**

The `google_client_id` and `google_client_secret` values will carry over from your database backup. You can verify/update them in the Cerulean **System Settings** page after logging in.

---

## 10. Auto-Renew SSL

Add a weekly cron job to renew the Let's Encrypt certificate:

```bash
crontab -e
```

Add this line:

```
0 3 * * 1 cd /opt/cerulean && docker compose -f docker-compose.yml -f docker-compose.prod.yml run --rm certbot renew && docker compose -f docker-compose.yml -f docker-compose.prod.yml restart nginx
```

---

## Quick Reference

| What | URL |
|------|-----|
| App | `https://cerulean-next.gallagher-family-hub.com` |
| Health Check | `https://cerulean-next.gallagher-family-hub.com/api/health` |
| API Docs | `https://cerulean-next.gallagher-family-hub.com/api/docs` |
| OAuth Callback | `https://cerulean-next.gallagher-family-hub.com/api/v1/auth/google/callback` |
| Flower (monitoring) | Port 5555 — keep firewalled, access via SSH tunnel: `ssh -L 5555:localhost:5555 root@<ip>` |

---

## Troubleshooting

### Containers won't start
```bash
docker compose -f docker-compose.yml -f docker-compose.prod.yml logs web --tail=50
docker compose -f docker-compose.yml -f docker-compose.prod.yml logs worker --tail=50
```

### Database migrations
```bash
docker compose -f docker-compose.yml -f docker-compose.prod.yml exec web alembic upgrade head
```

### OAuth not working after deploy
- Verify the callback URI in Google Cloud Console matches exactly
- Check System Settings page for correct `google_client_id`
- Restart web after changing OAuth settings: `docker compose -f docker-compose.yml -f docker-compose.prod.yml restart web`

### Updating the app
```bash
cd /opt/cerulean
git pull
docker compose -f docker-compose.yml -f docker-compose.prod.yml up -d --build
docker compose -f docker-compose.yml -f docker-compose.prod.yml exec web alembic upgrade head
```
