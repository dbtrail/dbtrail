# Bintrail — EC2 + RDS Deployment

Three-node setup: traffic and bintrail stream run on an app EC2, the bintrail index lives on a dedicated EC2 running Percona Server 8.0, and the source database is RDS MySQL 8.0.

## Architecture

```
                      your IP only
                          │
                SSH (22)  │  Grafana (3000)
                          ▼
┌─────────────────────────────────────────────────────┐
│  EC2 "app" (t4g.small, Ubuntu 24.04 ARM64)           │
│                                                      │
│  docker compose up:                                  │
│    ├── bintrail stream                               │
│    │     INDEX_DSN ──► EC2 "index" (3306)           │
│    │     SOURCE_DSN ──► RDS (3306)                  │
│    ├── traffic ──► RDS (3306)                       │
│    ├── prometheus (127.0.0.1:9090)                   │
│    └── grafana (:3000)                               │
└──────────────────────────────────────────────────────┘
         │                          │
         │  3306 (SG-to-SG)        │  3306 (SG-to-SG)
         ▼                          ▼
┌──────────────────────┐   ┌───────────────────────────┐
│  EC2 "index"          │   │  RDS MySQL 8.0             │
│  Percona Server 8.0   │   │    ├── demo                │
│    └── bintrail_index │   │    └── sbtest              │
│  Public access: NO    │   │  Public access: NO         │
└──────────────────────┘   └───────────────────────────┘
```

Three security groups:
- `bintrail-ec2-sg` — SSH 22 + TCP 3000 from your IP
- `bintrail-index-sg` — SSH 22 from your IP, MySQL 3306 from `bintrail-ec2-sg`
- `bintrail-rds-sg` — MySQL 3306 from `bintrail-ec2-sg`

Prometheus binds to `127.0.0.1:9090` only (not internet-exposed).

---

## Part A — Create RDS Parameter Group

Do this **before** creating the RDS instance.

1. RDS → **Parameter groups** → **Create parameter group**
2. Family: `mysql8.0`, Name: `bintrail-binlog`, Description: "Binlog ROW+FULL for bintrail"
3. **Edit parameters** — set these values:

   | Parameter | Value |
   |---|---|
   | `binlog_format` | `ROW` |
   | `binlog_row_image` | `FULL` |
   | `gtid-mode` | `ON` |
   | `enforce_gtid_consistency` | `ON` |
   | `log_bin_trust_function_creators` | `1` |
   | `binlog_expire_logs_seconds` | `604800` |

4. **Save changes**

---

## Part B — Create RDS MySQL Instance

1. RDS → **Create database**
2. Engine: **MySQL 8.0**
3. Template: **Free tier**
4. DB instance identifier: `bintrail-demo`
5. Instance class: **db.t4g.micro** (Graviton, free tier eligible)
6. Master username: `admin` — set a strong password
7. Connectivity:
   - VPC: default
   - **Public access: No**
   - Create new security group: `bintrail-rds-sg`
8. Additional configuration:
   - DB parameter group: `bintrail-binlog` (from Part A)
   - Leave "Initial database name" blank (RDS no longer hosts the index)
   - Enable automated backups: **yes** (required for binlogs on RDS)
   - Backup retention: 7 days
8b. Encryption: **Enable encryption** (check "Enable encryption" under Storage → Encryption). Free, no performance penalty.
9. **Create database** — wait ~5 min for status: Available
10. Note the **Endpoint** (e.g. `bintrail-demo.abc123.us-east-1.rds.amazonaws.com`)

---

## Part C — Create EC2 "index" Instance

This instance will run Percona Server 8.0 and store the bintrail index.

1. EC2 → **Launch instance**
2. Name: `bintrail-index`
3. AMI: **Ubuntu Server 24.04 LTS** — select the **arm64** version
4. Instance type: **t4g.micro** (1 vCPU, 1 GB RAM — index writes are light)
5. Key pair: same as app EC2
6. Network settings → **Create security group**: `bintrail-index-sg`
   - SSH (22): Source = **My IP**
   - _(MySQL 3306 rule added in Part E — needs app SG to exist first)_
7. Storage: 20 GB gp3, **Encrypted: Yes**
8. **Launch instance** — note the **private IP** (e.g. `10.0.1.42`)

---

## Part D — Create EC2 "app" Instance

1. EC2 → **Launch instance**
2. Name: `bintrail-app`
3. AMI: **Ubuntu Server 24.04 LTS** — select the **arm64** version
4. Instance type: **t4g.small** (2 vCPU, 2 GB RAM)
5. Key pair: create or select existing
6. Network settings → **Create security group**: `bintrail-ec2-sg`
   - SSH (22): Source = **My IP**
   - Custom TCP 3000: Source = **My IP** (Grafana)
   - ⚠️ Do NOT expose 9090 — Prometheus stays internal
7. Storage: 20 GB gp3, **Encrypted: Yes**
8. **Launch instance** — note the public IP

---

## Part E — Connect Security Groups

Three inbound rules to add:

**1. App EC2 → RDS (MySQL 3306):**
1. RDS → **bintrail-demo** → **Security** tab → click `bintrail-rds-sg`
2. **Edit inbound rules** → **Add rule**:
   - Type: MySQL/Aurora (3306), Source: `bintrail-ec2-sg`
3. **Save rules**

**2. App EC2 → Index EC2 (MySQL 3306):**
1. EC2 → **Security Groups** → `bintrail-index-sg`
2. **Edit inbound rules** → **Add rule**:
   - Type: MySQL/Aurora (3306), Source: `bintrail-ec2-sg`
3. **Save rules**

Result:
- `bintrail-rds-sg`: one inbound rule — 3306 from `bintrail-ec2-sg`
- `bintrail-index-sg`: SSH from your IP + 3306 from `bintrail-ec2-sg`
- No `0.0.0.0/0` rules anywhere.

---

## Part F — Set Up Index EC2 (Percona Server)

```bash
# From your laptop (or the app EC2 after cloning in Part H):
scp -i key.pem ~/bintrail/deploy/setup-index.sh ubuntu@<INDEX_EC2_IP>:~

# SSH into the index EC2
ssh -i key.pem ubuntu@<INDEX_EC2_IP>
bash ~/setup-index.sh
```

The script will:
- Install Percona Server 8.0 via the official apt repository
- Bind MySQL to `0.0.0.0` (security comes from the EC2 security group)
- Create the `bintrail_index` database
- Create a `bintrail` user with access only to `bintrail_index`
- Print the private IP and next steps

Note the **private IP** shown — you'll need it for `.env` in Part H.

---

## Part G — Initialize Demo Schema on RDS

Since RDS has public access OFF, connect through the app EC2:

```bash
ssh -i key.pem ubuntu@<APP_EC2_IP>

# Test connectivity to RDS
mysql -h <RDS_ENDPOINT> -u admin -p -e "SELECT 1"

# Load demo schema (creates demo + sbtest databases)
mysql -h <RDS_ENDPOINT> -u admin -p < ~/bintrail/demo/sql/00-schema.sql

# Verify
mysql -h <RDS_ENDPOINT> -u admin -p -e "SHOW DATABASES"
# Expected: demo, sbtest, information_schema, ...
```

---

## Part G½ — Create Dedicated RDS Users

Still connected to the app EC2, create least-privilege users so bintrail and the traffic generator do not share the RDS admin account:

```bash
mysql -h <RDS_ENDPOINT> -u admin -p
```

```sql
-- For bintrail stream (reads binlogs + schema metadata)
CREATE USER 'bintrail_reader'@'%' IDENTIFIED BY <choose a password>;
GRANT REPLICATION SLAVE, REPLICATION CLIENT ON *.* TO 'bintrail_reader'@'%';
GRANT SELECT ON demo.* TO 'bintrail_reader'@'%';
GRANT SELECT ON sbtest.* TO 'bintrail_reader'@'%';

-- For the traffic generator (DML on demo only)
CREATE USER 'traffic'@'%' IDENTIFIED BY <choose a password>;
GRANT SELECT, INSERT, UPDATE, DELETE ON demo.* TO 'traffic'@'%';
```

Put a password of your own in quotes where each line says `<choose a password>`.
As written, MySQL refuses the line on purpose, so no account is created with a
password copied from this page. Use the passwords you chose here as
`BINTRAIL_RDS_PASSWORD` and `TRAFFIC_RDS_PASSWORD` in `.env`.

---

## Part H — Deploy App on EC2

From your laptop, copy the bootstrap script to the EC2:

```bash
scp -i key.pem ~/bintrail/deploy/setup.sh ubuntu@<APP_EC2_IP>:~
```

SSH in and run it (installs git, Docker, mysql-client):

```bash
ssh -i key.pem ubuntu@<APP_EC2_IP>
bash ~/setup.sh

# Re-login for Docker group (or run: newgrp docker)
exit
ssh -i key.pem ubuntu@<APP_EC2_IP>
```

> **Private repo access**: generate a read-only deploy key on the EC2 rather than copying your personal SSH key:
> ```bash
> ssh-keygen -t ed25519 -f ~/.ssh/bintrail-deploy -N ""
> cat ~/.ssh/bintrail-deploy.pub
> # Add this as a deploy key: GitHub → repo Settings → Deploy keys → Add (read-only)
> ```
> Then configure SSH to use it:
> ```bash
> cat >> ~/.ssh/config <<'EOF'
> Host github.com
>     IdentityFile ~/.ssh/bintrail-deploy
> EOF
> ```

Clone the repo and create `.env`:

```bash
git clone git@github.com:dbtrail/dbtrail.git ~/bintrail
cd ~/bintrail/deploy
cp .env.example .env
chmod 600 .env
nano .env
```

Fill in `.env`:
```bash
# RDS — source database
RDS_ENDPOINT=bintrail-demo.abc123.us-east-1.rds.amazonaws.com

# bintrail stream (replication reader — created in Part G½)
BINTRAIL_RDS_USER=bintrail_reader
BINTRAIL_RDS_PASSWORD=your-bintrail-reader-password

# traffic generator (DML only — created in Part G½)
TRAFFIC_RDS_USER=traffic
TRAFFIC_RDS_PASSWORD=your-traffic-password

# Index EC2 — Percona Server
INDEX_HOST=10.0.1.42          # private IP from Part C / Part F
INDEX_USER=bintrail
INDEX_PASSWORD=your-index-password

# Grafana
GRAFANA_PASSWORD=your-grafana-admin-password

# bintrail settings
SCHEMAS=demo,sbtest
SERVER_ID=99999
BATCH_SIZE=500
CHECKPOINT=5
```

```bash
# Verify connectivity to both databases before launching
mysql -h <INDEX_HOST> -u bintrail -p -e "SHOW DATABASES"             # → bintrail_index
mysql -h <RDS_ENDPOINT> -u bintrail_reader -p -e "SELECT 1"          # → replication reader
mysql -h <RDS_ENDPOINT> -u traffic -p -e "USE demo; SELECT 1"        # → traffic user

# Lock down credentials
chmod 600 ~/bintrail/deploy/.env

# Build and launch
cd ~/bintrail/deploy
docker compose up --build -d

# Verify
docker compose logs -f bintrail   # look for "Checkpoint saved"
```

---

## Part I — Access & Verify

**From your laptop:**

- **Grafana**: `http://<APP_EC2_PUBLIC_IP>:3000` — log in as `admin` / `GRAFANA_PASSWORD`

**From app EC2 (via SSH):**

```bash
# Prometheus targets (bintrail-stream should be UP)
curl -s localhost:9090/targets | grep -A3 bintrail

# Logs
cd ~/bintrail/deploy && docker compose logs -f

# Test checkpoint recovery
docker compose down && docker compose up -d
docker compose logs -f bintrail   # should resume from saved GTID position
```

---

## Security checklist

- [ ] RDS **public access: No**
- [ ] RDS security group has **only** one inbound rule: 3306 from `bintrail-ec2-sg`
- [ ] Index EC2 security group has: SSH from your IP, 3306 from `bintrail-ec2-sg` only
- [ ] App EC2 security group has: SSH (22) + TCP 3000 from your IP only
- [ ] No `0.0.0.0/0` inbound rules anywhere
- [ ] Prometheus (9090) bound to **127.0.0.1 only** in `compose.yml`
- [ ] `.env` is `chmod 600`
- [ ] RDS master password is not in any committed file (`.env` is gitignored)
- [ ] RDS and EBS volumes encrypted at rest
- [ ] Dedicated RDS users (not admin) for bintrail and traffic
- [ ] Deploy key (not personal SSH key) for GitHub access
- [ ] Grafana has authentication enabled (no anonymous access)

---

## Teardown

```bash
# Stop services
cd ~/bintrail/deploy && docker compose down

# AWS Console:
#   - Terminate both EC2 instances (app + index)
#   - Delete RDS instance (delete automated backups when prompted if not needed)
```
