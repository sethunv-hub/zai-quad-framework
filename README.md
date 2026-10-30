# ZAI QUAD FRAMEWORK — Cloud Server (Render.com Free Tier)

Runs 24/7 on Render.com's **free tier**. Pushes ntfy signals to your phone when A-grade SMC setups form on BTC, Gold, Silver, or EUR/USD. **No laptop required. No credit card required. Truly $0/month.**

## What this is

A Node.js server that:
- Connects to Binance WebSockets (4 instruments in parallel)
- Builds 15-min candles from tick data
- Runs the exact same SMC engine as your `index.html` (sweeps, bias, triggers, TP1/TP2/TP3, A-grade filter, graduated cooldown, counter-trend filter)
- Pushes ntfy alerts to `sethu-btc` when A-grade signals fire
- **Real-time** — no 5-min cron lag like GitHub Actions
- Survives your laptop being off

You keep `index.html` as a *dashboard* for when you want to see live status — but signals fire whether or not you have a browser open.

## Cost

**$0/month.** Render.com free tier includes:
- 750 hours/month web service runtime (~31 days, full month)
- 512MB RAM, 0.1 CPU (enough for this server)
- Automatic HTTPS
- Custom subdomain (`your-app.onrender.com`)

The catch: free tier services sleep after 15 minutes of no HTTP requests. We solve this with a free keep-alive ping (cron-job.org) that hits your service every 10 minutes. **Total cost: $0.**

---

## Deploy — Step by step (20 minutes)

### Step 1: Push this folder to a GitHub repo

1. Create a new **public** GitHub repo (private also works on free tier, but public is simpler)
   - Go to https://github.com/new
   - Name it `zai-quad-framework` (or whatever you want)
   - Set to Public
   - Click "Create repository"
2. Push the contents of this `cloud-server/` folder to that repo:
   ```bash
   cd cloud-server
   git init
   git add .
   git commit -m "ZAI QUAD FRAMEWORK cloud server"
   git branch -M main
   git remote add origin https://github.com/YOUR_USERNAME/zai-quad-framework.git
   git push -u origin main
   ```

### Step 2: Sign up for Render.com

1. Go to https://render.com
2. Click "Sign Up" → choose "GitHub" login (uses your GitHub account, no credit card)
3. Authorize Render to access your GitHub

### Step 3: Create the web service

1. In Render dashboard, click **New +** → **Web Service**
2. Find your `zai-quad-framework` repo → click **Connect**
3. Fill in the form:
   - **Name:** `zai-quad-framework` (or whatever you want — must be unique)
   - **Region:** `Singapore` (closest to Manila)
   - **Runtime:** `Node`
   - **Start Command:** `node server.js` (Render auto-detects `package.json`, but set this explicitly)
   - **Instance Type:** `Free` (the $0 one)
4. Click **Advanced** → set environment variables:
   - `NTFY_TOPIC` = `sethu-btc`
   - `MIN_GRADE` = `A`
   - `BTC_OFFSET` = (your synced BTC offset, e.g., `126.2`)
   - `GOLD_OFFSET` = (your synced Gold offset, e.g., `10.2`)
   - `SILVER_OFFSET` = (your synced Silver offset, e.g., `0.05`)
   - `EUR_OFFSET` = (your synced EUR offset, e.g., `0.00015`)
5. Click **Create Web Service**

Render will now build and deploy. Takes 2–3 minutes.

### Step 4: Get your service URL

Once deployed, Render shows your service URL like:
```
https://zai-quad-framework.onrender.com
```

Open it in browser — you should see just `OK` (the ping response).

Open `https://zai-quad-framework.onrender.com/health` — you should see JSON with all 4 engine statuses.

### Step 5: Set up keep-alive ping (CRITICAL — without this, free tier sleeps)

1. Go to https://cron-job.org/en/ and create a free account
2. Click **Create Cron Job**
3. Fill in:
   - **Title:** `ZAI keep-alive`
   - **URL:** `https://zai-quad-framework.onrender.com/ping` (use your actual URL)
   - **Execution schedule:** Every 10 minutes (use the "Every 10 minutes" preset)
   - **Notifications:** Only on failure (optional, recommended)
4. Click **Save**

Now cron-job.org will ping your service every 10 minutes, which keeps Render's free tier from sleeping. **Your server now runs 24/7.**

**Alternative keep-alive services** (pick any one):
- https://uptimerobot.com (free, pings every 5 min)
- https://cron-job.org (free, pings every 1 min, no email verification needed)
- https://freshworks.com/freshping (free, pings every 1 min)

### Step 6: Test the ntfy push

From your laptop:
```bash
curl "https://ntfy.sh/sethu-btc/publish?message=Test%20from%20ZAI%20Render%20server&title=Test&priority=urgent"
```

You should get the test notification on your phone. The server itself will push real signals automatically when A-grade setups form.

### Step 7: Verify it survives your laptop being off

1. Open `https://zai-quad-framework.onrender.com/health` — note the uptime
2. Close your laptop, wait 30 minutes
3. Open the URL again — uptime should still be increasing (didn't reset)
4. If uptime reset to 0, the keep-alive ping isn't working — check cron-job.org dashboard

---

## Endpoints

| Endpoint | Purpose |
|---|---|
| `GET /` or `/ping` | Lightweight `OK` response — for keep-alive pings |
| `GET /health` | JSON status of all 4 engines (price, bias, signal, last fire time) |
| `GET /journal` | JSON array of all fired signals |
| `GET /journal.csv` | Download journal as CSV file (open in Excel) |
| `POST /offset/BTC/126.2` | Update BTC Exness offset at runtime (no restart needed) |
| `POST /offset/XAU/10.2` | Update Gold offset |
| `POST /offset/XAG/0.05` | Update Silver offset |
| `POST /offset/EUR/0.00015` | Update EUR offset |

---

## Managing the server

### Check logs
Render dashboard → your service → **Logs** tab. Shows real-time logs — connection status, signal fires, errors.

### Update code
After pushing new commits to your GitHub repo's main branch, Render auto-deploys within 1–2 minutes.

### Update environment variables (e.g., offsets)
Render dashboard → your service → **Environment** tab → edit and save. Service restarts automatically.

### Manual restart
Render dashboard → your service → top right **Manual Deploy** → **Clear build cache & deploy**

### Pause the server (stop the billing cycle — though it's free)
Render dashboard → your service → **Suspend**. Click **Resume** later.

---

## Updating Exness offsets

Exness spreads drift over time. Refresh offsets weekly:

### Option A: Via Render dashboard (easy)
1. Render dashboard → your service → **Environment**
2. Edit `BTC_OFFSET`, `GOLD_OFFSET`, etc.
3. Save — service restarts with new offsets

### Option B: Via HTTP endpoint (no restart)
```bash
curl -X POST https://zai-quad-framework.onrender.com/offset/BTC/126.2
curl -X POST https://zai-quad-framework.onrender.com/offset/XAU/10.2
curl -X POST https://zai-quad-framework.onrender.com/offset/XAG/0.05
curl -X POST https://zai-quad-framework.onrender.com/offset/EUR/0.00015
```

Note: Option B updates in-memory only. They reset on next deploy. Use Option A for permanent changes.

### How to find your offsets
1. Open `index.html` in browser
2. Click Sync on each instrument (enter Exness price)
3. The offset shown next to the Sync button (e.g., `+$126.2`) is what you set on the server

---

## What to expect

### Signal frequency
- **A-grade only** (default): 0–3 signals per day total across all 4 instruments
- Some days: zero signals. That's the filter working — be patient.
- Active trading days (London + NY overlap, high volatility): 2–4 signals possible

### Reliability
- Server auto-restarts on crash (Render handles this)
- WebSocket auto-reconnects on disconnect (2-second delay)
- Heartbeat check every 15 seconds — forces reconnect if no data for 45s
- Keep-alive ping prevents free-tier sleep

### When Render has issues
- Render status page: https://status.render.com
- If your service goes down, you'll miss signals until it's back up
- cron-job.org can email you if pings fail (sign up for that notification)

---

## Troubleshooting

### "Service keeps sleeping even with ping"
1. Verify cron-job.org is actually pinging — check its dashboard for execution history
2. Verify ping URL is correct: `https://YOUR-APP.onrender.com/ping`
3. Render free tier officially sleeps after 15 min of NO requests. Pings every 10 min should prevent this. If still sleeping, switch to UptimeRobot (pings every 5 min).

### "No signals firing"
1. Check `/health` — are prices updating?
2. Check Render logs — any errors?
3. A-only is strict — try setting `MIN_GRADE=AB` in Render Environment to loosen

### "ntfy not receiving"
1. Verify topic in `/health` output
2. Manually test: `curl "https://ntfy.sh/sethu-btc/publish?message=test"`
3. Check ntfy app on phone is subscribed to `sethu-btc`

### "WebSocket keeps disconnecting"
- Normal — Binance disconnects idle connections every 24h
- Server auto-reconnects in 2 seconds
- Check Render logs for reconnect frequency — if every few minutes, that's a problem (might be Render's network)

### "EUR/USD shows CLOSED"
- That's the weekend suppress working — Fri 22:00 PHT → Sun 00:00 PHT
- Other 3 instruments keep running 24/7

### "Render says free tier hours exhausted"
- You get 750 hours/month per service. One service running 24/7 = ~720 hours. Should fit.
- If you have other Render free services, they share the 750-hour quota across all of them.
- Solution: pause services you don't need, or upgrade this one to Starter ($7/month).

---

## Files in this folder

| File | Purpose |
|---|---|
| `server.js` | Main server — WebSockets, ntfy push, HTTP endpoints |
| `smc-engine.js` | SMC logic ported from `index.html` |
| `package.json` | Node.js dependencies (just `ws`) |
| `Dockerfile` | Container definition (optional — Render can also use native Node runtime) |
| `render.yaml` | Render.com deployment config (can use Render dashboard instead) |
| `README.md` | This file |
| `fly.toml` | Alternative deploy config for Fly.io ($3/month option for later) |

---

## Migrating to Fly.io later (when you're ready to pay)

The server code is identical — only the deploy target changes. When you decide to upgrade:

1. Install `flyctl`: `brew install flyctl` (macOS) or `curl -L https://fly.io/install.sh | sh` (Linux)
2. From this folder: `fly deploy`
3. Set secrets: `fly secrets set NTFY_TOPIC=sethu-btc MIN_GRADE=A BTC_OFFSET=126.2 ...`
4. Done. Same server, paid hosting, no keep-alive hack needed.

The `fly.toml` file in this folder is already configured for that. Just run `fly deploy` and it works.

---

## What's NOT in this server (intentionally)

- **No auto-execution** — you manually place trades on Exness mobile after ntfy alert
- **No dashboard** — use `index.html` if you want visual monitoring
- **No backtesting** — that's a separate project
- **No ML** — pure rule-based SMC, deterministic and debuggable

This is a **signal service**, not a trading bot. It tells you when to look at the market. You decide whether to take the trade.

---

## Cost breakdown (all $0)

- Render.com free tier web service: **$0**
- cron-job.org keep-alive: **$0**
- Binance API: **$0**
- ntfy.sh: **$0**
- GitHub repo (public): **$0**
- **Total: $0/month, forever (until Render changes their free tier policy)**

If Render ever changes their free tier, you can migrate to Fly.io in 10 minutes (same code, different deploy command).
