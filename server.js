// server.js — ZAI QUAD FRAMEWORK cloud server
// Runs 24/7 on Fly.io. Connects to Binance WebSockets, evaluates SMC signals, pushes ntfy alerts.
// No dashboard. No browser. Just signals to your phone.

const WebSocket = require('ws');
const { makeEngine, bootstrap, handlePrice, scanSig, fmtPht, fmtPrice, isFxWeekend } = require('./smc-engine');

// ===== CONFIGURATION =====
// All configurable via environment variables (set in fly.toml or `fly secrets set`)
const CONFIG = {
  ntfyTopic: process.env.NTFY_TOPIC || 'sethu-btc',
  minGrade: process.env.MIN_GRADE || 'A',          // A, AB, or ABC
  // Per-instrument Exness offsets — set via env vars or fetch from a config endpoint
  btcOffset: parseFloat(process.env.BTC_OFFSET || '0'),
  goldOffset: parseFloat(process.env.GOLD_OFFSET || '0'),
  silverOffset: parseFloat(process.env.SILVER_OFFSET || '0'),
  eurOffset: parseFloat(process.env.EUR_OFFSET || '0'),
  // Reconnect delay (ms) if WebSocket drops
  reconnectDelay: 2000
};

// ===== ENGINES — same config as the HTML version =====
const engines = [
  makeEngine('BTC', 'btcusdt', {
    prefix: 'btc', slBuffer: 50, minRisk: 150, maxRisk: 3000,
    roundStars: 2, riskWidePct: 0.008, breakoutTh: 0.003, pullbackTh: 0.01,
    roundStep: 1000, weekendSuppress: false, futures: false
  }),
  makeEngine('XAU', 'paxgusdt', {
    prefix: 'gold', slBuffer: 2.5, minRisk: 6, maxRisk: 30,
    roundStars: 1, riskWidePct: 0.008, breakoutTh: 0.0015, pullbackTh: 0.005,
    roundStep: 100, weekendSuppress: true, futures: false
  }),
  makeEngine('XAG', 'xagusdt', {
    prefix: 'silver', slBuffer: 0.05, minRisk: 0.10, maxRisk: 0.50,
    roundStars: 2, riskWidePct: 0.015, breakoutTh: 0.003, pullbackTh: 0.007,
    roundStep: 1, weekendSuppress: true, futures: true  // Silver only on Binance futures
  }),
  makeEngine('EUR', 'eurusdt', {
    prefix: 'eur', slBuffer: 0.0008, minRisk: 0.0008, maxRisk: 0.0050,
    roundStars: 3, riskWidePct: 0.008, breakoutTh: 0.0005, pullbackTh: 0.002,
    roundStep: 0.01, weekendSuppress: true, futures: false
  })
];

const offsetMap = {
  BTC: CONFIG.btcOffset, XAU: CONFIG.goldOffset,
  XAG: CONFIG.silverOffset, EUR: CONFIG.eurOffset
};

// ===== ntfy PUSH =====
async function sendNtfy(title, message) {
  try {
    const params = new URLSearchParams({
      message, title, priority: 'urgent', tags: 'chart_with_upwards_trend'
    });
    await fetch(`https://ntfy.sh/${encodeURIComponent(CONFIG.ntfyTopic)}/publish?${params.toString()}`, {
      method: 'GET',
      mode: 'no-cors'
    });
    console.log(`[ntfy] Pushed: ${title}`);
  } catch (e) {
    console.error(`[ntfy] Push failed:`, e.message);
  }
}

// ===== SIGNAL HANDLER — fires when a valid signal passes all filters =====
function onSignal(e, sig) {
  const nameMap = { BTC: 'BTC', XAU: 'XAU', XAG: 'XAG', EUR: 'EUR' };
  const instShort = nameMap[e.symbol] || e.symbol;
  const dir = sig.action === 'LONG' ? 'BUY' : 'SELL';
  const emoji = sig.action === 'LONG' ? '📈' : '📉';
  const title = `${emoji} ZAI ${instShort} ${dir}`;
  const phtStr = fmtPht(Date.now());
  const msg = `Entry: ${fmtPrice(e.symbol, sig.entry)}
SL: ${fmtPrice(e.symbol, sig.sl)}
TP: ${fmtPrice(e.symbol, sig.tp1)}
Time (PHT): ${phtStr}
Grade: ${sig.quality} • Conf ${sig.confidence}/5
Trigger: ${sig.trigger || 'Confluence'}
Sweep: ${sig.sweep.name}`;

  console.log(`\n[${new Date().toISOString()}] ${title}`);
  console.log(`  ${msg.replace(/\n/g, '\n  ')}\n`);

  sendNtfy(title, msg);

  // Log to journal (in-memory — for export via /journal endpoint if needed)
  journal.push({
    timestamp: new Date().toISOString(),
    instrument: e.symbol,
    action: sig.action,
    grade: sig.quality,
    entryType: sig.entryType,
    entry: sig.entry, sl: sig.sl,
    tp1: sig.tp1, tp2: sig.tp2, tp3: sig.tp3,
    zoneRisk: sig.zoneRisk, rr1: sig.rr1,
    sweep: sig.sweep.name, trigger: sig.trigger,
    bias: e.bias, session: curSessName(),
    outcome: 'pending'
  });
  if (journal.length > 200) journal.shift();
}

const journal = [];

function curSessName() {
  const h = new Date().getUTCHours();
  if (h >= 0 && h < 8) return 'asian';
  if (h >= 8 && h < 13) return 'london';
  if (h >= 13 && h < 21) return 'ny';
  return null;
}

// ===== WEBSOCKET CONNECTIONS — one per instrument =====
const sockets = {};

function connect(e) {
  const host = e.config.futures ? 'fstream.binance.com' : 'stream.binance.com:9443';
  const url = `wss://${host}/ws/${e.stream}@trade`;

  console.log(`[${e.symbol}] Connecting to ${url}`);
  const ws = new WebSocket(url);
  sockets[e.symbol] = ws;

  ws.on('open', () => {
    console.log(`[${e.symbol}] ✓ Connected (live)`);
  });

  ws.on('message', (data) => {
    try {
      const d = JSON.parse(data.toString());
      if (d.p) {
        const price = parseFloat(d.p);
        handlePrice(e, price, offsetMap[e.symbol] || 0);
        // scanSig runs inside handlePrice via the engine
        // But we need to call it explicitly here with the min grade filter + onSignal callback
        scanSig(e, CONFIG.minGrade, onSignal);
      }
    } catch (ex) {
      // ignore parse errors
    }
  });

  ws.on('error', (err) => {
    console.error(`[${e.symbol}] WS error:`, err.message);
  });

  ws.on('close', () => {
    console.log(`[${e.symbol}] WS closed — reconnecting in ${CONFIG.reconnectDelay}ms`);
    setTimeout(() => connect(e), CONFIG.reconnectDelay);
  });

  // Heartbeat check — if no message in 45s, force reconnect
  if (!e._heartbeatInterval) {
    e._heartbeatInterval = setInterval(() => {
      if (e.lhb && Date.now() - e.lhb > 45000) {
        console.log(`[${e.symbol}] No heartbeat in 45s — forcing reconnect`);
        try { ws.terminate(); } catch (ex) {}
      }
    }, 15000);
  }
}

// ===== HTTP SERVER — minimal, just for health check + journal export =====
const http = require('http');
const server = http.createServer((req, res) => {
  res.setHeader('Access-Control-Allow-Origin', '*');

  // Lightweight ping endpoint — for Render free-tier keep-alive services (cron-job.org, UptimeRobot)
  if (req.url === '/' || req.url === '/ping') {
    res.writeHead(200, { 'Content-Type': 'text/plain' });
    res.end('OK');
    return;
  }

  if (req.url === '/health') {
    const status = engines.map(e => ({
      symbol: e.symbol,
      price: e.price,
      bias: e.bias,
      signal: e.signal ? e.signal.action : 'NONE',
      reason: e.signal ? e.signal.reason : '',
      lastFire: e.lastFireTime ? new Date(e.lastFireTime).toISOString() : null,
      lastGrade: e.lastFireQuality,
      wsState: sockets[e.symbol] ? sockets[e.symbol].readyState : 'closed'
    }));
    res.writeHead(200, { 'Content-Type': 'application/json' });
    res.end(JSON.stringify({ status: 'ok', uptime: process.uptime(), engines: status, journalSize: journal.length }, null, 2));
    return;
  }

  if (req.url === '/journal') {
    res.writeHead(200, { 'Content-Type': 'application/json' });
    res.end(JSON.stringify(journal, null, 2));
    return;
  }

  if (req.url === '/journal.csv') {
    const headers = ['Timestamp', 'Instrument', 'Action', 'Grade', 'EntryType', 'Entry', 'SL', 'TP1', 'TP2', 'TP3', 'ZoneRisk', 'RR1', 'Sweep', 'Trigger', 'Bias', 'Session', 'Outcome'];
    const rows = journal.map(s => [
      s.timestamp, s.instrument, s.action, s.grade, s.entryType || '',
      s.entry != null ? s.entry : '',
      s.sl != null ? s.sl : '',
      s.tp1 != null ? s.tp1 : '',
      s.tp2 != null ? s.tp2 : '',
      s.tp3 != null ? s.tp3 : '',
      s.zoneRisk != null ? s.zoneRisk : '',
      s.rr1 != null ? s.rr1.toFixed(2) : '',
      s.sweep || '', s.trigger || '', s.bias || '', s.session || '', s.outcome || 'pending'
    ]);
    const csv = headers.join(',') + '\n' + rows.map(r => r.map(c => {
      const s = c == null ? '' : String(c);
      if (s.indexOf(',') >= 0 || s.indexOf('"') >= 0) return '"' + s.replace(/"/g, '""') + '"';
      return s;
    }).join(',')).join('\n');
    res.writeHead(200, { 'Content-Type': 'text/csv', 'Content-Disposition': 'attachment; filename="zai-quad-journal.csv"' });
    res.end(csv);
    return;
  }

  // Update offset endpoint: POST /offset/BTC/50.5
  const offsetMatch = req.url.match(/^\/offset\/(BTC|XAU|XAG|EUR)\/(-?[\d.]+)$/);
  if (offsetMatch) {
    const sym = offsetMatch[1];
    const off = parseFloat(offsetMatch[2]);
    offsetMap[sym] = off;
    console.log(`[config] ${sym} offset updated to ${off}`);
    res.writeHead(200, { 'Content-Type': 'application/json' });
    res.end(JSON.stringify({ ok: true, symbol: sym, offset: off }));
    return;
  }

  res.writeHead(404);
  res.end('Not found. Use /health, /journal, /journal.csv, or /offset/SYMBOL/VALUE');
});

const PORT = process.env.PORT || 8080;
const HOST = '0.0.0.0';  // Render (and most cloud hosts) require binding to 0.0.0.0, not localhost
server.listen(PORT, HOST, () => {
  console.log(`\n========================================`);
  console.log(`  ZAI QUAD FRAMEWORK — Cloud Server`);
  console.log(`========================================`);
  console.log(`HTTP:  http://${HOST}:${PORT}/health`);
  console.log(`ntfy:  ${CONFIG.ntfyTopic}`);
  console.log(`Grade: ${CONFIG.minGrade}-only filter`);
  console.log(`Offsets: BTC=${offsetMap.BTC}, XAU=${offsetMap.XAU}, XAG=${offsetMap.XAG}, EUR=${offsetMap.EUR}`);
  console.log(`========================================\n`);

  // Bootstrap all engines, then connect WebSockets
  (async () => {
    for (const e of engines) {
      await bootstrap(e, offsetMap[e.symbol] || 0);
      connect(e);
      // Stagger connections to avoid hitting rate limits
      await new Promise(r => setTimeout(r, 500));
    }
    console.log(`\n[${new Date().toISOString()}] All engines connected. Watching for signals...\n`);
  })();

  // Status log every 5 minutes
  setInterval(() => {
    const status = engines.map(e => `${e.symbol}:${e.signal ? e.signal.action : 'WAIT'}`).join(' ');
    console.log(`[status ${new Date().toISOString().slice(11, 19)}] ${status}`);
  }, 300000);
});

// Graceful shutdown
process.on('SIGTERM', () => {
  console.log('SIGTERM received — closing sockets...');
  Object.values(sockets).forEach(ws => { try { ws.close(); } catch (e) {} });
  server.close(() => process.exit(0));
});
process.on('SIGINT', () => process.exit(0));

// Catch uncaught errors so the process doesn't die
process.on('uncaughtException', (err) => {
  console.error('[uncaughtException]', err.message);
});
process.on('unhandledRejection', (err) => {
  console.error('[unhandledRejection]', err.message);
});
