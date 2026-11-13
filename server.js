// server.js — ZAI QUAD FRAMEWORK cloud server
// Runs 24/7 on Render. Connects to Binance WebSockets, evaluates SMC signals, pushes Telegram alerts.
// No dashboard. No browser. Just signals to your phone.

const WebSocket = require('ws');
const { makeEngine, bootstrap, refreshHtf, syncPdhPdl, handlePrice, scanSig, fmtPht, fmtPrice, isFxWeekend, htfBias, detectSwings, detectSweeps, updateZones, updateBias } = require('./smc-engine');

// ===== SHARED DATA LAYER =====
// market-data.js is the SINGLE source of truth for all candle data.
// All 3 engines (V2, Sniper, CL) read from this cache — no engine makes its own REST calls.
// 5m/15m update in real-time via WebSockets (free, unlimited).
// 1h refreshes at :00:05 every hour. 1d refreshes every 2 hours.
// Total: ~160 REST calls/day (vs ~2,496 before — 94% reduction).
const marketData = require('./market-data');

// ===== SNIPER ENGINE (Engine B) — runs in parallel, ISOLATED from V2 =====
// This is the EXACT logic from smc-signals-mobile.html (4/4 bias, 30-min sweep lookback)
// Instruments: BTC + XAU only (per user request — matches SMC Sniper HTML)
// Push prefix: [SNIPER] in Telegram messages
const SniperEngine = require('./smc-engine-sniper');
const sniperEngines = [
  SniperEngine.makeSniperEngine('BTC', 'btcusdt', {
    slBuffer: 50, minRisk: 150, maxRisk: 3000
  }),
  SniperEngine.makeSniperEngine('XAU', 'paxgusdt', {
    slBuffer: 2.5, minRisk: 6, maxRisk: 30
  })
];
// Map for quick lookup by symbol — used to mirror ticks from V2's WebSocket into Sniper engines
const sniperBySymbol = {};
sniperEngines.forEach(e => { sniperBySymbol[e.symbol] = e; });
let sniperBootstrapped = false;

// ===== CL ENGINE (Engine C) — runs in parallel, ISOLATED from V2 + Sniper =====
// This is the EXACT logic from cl-btc-xau.html (5m LTF, 1h HTF, 1d daily, 9-point checklist)
// Instruments: BTC + XAU + EUR (XAG removed — TwelveData free tier doesn't support XAG/USD)
// Push format: "CL BTC SELL" / "CL XAU BUY" / "CL EUR BUY"
// Has its OWN WebSocket connection (5m kline stream) — does NOT share with V2/Sniper
const CleanEngine = require('./smc-engine-cl');
const clEngines = [
  CleanEngine.makeCLEngine('BTC', 'btcusdt', { futures: false, weekendSuppress: false }),
  CleanEngine.makeCLEngine('XAU', 'paxgusdt', { futures: false, weekendSuppress: false }),
  CleanEngine.makeCLEngine('EUR', 'eurusdt', { futures: false, weekendSuppress: true  })   // Forex — closed weekends
];
const clBySymbol = {};
clEngines.forEach(e => { clBySymbol[e.symbol] = e; });
let clBootstrapped = false;
const clSockets = {}; // separate socket map for CL 5m connections

// ===== CONFIGURATION =====
// All configurable via environment variables (set in fly.toml or `fly secrets set`)
const CONFIG = {
  telegramBotToken: process.env.TELEGRAM_BOT_TOKEN || '9149020873:AAGiPgSRSWBThcu0lJERnQQvVEVLNaNR2io',
  // Comma-separated list of chat IDs - all will receive every signal
  // To add a recipient: have them start @ZaiQuadBot, get their ID from @userinfobot,
  // then add it here (comma-separated) or set TELEGRAM_CHAT_IDS env var
  telegramChatIds: (process.env.TELEGRAM_CHAT_IDS || process.env.TELEGRAM_CHAT_ID || '8855264786')
    .split(',')
    .map(id => id.trim())
    .filter(id => id.length > 0),
  // Legacy single chat ID kept for backwards compatibility
  get telegramChatId() { return this.telegramChatIds[0]; },
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
    roundStep: 100, weekendSuppress: false, futures: false    // C1 fix: matches index.html — PAXG trades 24/7 on Binance, no weekend suppression
  }),
  makeEngine('EUR', 'eurusdt', {
    prefix: 'eur', slBuffer: 0.0008, minRisk: 0.0008, maxRisk: 0.0050,
    roundStars: 3, riskWidePct: 0.008, breakoutTh: 0.0005, pullbackTh: 0.002,
    roundStep: 0.01, weekendSuppress: true, futures: false
  })
];

const offsetMap = {
  BTC: CONFIG.btcOffset, XAU: CONFIG.goldOffset,
  XAG: CONFIG.silverOffset, EUR: CONFIG.eurOffset  // XAG kept for offset endpoint compat, but no engine uses it
};

// ===== TELEGRAM PUSH =====
// Unlimited messages, no daily quota, instant delivery
async function sendNtfy(title, message) {
  const fullMessage = `${title}\n\n${message}`;
  // Send to every chat ID in the list (wife, you, anyone else added later)
  const results = await Promise.allSettled(
    CONFIG.telegramChatIds.map(chatId => sendToChat(chatId, fullMessage))
  );
  let ok = 0, fail = 0;
  results.forEach((r, i) => {
    if (r.status === 'fulfilled' && r.value === true) ok++;
    else {
      fail++;
      console.error(`[Telegram] Failed for chat ${CONFIG.telegramChatIds[i]}:`,
        r.status === 'rejected' ? r.reason : 'unknown');
    }
  });
  console.log(`[Telegram] Pushed "${title}" to ${ok}/${CONFIG.telegramChatIds.length} recipients`);
}

async function sendToChat(chatId, text) {
  try {
    const url = `https://api.telegram.org/bot${CONFIG.telegramBotToken}/sendMessage`;
    const controller = new AbortController();
    const timeout = setTimeout(() => controller.abort(), 10000);
    const res = await fetch(url, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ chat_id: chatId, text }),
      signal: controller.signal
    });
    clearTimeout(timeout);
    return res.ok;
  } catch (e) {
    console.error(`[Telegram] Push to ${chatId} failed: ${e.name} - ${e.message}`);
    return false;
  }
}

// ===== SIGNAL HANDLER — fires when a valid signal passes all filters =====
async function onSignal(e, sig) {
  const nameMap = { BTC: 'BTC', XAU: 'XAU', EUR: 'EUR' };
  const instShort = nameMap[e.symbol] || e.symbol;
  const dir = sig.action === 'LONG' ? 'BUY' : 'SELL';
  const emoji = sig.action === 'LONG' ? '📈' : '📉';
  const title = `${emoji} [V2] ZAI ${instShort} ${dir} [${sig.quality}]`;
  const phtStr = fmtPht(Date.now());
  
  // Position sizing (0.5% of $500 default account)
  const account = parseFloat(process.env.ACCOUNT_SIZE || '500');
  const riskPct = parseFloat(process.env.RISK_PCT || '0.5');
  const riskUsd = account * riskPct / 100;
  const contractSize = e.symbol === 'BTC' ? 1 : e.symbol === 'XAU' ? 100 : 100000;  // EUR: 100,000 units per lot (standard forex lot)
  const lotsRaw = riskUsd / (sig.zoneRisk * contractSize);
  // Fallback: if lots rounds to 0 (risk too tight), use minimum 0.01 lots
  const lots = Math.max(0.01, Math.floor(lotsRaw * 100) / 100);
  
  const msg = `Entry: ${fmtPrice(e.symbol, sig.entry)}
SL: ${fmtPrice(e.symbol, sig.sl)} (ATR)
TP1: ${fmtPrice(e.symbol, sig.tp1)} (${sig.rr1.toFixed(1)}R)${sig.tp2 ? ' | TP2: ' + fmtPrice(e.symbol, sig.tp2) + ' (' + sig.rr2.toFixed(1) + 'R)' : ''}
1H: ${sig.htfBias || '—'} | Trigger: ${sig.trigger || '—'}
Sweep: ${sig.sweep.name}${sig.poi ? ' | POI: ' + sig.poi.type : ''} | KZ: ${sig.killZone ? '✓' : '✗'}
Risk: ${fmtPrice(e.symbol, sig.zoneRisk)} | Account: $${riskUsd.toFixed(0)} → ${lots} lots | Time: ${phtStr}`;

  console.log(`\n[${new Date().toISOString()}] ${title}`);
  console.log(`  ${msg.replace(/\n/g, '\n  ')}\n`);

  await sendNtfy(title, msg);

  // Log to journal
  journal.push({
    timestamp: new Date().toISOString(),
    instrument: e.symbol,
    action: sig.action,
    grade: sig.quality,
    score: sig.score,
    entryType: sig.entryType,
    entry: sig.entry, sl: sig.sl,
    tp1: sig.tp1, tp2: sig.tp2, tp3: sig.tp3,
    zoneRisk: sig.zoneRisk, rr1: sig.rr1, rr2: sig.rr2,
    sweep: sig.sweep.name, trigger: sig.trigger,
    poi: sig.poi ? sig.poi.type : null,
    htfBias: sig.htfBias,
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

// ===== SNIPER SIGNAL HANDLER (Engine B) — pushes to Telegram with [SNIPER] prefix =====
// ISOLATED from V2's onSignal. Separate function, separate state, separate prefix.
async function onSniperSignal(e, sig) {
  const instShort = { BTC: 'BTC', XAU: 'XAU', EUR: 'EUR' }[e.symbol] || e.symbol;
  const dir = sig.action === 'LONG' ? 'BUY' : 'SELL';
  const emoji = sig.action === 'LONG' ? '📈' : '📉';
  // [SNIPER] prefix distinguishes these signals from V2's [V2] signals
  const title = `${emoji} [SNIPER] ZAI ${instShort} ${dir} — ${sig.quality} • ${sig.confidence}/5`;
  const phtStr = SniperEngine.fmtPht(Date.now());

  const msg = `Entry: ${SniperEngine.fmtPrice(e.symbol, sig.entry)}
SL: ${SniperEngine.fmtPrice(e.symbol, sig.sl)}
TP1: ${SniperEngine.fmtPrice(e.symbol, sig.tp1)} (${sig.rr1.toFixed(1)}R)${sig.tp2 ? ' | TP2: ' + SniperEngine.fmtPrice(e.symbol, sig.tp2) + ' (' + sig.rr2.toFixed(1) + 'R)' : ''}${sig.tp3 ? ' | TP3: ' + SniperEngine.fmtPrice(e.symbol, sig.tp3) + ' (' + sig.rr3.toFixed(1) + 'R)' : ''}
Trigger: ${sig.trigger || '—'} | Sweep: ${sig.sweep.name}
Bias: ${e.bias} | Type: ${(sig.entryType || 'smc').toUpperCase()} | Time: ${phtStr}
Risk: ${SniperEngine.fmtPrice(e.symbol, sig.zoneRisk)}`;

  console.log(`\n[${new Date().toISOString()}] ${title}`);
  console.log(`  ${msg.replace(/\n/g, '\n  ')}\n`);

  // Push to Telegram (same channel as V2, but with [SNIPER] prefix to distinguish)
  await sendNtfy(title, msg);

  // Log to separate sniper journal (kept separate from V2's journal)
  sniperJournal.push({
    timestamp: new Date().toISOString(),
    instrument: e.symbol, action: sig.action, grade: sig.quality,
    entry: sig.entry, sl: sig.sl,
    tp1: sig.tp1, tp2: sig.tp2, tp3: sig.tp3,
    zoneRisk: sig.zoneRisk, rr1: sig.rr1, rr2: sig.rr2, rr3: sig.rr3,
    sweep: sig.sweep.name, trigger: sig.trigger, entryType: sig.entryType,
    bias: e.bias, session: SniperEngine.curSess(), outcome: 'pending'
  });
  if (sniperJournal.length > 200) sniperJournal.shift();
}
const sniperJournal = [];

// ===== CL SIGNAL HANDLER (Engine C) — pushes to Telegram with "CL BTC SELL" prefix =====
// ISOLATED from V2 and Sniper. Separate function, separate state, separate prefix.
// Format EXACTLY matches cl-btc-xau.html (per user request):
//   "CL BTC SELL\nEntry: ...\nSL: ...\nTP: ... / ...\nTime: ... PHT\nTrigger: ...\nSweep: ..."
async function onCLSignal(e, setup) {
  const instShort = { BTC: 'BTC', XAU: 'XAU', EUR: 'EUR' }[e.symbol] || e.symbol;
  const dir = setup.dir === 'long' ? 'BUY' : 'SELL';
  const emoji = setup.dir === 'long' ? '📈' : '📉';
  // Title starts with "CL BTC SELL" / "CL XAU BUY" — exactly as user requested
  const title = `${emoji} CL ${instShort} ${dir} — ${setup.grade} • ${setup.score}/9`;
  const phtStr = CleanEngine.fmtPht(setup.formedAt);

  const msg = `Entry: ${CleanEngine.fmtPrice(e.symbol, setup.entry)}
SL: ${CleanEngine.fmtPrice(e.symbol, setup.stop)}
TP: ${CleanEngine.fmtPrice(e.symbol, setup.tp1)} / ${CleanEngine.fmtPrice(e.symbol, setup.tp2)}
Time: ${phtStr}
Trigger: ${setup.trigger}
Sweep: ${setup.sweep.label}`;

  console.log(`\n[${new Date().toISOString()}] ${title}`);
  console.log(`  ${msg.replace(/\n/g, '\n  ')}\n`);

  // Push to Telegram (same channel as V2 + Sniper, but with CL prefix to distinguish)
  await sendNtfy(title, msg);

  // Log to separate CL journal
  clJournal.push({
    timestamp: new Date().toISOString(),
    instrument: e.symbol, action: setup.dir === 'long' ? 'LONG' : 'SHORT',
    grade: setup.grade, score: setup.score,
    entry: setup.entry, sl: setup.stop,
    tp1: setup.tp1, tp2: setup.tp2,
    rr1: setup.rr1, rr2: setup.rr2, risk: setup.risk,
    sweep: setup.sweep.label, trigger: setup.trigger, poi: setup.poi,
    htfBias: e.an ? e.an.bias : null, formedAt: setup.formedAt,
    outcome: 'pending'
  });
  if (clJournal.length > 200) clJournal.shift();
}
const clJournal = [];

// ===== CL OUTCOME HANDLER (Engine C lifecycle) — fires when a CL signal transitions state =====
// Port of cl-btc-xau.html EV map (lines 563-574) + notify() function (line 576)
// Fires on: filled, tp1, tp2, be, stopped, missed, invalid, expired
async function onCLOutcome(e, outcomeType, sig) {
  const instShort = { BTC: 'BTC', XAU: 'XAU', EUR: 'EUR' }[e.symbol] || e.symbol;
  const dir = sig.dir === 'long' ? 'BUY' : 'SELL';
  const phtStr = CleanEngine.fmtPht(Date.now());

  // Build title + message based on outcome type (matches HTML EV map)
  let title = '', msg = '';
  switch (outcomeType) {
    case 'filled':
      title = `✅ CL ${instShort} ${dir} entry zone reached`;
      msg = `Price tapped ${CleanEngine.fmtPrice(e.symbol, sig.entry)}. SL ${CleanEngine.fmtPrice(e.symbol, sig.stop)} | TP1 ${CleanEngine.fmtPrice(e.symbol, sig.tp1)} | Time: ${phtStr}`;
      break;
    case 'tp1':
      title = `🎯 CL ${instShort} TP1 hit`;
      msg = `TP1 ${CleanEngine.fmtPrice(e.symbol, sig.tp1)} reached. Consider a partial and moving the stop to break-even. | Time: ${phtStr}`;
      break;
    case 'tp2':
      title = `🏆 CL ${instShort} TP2 hit +${(sig.r || 0).toFixed(2)}R`;
      msg = `Full target ${CleanEngine.fmtPrice(e.symbol, sig.tp2)} reached. | Time: ${phtStr}`;
      break;
    case 'be':
      title = `🤝 CL ${instShort} closed at break-even after TP1 +${(sig.r || 0).toFixed(2)}R`;
      msg = `Price returned to entry after TP1. | Time: ${phtStr}`;
      break;
    case 'stopped':
      title = `❌ CL ${instShort} ${dir} stopped out -1R`;
      msg = `Stop ${CleanEngine.fmtPrice(e.symbol, sig.stop)} hit. Review before the next trade. | Time: ${phtStr}`;
      break;
    case 'missed':
      title = `⏳ CL ${instShort} ${dir} setup missed`;
      msg = `Price reached TP1 without filling the entry. Do not chase. | Time: ${phtStr}`;
      break;
    case 'invalid':
      title = `🚫 CL ${instShort} ${dir} setup invalidated`;
      msg = `Price closed beyond the stop before filling. Idea cancelled. | Time: ${phtStr}`;
      break;
    case 'expired':
      title = `⌛ CL ${instShort} ${dir} setup expired`;
      msg = `Entry not reached within 40 bars. Idea cancelled. | Time: ${phtStr}`;
      break;
    default:
      return; // unknown outcome type — skip
  }

  console.log(`\n[${new Date().toISOString()}] ${title}`);
  console.log(`  ${msg}\n`);

  // Push to Telegram
  await sendNtfy(title, msg);

  // Update journal entry — find the original setup in clJournal and mark outcome
  // (match by instrument + formedAt timestamp)
  for (let i = clJournal.length - 1; i >= 0; i--) {
    if (clJournal[i].instrument === e.symbol && clJournal[i].formedAt === sig.formedAt) {
      clJournal[i].outcome = outcomeType;
      clJournal[i].closedAt = new Date().toISOString();
      clJournal[i].r = sig.r || 0;
      break;
    }
  }
}

// ===== WEBSOCKET CONNECTIONS — use kline stream for candle data =====
const sockets = {};

function connect(e) {
  const host = e.config.futures ? 'fstream.binance.com' : 'stream.binance.com:9443';
  // Use kline stream instead of trade stream — gives us full candle data
  const url = `wss://${host}/ws/${e.stream}@kline_15m`;

  console.log(`[${e.symbol}] Connecting to ${url}`);
  const ws = new WebSocket(url);
  sockets[e.symbol] = ws;

  ws.on('open', () => {
    console.log(`[${e.symbol}] ✓ Connected (live, kline stream)`);
    // Try REST bootstrap only if c15 is empty
    if (e.c15.length === 0) {
      bootstrap(e, offsetMap[e.symbol] || 0);
    }
  });

  ws.on('message', (data) => {
    try {
      const d = JSON.parse(data.toString());
      // Kline stream format: { k: { t, o, h, l, c, v, x: isClosed, ... } }
      if (d.k) {
        const k = d.k;
        const offset = offsetMap[e.symbol] || 0;
        const price = parseFloat(k.c) + offset; // current price from candle close

        // === MIRROR TICK TO SNIPER ENGINE (Engine B) ===
        // Feed the same tick into the corresponding Sniper engine (if exists)
        // CRITICAL FIX: pass onSniperSignal as callback so scanSig can fire the Telegram push directly.
        // Previously this called handlePrice() then scanSig() again — but handlePrice already calls
        // scanSig internally, and the second call would always return undefined (because e.actSig
        // was already set by the first call). This made the Sniper engine completely silent.
        const sniperE = sniperBySymbol[e.symbol];
        if (sniperE) {
          try {
            SniperEngine.handlePrice(sniperE, parseFloat(k.c), offset, onSniperSignal);
          } catch (sniperErr) {
            console.error(`[SNIPER:${e.symbol}] scan error:`, sniperErr.message);
          }
        }
        // === END SNIPER MIRROR ===
        
        // === UPDATE SHARED DATA LAYER ===
        // Push this 15m kline to the shared cache so all engines see the same data.
        // This is a no-cost operation (in-memory) — no REST API call.
        marketData.updateFromWebSocket(e.symbol, '15m', k, offset);
        
        // Sync V2's PDH/PDL from shared cache (in case market-data refreshed 1d)
        syncPdhPdl(e);
        
        // === V2 ENGINE: Update candle + run all detection on EVERY tick (matches index.html behavior) ===
        // BUG FIX (2026-11-09): Previously only called detectSweeps/updateZones/updateBias when a NEW
        // candle started. This caused the cloud to miss sweeps that formed mid-candle, while index.html
        // (which calls all 5 functions on every tick) caught them. Now we mirror index.html's handlePrice
        // behavior exactly: updateCB + updateSess + updateZones + detectSweeps + updateBias + scanSig
        // on every WebSocket message.
        const candleKey = Math.floor(k.t / 60000 / 15);
        const existing = e.c15[e.c15.length - 1];
        
        if (!existing || existing.key !== candleKey) {
          // New 15m candle — add it
          e.c15.push({
            key: candleKey,
            o: parseFloat(k.o) + offset,
            h: parseFloat(k.h) + offset,
            l: parseFloat(k.l) + offset,
            c: parseFloat(k.c) + offset,
            t: new Date(k.t)
          });
          if (e.c15.length > 100) e.c15.shift();
        } else {
          // Update existing candle — high/low/close
          existing.h = Math.max(existing.h, parseFloat(k.h) + offset);
          existing.l = Math.min(existing.l, parseFloat(k.l) + offset);
          existing.c = parseFloat(k.c) + offset;
        }
        
        // Always set price
        e.prev = e.price; e.price = price;
        e.ph.push({ t: new Date(), p: price });
        if (e.ph.length > 500) e.ph.shift();
        
        // Run ALL detection functions on every tick (matches index.html handlePrice exactly)
        // This is critical — detectSweeps must run on every tick to catch mid-candle sweeps
        if (e.c15.length >= 5) {
          try {
            detectSwings(e);
            updateZones(e);
            detectSweeps(e);
            updateBias(e);
            scanSig(e, CONFIG.minGrade, onSignal);
          } catch (err) { console.error(`[${e.symbol}] scanSig error:`, err.message); }
        } else {
          e.signal = { action: 'WAIT', reason: `Building candles (${e.c15.length}/5)` };
        }
      } else if (d.p) {
        // Fallback: trade stream format (if kline not available)
        const price = parseFloat(d.p);
        // M8 fix: pass minGrade + onSignal so handlePrice can fire Telegram pushes
        handlePrice(e, price, offsetMap[e.symbol] || 0, CONFIG.minGrade, onSignal);
        // Mirror to Sniper engine too (CRITICAL FIX: pass callback, no duplicate scanSig)
        const sniperE = sniperBySymbol[e.symbol];
        if (sniperE) {
          try {
            SniperEngine.handlePrice(sniperE, price, offsetMap[e.symbol] || 0, onSniperSignal);
          } catch (sniperErr) {
            console.error(`[SNIPER:${e.symbol}] trade-stream scan error:`, sniperErr.message);
          }
        }
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

  // Heartbeat check
  if (!e._heartbeatInterval) {
    e._heartbeatInterval = setInterval(() => {
      if (e.lhb && Date.now() - e.lhb > 45000) {
        console.log(`[${e.symbol}] No heartbeat in 45s — forcing reconnect`);
        try { ws.terminate(); } catch (ex) {}
      }
    }, 15000);
  }
}

// ===== CL WEBSOCKET CONNECTION (5m kline stream — separate from V2's 15m) =====
// CL engine needs 5m candles, so it has its own WebSocket connection per instrument.
// ISOLATED from V2's connect() function — separate sockets, separate message handling.
function connectCL(e) {
  // XAG (Silver) is on Binance Futures — use fstream.binance.com
  // BTC/XAU/EUR are on Binance Spot — use stream.binance.com:9443
  const host = e.config && e.config.futures ? 'fstream.binance.com' : 'stream.binance.com:9443';
  const url = `wss://${host}/ws/${e.stream}@kline_5m`;
  console.log(`[CL:${e.symbol}] Connecting to ${url}${e.config && e.config.futures ? ' (futures)' : ' (spot)'}`);
  const ws = new WebSocket(url);
  clSockets[e.symbol] = ws;

  ws.on('open', () => {
    console.log(`[CL:${e.symbol}] ✓ Connected (live, 5m kline stream${e.config && e.config.futures ? ', futures' : ', spot'})`);
    // If not bootstrapped yet, try now
    if (e.ltf.length === 0) {
      CleanEngine.bootstrap(e, offsetMap[e.symbol] || 0);
    }
  });

  ws.on('message', (data) => {
    try {
      const d = JSON.parse(data.toString());
      if (d.k) {
        const offset = offsetMap[e.symbol] || 0;
        // === UPDATE SHARED DATA LAYER ===
        // Push this 5m kline to the shared cache so all engines see the same data.
        marketData.updateFromWebSocket(e.symbol, '5m', d.k, offset);
        // Sync CL's 1h HTF + 1d PDH/PDL from shared cache (in case market-data refreshed)
        CleanEngine.syncFromCache(e);
        // CRITICAL: pass onCLOutcome callback so lifecycle events (filled/tp1/tp2/stopped/etc.) fire
        // Without this, only the SETUP notification would ever fire — no trade management alerts.
        const status = CleanEngine.handleKline(e, d.k, offset, onCLOutcome);
        // Only run scan on candle close (too expensive to run on every tick)
        if (status === 'closed') {
          try {
            const minGrade = process.env.CL_MIN_GRADE || 'B';
            const newsClear = process.env.CL_NEWS_CLEAR === 'true';
            const setup = CleanEngine.runScan(e, minGrade, newsClear);
            if (setup) {
              onCLSignal(e, setup);
            }
          } catch (clErr) {
            console.error(`[CL:${e.symbol}] scan error:`, clErr.message);
          }
        }
      }
    } catch (ex) {
      // ignore parse errors
    }
  });

  ws.on('error', (err) => {
    console.error(`[CL:${e.symbol}] WS error:`, err.message);
  });

  ws.on('close', () => {
    console.log(`[CL:${e.symbol}] WS closed — reconnecting in ${CONFIG.reconnectDelay}ms`);
    setTimeout(() => connectCL(e), CONFIG.reconnectDelay);
  });

  // Heartbeat check for CL
  if (!e._clHeartbeat) {
    e._clHeartbeat = setInterval(() => {
      if (e.lhb && Date.now() - e.lhb > 45000) {
        console.log(`[CL:${e.symbol}] No heartbeat in 45s — forcing reconnect`);
        try { ws.terminate(); } catch (ex) {}
      }
    }, 15000);
  }
}

// ===== HTTP SERVER — minimal, just for health check + journal export =====
const http = require('http');
const server = http.createServer(async (req, res) => {
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
    const sniperStatus = sniperEngines.map(e => ({
      symbol: e.symbol + '-SNIPER',
      price: e.price,
      bias: e.bias,
      signal: e.signal ? e.signal.action : 'NONE',
      reason: e.signal ? e.signal.reason : '',
      swept: e.swept.length
    }));
    const clStatus = clEngines.map(e => ({
      symbol: e.symbol + '-CL',
      price: e.price,
      bias: e.an ? e.an.bias : '—',
      ltfCount: e.ltf.length,
      htfCount: e.htf.length,
      hasDaily: !!e.daily,
      hasSetup: !!(e.an && e.an.setup),
      setupGrade: (e.an && e.an.setup) ? e.an.setup.grade : null,
      setupDir: (e.an && e.an.setup) ? e.an.setup.dir : null,
      seenCount: Object.keys(e.seen).length,
      wsState: clSockets[e.symbol] ? clSockets[e.symbol].readyState : 'closed',
      status: e.status
    }));
    res.writeHead(200, { 'Content-Type': 'application/json' });
    res.end(JSON.stringify({
      status: 'ok',
      uptime: process.uptime(),
      engines: status,
      sniperEngines: sniperStatus,
      clEngines: clStatus,
      journalSize: journal.length,
      sniperJournalSize: sniperJournal.length,
      clJournalSize: clJournal.length,
      marketData: {
        callCount: marketData.getCallCount(),
        cacheStats: marketData.getStats()
      }
    }, null, 2));
    return;
  }

  if (req.url === '/market-data') {
    res.writeHead(200, { 'Content-Type': 'application/json' });
    res.end(JSON.stringify({
      callCount: marketData.getCallCount(),
      cacheStats: marketData.getStats()
    }, null, 2));
    return;
  }

  if (req.url === '/debug-cl') {
    const status = clEngines.map(e => ({
      symbol: e.symbol,
      price: e.price,
      rawPrice: e.rawPrice,
      ltfCount: e.ltf.length,
      htfCount: e.htf.length,
      daily: e.daily,
      bias: e.an ? e.an.bias : '—',
      asia: e.an ? e.an.asia : null,
      atr: e.an ? e.an.atr : null,
      setup: e.an && e.an.setup ? {
        dir: e.an.setup.dir,
        grade: e.an.setup.grade,
        score: e.an.setup.score,
        entry: e.an.setup.entry,
        stop: e.an.setup.stop,
        tp1: e.an.setup.tp1,
        tp2: e.an.setup.tp2,
        trigger: e.an.setup.trigger,
        sweep: e.an.setup.sweep.label,
        poi: e.an.setup.poi,
        fresh: e.an.setup.fresh,
        mitigated: e.an.setup.mitigated,
        checks: e.an.setup.checks
      } : null,
      watch: e.an && e.an.watch ? {
        dir: e.an.watch.dir,
        poolLabel: e.an.watch.poolLabel,
        level: e.an.watch.level
      } : null,
      seenCount: Object.keys(e.seen).length,
      lastScan: e.lastScan ? new Date(e.lastScan).toISOString() : null
    }));
    res.writeHead(200, { 'Content-Type': 'application/json' });
    res.end(JSON.stringify({ status: 'ok', clEngines: status, journal: clJournal.slice(-10) }, null, 2));
    return;
  }

  if (req.url === '/debug-sniper') {
    const status = sniperEngines.map(e => ({
      symbol: e.symbol,
      price: e.price,
      rawPrice: e.rawPrice,
      bias: e.bias,
      biasDetail: e.bd,
      signal: e.signal,
      swept: e.swept.map(s => ({ level: s.level, name: s.name, time: s.time })),
      zones: e.zones.slice(0, 10),
      c15Count: e.c15.length,
      swH: e.swH, swL: e.swL
    }));
    res.writeHead(200, { 'Content-Type': 'application/json' });
    res.end(JSON.stringify({ status: 'ok', sniperEngines: status, journal: sniperJournal.slice(-10) }, null, 2));
    return;
  }

  if (req.url === '/debug') {
    const status = engines.map(e => ({
      symbol: e.symbol,
      price: e.price,
      bias: e.bias,
      c15len: e.c15 ? e.c15.length : 0,
      htfLen: e.htf ? e.htf.length : 0,
      hasDaily: !!e.daily,
      atr: e.atr || 0,
      pdh: e.pdh,
      pdl: e.pdl,
      swH: e.swH ? e.swH.length : 0,
      swL: e.swL ? e.swL.length : 0,
      zones: e.zones ? e.zones.length : 0,
      swept: e.swept ? e.swept.length : 0,
      lastBootstrapRetry: e._lastBootstrapRetry ? new Date(e._lastBootstrapRetry).toISOString() : null,
      signal: e.signal ? e.signal.action + ': ' + e.signal.reason : 'NONE'
    }));
    res.writeHead(200, { 'Content-Type': 'application/json' });
    res.end(JSON.stringify({ uptime: process.uptime(), engines: status }, null, 2));
    return;
  }

  // Test endpoint — shows what Binance API returns from this server
  if (req.url === '/test-fetch') {
    try {
      const r = await fetch('https://api.binance.com/api/v3/klines?symbol=BTCUSDT&interval=15m&limit=3');
      const text = await r.text();
      res.writeHead(200, { 'Content-Type': 'application/json' });
      res.end(JSON.stringify({ 
        status: r.status, 
        ok: r.ok,
        bodyPreview: text.slice(0, 500),
        isArray: text.startsWith('[')
      }, null, 2));
    } catch (err) {
      res.writeHead(200, { 'Content-Type': 'application/json' });
      res.end(JSON.stringify({ error: err.message, stack: err.stack }, null, 2));
    }
    return;
  }

  // Test endpoint 2 — try the fallback endpoint
  if (req.url === '/test-fetch2') {
    try {
      const r = await fetch('https://data-api.binance.vision/api/v3/klines?symbol=BTCUSDT&interval=15m&limit=3');
      const text = await r.text();
      res.writeHead(200, { 'Content-Type': 'application/json' });
      res.end(JSON.stringify({ 
        status: r.status, 
        ok: r.ok,
        bodyPreview: text.slice(0, 500),
        isArray: text.startsWith('[')
      }, null, 2));
    } catch (err) {
      res.writeHead(200, { 'Content-Type': 'application/json' });
      res.end(JSON.stringify({ error: err.message }, null, 2));
    }
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
  const offsetMatch = req.url.match(/^\/offset\/(BTC|XAU|EUR)\/(-?[\d.]+)$/);
  if (offsetMatch) {
    const sym = offsetMatch[1];
    const off = parseFloat(offsetMatch[2]);
    offsetMap[sym] = off;
    console.log(`[config] ${sym} offset updated to ${off}`);
    res.writeHead(200, { 'Content-Type': 'application/json' });
    res.end(JSON.stringify({ ok: true, symbol: sym, offset: off }));
    return;
  }

  // Test Telegram endpoint — sends a test notification and shows the result
  if (req.url === '/test-ntfy' || req.url === '/test-telegram') {
    try {
      const testText = '🧪 ZAI QUAD FRAMEWORK\n\nTelegram notifications are working!\n\nYou will receive trading signals here.';
      const results = await Promise.allSettled(
        CONFIG.telegramChatIds.map(id => sendToChat(id, testText))
      );
      const summary = CONFIG.telegramChatIds.map((id, i) => ({
        chatId: id,
        ok: results[i].status === 'fulfilled' && results[i].value === true
      }));
      const allOk = summary.every(s => s.ok);
      res.writeHead(200, { 'Content-Type': 'application/json' });
      res.end(JSON.stringify({
        ok: allOk,
        recipients: summary,
        totalRecipients: CONFIG.telegramChatIds.length
      }, null, 2));
    } catch (err) {
      res.writeHead(200, { 'Content-Type': 'application/json' });
      res.end(JSON.stringify({ 
        ok: false, 
        error: err.message, 
        name: err.name,
        chatId: CONFIG.telegramChatId
      }, null, 2));
    }
    return;
  }

  res.writeHead(404);
  res.end('Not found. Use /health, /debug, /journal, /test-telegram, or /offset/SYMBOL/VALUE');
});

const PORT = process.env.PORT || 8080;
const HOST = '0.0.0.0';  // Render (and most cloud hosts) require binding to 0.0.0.0, not localhost
server.listen(PORT, HOST, () => {
  console.log(`\n========================================`);
  console.log(`  ZAI QUAD FRAMEWORK — Cloud Server`);
  console.log(`========================================`);
  console.log(`HTTP:  http://${HOST}:${PORT}/health`);
  console.log(`Telegram: ${CONFIG.telegramChatIds.length} recipient(s): ${CONFIG.telegramChatIds.join(', ')}`);
  console.log(`Grade: ${CONFIG.minGrade}-only filter (Engine V2)`);
  console.log(`Engine V2: BTC + XAU + EUR (15m, 3/4 bias, 60-min sweep)`);
  console.log(`Engine SNIPER: BTC + XAU only (15m, 4/4 bias, 30-min sweep) — isolated`);
  console.log(`Engine CL: BTC + XAU + EUR (5m, 9-point checklist, FVG/OB, 1h HTF bias) — isolated, own WS`);
  console.log(`Offsets: BTC=${offsetMap.BTC}, XAU=${offsetMap.XAU}, EUR=${offsetMap.EUR}`);
  console.log(`========================================\n`);

  // === SHARED DATA LAYER BOOTSTRAP ===
  // market-data.js fetches 5m + 15m + 1h + 1d for ALL 3 instruments (BTC, XAU, EUR) in one pass.
  // Total: 16 REST calls at startup, then 0 for 5m/15m (WebSocket), 96/day for 1h, 48/day for 1d.
  // All 3 engines (V2, Sniper, CL) read from this shared cache.
  (async () => {
    console.log(`\n[${new Date().toISOString()}] Bootstrapping shared market-data layer...`);
    await marketData.bootstrapAll(offsetMap);
    console.log(`[${new Date().toISOString()}] Shared data layer ready. Starting scheduled refresh jobs...`);
    marketData.startScheduledRefresh(offsetMap);
    console.log(`[${new Date().toISOString()}] Scheduled refresh jobs started.\n`);

    // === BOOTSTRAP V2 ENGINES (read from shared cache) ===
    for (const e of engines) {
      await bootstrap(e, offsetMap[e.symbol] || 0);
      connect(e);  // V2 connects its own 15m WebSocket
      await new Promise(r => setTimeout(r, 2000));  // 2s gap between WS connections
    }
    console.log(`[${new Date().toISOString()}] All V2 engines connected. Watching for signals...`);

    // === BOOTSTRAP SNIPER ENGINES (read from shared cache — no extra REST calls) ===
    if (!sniperBootstrapped) {
      sniperBootstrapped = true;
      for (const se of sniperEngines) {
        await SniperEngine.bootstrap(se, offsetMap[se.symbol] || 0);
        await new Promise(r => setTimeout(r, 1000));
      }
      console.log(`[${new Date().toISOString()}] SNIPER engines bootstrapped (BTC + XAU) from shared cache.`);
      console.log(`[SNIPER] Now receiving mirrored ticks from V2 WebSocket connections.`);
      console.log(`[SNIPER] Signals will push to Telegram with [SNIPER] prefix.\n`);
    }

    // === BOOTSTRAP CL ENGINES (read from shared cache — no extra REST calls) ===
    if (!clBootstrapped) {
      clBootstrapped = true;
      for (const ce of clEngines) {
        await CleanEngine.bootstrap(ce, offsetMap[ce.symbol] || 0);
        await new Promise(r => setTimeout(r, 1000));
      }
      console.log(`[${new Date().toISOString()}] CL engines bootstrapped (BTC + XAU + EUR) from shared cache.`);
      // Now connect CL's own 5m WebSocket for each instrument
      clEngines.forEach(ce => connectCL(ce));
      console.log(`[CL] 5m WebSocket connections established.`);
      console.log(`[CL] Signals will push to Telegram with "CL BTC SELL" / "CL XAU BUY" / "CL EUR BUY" prefix.\n`);
    }
  })();

  // Periodic maintenance every 5 min: retry bootstrap if engines have low candle count
  // No more REST calls here — market-data.js handles all refreshes via scheduled jobs.
  setInterval(() => {
    engines.forEach(e => {
      if (e.c15.length < 50) {
        const now = Date.now();
        if (!e._lastBootstrapRetry || now - e._lastBootstrapRetry > 300000) {
          e._lastBootstrapRetry = now;
          console.log(`[V2:${e.symbol}] Periodic bootstrap retry (c15=${e.c15.length})`);
          bootstrap(e, offsetMap[e.symbol] || 0);
        }
      }
    });
    sniperEngines.forEach(se => {
      if (se.c15.length < 50) {
        const now = Date.now();
        if (!se._lastBootstrapRetry || now - se._lastBootstrapRetry > 300000) {
          se._lastBootstrapRetry = now;
          console.log(`[SNIPER:${se.symbol}] Periodic bootstrap retry (c15=${se.c15.length})`);
          SniperEngine.bootstrap(se, offsetMap[se.symbol] || 0);
        }
      }
    });
    clEngines.forEach(ce => {
      if (ce.ltf.length < 80 || ce.htf.length < 20 || !ce.daily) {
        const now = Date.now();
        if (!ce._lastBootstrapRetry || now - ce._lastBootstrapRetry > 300000) {
          ce._lastBootstrapRetry = now;
          console.log(`[CL:${ce.symbol}] Periodic bootstrap retry (ltf=${ce.ltf.length}, htf=${ce.htf.length})`);
          CleanEngine.bootstrap(ce, offsetMap[ce.symbol] || 0);
        }
      }
    });
    // Log market-data call count for monitoring
    const calls = marketData.getCallCount();
    console.log(`[MarketData] Call count: ${calls.total} total (${JSON.stringify(calls.byType)})`);
  }, 300000);

  // Status log every 5 minutes — all three engines + market-data stats
  setInterval(() => {
    const v2status = engines.map(e => `${e.symbol}:c15=${e.c15.length}/${e.bias}`).join(' ');
    const snipStatus = sniperEngines.map(e => `${e.symbol}:c15=${e.c15.length}/${e.bias}`).join(' ');
    const clStatus = clEngines.map(e => `${e.symbol}:ltf=${e.ltf.length}/htf=${e.htf.length}/${e.an ? e.an.bias : '—'}`).join(' ');
    console.log(`[status ${new Date().toISOString().slice(11, 19)}] V2: ${v2status} | SNIPER: ${snipStatus} | CL: ${clStatus}`);
  }, 300000);
});

// Graceful shutdown
process.on('SIGTERM', () => {
  console.log('SIGTERM received — closing sockets...');
  marketData.stopScheduledRefresh();
  Object.values(sockets).forEach(ws => { try { ws.close(); } catch (e) {} });
  Object.values(clSockets).forEach(ws => { try { ws.close(); } catch (e) {} });
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
