// smc-engine-cl.js — EXACT PORT of cl-btc-xau.html Engine logic
// This is Engine "CL" — runs alongside Engine "V2" and Engine "SNIPER" without interference.
// Key traits from cl-btc-xau.html:
//   - 5m LTF candles (configurable, default 5m)
//   - 1h HTF bias from swing structure (HH+HL=bull, LH+LL=bear)
//   - 1d daily for PDH/PDL
//   - Asia range (00:00-08:00 UTC) as extra liquidity
//   - 9-point checklist: bias, premium/discount, sweep, CHoCH, POI (FVG/OB), killzone, news, R:R, cost
//   - Grade: A (8+ checks), B (6+), C (else)
//   - Mirror trick for short-side detection
//
// Instruments: BTC + XAU only (per user request — matches cl-btc-xau.html)
// Push prefix: "CL BTC SELL" / "CL XAU BUY" (per user request — exactly matches HTML format)
//
// ISOLATION GUARANTEE:
//   - This file does NOT import or modify smc-engine.js or smc-engine-sniper.js
//   - V2 and Sniper engines are completely independent
//   - CL engine has its own WebSocket (5m kline stream), own state, own push, own journal

'use strict';

// ===== TIME/SESSION HELPERS =====
function fmtPht(d) {
  return new Date(d).toLocaleTimeString('en-GB', {
    timeZone: 'Asia/Manila',
    hour: '2-digit', minute: '2-digit', hour12: false
  }) + ' PHT';
}

function fmtPrice(symbol, n) {
  if (n == null || isNaN(n)) return '—';
  const dec = symbol === 'BTC' ? 1 : symbol === 'XAU' ? 2 : symbol === 'XAG' ? 2 : symbol === 'EUR' ? 5 : 2;
  return n.toLocaleString('en-US', { minimumFractionDigits: dec, maximumFractionDigits: dec });
}

function isFxWeekend() {
  const d = new Date();
  const day = d.getUTCDay(), h = d.getUTCHours() + d.getUTCMinutes() / 60;
  if (day === 6) return true;
  if (day === 5 && h >= 21) return true;
  if (day === 0 && h < 22) return true;
  return false;
}

// Market open check — respects per-engine weekendSuppress config
// BTC/XAU/XAG trade 24/7 (crypto), EUR is forex (closed weekends)
function marketOpen(e) {
  if (!e.config || !e.config.weekendSuppress) return true;
  return !isFxWeekend();
}

function sess(t) {
  const h = new Date(t).getUTCHours();
  return h < 7 ? 'Asia' : h < 12 ? 'London' : 'NY';
}

// ===== DATA LAYER (shared with V2 + Sniper via market-data.js) =====
// CL reads 5m + 1h + 1d from the shared cache. No direct REST calls.
const marketData = require('./market-data');

// ===== ENGINE FACTORY =====
function makeCLEngine(symbol, stream, config) {
  return {
    symbol: symbol,
    stream: stream,
    config: config,
    // Candle data
    ltf: [],          // 5m closed candles
    htf: [],          // 1h closed candles
    daily: null,      // { pdh, pdl } from yesterday
    forming: null,    // current 5m forming candle
    price: null,      // current price
    rawPrice: null,
    // Signal state
    sig: null,
    seen: {},         // dedup map: signalId -> timestamp
    // Analysis cache
    an: null,         // last scan result
    // Bookkeeping
    status: 'connecting',
    lastScan: 0,
    lhb: 0
  };
}

// ===== ENGINE CORE (exact port from cl-btc-xau.html Engine module) =====
const ATR = (C, p = 14) => {
  if (C.length < p + 1) return 0;
  let s = 0;
  for (let i = C.length - p; i < C.length; i++) {
    const c = C[i], pc = C[i - 1].c;
    s += Math.max(c.h - c.l, Math.abs(c.h - pc), Math.abs(c.l - pc));
  }
  return s / p;
};

// confirmed swing points: left side strict, right side non-strict (allows equal highs/lows)
const swings = (C, n) => {
  const H = [], L = [];
  for (let i = n; i < C.length - n; i++) {
    let h = true, l = true;
    for (let k = 1; k <= n; k++) {
      if (!(C[i].h > C[i - k].h && C[i].h >= C[i + k].h)) h = false;
      if (!(C[i].l < C[i - k].l && C[i].l <= C[i + k].l)) l = false;
    }
    if (h) H.push({ i, p: C[i].h });
    if (l) L.push({ i, p: C[i].l });
  }
  return { H, L };
};

const htfBias = (H) => {
  const sw = swings(H, 2);
  if (sw.H.length < 2 || sw.L.length < 2) return 'range';
  const h1 = sw.H[sw.H.length - 1].p, h0 = sw.H[sw.H.length - 2].p;
  const l1 = sw.L[sw.L.length - 1].p, l0 = sw.L[sw.L.length - 2].p;
  if (h1 > h0 && l1 > l0) return 'bull';
  if (h1 < h0 && l1 < l0) return 'bear';
  return 'range';
};

// Asia range (00:00-08:00 UTC) of the current UTC day, only once it has completed
const asiaRange = (C) => {
  if (!C.length) return null;
  const last = C[C.length - 1].t;
  const d0 = Date.UTC(new Date(last).getUTCFullYear(), new Date(last).getUTCMonth(), new Date(last).getUTCDate());
  if (last < d0 + 8 * 3600e3) return null;
  let hi = -Infinity, lo = Infinity, any = false;
  for (const c of C) if (c.t >= d0 && c.t < d0 + 8 * 3600e3) { hi = Math.max(hi, c.h); lo = Math.min(lo, c.l); any = true; }
  return any ? { hi, lo } : null;
};

const mirror = (C) => C.map(c => ({ t: c.t, o: -c.o, h: -c.l, l: -c.h, c: -c.c, v: c.v }));

// Long-side SMC model: sweep of sell-side liquidity -> CHoCH with displacement -> FVG/OB entry
function detectLong(C, atr, ctx) {
  const n = C.length, LOOK = 30, FRESH = 12;
  const sw = swings(C, 3), swI = swings(C, 2);
  const pools = [];
  for (const x of sw.L) if (x.i >= n - 150) pools.push({ p: x.p, i: x.i, label: sess(C[x.i].t) + ' ' + ctx.sfxSw });
  for (const x of ctx.extraSweep) pools.push({ p: x.p, i: -1, label: x.label });
  let best = null, watch = null;
  for (const pool of pools) {
    const from = pool.i >= 0 ? pool.i + 1 : Math.max(0, n - 200);
    let s = -1;
    for (let b = from; b < n; b++) { if (C[b].l < pool.p) { s = b; break; } }
    if (s < 0 || s < n - LOOK) continue;
    let j = -1;
    for (let b = s; b <= Math.min(s + 3, n - 1); b++) { if (C[b].c > pool.p) { j = b; break; } }
    if (j < 0) continue;
    const hPrev = swI.H.filter(x => x.i < s);
    if (!hPrev.length) continue;
    const lvl = hPrev[hPrev.length - 1];
    let k = -1;
    for (let b = j; b < n; b++) { if (C[b].c > lvl.p) { k = b; break; } }
    let ext = Infinity;
    for (let b = s; b < (k < 0 ? n : k + 1); b++) ext = Math.min(ext, C[b].l);
    if (k < 0) {
      if (C[n - 1].c < ext) continue;
      if (!watch || s > watch.s) watch = { dir: ctx.dir, s, t: C[s].t, pool: pool.p, poolLabel: pool.label, ext, level: lvl.p };
      continue;
    }
    let fvg = null;
    for (let m = Math.max(2, k - 1); m <= Math.min(k + 1, n - 1); m++) {
      if (m - 2 >= s && C[m].l > C[m - 2].h) {
        const g = { lo: C[m - 2].h, hi: C[m].l, m };
        if (!fvg || g.hi - g.lo > fvg.hi - fvg.lo) fvg = g;
      }
    }
    const body = C[k].c - C[k].o;
    if (!(fvg || body >= 0.8 * atr)) continue;
    let ob = null;
    for (let b = k - 1; b >= s; b--) { if (C[b].c < C[b].o) { ob = { lo: C[b].l, hi: C[b].o, i: b }; break; } }
    let zone, poi;
    if (fvg) { zone = { lo: fvg.lo, hi: fvg.hi }; poi = 'FVG'; if (ob && ob.hi >= fvg.lo && ob.lo <= fvg.hi) poi = 'OB+FVG'; }
    else if (ob) { zone = { lo: ob.lo, hi: ob.hi }; poi = 'OB'; }
    else continue;
    const bull = ctx.dir === 'long' ? 'Bullish' : 'Bearish';
    const eng = b => b >= 1 && C[b].c > C[b].o && C[b - 1].c < C[b - 1].o && C[b].c >= C[b - 1].o && C[b].o <= C[b - 1].c;
    const sr = C[s].h - C[s].l, sBody = Math.abs(C[s].c - C[s].o), sWick = Math.min(C[s].o, C[s].c) - C[s].l;
    let trigger;
    if (eng(k) || (k - 1 > s && eng(k - 1))) trigger = bull + ' Engulfing';
    else if (body >= 1.2 * atr) trigger = bull + ' Displacement';
    else if (sr > 0 && sWick >= 2 * Math.max(sBody, 0.1 * sr) && C[s].c >= C[s].l + 0.6 * sr) trigger = ctx.dir === 'long' ? 'Bullish Hammer' : 'Bearish Shooting Star';
    else trigger = bull + ' CHoCH + ' + (fvg ? 'FVG' : 'OB');
    const entry = (zone.lo + zone.hi) / 2;
    const zEnd = fvg ? fvg.m : k;
    let mitigated = false;
    for (let b = zEnd + 1; b < n; b++) { if (C[b].l <= entry) { mitigated = true; break; } }
    const stop = ext - 0.15 * atr;
    const risk = entry - stop;
    if (!(risk > 0)) continue;
    let broken = false;
    for (let b = k + 1; b < n; b++) { if (C[b].c < stop) { broken = true; break; } }
    if (broken) continue;
    const cands = [];
    for (const x of sw.H) {
      if (x.p > entry && x.i >= n - 200) {
        let swept = false;
        for (let b = x.i + 1; b < n; b++) { if (C[b].h > x.p) { swept = true; break; } }
        if (!swept) cands.push({ p: x.p, label: sess(C[x.i].t) + ' ' + ctx.sfxTg });
      }
    }
    for (const x of ctx.extraTarget) if (x.p > entry) cands.push(x);
    cands.sort((a, b) => a.p - b.p);
    const t1 = cands.find(c => c.p >= entry + risk);
    const tp1 = t1 ? t1.p : entry + risk, tp1Label = t1 ? t1.label : '1R';
    const base2 = Math.max(entry + 2 * risk, tp1 + 0.5 * risk);
    const t2 = cands.find(c => c.p >= base2);
    const tp2 = t2 ? t2.p : base2, tp2Label = t2 ? t2.label : 'Projection';
    let rh = -Infinity;
    for (let b = Math.max(0, n - 120); b < n; b++) rh = Math.max(rh, C[b].h);
    const disc = entry <= (rh + ext) / 2;
    const hr = new Date(C[k].t).getUTCHours();
    const kz = (hr >= 7 && hr < 10) || (hr >= 12 && hr < 15);
    const rr1 = (tp1 - entry) / risk, rr2 = (tp2 - entry) / risk;
    const checks = [
      { k: 'bias', label: 'HTF (1H) bias aligned', ok: ctx.bias === 1 },
      { k: 'pd', label: ctx.dir === 'long' ? 'Entry in discount' : 'Entry in premium', ok: disc },
      { k: 'sweep', label: 'Liquidity swept: ' + pool.label, ok: true },
      { k: 'choch', label: 'CHoCH + displacement', ok: true },
      { k: 'poi', label: 'Entry at ' + poi, ok: true },
      { k: 'kz', label: 'Kill zone (London / NY open)', ok: kz },
      { k: 'news', label: 'News checked & clear', ok: !!ctx.news },
      { k: 'rr', label: 'Real liquidity target >= 1:2', ok: !!t2 },
      { k: 'cost', label: 'Stop >= 0.5xATR (spread-proof)', ok: risk >= 0.5 * atr },
    ];
    const score = checks.filter(c => c.ok).length;
    const cand = {
      id: `${ctx.sym}|${ctx.dir}|${C[s].t}`, dir: ctx.dir, k, s, fresh: k >= n - FRESH, mitigated,
      entry, stop, tp1, tp1Label, tp2, tp2Label, rr1, rr2, risk, zone, poi,
      sweep: { t: C[s].t, p: pool.p, label: pool.label, ext },
      choch: { t: C[k].t, level: lvl.p }, formedT: C[zEnd].t, formedAt: C[k].t + (C.length > 1 ? C[1].t - C[0].t : 0), trigger, atr, checks, score,
      grade: score >= 8 ? 'A' : score >= 6 ? 'B' : 'C',
    };
    if (!best || cand.k > best.k || (cand.k === best.k && cand.score > best.score)) best = cand;
  }
  return { setup: best, watch };
}

function flipSetup(x) {
  if (!x) return x;
  const f = v => -v, o = { ...x };
  ['entry', 'stop', 'tp1', 'tp2'].forEach(k => o[k] = f(x[k]));
  o.zone = { lo: f(x.zone.hi), hi: f(x.zone.lo) };
  o.sweep = { ...x.sweep, p: f(x.sweep.p), ext: f(x.sweep.ext) };
  o.choch = { ...x.choch, level: f(x.choch.level) };
  return o;
}

function flipWatch(w) { return w && ({ ...w, pool: -w.pool, ext: -w.ext, level: -w.level }); }
function neg(a) { return a.map(x => ({ p: -x.p, label: x.label })); }

function scan(C, H, D, opt) {
  if (C.length < 80 || H.length < 20) return null;
  const atr = ATR(C, 14);
  const bias = htfBias(H);
  const hs = swings(H, 2);
  const asia = asiaRange(C);
  const highs = [], lows = [];
  if (D && D.pdh) highs.push({ p: D.pdh, label: 'PDH' });
  if (D && D.pdl) lows.push({ p: D.pdl, label: 'PDL' });
  if (asia) { highs.push({ p: asia.hi, label: 'Asia H' }); lows.push({ p: asia.lo, label: 'Asia L' }); }
  hs.H.slice(-3).forEach(x => highs.push({ p: x.p, label: '1H H' }));
  hs.L.slice(-3).forEach(x => lows.push({ p: x.p, label: '1H L' }));
  const L = detectLong(C, atr, { dir: 'long', sym: opt.sym, bias: bias === 'bull' ? 1 : 0, news: opt.news,
    extraSweep: lows, extraTarget: highs, sfxSw: 'L', sfxTg: 'H' });
  const M = detectLong(mirror(C), atr, { dir: 'short', sym: opt.sym, bias: bias === 'bear' ? 1 : 0, news: opt.news,
    extraSweep: neg(highs), extraTarget: neg(lows), sfxSw: 'H', sfxTg: 'L' });
  const a = L.setup, b = flipSetup(M.setup);
  let setup = a || b;
  if (a && b) setup = (a.k > b.k || (a.k === b.k && a.score >= b.score)) ? a : b;
  const wl = L.watch, ws = flipWatch(M.watch);
  let watch = wl || ws;
  if (wl && ws) watch = wl.s >= ws.s ? wl : ws;
  return { atr, bias, asia, pdh: D && D.pdh, pdl: D && D.pdl, setup, watch, swings: swings(C, 3) };
}

// ===== BOOTSTRAP (CL — reads from shared market-data cache) =====
// No REST calls — all data comes from market-data.js which fetches once per instrument.
async function bootstrap(e, offset) {
  if (e.ltf.length >= 80 && e.htf.length >= 20 && e.daily) return;
  console.log(`[CL:${e.symbol}] Bootstrap from shared cache...`);

  const cached5m = marketData.getCandles(e.symbol, '5m');
  const cached1h = marketData.getCandles(e.symbol, '1h');
  const cachedDaily = marketData.getDaily(e.symbol);

  if (cached5m.length === 0) {
    console.warn(`[CL:${e.symbol}] No 5m candles in cache yet — will retry`);
    e.status = 'connecting';
    return;
  }

  // 5m LTF — copy from shared cache (data already has offset applied)
  e.ltf = cached5m.slice();

  // 1h HTF
  e.htf = cached1h.slice();

  // 1d daily — PDH/PDL from shared cache
  if (cachedDaily) {
    e.daily = { pdh: cachedDaily.pdh, pdl: cachedDaily.pdl };
  }

  // Set initial price from last 5m candle
  if (e.ltf.length > 0) {
    const last = e.ltf[e.ltf.length - 1];
    e.price = last.c;
    e.rawPrice = last.c - offset;
    e.forming = null; // will be set by WS
  }

  console.log(`[CL:${e.symbol}] Bootstrap OK — ltf=${e.ltf.length}, htf=${e.htf.length}, daily=${e.daily ? 'yes' : 'no'}`);
  e.status = 'live';
}

// Refresh 1h HTF — now reads from shared cache (no REST call)
// market-data.js refreshes 1h at :00:05 every hour.
// This function is kept for backwards compat — it just syncs from cache.
async function refreshHtf(e, offset) {
  const cached1h = marketData.getCandles(e.symbol, '1h');
  if (cached1h.length > 0) {
    // Only update if cache has newer data than what we have
    const ourLast = e.htf[e.htf.length - 1];
    const cacheLast = cached1h[cached1h.length - 1];
    if (!ourLast || (cacheLast && cacheLast.t > ourLast.t)) {
      e.htf = cached1h.slice();
    }
  }
}

// Refresh 1d daily — now reads from shared cache (no REST call)
async function refreshDaily(e, offset) {
  const cachedDaily = marketData.getDaily(e.symbol);
  if (cachedDaily) {
    if (!e.daily || e.daily.pdh !== cachedDaily.pdh || e.daily.pdl !== cachedDaily.pdl) {
      e.daily = { pdh: cachedDaily.pdh, pdl: cachedDaily.pdl };
    }
  }
}

// Sync HTF + daily from shared cache. Called by server.js on every 5m candle close
// before runScan, so CL always has fresh 1h bias + PDH/PDL.
function syncFromCache(e) {
  const cached1h = marketData.getCandles(e.symbol, '1h');
  if (cached1h.length > 0) {
    const ourLast = e.htf[e.htf.length - 1];
    const cacheLast = cached1h[cached1h.length - 1];
    if (!ourLast || (cacheLast && cacheLast.t > ourLast.t)) {
      e.htf = cached1h.slice();
    }
  }
  const cachedDaily = marketData.getDaily(e.symbol);
  if (cachedDaily) {
    if (!e.daily || e.daily.pdh !== cachedDaily.pdh || e.daily.pdl !== cachedDaily.pdl) {
      e.daily = { pdh: cachedDaily.pdh, pdl: cachedDaily.pdl };
    }
  }
}

// ===== KLINE HANDLER (process a 5m kline WS message) =====
// Returns 'closed' if a 5m candle just closed, 'forming' if still forming, 'duplicate' if re-received
// CRITICAL: also runs lifecycle track() on every tick so filled/tp1/tp2/stopped events fire in real-time
// onOutcome callback: (e, outcomeType, sig) => void
function handleKline(e, k, offset, onOutcome) {
  const t = k.t; // candle open time in ms
  const lastClosed = e.ltf[e.ltf.length - 1];
  const kH = parseFloat(k.h) + offset;
  const kL = parseFloat(k.l) + offset;
  const kC = parseFloat(k.c) + offset;

  // Update forming candle
  if (!e.forming || e.forming.t !== t) {
    // New candle started
    e.forming = {
      t: t,
      o: parseFloat(k.o) + offset,
      h: kH,
      l: kL,
      c: kC,
      v: parseFloat(k.v || 0)
    };
  } else {
    // Update existing forming candle
    e.forming.h = Math.max(e.forming.h, kH);
    e.forming.l = Math.min(e.forming.l, kL);
    e.forming.c = kC;
    e.forming.v = parseFloat(k.v || 0);
  }

  // Update current price
  e.price = kC;
  e.rawPrice = parseFloat(k.c);
  e.lhb = Date.now();

  // === LIFECYCLE TRACKING (CRITICAL — port from cl-btc-xau.html onTick/onClosed) ===
  // Run on every tick so fills/stops/TPs fire in real-time, not just on candle close.
  if (e.sig && onOutcome) {
    if (k.x) {
      // Closed bar — track with full h/l and the close price
      const closedBar = { t: t, c: kC };
      track(e, kH, kL, 'closed', closedBar, onOutcome);
    } else {
      // Forming tick — track with forming h/l
      track(e, kH, kL, 'forming', null, onOutcome);
    }
  }

  // If kline is closed (k.x === true), push to ltf[]
  if (k.x) {
    // Don't add duplicate
    if (!lastClosed || lastClosed.t !== t) {
      e.ltf.push(e.forming);
      if (e.ltf.length > 700) e.ltf.shift();
      e.forming = null;
      return 'closed';
    }
    return 'duplicate';
  }
  return 'forming';
}

// ===== RUN SCAN (called on each closed 5m candle) =====
// Returns the setup if a fresh, unmitigated, unseen signal of grade >= minGrade was found
function runScan(e, minGrade, newsClear) {
  if (!marketOpen(e)) return null;
  // CRITICAL FIX: do not create a new signal while one is already being tracked.
  // Matches HTML's `if (!res || s.sig || !marketOpen(sym)) return;` at cl-btc-xau.html line 496.
  // Without this guard, a new 5m candle close could overwrite the active signal mid-lifecycle,
  // losing the fill/tp1/tp2/stopped tracking for the original signal.
  if (e.sig) return null;
  if (e.ltf.length < 80 || e.htf.length < 20 || !e.daily) return null;

  const res = scan(e.ltf, e.htf, e.daily, { sym: e.symbol, news: newsClear });
  e.an = res;
  e.lastScan = Date.now();
  if (!res || !res.setup) return null;

  const c = res.setup;
  const GRADE_RANK = { C: 0, B: 1, A: 2 };
  // Filter: fresh (CHoCH in last 12 candles), not mitigated, not seen, grade >= minGrade
  if (!c.fresh) return null;
  if (c.mitigated) return null;
  if (e.seen[c.id]) return null;
  if (GRADE_RANK[c.grade] < GRADE_RANK[minGrade]) return null;

  // Mark as seen (7-day TTL)
  e.seen[c.id] = Date.now();
  for (const k of Object.keys(e.seen)) {
    if (Date.now() - e.seen[k] > 7 * 864e5) delete e.seen[k];
  }

  // Store as active signal — lifecycle tracking will run via track() on every tick
  e.sig = { ...c, sym: e.symbol, tf: '5m', status: 'pending', age: 0, createdAt: Date.now() };
  return c;
}

// ===== SIGNAL LIFECYCLE TRACKING (port from cl-btc-xau.html lines 486-550) =====
// Without this, the cloud only ever fires the SETUP notification — never filled/tp1/tp2/stopped/etc.
// onOutcome is a callback: (e, outcomeType, sig) => void
// outcomeType values: 'filled', 'tp1', 'tp2', 'be', 'stopped', 'missed', 'invalid', 'expired'
const sigDir = sig => sig.dir === 'long';
const fillHit = (g, h, l) => sigDir(g) ? l <= g.entry : h >= g.entry;
const stopHit = (g, h, l) => sigDir(g) ? l <= g.stop : h >= g.stop;
const tpHit = (g, p, h, l) => sigDir(g) ? h >= p : l <= p;

function track(e, h, l, mode, bar, onOutcome) {
  const g = e.sig; if (!g) return;
  if (g.status === 'pending') {
    const filled = fillHit(g, h, l);
    if (mode === 'closed') {
      g.age++;
      if (!filled) {
        const broke = sigDir(g) ? bar.c < g.stop : bar.c > g.stop;
        if (tpHit(g, g.tp1, h, l)) return finish(e, 'missed', 0, onOutcome);
        if (broke) return finish(e, 'invalid', 0, onOutcome);
        if (g.age >= 40) return finish(e, 'expired', 0, onOutcome);
      }
    }
    if (filled) {
      g.status = 'open'; g.filledAt = Date.now();
      g.fillBarT = mode === 'closed' ? bar.t : (e.forming ? e.forming.t : Date.now());
      if (onOutcome) onOutcome(e, 'filled', g);
      if (stopHit(g, h, l)) return finish(e, 'stopped', -1, onOutcome);
    }
    return;
  }
  if (mode === 'forming') return; // after a fill only ticks and later closed bars are trusted
  if (mode === 'closed' && bar && bar.t <= (g.fillBarT || 0)) return;
  if (g.status === 'open') {
    if (stopHit(g, h, l)) return finish(e, 'stopped', -1, onOutcome);
    if (tpHit(g, g.tp1, h, l)) {
      g.status = 'tp1';
      if (onOutcome) onOutcome(e, 'tp1', g);
      if (tpHit(g, g.tp2, h, l)) return finish(e, 'tp2', 0.5 * g.rr1 + 0.5 * g.rr2, onOutcome);
    }
    return;
  }
  if (g.status === 'tp1') {
    if (fillHit(g, h, l)) return finish(e, 'be', 0.5 * g.rr1, onOutcome);
    if (tpHit(g, g.tp2, h, l)) return finish(e, 'tp2', 0.5 * g.rr1 + 0.5 * g.rr2, onOutcome);
  }
}

function finish(e, result, r, onOutcome) {
  const sig = e.sig; if (!sig) return;
  sig.status = result; sig.r = r; sig.closedAt = Date.now();
  if (onOutcome) onOutcome(e, result, sig);
  e.sig = null; // clear active signal so runScan can create a new one
}

// ===== MODULE EXPORTS =====
module.exports = {
  makeCLEngine,
  bootstrap,
  refreshHtf,
  refreshDaily,
  syncFromCache,
  handleKline,
  runScan,
  scan,
  htfBias,
  swings,
  ATR,
  fmtPht,
  fmtPrice,
  isFxWeekend,
  marketOpen,
  sess
};
