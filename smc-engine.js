// smc-engine.js v3 — Merged SMC engine
// Combines Claude's SMC logic (multi-timeframe, CHoCH, OB, ATR stops, 9-point scoring)
// with our cloud infrastructure (4 instruments, cooldown, TP fixes, weekend suppress).
//
// Key improvements over v2:
// - 1H HTF bias (replaces 4-candle momentum) — stable directional read
// - Full SMC sequence: Sweep → CHoCH → POI (FVG/OB) — textbook ICT
// - ATR-based stops (replaces fixed slBuffer) — adapts to volatility
// - Order Block detection (in addition to FVG)
// - 9-point checklist scoring (transparent quality)
// - Premium/Discount, Kill Zone, Asia range as scoring factors
// - Breakout Continuation (our addition, catches trend moves)
//
// Ensures trades fire: core requirements are sweep + CHoCH + valid SL/TP.
// Everything else is scoring (not hard filter).

function makeEngine(symbol, stream, config) {
  return {
    symbol, stream, config,
    prefix: config.prefix,
    price: null, prev: null,
    ph: [], cb: [], c15: [],
    htf: [],
    daily: null,
    asiaRange: null,
    ses: { asian: { h: null, l: null }, london: { h: null, l: null }, ny: { h: null, l: null } },
    pdh: null, pdl: null,
    zones: [], bias: 'neutral', bd: null,
    swH: [], swL: [], swept: [], consumed: [],
    signal: null, actSig: null,
    lockUntil: 0, lastCK: null, lastSK: null,
    sigFormedAt: null, lastFireTime: 0, lastFireQuality: null,
    lhb: 0, atr: 0
  };
}

function gH() { return new Date().getUTCHours(); }

function curSess() {
  var h = gH();
  if (h >= 0 && h < 8) return 'asian';
  if (h >= 8 && h < 13) return 'london';
  if (h >= 13 && h < 21) return 'ny';
  return null;
}

function isSessWin() {
  var h = gH();
  return (h >= 0 && h < 8) || (h >= 13 && h < 21);
}

function isFxWeekend() {
  var d = new Date();
  var day = d.getUTCDay(), h = d.getUTCHours();
  if (day === 5 && h >= 14) return true;
  if (day === 6 && h < 16) return true;
  return false;
}

function isKillZone() {
  var h = gH();
  return (h >= 7 && h < 10) || (h >= 12 && h < 15);
}

function fmtPht(d) {
  return new Date(d).toLocaleTimeString('en-US', {
    timeZone: 'Asia/Manila',
    hour: '2-digit', minute: '2-digit', second: '2-digit', hour12: true
  });
}

function fmtPrice(symbol, n) {
  if (n == null) return '—';
  if (symbol === 'BTC') return '$' + n.toLocaleString('en-US', { maximumFractionDigits: 0 });
  if (symbol === 'XAU') return '$' + n.toFixed(1);
  if (symbol === 'XAG') return '$' + n.toFixed(2);
  if (symbol === 'EUR') return '$' + n.toFixed(5);
  return '$' + n;
}

// ===== ATR calculation (14-period on 15m candles) =====
function calcAtr(e) {
  const c = e.c15;
  const p = 14;
  if (c.length < p + 1) return 0;
  let s = 0;
  for (let i = c.length - p; i < c.length; i++) {
    const cur = c[i], prev = c[i - 1].c;
    s += Math.max(cur.h - cur.l, Math.abs(cur.h - prev), Math.abs(cur.l - prev));
  }
  return s / p;
}

// ===== 1H HTF Bias (from swings) =====
function htfBias(e) {
  const H = e.htf;
  if (H.length < 10) return 'range';
  const swH = [], swL = [];
  for (let i = 2; i < H.length - 2; i++) {
    if (H[i].h > H[i-1].h && H[i].h > H[i-2].h && H[i].h > H[i+1].h && H[i].h > H[i+2].h) swH.push(H[i].h);
    if (H[i].l < H[i-1].l && H[i].l < H[i-2].l && H[i].l < H[i+1].l && H[i].l < H[i+2].l) swL.push(H[i].l);
  }
  if (swH.length < 2 || swL.length < 2) return 'range';
  const hh = swH[swH.length-1] > swH[swH.length-2];
  const hl = swL[swL.length-1] > swL[swL.length-2];
  const lh = swH[swH.length-1] < swH[swH.length-2];
  const ll = swL[swL.length-1] < swL[swL.length-2];
  if (hh && hl) return 'bull';
  if (lh && ll) return 'bear';
  return 'range';
}

// ===== Asia Range (00:00-08:00 UTC of current day) =====
function calcAsiaRange(e) {
  const c = e.c15;
  if (!c.length) return null;
  const last = c[c.length - 1].t;
  const d0 = Date.UTC(new Date(last).getUTCFullYear(), new Date(last).getUTCMonth(), new Date(last).getUTCDate());
  if (last < d0 + 8 * 3600000) return null;
  let hi = -Infinity, lo = Infinity, any = false;
  for (const candle of c) {
    if (candle.t >= d0 && candle.t < d0 + 8 * 3600000) {
      hi = Math.max(hi, candle.h);
      lo = Math.min(lo, candle.l);
      any = true;
    }
  }
  return any ? { hi, lo } : null;
}

// ===== Bootstrap: fetch 15m + 1H + daily candles (with fallback endpoints) =====
// Global ban tracking — if Binance returns 418, stop retrying for 30 minutes
let _binanceBannedUntil = 0;
function isBinanceBanned() { return Date.now() < _binanceBannedUntil; }

async function fetchKlines(base, sym, interval, limit) {
  const r = await fetch(base + '?symbol=' + sym + '&interval=' + interval + '&limit=' + limit);
  if (r.status === 418 || r.status === 429) {
    _binanceBannedUntil = Date.now() + 1800000; // 30 min cooldown
    throw new Error('Binance rate-limited (HTTP ' + r.status + ') — cooldown 30min');
  }
  if (!r.ok) throw new Error('HTTP ' + r.status);
  const data = await r.json();
  if (!Array.isArray(data)) throw new Error('Non-array response: ' + JSON.stringify(data).slice(0, 100));
  return data;
}

async function bootstrap(e, offset) {
  if (e.c15.length >= 50) return;
  if (isBinanceBanned()) { console.log(`[${e.symbol}] Bootstrap skipped — Binance banned for ${Math.ceil((_binanceBannedUntil - Date.now())/60000)}min`); return; }
  // Try multiple endpoints — Binance sometimes rate-limits or blocks cloud IPs
  const endpoints = e.config.futures
    ? ['https://fapi.binance.com/fapi/v1/klines']
    : ['https://api.binance.com/api/v3/klines', 'https://data-api.binance.vision/api/v3/klines'];
  const sym = e.stream.toUpperCase();

  // Helper: try each endpoint until one works
  async function tryFetch(interval, limit) {
    for (const ep of endpoints) {
      try {
        return await fetchKlines(ep, sym, interval, limit);
      } catch (err) {
        console.warn(`[${e.symbol}] ${interval} fetch from ${ep.split('//')[1].split('/')[0]} failed: ${err.message}`);
      }
    }
    return null;
  }

  // 15m candles (most important)
  try {
    const d15 = await tryFetch('15m', 100);
    if (d15 && d15.length > 0) {
      e.c15 = d15.map(k => ({
        key: Math.floor(k[0]/60000/15),
        o: parseFloat(k[1])+offset, h: parseFloat(k[2])+offset,
        l: parseFloat(k[3])+offset, c: parseFloat(k[4])+offset,
        t: new Date(k[0])
      }));
      console.log(`[${e.symbol}] Loaded ${e.c15.length} 15m candles`);
    } else {
      console.error(`[${e.symbol}] 15m fetch failed from all endpoints`);
    }
  } catch (err) {
    console.warn(`[${e.symbol}] 15m bootstrap error:`, err.message);
  }

  // 1H candles
  try {
    const d1h = await tryFetch('1h', 100);
    if (d1h && d1h.length > 0) {
      e.htf = d1h.map(k => ({
        o: parseFloat(k[1])+offset, h: parseFloat(k[2])+offset,
        l: parseFloat(k[3])+offset, c: parseFloat(k[4])+offset,
        t: new Date(k[0])
      }));
      console.log(`[${e.symbol}] Loaded ${e.htf.length} 1H candles`);
    }
  } catch (err) {
    console.warn(`[${e.symbol}] 1H bootstrap failed:`, err.message);
  }

  // Daily candles
  try {
    const d1d = await tryFetch('1d', 5);
    if (d1d && d1d.length >= 2) {
      const prev = d1d[d1d.length - 2];
      e.daily = { pdh: parseFloat(prev[2])+offset, pdl: parseFloat(prev[3])+offset };
      e.pdh = e.daily.pdh;
      e.pdl = e.daily.pdl;
      console.log(`[${e.symbol}] Loaded daily: PDH=${e.pdh}, PDL=${e.pdl}`);
    }
  } catch (err) {
    console.warn(`[${e.symbol}] Daily bootstrap failed:`, err.message);
  }

  if (e.c15.length > 0) {
    e.asiaRange = calcAsiaRange(e);
    e.atr = calcAtr(e);
    const last = e.c15[e.c15.length - 1];
    e.cb = [{ minuteKey: Math.floor(Date.now()/60000), o: last.c, h: last.c, l: last.c, c: last.c, t: new Date() }];
    detectSwings(e); updateZones(e); updateBias(e);
    if (e.price == null) handlePrice(e, last.c, offset);
    console.log(`[${e.symbol}] Bootstrap complete: ${e.c15.length}×15m, ${e.htf.length}×1H, ATR=${e.atr.toFixed(2)}, bias=${e.bias}, price=${fmtPrice(e.symbol, last.c)}`);
  } else {
    console.error(`[${e.symbol}] Bootstrap FAILED — no 15m candles loaded. Will retry in 30s.`);
  }
}

// ===== Refresh 1H candles (called every 5 min from server) =====
async function refreshHtf(e, offset) {
  const endpoints = e.config.futures
    ? ['https://fapi.binance.com/fapi/v1/klines']
    : ['https://api.binance.com/api/v3/klines', 'https://data-api.binance.vision/api/v3/klines'];
  const sym = e.stream.toUpperCase();
  for (const ep of endpoints) {
    try {
      const r = await fetch(ep + '?symbol=' + sym + '&interval=1h&limit=100');
      const data = await r.json();
      if (Array.isArray(data) && data.length > 0) {
        e.htf = data.map(k => ({
          o: parseFloat(k[1])+offset, h: parseFloat(k[2])+offset,
          l: parseFloat(k[3])+offset, c: parseFloat(k[4])+offset,
          t: new Date(k[0])
        }));
        return;
      }
    } catch (err) {
      console.warn(`[${e.symbol}] HTF refresh from ${ep.split('//')[1].split('/')[0]} failed:`, err.message);
    }
  }
}

function bootSess(e) {
  e.c15.forEach(function (c) {
    const h = c.t.getUTCHours();
    let s = null;
    if (h >= 0 && h < 8) s = 'asian';
    else if (h >= 8 && h < 13) s = 'london';
    else if (h >= 13 && h < 21) s = 'ny';
    if (!s) return;
    const se = e.ses[s];
    if (se.h == null || c.h > se.h) se.h = c.h;
    if (se.l == null || c.l < se.l) se.l = c.l;
  });
}

function bootPD(e) {
  if (e.daily) { e.pdh = e.daily.pdh; e.pdl = e.daily.pdl; }
}

function handlePrice(e, price, offset) {
  if (!price || price < 0.5 || price > 1000000) return;
  // Always set price first — even if c15 is empty, we want /health to show live prices
  const adj = offset !== 0 ? price + offset : price;
  e.prev = e.price; e.price = adj;
  e.ph.push({ t: new Date(), p: adj });
  if (e.ph.length > 500) e.ph.shift();
  // Auto-retry bootstrap if c15 is empty — but only once per 5 minutes (avoids rate limit bans)
  if (e.c15.length === 0) {
    const now = Date.now();
    if (!e._lastBootstrapRetry || now - e._lastBootstrapRetry > 300000) {
      e._lastBootstrapRetry = now;
      console.log(`[${e.symbol}] c15 empty on price tick — retrying bootstrap (5min cooldown)`);
      bootstrap(e, offset);
    }
    e.signal = { action: 'WAIT', reason: 'Building candles…' };
    return;
  }
  try {
    updateCB(e, adj); updateSess(e, adj);
    updateZones(e); detectSweeps(e); updateBias(e);
    e.atr = calcAtr(e);
    scanSig(e);
  } catch (err) {
    console.error(`[${e.symbol}] handlePrice error:`, err.message);
  }
}

function updateCB(e, p) {
  const t = new Date();
  const mk = Math.floor(t.getTime() / 60000);
  const l = e.cb[e.cb.length - 1];
  if (!l || l.minuteKey !== mk) {
    e.cb.push({ minuteKey: mk, o: p, h: p, l: p, c: p, t });
    if (e.cb.length > 200) e.cb.shift();
  } else {
    l.h = Math.max(l.h, p); l.l = Math.min(l.l, p); l.c = p;
  }
  if (e.cb.length >= 15) {
    const r15 = e.cb.slice(-15);
    const k15 = Math.floor(mk / 15);
    const ex = e.c15[e.c15.length - 1];
    if (!ex || ex.key !== k15) {
      e.c15.push({
        key: k15, o: r15[0].o,
        h: Math.max.apply(null, r15.map(c => c.h)),
        l: Math.min.apply(null, r15.map(c => c.l)),
        c: r15[r15.length - 1].c,
        t: new Date(k15 * 60000)
      });
      if (e.c15.length > 100) e.c15.shift();
      detectSwings(e);
    } else {
      ex.h = Math.max(ex.h, Math.max.apply(null, r15.map(c => c.h)));
      ex.l = Math.min(ex.l, Math.min.apply(null, r15.map(c => c.l)));
      ex.c = r15[r15.length - 1].c;
    }
  }
}

function updateSess(e, p) {
  const s = curSess();
  if (!s) return;
  const se = e.ses[s];
  if (se.h == null) se.h = p;
  if (se.l == null) se.l = p;
  se.h = Math.max(se.h, p); se.l = Math.min(se.l, p);
  if (gH() === 0) {
    e.pdh = Math.max(e.ses.asian.h || 0, e.ses.london.h || 0, e.ses.ny.h || 0);
    e.pdl = Math.min(e.ses.asian.l || 9e9, e.ses.london.l || 9e9, e.ses.ny.l || 9e9);
    e.ses = { asian: { h: null, l: null }, london: { h: null, l: null }, ny: { h: null, l: null } };
  }
}

function updateZones(e) {
  const z = [];
  Object.keys(e.ses).forEach(function (n) {
    const s = e.ses[n];
    if (s.h != null) z.push({ type: 'session_high', level: s.h, name: n + ' H' });
    if (s.l != null) z.push({ type: 'session_low', level: s.l, name: n + ' L' });
  });
  if (e.pdh != null) z.push({ type: 'pdh', level: e.pdh, name: 'PDH' });
  if (e.pdl != null) z.push({ type: 'pdl', level: e.pdl, name: 'PDL' });
  if (e.asiaRange) {
    z.push({ type: 'session_high', level: e.asiaRange.hi, name: 'Asia H' });
    z.push({ type: 'session_low', level: e.asiaRange.lo, name: 'Asia L' });
  }
  e.swH.forEach(sw => z.push({ type: 'swing_high', level: sw, name: 'Swing H' }));
  e.swL.forEach(sw => z.push({ type: 'swing_low', level: sw, name: 'Swing L' }));
  const th = e.price ? e.price * 0.001 : 10;
  for (let i = 0; i < e.swH.length; i++)
    for (let j = i + 1; j < e.swH.length; j++)
      if (Math.abs(e.swH[i] - e.swH[j]) < th)
        z.push({ type: 'eqh', level: (e.swH[i] + e.swH[j]) / 2, name: 'EQH' });
  for (let i = 0; i < e.swL.length; i++)
    for (let j = i + 1; j < e.swL.length; j++)
      if (Math.abs(e.swL[i] - e.swL[j]) < th)
        z.push({ type: 'eql', level: (e.swL[i] + e.swL[j]) / 2, name: 'EQL' });
  if (e.price != null) {
    const step = e.config.roundStep;
    const base = Math.floor(e.price / step) * step;
    for (let i = -3; i <= 3; i++) {
      const lv = base + i * step;
      if (lv > 0) z.push({ type: 'round', level: lv, name: fmtPrice(e.symbol, lv) });
    }
  }
  const sTh = e.price ? e.price * 0.001 : 1;
  z.forEach(function (zn) {
    if (e.swept.some(s => Math.abs(s.level - zn.level) < sTh)) zn.swept = true;
  });
  e.zones = z.sort((a, b) => b.level - a.level);
}

function detectSwings(e) {
  const h = [], l = [];
  const c = e.c15;
  if (c.length < 5) return;
  for (let i = 2; i < c.length - 2; i++) {
    if (c[i].h > c[i-1].h && c[i].h > c[i-2].h && c[i].h > c[i+1].h && c[i].h > c[i+2].h) h.push(c[i].h);
    if (c[i].l < c[i-1].l && c[i].l < c[i-2].l && c[i].l < c[i+1].l && c[i].l < c[i+2].l) l.push(c[i].l);
  }
  e.swH = h.slice(-8); e.swL = l.slice(-8);
}

function detectSweeps(e) {
  if (!e.price || e.c15.length < 3) return;
  const recent = e.c15.slice(-3);
  const already = e.swept.map(s => s.level);
  const th = e.price * 0.0005, dd = e.price * 0.002;
  const zones = e.zones.filter(z => !z.swept && !already.some(l => Math.abs(l - z.level) < dd));
  const swept = [];
  zones.forEach(function (z) {
    recent.forEach(function (c) {
      const sw = z.type.indexOf('high') >= 0
        ? (c.h > z.level + th && c.c < z.level - th / 2)
        : (c.l < z.level - th && c.c > z.level + th / 2);
      if (sw) swept.push(Object.assign({}, z, { time: new Date() }));
    });
  });
  const prio = { eqh: 5, eql: 5, pdh: 4, pdl: 4, session_high: 3, session_low: 3, swing_high: 2, swing_low: 2, round: 1 };
  const clusters = [];
  swept.forEach(function (s) {
    const ex = clusters.find(cl => Math.abs(cl.level - s.level) < dd);
    if (ex) { if ((prio[s.type] || 0) > (prio[ex.type] || 0)) Object.assign(ex, s); }
    else clusters.push(s);
  });
  clusters.forEach(s => e.swept.push(Object.assign({}, s, { locked: true })));
  e.swept = e.swept.filter(s => (new Date() - s.time) < 7200000);
}

// ===== Bias: 1H HTF primary, 15m breakout secondary =====
function updateBias(e) {
  const c = e.c15;
  if (c.length < 10) { e.bias = 'neutral'; e.bd = null; return; }
  
  // 1H bias
  const hb = htfBias(e);
  
  // 15m breakout check (existing logic)
  if (c.length >= 3) {
    const l3 = c.slice(-3);
    const ms = l3[2].c - l3[0].o;
    const mp = Math.abs(ms) / e.price;
    const th = e.config.breakoutTh;
    if (mp > th) {
      if (ms > 0) {
        const bl = Math.max.apply(null, l3.map(x => x.h));
        const rh = Math.max.apply(null, c.slice(-20, -3).map(x => x.h));
        if (bl > rh) { e.bias = 'bullish'; e.bd = { strength: 3, structure: 'Breakout bullish', displacement: 'Normal' }; return; }
      } else {
        const bl2 = Math.min.apply(null, l3.map(x => x.l));
        const rl = Math.min.apply(null, c.slice(-20, -3).map(x => x.l));
        if (bl2 < rl) { e.bias = 'bearish'; e.bd = { strength: 3, structure: 'Breakout bearish', displacement: 'Normal' }; return; }
      }
    }
  }
  
  // Use 1H bias
  if (hb === 'bull') { e.bias = 'bullish'; e.bd = { strength: 4, structure: '1H HH+HL', displacement: 'Normal' }; return; }
  if (hb === 'bear') { e.bias = 'bearish'; e.bd = { strength: 4, structure: '1H LH+LL', displacement: 'Normal' }; return; }
  
  // 15m swing structure — use ALL available candles (not just last 20) for better bias detection
  const r = c.slice(-Math.min(c.length, 50));
  const swH = [], swL = [];
  for (let i = 2; i < r.length - 2; i++) {
    if (r[i].h > r[i-1].h && r[i].h > r[i-2].h && r[i].h > r[i+1].h && r[i].h > r[i+2].h) swH.push({ h: r[i].h });
    if (r[i].l < r[i-1].l && r[i].l < r[i-2].l && r[i].l < r[i+1].l && r[i].l < r[i+2].l) swL.push({ l: r[i].l });
  }
  let sb = 'neutral', st = 0;
  if (swH.length >= 2 && swL.length >= 2) {
    const hh = swH[swH.length-1].h > swH[swH.length-2].h;
    const hl = swL[swL.length-1].l > swL[swL.length-2].l;
    const lh = swH[swH.length-1].h < swH[swH.length-2].h;
    const ll = swL[swL.length-1].l < swL[swL.length-2].l;
    if (hh && hl) { sb = 'bullish'; st = 3; }
    else if (lh && ll) { sb = 'bearish'; st = 3; }
    else if (hh && !hl) { sb = 'bullish'; st = 2; }
    else if (ll && !lh) { sb = 'bearish'; st = 2; }
  }
  
  // If swing structure inconclusive, fall back to momentum (lowered threshold for more signals)
  if (sb === 'neutral' && c.length >= 4) {
    const l4 = c.slice(-4);
    const bC = l4.filter(x => x.c < x.o).length;
    const buC = l4.filter(x => x.c > x.o).length;
    if (bC >= 3) { sb = 'bearish'; st = 2; }
    else if (buC >= 3) { sb = 'bullish'; st = 2; }
    else if (c.length >= 10) {
      const l10 = c.slice(-10);
      const sma = l10.reduce((s, x) => s + x.c, 0) / 10;
      const dev = (c[c.length-1].c - sma) / sma;
      if (dev < -0.002) { sb = 'bearish'; st = 2; }
      else if (dev > 0.002) { sb = 'bullish'; st = 2; }
    }
  }
  
  e.bias = sb;
  e.bd = st > 0 ? { strength: st, structure: st >= 3 ? '15m swing' : '15m momentum', displacement: 'Normal' } : null;
}

function isConsumed(e, s) {
  const th = e.price * 0.001;
  return e.consumed.some(cs => Math.abs(cs.level - s.level) < th && Date.now() - cs.time < 3600000);
}

function scanSig(e, minGrade, onSignal) {
  if (!e.price) { e.signal = null; return; }
  const now = Date.now();
  if (e.config.weekendSuppress && isFxWeekend()) {
    e.signal = { action: 'WAIT', reason: 'Market closed (weekend)' };
    e.actSig = null; e.sigFormedAt = null;
    return;
  }
  if (e.actSig && now < e.lockUntil) {
    e.signal = e.actSig; e.signal.currentPrice = e.price;
    return;
  }
  if (e.lastFireTime && e.lastFireQuality) {
    const cdMins = { A: 10, B: 25, C: 45 }[e.lastFireQuality] || 30;
    const elapsed = now - e.lastFireTime;
    if (elapsed < cdMins * 60000) {
      e.signal = { action: 'WAIT', reason: `Cooldown ${cdMins - Math.floor(elapsed / 60000)}m (last was ${e.lastFireQuality}-grade)` };
      return;
    }
  }
  const lc = e.c15[e.c15.length - 1];
  if (!lc) { e.signal = { action: 'WAIT', reason: 'Building candles…' }; return; }
  const isNew = lc.key !== e.lastCK;
  if (e.actSig && !isNew) {
    e.lockUntil = now + 300000;
    e.signal = e.actSig; e.signal.currentPrice = e.price;
    return;
  }
  e.lastCK = lc.key;
  const result = evaluate(e);
  if (!result) { e.signal = { action: 'WAIT', reason: 'No setup' }; e.actSig = null; return; }
  if (result.action === 'LONG' || result.action === 'SHORT') {
    if (!minGrade || minGrade.indexOf(result.quality) < 0) {
      e.signal = { action: 'WATCH', reason: `${result.quality}-grade below ${minGrade}-only filter (score ${result.score}/9)` };
      e.actSig = null;
      return;
    }
    const errs = [];
    if (result.action === 'LONG' && result.sl >= result.entry) errs.push('SL above entry');
    if (result.action === 'SHORT' && result.sl <= result.entry) errs.push('SL below entry');
    if (result.action === 'LONG' && result.tp1 <= result.entry) errs.push('TP1 below entry');
    if (result.action === 'SHORT' && result.tp1 >= result.entry) errs.push('TP1 above entry');
    if (errs.length > 0) {
      e.signal = { action: 'WAIT', reason: 'Rejected: ' + errs.join('; ') };
      e.actSig = null;
      return;
    }
    if (result.sweep) {
      e.consumed.push({ level: result.sweep.level, time: Date.now() });
      e.consumed = e.consumed.filter(s => Date.now() - s.time < 3600000);
    }
    e.actSig = result;
    e.lockUntil = now + 300000;
    e.signal = result; e.signal.currentPrice = e.price;
    e.lastFireTime = now;
    e.lastFireQuality = result.quality;
    e.sigFormedAt = now;
    if (onSignal) onSignal(e, result);
  } else {
    e.signal = result;
    e.actSig = null;
  }
}

// ===== FVG detection (Fair Value Gap) =====
function findFVG(e, startIdx, endIdx, bias) {
  const c = e.c15;
  for (let m = Math.max(2, startIdx); m <= Math.min(endIdx, c.length - 1); m++) {
    if (bias === 'bullish' && c[m].l > c[m-2].h) {
      return { lo: c[m-2].h, hi: c[m].l, idx: m };
    }
    if (bias === 'bearish' && c[m].h < c[m-2].l) {
      return { lo: c[m].h, hi: c[m-2].l, idx: m };
    }
  }
  return null;
}

// ===== Order Block detection =====
function findOB(e, startIdx, endIdx, bias) {
  const c = e.c15;
  for (let b = Math.min(endIdx, c.length - 1); b >= startIdx; b--) {
    if (bias === 'bullish' && c[b].c < c[b].o) {
      return { lo: c[b].l, hi: c[b].o, idx: b };
    }
    if (bias === 'bearish' && c[b].c > c[b].o) {
      return { lo: c[b].o, hi: c[b].h, idx: b };
    }
  }
  return null;
}

// ===== Premium/Discount check =====
function inPremiumOrDiscount(e, entry, bias) {
  const c = e.c15;
  if (c.length < 120) return null;
  const r120 = c.slice(-120);
  let hi = -Infinity, lo = Infinity;
  for (const k of r120) { hi = Math.max(hi, k.h); lo = Math.min(lo, k.l); }
  const mid = (hi + lo) / 2;
  if (bias === 'bullish') return entry < mid ? 'discount' : 'premium';
  if (bias === 'bearish') return entry > mid ? 'premium' : 'discount';
  return null;
}

// ===== CHoCH detection (Change of Character) =====
// After a sweep, look for close beyond the most recent swing high (bullish) or swing low (bearish)
function findCHoCH(e, sweepIdx, bias) {
  const c = e.c15;
  const n = c.length;
  // Look in candles after the sweep, up to 5 candles forward
  for (let b = sweepIdx + 1; b <= Math.min(sweepIdx + 5, n - 1); b++) {
    if (bias === 'bullish') {
      // Find most recent swing high before the sweep
      const prior = e.swH.filter(sw => sw < c[sweepIdx].l);
      if (prior.length > 0) {
        const swingHi = prior[prior.length - 1];
        if (c[b].c > swingHi) return { idx: b, level: swingHi };
      }
    } else {
      const prior = e.swL.filter(sw => sw > c[sweepIdx].h);
      if (prior.length > 0) {
        const swingLo = prior[prior.length - 1];
        if (c[b].c < swingLo) return { idx: b, level: swingLo };
      }
    }
  }
  return null;
}

function evaluate(e) {
  const c = e.c15;
  if (c.length < 20) return { action: 'WAIT', reason: 'Not enough candles' };
  
  const bias = e.bias;
  if (bias === 'neutral') return { action: 'WAIT', reason: 'No bias (1H range + 15m neutral)' };
  
  const cp = e.price;
  const atr = e.atr || calcAtr(e);
  if (atr <= 0) return { action: 'WAIT', reason: 'ATR not ready' };
  
  // Find recent sweeps (within 30 min)
  const recent = e.swept.filter(s => (new Date() - s.time) < 1800000);
  const majT = ['session_high', 'session_low', 'pdh', 'pdl', 'eqh', 'eql'];
  const balT = majT.concat(['swing_high', 'swing_low']);
  
  let sweep = null;
  let sweepIdx = -1;
  let choch = null;
  let poi = null;
  let poiType = null;
  let trigger = null;
  let entryType = 'smc';
  
  // === PATH 1: SMC sequence (sweep → CHoCH → POI) ===
  if (bias === 'bullish') {
    sweep = recent.find(s => (s.type.indexOf('low') >= 0 || s.type === 'pdl' || s.type === 'eql') && s.level < cp - cp * 0.001 && !isConsumed(e, s));
  } else {
    sweep = recent.find(s => (s.type.indexOf('high') >= 0 || s.type === 'pdh' || s.type === 'eqh') && s.level > cp + cp * 0.001 && !isConsumed(e, s));
  }
  
  if (sweep) {
    // Find sweep candle index
    for (let i = c.length - 1; i >= Math.max(0, c.length - 6); i--) {
      if (bias === 'bullish' && c[i].l <= sweep.level + cp * 0.0005) { sweepIdx = i; break; }
      if (bias === 'bearish' && c[i].h >= sweep.level - cp * 0.0005) { sweepIdx = i; break; }
    }
    
    if (sweepIdx >= 0) {
      // Look for CHoCH
      choch = findCHoCH(e, sweepIdx, bias);
      
      if (choch) {
        // Look for POI (FVG or OB) after CHoCH
        const poiEnd = Math.min(c.length - 1, choch.idx + 3);
        const fvg = findFVG(e, choch.idx, poiEnd, bias);
        const ob = findOB(e, choch.idx, poiEnd, bias);
        
        if (fvg) { poi = fvg; poiType = 'FVG'; }
        else if (ob) { poi = ob; poiType = 'OB'; }
        
        // Determine trigger
        const lc = c[c.length - 1];
        const body = Math.abs(lc.c - lc.o), range = lc.h - lc.l;
        const isBuE = lc.c > lc.o && body > range * 0.55;
        const isBE = lc.c < lc.o && body > range * 0.55;
        
        if (bias === 'bullish' && isBuE) trigger = 'Bullish Engulfing';
        else if (bias === 'bearish' && isBE) trigger = 'Bearish Engulfing';
        else if (body >= 1.2 * atr) trigger = bias === 'bullish' ? 'Bullish Displacement' : 'Bearish Displacement';
        else trigger = bias === 'bullish' ? 'Bullish CHoCH' : 'Bearish CHoCH';
      }
    }
  }
  
  // === PATH 2: Breakout (if no sweep found but breakout bias exists) ===
  if (!sweep && e.bd && e.bd.structure && e.bd.structure.indexOf('Breakout') >= 0) {
    if (bias === 'bullish') {
      const rl = Math.min.apply(null, c.slice(-20, -3).map(x => x.l));
      sweep = { level: rl, name: 'Breakout L', type: 'swing_low', time: new Date() };
    } else {
      const rh = Math.max.apply(null, c.slice(-20, -3).map(x => x.h));
      sweep = { level: rh, name: 'Breakout H', type: 'swing_high', time: new Date() };
    }
    trigger = bias === 'bullish' ? 'Bullish Breakout' : 'Bearish Breakout';
    entryType = 'breakout';
  }
  
  // === PATH 3: Breakout Continuation (strong momentum, no sweep needed) ===
  if (!sweep && !trigger && e.bd && e.bd.strength >= 3) {
    const lc2 = c[c.length - 1];
    const body2 = Math.abs(lc2.c - lc2.o);
    const range2 = lc2.h - lc2.l;
    const prev10 = c.slice(-11, -1);
    const avgBody = prev10.reduce((s, x) => s + Math.abs(x.c - x.o), 0) / prev10.length;
    const isStrongBody = body2 > avgBody * 1.5 && body2 > cp * 0.0015;
    const closedTop25 = lc2.c > lc2.l + range2 * 0.75;
    const closedBottom25 = lc2.c < lc2.l + range2 * 0.25;
    
    if (isStrongBody && range2 > 0) {
      if (bias === 'bullish' && closedTop25 && lc2.c > lc2.o) {
        const lastSwingLow = e.swL.length > 0 ? Math.min.apply(null, e.swL.slice(-3)) : lc2.l;
        sweep = { level: lastSwingLow, name: 'Swing L (continuation)', type: 'swing_low', time: new Date() };
        trigger = 'Breakout Continuation'; entryType = 'continuation';
      } else if (bias === 'bearish' && closedBottom25 && lc2.c < lc2.o) {
        const lastSwingHigh = e.swH.length > 0 ? Math.max.apply(null, e.swH.slice(-3)) : lc2.h;
        sweep = { level: lastSwingHigh, name: 'Swing H (continuation)', type: 'swing_high', time: new Date() };
        trigger = 'Breakout Continuation'; entryType = 'continuation';
      }
    }
  }
  
  if (!sweep) {
    return { action: 'WAIT', reason: bias === 'bullish' ? 'No downside sweep' : 'No upside sweep' };
  }
  if (!balT.includes(sweep.type) && sweep.name.indexOf('Breakout') < 0 && sweep.name.indexOf('continuation') < 0) {
    return { action: 'WAIT', reason: sweep.name + ' not allowed' };
  }
  if (!trigger) {
    return { action: 'WATCH', reason: 'Sweep at ' + sweep.name + ' — waiting for CHoCH/trigger' };
  }
  
  // === Entry & SL (ATR-based) ===
  const entry = e.price;
  const atrStop = Math.max(0.15 * atr, e.config.slBuffer);
  const sl = bias === 'bullish' ? sweep.level - atrStop : sweep.level + atrStop;
  
  if (bias === 'bullish' && sl >= entry) return { action: 'WAIT', reason: 'SL above entry' };
  if (bias === 'bearish' && sl <= entry) return { action: 'WAIT', reason: 'SL below entry' };
  
  const zr = Math.abs(entry - sl);
  const rwp = e.config.riskWidePct || 0.008;
  if (zr > cp * rwp) return { action: 'WAIT', reason: 'Risk too wide' };
  
  // Min SL filter
  const minSlPct = e.symbol === 'BTC' ? 0.003 : 0.002;
  if (zr < cp * minSlPct) return { action: 'WAIT', reason: 'SL too tight (' + (zr / cp * 100).toFixed(2) + '%)' };
  
  // === Targets (liquidity zones) ===
  const untouched = e.zones.filter(z => !z.swept);
  const maxDist = Math.max(cp * 0.05, zr * 5);
  const scored = untouched.map(function (z) {
    let s = 0, rev = false;
    if (z.type === 'eqh' || z.type === 'eql') { s = 3; rev = true; }
    else if (z.type === 'pdh' || z.type === 'pdl') { s = 3; rev = true; }
    else if (z.type.indexOf('session') >= 0) s = 2;
    else if (z.type.indexOf('swing') >= 0) s = 2;
    else if (z.type === 'round') s = e.config.roundStars || 2;
    const nb = untouched.filter(o => o !== z && Math.abs(o.level - z.level) < cp * 0.001);
    if (nb.length > 0) s = Math.min(3, s + 1);
    return Object.assign({}, z, { stars: s, isRev: rev });
  });
  const inDir = bias === 'bullish'
    ? scored.filter(z => z.level > entry + entry * 0.001 && (z.level - entry) <= maxDist).sort((a, b) => a.level - b.level)
    : scored.filter(z => z.level < entry - entry * 0.001 && (entry - z.level) <= maxDist).sort((a, b) => b.level - a.level);
  
  let tp1, tp2;
  if (inDir.length === 0) {
    return { action: 'WAIT', reason: 'No liquidity zone for TP' };
  }
  const minTP1R = zr * 1.0;
  const tp1C = inDir.filter(z => Math.abs(z.level - entry) >= minTP1R);
  tp1 = tp1C[0] || inDir[0];
  const tp1R = Math.abs(tp1.level - entry) / zr;
  if (tp1R > 3.0) return { action: 'WAIT', reason: 'TP1 too far (' + tp1R.toFixed(1) + 'R)' };
  
  const ms2 = cp * 0.001;
  const isLng = bias === 'bullish';
  const further = inDir.filter(z => z !== tp1 && (isLng ? (z.level > tp1.level + ms2) : (z.level < tp1.level - ms2)));
  tp2 = further.find(z => z.stars >= 2) || further[0] || null;
  
  const rr1 = zr > 0 ? Math.abs(tp1.level - entry) / zr : 0;
  const rr2 = tp2 ? (zr > 0 ? Math.abs(tp2.level - entry) / zr : 0) : 0;
  
  if (zr < e.config.minRisk) return { action: 'WAIT', reason: 'Risk below min' };
  if (zr > e.config.maxRisk) return { action: 'WAIT', reason: 'Risk too high' };
  if (bias === 'bullish' && tp1.level <= entry) return { action: 'WAIT', reason: 'TP1 below entry' };
  if (bias === 'bearish' && tp1.level >= entry) return { action: 'WAIT', reason: 'TP1 above entry' };
  
  // ===== 9-POINT CHECKLIST SCORING =====
  const isStructural = (z) => z && z.type !== 'round';
  const htfB = htfBias(e);
  const pd = inPremiumOrDiscount(e, entry, bias);
  const kz = isKillZone();
  const disp = trigger && (trigger.indexOf('Displacement') >= 0 || trigger.indexOf('Engulfing') >= 0);
  
  const checks = [
    { k: 'bias', label: '1H bias aligned', ok: (htfB === 'bull' && bias === 'bullish') || (htfB === 'bear' && bias === 'bearish') },
    { k: 'sweep', label: 'Liquidity swept: ' + sweep.name, ok: true },
    { k: 'choch', label: 'CHoCH confirmed', ok: !!choch },
    { k: 'poi', label: 'POI: ' + (poiType || 'none'), ok: !!poi },
    { k: 'pd', label: 'Entry in ' + (pd || '—'), ok: pd === 'discount' || pd === 'premium' },
    { k: 'kz', label: 'Kill Zone', ok: kz },
    { k: 'rr', label: 'R:R ≥ 2', ok: rr1 >= 2.0 },
    { k: 'atr', label: 'SL ≥ 0.5×ATR', ok: zr >= 0.5 * atr },
    { k: 'trigger', label: 'Strong trigger', ok: !!disp }
  ];
  const score = checks.filter(c => c.ok).length;
  
  // Grade from score: 7-9 = A, 5-6 = B, 3-4 = C
  let quality = 'C';
  if (score >= 7 && isStructural(tp1)) quality = 'A';
  else if (score >= 5 && isStructural(tp1)) quality = 'B';
  
  const confidence = Math.min(5, Math.ceil(score / 2));
  
  return {
    action: bias === 'bullish' ? 'LONG' : 'SHORT',
    quality, entryType, score, checks,
    confidence,
    entry, sl,
    tp1: tp1.level, tp2: tp2 ? tp2.level : null, tp3: null,
    zoneRisk: zr, rr1, rr2,
    sweep, trigger, confluence: poiType || 'No POI',
    poi: poi ? { lo: poi.lo, hi: poi.hi, type: poiType } : null,
    atr, htfBias: htfB, premiumDiscount: pd, killZone: kz
  };
}

module.exports = {
  makeEngine, bootstrap, refreshHtf, handlePrice, scanSig, fmtPht, fmtPrice, isFxWeekend, calcAtr, htfBias,
  detectSwings, updateZones, updateBias
};
