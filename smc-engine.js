// smc-engine.js — SMC Liquidity Zone engine, extracted from ZAI QUAD FRAMEWORK
// Same logic as the browser version, ported to Node.js for server-side 24/7 operation.

function makeEngine(symbol, stream, config) {
  return {
    symbol, stream, config,
    prefix: config.prefix,
    price: null, prev: null,
    ph: [], cb: [], c15: [],
    ses: { asian: { h: null, l: null }, london: { h: null, l: null }, ny: { h: null, l: null } },
    pdh: null, pdl: null,
    zones: [], bias: 'neutral', bd: null,
    swH: [], swL: [], swept: [], consumed: [],
    signal: null, actSig: null,
    lockUntil: 0, lastCK: null, lastSK: null,
    sigFormedAt: null, lastFireTime: 0, lastFireQuality: null,
    lhb: 0
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
  if (day === 5 && h >= 14) return true;       // Friday after 14:00 UTC = Fri 22:00 PHT
  if (day === 6 && h < 16) return true;        // Saturday before 16:00 UTC = Sat 23:59 PHT
  return false;
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

// Bootstrap historical candles from Binance REST API
async function bootstrap(e, offset) {
  if (e.c15.length >= 50) return;
  try {
    const base = e.config.futures
      ? 'https://fapi.binance.com/fapi/v1/klines'
      : 'https://api.binance.com/api/v3/klines';
    const r = await fetch(base + '?symbol=' + e.stream.toUpperCase() + '&interval=15m&limit=100');
    if (!r.ok) {
      console.warn(`[${e.symbol}] Bootstrap HTTP ${r.status}: ${await r.text()}`);
      return;
    }
    const data = await r.json();
    if (!Array.isArray(data)) {
      console.warn(`[${e.symbol}] Bootstrap returned non-array:`, data);
      return;
    }
    e.c15 = data.map(function (k) {
      return {
        key: Math.floor(k[0] / 60000 / 15),
        o: parseFloat(k[1]) + offset,
        h: parseFloat(k[2]) + offset,
        l: parseFloat(k[3]) + offset,
        c: parseFloat(k[4]) + offset,
        t: new Date(k[0])
      };
    });
    const last = e.c15[e.c15.length - 1];
    e.cb = [{ minuteKey: Math.floor(Date.now() / 60000), o: last.c, h: last.c, l: last.c, c: last.c, t: new Date() }];
    bootSess(e); bootPD(e); detectSwings(e); updateZones(e); updateBias(e);
    if (e.price == null) handlePrice(e, last.c, offset);
    console.log(`[${e.symbol}] Bootstrapped ${e.c15.length} candles, price=${fmtPrice(e.symbol, last.c)}`);
  } catch (err) {
    console.warn(`[${e.symbol}] Bootstrap failed:`, err.message);
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
  const t = new Date(); t.setUTCHours(0, 0, 0, 0);
  const y = t.getTime() - 86400000;
  let ph = -Infinity, pl = Infinity, f = false;
  e.c15.forEach(function (c) {
    const d = new Date(c.t); d.setUTCHours(0, 0, 0, 0);
    if (d.getTime() === y) { f = true; ph = Math.max(ph, c.h); pl = Math.min(pl, c.l); }
  });
  if (f) { e.pdh = ph; e.pdl = pl; }
}

function handlePrice(e, price, offset) {
  if (!price || price < 0.5 || price > 1000000) return;
  const adj = offset !== 0 ? price + offset : price;
  e.prev = e.price; e.price = adj;
  e.ph.push({ t: new Date(), p: adj });
  if (e.ph.length > 500) e.ph.shift();
  try {
    updateCB(e, adj); updateSess(e, adj);
    updateZones(e); detectSweeps(e); updateBias(e);
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
    if (c[i].h > c[i - 1].h && c[i].h > c[i - 2].h && c[i].h > c[i + 1].h && c[i].h > c[i + 2].h) h.push(c[i].h);
    if (c[i].l < c[i - 1].l && c[i].l < c[i - 2].l && c[i].l < c[i + 1].l && c[i].l < c[i + 2].l) l.push(c[i].l);
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

function updateBias(e) {
  const c = e.c15;
  if (c.length < 10) { e.bias = 'neutral'; e.bd = null; return; }
  const r = c.slice(-20);
  const swH = [], swL = [];
  for (let i = 2; i < r.length - 2; i++) {
    if (r[i].h > r[i - 1].h && r[i].h > r[i - 2].h && r[i].h > r[i + 1].h && r[i].h > r[i + 2].h) swH.push({ h: r[i].h });
    if (r[i].l < r[i - 1].l && r[i].l < r[i - 2].l && r[i].l < r[i + 1].l && r[i].l < r[i + 2].l) swL.push({ l: r[i].l });
  }
  let sb = 'neutral', st = 0, str = '—', disp = '—';
  if (swH.length >= 2 && swL.length >= 2) {
    const hh = swH[swH.length - 1].h > swH[swH.length - 2].h;
    const hl = swL[swL.length - 1].l > swL[swL.length - 2].l;
    const lh = swH[swH.length - 1].h < swH[swH.length - 2].h;
    const ll = swL[swL.length - 1].l < swL[swL.length - 2].l;
    if (hh && hl) { sb = 'bullish'; st = 4; str = 'HH+HL'; }
    else if (lh && ll) { sb = 'bearish'; st = 4; str = 'LH+LL'; }
    else if (hh && !hl) { sb = 'bullish'; st = 2; str = 'HH(weak)'; }
    else if (ll && !lh) { sb = 'bearish'; st = 2; str = 'LL(weak)'; }
  }
  let tHi = -Infinity, tLo = Infinity;
  if (c.length >= 50) {
    const big = c.slice(-50);
    for (let i = 0; i < big.length; i++) { if (big[i].h > tHi) tHi = big[i].h; if (big[i].l < tLo) tLo = big[i].l; }
  } else {
    tHi = Math.max.apply(null, c.map(x => x.h));
    tLo = Math.min.apply(null, c.map(x => x.l));
  }
  const tMid = (tHi + tLo) / 2;
  const mBull = c[c.length - 1].c > tMid;
  const mBear = c[c.length - 1].c < tMid;

  let mb = 'neutral', mn = '', bC = 0, buC = 0;
  if (c.length >= 4) {
    const l4 = c.slice(-4);
    bC = l4.filter(x => x.c < x.o).length;
    buC = l4.filter(x => x.c > x.o).length;
    if (bC === 4) { mb = 'bearish'; mn = ' (4-bear)'; }
    else if (buC === 4) { mb = 'bullish'; mn = ' (4-bull)'; }
  }
  if (c.length >= 10 && mb === 'neutral') {
    const l10 = c.slice(-10);
    const sma = l10.reduce((s, x) => s + x.c, 0) / 10;
    const dev = (c[c.length - 1].c - sma) / sma;
    if (dev < -0.003 && bC >= 3) { mb = 'bearish'; mn = ' (SMA+3/4)'; }
    else if (dev > 0.003 && buC >= 3) { mb = 'bullish'; mn = ' (SMA+3/4)'; }
  }
  if (mb === 'bullish' && mBear) mn = ' (counter-trend)';
  if (mb === 'bearish' && mBull) mn = ' (counter-trend)';
  if (sb === 'bullish' && mBear && st <= 2) str = 'HH(weak,CT)';
  if (sb === 'bearish' && mBull && st <= 2) str = 'LL(weak,CT)';

  let bias = sb;
  if (mb !== 'neutral' && mb !== sb) {
    // Stronger counter-trend filter — only allow flip if structure or macro agrees
    const structureAgrees = (mb === 'bullish' && sb === 'bullish') || (mb === 'bearish' && sb === 'bearish');
    const macroAgrees = (mb === 'bullish' && mBull) || (mb === 'bearish' && mBear);
    const macroNeutral = !mBull && !mBear;
    if (structureAgrees || macroAgrees || (macroNeutral && st >= 3)) {
      bias = mb;
      const isCT = !macroAgrees;
      st = isCT ? 2 : Math.max(st, 3);
      str += mn;
    } else {
      bias = sb === 'neutral' ? 'neutral' : sb;
      str += mn + ' [BLOCKED CT flip]';
    }
  } else if (mb === sb && mb !== 'neutral') { st = Math.min(5, st + 1); str += mn; }
  else if (mn) str += mn;

  if (c.length >= 6) {
    const l3 = c.slice(-3), p3 = c.slice(-6, -3);
    const lr = l3.reduce((s, x) => s + (x.h - x.l), 0) / 3;
    const pr = p3.reduce((s, x) => s + (x.h - x.l), 0) / 3;
    if (lr > pr * 1.5) { disp = l3[l3.length - 1].c > p3[0].o ? 'Bullish' : 'Bearish'; st = Math.min(5, st + 1); }
    else if (lr < pr * 0.5) { disp = 'Compression'; st = Math.max(0, st - 1); }
    else disp = 'Normal';
  }
  e.bias = bias;
  e.bd = { strength: st, structure: str, displacement: disp };
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
  // Graduated cooldown by last signal grade
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
    // Grade filter — only fire if quality meets minimum threshold
    if (!minGrade || minGrade.indexOf(result.quality) < 0) {
      e.signal = { action: 'WATCH', reason: `${result.quality}-grade below ${minGrade}-only filter` };
      e.actSig = null;
      return;
    }
    // Invariant validation
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
    // Fire the alert callback
    if (onSignal) onSignal(e, result);
  } else {
    e.signal = result;
    e.actSig = null;
  }
}

function evaluate(e) {
  // Breakout check first
  if (e.c15.length >= 3) {
    const c2 = e.c15, l3 = c2.slice(-3);
    const ms = l3[2].c - l3[0].o;
    const mp = Math.abs(ms) / e.price;
    const th = e.config.breakoutTh;
    if (mp > th) {
      if (ms > 0) {
        const bl = Math.max.apply(null, l3.map(x => x.h));
        const rh = Math.max.apply(null, c2.slice(-20, -3).map(x => x.h));
        if (bl > rh) { e.bias = 'bullish'; if (!e.bd) e.bd = {}; e.bd.strength = 3; e.bd.structure = 'Breakout bullish'; }
      } else {
        const bl2 = Math.min.apply(null, l3.map(x => x.l));
        const rl = Math.min.apply(null, c2.slice(-20, -3).map(x => x.l));
        if (bl2 < rl) { e.bias = 'bearish'; if (!e.bd) e.bd = {}; e.bd.strength = 3; e.bd.structure = 'Breakout bearish'; }
      }
    }
  }

  const bias = e.bias;
  if (bias === 'neutral') return { action: 'WAIT', reason: 'No bias' };

  const recent = e.swept.filter(s => (new Date() - s.time) < 1800000);
  let sweep = null;
  const cp = e.price;
  const majT = ['session_high', 'session_low', 'pdh', 'pdl', 'eqh', 'eql'];
  const balT = majT.concat(['swing_high', 'swing_low']);

  if (bias === 'bullish') {
    sweep = recent.find(s => (s.type.indexOf('low') >= 0 || s.type === 'pdl' || s.type === 'eql') && s.level < cp - cp * 0.001 && !isConsumed(e, s));
    if (!sweep) {
      if (e.bd && e.bd.structure && e.bd.structure.indexOf('Breakout') >= 0) {
        const rl2 = Math.min.apply(null, e.c15.slice(-20, -3).map(x => x.l));
        sweep = { level: rl2, name: 'Breakout L', type: 'swing_low', time: new Date() };
      } else return { action: 'WAIT', reason: 'No downside sweep' };
    }
  } else {
    sweep = recent.find(s => (s.type.indexOf('high') >= 0 || s.type === 'pdh' || s.type === 'eqh') && s.level > cp + cp * 0.001 && !isConsumed(e, s));
    if (!sweep) {
      if (e.bd && e.bd.structure && e.bd.structure.indexOf('Breakout') >= 0) {
        const rh2 = Math.max.apply(null, e.c15.slice(-20, -3).map(x => x.h));
        sweep = { level: rh2, name: 'Breakout H', type: 'swing_high', time: new Date() };
      } else return { action: 'WAIT', reason: 'No upside sweep' };
    }
  }
  if (!balT.includes(sweep.type) && sweep.name.indexOf('Breakout') < 0)
    return { action: 'WAIT', reason: sweep.name + ' not allowed' };

  const lc = e.c15[e.c15.length - 1];
  const body = Math.abs(lc.c - lc.o), range = lc.h - lc.l;
  const isBE = lc.c < lc.o && body > range * 0.55, isPE = (lc.h - lc.o) > body * 2 && lc.c < lc.o;
  const isBuE = lc.c > lc.o && body > range * 0.55, isPBu = (lc.o - lc.l) > body * 2 && lc.c > lc.o;
  let trigger = null, entryType = 'smc';
  if (bias === 'bullish' && (isBuE || isPBu)) { trigger = 'Bullish engulfing'; entryType = 'smc'; }
  if (bias === 'bearish' && (isBE || isPE)) { trigger = 'Bearish engulfing'; entryType = 'smc'; }
  // FVG confluence
  let confl = false, conflDesc = 'No FVG/OB';
  if (e.c15.length >= 3) {
    const l3c = e.c15.slice(-3);
    const th2 = cp * 0.002;
    if (bias === 'bullish') {
      const fL = l3c[0].h, fH = l3c[2].l;
      if (fH > fL && cp > fL && cp < fH + th2) { confl = true; conflDesc = 'FVG'; }
    } else {
      const fH2 = l3c[0].l, fL2 = l3c[2].h;
      if (fL2 < fH2 && cp < fH2 && cp > fL2 - th2) { confl = true; conflDesc = 'FVG'; }
    }
  }
  // Momentum trigger
  if (!trigger && e.bd && e.bd.strength >= 3 && majT.includes(sweep.type)) {
    const cb = bias === 'bullish' ? lc.c > sweep.level : lc.c < sweep.level;
    if (cb) { trigger = 'Momentum sweep'; entryType = 'momentum'; }
  }
  // Breakout trigger
  if (!trigger && e.bd && e.bd.structure && e.bd.structure.indexOf('Breakout') >= 0) {
    trigger = bias === 'bullish' ? 'Bullish breakout' : 'Bearish breakout';
    entryType = 'breakout';
  }
  // Pullback trigger
  if (!trigger && e.c15.length >= 10) {
    const r10 = e.c15.slice(-10);
    const mh = Math.max.apply(null, r10.map(x => x.h));
    const ml = Math.min.apply(null, r10.map(x => x.l));
    const msz = mh - ml;
    const mid = (mh + ml) / 2;
    const rt = cp * 0.002;
    const minMv = cp * e.config.pullbackTh;
    if (Math.abs(cp - mid) < rt && msz > minMv) {
      if (bias === 'bearish' && cp > mid) {
        trigger = 'Pullback 50%'; entryType = 'pullback';
        sweep = Object.assign({}, sweep, { level: mid, name: '50%', type: 'swing_high' });
      } else if (bias === 'bullish' && cp < mid) {
        trigger = 'Pullback 50%'; entryType = 'pullback';
        sweep = Object.assign({}, sweep, { level: mid, name: '50%', type: 'swing_low' });
      }
    }
  }
  if (!trigger && !confl) return { action: 'WATCH', reason: 'Sweep at ' + sweep.name + ' — waiting for trigger' };

  // Entry = current market price; SL = sweep level ± buffer
  const entry = e.price;
  const slBuf = e.config.slBuffer;
  const sl = bias === 'bullish' ? sweep.level - slBuf : sweep.level + slBuf;
  if (bias === 'bullish' && sl >= entry) return { action: 'WAIT', reason: 'SL above entry' };
  if (bias === 'bearish' && sl <= entry) return { action: 'WAIT', reason: 'SL below entry' };
  const zr = Math.abs(entry - sl);
  const rwp = e.config.riskWidePct || 0.008;
  if (zr > cp * rwp) return { action: 'WAIT', reason: 'Risk too wide' };

  // Targets — liquidity zones in trade direction
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

  let tp1, tp2, tp3;
  if (inDir.length === 0) {
    const tp1L = bias === 'bullish' ? entry + zr * 1.5 : entry - zr * 1.5;
    tp1 = { level: tp1L, stars: 1, name: 'Fallback' };
  } else {
    const minTP1R = zr * 1.0;
    const tp1C = inDir.filter(z => Math.abs(z.level - entry) >= minTP1R);
    tp1 = tp1C[0] || inDir[0];
    const ms2 = cp * 0.001;
    const isLng = bias === 'bullish';
    const further = inDir.filter(z => z !== tp1 && (isLng ? (z.level > tp1.level + ms2) : (z.level < tp1.level - ms2)));
    tp2 = further.find(z => z.stars >= 2) || further[0] || null;
    const further2 = tp2 ? further.filter(z => z !== tp2 && Math.abs(z.level - tp2.level) > ms2) : [];
    tp3 = further2.find(z => z.stars >= 3 || z.isRev) || further2[0] || null;
  }
  const rr1 = zr > 0 ? Math.abs(tp1.level - entry) / zr : 0;
  if (zr < e.config.minRisk) return { action: 'WAIT', reason: 'Risk below min' };
  if (zr > e.config.maxRisk) return { action: 'WAIT', reason: 'Risk too high' };
  if (bias === 'bullish' && tp1.level <= entry) return { action: 'WAIT', reason: 'TP1 below entry' };
  if (bias === 'bearish' && tp1.level >= entry) return { action: 'WAIT', reason: 'TP1 above entry' };

  let quality = 'C';
  const t3s = tp3 ? tp3.stars : 0, t2s = tp2 ? tp2.stars : 0;
  if (t3s >= 3 && t2s >= 2 && trigger && confl) quality = 'A';
  else if (t2s >= 2 && (trigger || confl)) quality = 'B';

  return {
    action: bias === 'bullish' ? 'LONG' : 'SHORT',
    quality, entryType,
    confidence: (trigger ? 2 : 0) + (confl ? 2 : 0) + (isSessWin() ? 1 : 0) + (quality === 'A' ? 1 : 0),
    entry, sl,
    tp1: tp1.level, tp2: tp2 ? tp2.level : null, tp3: tp3 ? tp3.level : null,
    zoneRisk: zr, rr1,
    sweep, trigger, confluence: conflDesc
  };
}

module.exports = {
  makeEngine, bootstrap, handlePrice, scanSig, fmtPht, fmtPrice, isFxWeekend
};
