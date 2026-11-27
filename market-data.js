// market-data.js — Shared data layer for all engines (V2, Sniper, CL)
//
// DATA SOURCE: NATIVE EXNESS API (replaces Binance WS + TwelveData REST entirely)
//
//   - REST bootstrap: GET /v1/market-data/accounts/{id}/candles?instrument=BTCUSD&timeframe=M5&from=...&count=300&price_type=bid
//   - Real-time ticks: Exness WebSocket → onTick() builds 5m + 15m candles live in the cache
//   - 1h is refreshed at :00:05 every hour (when a new 1h candle closes)
//   - 1d is refreshed every 2 hours (PDH/PDL only change at UTC midnight)
//
// INTERFACE (must stay compatible — engines depend on this):
//   bootstrapAll()                                → fetch M5 + M15 + H1 + D1 for BTCUSD + XAUUSD from Exness REST
//   getCandles(symbol, timeframe)                 → array of {t, o, h, l, c, v} where t = ms timestamp
//                                                  symbol: 'BTC' or 'XAU' (NOT 'BTCUSD')
//                                                  timeframe: '5m', '15m', '1h', '1d'
//   getDaily(symbol)                              → { pdh, pdl } from yesterday's D1 candle
//   getStats()                                    → cache stats for monitoring
//   getCallCount()                                → API call count
//   startScheduledRefresh()                       → 1h every hour, 1d every 2 hours
//   stopScheduledRefresh()                        → cleanup
//   onTick(instrument, bid, ask, timestamp)       → called by exness-api.js WS handler
//                                                  updates price + builds 5m and 15m candles
//                                                  returns { closed5m, forming5m } event info
//
// SYMBOL MAPPING:
//   Internal 'BTC' → Exness 'BTCUSD'
//   Internal 'XAU' → Exness 'XAUUSD'
//
// CANDLE FORMAT (Exness REST → internal):
//   Exness: { open, high, low, close, time, volume }    (time is ISO8601 with +00:00)
//   Internal: { t: new Date(candle.time).getTime(), o, h, l, c, v }
//
// INSTRUMENTS: Only BTC and XAU (no EUR, no XAG — per spec)
//
// RATE LIMITS:
//   Exness REST candle endpoint: 2 req/s (600ms gap enforced by exness-api.js)
//   Global REST: 15 req/s (we never approach this — bootstrap is 8 calls, refresh is 2 calls)
//   WebSocket: no rate limit (persistent connection)
//
// ENGINES USING THIS LAYER:
//   - V2:     reads 15m candles + 1d PDH/PDL (live 15m built by onTick from WS ticks)
//   - Sniper: reads 15m candles (live 15m built by onTick from WS ticks)
//   - CL:     reads 5m candles + 1h candles + 1d PDH/PDL (live 5m built by onTick from WS ticks)

'use strict';

const exnessAPI = require('./exness-api');

// ===== SYMBOL MAPPING (internal ↔ Exness) =====
const SYMBOLS = ['BTC', 'XAU'];
const EXNESS_SYMBOLS = { BTC: 'BTCUSD', XAU: 'XAUUSD' };
const INTERNAL_FROM_EXNESS = { BTCUSD: 'BTC', XAUUSD: 'XAU' };

// ===== TIMEFRAME MAPPING (internal ↔ Exness) =====
// internal keys: '5m', '15m', '1h', '1d'
// Exness keys:   'M5', 'M15', 'H1', 'D1'
const TF_TO_EXNESS = { '5m': 'M5', '15m': 'M15', '1h': 'H1', '1d': 'D1' };

// ===== MAX CANDLE ARRAY LENGTHS =====
const MAX_LEN = { '5m': 700, '15m': 400, '1h': 300, '1d': 5 };

// ===== CANDLE PERIOD IN MS (for onTick bucket alignment) =====
const PERIOD_MS = {
  '5m':  5 * 60 * 1000,    // 300000
  '15m': 15 * 60 * 1000    // 900000
};

// ===== IN-MEMORY CACHE =====
// Structure: cache[symbol] = {
//   '5m':  [],         // 700 max — live-built from WS ticks
//   '15m': [],         // 400 max — live-built from WS ticks
//   '1h':  [],         // 300 max — REST only, refreshed hourly
//   '1d':  [],         // 5 max   — REST only, refreshed every 2h
//   daily: null,       // { pdh, pdl } from yesterday's D1 candle
//   lastBootstrap: 0,  // ms timestamp of last successful bootstrap
//   price: null,       // current bid price (updated on every tick)
//   bid: null,
//   ask: null,
//   lastTickAt: 0      // ms timestamp of last tick received
// }
const cache = {};
SYMBOLS.forEach((s) => {
  cache[s] = {
    '5m': [], '15m': [], '1h': [], '1d': [],
    daily: null,
    lastBootstrap: 0,
    price: null, bid: null, ask: null,
    lastTickAt: 0
  };
});

// ===== CALL COUNTER (for monitoring) =====
let callCount = 0;
let callCountByType = { bootstrap: 0, refresh1h: 0, refresh1d: 0, retry: 0, failed: 0 };

function getCallCount() {
  // Pull rate stats from exness-api so callers see one unified view
  const rate = exnessAPI.getRateStats ? exnessAPI.getRateStats() : null;
  return {
    total: callCount,
    byType: callCountByType,
    exnessRateStats: rate
  };
}

// ===== CONVERT EXNESS REST CANDLE → INTERNAL FORMAT =====
// Exness: { open, high, low, close, time, volume }    time = ISO8601 string
// Internal: { t, o, h, l, c, v }                       t = ms epoch
// No offset applied — Exness IS the broker, prices are already correct.
function toInternalCandle(c) {
  if (!c || !c.time) return null;
  const t = new Date(c.time).getTime();
  if (!isFinite(t)) return null;
  return {
    t: t,
    o: parseFloat(c.open),
    h: parseFloat(c.high),
    l: parseFloat(c.low),
    c: parseFloat(c.close),
    v: parseFloat(c.volume || 0)
  };
}

// ===== FETCH ONE TIMEFRAME FROM EXNESS REST =====
// Returns array of internal-format candles, oldest→newest.
// Exness returns candles oldest→newest already (time ascending).
async function fetchCandles(symbol, timeframe, count) {
  const exnessSym = EXNESS_SYMBOLS[symbol];
  const exnessTf = TF_TO_EXNESS[timeframe];
  if (!exnessSym || !exnessTf) {
    console.error('[MarketData] Bad symbol/timeframe:', symbol, timeframe);
    return [];
  }
  const resp = await exnessAPI.getCandles(exnessSym, exnessTf, count);
  if (!resp || !Array.isArray(resp.candles) || resp.candles.length === 0) {
    return [];
  }
  const out = [];
  for (const c of resp.candles) {
    const ic = toInternalCandle(c);
    if (ic) out.push(ic);
  }
  // De-duplicate by `t` (Exness occasionally returns overlapping candles at the boundary)
  // and ensure ascending order
  out.sort((a, b) => a.t - b.t);
  const dedup = [];
  for (const c of out) {
    const last = dedup[dedup.length - 1];
    if (!last || last.t !== c.t) dedup.push(c);
  }
  return dedup;
}

// ===== BOOTSTRAP ONE INSTRUMENT (fetch all 4 timeframes from Exness REST) =====
// Staggered with 600ms gaps (handled inside exnessAPI.getCandles) to respect 2 req/s.
async function bootstrapSymbol(symbol) {
  // Throttle re-bootstraps to once per 5 minutes
  if (cache[symbol].lastBootstrap && Date.now() - cache[symbol].lastBootstrap < 300000) {
    console.log('[MarketData]', symbol, 'already bootstrapped recently, skipping');
    return;
  }
  console.log('[MarketData] Bootstrapping', symbol,
    '(' + EXNESS_SYMBOLS[symbol] + ') — fetching M5, M15, H1, D1 from Exness REST...');

  try {
    // Fetch all 4 timeframes. exnessAPI.getCandles enforces 600ms gap between calls.
    const c5m  = await fetchCandles(symbol, '5m',  MAX_LEN['5m']);
    callCount++; callCountByType.bootstrap++;
    const c15m = await fetchCandles(symbol, '15m', MAX_LEN['15m']);
    callCount++; callCountByType.bootstrap++;
    const c1h  = await fetchCandles(symbol, '1h',  MAX_LEN['1h']);
    callCount++; callCountByType.bootstrap++;
    const c1d  = await fetchCandles(symbol, '1d',  MAX_LEN['1d']);
    callCount++; callCountByType.bootstrap++;

    if (c5m.length)   cache[symbol]['5m']  = c5m.slice(-MAX_LEN['5m']);
    if (c15m.length)  cache[symbol]['15m'] = c15m.slice(-MAX_LEN['15m']);
    if (c1h.length)   cache[symbol]['1h']  = c1h.slice(-MAX_LEN['1h']);
    if (c1d.length)   cache[symbol]['1d']  = c1d.slice(-MAX_LEN['1d']);

    // Compute PDH/PDL from yesterday's D1 candle (second-to-last)
    if (c1d.length >= 2) {
      const yesterday = c1d[c1d.length - 2];
      cache[symbol].daily = { pdh: yesterday.h, pdl: yesterday.l };
    } else if (c1d.length === 1) {
      // Edge case: only one D1 candle returned — use it as a fallback
      cache[symbol].daily = { pdh: c1d[0].h, pdl: c1d[0].l };
    }

    cache[symbol].lastBootstrap = Date.now();

    // If we don't yet have a live price (WS not connected), seed from the last 5m close
    if (cache[symbol].price == null && c5m.length > 0) {
      const last = c5m[c5m.length - 1];
      cache[symbol].price = last.c;
      cache[symbol].bid = last.c;
      cache[symbol].ask = last.c;
    }

    console.log('[MarketData]', symbol, 'bootstrap OK —',
      '5m:' + cache[symbol]['5m'].length,
      '15m:' + cache[symbol]['15m'].length,
      '1h:' + cache[symbol]['1h'].length,
      '1d:' + cache[symbol]['1d'].length,
      'PDH:' + (cache[symbol].daily ? cache[symbol].daily.pdh : 'n/a'),
      'PDL:' + (cache[symbol].daily ? cache[symbol].daily.pdl : 'n/a'));
  } catch (err) {
    callCountByType.failed++;
    console.error('[MarketData]', symbol, 'bootstrap FAILED:', err.message);
  }
}

// ===== BOOTSTRAP ALL INSTRUMENTS (staggered) =====
// No offset parameter — Exness IS the broker, no offset needed.
async function bootstrapAll() {
  console.log('[MarketData] Bootstrapping all instruments from Exness REST...');
  for (const symbol of SYMBOLS) {
    await bootstrapSymbol(symbol);
    // Small buffer between instruments (600ms gap is already enforced inside getCandles)
    await new Promise((r) => setTimeout(r, 800));
  }
  console.log('[MarketData] All instruments bootstrapped. Total REST calls so far:', callCount);
}

// ===== REFRESH 1H FOR ONE INSTRUMENT (called at :00:05 each hour) =====
async function refresh1h(symbol) {
  try {
    const fresh = await fetchCandles(symbol, '1h', MAX_LEN['1h']);
    callCount++; callCountByType.refresh1h++;
    if (fresh.length === 0) {
      console.warn('[MarketData]', symbol, '1h refresh returned no candles');
      return;
    }
    // Only update if the cache's last 1h candle is older than the fresh batch's last
    const oldLast = cache[symbol]['1h'][cache[symbol]['1h'].length - 1];
    const newLast = fresh[fresh.length - 1];
    if (!oldLast || (newLast && newLast.t > oldLast.t)) {
      cache[symbol]['1h'] = fresh.slice(-MAX_LEN['1h']);
      console.log('[MarketData]', symbol, '1h refresh OK —', fresh.length, 'candles');
    }
  } catch (err) {
    callCountByType.failed++;
    console.error('[MarketData]', symbol, '1h refresh failed:', err.message);
  }
}

// ===== REFRESH 1D FOR ONE INSTRUMENT (called every 2 hours) =====
async function refresh1d(symbol) {
  try {
    const fresh = await fetchCandles(symbol, '1d', MAX_LEN['1d']);
    callCount++; callCountByType.refresh1d++;
    if (fresh.length === 0) {
      console.warn('[MarketData]', symbol, '1d refresh returned no candles');
      return;
    }
    cache[symbol]['1d'] = fresh.slice(-MAX_LEN['1d']);
    // Recompute PDH/PDL from yesterday's D1 candle
    if (fresh.length >= 2) {
      const yesterday = fresh[fresh.length - 2];
      const newPdh = yesterday.h;
      const newPdl = yesterday.l;
      // Only log if PDH/PDL actually changed
      if (!cache[symbol].daily ||
          cache[symbol].daily.pdh !== newPdh ||
          cache[symbol].daily.pdl !== newPdl) {
        cache[symbol].daily = { pdh: newPdh, pdl: newPdl };
        console.log('[MarketData]', symbol, '1d refresh — PDH=' + newPdh + ', PDL=' + newPdl);
      }
    }
  } catch (err) {
    callCountByType.failed++;
    console.error('[MarketData]', symbol, '1d refresh failed:', err.message);
  }
}

// ===== SCHEDULED REFRESH JOBS =====
let hourlyTimer = null;
let dailyTimer = null;

function startScheduledRefresh() {
  // 1h refresh: run at :00:05 every hour (when 1h candle closes + 5s buffer for Exness to publish)
  function scheduleNext1h() {
    const now = new Date();
    const next = new Date(now);
    next.setUTCMinutes(0, 5, 0); // top of next hour + 5 seconds
    if (next <= now) next.setUTCHours(next.getUTCHours() + 1);
    const delay = next - now;
    hourlyTimer = setTimeout(async () => {
      console.log('[MarketData] Hourly 1h refresh for all instruments...');
      for (const symbol of SYMBOLS) {
        await refresh1h(symbol);
        // 800ms buffer between instruments (600ms gap already enforced inside getCandles)
        await new Promise((r) => setTimeout(r, 800));
      }
      scheduleNext1h();
    }, delay);
    if (hourlyTimer.unref) hourlyTimer.unref();
    console.log('[MarketData] Next 1h refresh scheduled in ' + Math.round(delay / 1000) + 's');
  }

  // 1d refresh: every 2 hours
  function scheduleNext1d() {
    const delay = 2 * 60 * 60 * 1000; // 2 hours
    dailyTimer = setTimeout(async () => {
      console.log('[MarketData] Bi-hourly 1d refresh for all instruments...');
      for (const symbol of SYMBOLS) {
        await refresh1d(symbol);
        await new Promise((r) => setTimeout(r, 800));
      }
      scheduleNext1d();
    }, delay);
    if (dailyTimer.unref) dailyTimer.unref();
  }

  scheduleNext1h();
  scheduleNext1d();
  console.log('[MarketData] Scheduled refresh jobs started (1h: hourly at :00:05, 1d: every 2h)');
}

function stopScheduledRefresh() {
  if (hourlyTimer) { clearTimeout(hourlyTimer); hourlyTimer = null; }
  if (dailyTimer)  { clearTimeout(dailyTimer);  dailyTimer  = null; }
}

// ===== PUBLIC API — engines read from here =====

// Get cached candles for a symbol + timeframe.
// symbol: 'BTC' or 'XAU' (internal naming, NOT 'BTCUSD')
// timeframe: '5m', '15m', '1h', '1d'
// Returns empty array if not yet bootstrapped.
function getCandles(symbol, timeframe) {
  if (!cache[symbol]) return [];
  return cache[symbol][timeframe] || [];
}

// Get cached daily PDH/PDL for a symbol.
// Returns null if not yet bootstrapped.
function getDaily(symbol) {
  return cache[symbol] ? cache[symbol].daily : null;
}

// Get cache stats for monitoring.
function getStats() {
  const stats = {};
  SYMBOLS.forEach((s) => {
    stats[s] = {
      '5m':  cache[s]['5m'].length,
      '15m': cache[s]['15m'].length,
      '1h':  cache[s]['1h'].length,
      '1d':  cache[s]['1d'].length,
      daily: cache[s].daily ? { pdh: cache[s].daily.pdh, pdl: cache[s].daily.pdl } : null,
      lastBootstrap: cache[s].lastBootstrap ? new Date(cache[s].lastBootstrap).toISOString() : null,
      price: cache[s].price,
      bid:   cache[s].bid,
      ask:   cache[s].ask,
      lastTickAt: cache[s].lastTickAt ? new Date(cache[s].lastTickAt).toISOString() : null
    };
  });
  return stats;
}

// ===== WEBSOCKET-DRIVEN CANDLE BUILDER =====
// Called by exness-api.js WS handler on every tick.
// Updates the cache:
//   - price / bid / ask / lastTickAt
//   - 5m candle array (live-built from ticks)
//   - 15m candle array (live-built from ticks)
//
// Period alignment (per spec):
//   5m:  Math.floor(tickTime / 300000) * 300000
//   15m: Math.floor(tickTime / 900000) * 900000
//
// On new candle period: push new candle, shift old if > maxLen
// On same period: update high/low/close
//
// Returns an event object so server.js can react to candle closes:
//   { closed5m: candle|null, forming5m: candle|null, closed15m: candle|null }
// Each candle is in internal format {t, o, h, l, c, v}. The closed candles are the
// final, frozen state of the candle that just finished (used by CL's handleKline).
function onTick(instrument, bid, ask, timestamp) {
  // Map Exness instrument → internal symbol
  const symbol = INTERNAL_FROM_EXNESS[instrument];
  if (!symbol || !cache[symbol]) return { closed5m: null, forming5m: null, closed15m: null };

  // Parse tick time. Exness sends ISO8601 string. Fall back to Date.now() if missing/invalid.
  let tickMs;
  if (timestamp) {
    tickMs = new Date(timestamp).getTime();
    if (!isFinite(tickMs)) tickMs = Date.now();
  } else {
    tickMs = Date.now();
  }

  // Update current price (use bid — matches what engines trade against)
  cache[symbol].price = bid;
  cache[symbol].bid = bid;
  cache[symbol].ask = ask;
  cache[symbol].lastTickAt = tickMs;

  let closed5m = null;
  let closed15m = null;

  // === 5m candle update ===
  closed5m = _updateTfCandle(symbol, '5m', tickMs, bid, ask);

  // === 15m candle update (uses bid only — 15m candle is OHLC of bid) ===
  closed15m = _updateTfCandle(symbol, '15m', tickMs, bid, ask);

  // Current forming 5m candle (for CL's handleKline on forming ticks)
  const forming5m = cache[symbol]['5m'].length > 0
    ? cache[symbol]['5m'][cache[symbol]['5m'].length - 1]
    : null;

  return { closed5m: closed5m, forming5m: forming5m, closed15m: closed15m };
}

// Internal helper — update one timeframe's candle array from a tick.
// Returns the just-closed candle (frozen) if a period transition happened, else null.
function _updateTfCandle(symbol, tf, tickMs, bid, ask) {
  const arr = cache[symbol][tf];
  if (!arr) return null;
  const periodMs = PERIOD_MS[tf];
  if (!periodMs) return null;

  const periodStart = Math.floor(tickMs / periodMs) * periodMs;
  const last = arr[arr.length - 1];
  let closedCandle = null;

  if (!last || last.t !== periodStart) {
    // New candle period started.
    // If we had a previous forming candle, freeze it as the "closed" candle.
    // (This happens when ticks resume after a gap, or naturally at period boundary.)
    if (last && last.t < periodStart) {
      // The previous candle is now closed — but we already had it in `arr`.
      // We expose it as the closed event so subscribers (CL's handleKline) can react.
      closedCandle = { t: last.t, o: last.o, h: last.h, l: last.l, c: last.c, v: last.v };
    }
    // Push the new candle, opened at the bid price
    arr.push({
      t: periodStart,
      o: bid,
      h: bid,
      l: bid,
      c: bid,
      v: 0
    });
    const maxLen = MAX_LEN[tf];
    if (arr.length > maxLen) arr.shift();
  } else {
    // Same period — update high/low/close
    if (bid > last.h) last.h = bid;
    if (bid < last.l) last.l = bid;
    last.c = bid;
    // Volume is tick count (Exness WS doesn't provide real volume per tick)
    last.v = (last.v || 0) + 1;
  }
  return closedCandle;
}

module.exports = {
  // Bootstrap + scheduled refresh
  bootstrapAll,
  bootstrapSymbol,
  startScheduledRefresh,
  stopScheduledRefresh,
  // Public read API (used by smc-engine.js, smc-engine-sniper.js, smc-engine-cl.js)
  getCandles,
  getDaily,
  getStats,
  getCallCount,
  // WebSocket tick handler (called by exness-api.js → server.js)
  onTick,
  // Constants
  SYMBOLS,
  EXNESS_SYMBOLS
};
