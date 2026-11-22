// market-data.js — Shared data layer for all engines (V2, Sniper, CL)
//
// PROBLEM:
//   Each engine was making its own TwelveData REST calls independently.
//   V2 needed 15m + 1d, Sniper needed 15m, CL needed 5m + 1h + 1d.
//   Total: ~2,496 REST calls/day → way over TwelveData's 800/day free tier limit.
//
// SOLUTION:
//   Single shared cache. Each timeframe per instrument is fetched ONCE.
//   - 5m and 15m come from WebSockets (free, unlimited, real-time)
//   - 1h is refreshed at :00:05 every hour (when a new 1h candle actually closes)
//   - 1d is refreshed every 2 hours (PDH/PDL only change at UTC midnight)
//
// ESTIMATED DAILY CALLS:
//   - Startup: 4 instruments × 4 timeframes = 16 calls
//   - 1h refresh: 4 instruments × 24 hours = 96 calls
//   - 1d refresh: 4 instruments × 12 (every 2h) = 48 calls
//   - TOTAL: ~160 calls/day = 20% of free tier ✅
//
// ENGINES USING THIS LAYER:
//   - V2:    reads 15m candles + 1d PDH/PDL
//   - Sniper: reads 15m candles (same as V2)
//   - CL:    reads 5m candles + 1h candles + 1d PDH/PDL
//
// All engines READ from this cache. They don't fetch their own data.

'use strict';

const TWELVEDATA_KEY = process.env.TWELVEDATA_KEY || '2577565126b44bf2bdff2d25e5f957f0';
const TWELVEDATA_SYMBOLS = { BTC: 'BTC/USD', XAU: 'XAU/USD' };
// EUR removed — Binance EURUSDT diverges 100+ pips from Exness EURUSD.
// XAG removed — TwelveData free tier doesn't support XAG/USD.

// ===== IN-MEMORY CACHE =====
// Structure: cache[symbol][timeframe] = array of {t, o, h, l, c, v}
// Timeframes: '5m', '15m', '1h', '1d'
// Daily also stored as cache[symbol].daily = { pdh, pdl } for convenience
const cache = {};
const SYMBOLS = ['BTC', 'XAU'];
// EUR + XAG removed — see TWELVEDATA_SYMBOLS comment above.
SYMBOLS.forEach(s => {
  cache[s] = { '5m': [], '15m': [], '1h': [], '1d': [], daily: null, lastBootstrap: 0 };
});

// ===== CALL COUNTER (for monitoring) =====
let callCount = 0;
let callCountByType = { bootstrap: 0, refresh1h: 0, refresh1d: 0, retry: 0 };

function getCallCount() {
  return { total: callCount, byType: callCountByType };
}

// ===== TWELVEDATA FETCH (with retry on 429) =====
async function fetchTwelveData(tdSymbol, interval, outputsize, callType = 'bootstrap') {
  const url = `https://api.twelvedata.com/time_series?symbol=${tdSymbol}&interval=${interval}&outputsize=${outputsize}&apikey=${TWELVEDATA_KEY}`;
  let retries = 3;
  while (retries > 0) {
    callCount++;
    callCountByType[callType] = (callCountByType[callType] || 0) + 1;
    try {
      const r = await fetch(url);
      if (r.status === 429) {
        console.warn(`[MarketData] TwelveData rate limited (${callType} ${tdSymbol} ${interval}), waiting 60s... (${retries} retries left)`);
        callCountByType.retry++;
        await new Promise(resolve => setTimeout(resolve, 60000));
        retries--;
        continue;
      }
      if (!r.ok) throw new Error('TwelveData HTTP ' + r.status);
      const data = await r.json();
      if (data.status === 'error' || data.code === 429) {
        throw new Error('TwelveData: ' + (data.message || 'rate limited'));
      }
      return data;
    } catch (err) {
      if (retries <= 1) throw err;
      console.warn(`[MarketData] ${callType} ${tdSymbol} ${interval} error: ${err.message}, retrying...`);
      await new Promise(resolve => setTimeout(resolve, 5000));
      retries--;
    }
  }
  throw new Error('TwelveData: exhausted retries');
}

// ===== CONVERT TWELVEDATA RESPONSE TO CANDLE ARRAY =====
// TwelveData returns newest first; we reverse to oldest first.
function toCandles(data, offset) {
  if (!data || !data.values) return [];
  return data.values.slice().reverse().map(v => ({
    t: new Date(v.datetime).getTime(),
    o: parseFloat(v.open) + offset,
    h: parseFloat(v.high) + offset,
    l: parseFloat(v.low) + offset,
    c: parseFloat(v.close) + offset,
    v: parseFloat(v.volume || 0)
  }));
}

// ===== BOOTSTRAP ONE INSTRUMENT (fetch all 4 timeframes) =====
// Stagger 8s between calls to respect TwelveData 8 calls/min limit.
async function bootstrapSymbol(symbol, offset) {
  if (cache[symbol].lastBootstrap && Date.now() - cache[symbol].lastBootstrap < 300000) {
    console.log(`[MarketData] ${symbol} already bootstrapped recently, skipping`);
    return;
  }
  const tdSym = TWELVEDATA_SYMBOLS[symbol];
  if (!tdSym) {
    console.error(`[MarketData] No TwelveData symbol for ${symbol}`);
    return;
  }
  console.log(`[MarketData] Bootstrapping ${symbol} (${tdSym}) — fetching 5m, 15m, 1h, 1d...`);
  try {
    // Fetch all 4 timeframes with 8s stagger
    const data5m = await fetchTwelveData(tdSym, '5min', 700, 'bootstrap');
    await new Promise(r => setTimeout(r, 8000));
    const data15m = await fetchTwelveData(tdSym, '15min', 400, 'bootstrap');
    await new Promise(r => setTimeout(r, 8000));
    const data1h = await fetchTwelveData(tdSym, '1h', 300, 'bootstrap');
    await new Promise(r => setTimeout(r, 8000));
    const data1d = await fetchTwelveData(tdSym, '1day', 5, 'bootstrap');

    cache[symbol]['5m'] = toCandles(data5m, offset);
    cache[symbol]['15m'] = toCandles(data15m, offset);
    cache[symbol]['1h'] = toCandles(data1h, offset);

    // 1d — extract yesterday's high/low for PDH/PDL
    const dailyCandles = toCandles(data1d, offset);
    cache[symbol]['1d'] = dailyCandles;
    if (dailyCandles.length >= 2) {
      const yesterday = dailyCandles[dailyCandles.length - 2];
      cache[symbol].daily = { pdh: yesterday.h, pdl: yesterday.l };
    }

    cache[symbol].lastBootstrap = Date.now();
    console.log(`[MarketData] ${symbol} bootstrap OK — 5m:${cache[symbol]['5m'].length}, 15m:${cache[symbol]['15m'].length}, 1h:${cache[symbol]['1h'].length}, 1d:${cache[symbol]['1d'].length}, PDH:${cache[symbol].daily?.pdh}, PDL:${cache[symbol].daily?.pdl}`);
  } catch (err) {
    console.error(`[MarketData] ${symbol} bootstrap FAILED:`, err.message);
  }
}

// ===== BOOTSTRAP ALL INSTRUMENTS (staggered) =====
async function bootstrapAll(offsetMap) {
  console.log(`[MarketData] Bootstrapping all instruments...`);
  for (const symbol of SYMBOLS) {
    const offset = offsetMap[symbol] || 0;
    await bootstrapSymbol(symbol, offset);
    // 15s between instruments (4 calls × 8s stagger + buffer = ~40s per instrument, well under 8/min)
    await new Promise(r => setTimeout(r, 15000));
  }
  console.log(`[MarketData] All instruments bootstrapped. Total calls so far: ${callCount}`);
}

// ===== REFRESH 1H FOR ONE INSTRUMENT (called at :00:05 each hour) =====
async function refresh1h(symbol, offset) {
  const tdSym = TWELVEDATA_SYMBOLS[symbol];
  if (!tdSym) return;
  try {
    const data = await fetchTwelveData(tdSym, '1h', 100, 'refresh1h');
    const newCandles = toCandles(data, offset);
    if (newCandles.length > 0) {
      // Only update if we got new data
      const oldLast = cache[symbol]['1h'][cache[symbol]['1h'].length - 1];
      const newLast = newCandles[newCandles.length - 1];
      if (!oldLast || newLast.t > oldLast.t) {
        cache[symbol]['1h'] = newCandles;
      }
    }
  } catch (err) {
    console.error(`[MarketData] ${symbol} 1h refresh failed:`, err.message);
  }
}

// ===== REFRESH 1D FOR ONE INSTRUMENT (called every 2 hours) =====
async function refresh1d(symbol, offset) {
  const tdSym = TWELVEDATA_SYMBOLS[symbol];
  if (!tdSym) return;
  try {
    const data = await fetchTwelveData(tdSym, '1day', 5, 'refresh1d');
    const dailyCandles = toCandles(data, offset);
    if (dailyCandles.length >= 2) {
      const yesterday = dailyCandles[dailyCandles.length - 2];
      const newPdh = yesterday.h;
      const newPdl = yesterday.l;
      // Only update + log if PDH/PDL actually changed
      if (!cache[symbol].daily || cache[symbol].daily.pdh !== newPdh || cache[symbol].daily.pdl !== newPdl) {
        cache[symbol].daily = { pdh: newPdh, pdl: newPdl };
        cache[symbol]['1d'] = dailyCandles;
        console.log(`[MarketData] ${symbol} 1d refresh — PDH=${newPdh}, PDL=${newPdl}`);
      }
    }
  } catch (err) {
    console.error(`[MarketData] ${symbol} 1d refresh failed:`, err.message);
  }
}

// ===== SCHEDULED REFRESH JOBS =====
let hourlyTimer = null;
let dailyTimer = null;

function startScheduledRefresh(offsetMap) {
  // 1h refresh: run at :00:05 every hour (when 1h candle closes + 5s buffer)
  // Use 60-min interval, aligned to top of hour
  function scheduleNext1h() {
    const now = new Date();
    const next = new Date(now);
    next.setUTCMinutes(0, 5, 0); // top of next hour + 5 seconds
    if (next <= now) next.setUTCHours(next.getUTCHours() + 1);
    const delay = next - now;
    hourlyTimer = setTimeout(async () => {
      console.log(`[MarketData] Hourly 1h refresh for all instruments...`);
      for (const symbol of SYMBOLS) {
        await refresh1h(symbol, offsetMap[symbol] || 0);
        await new Promise(r => setTimeout(r, 8000)); // 8s stagger
      }
      scheduleNext1h();
    }, delay);
    console.log(`[MarketData] Next 1h refresh scheduled in ${Math.round(delay / 1000)}s`);
  }

  // 1d refresh: every 2 hours
  function scheduleNext1d() {
    const delay = 2 * 60 * 60 * 1000; // 2 hours
    dailyTimer = setTimeout(async () => {
      console.log(`[MarketData] Bi-hourly 1d refresh for all instruments...`);
      for (const symbol of SYMBOLS) {
        await refresh1d(symbol, offsetMap[symbol] || 0);
        await new Promise(r => setTimeout(r, 8000)); // 8s stagger
      }
      scheduleNext1d();
    }, delay);
  }

  scheduleNext1h();
  scheduleNext1d();
  console.log(`[MarketData] Scheduled refresh jobs started (1h: hourly at :00:05, 1d: every 2h)`);
}

function stopScheduledRefresh() {
  if (hourlyTimer) clearTimeout(hourlyTimer);
  if (dailyTimer) clearTimeout(dailyTimer);
}

// ===== PUBLIC API — engines read from here =====

// Get cached candles for a symbol + timeframe
// Returns empty array if not yet bootstrapped
function getCandles(symbol, timeframe) {
  return cache[symbol] ? (cache[symbol][timeframe] || []) : [];
}

// Get cached daily PDH/PDL for a symbol
// Returns null if not yet bootstrapped
function getDaily(symbol) {
  return cache[symbol] ? cache[symbol].daily : null;
}

// Get cache stats for monitoring
function getStats() {
  const stats = {};
  SYMBOLS.forEach(s => {
    stats[s] = {
      '5m': cache[s]['5m'].length,
      '15m': cache[s]['15m'].length,
      '1h': cache[s]['1h'].length,
      '1d': cache[s]['1d'].length,
      daily: cache[s].daily ? { pdh: cache[s].daily.pdh, pdl: cache[s].daily.pdl } : null,
      lastBootstrap: cache[s].lastBootstrap ? new Date(cache[s].lastBootstrap).toISOString() : null
    };
  });
  return stats;
}

// ===== WEBSOCKET-DRIVEN UPDATES =====
// Called by server.js when a 5m or 15m kline message arrives.
// Updates the cache in real-time (no REST cost).
// kline: { t, o, h, l, c, v, x } where x = isClosed
function updateFromWebSocket(symbol, timeframe, kline, offset) {
  if (!cache[symbol]) return;
  const arr = cache[symbol][timeframe];
  if (!arr) return;

  const candleKey = Math.floor(kline.t / 60000 / (timeframe === '5m' ? 5 : 15));
  const t = kline.t;
  const o = parseFloat(kline.o) + offset;
  const h = parseFloat(kline.h) + offset;
  const l = parseFloat(kline.l) + offset;
  const c = parseFloat(kline.c) + offset;
  const v = parseFloat(kline.v || 0);

  const last = arr[arr.length - 1];
  if (!last || last.t !== t) {
    // New candle — add it
    arr.push({ t, o, h, l, c, v });
    const maxLen = timeframe === '5m' ? 700 : 400;
    if (arr.length > maxLen) arr.shift();
  } else {
    // Update existing candle
    last.h = Math.max(last.h, h);
    last.l = Math.min(last.l, l);
    last.c = c;
    last.v = v;
  }
}

module.exports = {
  bootstrapAll,
  bootstrapSymbol,
  startScheduledRefresh,
  stopScheduledRefresh,
  getCandles,
  getDaily,
  getStats,
  getCallCount,
  updateFromWebSocket,
  SYMBOLS,
  TWELVEDATA_SYMBOLS
};
