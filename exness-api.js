// exness-api.js — NATIVE EXNESS API integration (replaces Binance + TwelveData)
//
// AUTHENTICATION: Ed25519 signed requests with EXN-* headers
//   - timestamp and sign_version must be INTEGERS in the JSON payload (not strings)
//   - signed `path` MUST exactly match the transmitted URL path (use URLSearchParams)
//
// REST ENDPOINTS:
//   - GET /v1/trading/access-point?account_id={id}        → discover account host
//   - GET /v1/accounts/{id}/balance                        → health check
//   - GET /v1/configuration/accounts/{id}/instruments      → list (BTCUSD, XAUUSD)
//   - GET /v1/market-data/accounts/{id}/candles             → historical candles
//       params: instrument, timeframe (M5/M15/H1/D1), from (ISO8601), count, price_type=bid
//
// WEBSOCKET (real-time ticks):
//   - URL: wss://{host}/v1/server-events/accounts/{id}/ws/ticks
//   - Subprotocol: 'exness-ws-protocol' (MUST be specified or handshake fails)
//   - Sign the WS handshake as a GET request (same EXN-* headers)
//   - After connect, send: {"id":"market-data-1","subscribe":{"event":"ticks","instruments":["BTCUSD","XAUUSD"]}}
//   - First response: {"id":"market-data-1","code":200} (subscription confirmed)
//   - Tick stream: {"instrument":"BTCUSD","bid":61675.64,"ask":61682.04,"timestamp":"2026-11-27T08:46:12.661Z"}
//   - Updates throttled to ~500ms per instrument
//   - Auto-reconnect with exponential backoff (1s → 2s → 4s → ... → 30s max)
//
// RATE LIMITS:
//   - REST candle endpoint: 2 requests/second (600ms gap enforced)
//   - Global REST: 15 requests/second
//   - WebSocket: no rate limit (persistent connection)
//
// ENV VARS:
//   EXNESS_API_KEY     = exnsk-...
//   EXNESS_SECRET_KEY  = <128-char hex> (first 32 bytes = Ed25519 private key seed, last 32 = public key)
//   EXNESS_ACCOUNT_ID  = 31000017203

'use strict';

const crypto = require('crypto');
const https = require('https');
const WebSocket = require('ws');

const EXNESS_API_KEY = process.env.EXNESS_API_KEY || '';
const EXNESS_SECRET_KEY = process.env.EXNESS_SECRET_KEY || '';
const EXNESS_ACCOUNT_ID = process.env.EXNESS_ACCOUNT_ID || '';
const DISCOVERY_HOST = 'api.exness-api.com';

// Tick subscription id — Exness echoes this back in the subscription confirmation
const SUB_ID = 'market-data-1';
// Instruments we subscribe to for real-time ticks (Exness naming)
const SUB_INSTRUMENTS = ['BTCUSD', 'XAUUSD'];

// ===== RATE LIMIT / HEALTH STATS =====
let rateStats = {
  restCalls: 0,
  restCallsByEndpoint: { balance: 0, instruments: 0, candles: 0, accessPoint: 0 },
  wsConnects: 0,
  wsDisconnects: 0,
  wsErrors: 0,
  ticksReceived: 0,
  ticksByInstrument: { BTCUSD: 0, XAUUSD: 0 },
  lastResetAt: Date.now()
};

function getRateStats() {
  // Reset counters every hour
  if (Date.now() - rateStats.lastResetAt > 3600000) {
    rateStats.restCalls = 0;
    rateStats.restCallsByEndpoint = { balance: 0, instruments: 0, candles: 0, accessPoint: 0 };
    rateStats.wsConnects = 0;
    rateStats.wsDisconnects = 0;
    rateStats.wsErrors = 0;
    rateStats.ticksReceived = 0;
    rateStats.ticksByInstrument = { BTCUSD: 0, XAUUSD: 0 };
    rateStats.lastResetAt = Date.now();
  }
  return {
    restCalls: rateStats.restCalls,
    restCallsByEndpoint: { ...rateStats.restCallsByEndpoint },
    wsConnects: rateStats.wsConnects,
    wsDisconnects: rateStats.wsDisconnects,
    wsErrors: rateStats.wsErrors,
    ticksReceived: rateStats.ticksReceived,
    ticksByInstrument: { ...rateStats.ticksByInstrument },
    wsConnected: _ws && _ws.readyState === WebSocket.OPEN,
    cachedAccessPoint: cachedAccessPoint,
    uptimeMin: Math.round(process.uptime() / 60)
  };
}

// ===== CREATE ED25519 PRIVATE KEY FROM SECRET =====
// The secret key is 128 hex chars (64 bytes):
//   - First 32 bytes = Ed25519 private key seed
//   - Last 32 bytes = Ed25519 public key (verified match — present but not needed for signing)
let _privateKey = null;
let _keyError = null;

function getPrivateKey() {
  if (_privateKey) return _privateKey;
  if (_keyError) return null;
  if (!EXNESS_SECRET_KEY) {
    _keyError = 'EXNESS_SECRET_KEY not set';
    return null;
  }
  const secretBuf = Buffer.from(EXNESS_SECRET_KEY, 'hex');
  if (secretBuf.length !== 64) {
    _keyError = `EXNESS_SECRET_KEY must be 128 hex chars (64 bytes), got ${secretBuf.length} bytes`;
    console.error('[ExnessAPI]', _keyError);
    return null;
  }
  // PKCS#8 DER prefix for Ed25519 (16 bytes), then the 32-byte seed → valid PKCS#8 Ed25519 private key
  const pkcs8Prefix = Buffer.from('302e020100300506032b657004220420', 'hex');
  try {
    _privateKey = crypto.createPrivateKey({
      key: Buffer.concat([pkcs8Prefix, secretBuf.slice(0, 32)]),
      format: 'der',
      type: 'pkcs8'
    });
    return _privateKey;
  } catch (err) {
    _keyError = `Failed to load Ed25519 key: ${err.message}`;
    console.error('[ExnessAPI]', _keyError);
    return null;
  }
}

// ===== SIGNED REQUEST BUILDER =====
// Builds EXN-* headers for a given HTTP method + full path (with encoded query string).
// CRITICAL: timestamp and sign_version must be INTEGERS in the JSON payload (not strings).
// The signed `path` MUST exactly match the transmitted URL path.
//   - Use URLSearchParams for query string encoding (deterministic order, correct escaping)
//   - Pass the exact same path string to both buildSignedHeaders and the HTTP request
function buildSignedHeaders(method, fullPath) {
  const privKey = getPrivateKey();
  if (!privKey) throw new Error(_keyError || 'No Ed25519 private key available');

  const timestamp = Date.now();                              // INTEGER, not string
  const bodyHash = crypto.createHash('sha256').update('').digest('base64url');

  // Payload field order matters for signature — keep this exact order
  const payload = {
    api_key: EXNESS_API_KEY,
    idempotency_key: '',
    timestamp: timestamp,                                    // INTEGER
    sign_version: 1,                                         // INTEGER
    method: method,
    path: fullPath,
    body_hash: bodyHash
  };

  const payloadBytes = Buffer.from(JSON.stringify(payload));
  const sign = crypto.sign(null, payloadBytes, privKey);

  return {
    'EXN-API-KEY': EXNESS_API_KEY,
    'EXN-IDEMPOTENCY-KEY': '',
    'EXN-TIMESTAMP': timestamp.toString(),
    'EXN-SIGN-VERSION': '1',
    'EXN-DATA': payloadBytes.toString('base64url'),
    'EXN-SIGN': sign.toString('base64url'),
    'Content-Type': 'application/json'
  };
}

// ===== HTTP REQUEST HELPER (REST GET) =====
function apiGet(host, fullPath, endpointTag, timeoutMs) {
  timeoutMs = timeoutMs || 8000;
  endpointTag = endpointTag || 'other';
  return new Promise((resolve) => {
    let headers;
    try {
      headers = buildSignedHeaders('GET', fullPath);
    } catch (err) {
      resolve({ status: 'error', data: err.message });
      return;
    }
    const req = https.request({
      hostname: host,
      port: 443,
      path: fullPath,
      method: 'GET',
      headers: headers
    }, (res) => {
      let data = '';
      res.on('data', (chunk) => { data += chunk; });
      res.on('end', () => {
        rateStats.restCalls++;
        rateStats.restCallsByEndpoint[endpointTag] =
          (rateStats.restCallsByEndpoint[endpointTag] || 0) + 1;
        try {
          resolve({ status: res.statusCode, data: JSON.parse(data) });
        } catch {
          resolve({ status: res.statusCode, data: data });
        }
      });
    });
    req.on('error', (e) => resolve({ status: 'error', data: e.message }));
    req.setTimeout(timeoutMs, () => {
      req.destroy();
      resolve({ status: 'timeout', data: 'timeout after ' + timeoutMs + 'ms' });
    });
    req.end();
  });
}

// ===== DISCOVER ACCOUNT-SPECIFIC API HOST (cached for 1 hour) =====
let cachedAccessPoint = null;
let lastDiscoveryTime = 0;

async function discoverAccessPoint() {
  // Cache hit
  if (cachedAccessPoint && Date.now() - lastDiscoveryTime < 3600000) {
    return cachedAccessPoint;
  }
  if (!EXNESS_ACCOUNT_ID) {
    console.error('[ExnessAPI] EXNESS_ACCOUNT_ID not set — cannot discover access point');
    return null;
  }
  // Use URLSearchParams for query encoding so signed path matches transmitted path
  const params = new URLSearchParams({ account_id: EXNESS_ACCOUNT_ID });
  const fullPath = '/v1/trading/access-point?' + params.toString();
  const result = await apiGet(DISCOVERY_HOST, fullPath, 'accessPoint');

  if (result.status === 200 && result.data && result.data.access_point) {
    const url = result.data.access_point;
    // Strip scheme + trailing slash → bare hostname for use in HTTPS and WSS
    cachedAccessPoint = String(url)
      .replace(/^https?:\/\//i, '')
      .replace(/\/$/, '');
    lastDiscoveryTime = Date.now();
    console.log('[ExnessAPI] Access point discovered:', cachedAccessPoint);
    return cachedAccessPoint;
  }
  console.error('[ExnessAPI] Failed to discover access point:',
    result.status,
    typeof result.data === 'string' ? result.data.substring(0, 200) : JSON.stringify(result.data || {}).substring(0, 200));
  return null;
}

// ===== ACCOUNT BALANCE (used by healthCheck) =====
async function getBalance() {
  const host = await discoverAccessPoint();
  if (!host) return null;
  const fullPath = '/v1/accounts/' + EXNESS_ACCOUNT_ID + '/balance';
  const result = await apiGet(host, fullPath, 'balance');
  if (result.status === 200) return result.data;
  console.error('[ExnessAPI] Balance failed:', result.status, result.data);
  return null;
}

// ===== INSTRUMENTS LIST =====
async function getInstruments() {
  const host = await discoverAccessPoint();
  if (!host) return null;
  const fullPath = '/v1/configuration/accounts/' + EXNESS_ACCOUNT_ID + '/instruments';
  const result = await apiGet(host, fullPath, 'instruments');
  if (result.status === 200) return result.data;
  console.error('[ExnessAPI] Instruments failed:', result.status, result.data);
  return null;
}

// ===== TIMEFRAME → SECONDS MAP (for `from` calculation) =====
const TF_SECONDS = {
  M1:  60,
  M5:  300,
  M15: 900,
  M30: 1800,
  H1:  3600,
  H4:  14400,
  D1:  86400
};

// ===== RATE LIMIT GATE =====
// Exness REST candle endpoint: max 2 requests/second per connection.
// We enforce a 600ms minimum gap between candle calls (well under the 500ms limit).
let _lastCandleCallAt = 0;
const CANDLE_CALL_MIN_GAP_MS = 600;

function _rateLimitDelay() {
  return new Promise((resolve) => {
    const now = Date.now();
    const elapsed = now - _lastCandleCallAt;
    const wait = Math.max(0, CANDLE_CALL_MIN_GAP_MS - elapsed);
    if (wait > 0) {
      setTimeout(() => {
        _lastCandleCallAt = Date.now();
        resolve();
      }, wait);
    } else {
      _lastCandleCallAt = now;
      resolve();
    }
  });
}

// ===== CANDLE FETCHER =====
// GET /v1/market-data/accounts/{id}/candles
//   ?instrument=BTCUSD
//   &timeframe=M5         (M5, M15, H1, D1)
//   &from={ISO8601}        (calculated from count + timeframe)
//   &count={N}             (1..1000)
//   &price_type=bid
// Returns: { candles: [{ open, high, low, close, time, volume }] }
//
// Exness REST returns candles oldest→newest (time ascending).
// Each candle's `time` is an ISO8601 string with +00:00 offset.
// We pass through the raw Exness response — market-data.js does the format conversion.
async function getCandles(instrument, timeframe, count) {
  const host = await discoverAccessPoint();
  if (!host) return null;
  if (!TF_SECONDS[timeframe]) {
    console.error('[ExnessAPI] Invalid timeframe:', timeframe);
    return null;
  }
  count = Math.max(1, Math.min(1000, count | 0));

  // `from` = now - (count + 5) * tfSeconds, with a 5-candle safety buffer
  const tfSec = TF_SECONDS[timeframe];
  const fromMs = Date.now() - (count + 5) * tfSec * 1000;
  const fromIso = new Date(fromMs).toISOString();

  // CRITICAL: use URLSearchParams so the signed path matches the transmitted path exactly.
  const params = new URLSearchParams({
    instrument: instrument,
    timeframe: timeframe,
    from: fromIso,
    count: String(count),
    price_type: 'bid'
  });
  const fullPath = '/v1/market-data/accounts/' + EXNESS_ACCOUNT_ID + '/candles?' + params.toString();

  // Enforce 600ms gap between candle requests (2 req/s limit)
  await _rateLimitDelay();

  const result = await apiGet(host, fullPath, 'candles');
  if (result.status === 200 && result.data && Array.isArray(result.data.candles)) {
    return result.data;
  }
  console.error('[ExnessAPI] Candles failed for', instrument, timeframe,
    '— status:', result.status,
    '— body:', typeof result.data === 'string'
      ? result.data.substring(0, 200)
      : JSON.stringify(result.data || {}).substring(0, 200));
  return null;
}

// ===== WEBSOCKET TICK STREAM =====
// Connects to wss://{host}/v1/server-events/accounts/{id}/ws/ticks
//   - Subprotocol: 'exness-ws-protocol' (MUST be specified — connection fails without it)
//   - Signs the WS handshake as a GET request with EXN-* headers
//   - After open, sends subscription message for BTCUSD + XAUUSD ticks
//   - Calls onTick(instrument, bid, ask, timestamp) on each tick
//   - Calls onStatus(status, info) on connect/disconnect/error/reconnect
//   - Auto-reconnects with exponential backoff on unexpected close
//
// onStatus values:
//   'connecting'    — about to open WS
//   'open'          — WS handshake completed, subscription sent
//   'subscribed'    — Exness confirmed subscription (code 200)
//   'reconnecting'  — scheduled reconnect after delay
//   'closed'        — WS closed (will auto-reconnect unless intentional)
//   'error'         — WS error
let _ws = null;
let _wsReconnectTimer = null;
let _wsBackoff = 1000;             // start at 1s, max 30s
let _wsIntentionalClose = false;
let _wsOnTick = null;
let _wsOnStatus = null;
let _wsLastTickAt = 0;

function connectTicks(onTick, onStatus) {
  _wsOnTick = onTick || function () {};
  _wsOnStatus = onStatus || function () {};
  _wsIntentionalClose = false;
  _wsBackoff = 1000;
  _connectOnce();
}

function disconnectTicks() {
  _wsIntentionalClose = true;
  if (_wsReconnectTimer) {
    clearTimeout(_wsReconnectTimer);
    _wsReconnectTimer = null;
  }
  if (_ws) {
    try { _ws.close(); } catch (e) {}
    _ws = null;
  }
}

function _emitStatus(status, info) {
  try { _wsOnStatus(status, info); } catch (e) {
    console.error('[ExnessWS] onStatus handler error:', e.message);
  }
}

async function _connectOnce() {
  const host = await discoverAccessPoint();
  if (!host) {
    _emitStatus('error', 'Host discovery failed');
    _scheduleReconnect();
    return;
  }
  if (!EXNESS_ACCOUNT_ID) {
    _emitStatus('error', 'EXNESS_ACCOUNT_ID not set');
    return;
  }

  const wsPath = '/v1/server-events/accounts/' + EXNESS_ACCOUNT_ID + '/ws/ticks';
  const wsUrl = 'wss://' + host + wsPath;

  // Sign the handshake as a GET request — same EXN-* headers as REST.
  // The signed `path` is exactly the WS URL path (no query string for ticks endpoint).
  let headers;
  try {
    headers = buildSignedHeaders('GET', wsPath);
  } catch (err) {
    _emitStatus('error', err.message);
    _scheduleReconnect();
    return;
  }

  _emitStatus('connecting', { url: wsUrl });
  console.log('[ExnessWS] Connecting to', wsUrl, '(subprotocol: exness-ws-protocol)');

  let ws;
  try {
    // CRITICAL: pass 'exness-ws-protocol' as the subprotocol — Exness rejects the handshake without it.
    ws = new WebSocket(wsUrl, ['exness-ws-protocol'], {
      headers: headers,
      perMessageDeflate: false,
      handshakeTimeout: 10000
    });
  } catch (err) {
    _emitStatus('error', 'WS constructor failed: ' + err.message);
    _scheduleReconnect();
    return;
  }
  _ws = ws;
  rateStats.wsConnects++;

  ws.on('open', () => {
    console.log('[ExnessWS] Connected — sending tick subscription for', SUB_INSTRUMENTS.join(', '));
    _emitStatus('open');
    // Reset backoff after successful connect
    _wsBackoff = 1000;
    // Send subscription: {"id":"market-data-1","subscribe":{"event":"ticks","instruments":[...]}}
    const subMsg = {
      id: SUB_ID,
      subscribe: {
        event: 'ticks',
        instruments: SUB_INSTRUMENTS
      }
    };
    try {
      ws.send(JSON.stringify(subMsg));
    } catch (err) {
      console.error('[ExnessWS] Failed to send subscription:', err.message);
    }
  });

  ws.on('message', (raw) => {
    let msg;
    try {
      msg = JSON.parse(raw.toString());
    } catch {
      return; // ignore non-JSON frames
    }
    // Subscription confirmation: {"id":"market-data-1","code":200}
    if (msg.id === SUB_ID && msg.code !== undefined) {
      if (msg.code === 200) {
        console.log('[ExnessWS] Subscription confirmed (code 200) — awaiting tick stream');
        _emitStatus('subscribed', { instruments: SUB_INSTRUMENTS });
      } else {
        console.error('[ExnessWS] Subscription rejected:', JSON.stringify(msg));
        _emitStatus('error', 'Subscription rejected: code=' + msg.code);
      }
      return;
    }
    // Tick frame: {"instrument":"BTCUSD","bid":61675.64,"ask":61682.04,"timestamp":"2026-11-27T08:46:12.661Z"}
    if (msg.instrument && msg.bid !== undefined && msg.ask !== undefined) {
      rateStats.ticksReceived++;
      rateStats.ticksByInstrument[msg.instrument] =
        (rateStats.ticksByInstrument[msg.instrument] || 0) + 1;
      _wsLastTickAt = Date.now();
      const bid = parseFloat(msg.bid);
      const ask = parseFloat(msg.ask);
      // Skip invalid ticks (zero/negative/non-numeric) — protects downstream engines
      if (!isFinite(bid) || !isFinite(ask) || bid <= 0 || ask <= 0) return;
      try {
        _wsOnTick(msg.instrument, bid, ask, msg.timestamp);
      } catch (err) {
        console.error('[ExnessWS] onTick handler error:', err.message);
      }
    }
  });

  ws.on('error', (err) => {
    rateStats.wsErrors++;
    console.error('[ExnessWS] Error:', err.message);
    _emitStatus('error', err.message);
  });

  ws.on('close', (code, reason) => {
    rateStats.wsDisconnects++;
    const reasonStr = reason ? reason.toString() : '';
    console.log('[ExnessWS] Closed (code=' + code +
      (reasonStr ? ', reason=' + reasonStr : '') + ')');
    _emitStatus('closed', { code: code, reason: reasonStr });
    if (!_wsIntentionalClose) {
      _scheduleReconnect();
    }
  });

  // Liveness watchdog — if no ticks for 90s, force reconnect
  if (!_wsWatchdog) {
    _wsWatchdog = setInterval(() => {
      if (!_ws || _ws.readyState !== WebSocket.OPEN) return;
      if (_wsLastTickAt && Date.now() - _wsLastTickAt > 90000) {
        console.log('[ExnessWS] No ticks in 90s — forcing reconnect');
        try { _ws.terminate(); } catch (e) {}
        // terminate triggers 'close' → _scheduleReconnect
      }
    }, 30000);
    // Don't keep the event loop alive just for the watchdog
    if (_wsWatchdog.unref) _wsWatchdog.unref();
  }
}

let _wsWatchdog = null;

function _scheduleReconnect() {
  if (_wsIntentionalClose) return;
  if (_wsReconnectTimer) return; // already scheduled
  const delay = Math.min(_wsBackoff, 30000);
  _wsBackoff = Math.min(_wsBackoff * 2, 30000);
  console.log('[ExnessWS] Reconnecting in ' + delay + 'ms (exponential backoff)');
  _emitStatus('reconnecting', { delayMs: delay });
  _wsReconnectTimer = setTimeout(() => {
    _wsReconnectTimer = null;
    _connectOnce();
  }, delay);
}

// ===== HEALTH CHECK =====
// Tests Exness connectivity by fetching account balance.
// Also reports cached access point + WS connection state + rate stats.
async function healthCheck() {
  if (!EXNESS_API_KEY || !EXNESS_SECRET_KEY || !EXNESS_ACCOUNT_ID) {
    return {
      configured: false,
      message: 'EXNESS_API_KEY, EXNESS_SECRET_KEY, or EXNESS_ACCOUNT_ID not set',
      rateStats: getRateStats()
    };
  }
  const wsConnected = _ws && _ws.readyState === WebSocket.OPEN;
  // Use WebSocket connection status as primary health indicator.
  // Balance endpoint can fail on account-specific host (known issue) — WS is the real signal.
  if (wsConnected) {
    return {
      configured: true,
      connected: true,
      message: 'WebSocket connected — real-time ticks flowing',
      accountId: EXNESS_ACCOUNT_ID,
      accessPoint: cachedAccessPoint,
      wsConnected: true,
      wsLastTickAt: _wsLastTickAt ? new Date(_wsLastTickAt).toISOString() : null,
      subscribedInstruments: SUB_INSTRUMENTS,
      rateStats: getRateStats()
    };
  }
  // If WS is down, try balance endpoint as fallback
  const balance = await getBalance();
  if (balance) {
    return {
      configured: true,
      connected: true,
      message: 'REST connected (WebSocket reconnecting)',
      accountId: EXNESS_ACCOUNT_ID,
      accessPoint: cachedAccessPoint,
      balance: balance.amount,
      currency: balance.currency,
      wsConnected: false,
      rateStats: getRateStats()
    };
  }
  return {
    configured: true,
    connected: false,
    accountId: EXNESS_ACCOUNT_ID,
    accessPoint: cachedAccessPoint,
    wsConnected: false,
    message: 'API auth failed or balance endpoint unavailable',
    rateStats: getRateStats()
  };
}

module.exports = {
  // REST
  discoverAccessPoint,
  getBalance,
  getInstruments,
  getCandles,
  buildSignedHeaders,
  getPrivateKey,
  TF_SECONDS,
  // WebSocket
  connectTicks,
  disconnectTicks,
  // Health
  healthCheck,
  getRateStats,
  // Constants for downstream introspection
  SUB_INSTRUMENTS,
  DISCOVERY_HOST
};
