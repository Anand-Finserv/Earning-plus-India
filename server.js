/**
 * ╔══════════════════════════════════════════════════════════════════════╗
 * ║   EARNINGSPULSE INDIA — NODE.JS REAL-TIME WEBSOCKET SERVER          ║
 * ║   Live NSE/BSE price streaming + 5-Layer analysis API               ║
 * ╚══════════════════════════════════════════════════════════════════════╝
 *
 * HOW TO RUN LOCALLY:
 *   npm install
 *   node server.js
 *
 * HOW TO DEPLOY ON RENDER:
 *   Build Command : npm install
 *   Start Command : node server.js
 *
 * WEBSOCKET EVENTS:
 *   Client → Server:
 *     { type: "SUBSCRIBE",   tickers: ["TCS","INFY","RELIANCE"] }
 *     { type: "UNSUBSCRIBE", tickers: ["TCS"] }
 *     { type: "PING" }
 *
 *   Server → Client:
 *     { type: "PRICE_UPDATE",  data: { TCS: { price, change, changePct, ... } } }
 *     { type: "MARKET_UPDATE", data: { nifty50, sensex } }
 *     { type: "ANALYSIS",      ticker, data: { prediction, confidence, ... } }
 *     { type: "ERROR",         message }
 *     { type: "PONG" }
 */

const express    = require("express");
const http       = require("http");
const WebSocket  = require("ws");
const cors       = require("cors");
const yahooFin   = require("yahoo-finance2").default;

// ── Config ────────────────────────────────────────────────────────────────────
const PORT             = process.env.PORT || 8000;
const PRICE_INTERVAL   = 5000;   // push live prices every 5 seconds
const MARKET_INTERVAL  = 10000;  // push Nifty/Sensex every 10 seconds
const MAX_CLIENTS      = 100;

// ── Stock Universe ────────────────────────────────────────────────────────────
const UNIVERSE = {
  // IT
  TCS:         { yahoo: "TCS.NS",         sector: "IT",       sub: "IT Services",       bse: "532540", nifty50: true  },
  INFY:        { yahoo: "INFY.NS",         sector: "IT",       sub: "IT Services",       bse: "500209", nifty50: true  },
  WIPRO:       { yahoo: "WIPRO.NS",        sector: "IT",       sub: "IT Services",       bse: "507685", nifty50: true  },
  HCLTECH:     { yahoo: "HCLTECH.NS",      sector: "IT",       sub: "IT Services",       bse: "532281", nifty50: true  },
  TECHM:       { yahoo: "TECHM.NS",        sector: "IT",       sub: "IT Services",       bse: "532755", nifty50: true  },
  LTIM:        { yahoo: "LTIM.NS",         sector: "IT",       sub: "IT Services",       bse: "540005", nifty50: false },
  // Banking
  HDFCBANK:    { yahoo: "HDFCBANK.NS",     sector: "BANKING",  sub: "Private Bank",      bse: "500180", nifty50: true  },
  ICICIBANK:   { yahoo: "ICICIBANK.NS",    sector: "BANKING",  sub: "Private Bank",      bse: "532174", nifty50: true  },
  SBIN:        { yahoo: "SBIN.NS",         sector: "BANKING",  sub: "PSU Bank",          bse: "500112", nifty50: true  },
  KOTAKBANK:   { yahoo: "KOTAKBANK.NS",    sector: "BANKING",  sub: "Private Bank",      bse: "500247", nifty50: true  },
  AXISBANK:    { yahoo: "AXISBANK.NS",     sector: "BANKING",  sub: "Private Bank",      bse: "532215", nifty50: true  },
  BAJFINANCE:  { yahoo: "BAJFINANCE.NS",   sector: "BANKING",  sub: "NBFC",              bse: "500034", nifty50: true  },
  // FMCG
  HINDUNILVR:  { yahoo: "HINDUNILVR.NS",   sector: "FMCG",     sub: "Household",         bse: "500696", nifty50: true  },
  ITC:         { yahoo: "ITC.NS",          sector: "FMCG",     sub: "Diversified",       bse: "500875", nifty50: true  },
  NESTLEIND:   { yahoo: "NESTLEIND.NS",    sector: "FMCG",     sub: "Food",              bse: "500790", nifty50: true  },
  BRITANNIA:   { yahoo: "BRITANNIA.NS",    sector: "FMCG",     sub: "Food",              bse: "500825", nifty50: false },
  DABUR:       { yahoo: "DABUR.NS",        sector: "FMCG",     sub: "Healthcare FMCG",   bse: "500096", nifty50: false },
  // Auto
  MARUTI:      { yahoo: "MARUTI.NS",       sector: "AUTO",     sub: "Passenger Cars",    bse: "532500", nifty50: true  },
  TATAMOTORS:  { yahoo: "TATAMOTORS.NS",   sector: "AUTO",     sub: "CV & PV",           bse: "500570", nifty50: true  },
  "M&M":       { yahoo: "M&M.NS",          sector: "AUTO",     sub: "UV & Farm Equip",   bse: "500520", nifty50: true  },
  HEROMOTOCO:  { yahoo: "HEROMOTOCO.NS",   sector: "AUTO",     sub: "2-Wheeler",         bse: "500182", nifty50: true  },
  EICHERMOT:   { yahoo: "EICHERMOT.NS",    sector: "AUTO",     sub: "Premium 2W",        bse: "505200", nifty50: true  },
  // Energy
  RELIANCE:    { yahoo: "RELIANCE.NS",     sector: "ENERGY",   sub: "O&G + Retail + JIO",bse: "500325", nifty50: true  },
  ONGC:        { yahoo: "ONGC.NS",         sector: "ENERGY",   sub: "Upstream O&G",      bse: "500312", nifty50: true  },
  IOC:         { yahoo: "IOC.NS",          sector: "ENERGY",   sub: "Refining & Mktg",   bse: "530965", nifty50: true  },
  NTPC:        { yahoo: "NTPC.NS",         sector: "ENERGY",   sub: "Power Generation",  bse: "532555", nifty50: true  },
  POWERGRID:   { yahoo: "POWERGRID.NS",    sector: "ENERGY",   sub: "Power Transmission",bse: "532898", nifty50: true  },
  // Pharma
  SUNPHARMA:   { yahoo: "SUNPHARMA.NS",    sector: "PHARMA",   sub: "Formulations",      bse: "524715", nifty50: true  },
  DRREDDY:     { yahoo: "DRREDDY.NS",      sector: "PHARMA",   sub: "Generics",          bse: "500124", nifty50: true  },
  CIPLA:       { yahoo: "CIPLA.NS",        sector: "PHARMA",   sub: "Formulations",      bse: "500087", nifty50: true  },
  DIVISLAB:    { yahoo: "DIVISLAB.NS",     sector: "PHARMA",   sub: "API Manufacturer",  bse: "532488", nifty50: true  },
  // Metals
  TATASTEEL:   { yahoo: "TATASTEEL.NS",    sector: "METALS",   sub: "Integrated Steel",  bse: "500470", nifty50: true  },
  JSWSTEEL:    { yahoo: "JSWSTEEL.NS",     sector: "METALS",   sub: "Flat Steel",        bse: "500228", nifty50: true  },
  HINDALCO:    { yahoo: "HINDALCO.NS",     sector: "METALS",   sub: "Aluminium",         bse: "500440", nifty50: true  },
  COALINDIA:   { yahoo: "COALINDIA.NS",    sector: "METALS",   sub: "Coal Mining",       bse: "533278", nifty50: true  },
  // Consumer & Retail
  TITAN:       { yahoo: "TITAN.NS",        sector: "CONSUMER", sub: "Jewellery & Watches",bse: "500114",nifty50: true  },
  ASIANPAINT:  { yahoo: "ASIANPAINT.NS",   sector: "CONSUMER", sub: "Decorative Paints", bse: "500820", nifty50: true  },
  DMART:       { yahoo: "DMART.NS",        sector: "RETAIL",   sub: "Hypermarket",       bse: "540376", nifty50: true  },
  // Cement & Infra
  ULTRACEMCO:  { yahoo: "ULTRACEMCO.NS",   sector: "CEMENT",   sub: "Cement",            bse: "532538", nifty50: true  },
  LT:          { yahoo: "LT.NS",           sector: "INFRA",    sub: "EPC & Engineering", bse: "500510", nifty50: true  },
  // Telecom
  BHARTIARTL:  { yahoo: "BHARTIARTL.NS",   sector: "TELECOM",  sub: "Mobile + Broadband",bse: "532454", nifty50: true  },
  // Insurance
  HDFCLIFE:    { yahoo: "HDFCLIFE.NS",     sector: "INSURANCE",sub: "Life Insurance",    bse: "540777", nifty50: true  },
  SBILIFE:     { yahoo: "SBILIFE.NS",      sector: "INSURANCE",sub: "Life Insurance",    bse: "540719", nifty50: false },
};

// ── Price cache (last known prices) ──────────────────────────────────────────
const priceCache = {};
const analysisCache = {};

// ── Express App ───────────────────────────────────────────────────────────────
const app    = express();
const server = http.createServer(app);

app.use(cors());
app.use(express.json());

// ── WebSocket Server ──────────────────────────────────────────────────────────
const wss = new WebSocket.Server({ server, path: "/ws" });

// Track subscriptions per client
const clients = new Map();  // ws → Set of subscribed tickers

// ── Helpers ───────────────────────────────────────────────────────────────────
function send(ws, payload) {
  if (ws.readyState === WebSocket.OPEN) {
    ws.send(JSON.stringify(payload));
  }
}

function broadcast(payload, tickerFilter = null) {
  clients.forEach((subs, ws) => {
    if (ws.readyState !== WebSocket.OPEN) return;
    if (tickerFilter && !subs.has(tickerFilter)) return;
    ws.send(JSON.stringify(payload));
  });
}

function fmt(v) {
  return v != null ? Number(v) : null;
}

// ── Fetch live price for one ticker ──────────────────────────────────────────
async function fetchPrice(ticker) {
  const meta = UNIVERSE[ticker];
  if (!meta) return null;

  try {
    const q = await yahooFin.quote(meta.yahoo, {}, { validateResult: false });

    const price      = fmt(q.regularMarketPrice);
    const prev       = fmt(q.regularMarketPreviousClose);
    const change     = price && prev ? +(price - prev).toFixed(2) : 0;
    const changePct  = price && prev ? +((change / prev) * 100).toFixed(2) : 0;
    const dayHigh    = fmt(q.regularMarketDayHigh);
    const dayLow     = fmt(q.regularMarketDayLow);
    const volume     = fmt(q.regularMarketVolume);
    const mktCap     = fmt(q.marketCap);
    const pe         = fmt(q.trailingPE);
    const weekHigh52 = fmt(q.fiftyTwoWeekHigh);
    const weekLow52  = fmt(q.fiftyTwoWeekLow);

    const data = {
      ticker,
      name:        q.longName || q.shortName || ticker,
      sector:      meta.sector,
      sub:         meta.sub,
      price,
      change,
      changePct,
      dayHigh,
      dayLow,
      volume,
      mktCap,
      pe,
      weekHigh52,
      weekLow52,
      currency:    q.currency || "INR",
      marketState: q.marketState || "CLOSED",
      timestamp:   new Date().toISOString(),
    };

    priceCache[ticker] = data;
    return data;
  } catch {
    return priceCache[ticker] || null;   // return cached if fetch fails
  }
}

// ── Fetch market indices ──────────────────────────────────────────────────────
async function fetchMarket() {
  try {
    const [nifty, sensex] = await Promise.all([
      yahooFin.quote("^NSEI",  {}, { validateResult: false }),
      yahooFin.quote("^BSESN", {}, { validateResult: false }),
    ]);

    const mkIndex = (q, label) => ({
      label,
      price:     fmt(q.regularMarketPrice),
      change:    fmt(q.regularMarketChange),
      changePct: fmt(q.regularMarketChangePercent),
      dayHigh:   fmt(q.regularMarketDayHigh),
      dayLow:    fmt(q.regularMarketDayLow),
      timestamp: new Date().toISOString(),
    });

    return {
      nifty50: mkIndex(nifty,  "NIFTY 50"),
      sensex:  mkIndex(sensex, "SENSEX"),
    };
  } catch {
    return null;
  }
}

// ── 5-Layer analysis (lightweight JS version) ─────────────────────────────────
async function runAnalysis(ticker) {
  if (analysisCache[ticker] && Date.now() - analysisCache[ticker]._ts < 300_000) {
    return analysisCache[ticker];   // cache for 5 minutes
  }

  const meta = UNIVERSE[ticker];
  if (!meta) throw new Error(`Unknown ticker: ${ticker}`);

  try {
    const q = await yahooFin.quoteSummary(meta.yahoo, {
      modules: ["price","summaryDetail","defaultKeyStatistics","financialData","recommendationTrend","earningsTrend"],
    }, { validateResult: false });

    const fd  = q.financialData   || {};
    const sd  = q.summaryDetail   || {};
    const ks  = q.defaultKeyStatistics || {};
    const pr  = q.price           || {};
    const rt  = q.recommendationTrend?.trend?.[0] || {};

    // ── Layer scores ──────────────────────────────────────────────────────────
    const revGr   = fd.revenueGrowth?.raw    ?? 0;
    const earnGr  = fd.earningsGrowth?.raw   ?? 0;
    const netMg   = fd.profitMargins?.raw    ?? 0;
    const operMg  = fd.operatingMargins?.raw ?? 0;
    const roe     = fd.returnOnEquity?.raw   ?? 0;
    const de      = fd.debtToEquity?.raw     ?? 50;
    const cr      = fd.currentRatio?.raw     ?? 1.2;
    const rec     = fd.recommendationMean?.raw ?? 3;
    const target  = fd.targetMeanPrice?.raw  ?? 0;
    const price   = pr.regularMarketPrice?.raw ?? fd.currentPrice?.raw ?? 0;
    const peg     = ks.pegRatio?.raw         ?? null;
    const inst    = ks.heldPercentInstitutions?.raw ?? 0.45;
    const shortR  = ks.shortRatio?.raw       ?? 3;
    const beta    = ks.beta?.raw             ?? 1;

    const clamp   = (v, lo=0, hi=100) => Math.max(lo, Math.min(hi, Math.round(v)));
    const upside  = target && price ? ((target - price) / price * 100) : 0;

    // L1 — Alt Data
    const l1 = clamp(50 + revGr*130 + earnGr*60 + inst*40 + Math.max(0,(5-shortR)*4));

    // L2 — Sentiment
    const recScore = clamp(((5-rec)/4)*60 + Math.min(40, upside*1.3));
    const beatBonus= (rt.strongBuy??0)*4 + (rt.buy??0)*2 - (rt.sell??0)*3;
    const l2 = clamp((recScore + 50 + beatBonus) / 2);

    // L3 — Financial Model
    const pegScore  = peg ? clamp(70 - (peg-1)*18) : 55;
    const mgScore   = clamp(netMg*200 + operMg*120 + revGr*90 + 25);
    const bsScore   = clamp(Math.min(28,cr*11) + Math.max(0,30-de*0.18) + Math.min(26,roe*88));
    const l3 = clamp((pegScore + mgScore + bsScore) / 3);

    // L4 — Options Flow
    const instScore = clamp(inst*52 + Math.max(0,(6-shortR)*7));
    const mom       = priceCache[ticker]?.changePct ?? 0;
    const momScore  = clamp(50 + mom*4);
    const l4 = clamp((instScore + momScore) / 2);

    // L5 — Sector KPI
    const kpiScore  = clamp(50 + revGr*180 + earnGr*100 + netMg*150 + roe*80);
    const l5 = kpiScore;

    // Weighted ensemble
    const conf = clamp(l1*0.22 + l2*0.20 + l3*0.25 + l4*0.13 + l5*0.20);

    const prediction    = conf >= 66 ? "BEAT" : conf <= 44 ? "MISS" : "INLINE";
    const position      = prediction==="BEAT" ? "LONG" : prediction==="MISS" ? "SHORT" : "NEUTRAL";
    const posStrength   = conf>=78?"Strong Conviction":conf>=66?"Moderate":conf<=35?"Strong Conviction":conf<=44?"Moderate":"Weak";
    const impliedMove   = +(Math.max(2, Math.min(25, (32 + beta*8) * Math.sqrt(1/52) * 100))).toFixed(1);

    const result = {
      ticker,
      name:           pr.longName || ticker,
      sector:         meta.sector,
      sub:            meta.sub,
      bseCode:        meta.bse,
      nifty50:        meta.nifty50,
      price:          price,
      marketCap:      pr.marketCap?.raw ?? 0,
      prediction,
      confidence:     conf,
      position,
      positionStrength: posStrength,
      impliedMove,
      layerScores: {
        altData:        l1,
        sentiment:      l2,
        financialModel: l3,
        optionsFlow:    l4,
        sectorKPI:      l5,
      },
      metrics: {
        revenueGrowth:   revGr,
        earningsGrowth:  earnGr,
        netMargin:       netMg,
        operatingMargin: operMg,
        roe,
        debtToEquity:    de,
        currentRatio:    cr,
        peRatio:         sd.trailingPE?.raw ?? null,
        pbRatio:         ks.priceToBook?.raw ?? null,
        pegRatio:        peg,
        beta,
        week52High:      sd.fiftyTwoWeekHigh?.raw ?? null,
        week52Low:       sd.fiftyTwoWeekLow?.raw  ?? null,
        targetPrice:     target,
        upside,
        institutionalHolding: inst,
      },
      timestamp: new Date().toISOString(),
      _ts: Date.now(),
    };

    analysisCache[ticker] = result;
    return result;

  } catch (e) {
    throw new Error(`Analysis failed for ${ticker}: ${e.message}`);
  }
}

// ─────────────────────────────────────────────────────────────────────────────
// WEBSOCKET HANDLER
// ─────────────────────────────────────────────────────────────────────────────
wss.on("connection", (ws, req) => {
  if (clients.size >= MAX_CLIENTS) {
    ws.close(1008, "Server at capacity");
    return;
  }

  clients.set(ws, new Set());
  console.log(`[WS] Client connected. Total: ${clients.size}`);

  // Send welcome + universe on connect
  send(ws, {
    type: "CONNECTED",
    message: "EarningsPulse India WebSocket connected",
    universe: Object.keys(UNIVERSE).length,
    timestamp: new Date().toISOString(),
  });

  ws.on("message", async (raw) => {
    let msg;
    try { msg = JSON.parse(raw); } catch { return; }

    switch (msg.type) {

      // ── Subscribe to live prices for tickers ─────────────────────────────
      case "SUBSCRIBE": {
        const tickers = (msg.tickers || [])
          .map(t => t.toUpperCase())
          .filter(t => UNIVERSE[t]);

        const subs = clients.get(ws);
        tickers.forEach(t => subs.add(t));

        // Immediately push current cached or fresh prices
        const priceData = {};
        await Promise.all(tickers.map(async t => {
          const p = await fetchPrice(t);
          if (p) priceData[t] = p;
        }));

        if (Object.keys(priceData).length > 0) {
          send(ws, { type: "PRICE_UPDATE", data: priceData, timestamp: new Date().toISOString() });
        }

        send(ws, { type: "SUBSCRIBED", tickers, count: subs.size });
        console.log(`[WS] Subscribed: ${tickers.join(", ")}`);
        break;
      }

      // ── Unsubscribe ───────────────────────────────────────────────────────
      case "UNSUBSCRIBE": {
        const subs = clients.get(ws);
        (msg.tickers || []).forEach(t => subs.delete(t.toUpperCase()));
        send(ws, { type: "UNSUBSCRIBED", tickers: msg.tickers });
        break;
      }

      // ── Request full 5-layer analysis ─────────────────────────────────────
      case "ANALYZE": {
        const ticker = (msg.ticker || "").toUpperCase();
        if (!ticker || !UNIVERSE[ticker]) {
          send(ws, { type: "ERROR", message: `Unknown ticker: ${ticker}` });
          break;
        }
        try {
          send(ws, { type: "ANALYZING", ticker });
          const analysis = await runAnalysis(ticker);
          send(ws, { type: "ANALYSIS", ticker, data: analysis });
        } catch (e) {
          send(ws, { type: "ERROR", message: e.message });
        }
        break;
      }

      // ── Ping/pong keepalive ───────────────────────────────────────────────
      case "PING":
        send(ws, { type: "PONG", timestamp: new Date().toISOString() });
        break;

      default:
        send(ws, { type: "ERROR", message: `Unknown message type: ${msg.type}` });
    }
  });

  ws.on("close", () => {
    clients.delete(ws);
    console.log(`[WS] Client disconnected. Total: ${clients.size}`);
  });

  ws.on("error", (err) => {
    console.error("[WS] Error:", err.message);
    clients.delete(ws);
  });
});

// ─────────────────────────────────────────────────────────────────────────────
// PRICE BROADCAST LOOP — every 5 seconds
// ─────────────────────────────────────────────────────────────────────────────
setInterval(async () => {
  if (clients.size === 0) return;

  // Collect all subscribed tickers across all clients
  const allSubs = new Set();
  clients.forEach(subs => subs.forEach(t => allSubs.add(t)));
  if (allSubs.size === 0) return;

  // Fetch prices in parallel
  const prices = await Promise.all([...allSubs].map(fetchPrice));
  const priceMap = {};
  prices.forEach(p => { if (p) priceMap[p.ticker] = p; });

  if (Object.keys(priceMap).length === 0) return;

  // Push to each client only their subscribed tickers
  clients.forEach((subs, ws) => {
    if (ws.readyState !== WebSocket.OPEN) return;
    const clientPrices = {};
    subs.forEach(t => { if (priceMap[t]) clientPrices[t] = priceMap[t]; });
    if (Object.keys(clientPrices).length > 0) {
      send(ws, { type: "PRICE_UPDATE", data: clientPrices, timestamp: new Date().toISOString() });
    }
  });
}, PRICE_INTERVAL);

// ─────────────────────────────────────────────────────────────────────────────
// MARKET INDEX BROADCAST LOOP — every 10 seconds
// ─────────────────────────────────────────────────────────────────────────────
setInterval(async () => {
  if (clients.size === 0) return;
  const market = await fetchMarket();
  if (!market) return;
  wss.clients.forEach(ws => {
    if (ws.readyState === WebSocket.OPEN) {
      send(ws, { type: "MARKET_UPDATE", data: market, timestamp: new Date().toISOString() });
    }
  });
}, MARKET_INTERVAL);

// ─────────────────────────────────────────────────────────────────────────────
// REST API ROUTES
// ─────────────────────────────────────────────────────────────────────────────

// Health check
app.get("/", (req, res) => {
  res.json({
    service:   "EarningsPulse India — WebSocket Server",
    version:   "2.0.0",
    status:    "operational",
    clients:   clients.size,
    universe:  Object.keys(UNIVERSE).length,
    websocket: `ws://${req.headers.host}/ws`,
    timestamp: new Date().toISOString(),
  });
});

// Stock universe list
app.get("/stocks/list", (req, res) => {
  res.json({
    total: Object.keys(UNIVERSE).length,
    stocks: Object.entries(UNIVERSE).map(([ticker, m]) => ({
      ticker,
      yahoo:    m.yahoo,
      sector:   m.sector,
      sub:      m.sub,
      bse:      m.bse,
      nifty50:  m.nifty50,
    })),
  });
});

// Live price snapshot (REST fallback)
app.get("/price/:ticker", async (req, res) => {
  const ticker = req.params.ticker.toUpperCase();
  if (!UNIVERSE[ticker]) return res.status(404).json({ error: `Unknown ticker: ${ticker}` });
  try {
    const p = await fetchPrice(ticker);
    res.json(p);
  } catch (e) {
    res.status(500).json({ error: e.message });
  }
});

// Full 5-layer analysis (REST)
app.get("/stock/:ticker", async (req, res) => {
  const ticker = req.params.ticker.toUpperCase();
  if (!UNIVERSE[ticker]) return res.status(404).json({ error: `Unknown ticker: ${ticker}` });
  try {
    const analysis = await runAnalysis(ticker);
    res.json(analysis);
  } catch (e) {
    res.status(500).json({ error: e.message });
  }
});

// Market overview (REST)
app.get("/market/overview", async (req, res) => {
  try {
    const market = await fetchMarket();
    res.json(market || { error: "Market data unavailable" });
  } catch (e) {
    res.status(500).json({ error: e.message });
  }
});

// Multiple prices at once (REST)
app.post("/prices/bulk", async (req, res) => {
  const tickers = (req.body.tickers || []).map(t => t.toUpperCase()).filter(t => UNIVERSE[t]);
  try {
    const prices = await Promise.all(tickers.map(fetchPrice));
    const result = {};
    prices.forEach(p => { if (p) result[p.ticker] = p; });
    res.json(result);
  } catch (e) {
    res.status(500).json({ error: e.message });
  }
});

// ─────────────────────────────────────────────────────────────────────────────
// START SERVER
// ─────────────────────────────────────────────────────────────────────────────
server.listen(PORT, () => {
  console.log("╔══════════════════════════════════════════════════════════╗");
  console.log("║   EARNINGSPULSE INDIA — WebSocket Server                 ║");
  console.log(`║   HTTP  → http://localhost:${PORT}                          ║`);
  console.log(`║   WS    → ws://localhost:${PORT}/ws                         ║`);
  console.log(`║   Stocks → ${Object.keys(UNIVERSE).length} NSE stocks in universe             ║`);
  console.log("╚══════════════════════════════════════════════════════════╝");
});
