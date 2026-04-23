require('dotenv').config();
const express = require('express');
const cors = require('cors');
const cookieSession = require('cookie-session');
const path = require('path');
const fs = require('fs');
const crypto = require('crypto');
const ExcelJS = require('exceljs');

// Course costs persistent storage (file-based + Airtable for deploy persistence)
const COSTS_FILE = path.join(__dirname, 'data', 'course-costs.json');
const SHARE_FILE = path.join(__dirname, 'data', 'share-tokens.json');

function loadCostsFromFile() {
  try {
    if (fs.existsSync(COSTS_FILE)) {
      return JSON.parse(fs.readFileSync(COSTS_FILE, 'utf8'));
    }
  } catch (e) { console.error('Error loading costs from file:', e); }
  return {};
}

function saveCostsToFile(costs) {
  try {
    const dir = path.dirname(COSTS_FILE);
    if (!fs.existsSync(dir)) fs.mkdirSync(dir, { recursive: true });
    fs.writeFileSync(COSTS_FILE, JSON.stringify(costs, null, 2));
  } catch (e) { console.error('Error saving costs to file:', e); }
  // Also persist to Airtable (async, fire-and-forget)
  airtableConfigSet('course_costs', costs).catch(e =>
    console.error('Airtable costs save failed:', e.message)
  );
}

function loadShareTokensFromFile() {
  try {
    if (fs.existsSync(SHARE_FILE)) {
      return JSON.parse(fs.readFileSync(SHARE_FILE, 'utf8'));
    }
  } catch (e) { console.error('Error loading share tokens from file:', e); }
  return {};
}

function saveShareTokensToFile(tokens) {
  try {
    const dir = path.dirname(SHARE_FILE);
    if (!fs.existsSync(dir)) fs.mkdirSync(dir, { recursive: true });
    fs.writeFileSync(SHARE_FILE, JSON.stringify(tokens, null, 2));
  } catch (e) { console.error('Error saving share tokens to file:', e); }
  // Also persist to Airtable (async, fire-and-forget)
  airtableConfigSet('share_tokens', tokens).catch(e =>
    console.error('Airtable tokens save failed:', e.message)
  );
}

function generateShareToken() {
  return crypto.randomUUID();
}

// In-memory state (loaded from file first, then enriched from Airtable on startup)
const courseCosts = loadCostsFromFile();
let shareTokens = loadShareTokensFromFile();

// ============================================================================
// AIRTABLE-BASED PERSISTENCE (survives Render ephemeral filesystem wipes)
// Uses table NAME directly in REST API (no metadata API / schema permissions needed)
// ============================================================================
const CONFIG_TABLE_NAME = 'SSA_CourseConfig';
let airtablePersistenceActive = false;

async function initConfigTable() {
  const apiKey = process.env.AIRTABLE_API_KEY;
  const baseId = process.env.AIRTABLE_BASE_ID || 'appwCWGRd0jXOCxMA';
  if (!apiKey) {
    console.log('Airtable persistence: DISABLED (no API key)');
    return;
  }
  try {
    // Test connectivity by querying the table by NAME (no metadata API needed)
    const testResp = await fetch(
      `https://api.airtable.com/v0/${baseId}/${encodeURIComponent(CONFIG_TABLE_NAME)}?maxRecords=1`,
      { headers: { 'Authorization': `Bearer ${apiKey}` } }
    );
    if (testResp.ok) {
      airtablePersistenceActive = true;
      console.log(`Airtable persistence: ACTIVE (table ${CONFIG_TABLE_NAME})`);
    } else {
      const errText = await testResp.text();
      console.log(`Airtable persistence: DISABLED (table not accessible: ${testResp.status} ${errText})`);
    }
  } catch (e) {
    console.error('Airtable persistence init failed:', e.message);
  }
}

async function airtableConfigGet(key) {
  if (!airtablePersistenceActive) return null;
  const apiKey = process.env.AIRTABLE_API_KEY;
  const baseId = process.env.AIRTABLE_BASE_ID || 'appwCWGRd0jXOCxMA';
  const tablePath = encodeURIComponent(CONFIG_TABLE_NAME);
  try {
    const formula = encodeURIComponent(`{Key}="${key}"`);
    const resp = await fetch(
      `https://api.airtable.com/v0/${baseId}/${tablePath}?filterByFormula=${formula}`,
      { headers: { 'Authorization': `Bearer ${apiKey}` } }
    );
    if (!resp.ok) return null;
    const data = await resp.json();
    if (data.records && data.records.length > 0) {
      const val = data.records[0].fields.Value;
      return val ? JSON.parse(val) : null;
    }
  } catch (e) {
    console.error(`Airtable config get '${key}' failed:`, e.message);
  }
  return null;
}

async function airtableConfigSet(key, value) {
  if (!airtablePersistenceActive) return;
  const apiKey = process.env.AIRTABLE_API_KEY;
  const baseId = process.env.AIRTABLE_BASE_ID || 'appwCWGRd0jXOCxMA';
  const tablePath = encodeURIComponent(CONFIG_TABLE_NAME);
  try {
    const formula = encodeURIComponent(`{Key}="${key}"`);
    const resp = await fetch(
      `https://api.airtable.com/v0/${baseId}/${tablePath}?filterByFormula=${formula}`,
      { headers: { 'Authorization': `Bearer ${apiKey}` } }
    );
    const data = resp.ok ? await resp.json() : { records: [] };
    const jsonValue = JSON.stringify(value);

    // Filter to records that actually have a Key value matching
    const matching = (data.records || []).filter(r => r.fields && r.fields.Key === key);

    if (matching.length > 0) {
      // Update existing record
      const writeResp = await fetch(`https://api.airtable.com/v0/${baseId}/${tablePath}/${matching[0].id}`, {
        method: 'PATCH',
        headers: {
          'Authorization': `Bearer ${apiKey}`,
          'Content-Type': 'application/json'
        },
        body: JSON.stringify({ fields: { Key: key, Value: jsonValue } })
      });
      if (!writeResp.ok) {
        const errBody = await writeResp.text();
        console.error(`Airtable PATCH '${key}' failed: ${writeResp.status} ${errBody}`);
      } else {
        console.log(`Airtable config '${key}' updated (${jsonValue.length} chars)`);
      }
    } else {
      // Create new record (single record format)
      const writeResp = await fetch(`https://api.airtable.com/v0/${baseId}/${tablePath}`, {
        method: 'POST',
        headers: {
          'Authorization': `Bearer ${apiKey}`,
          'Content-Type': 'application/json'
        },
        body: JSON.stringify({ fields: { Key: key, Value: jsonValue } })
      });
      if (!writeResp.ok) {
        const errBody = await writeResp.text();
        console.error(`Airtable POST '${key}' failed: ${writeResp.status} ${errBody}`);
      } else {
        console.log(`Airtable config '${key}' created (${jsonValue.length} chars)`);
      }
    }
  } catch (e) {
    console.error(`Airtable config set '${key}' failed:`, e.message);
  }
}

async function loadFromAirtable() {
  if (!airtablePersistenceActive) return;
  try {
    const costsData = await airtableConfigGet('course_costs');
    if (costsData && Object.keys(costsData).length > 0) {
      // Airtable ALWAYS wins (file is ephemeral on Render, Airtable is the source of truth)
      Object.keys(costsData).forEach(k => {
        courseCosts[k] = costsData[k];
      });
      console.log(`Loaded ${Object.keys(costsData).length} course costs from Airtable (overwriting file data)`);
    }
    const tokensData = await airtableConfigGet('share_tokens');
    if (tokensData && Object.keys(tokensData).length > 0) {
      Object.keys(tokensData).forEach(k => {
        shareTokens[k] = tokensData[k];
      });
      console.log(`Loaded ${Object.keys(tokensData).length} share tokens from Airtable (overwriting file data)`);
    }
  } catch (e) {
    console.error('Failed to load from Airtable:', e.message);
  }
}

const app = express();
const PORT = process.env.PORT || 3000;

// Middleware
app.use(cors());
app.use(express.json());

// ============================================================================
// AUTHENTICATION (cookie-session + simple username/password from .env)
// ============================================================================
const AUTH_USER = process.env.AUTH_USERNAME || 'admin';
const AUTH_PASS = process.env.AUTH_PASSWORD || 'changeme';

app.use(cookieSession({
  name: 'ssa_session',
  keys: [process.env.SESSION_SECRET || 'ssa-corsi-default-secret-change-me'],
  maxAge: 7 * 24 * 60 * 60 * 1000, // 7 days
  httpOnly: true,
  sameSite: 'lax'
}));

// Login page (served without auth)
app.get('/login', (req, res) => {
  if (req.session && req.session.authenticated) return res.redirect('/');
  res.sendFile(path.join(__dirname, 'public', 'login.html'));
});

// Login API
app.post('/auth/login', (req, res) => {
  const { username, password } = req.body;
  if (username === AUTH_USER && password === AUTH_PASS) {
    req.session.authenticated = true;
    req.session.user = username;
    return res.json({ success: true });
  }
  res.status(401).json({ success: false, error: 'Invalid credentials' });
});

// Logout
app.post('/auth/logout', (req, res) => {
  req.session = null;
  res.json({ success: true });
});
app.get('/auth/logout', (req, res) => {
  req.session = null;
  res.redirect('/login');
});

// Share pages are public (no auth needed)
app.get('/share/:token', (req, res, next) => { next(); });
app.get('/api/shared/:token', (req, res, next) => { next(); });

// Auth middleware — protect everything else except login assets
app.use((req, res, next) => {
  // Allow login page assets
  if (req.path === '/login' || req.path === '/login.html' ||
      req.path === '/ssa-logo.png' || req.path.startsWith('/auth/') ||
      req.path.startsWith('/share/') || req.path.startsWith('/api/shared/')) {
    return next();
  }
  if (req.session && req.session.authenticated) {
    return next();
  }
  // API calls get 401, page requests get redirected
  if (req.path.startsWith('/api/')) {
    return res.status(401).json({ success: false, error: 'Not authenticated' });
  }
  res.redirect('/login');
});

app.use(express.static(path.join(__dirname, 'public')));

// ============================================================================
// CACHE UTILITY (in-memory cache with TTL)
// ============================================================================
const cache = new Map();

function getCache(key) {
  const item = cache.get(key);
  if (!item) return null;
  if (Date.now() > item.expiresAt) {
    cache.delete(key);
    return null;
  }
  return item.value;
}

function setCache(key, value, ttlSeconds = 300) {
  cache.set(key, {
    value,
    expiresAt: Date.now() + ttlSeconds * 1000
  });
}

// ============================================================================
// EXAM RESULTS (Airtable "SSA Sommelier database" — Socrative pipeline)
// Reads student exam outcomes processed by the Airtable script and exposes
// them as a map keyed by email (lowercase, trimmed) for matching with Shopify
// customer data on the dashboard.
// ============================================================================
const EXAM_BASE_ID = process.env.AIRTABLE_EXAM_BASE_ID || 'appj4DEH3RYFqct1Q';
const EXAM_STUDENTS_TABLE = process.env.AIRTABLE_EXAM_STUDENTS_TABLE || 'tblq6DIMSpZumpB6S';
const EXAM_COURSES_TABLE = process.env.AIRTABLE_EXAM_COURSES_TABLE || 'tblhsCnD5AJbkmWjR';
const EXAM_THRESHOLDS_TABLE = process.env.AIRTABLE_EXAM_THRESHOLDS_TABLE || 'tblalXmeAnhXWYawM';

// Result priority: higher wins when student has multiple exam attempts (retrial)
const RESULT_PRIORITY = { passed: 3, retrial: 2, failed: 1, unknown: 0 };

// Map Airtable threshold name -> normalized outcome
function normalizeResultName(name) {
  const n = (name || '').toLowerCase();
  if (n.includes('pass')) return 'passed';
  if (n.includes('retrial') || n.includes('retr')) return 'retrial';
  if (n.includes('fail')) return 'failed';
  return 'unknown';
}

async function airtableFetchAll(baseId, tableId, params = {}) {
  const apiKey = process.env.AIRTABLE_API_KEY;
  if (!apiKey) return [];
  const all = [];
  let offset = null;
  let pages = 0;
  do {
    const qp = new URLSearchParams({ pageSize: '100', ...params });
    if (offset) qp.set('offset', offset);
    const resp = await fetch(`https://api.airtable.com/v0/${baseId}/${tableId}?${qp.toString()}`, {
      headers: { 'Authorization': `Bearer ${apiKey}` }
    });
    if (!resp.ok) {
      console.error(`Airtable fetch ${tableId} failed: ${resp.status}`);
      return all;
    }
    const data = await resp.json();
    all.push(...(data.records || []));
    offset = data.offset || null;
    pages++;
    if (pages > 50) break; // safety
  } while (offset);
  return all;
}

async function fetchExamResults() {
  const cacheKey = 'exam_results';
  const cached = getCache(cacheKey);
  if (cached) return cached;

  const apiKey = process.env.AIRTABLE_API_KEY;
  if (!apiKey) {
    console.log('Exam results: AIRTABLE_API_KEY not configured');
    return { byEmail: {}, count: 0, lastUpdated: null };
  }

  try {
    const t0 = Date.now();
    // Parallel fetch: thresholds (3 records) + courses (~120) + students (~200)
    const [thresholds, courses, students] = await Promise.all([
      airtableFetchAll(EXAM_BASE_ID, EXAM_THRESHOLDS_TABLE),
      airtableFetchAll(EXAM_BASE_ID, EXAM_COURSES_TABLE),
      airtableFetchAll(EXAM_BASE_ID, EXAM_STUDENTS_TABLE, {
        filterByFormula: 'AND({Is finished}=TRUE(), NOT({Email}=""))'
      })
    ]);

    // Build lookup maps
    const thresholdMap = {};
    thresholds.forEach(r => { thresholdMap[r.id] = normalizeResultName(r.fields.Name); });

    const courseMap = {};
    courses.forEach(r => {
      const f = r.fields || {};
      courseMap[r.id] = {
        name: f.Name || '',
        startTime: f['Start time'] || null
      };
    });

    // Aggregate by email — keep best result per student (priority) + count attempts
    const byEmail = {};
    students.forEach(r => {
      const f = r.fields || {};
      const emailKey = (f.Email || '').toLowerCase().trim();
      if (!emailKey) return;
      const resultIds = f.Result || [];
      const resultName = resultIds.length ? (thresholdMap[resultIds[0]] || 'unknown') : 'unknown';
      // Parse score: Airtable returns "90%" as string
      let scorePct = null;
      const sRaw = f['Score, %'];
      if (typeof sRaw === 'string') {
        const m = sRaw.match(/(\d+)/);
        if (m) scorePct = parseInt(m[1], 10);
      } else if (typeof sRaw === 'number') {
        scorePct = sRaw;
      }
      const courseIds = f.Course || [];
      const courseInfo = courseIds.length ? courseMap[courseIds[0]] : null;
      const examDate = courseInfo ? courseInfo.startTime : null;
      const courseName = courseInfo ? courseInfo.name : '';

      const entry = {
        fullName: f['Full Name'] || '',
        result: resultName,
        score: scorePct,
        examDate,
        courseName,
        recordId: r.id,
        attempts: 1
      };

      const existing = byEmail[emailKey];
      if (!existing) {
        byEmail[emailKey] = entry;
      } else {
        // More attempts detected
        existing.attempts += 1;
        // Keep entry with highest priority; if tie, keep most recent examDate
        const prOld = RESULT_PRIORITY[existing.result] || 0;
        const prNew = RESULT_PRIORITY[entry.result] || 0;
        let replace = false;
        if (prNew > prOld) replace = true;
        else if (prNew === prOld) {
          if ((entry.examDate || '') > (existing.examDate || '')) replace = true;
        }
        if (replace) {
          entry.attempts = existing.attempts;
          byEmail[emailKey] = entry;
        }
      }
    });

    const result = {
      byEmail,
      count: Object.keys(byEmail).length,
      totalAttempts: students.length,
      lastUpdated: new Date().toISOString()
    };

    setCache(cacheKey, result, 900); // 15 min
    console.log(`[exam-results] fetched ${students.length} attempts, ${result.count} unique students in ${Date.now() - t0}ms`);
    return result;
  } catch (e) {
    console.error('fetchExamResults failed:', e.message);
    return { byEmail: {}, count: 0, error: e.message };
  }
}

// ============================================================================
// SHOPIFY API UTILITIES
// ============================================================================
const SHOPIFY_STORE = process.env.SHOPIFY_STORE || 'sakesommelierassociation.myshopify.com';
const SHOPIFY_ACCESS_TOKEN = process.env.SHOPIFY_ACCESS_TOKEN;
const SHOPIFY_API_VERSION = '2024-01';

async function shopifyFetch(endpoint, options = {}) {
  if (!SHOPIFY_ACCESS_TOKEN) {
    throw new Error('SHOPIFY_ACCESS_TOKEN not configured');
  }
  const url = `https://${SHOPIFY_STORE}/admin/api/${SHOPIFY_API_VERSION}${endpoint}`;
  const maxRetries = 3;
  for (let attempt = 0; attempt <= maxRetries; attempt++) {
    const response = await fetch(url, {
      method: options.method || 'GET',
      headers: {
        'X-Shopify-Access-Token': SHOPIFY_ACCESS_TOKEN,
        'Content-Type': 'application/json',
        ...options.headers
      },
      body: options.body ? JSON.stringify(options.body) : undefined
    });
    if (response.status === 429) {
      // Rate limited - wait and retry
      const retryAfter = parseFloat(response.headers.get('Retry-After') || '2');
      console.log(`Shopify rate limited on ${endpoint}, waiting ${retryAfter}s (attempt ${attempt + 1}/${maxRetries})`);
      await new Promise(r => setTimeout(r, retryAfter * 1000));
      continue;
    }
    if (!response.ok) {
      const error = await response.text();
      throw new Error(`Shopify API Error (${response.status}): ${error}`);
    }
    return await response.json();
  }
  throw new Error(`Shopify API rate limit exceeded after ${maxRetries} retries: ${endpoint}`);
}

// Fetch ALL Shopify products with pagination
async function fetchAllShopifyProducts() {
  const cacheKey = 'shopify_all_products';
  let cached = getCache(cacheKey);
  if (cached) return cached;

  let allProducts = [];
  let url = `/products.json?limit=250`;

  while (url) {
    const response = await shopifyFetch(url);
    const products = response.products || [];
    allProducts = allProducts.concat(products);

    // Check for pagination (Shopify link header not available via this method)
    // For now, if we got less than 250, we've got them all
    if (products.length < 250) break;

    // Simple pagination using since_id
    const lastId = products[products.length - 1].id;
    url = `/products.json?limit=250&since_id=${lastId}`;
  }

  setCache(cacheKey, allProducts, 900); // 15 min cache (products rarely change)
  return allProducts;
}

// Fetch ALL Shopify orders with pagination
async function fetchAllShopifyOrders() {
  const cacheKey = 'shopify_all_orders';
  let cached = getCache(cacheKey);
  if (cached) return cached;

  let allOrders = [];
  // Fetch ALL orders since Jan 2024 (Shopify defaults to last 60 days only)
  let url = `/orders.json?limit=250&status=any&created_at_min=2024-01-01T00:00:00Z`;

  while (url) {
    const response = await shopifyFetch(url);
    const orders = response.orders || [];
    allOrders = allOrders.concat(orders);
    if (orders.length < 250) break;
    const lastId = orders[orders.length - 1].id;
    url = `/orders.json?limit=250&status=any&since_id=${lastId}`;
  }

  setCache(cacheKey, allOrders, 600); // 10 min cache
  return allOrders;
}

// ============================================================================
// AIRTABLE API UTILITIES
// ============================================================================
const AIRTABLE_API_KEY = process.env.AIRTABLE_API_KEY;
const AIRTABLE_BASE_ID = process.env.AIRTABLE_BASE_ID || 'appwCWGRd0jXOCxMA';
const AIRTABLE_TABLE_ID = process.env.AIRTABLE_TABLE_ID || 'tblnJO5Mf7EVmteRk';

// Airtable "Form registrazione studenti SSA" base (student QR code registration)
const AIRTABLE_REG_BASE_ID = 'app8OYdmX32x7Frjk';
const AIRTABLE_REG_TABLE_ID = 'tblmHWvzfar6Wf0hw';

async function airtableFetch(endpoint, options = {}) {
  if (!AIRTABLE_API_KEY) {
    throw new Error('AIRTABLE_API_KEY not configured');
  }
  const url = `https://api.airtable.com/v0/${AIRTABLE_BASE_ID}/${endpoint}`;
  const response = await fetch(url, {
    method: options.method || 'GET',
    headers: {
      'Authorization': `Bearer ${AIRTABLE_API_KEY}`,
      'Content-Type': 'application/json',
      ...options.headers
    },
    body: options.body ? JSON.stringify(options.body) : undefined
  });
  if (!response.ok) {
    const error = await response.text();
    throw new Error(`Airtable API Error (${response.status}): ${error}`);
  }
  return await response.json();
}

// ============================================================================
// AIRTABLE STUDENT REGISTRATION FORM — fetch & cache (5 min)
// ============================================================================
let _regCache = { data: null, ts: 0 };
const REG_CACHE_TTL = 15 * 60 * 1000; // 15 minutes

async function fetchRegistrationStudents() {
  if (_regCache.data && Date.now() - _regCache.ts < REG_CACHE_TTL) {
    return _regCache.data;
  }
  if (!AIRTABLE_API_KEY) return {};
  try {
    const allRecords = [];
    let offset = null;
    do {
      const params = new URLSearchParams({ pageSize: '100' });
      if (offset) params.set('offset', offset);
      const url = `https://api.airtable.com/v0/${AIRTABLE_REG_BASE_ID}/${AIRTABLE_REG_TABLE_ID}?${params}`;
      const resp = await fetch(url, {
        headers: { 'Authorization': `Bearer ${AIRTABLE_API_KEY}` }
      });
      if (!resp.ok) {
        console.error('Airtable registration fetch error:', resp.status, await resp.text());
        return _regCache.data || {};
      }
      const json = await resp.json();
      allRecords.push(...(json.records || []));
      offset = json.offset || null;
    } while (offset);

    // Build lookup: email → { nome, cognome } (latest record wins)
    const lookup = {};
    for (const rec of allRecords) {
      const f = rec.fields;
      const email = (f['E-mail'] || '').toLowerCase().trim();
      if (!email) continue;
      const nome = (f['Nome'] || '').trim();
      const cognome = (f['Cognome'] || '').trim();
      const fullName = [nome, cognome].filter(Boolean).join(' ');
      if (fullName) {
        lookup[email] = { name: fullName, nome, cognome };
      }
    }
    _regCache = { data: lookup, ts: Date.now() };
    console.log(`Airtable registration cache: ${Object.keys(lookup).length} students loaded`);
    return lookup;
  } catch (err) {
    console.error('Airtable registration fetch failed:', err.message);
    return _regCache.data || {};
  }
}

// ============================================================================
// COURSE HANDLE PATTERNS (to filter out non-course products)
// ============================================================================
const COURSE_HANDLE_PATTERNS = [
  'certificato',
  'introduttivo',
  'shochu',
  'masterclass',
  'mixology',
  'bartending',
  'spirits-of-japan'
];

function isCourseProduct(product) {
  const handle = (product.handle || '').toLowerCase();
  // Exclude known non-course products
  if (handle.startsWith('canvas-') || handle.startsWith('poster-') || handle.startsWith('puzzle-') ||
      handle.startsWith('bottiglia-') || handle.startsWith('gift-card') || handle.startsWith('guida-') ||
      handle === 'corsi-ed-eventi-2024-25' || handle.startsWith('copy-of-') ||
      handle.startsWith('bundle-') || handle.startsWith('pacchetto-')) {
    return false;
  }
  return COURSE_HANDLE_PATTERNS.some(pattern => handle.includes(pattern));
}

// ============================================================================
// HEALTH CHECK
// ============================================================================
app.get('/api/health', (req, res) => {
  res.json({
    status: 'ok',
    timestamp: new Date().toISOString(),
    uptime: process.uptime()
  });
});

// ============================================================================
// SHOPIFY ROUTES
// ============================================================================
app.get('/api/shopify/products', async (req, res) => {
  try {
    const data = await fetchAllShopifyProducts();
    res.json({ success: true, count: data.length, data });
  } catch (error) {
    res.status(500).json({ success: false, error: error.message });
  }
});

app.get('/api/shopify/orders', async (req, res) => {
  try {
    const data = await fetchAllShopifyOrders();
    res.json({ success: true, count: data.length, data });
  } catch (error) {
    res.status(500).json({ success: false, error: error.message });
  }
});

app.get('/api/shopify/customers', async (req, res) => {
  try {
    const cacheKey = 'shopify_customers';
    let data = getCache(cacheKey);
    if (!data) {
      const response = await shopifyFetch('/customers.json?limit=250');
      data = response.customers || [];
      setCache(cacheKey, data, 300);
    }
    res.json({ success: true, count: data.length, data });
  } catch (error) {
    res.status(500).json({ success: false, error: error.message });
  }
});

// ============================================================================
// AIRTABLE ROUTES
// ============================================================================
app.get('/api/airtable/sake', async (req, res) => {
  try {
    const cacheKey = 'airtable_sake_products';
    let data = getCache(cacheKey);
    if (!data) {
      // Paginate through all Airtable records (max 100 per page)
      data = [];
      let offset = null;
      do {
        const endpoint = encodeURIComponent(AIRTABLE_TABLE_ID) + '?pageSize=100' + (offset ? `&offset=${offset}` : '');
        const response = await airtableFetch(endpoint);
        data.push(...(response.records || []));
        offset = response.offset || null;
      } while (offset);
      console.log(`Airtable: loaded ${data.length} sake products`);
      setCache(cacheKey, data, 300);
    }
    res.json({ success: true, count: data.length, data });
  } catch (error) {
    res.status(500).json({ success: false, error: error.message });
  }
});

// ============================================================================
// EDUCATOR NAME MATCHING - Extract clean name from multi-line metafield values
// Metafield values look like: "Brunella Bettati\nSake Educator SSA" or
// "Francesco Menestrina. \nEnologo, Sommelier AIS e Sake Sommelier SSA."
// We need to extract just the name and match it to Chi Siamo profiles.
// ============================================================================
function matchEducatorProfile(rawName, profiles) {
  if (!rawName) return { cleanName: '', profile: null };

  // Step 1: Take only the first line
  let cleanName = rawName.split('\n')[0].trim();
  // Step 2: Remove trailing dots, commas and common suffixes
  cleanName = cleanName.replace(/[.,;:]+$/, '').trim();
  // Remove common title suffixes that appear after the name
  cleanName = cleanName.replace(/,\s*(Sake\s+)?Educator.*$/i, '').trim();
  cleanName = cleanName.replace(/,\s*(Sake\s+)?Sommelier.*$/i, '').trim();
  cleanName = cleanName.replace(/,\s*bartender.*$/i, '').trim();
  cleanName = cleanName.replace(/,\s*Head\s+Educator.*$/i, '').trim();
  cleanName = cleanName.replace(/[.,;:]+$/, '').trim();

  // Step 3: Try exact match first
  if (profiles[cleanName]) {
    return { cleanName, profile: profiles[cleanName] };
  }

  // Step 4: Case-insensitive match
  const profileNames = Object.keys(profiles);
  const lowerClean = cleanName.toLowerCase();
  const ciMatch = profileNames.find(n => n.toLowerCase() === lowerClean);
  if (ciMatch) {
    return { cleanName, profile: profiles[ciMatch] };
  }

  // Step 5: Fuzzy match - check if the clean name contains a profile name or vice versa
  const containsMatch = profileNames.find(n =>
    lowerClean.includes(n.toLowerCase()) || n.toLowerCase().includes(lowerClean)
  );
  if (containsMatch) {
    return { cleanName, profile: profiles[containsMatch] };
  }

  // Step 6: Match by surname (last word of each name)
  const cleanWords = cleanName.split(/\s+/);
  if (cleanWords.length >= 2) {
    const cleanSurname = cleanWords[cleanWords.length - 1].toLowerCase();
    const surnameMatch = profileNames.find(n => {
      const words = n.split(/\s+/);
      return words.length >= 2 && words[words.length - 1].toLowerCase() === cleanSurname;
    });
    if (surnameMatch) {
      return { cleanName, profile: profiles[surnameMatch] };
    }
  }

  return { cleanName, profile: null };
}

// ============================================================================
// METAFIELD CACHE - Fetches educator metafields with caching (5 min TTL)
// ============================================================================
let cachedMetafieldMap = null;
let metafieldMapCacheTime = 0;
const METAFIELD_CACHE_TTL = 30 * 60 * 1000; // 30 minutes (metafields rarely change)

async function fetchCourseMetafields(courseProducts) {
  // Return cached if fresh
  if (cachedMetafieldMap && (Date.now() - metafieldMapCacheTime < METAFIELD_CACHE_TTL)) {
    return cachedMetafieldMap;
  }

  const metafieldMap = {};
  const MFBATCH = 12; // Large batches for faster loading
  for (let i = 0; i < courseProducts.length; i += MFBATCH) {
    const batch = courseProducts.slice(i, i + MFBATCH);
    await Promise.all(batch.map(async (product) => {
      try {
        const mfResp = await shopifyFetch(`/products/${product.id}/metafields.json`);
        const metafields = mfResp.metafields || [];
        const entry = {};
        // Look for educator name metafield
        const educatorMf = metafields.find(mf =>
          mf.key === 'sake_educator' || mf.key === 'educator' ||
          (mf.key && mf.key.toLowerCase().includes('educator') && !mf.key.toLowerCase().includes('photo') && !mf.key.toLowerCase().includes('bio') && !mf.key.toLowerCase().includes('image'))
        );
        if (educatorMf && educatorMf.value) {
          entry.name = educatorMf.value;
        }
        // Look for educator photo metafield
        const photoMf = metafields.find(mf =>
          mf.key === 'educator_photo' || mf.key === 'sake_educator_photo' ||
          mf.key === 'educator_image' || mf.key === 'sake_educator_image' ||
          (mf.key && mf.key.toLowerCase().includes('educator') && (mf.key.toLowerCase().includes('photo') || mf.key.toLowerCase().includes('image')))
        );
        if (photoMf && photoMf.value) {
          entry.photo = photoMf.value;
        }
        // Look for educator bio metafield
        const bioMf = metafields.find(mf =>
          mf.key === 'educator_bio' || mf.key === 'sake_educator_bio' ||
          (mf.key && mf.key.toLowerCase().includes('educator') && mf.key.toLowerCase().includes('bio'))
        );
        if (bioMf && bioMf.value) {
          entry.bio = bioMf.value;
        }
        if (Object.keys(entry).length > 0) {
          metafieldMap[product.id] = entry;
        }
      } catch (e) {
        console.error(`Metafield fetch failed for product ${product.id} (${product.title}): ${e.message}`);
      }
    }));
    // Small delay between batches to avoid Shopify rate limits
    if (i + MFBATCH < courseProducts.length) {
      await new Promise(r => setTimeout(r, 100));
    }
  }

  // Cache the result
  if (Object.keys(metafieldMap).length > 0) {
    cachedMetafieldMap = metafieldMap;
    metafieldMapCacheTime = Date.now();
    console.log(`Cached metafields for ${Object.keys(metafieldMap).length} course products`);
  }
  return metafieldMap;
}

// ============================================================================
// COURSES API - Returns ONLY actual courses with FULL Shopify data + orders
// ============================================================================
app.get('/api/courses', async (req, res) => {
  try {
    // Check full response cache first (avoids all processing on repeated requests)
    const fullCacheKey = 'api_courses_full_response';
    const cachedResponse = getCache(fullCacheKey);
    if (cachedResponse && !req.query.nocache) {
      res.set('Cache-Control', 'private, max-age=120');
      return res.json(cachedResponse);
    }

    const apiStart = Date.now();
    // Parallel fetch: Shopify products + orders + educator profiles + Airtable registrations
    const [products, orders, educatorProfiles, registrationLookup] = await Promise.all([
      fetchAllShopifyProducts(),
      fetchAllShopifyOrders(),
      fetchEducatorProfiles(),
      fetchRegistrationStudents()
    ]);
    console.log(`API parallel fetch took ${Date.now() - apiStart}ms`);

    // Filter to only course products
    const courseProducts = products.filter(isCourseProduct);
    // Diagnostic log: helps investigating Shopify<->dashboard course mismatches
    const statusBreakdown = courseProducts.reduce((acc, p) => {
      acc[p.status || 'unknown'] = (acc[p.status || 'unknown'] || 0) + 1;
      return acc;
    }, {});
    console.log(`[courses] shopify_products=${products.length} course_products=${courseProducts.length} status=${JSON.stringify(statusBreakdown)}`);

    // Fetch metafields (cached 5 min) - includes rate limit retry
    const metafieldMap = await fetchCourseMetafields(courseProducts);

    // Build enrollment data from orders
    const courseMap = new Map();
    courseProducts.forEach(product => {
      // Get educator name from metafields (may contain multi-line text with title/description)
      const rawEducatorName = (metafieldMap[product.id] && metafieldMap[product.id].name) || '';
      // Extract clean educator name and look up profile
      const { cleanName: educatorName, profile } = matchEducatorProfile(rawEducatorName, educatorProfiles);

      courseMap.set(product.id, {
        // Pass through ALL Shopify data
        shopifyId: product.id,
        title: product.title,
        handle: product.handle,
        body_html: product.body_html,
        product_type: product.product_type,
        tags: product.tags,
        status: product.status,
        created_at: product.created_at,
        updated_at: product.updated_at,
        published_at: product.published_at,
        variants: product.variants,
        images: product.images,
        // Educator from Shopify metafields + Chi Siamo profile
        educatorName: educatorName,
        educatorPhoto: (profile && profile.photo) || '',
        educatorBio: (profile && profile.bio) || '',
        educatorRegion: (profile && profile.region) || '',
        // Enrollment data
        enrollmentCount: 0,
        revenue: 0,
        students: []
      });
    });

    // Match orders to courses
    orders.forEach(order => {
      if (!order.line_items) return;
      order.line_items.forEach(item => {
        if (courseMap.has(item.product_id)) {
          const course = courseMap.get(item.product_id);
          // Calculate actual paid amount (subtract discounts from gross price)
          const itemDiscounts = (item.discount_allocations || []).reduce((sum, d) => sum + parseFloat(d.amount || 0), 0);
          const actualAmount = (parseFloat(item.price || 0) * item.quantity) - itemDiscounts;

          course.enrollmentCount += item.quantity;
          course.revenue += actualAmount;

          const customerName = order.customer
            ? `${order.customer.first_name || ''} ${order.customer.last_name || ''}`.trim()
            : 'Sconosciuto';

          // Better phone collection: check customer phone, fallback to shipping/billing addresses
          let phone = order.customer?.phone || '';
          if (!phone && order.shipping_address?.phone) {
            phone = order.shipping_address.phone;
          }
          if (!phone && order.billing_address?.phone) {
            phone = order.billing_address.phone;
          }
          if (!phone && order.customer?.default_address?.phone) {
            phone = order.customer.default_address.phone;
          }

          // Extract discount reason from order's discount_applications
          let discountCode = '';
          if (item.discount_allocations && item.discount_allocations.length > 0) {
            const discApps = order.discount_applications || [];
            const reasons = item.discount_allocations.map(da => {
              const app = discApps[da.discount_application_index];
              if (app) return app.title || app.description || app.code || '';
              return '';
            }).filter(Boolean);
            discountCode = [...new Set(reasons)].join(', ');
          }

          course.students.push({
            name: customerName,
            email: order.customer?.email || '',
            phone: phone,
            orderId: order.id,
            orderNumber: order.name || `#${order.order_number}`,
            orderDate: order.created_at,
            financialStatus: order.financial_status,
            amount: actualAmount,
            grossAmount: parseFloat(item.price || 0) * item.quantity,
            discountCode: discountCode,
            variantTitle: item.variant_title || ''
          });
        }
      });
    });

    const courses = Array.from(courseMap.values());

    // Apply phone overrides from saved config (per-order first, then email fallback)
    courses.forEach(course => {
      const byOrder = courseCosts[course.handle]?.phoneOverridesByOrder;
      const byEmail = courseCosts[course.handle]?.phoneOverrides;
      if ((byOrder || byEmail) && course.students) {
        course.students.forEach(st => {
          // Per-order override takes priority (allows different phones for same email)
          if (byOrder && st.orderNumber && byOrder[st.orderNumber]) {
            st.phone = byOrder[st.orderNumber];
          } else {
            const emailKey = (st.email || '').toLowerCase().trim();
            if (byEmail && emailKey && byEmail[emailKey]) {
              st.phone = byEmail[emailKey];
            }
          }
        });
      }
    });

    // Apply name overrides from saved config (keyed by orderNumber)
    courses.forEach(course => {
      const nameOvr = courseCosts[course.handle]?.nameOverrides;
      if (nameOvr && course.students) {
        course.students.forEach(st => {
          if (st.orderNumber && nameOvr[st.orderNumber]) {
            st.nameOverride = nameOvr[st.orderNumber];
          }
        });
      }
    });

    // Cross-reference with Airtable student registration form (QR code at course)
    if (registrationLookup && Object.keys(registrationLookup).length > 0) {
      courses.forEach(course => {
        if (!course.students) return;
        course.students.forEach(st => {
          const emailKey = (st.email || '').toLowerCase().trim();
          if (emailKey && registrationLookup[emailKey]) {
            const reg = registrationLookup[emailKey];
            st.registrationName = reg.name; // Name from QR code registration
            // Flag mismatch: compare Shopify name vs registration name (case-insensitive)
            const shopifyName = (st.name || '').toLowerCase().trim();
            const regName = (reg.name || '').toLowerCase().trim();
            if (shopifyName && regName && shopifyName !== regName) {
              st.nameMismatch = true;
            }
          }
        });
      });
    }

    // Apply cached Twilio data immediately (non-blocking — no new API calls)
    applyCachedTwilioData(courses);

    const lastUpdated = new Date().toISOString();
    const responseData = { success: true, count: courses.length, data: courses, lastUpdated };
    // Cache the full response for 10 minutes
    setCache(fullCacheKey, responseData, 600);
    console.log(`API /api/courses total time: ${Date.now() - apiStart}ms (${courses.length} courses)`);

    // Minimal mode: strip fields the dashboard does not read to cut payload size by ~60%
    // Activated by ?minimal=1 (or ?lite=1). Kept as an opt-in to preserve compatibility.
    let payload = responseData;
    if (req.query.minimal === '1' || req.query.lite === '1') {
      const slimCourses = courses.map(c => ({
        shopifyId: c.shopifyId,
        title: c.title,
        handle: c.handle,
        tags: c.tags,
        status: c.status,
        created_at: c.created_at,
        published_at: c.published_at,
        updated_at: c.updated_at,
        educatorName: c.educatorName,
        educatorPhoto: c.educatorPhoto,
        educatorBio: c.educatorBio,
        educatorRegion: c.educatorRegion,
        enrollmentCount: c.enrollmentCount,
        revenue: c.revenue,
        // Variants trimmed to just price (frontend reads variants[0].price only)
        variants: (c.variants || []).slice(0, 1).map(v => ({ id: v.id, price: v.price })),
        students: (c.students || []).map(s => ({
          name: s.name,
          email: s.email,
          phone: s.phone,
          orderId: s.orderId,
          orderNumber: s.orderNumber,
          orderDate: s.orderDate,
          financialStatus: s.financialStatus,
          amount: s.amount,
          discountCode: s.discountCode,
          hasWhatsApp: s.hasWhatsApp,
          registrationName: s.registrationName,
          nameMismatch: s.nameMismatch
        }))
      }));
      payload = { success: true, count: slimCourses.length, minimal: true, data: slimCourses, lastUpdated };
    }

    // Browser/CDN cache: 5 min hit, 10 min stale-while-revalidate
    res.set('Cache-Control', 'private, max-age=300, stale-while-revalidate=600');
    res.json(payload);

    // Enrich Twilio data in background (doesn't block the response)
    enrichStudentsWithWhatsApp(courses).then(() => {
      // Update cache with enriched data
      const enrichedData = { success: true, count: courses.length, data: courses, lastUpdated };
      setCache(fullCacheKey, enrichedData, 600);
    }).catch(err => console.log('Background Twilio enrichment failed:', err.message));
  } catch (error) {
    console.error('Error in /api/courses:', error.message);
    res.status(500).json({ success: false, error: error.message });
  }
});

// ============================================================================
// DEBUG: Diff Shopify products vs course filter
// Usage: /api/debug/course-products-diff  or  ?status=archived  ?included=false
// ============================================================================
app.get('/api/debug/course-products-diff', async (req, res) => {
  try {
    const products = await fetchAllShopifyProducts();
    const reasonFor = (p) => {
      const h = (p.handle || '').toLowerCase();
      if (h.startsWith('canvas-')) return 'excluded:canvas-';
      if (h.startsWith('poster-')) return 'excluded:poster-';
      if (h.startsWith('puzzle-')) return 'excluded:puzzle-';
      if (h.startsWith('bottiglia-')) return 'excluded:bottiglia-';
      if (h.startsWith('gift-card')) return 'excluded:gift-card';
      if (h.startsWith('guida-')) return 'excluded:guida-';
      if (h === 'corsi-ed-eventi-2024-25') return 'excluded:corsi-ed-eventi-2024-25';
      if (h.startsWith('copy-of-')) return 'excluded:copy-of-';
      if (h.startsWith('bundle-')) return 'excluded:bundle-';
      if (h.startsWith('pacchetto-')) return 'excluded:pacchetto-';
      const matchedPattern = COURSE_HANDLE_PATTERNS.find(pat => h.includes(pat));
      if (matchedPattern) return `included:pattern=${matchedPattern}`;
      return 'excluded:no-pattern-match';
    };
    const rows = products.map(p => ({
      shopifyId: p.id,
      handle: p.handle,
      title: p.title,
      status: p.status,
      created_at: p.created_at,
      updated_at: p.updated_at,
      reason: reasonFor(p),
      included: isCourseProduct(p)
    }));

    // Optional filters via query string
    let filtered = rows;
    if (req.query.status) filtered = filtered.filter(r => r.status === req.query.status);
    if (req.query.included === 'true') filtered = filtered.filter(r => r.included);
    if (req.query.included === 'false') filtered = filtered.filter(r => !r.included);

    // Summary
    const summary = {
      total_products: products.length,
      included_as_courses: rows.filter(r => r.included).length,
      by_status: rows.reduce((acc, r) => { acc[r.status || 'unknown'] = (acc[r.status || 'unknown'] || 0) + 1; return acc; }, {}),
      by_reason: rows.reduce((acc, r) => { acc[r.reason] = (acc[r.reason] || 0) + 1; return acc; }, {}),
      included_by_status: rows.filter(r => r.included).reduce((acc, r) => { acc[r.status || 'unknown'] = (acc[r.status || 'unknown'] || 0) + 1; return acc; }, {})
    };

    res.json({
      success: true,
      patterns: COURSE_HANDLE_PATTERNS,
      summary,
      count: filtered.length,
      products: filtered.sort((a, b) => (b.updated_at || '').localeCompare(a.updated_at || ''))
    });
  } catch (error) {
    res.status(500).json({ success: false, error: error.message });
  }
});

// ============================================================================
// DEBUG: Inspect all metafields for a course product (by handle)
// ============================================================================
app.get('/api/debug/metafields/:handle', async (req, res) => {
  try {
    const products = await fetchAllShopifyProducts();
    const product = products.find(p => p.handle === req.params.handle);
    if (!product) return res.status(404).json({ error: 'Product not found', handle: req.params.handle });
    const mfResp = await shopifyFetch(`/products/${product.id}/metafields.json`);
    res.json({
      productId: product.id,
      handle: product.handle,
      title: product.title,
      metafields: (mfResp.metafields || []).map(mf => ({
        namespace: mf.namespace,
        key: mf.key,
        type: mf.type,
        value: mf.value
      }))
    });
  } catch (error) {
    res.status(500).json({ error: error.message });
  }
});

// ============================================================================
// DEBUG: Explore Shopify metaobject definitions and metaobjects (GraphQL)
// ============================================================================
async function shopifyGraphQL(query, variables = {}) {
  const url = `https://${SHOPIFY_STORE}/admin/api/${SHOPIFY_API_VERSION}/graphql.json`;
  const response = await fetch(url, {
    method: 'POST',
    headers: {
      'Content-Type': 'application/json',
      'X-Shopify-Access-Token': SHOPIFY_ACCESS_TOKEN
    },
    body: JSON.stringify({ query, variables })
  });
  if (!response.ok) {
    const text = await response.text();
    throw new Error(`Shopify GraphQL error ${response.status}: ${text}`);
  }
  return response.json();
}

// List all metaobject definitions
app.get('/api/debug/metaobject-definitions', async (req, res) => {
  try {
    const result = await shopifyGraphQL(`{
      metaobjectDefinitions(first: 50) {
        edges {
          node {
            id
            name
            type
            fieldDefinitions {
              key
              name
              type { name }
            }
          }
        }
      }
    }`);
    res.json(result.data);
  } catch (error) {
    res.status(500).json({ error: error.message });
  }
});

// List all metaobjects of a given type
app.get('/api/debug/metaobjects/:type', async (req, res) => {
  try {
    const type = req.params.type;
    const result = await shopifyGraphQL(`{
      metaobjects(type: "${type}", first: 50) {
        edges {
          node {
            id
            handle
            type
            fields {
              key
              value
              type
              reference {
                ... on MediaImage {
                  image {
                    url
                    altText
                  }
                }
              }
            }
          }
        }
      }
    }`);
    res.json(result.data);
  } catch (error) {
    res.status(500).json({ error: error.message });
  }
});

// DEBUG: Check Shopify pages for educator content
app.get('/api/debug/shopify-pages', async (req, res) => {
  try {
    const resp = await shopifyFetch('/pages.json?limit=250');
    res.json((resp.pages || []).map(p => ({
      id: p.id, title: p.title, handle: p.handle,
      body_snippet: (p.body_html || '').substring(0, 200)
    })));
  } catch (error) {
    res.status(500).json({ error: error.message });
  }
});

// DEBUG: Check Shopify blogs and articles
app.get('/api/debug/shopify-blogs', async (req, res) => {
  try {
    const blogsResp = await shopifyFetch('/blogs.json');
    const blogs = blogsResp.blogs || [];
    const result = [];
    for (const blog of blogs) {
      const articlesResp = await shopifyFetch(`/blogs/${blog.id}/articles.json?limit=50`);
      result.push({
        blog: { id: blog.id, title: blog.title, handle: blog.handle },
        articles: (articlesResp.articles || []).map(a => ({
          id: a.id, title: a.title, handle: a.handle, author: a.author,
          tags: a.tags, image: a.image?.src,
          body_snippet: (a.body_html || '').substring(0, 200)
        }))
      });
    }
    res.json(result);
  } catch (error) {
    res.status(500).json({ error: error.message });
  }
});

// DEBUG: Search shop-level metafields for educator data
app.get('/api/debug/shop-metafields', async (req, res) => {
  try {
    const resp = await shopifyFetch('/metafields.json?limit=250');
    res.json((resp.metafields || []).map(mf => ({
      namespace: mf.namespace, key: mf.key, type: mf.type,
      value_snippet: String(mf.value || '').substring(0, 300)
    })));
  } catch (error) {
    res.status(500).json({ error: error.message });
  }
});

// DEBUG: Check theme sections/templates via GraphQL for educator data
app.get('/api/debug/shopify-collections', async (req, res) => {
  try {
    const resp = await shopifyFetch('/custom_collections.json?limit=250');
    const custom = (resp.custom_collections || []).map(c => ({ id: c.id, title: c.title, handle: c.handle }));
    const resp2 = await shopifyFetch('/smart_collections.json?limit=250');
    const smart = (resp2.smart_collections || []).map(c => ({ id: c.id, title: c.title, handle: c.handle }));
    res.json({ custom_collections: custom, smart_collections: smart });
  } catch (error) {
    res.status(500).json({ error: error.message });
  }
});

// ============================================================================
// EDUCATOR PROFILES - scraped from Chi Siamo page on sakesommelierassociation.it
// ============================================================================
let cachedEducatorProfiles = null;
let educatorProfilesCacheTime = 0;
const EDUCATOR_CACHE_TTL = 24 * 60 * 60 * 1000; // 24 hours

async function fetchEducatorProfiles() {
  // Return cached if fresh
  if (cachedEducatorProfiles && (Date.now() - educatorProfilesCacheTime < EDUCATOR_CACHE_TTL)) {
    return cachedEducatorProfiles;
  }
  try {
    const response = await fetch('https://www.sakesommelierassociation.it/pages/chi-siamo');
    if (!response.ok) throw new Error(`HTTP ${response.status}`);
    const html = await response.text();

    // Split HTML at each "multicolumn-card content-container" boundary (exact class)
    const profiles = {};
    const cardChunks = html.split('multicolumn-card content-container');
    console.log(`Chi Siamo: ${html.length} bytes, ${cardChunks.length - 1} educator cards`);
    // Skip first chunk (before the first card)
    for (let i = 1; i < cardChunks.length; i++) {
      const cardHtml = cardChunks[i];

      // Extract image URL (src may start with // or https://)
      const imgMatch = cardHtml.match(/<img[^>]*src="([^"]+)"/i);
      let photoUrl = imgMatch ? imgMatch[1].split('?')[0] : '';
      if (photoUrl.startsWith('//')) photoUrl = 'https:' + photoUrl;

      // Extract name from h3
      const h3Match = cardHtml.match(/<h3[^>]*>([\s\S]*?)<\/h3>/i);
      const name = h3Match ? h3Match[1].replace(/<[^>]+>/g, '').trim() : '';

      // Extract region from first <strong> or <b> in a <p>
      const regionMatch = cardHtml.match(/<p[^>]*>\s*<strong>([\s\S]*?)<\/strong>/i) ||
                          cardHtml.match(/<p[^>]*>\s*<b>([\s\S]*?)<\/b>/i);
      const region = regionMatch ? regionMatch[1].replace(/<[^>]+>/g, '').trim() : '';

      // Extract bio - get all <p> tags, find the one with actual bio text
      const pRegex = /<p[^>]*>([\s\S]*?)<\/p>/gi;
      let pMatch;
      let bio = '';
      while ((pMatch = pRegex.exec(cardHtml)) !== null) {
        const pText = pMatch[1].replace(/<[^>]+>/g, '').trim();
        // Skip empty, short, or region-only paragraphs
        if (!pText || pText.length < 20) continue;
        // If it starts with region text, extract the bio portion after it
        if (region && pText.startsWith(region)) {
          const remainder = pText.substring(region.length).trim();
          if (remainder.length > 20) { bio = remainder; break; }
          continue;
        }
        // Otherwise this is the bio paragraph
        bio = pText;
        break;
      }

      if (name) {
        profiles[name] = { name, photo: photoUrl, region, bio };
      }
    }

    // Only cache if we found profiles (don't cache failures)
    if (Object.keys(profiles).length > 0) {
      cachedEducatorProfiles = profiles;
      educatorProfilesCacheTime = Date.now();
    }
    console.log(`Fetched ${Object.keys(profiles).length} educator profiles from Chi Siamo page`);
    return profiles;
  } catch (error) {
    console.error('Error fetching educator profiles:', error.message);
    // Return cached even if stale, or empty
    return cachedEducatorProfiles || {};
  }
}

// API endpoint to get all educator profiles
app.get('/api/educator-profiles', async (req, res) => {
  try {
    // ?refresh=1 to force cache refresh
    if (req.query.refresh) {
      cachedEducatorProfiles = null;
      educatorProfilesCacheTime = 0;
    }
    const profiles = await fetchEducatorProfiles();
    res.json({ success: true, count: Object.keys(profiles).length, profiles });
  } catch (error) {
    res.status(500).json({ success: false, error: error.message });
  }
});

// ============================================================================
// EXAM RESULTS ROUTES (read-only, Airtable Socrative pipeline)
// ============================================================================
app.get('/api/exam-results', async (req, res) => {
  try {
    if (req.query.refresh) cache.delete('exam_results');
    const data = await fetchExamResults();
    res.json({ success: true, ...data });
  } catch (error) {
    res.status(500).json({ success: false, error: error.message });
  }
});

app.get('/api/exam-results/by-email/:email', async (req, res) => {
  try {
    const data = await fetchExamResults();
    const key = (req.params.email || '').toLowerCase().trim();
    const entry = data.byEmail[key] || null;
    res.json({ success: true, email: key, found: !!entry, data: entry });
  } catch (error) {
    res.status(500).json({ success: false, error: error.message });
  }
});

// ============================================================================
// HISTORICAL STUDENTS (pre-2024 Eventbrite archive + legacy passed/failed lists)
// Data file produced by scripts/import-historical.py; served as-is.
// ============================================================================
const HISTORICAL_FILE = path.join(__dirname, 'data', 'historical-students.json');
let _historicalCache = null;
let _historicalMtime = 0;

function loadHistoricalStudents() {
  try {
    const stats = fs.statSync(HISTORICAL_FILE);
    if (_historicalCache && stats.mtimeMs === _historicalMtime) return _historicalCache;
    const raw = fs.readFileSync(HISTORICAL_FILE, 'utf-8');
    _historicalCache = JSON.parse(raw);
    _historicalMtime = stats.mtimeMs;
    return _historicalCache;
  } catch (e) {
    return { count: 0, students: [], error: e.code === 'ENOENT' ? 'file-not-found' : e.message };
  }
}

app.get('/api/historical-students', (req, res) => {
  const data = loadHistoricalStudents();
  res.set('Cache-Control', 'private, max-age=600');
  res.json({ success: !data.error, ...data });
});

// ============================================================================
// SAKE TEMPLATES — canonical sake program per course type.
// Stored in the SSA_CourseConfig Airtable table under key 'sake_templates'.
// Shape: { certificato: [sakes], introduttivo: [sakes], shochu: [sakes], masterclass: [sakes] }
// Each sake item has the same shape used inside program groups, so
// apply-template is a direct copy into the course's program.
// ============================================================================

const SAKE_TEMPLATE_KEYS = ['certificato', 'introduttivo', 'shochu', 'masterclass'];

app.get('/api/sake-templates', async (req, res) => {
  try {
    const stored = await airtableConfigGet('sake_templates');
    const result = {};
    for (const k of SAKE_TEMPLATE_KEYS) {
      result[k] = (stored && Array.isArray(stored[k])) ? stored[k] : [];
    }
    res.set('Cache-Control', 'private, max-age=60');
    res.json({ success: true, templates: result });
  } catch (err) {
    res.status(500).json({ success: false, error: err.message });
  }
});

app.post('/api/sake-templates/:type', async (req, res) => {
  try {
    const type = (req.params.type || '').toLowerCase();
    if (!SAKE_TEMPLATE_KEYS.includes(type)) {
      return res.status(400).json({ success: false, error: 'unknown type' });
    }
    const incoming = (req.body && Array.isArray(req.body.sakes)) ? req.body.sakes : null;
    if (!incoming) {
      return res.status(400).json({ success: false, error: 'sakes[] required' });
    }
    const stored = (await airtableConfigGet('sake_templates')) || {};
    stored[type] = incoming.map(s => ({
      id: s.id, code: s.code, name: s.name, nameJp: s.nameJp || '',
      type: s.type || '', sakagura: s.sakagura || '', size: s.size,
      cost: parseFloat(s.cost) || 0, qty: parseInt(s.qty, 10) || 1,
      note: s.note || '', image: s.image || '',
    }));
    await airtableConfigSet('sake_templates', stored);
    res.json({ success: true, type, count: stored[type].length });
  } catch (err) {
    res.status(500).json({ success: false, error: err.message });
  }
});

// ============================================================================
// RECOMMENDATIONS (data-driven, transparent, no ML)
// Reads already-cached course list, splits past/future, builds per-segment
// historical baseline, emits verdict + reasoning per upcoming course.
// ============================================================================
const recommendations = require('./recommendations');

// Mirror the frontend parseCourseDate(handle) month-from-handle logic.
// Returns a midpoint Date for the course month (day 15) so analyse()
// can put it on the past/future timeline. If no month in handle, falls
// back to created_at.
const ITALIAN_MONTHS = {
  gennaio: 1, febbraio: 2, marzo: 3, aprile: 4, maggio: 5, giugno: 6,
  luglio: 7, agosto: 8, settembre: 9, ottobre: 10, novembre: 11, dicembre: 12,
};

function getCourseEventDate(course) {
  const h = (course.handle || '').toLowerCase();
  for (const [name, num] of Object.entries(ITALIAN_MONTHS)) {
    if (h.includes(name)) {
      const m = h.match(/(\d{4})/);
      if (m) {
        return new Date(Date.UTC(parseInt(m[1], 10), num - 1, 15));
      }
    }
  }
  if (course.created_at) return new Date(course.created_at);
  return null;
}

app.get('/api/recommendations', async (req, res) => {
  try {
    const cacheKey = 'recommendations';
    if (req.query.refresh) cache.delete(cacheKey);
    const cached = getCache(cacheKey);
    if (cached) return res.json({ success: true, ...cached });

    // Reuse the cached /api/courses full response. If absent, re-fetch raw
    // products + orders minimally (without the full Twilio/educator pipeline
    // — recommendations only need enrolment counts and dates).
    const fullCacheKey = 'api_courses_full_response';
    const fullCache = getCache(fullCacheKey);
    let courses;
    if (fullCache && Array.isArray(fullCache.data)) {
      courses = fullCache.data;
    } else {
      const products = await fetchAllShopifyProducts();
      const orders = await fetchAllShopifyOrders();
      courses = products.filter(isCourseProduct).map(p => {
        const enrol = [];
        let revenue = 0;
        orders.forEach(o => (o.line_items || []).forEach(li => {
          if (li.product_id === p.id) {
            enrol.push({ email: (o.customer && o.customer.email) || '' });
            revenue += parseFloat(li.price || 0) * (li.quantity || 1);
          }
        }));
        return {
          handle: p.handle, title: p.title, created_at: p.created_at,
          published_at: p.published_at, tags: p.tags, status: p.status,
          educatorName: '',
          students: enrol, enrollmentCount: enrol.length, revenue,
        };
      });
    }

    const result = recommendations.analyse(courses, new Date(), getCourseEventDate);
    setCache(cacheKey, result, 900); // 15 min
    res.set('Cache-Control', 'private, max-age=300');
    res.json({ success: true, ...result });
  } catch (err) {
    console.error('Recommendations failed:', err.message);
    res.status(500).json({ success: false, error: err.message });
  }
});

// ============================================================================
// EDUCATOR ROUTE
// ============================================================================
app.get('/api/educator/:id', async (req, res) => {
  try {
    const educatorId = req.params.id;
    const products = await fetchAllShopifyProducts();
    const orders = await fetchAllShopifyOrders();
    const educatorCourses = products.filter(product =>
      product.vendor?.toLowerCase() === educatorId.toLowerCase() ||
      product.tags?.includes(`educator:${educatorId}`)
    );
    const courseData = [];
    const studentSet = new Set();
    educatorCourses.forEach(product => {
      const course = { id: product.id, name: product.title, handle: product.handle, enrollmentCount: 0, revenue: 0 };
      orders.forEach(order => {
        order.line_items?.forEach(item => {
          if (item.product_id === product.id) {
            const edItemDiscounts = (item.discount_allocations || []).reduce((sum, d) => sum + parseFloat(d.amount || 0), 0);
            course.enrollmentCount += item.quantity;
            course.revenue += (parseFloat(item.price || 0) * item.quantity) - edItemDiscounts;
            if (order.customer?.email) {
              studentSet.add(JSON.stringify({ name: `${order.customer.first_name} ${order.customer.last_name}`, email: order.customer.email }));
            }
          }
        });
      });
      courseData.push(course);
    });
    const students = Array.from(studentSet).map(s => JSON.parse(s));
    res.json({ success: true, educatorId, coursesCount: courseData.length, studentsCount: students.length, courses: courseData, students });
  } catch (error) {
    res.status(500).json({ success: false, error: error.message });
  }
});

// ============================================================================
// EXPORT CSV
// ============================================================================
app.get('/api/export/corsisti', async (req, res) => {
  try {
    const response = await shopifyFetch('/customers.json?limit=250');
    const customers = response.customers || [];
    let csv = 'ID,First Name,Last Name,Email,Phone,City,State,Country,Total Orders,Total Spent\n';
    customers.forEach(customer => {
      const id = customer.id;
      const firstName = (customer.first_name || '').replace(/"/g, '""');
      const lastName = (customer.last_name || '').replace(/"/g, '""');
      const email = customer.email || '';
      const phone = customer.phone || '';
      const city = (customer.default_address?.city || '').replace(/"/g, '""');
      const state = (customer.default_address?.province || '').replace(/"/g, '""');
      const country = (customer.default_address?.country || '').replace(/"/g, '""');
      const totalOrders = customer.orders_count || 0;
      const totalSpent = customer.total_spent || 0;
      csv += `"${id}","${firstName}","${lastName}","${email}","${phone}","${city}","${state}","${country}",${totalOrders},${totalSpent}\n`;
    });
    res.setHeader('Content-Type', 'text/csv');
    res.setHeader('Content-Disposition', 'attachment; filename="corsisti-export.csv"');
    res.send(csv);
  } catch (error) {
    res.status(500).json({ success: false, error: error.message });
  }
});

// ============================================================================
// COURSE EXPORT WITH ADDRESSES (JSON, for Excel generation)
// ============================================================================
app.get('/api/export/course/:handle', async (req, res) => {
  try {
    const handle = req.params.handle;
    const products = await fetchAllShopifyProducts();
    const orders = await fetchAllShopifyOrders();
    const product = products.find(p => p.handle === handle);
    if (!product) return res.status(404).json({ success: false, error: 'Course not found' });

    const students = [];
    orders.forEach(order => {
      if (!order.line_items) return;
      order.line_items.forEach(item => {
        if (item.product_id === product.id) {
          const addr = order.shipping_address || order.billing_address || order.customer?.default_address || {};
          students.push({
            name: order.customer ? `${order.customer.first_name || ''} ${order.customer.last_name || ''}`.trim() : 'Sconosciuto',
            email: order.customer?.email || '',
            phone: order.customer?.phone || addr.phone || '',
            address1: addr.address1 || '',
            address2: addr.address2 || '',
            city: addr.city || '',
            zip: addr.zip || '',
            province: addr.province || '',
            country: addr.country || '',
            orderNumber: order.name || '',
            orderDate: order.created_at,
            amount: (parseFloat(item.price || 0) * item.quantity) - (item.discount_allocations || []).reduce((s, d) => s + parseFloat(d.amount || 0), 0)
          });
        }
      });
    });

    // Apply phone overrides (per-order first, then email fallback)
    const pByOrder = courseCosts[handle]?.phoneOverridesByOrder;
    const pByEmail = courseCosts[handle]?.phoneOverrides;
    if (pByOrder || pByEmail) {
      students.forEach(st => {
        if (pByOrder && st.orderNumber && pByOrder[st.orderNumber]) {
          st.phone = pByOrder[st.orderNumber];
        } else {
          const ek = (st.email || '').toLowerCase().trim();
          if (pByEmail && ek && pByEmail[ek]) st.phone = pByEmail[ek];
        }
      });
    }

    // Deduplicate
    const seen = new Set();
    const unique = students.filter(st => {
      const key = (st.email || st.name).toLowerCase().trim();
      if (seen.has(key)) return false;
      seen.add(key);
      return true;
    });

    // Generate XLSX if format=xlsx requested, otherwise JSON
    if (req.query.format === 'xlsx') {
      const wb = new ExcelJS.Workbook();
      wb.creator = 'SSA Gestione Corsi';
      const ws = wb.addWorksheet('Iscritti');

      // Header row
      ws.columns = [
        { header: 'Nome', key: 'name', width: 25 },
        { header: 'Email', key: 'email', width: 30 },
        { header: 'Telefono', key: 'phone', width: 18 },
        { header: 'Indirizzo', key: 'address', width: 35 },
        { header: 'CAP', key: 'zip', width: 8 },
        { header: 'Città', key: 'city', width: 18 },
        { header: 'Provincia', key: 'province', width: 12 },
        { header: 'Paese', key: 'country', width: 12 },
        { header: 'Ordine', key: 'orderNumber', width: 12 },
        { header: 'Data Ordine', key: 'orderDate', width: 14 },
        { header: 'Importo €', key: 'amount', width: 12 }
      ];

      // Style header
      ws.getRow(1).font = { bold: true, color: { argb: 'FFFFFFFF' } };
      ws.getRow(1).fill = { type: 'pattern', pattern: 'solid', fgColor: { argb: 'FF0D5CAB' } };
      ws.getRow(1).alignment = { horizontal: 'center' };

      // Data rows
      unique.forEach(st => {
        ws.addRow({
          name: st.name,
          email: st.email,
          phone: st.phone,
          address: [st.address1, st.address2].filter(Boolean).join(', '),
          zip: st.zip,
          city: st.city,
          province: st.province,
          country: st.country,
          orderNumber: st.orderNumber,
          orderDate: st.orderDate ? new Date(st.orderDate).toLocaleDateString('it-IT') : '',
          amount: st.amount ? parseFloat(st.amount.toFixed(2)) : 0
        });
      });

      // Format amount column as currency
      ws.getColumn('amount').numFmt = '#,##0.00 €';

      // Auto-filter
      ws.autoFilter = { from: 'A1', to: `K${unique.length + 1}` };

      // Add course title as sheet header
      ws.headerFooter.oddHeader = `&C&B${product.title}`;

      const safeName = product.title.replace(/[^a-zA-Z0-9àèéìòù\s-]/g, '').replace(/\s+/g, '-').substring(0, 50);
      res.setHeader('Content-Type', 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet');
      res.setHeader('Content-Disposition', `attachment; filename="iscritti-${safeName}.xlsx"`);
      await wb.xlsx.write(res);
      res.end();
    } else {
      res.json({ success: true, course: product.title, handle, students: unique });
    }
  } catch (error) {
    res.status(500).json({ success: false, error: error.message });
  }
});

// ============================================================================
// SAKE PROGRAM EXPORT (Excel with sake list for a course)
// ============================================================================
app.get('/api/export/sake/:handle', async (req, res) => {
  try {
    const handle = req.params.handle;
    const costs = courseCosts[handle];
    const program = costs?.program || [];

    // Get course info
    const products = await fetchAllShopifyProducts();
    const product = products.find(p => p.handle === handle);
    const courseTitle = product ? product.title : handle;

    // Parse date and city from handle
    const months = {'gennaio':'Gennaio','febbraio':'Febbraio','marzo':'Marzo','aprile':'Aprile','maggio':'Maggio','giugno':'Giugno','luglio':'Luglio','agosto':'Agosto','settembre':'Settembre','ottobre':'Ottobre','novembre':'Novembre','dicembre':'Dicembre'};
    let courseDate = '';
    const h = handle.toLowerCase();
    for (const [mName, mLabel] of Object.entries(months)) {
      if (h.includes(mName)) {
        const ym = h.match(/(\d{4})/);
        courseDate = ym ? `${mLabel} ${ym[1]}` : mLabel;
        break;
      }
    }
    if (!courseDate && product?.created_at) {
      const d = new Date(product.created_at);
      courseDate = d.toLocaleDateString('it-IT', { day: 'numeric', month: 'long', year: 'numeric' });
    }

    // Get educator name
    let educator = costs?.educatorName || '';
    if (!educator && product) {
      const metafieldMap = await fetchCourseMetafields([product]);
      if (metafieldMap[product.id]?.name) educator = metafieldMap[product.id].name;
    }

    // Get city/address
    const cities = {'milano':'Milano','roma':'Roma','torino':'Torino','napoli':'Napoli','bolzano':'Bolzano','vercelli':'Vercelli','firenze':'Firenze','castelfranco':'Castelfranco Veneto','tortona':'Tortona','colli-del-tronto':'Colli del Tronto','piacenza':'Piacenza'};
    let courseCity = '';
    for (const [key, city] of Object.entries(cities)) {
      if (h.includes(key)) { courseCity = city; break; }
    }
    if (h.includes('online')) courseCity = 'Online';

    // Build Excel
    const wb = new ExcelJS.Workbook();
    wb.creator = 'SSA Gestione Corsi';
    const ws = wb.addWorksheet('Sake Programma');

    // Row 1: Course name
    ws.mergeCells('A1:C1');
    ws.getCell('A1').value = courseTitle;
    ws.getCell('A1').font = { bold: true, size: 14 };

    // Row 2: Date
    ws.mergeCells('A2:C2');
    ws.getCell('A2').value = `Data: ${courseDate || 'Da definire'}`;
    ws.getCell('A2').font = { size: 11, color: { argb: 'FF555555' } };

    // Row 3: Address/City
    ws.mergeCells('A3:C3');
    ws.getCell('A3').value = `Luogo: ${courseCity || 'Da definire'}`;
    ws.getCell('A3').font = { size: 11, color: { argb: 'FF555555' } };

    // Row 4: Educator
    ws.mergeCells('A4:C4');
    ws.getCell('A4').value = `Educator: ${educator || 'Da assegnare'}`;
    ws.getCell('A4').font = { size: 11, color: { argb: 'FF555555' } };

    // Row 5: Empty separator
    ws.addRow([]);

    // Row 6: Headers
    const headerRow = ws.addRow(['Codice', 'Nome Sake', 'N. Bottiglie']);
    headerRow.font = { bold: true, color: { argb: 'FFFFFFFF' } };
    headerRow.fill = { type: 'pattern', pattern: 'solid', fgColor: { argb: 'FF2563EB' } };
    headerRow.alignment = { horizontal: 'center' };

    // Column widths
    ws.getColumn(1).width = 16;
    ws.getColumn(2).width = 45;
    ws.getColumn(3).width = 14;

    // Data rows: flatten all groups → sakes
    let totalBottles = 0;
    program.forEach(group => {
      (group.sakes || []).forEach(sake => {
        ws.addRow([
          sake.code || sake.sku || '',
          sake.name || sake.title || '',
          sake.qty || 1
        ]);
        totalBottles += (sake.qty || 1);
      });
    });

    // Total row
    const totalRow = ws.addRow(['', 'TOTALE BOTTIGLIE', totalBottles]);
    totalRow.font = { bold: true };
    totalRow.getCell(3).alignment = { horizontal: 'center' };

    // Format
    ws.getColumn(3).alignment = { horizontal: 'center' };

    const safeName = courseTitle.replace(/[^a-zA-Z0-9àèéìòù\s-]/g, '').replace(/\s+/g, '-').substring(0, 50);
    res.setHeader('Content-Type', 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet');
    res.setHeader('Content-Disposition', `attachment; filename="sake-${safeName}.xlsx"`);
    await wb.xlsx.write(res);
    res.end();
  } catch (error) {
    console.error('Sake export error:', error);
    res.status(500).json({ success: false, error: error.message });
  }
});

// ============================================================================
// COURSE COSTS API
// ============================================================================
app.get('/api/costs', (req, res) => {
  res.json({ success: true, data: courseCosts });
});

app.get('/api/costs/:courseId', (req, res) => {
  const costs = courseCosts[req.params.courseId] || null;
  res.json(costs);
});

app.post('/api/costs/:courseId', (req, res) => {
  const { courseId } = req.params;
  const { location, educator, food, sake, adv, program, lines, educatorName, whatsappGroupLink } = req.body;
  const existing = courseCosts[courseId] || {};
  const next = { ...existing }; // preserve sibling fields (notebook, phoneOverrides, nameOverrides, fatturato, ...)
  if (location !== undefined) next.location = parseFloat(location) || 0;
  if (educator !== undefined) next.educator = parseFloat(educator) || 0;
  if (food !== undefined) next.food = parseFloat(food) || 0;
  if (sake !== undefined) next.sake = parseFloat(sake) || 0;
  if (adv !== undefined) next.adv = parseFloat(adv) || 0;
  if (program !== undefined) next.program = program;
  if (lines !== undefined) next.lines = lines;
  if (educatorName !== undefined) next.educatorName = educatorName;
  if (whatsappGroupLink !== undefined) next.whatsappGroupLink = whatsappGroupLink;
  courseCosts[courseId] = next;
  saveCostsToFile(courseCosts);
  res.json({ success: true, costs: courseCosts[courseId] });
});

// ============================================================================
// NOTEBOOK — admin-facing (authenticated) notes + tags + planned action
// Per-course subobject persisted inside courseCosts[handle].notebook.
// Structure:
//   {
//     notes:          [{ id, author:"admin", text, createdAt, updatedAt, visibility:"admin" }, ...]
//     educatorNotes:  [{ id, shareToken, educatorName, text, createdAt, updatedAt }, ...]
//     tags:           ["tag1", "tag2"]
//     plannedAction:  "monitor" | "spingere" | "posticipare" | "cancellare" | "in-traiettoria" | null
//     plannedActionDeadline: "YYYY-MM-DD" | null
//   }
// ============================================================================

// Append a single admin note
app.post('/api/notebook/:courseId/notes', (req, res) => {
  try {
    const courseId = req.params.courseId;
    const { text } = req.body || {};
    if (!text || typeof text !== 'string' || !text.trim()) {
      return res.status(400).json({ success: false, error: 'text required' });
    }
    const existing = courseCosts[courseId] || {};
    const notebook = existing.notebook || {};
    const notes = Array.isArray(notebook.notes) ? notebook.notes.slice() : [];
    const note = {
      id: crypto.randomUUID(),
      author: 'admin',
      text: text.trim(),
      createdAt: new Date().toISOString(),
      updatedAt: null,
      visibility: 'admin',
    };
    notes.push(note);
    courseCosts[courseId] = { ...existing, notebook: { ...notebook, notes } };
    saveCostsToFile(courseCosts);
    airtableConfigSet('course_costs', courseCosts).catch(() => {});
    res.json({ success: true, note });
  } catch (e) {
    res.status(500).json({ success: false, error: e.message });
  }
});

// Delete an admin note by id
app.delete('/api/notebook/:courseId/notes/:noteId', (req, res) => {
  try {
    const { courseId, noteId } = req.params;
    const existing = courseCosts[courseId];
    if (!existing || !existing.notebook) return res.status(404).json({ success: false });
    const notes = (existing.notebook.notes || []).filter(n => n.id !== noteId);
    courseCosts[courseId] = { ...existing, notebook: { ...existing.notebook, notes } };
    saveCostsToFile(courseCosts);
    airtableConfigSet('course_costs', courseCosts).catch(() => {});
    res.json({ success: true });
  } catch (e) {
    res.status(500).json({ success: false, error: e.message });
  }
});

// Set tags and plannedAction (atomic PATCH to avoid races)
app.post('/api/notebook/:courseId/meta', (req, res) => {
  try {
    const courseId = req.params.courseId;
    const { tags, plannedAction, plannedActionDeadline } = req.body || {};
    const existing = courseCosts[courseId] || {};
    const notebook = existing.notebook || {};
    const next = { ...notebook };
    if (Array.isArray(tags)) next.tags = tags.filter(t => typeof t === 'string').slice(0, 10);
    if (typeof plannedAction === 'string' || plannedAction === null) next.plannedAction = plannedAction || null;
    if (typeof plannedActionDeadline === 'string' || plannedActionDeadline === null) {
      next.plannedActionDeadline = plannedActionDeadline || null;
    }
    courseCosts[courseId] = { ...existing, notebook: next };
    saveCostsToFile(courseCosts);
    airtableConfigSet('course_costs', courseCosts).catch(() => {});
    res.json({ success: true, notebook: next });
  } catch (e) {
    res.status(500).json({ success: false, error: e.message });
  }
});

// Phone overrides: save corrected phone numbers (keyed by email)
app.post('/api/phone-overrides/:courseId', (req, res) => {
  const { courseId } = req.params;
  const { email, phone, orderNumber } = req.body;
  if (!email && !orderNumber) return res.json({ success: false, error: 'email or orderNumber required' });
  if (!courseCosts[courseId]) courseCosts[courseId] = {};

  // Prefer per-order phone overrides (allows different phones for same email)
  if (orderNumber) {
    if (!courseCosts[courseId].phoneOverridesByOrder) courseCosts[courseId].phoneOverridesByOrder = {};
    courseCosts[courseId].phoneOverridesByOrder[orderNumber] = phone;
  } else {
    // Fallback: email-based (legacy)
    if (!courseCosts[courseId].phoneOverrides) courseCosts[courseId].phoneOverrides = {};
    courseCosts[courseId].phoneOverrides[email.toLowerCase().trim()] = phone;
  }
  saveCostsToFile(courseCosts);
  res.json({ success: true });
});

// Name override per student (keyed by orderNumber)
app.post('/api/name-overrides/:courseId', (req, res) => {
  const { courseId } = req.params;
  const { orderNumber, name } = req.body;
  if (!orderNumber) return res.json({ success: false, error: 'orderNumber required' });
  if (!courseCosts[courseId]) courseCosts[courseId] = {};
  if (!courseCosts[courseId].nameOverrides) courseCosts[courseId].nameOverrides = {};
  if (name === null || name === undefined) {
    delete courseCosts[courseId].nameOverrides[orderNumber];
  } else {
    courseCosts[courseId].nameOverrides[orderNumber] = name;
  }
  saveCostsToFile(courseCosts);
  res.json({ success: true });
});

// Toggle "fatturato" (invoiced) status for a course
app.post('/api/fatturato/:courseId', (req, res) => {
  const { courseId } = req.params;
  const { fatturato } = req.body; // boolean
  if (!courseCosts[courseId]) courseCosts[courseId] = {};
  courseCosts[courseId].fatturato = !!fatturato;
  saveCostsToFile(courseCosts);
  res.json({ success: true, fatturato: !!fatturato });
});

// ============================================================================
// TWILIO LOOKUP V2 PHONE VERIFICATION
// ============================================================================
const twilioLookupCache = {};        // { phone: true|false }
const TWILIO_CACHE_TTL = 30 * 24 * 60 * 60 * 1000; // 30 days
const twilioLookupTimestamps = {};   // { phone: timestamp }

// Check if Twilio credentials are configured
function hasTwilioConfig() {
  return !!(process.env.TWILIO_ACCOUNT_SID && process.env.TWILIO_AUTH_TOKEN);
}

// Core: check a single phone number via Twilio Lookup v2
async function checkSingleTwilioLookup(phoneWithPlus) {
  // Return cached result if still valid
  if (twilioLookupCache[phoneWithPlus] !== undefined && twilioLookupTimestamps[phoneWithPlus] &&
      Date.now() - twilioLookupTimestamps[phoneWithPlus] < TWILIO_CACHE_TTL) {
    return twilioLookupCache[phoneWithPlus];
  }

  try {
    const accountSid = process.env.TWILIO_ACCOUNT_SID;
    const authToken = process.env.TWILIO_AUTH_TOKEN;
    const auth = Buffer.from(`${accountSid}:${authToken}`).toString('base64');

    const url = `https://lookups.twilio.com/v2/PhoneNumbers/${encodeURIComponent(phoneWithPlus)}?Fields=line_type_intelligence`;
    const resp = await fetch(url, {
      method: 'GET',
      headers: {
        'Authorization': `Basic ${auth}`,
        'User-Agent': 'SSA-Dashboard/1.0'
      },
      signal: AbortSignal.timeout(8000)
    });

    if (!resp.ok) {
      console.debug(`Twilio Lookup failed for ${phoneWithPlus}: ${resp.status}`);
      return null; // error - don't cache
    }

    const data = await resp.json();
    const lineType = data.line_type_intelligence?.type;
    const isMobile = lineType === 'mobile' || lineType === 'voip';

    twilioLookupCache[phoneWithPlus] = isMobile;
    twilioLookupTimestamps[phoneWithPlus] = Date.now();
    return isMobile;
  } catch (e) {
    console.debug(`Twilio Lookup error for ${phoneWithPlus}:`, e.message);
    return null; // error - don't cache
  }
}

// Apply ONLY cached Twilio data (no API calls — instant)
function applyCachedTwilioData(courses) {
  if (!hasTwilioConfig()) return;
  let applied = 0;
  courses.forEach(course => {
    (course.students || []).forEach(st => {
      if (!st.phone) { st.hasWhatsApp = undefined; return; }
      const phone = st.phone.trim();
      if (!phone.startsWith('+')) { st.hasWhatsApp = undefined; return; }
      if (twilioLookupCache[phone] !== undefined && twilioLookupTimestamps[phone] &&
          Date.now() - twilioLookupTimestamps[phone] < TWILIO_CACHE_TTL) {
        st.hasWhatsApp = twilioLookupCache[phone];
        applied++;
      } else {
        st.hasWhatsApp = undefined;
      }
    });
  });
  console.log(`Applied ${applied} cached Twilio results`);
}

// Batch check: enrich an array of students with hasWhatsApp field
// Runs in parallel batches of 5, non-blocking
async function enrichStudentsWithWhatsApp(courses) {
  // Skip if Twilio not configured
  if (!hasTwilioConfig()) {
    return;
  }

  // Collect unique phone numbers across all courses (only those with + prefix)
  const phoneSet = new Map(); // phoneWithPlus → [{ course, studentIndex }]
  courses.forEach(course => {
    (course.students || []).forEach((st, idx) => {
      if (!st.phone) { st.hasWhatsApp = undefined; return; }
      const phone = st.phone.trim();
      // Only check numbers with + prefix
      if (!phone.startsWith('+')) { st.hasWhatsApp = undefined; return; }
      // If cached, apply immediately
      if (twilioLookupCache[phone] !== undefined && twilioLookupTimestamps[phone] &&
          Date.now() - twilioLookupTimestamps[phone] < TWILIO_CACHE_TTL) {
        st.hasWhatsApp = twilioLookupCache[phone];
        return;
      }
      if (!phoneSet.has(phone)) phoneSet.set(phone, []);
      phoneSet.get(phone).push({ course, idx });
    });
  });

  const toCheck = Array.from(phoneSet.keys());
  if (toCheck.length === 0) return;

  console.log(`Twilio Lookup: ${toCheck.length} numbers to verify (${Object.keys(twilioLookupCache).length} cached)`);

  let mobileCount = 0, nonMobileCount = 0, errorCount = 0;

  // Check in parallel batches of 5
  const BATCH = 5;
  for (let i = 0; i < toCheck.length; i += BATCH) {
    const batch = toCheck.slice(i, i + BATCH);
    const results = await Promise.all(batch.map(phone => checkSingleTwilioLookup(phone)));
    batch.forEach((phone, j) => {
      const isMobile = results[j];
      // Apply result to all students with this phone
      phoneSet.get(phone).forEach(({ course, idx }) => {
        course.students[idx].hasWhatsApp = isMobile;
        if (isMobile === true) mobileCount++;
        else if (isMobile === false) nonMobileCount++;
        else errorCount++;
      });
    });
  }

  console.log(`Twilio Lookup: ${mobileCount} mobile, ${nonMobileCount} non-mobile, ${errorCount} errors out of ${toCheck.length} checked`);
}

// ============================================================================
// SAKE COMPANY SHOPIFY (Storefront API - product images)
// ============================================================================
const SAKE_COMPANY_STORE = process.env.SAKE_COMPANY_STORE || 'sake-company.myshopify.com';
const SAKE_COMPANY_STOREFRONT_TOKEN = process.env.SAKE_COMPANY_STOREFRONT_TOKEN || '0ce8a134410b855b96fd6367301785cb';

async function fetchSakeCompanyProducts() {
  const cacheKey = 'sake_company_products';
  let cached = getCache(cacheKey);
  if (cached) return cached;

  const allProducts = [];
  let hasNextPage = true;
  let cursor = null;

  while (hasNextPage) {
    const query = `{
      products(first: 250${cursor ? `, after: "${cursor}"` : ''}) {
        edges {
          node {
            id
            title
            handle
            variants(first: 10) {
              edges {
                node {
                  sku
                  title
                }
              }
            }
            images(first: 3) {
              edges {
                node {
                  url
                  altText
                }
              }
            }
          }
          cursor
        }
        pageInfo {
          hasNextPage
        }
      }
    }`;

    const url = `https://${SAKE_COMPANY_STORE}/api/2024-01/graphql.json`;
    const response = await fetch(url, {
      method: 'POST',
      headers: {
        'X-Shopify-Storefront-Access-Token': SAKE_COMPANY_STOREFRONT_TOKEN,
        'Content-Type': 'application/json'
      },
      body: JSON.stringify({ query })
    });

    if (!response.ok) {
      const error = await response.text();
      throw new Error(`Sake Company Storefront API Error (${response.status}): ${error}`);
    }

    const data = await response.json();
    const edges = data?.data?.products?.edges || [];
    edges.forEach(edge => {
      const node = edge.node;
      const skus = (node.variants?.edges || []).map(v => v.node.sku).filter(Boolean);
      const images = (node.images?.edges || []).map(img => img.node.url);
      allProducts.push({
        id: node.id,
        title: node.title,
        handle: node.handle,
        skus,
        image: images[0] || null,
        images
      });
    });

    hasNextPage = data?.data?.products?.pageInfo?.hasNextPage || false;
    if (edges.length > 0) {
      cursor = edges[edges.length - 1].cursor;
    } else {
      hasNextPage = false;
    }
  }

  // Build SKU-to-image lookup map
  const skuImageMap = {};
  allProducts.forEach(p => {
    if (p.image) {
      p.skus.forEach(sku => {
        if (!skuImageMap[sku]) skuImageMap[sku] = p.image;
      });
    }
  });

  const result = { products: allProducts, skuImageMap };
  setCache(cacheKey, result, 600); // 10 min cache
  return result;
}

app.get('/api/sakecompany/products', async (req, res) => {
  try {
    const data = await fetchSakeCompanyProducts();
    res.json({ success: true, count: data.products.length, data: data.products, skuImageMap: data.skuImageMap });
  } catch (error) {
    console.error('Sake Company API error:', error.message);
    res.status(500).json({ success: false, error: error.message });
  }
});

// ============================================================================
// SHARE TOKEN ROUTES
// ============================================================================

// Helper: parse course end date from title (e.g. "Corso Certificato Sake Sommelier - Maggio 2026, Milano")
// Returns end-of-month + 10 days as expiry
function computeShareExpiry(courseTitle) {
  const italianMonths = {
    'gennaio': 0, 'febbraio': 1, 'marzo': 2, 'aprile': 3, 'maggio': 4, 'giugno': 5,
    'luglio': 6, 'agosto': 7, 'settembre': 8, 'ottobre': 9, 'novembre': 10, 'dicembre': 11
  };
  const match = (courseTitle || '').match(/(Gennaio|Febbraio|Marzo|Aprile|Maggio|Giugno|Luglio|Agosto|Settembre|Ottobre|Novembre|Dicembre)\s+(\d{4})/i);
  if (match) {
    const monthIdx = italianMonths[match[1].toLowerCase()];
    const year = parseInt(match[2]);
    // End of course month + 10 days
    const endOfMonth = new Date(year, monthIdx + 1, 0); // last day of month
    const expiry = new Date(endOfMonth.getTime() + 10 * 24 * 60 * 60 * 1000);
    return expiry;
  }
  // Fallback: 60 days from now
  return new Date(Date.now() + 60 * 24 * 60 * 60 * 1000);
}

// GET /api/share-link/:courseHandle — auto-generate or return existing single link
app.get('/api/share-link/:courseHandle', async (req, res) => {
  try {
    const { courseHandle } = req.params;
    const now = new Date();

    // Find existing active token for this course
    const existing = Object.values(shareTokens).find(
      t => t.courseHandle === courseHandle && new Date(t.expiresAt) > now
    );
    if (existing) {
      return res.json({
        success: true,
        token: existing.token,
        courseHandle,
        expiresAt: existing.expiresAt
      });
    }

    // No active token — auto-create one
    // Fetch course title to compute expiry
    let courseTitle = req.query.title || '';
    if (!courseTitle) {
      try {
        const products = await fetchAllShopifyProducts();
        const product = products.find(p => p.handle === courseHandle);
        if (product) courseTitle = product.title;
      } catch (e) { /* use fallback expiry */ }
    }

    // Clean up old expired tokens for this course
    Object.keys(shareTokens).forEach(k => {
      if (shareTokens[k].courseHandle === courseHandle) {
        delete shareTokens[k];
      }
    });

    const token = generateShareToken();
    const expiresAt = computeShareExpiry(courseTitle);

    shareTokens[token] = {
      token,
      courseHandle,
      createdAt: now.toISOString(),
      expiresAt: expiresAt.toISOString()
    };

    saveShareTokensToFile(shareTokens);
    res.json({
      success: true,
      token,
      courseHandle,
      expiresAt: expiresAt.toISOString()
    });
  } catch (error) {
    res.status(500).json({ success: false, error: error.message });
  }
});

// Legacy endpoints kept for backward compatibility
app.post('/api/share/:courseHandle', async (req, res) => {
  // Redirect to auto-generate
  res.redirect(307, `/api/share-link/${req.params.courseHandle}`);
});

app.delete('/api/share/:token', (req, res) => {
  try {
    const { token } = req.params;
    if (shareTokens[token]) {
      delete shareTokens[token];
      saveShareTokensToFile(shareTokens);
      res.json({ success: true, message: 'Token revoked' });
    } else {
      res.status(404).json({ success: false, error: 'Token not found' });
    }
  } catch (error) {
    res.status(500).json({ success: false, error: error.message });
  }
});

app.get('/api/share-tokens/:courseHandle', (req, res) => {
  try {
    const { courseHandle } = req.params;
    const now = new Date();
    const activeTokens = Object.values(shareTokens)
      .filter(t => t.courseHandle === courseHandle && new Date(t.expiresAt) > now)
      .map(t => ({
        token: t.token,
        createdAt: t.createdAt,
        expiresAt: t.expiresAt
      }));
    res.json({ success: true, tokens: activeTokens });
  } catch (error) {
    res.status(500).json({ success: false, error: error.message });
  }
});

app.get('/api/shared/:token', async (req, res) => {
  try {
    const { token } = req.params;
    const tokenData = shareTokens[token];

    if (!tokenData) {
      return res.status(404).json({ success: false, error: 'Token not found' });
    }

    const expiresAt = new Date(tokenData.expiresAt);
    if (new Date() > expiresAt) {
      return res.status(401).json({ success: false, error: 'Token expired' });
    }

    // Fetch course data by handle (parallel) — include registration lookup for consistency
    const [products, orders, registrationLookup] = await Promise.all([
      fetchAllShopifyProducts(),
      fetchAllShopifyOrders(),
      fetchRegistrationStudents()
    ]);

    const courseProduct = products.find(p => p.handle === tokenData.courseHandle);
    if (!courseProduct) {
      return res.status(404).json({ success: false, error: 'Course not found' });
    }

    // Parse tags
    const tagsArray = typeof courseProduct.tags === 'string'
      ? courseProduct.tags.split(',').map(t => t.trim())
      : (Array.isArray(courseProduct.tags) ? courseProduct.tags : []);

    // Build rich course data (same as main API but no financial margins)
    const course = {
      id: courseProduct.id,
      title: courseProduct.title,
      handle: courseProduct.handle,
      body_html: courseProduct.body_html,
      images: courseProduct.images,
      tags: tagsArray,
      product_type: courseProduct.product_type,
      variants: courseProduct.variants,
      students: [],
      enrollmentCount: 0
    };

    // Educator: from metafield first, then saved config, then tag, then vendor
    let shareEducator = '';
    let shareEducatorPhoto = '';
    let shareEducatorBio = '';
    try {
      const mfResp = await shopifyFetch(`/products/${courseProduct.id}/metafields.json`);
      const metafields = mfResp.metafields || [];
      const educatorMf = metafields.find(mf =>
        mf.key === 'sake_educator' || mf.key === 'educator' ||
        (mf.key && mf.key.toLowerCase().includes('educator') && !mf.key.toLowerCase().includes('photo') && !mf.key.toLowerCase().includes('bio') && !mf.key.toLowerCase().includes('image'))
      );
      if (educatorMf && educatorMf.value) shareEducator = educatorMf.value;
    } catch (e) { /* skip */ }
    if (!shareEducator) {
      const educatorTag = tagsArray.find(tag => tag.startsWith('educator:'));
      const savedEducator = courseCosts[courseProduct.handle]?.educatorName;
      shareEducator = educatorTag ? educatorTag.replace('educator:', '') : (savedEducator || '');
    }
    // Look up educator profile from Chi Siamo page (with fuzzy name matching)
    const shareEducatorProfiles = await fetchEducatorProfiles();
    const { cleanName: cleanShareEducator, profile: shareProfile } = matchEducatorProfile(shareEducator, shareEducatorProfiles);
    course.educator = cleanShareEducator || shareEducator;
    course.educatorPhoto = (shareProfile && shareProfile.photo) || '';
    course.educatorBio = (shareProfile && shareProfile.bio) || '';
    course.educatorRegion = (shareProfile && shareProfile.region) || '';

    // Program & costs (no financial details, only program structure)
    const costs = courseCosts[courseProduct.handle];
    if (costs && costs.program) {
      course.program = costs.program;

      // Enrich sake items with Sake Company product handles (for product page links)
      try {
        const scData = await fetchSakeCompanyProducts();
        const skuHandleMap = {};
        scData.products.forEach(p => {
          p.skus.forEach(sku => {
            if (!skuHandleMap[sku]) skuHandleMap[sku] = p.handle;
          });
        });
        course.program.forEach(group => {
          if (group.sakes) {
            group.sakes.forEach(sake => {
              if (sake.code && skuHandleMap[sake.code]) {
                sake.handle = skuHandleMap[sake.code];
              }
            });
          }
        });
      } catch (e) {
        console.error('Share: sake company enrichment failed:', e.message);
      }
    }

    // Get full student data (name, email, phone, order date, amount)
    orders.forEach(order => {
      if (!order.line_items) return;
      order.line_items.forEach(item => {
        if (item.product_id === courseProduct.id) {
          const customerName = order.customer
            ? `${order.customer.first_name || ''} ${order.customer.last_name || ''}`.trim()
            : 'Sconosciuto';

          let phone = order.customer?.phone || '';
          if (!phone && order.shipping_address?.phone) phone = order.shipping_address.phone;
          if (!phone && order.billing_address?.phone) phone = order.billing_address.phone;
          if (!phone && order.customer?.default_address?.phone) phone = order.customer.default_address.phone;

          // Calculate actual paid amount (subtract discounts)
          const shareItemDiscounts = (item.discount_allocations || []).reduce((sum, d) => sum + parseFloat(d.amount || 0), 0);
          const shareAmount = (parseFloat(item.price || 0) * item.quantity) - shareItemDiscounts;

          course.students.push({
            name: customerName,
            email: order.customer?.email || '',
            phone: phone,
            orderDate: order.created_at,
            orderNumber: order.name || `#${order.order_number}`,
            amount: shareAmount
          });
          course.enrollmentCount += item.quantity;
        }
      });
    });

    // Deduplicate students by orderNumber only (allow same email for multiple tickets)
    const seenOrders = new Set();
    course.students = course.students.filter(st => {
      if (st.orderNumber && seenOrders.has(st.orderNumber)) return false;
      if (st.orderNumber) seenOrders.add(st.orderNumber);
      return true;
    });
    course.enrollmentCount = course.students.length;

    // Apply phone overrides from saved config (per-order first, then email fallback)
    const phoneByOrder = courseCosts[courseProduct.handle]?.phoneOverridesByOrder;
    const phoneByEmail = courseCosts[courseProduct.handle]?.phoneOverrides;
    if (phoneByOrder || phoneByEmail) {
      course.students.forEach(st => {
        if (phoneByOrder && st.orderNumber && phoneByOrder[st.orderNumber]) {
          st.phone = phoneByOrder[st.orderNumber];
        } else {
          const emailKey = (st.email || '').toLowerCase().trim();
          if (phoneByEmail && emailKey && phoneByEmail[emailKey]) {
            st.phone = phoneByEmail[emailKey];
          }
        }
      });
    }

    // Apply name overrides from saved config (same as main dashboard)
    const nameOverrides = courseCosts[courseProduct.handle]?.nameOverrides;
    if (nameOverrides) {
      course.students.forEach(st => {
        if (st.orderNumber && nameOverrides[st.orderNumber]) {
          st.originalName = st.name;
          st.name = nameOverrides[st.orderNumber];
        }
      });
    }

    // Cross-reference with Airtable student registration (QR code data)
    if (registrationLookup && Object.keys(registrationLookup).length > 0) {
      course.students.forEach(st => {
        const emailKey = (st.email || '').toLowerCase().trim();
        if (emailKey && registrationLookup[emailKey]) {
          const reg = registrationLookup[emailKey];
          if (reg.name) st.registrationName = reg.name;
          if (reg.phone && !st.phone) st.phone = reg.phone;
        }
      });
    }

    res.json({ success: true, data: course });
  } catch (error) {
    console.error('Error in /api/shared/:token:', error.message);
    res.status(500).json({ success: false, error: error.message });
  }
});

// ============================================================================
// NOTEBOOK — educator-facing (no admin auth, scoped to share token)
// An educator accesses /share/:token and can write notes that are:
//   - stored under courseCosts[handle].notebook.educatorNotes
//   - visible ONLY to admin and to THAT educator (filter by shareToken)
//   - NEVER visible to other educators (filter below enforces this)
// ============================================================================

app.get('/api/shared/:token/notes', (req, res) => {
  try {
    const { token } = req.params;
    const share = shareTokens[token];
    if (!share) return res.status(404).json({ success: false, error: 'invalid-token' });
    const notebook = (courseCosts[share.courseHandle] || {}).notebook || {};
    const myNotes = (notebook.educatorNotes || [])
      .filter(n => n.shareToken === token)
      .map(n => ({ id: n.id, text: n.text, createdAt: n.createdAt }));
    res.json({ success: true, courseHandle: share.courseHandle, notes: myNotes });
  } catch (e) {
    res.status(500).json({ success: false, error: e.message });
  }
});

app.post('/api/shared/:token/notes', (req, res) => {
  try {
    const { token } = req.params;
    const share = shareTokens[token];
    if (!share) return res.status(404).json({ success: false, error: 'invalid-token' });
    const { text } = req.body || {};
    if (!text || typeof text !== 'string' || !text.trim()) {
      return res.status(400).json({ success: false, error: 'text required' });
    }
    const courseId = share.courseHandle;
    const existing = courseCosts[courseId] || {};
    const notebook = existing.notebook || {};
    const educatorNotes = Array.isArray(notebook.educatorNotes) ? notebook.educatorNotes.slice() : [];
    const note = {
      id: crypto.randomUUID(),
      shareToken: token,
      educatorName: share.educatorName || '',
      text: text.trim(),
      createdAt: new Date().toISOString(),
      updatedAt: null,
    };
    educatorNotes.push(note);
    courseCosts[courseId] = { ...existing, notebook: { ...notebook, educatorNotes } };
    saveCostsToFile(courseCosts);
    airtableConfigSet('course_costs', courseCosts).catch(() => {});
    res.json({ success: true, note: { id: note.id, text: note.text, createdAt: note.createdAt } });
  } catch (e) {
    res.status(500).json({ success: false, error: e.message });
  }
});

app.delete('/api/shared/:token/notes/:noteId', (req, res) => {
  try {
    const { token, noteId } = req.params;
    const share = shareTokens[token];
    if (!share) return res.status(404).json({ success: false, error: 'invalid-token' });
    const courseId = share.courseHandle;
    const existing = courseCosts[courseId] || {};
    const notebook = existing.notebook || {};
    // Educator can only delete their own notes (matching shareToken AND id)
    const kept = (notebook.educatorNotes || []).filter(n => !(n.id === noteId && n.shareToken === token));
    courseCosts[courseId] = { ...existing, notebook: { ...notebook, educatorNotes: kept } };
    saveCostsToFile(courseCosts);
    airtableConfigSet('course_costs', courseCosts).catch(() => {});
    res.json({ success: true });
  } catch (e) {
    res.status(500).json({ success: false, error: e.message });
  }
});

// ============================================================================
// SHARE TOKEN READ-ONLY PAGE (must be BEFORE wildcard route)
// ============================================================================
app.get('/share/:token', (req, res) => {
  const token = req.params.token;
  const sakeCompanyDomain = SAKE_COMPANY_STORE.replace('.myshopify.com', '');
  res.send(`
<!DOCTYPE html>
<html lang="it">
<head>
  <meta charset="UTF-8">
  <meta name="viewport" content="width=device-width, initial-scale=1.0">
  <title>SSA - Dettaglio Corso</title>
  <link href="https://cdnjs.cloudflare.com/ajax/libs/font-awesome/6.4.0/css/all.min.css" rel="stylesheet">
  <style>
    * { margin: 0; padding: 0; box-sizing: border-box; }
    :root {
      --primary: #0D5CAB;
      --bg: #f7f8fa;
      --card: #ffffff;
      --text: #1a1a1a;
      --text-light: #6b7280;
      --border: #e5e7eb;
      --success: #16a34a;
    }
    body { font-family: 'Segoe UI', system-ui, -apple-system, sans-serif; background: var(--bg); color: var(--text); min-height: 100vh; }
    .header { background: #fff; border-bottom: 1px solid var(--border); padding: 12px 24px; display: flex; align-items: center; gap: 12px; }
    .header img { width: 36px; height: 36px; border-radius: 50%; }
    .header h1 { font-size: 16px; font-weight: 600; }
    .header .sub { font-size: 11px; color: var(--text-light); }
    .main { max-width: 1100px; margin: 0 auto; padding: 24px; }
    .course-title-bar { font-size: 18px; font-weight: 600; margin-bottom: 16px; }
    .badges { display: flex; flex-wrap: wrap; gap: 8px; margin-bottom: 16px; }
    .badge { display: inline-flex; padding: 4px 12px; border-radius: 20px; font-size: 11px; font-weight: 600; text-transform: uppercase; letter-spacing: 0.5px; }
    .badge.blue { background: #dbeafe; color: #1e40af; }
    .badge.green { background: #dcfce7; color: #166534; }
    .badge.cyan { background: #cffafe; color: #0e7490; }

    /* Collapsible sections */
    .section-card { background: var(--card); border: 1px solid var(--border); border-radius: 12px; margin-bottom: 16px; overflow: hidden; }
    .section-header { padding: 16px 20px; cursor: pointer; display: flex; align-items: center; justify-content: space-between; user-select: none; }
    .section-header:hover { background: #f9fafb; }
    .section-header .section-label { font-size: 14px; font-weight: 600; display: flex; align-items: center; gap: 8px; color: var(--text); text-transform: uppercase; letter-spacing: 0.5px; }
    .section-header .section-label i { color: var(--primary); }
    .section-header .toggle-chevron { font-size: 12px; color: var(--text-light); transition: transform 0.25s ease; }
    .section-header .toggle-chevron.collapsed { transform: rotate(-90deg); }
    .section-body { transition: max-height 0.35s ease, padding 0.35s ease; overflow: hidden; }
    .section-body.collapsed { max-height: 0 !important; padding-top: 0 !important; padding-bottom: 0 !important; }
    .section-body-inner { padding: 0 20px 20px; }

    /* Info grid */
    .info-grid { display: grid; grid-template-columns: 1fr 1fr; gap: 12px; }
    .info-grid .item { font-size: 13px; }
    .info-grid .item strong { color: var(--text); }

    /* Stats */
    .stat-row { display: grid; grid-template-columns: repeat(2, 1fr); gap: 12px; margin-bottom: 16px; }
    .stat-box { background: var(--card); border: 1px solid var(--border); border-radius: 12px; padding: 16px 20px; position: relative; overflow: hidden; }
    .stat-box::before { content: ''; position: absolute; top: 0; left: 0; right: 0; height: 3px; background: var(--primary); }
    .stat-box.green::before { background: var(--success); }
    .stat-box .label { font-size: 11px; color: var(--text-light); text-transform: uppercase; letter-spacing: 0.5px; margin-bottom: 4px; }
    .stat-box .value { font-size: 22px; font-weight: 700; }
    .stat-box .sub { font-size: 11px; color: var(--text-light); margin-top: 2px; }

    /* Program: day sections */
    .day-section { background: #f9fafb; border: 1px solid var(--border); border-radius: 10px; margin-bottom: 12px; overflow: hidden; }
    .day-section:last-child { margin-bottom: 0; }
    .day-header { background: linear-gradient(135deg, #eef2ff, #f0f4ff); padding: 12px 16px; font-size: 13px; font-weight: 700; color: var(--primary); text-transform: uppercase; letter-spacing: 0.5px; display: flex; align-items: center; gap: 8px; }
    .day-header i { font-size: 12px; }
    .day-body { padding: 12px 16px; }

    /* Group card */
    .group-card { margin-bottom: 14px; }
    .group-card:last-child { margin-bottom: 0; }
    .group-name { font-size: 13px; font-weight: 600; color: var(--text); margin-bottom: 10px; padding-bottom: 6px; border-bottom: 1px solid var(--border); }

    /* Sake card — large photo + side info */
    .sake-card { display: flex; gap: 16px; background: #fff; border: 1px solid var(--border); border-radius: 10px; padding: 12px; margin-bottom: 10px; align-items: flex-start; transition: box-shadow 0.2s; }
    .sake-card:hover { box-shadow: 0 2px 8px rgba(0,0,0,0.06); }
    .sake-card:last-child { margin-bottom: 0; }
    .sake-photo { flex-shrink: 0; width: 100px; height: 140px; border-radius: 8px; overflow: hidden; background: #f3f4f6; display: flex; align-items: center; justify-content: center; }
    .sake-photo img { width: 100%; height: 100%; object-fit: contain; }
    .sake-photo .sake-placeholder { font-size: 40px; opacity: 0.3; }
    .sake-info { flex: 1; min-width: 0; }
    .sake-info .sake-title { font-size: 14px; font-weight: 600; color: var(--text); margin-bottom: 2px; }
    .sake-info .sake-jp { font-size: 12px; color: var(--text-light); margin-bottom: 6px; }
    .sake-info .sake-meta { font-size: 11px; color: var(--text-light); line-height: 1.6; }
    .sake-info .sake-meta span { display: inline-block; margin-right: 8px; }
    .sake-info .sake-qty-badge { display: inline-flex; align-items: center; gap: 4px; background: #dbeafe; color: #1e40af; font-size: 11px; font-weight: 600; padding: 2px 8px; border-radius: 10px; margin-top: 6px; }
    .sake-info .sake-link { display: inline-flex; align-items: center; gap: 4px; font-size: 11px; color: var(--primary); text-decoration: none; margin-top: 6px; margin-left: 8px; }
    .sake-info .sake-link:hover { text-decoration: underline; }

    /* Student table */
    .student-table { width: 100%; border-collapse: collapse; }
    .student-table th { text-align: left; padding: 10px 16px; font-size: 11px; text-transform: uppercase; letter-spacing: 0.5px; color: var(--text-light); background: #f9fafb; border-bottom: 1px solid var(--border); font-weight: 600; }
    .student-table td { padding: 12px 16px; font-size: 13px; border-bottom: 1px solid var(--border); vertical-align: middle; }
    .student-table tr:last-child td { border-bottom: none; }
    .student-table .avatar { width: 30px; height: 30px; border-radius: 50%; background: var(--primary); color: white; display: inline-flex; align-items: center; justify-content: center; font-size: 11px; font-weight: 600; margin-right: 8px; vertical-align: middle; }
    .student-table .email { color: var(--text-light); font-size: 12px; }
    .student-table .wa-link { color: #25D366; text-decoration: none; font-size: 16px; }

    .status-error { background: #fee2e2; border: 1px solid #fca5a5; color: #dc2626; padding: 16px; border-radius: 8px; text-align: center; }
    .status-expired { background: #fef3c7; border: 1px solid #fcd34d; color: #92400e; padding: 20px; border-radius: 8px; text-align: center; font-size: 15px; }
    .loading { text-align: center; padding: 3rem; color: var(--text-light); }
    .spinner { border: 3px solid var(--border); border-top: 3px solid var(--primary); border-radius: 50%; width: 36px; height: 36px; animation: spin 0.8s linear infinite; margin: 0 auto 12px; }
    @keyframes spin { 0% { transform: rotate(0deg); } 100% { transform: rotate(360deg); } }
    .footer { text-align: center; padding: 24px; font-size: 11px; color: var(--text-light); }
    @media (max-width: 700px) {
      .main { padding: 12px; }
      .info-grid { grid-template-columns: 1fr; }
      .stat-row { grid-template-columns: 1fr; }
      .stat-box .value { font-size: 18px; }
      .sake-card { flex-direction: column; align-items: center; text-align: center; }
      .sake-photo { width: 140px; height: 180px; }
      .student-table-wrap { overflow-x: auto; -webkit-overflow-scrolling: touch; }
      .student-table { min-width: 500px; }
    }
  </style>
</head>
<body>
  <div class="header">
    <img src="/ssa-logo.png" alt="SSA">
    <div><h1>SSA Gestione Corsi</h1><div class="sub">Sake Sommelier Association</div></div>
  </div>
  <div class="main">
    <div id="content">
      <div class="loading"><div class="spinner"></div><p>Caricamento corso...</p></div>
    </div>

    <!-- Private notebook for this educator (visible only to admin + this educator) -->
    <section id="educatorNotebook" style="max-width: 100%; margin: 24px 0; padding: 20px; background: white; border: 1px solid var(--border); border-radius: 12px;">
      <h3 style="font-size: 14px; font-weight: 600; margin-bottom: 10px; text-transform: uppercase; letter-spacing: 0.5px; color: var(--text); display:flex; align-items:center; gap:8px;">
        <i class="fas fa-sticky-note" style="color: var(--primary);"></i> Le tue note private
      </h3>
      <p style="font-size: 12px; color: var(--text-light); margin-bottom: 12px; line-height: 1.4;">
        Queste note sono visibili solo a te e all'organizzazione SSA. Gli altri educator non possono vederle.
      </p>
      <textarea id="edNoteText" placeholder="Scrivi una nota..." rows="3" style="width: 100%; padding: 10px; border: 1px solid var(--border); border-radius: 8px; font-size: 14px; font-family: inherit; resize: vertical; box-sizing: border-box;"></textarea>
      <div style="display:flex;gap:8px;margin-top:8px;">
        <button id="edNoteSubmit" style="padding: 8px 18px; background: var(--primary); color: white; border: none; border-radius: 8px; font-size: 14px; font-weight: 600; cursor: pointer;"><i class="fas fa-plus"></i> Aggiungi</button>
      </div>
      <div id="edNotesList" style="margin-top: 16px; display: flex; flex-direction: column; gap: 8px;"></div>
    </section>
  </div>
  <div class="footer">Sake Sommelier Association &copy; 2026 &mdash; Link condiviso</div>

  <script>
    const SAKE_COMPANY_DOMAIN = '${sakeCompanyDomain}';

    function toggleSection(id) {
      const body = document.getElementById(id);
      const chevron = document.querySelector('[data-toggle="' + id + '"]');
      if (!body) return;
      if (body.classList.contains('collapsed')) {
        body.style.maxHeight = body.scrollHeight + 'px';
        body.classList.remove('collapsed');
        if (chevron) chevron.classList.remove('collapsed');
        setTimeout(() => { body.style.maxHeight = 'none'; }, 350);
      } else {
        body.style.maxHeight = body.scrollHeight + 'px';
        requestAnimationFrame(() => {
          body.style.maxHeight = '0px';
          body.classList.add('collapsed');
          if (chevron) chevron.classList.add('collapsed');
        });
      }
    }

    function parseTitle(title) {
      let city = '', dateStr = '';
      const cityMatch = title.match(/,\\s*([A-Za-z\\u00C0-\\u00ff\\s]+)$/);
      if (cityMatch) city = cityMatch[1].trim();
      const dateMatch = title.match(/(Gennaio|Febbraio|Marzo|Aprile|Maggio|Giugno|Luglio|Agosto|Settembre|Ottobre|Novembre|Dicembre)\\s+(\\d{4})/i);
      if (dateMatch) dateStr = dateMatch[1] + ' ' + dateMatch[2];
      return { city, dateStr };
    }

    function formatDate(d) {
      if (!d) return '-';
      const dt = new Date(d);
      return dt.toLocaleDateString('it-IT', { day: '2-digit', month: '2-digit', year: 'numeric' });
    }

    function sakeCompanyUrl(handle) {
      if (!handle) return '';
      return 'https://' + SAKE_COMPANY_DOMAIN + '.myshopify.com/products/' + handle;
    }

    async function loadCourseData() {
      try {
        const response = await fetch('/api/shared/${token}');
        const result = await response.json();
        if (!response.ok) {
          if (response.status === 401) {
            document.getElementById('content').innerHTML = '<div class="status-expired"><i class="fas fa-clock"></i> Questo link \\u00e8 scaduto. Richiedi un nuovo link di accesso.</div>';
          } else {
            document.getElementById('content').innerHTML = '<div class="status-error">Errore: ' + (result.error || 'Corso non trovato') + '</div>';
          }
          return;
        }
        const c = result.data;
        const parsed = parseTitle(c.title);
        const price = c.variants && c.variants[0] ? parseFloat(c.variants[0].price).toLocaleString('it-IT', {minimumFractionDigits: 2}) + ' \\u20ac' : '';
        const maxStudents = c.variants && c.variants[0] && c.variants[0].inventory_quantity != null ? (c.enrollmentCount + (c.variants[0].inventory_quantity || 0)) : 20;
        const tags = c.tags || [];
        const isOnline = tags.some(t => t.toLowerCase().includes('online'));
        const isCertificato = tags.some(t => t.toLowerCase().includes('certificato')) || c.title.toLowerCase().includes('certificato');

        let html = '';

        // Title + badges
        html += '<div class="course-title-bar">' + c.title + '</div>';
        html += '<div class="badges">';
        if (isCertificato) html += '<span class="badge blue">IN PRESENZA CERTIFICATO</span>';
        if (isOnline) html += '<span class="badge cyan">ONLINE</span>';
        if (parsed.city) html += '<span class="badge green"><i class="fas fa-map-marker-alt"></i>&nbsp;' + parsed.city + '</span>';
        html += '</div>';

        // Stats
        html += '<div class="stat-row">';
        html += '<div class="stat-box"><div class="label">Iscritti</div><div class="value">' + c.enrollmentCount + '/' + maxStudents + '</div><div class="sub">Min. 6</div></div>';
        html += '<div class="stat-box green"><div class="label">Posti disponibili</div><div class="value">' + Math.max(0, maxStudents - c.enrollmentCount) + '</div></div>';
        html += '</div>';

        // ── SECTION: Info ──
        html += '<div class="section-card">';
        html += '<div class="section-header" onclick="toggleSection(\\'sec-info\\')">';
        html += '<span class="section-label"><i class="fas fa-info-circle"></i> Informazioni</span>';
        html += '<i class="fas fa-chevron-down toggle-chevron" data-toggle="sec-info"></i>';
        html += '</div>';
        html += '<div class="section-body" id="sec-info"><div class="section-body-inner">';
        html += '<div class="info-grid">';
        if (parsed.dateStr) html += '<div class="item"><strong>Periodo:</strong> ' + parsed.dateStr + '</div>';
        if (c.educator) html += '<div class="item"><strong>Educator:</strong> ' + c.educator + '</div>';
        if (parsed.city && !isOnline) html += '<div class="item"><strong>Citt\\u00e0:</strong> ' + parsed.city + '</div>';
        if (price) html += '<div class="item"><strong>Prezzo:</strong> ' + price + '/persona</div>';
        html += '</div>';
        html += '</div></div></div>';

        // ── SECTION: Programma ──
        if (c.program && Array.isArray(c.program) && c.program.length > 0) {
          html += '<div class="section-card">';
          html += '<div class="section-header" onclick="toggleSection(\\'sec-program\\')">';
          html += '<span class="section-label"><i class="fas fa-book"></i> Programma</span>';
          html += '<i class="fas fa-chevron-down toggle-chevron" data-toggle="sec-program"></i>';
          html += '</div>';
          html += '<div class="section-body" id="sec-program"><div class="section-body-inner">';

          // Group by day
          const days = {};
          c.program.forEach(group => {
            const d = group.day || 1;
            if (!days[d]) days[d] = [];
            days[d].push(group);
          });

          Object.keys(days).sort((a,b) => a - b).forEach(dayNum => {
            const groups = days[dayNum];
            html += '<div class="day-section">';
            html += '<div class="day-header"><i class="fas fa-calendar-day"></i> Giorno ' + dayNum + '</div>';
            html += '<div class="day-body">';

            groups.forEach(group => {
              html += '<div class="group-card">';
              if (group.name) html += '<div class="group-name">' + group.name + '</div>';
              if (Array.isArray(group.sakes) && group.sakes.length > 0) {
                group.sakes.forEach(s => {
                  const scUrl = sakeCompanyUrl(s.handle);
                  html += '<div class="sake-card">';
                  html += '<div class="sake-photo">';
                  if (s.image) {
                    html += '<img src="' + s.image + '" alt="' + (s.name || '') + '">';
                  } else {
                    html += '<div class="sake-placeholder">\\ud83c\\udf76</div>';
                  }
                  html += '</div>';
                  html += '<div class="sake-info">';
                  html += '<div class="sake-title">' + (s.name || '') + '</div>';
                  if (s.nameJp) html += '<div class="sake-jp">' + s.nameJp + '</div>';
                  html += '<div class="sake-meta">';
                  if (s.code) html += '<span><strong>SKU:</strong> ' + s.code + '</span>';
                  if (s.type) html += '<span><strong>Tipo:</strong> ' + s.type + '</span>';
                  if (s.sakagura) html += '<span><strong>Sakagura:</strong> ' + s.sakagura + '</span>';
                  if (s.size) html += '<span><strong>Formato:</strong> ' + s.size + 'ml</span>';
                  html += '</div>';
                  html += '<div style="margin-top:6px;">';
                  if (s.qty) html += '<span class="sake-qty-badge"><i class="fas fa-boxes"></i> Qt\\u00e0: ' + s.qty + '</span>';
                  if (scUrl) html += '<a href="' + scUrl + '" target="_blank" class="sake-link"><i class="fas fa-external-link-alt"></i> Scheda Tecnica</a>';
                  html += '</div>';
                  html += '</div></div>';
                });
              }
              html += '</div>';
            });

            html += '</div></div>';
          });

          html += '</div></div></div>';
        }

        // ── SECTION: Iscritti ──
        if (c.students && c.students.length > 0) {
          html += '<div class="section-card">';
          html += '<div class="section-header" onclick="toggleSection(\\'sec-students\\')">';
          html += '<span class="section-label"><i class="fas fa-users"></i> Iscritti (' + c.students.length + ')</span>';
          html += '<i class="fas fa-chevron-down toggle-chevron" data-toggle="sec-students"></i>';
          html += '</div>';
          html += '<div class="section-body" id="sec-students"><div class="section-body-inner">';
          html += '<div class="student-table-wrap"><table class="student-table"><thead><tr><th>Corsista</th><th>Telefono</th><th>Data Iscrizione</th></tr></thead><tbody>';
          c.students.forEach(st => {
            const initials = st.name ? st.name.split(' ').map(w => w[0]).join('').substring(0,2).toUpperCase() : '?';
            const cleanPhone = st.phone ? st.phone.replace(/[^\\d+]/g, '') : '';
            const waPhone = cleanPhone.replace(/^\\+/, '');
            html += '<tr>';
            html += '<td><span class="avatar">' + initials + '</span><span>' + (st.name || '-') + '</span><br><span class="email">' + (st.email || '') + '</span></td>';
            html += '<td>' + (cleanPhone ? '<a href="https://wa.me/' + waPhone + '" target="_blank" class="wa-link"><i class="fab fa-whatsapp"></i></a> ' + st.phone : '-') + '</td>';
            html += '<td>' + formatDate(st.orderDate) + '</td>';
            html += '</tr>';
          });
          html += '</tbody></table></div>';
          html += '</div></div></div>';
        }

        document.getElementById('content').innerHTML = html;
      } catch (error) {
        document.getElementById('content').innerHTML = '<div class="status-error">Errore nel caricamento: ' + error.message + '</div>';
      }
    }
    loadCourseData();

    // ---- Educator private notebook ----
    (function initEducatorNotebook() {
      const token = ${JSON.stringify(req.params.token)};
      const textarea = document.getElementById('edNoteText');
      const button = document.getElementById('edNoteSubmit');
      const list = document.getElementById('edNotesList');
      if (!textarea || !button || !list) return;

      function escapeHTML(s) {
        return String(s == null ? '' : s)
          .replace(/&/g, '&amp;')
          .replace(/</g, '&lt;')
          .replace(/>/g, '&gt;')
          .replace(/"/g, '&quot;')
          .replace(/'/g, '&#39;');
      }
      function fmt(iso) {
        try { return new Date(iso).toLocaleString('it-IT'); } catch (e) { return iso; }
      }

      async function loadNotes() {
        try {
          const r = await fetch('/api/shared/' + token + '/notes');
          const d = await r.json();
          if (!d.success) { list.innerHTML = '<div style="color:#b91c1c;font-size:12px;">Errore: ' + escapeHTML(d.error || '') + '</div>'; return; }
          if (!d.notes.length) {
            list.innerHTML = '<div style="color:#9ca3af;font-style:italic;font-size:13px;">Nessuna tua nota ancora.</div>';
            return;
          }
          list.innerHTML = d.notes.slice().reverse().map(function(n) {
            return '<div style="padding:10px;background:#fafafa;border-radius:8px;display:flex;gap:10px;align-items:flex-start;">' +
              '<div style="flex:1;">' +
                '<div style="font-size:10px;color:#6b7280;margin-bottom:4px;">' + escapeHTML(fmt(n.createdAt)) + '</div>' +
                '<div style="font-size:13px;white-space:pre-wrap;">' + escapeHTML(n.text) + '</div>' +
              '</div>' +
              '<button onclick="edDeleteNote(\\'' + n.id + '\\')" style="background:none;border:none;color:#9ca3af;cursor:pointer;font-size:12px;" title="Elimina"><i class="fas fa-trash"></i></button>' +
            '</div>';
          }).join('');
        } catch (e) {
          list.innerHTML = '<div style="color:#b91c1c;font-size:12px;">Errore: ' + escapeHTML(e.message) + '</div>';
        }
      }

      window.edDeleteNote = async function(id) {
        if (!confirm('Eliminare questa nota?')) return;
        try {
          const r = await fetch('/api/shared/' + token + '/notes/' + encodeURIComponent(id), { method: 'DELETE' });
          const d = await r.json();
          if (!d.success) throw new Error(d.error || 'delete failed');
          await loadNotes();
        } catch (e) {
          alert('Errore cancellando nota: ' + e.message);
        }
      };

      button.addEventListener('click', async function() {
        const text = (textarea.value || '').trim();
        if (!text) return;
        button.disabled = true;
        try {
          const r = await fetch('/api/shared/' + token + '/notes', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ text: text })
          });
          const d = await r.json();
          if (!d.success) throw new Error(d.error || 'save failed');
          textarea.value = '';
          await loadNotes();
        } catch (e) {
          alert('Errore: ' + e.message);
        } finally {
          button.disabled = false;
        }
      });

      loadNotes();
    })();
  </script>
</body>
</html>
  `);
});

// ============================================================================
// STATIC FILES & FALLBACK (must be LAST)
// ============================================================================
app.get('*', (req, res) => {
  res.sendFile(path.join(__dirname, 'public', 'index.html'), (err) => {
    if (err) { res.status(404).json({ success: false, error: 'Not found' }); }
  });
});

// ============================================================================
// ERROR HANDLING
// ============================================================================
app.use((err, req, res, next) => {
  console.error('Unhandled error:', err);
  res.status(500).json({
    success: false, error: 'Internal server error',
    message: process.env.NODE_ENV === 'development' ? err.message : undefined
  });
});

// ============================================================================
// START SERVER (with Airtable persistence init)
// ============================================================================
async function startServer() {
  // Initialize Airtable persistence before accepting requests
  await initConfigTable();
  await loadFromAirtable();

  app.listen(PORT, () => {
    console.log(`SSA Backend Server running on http://localhost:${PORT}`);
    console.log(`Environment: ${process.env.NODE_ENV || 'development'}`);
    console.log(`Shopify Store: ${SHOPIFY_STORE}`);
    console.log(`Airtable Base: ${AIRTABLE_BASE_ID}`);
    console.log(`Airtable Persistence: ${airtablePersistenceActive ? 'ACTIVE' : 'DISABLED (file-only)'}`);

    // Warm-up: pre-fetch and cache all Shopify + Airtable data in background
    // so the first user request is fast (doesn't need to wait for cold API calls)
    console.log('Starting background warm-up of API data...');
    (async () => {
      try {
        const warmStart = Date.now();
        // Trigger a full /api/courses build so the first user request is instant
        const fakeReq = { query: {} };
        const fakeRes = {
          set: () => {},
          json: (data) => {
            console.log(`Warm-up: /api/courses response ready (${data.count} courses) in ${Date.now() - warmStart}ms`);
          },
          status: () => ({ json: () => {} })
        };
        // Use internal route handler simulation - just fetch the data to populate all caches
        const [products, orders, educatorProfiles, registrationLookup] = await Promise.all([
          fetchAllShopifyProducts(),
          fetchAllShopifyOrders(),
          fetchEducatorProfiles(),
          fetchRegistrationStudents()
        ]);
        const courseProducts = products.filter(isCourseProduct);
        await fetchCourseMetafields(courseProducts);
        console.log(`Warm-up phase 1 complete in ${Date.now() - warmStart}ms: ${products.length} products, ${orders.length} orders, ${courseProducts.length} course metafields cached`);

        // Now trigger an actual /api/courses request to populate the full response cache
        try {
          const http = require('http');
          http.get(`http://localhost:${PORT}/api/courses`, (res) => {
            let body = '';
            res.on('data', chunk => body += chunk);
            res.on('end', () => {
              console.log(`Warm-up phase 2: /api/courses response cached in ${Date.now() - warmStart}ms`);
            });
          }).on('error', () => {});
        } catch (e) {}
      } catch (err) {
        console.log('Warm-up failed (non-critical):', err.message);
      }

      // Auto-refresh cache every 8 minutes to keep data fresh
      setInterval(async () => {
        try {
          console.log('Background cache refresh starting...');
          const refreshStart = Date.now();
          const http = require('http');
          http.get(`http://localhost:${PORT}/api/courses?nocache=1`, (res) => {
            let body = '';
            res.on('data', chunk => body += chunk);
            res.on('end', () => {
              console.log(`Background cache refresh done in ${Date.now() - refreshStart}ms`);
            });
          }).on('error', () => {});
        } catch (e) {
          console.log('Background refresh error:', e.message);
        }
      }, 8 * 60 * 1000); // Every 8 minutes
    })();
  });
}

startServer().catch(err => {
  console.error('Failed to start server:', err);
  // Fallback: start without Airtable persistence
  app.listen(PORT, () => {
    console.log(`SSA Backend Server running on http://localhost:${PORT} (without Airtable persistence)`);
  });
});
