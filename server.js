require('dotenv').config();
const express = require('express');
const cors = require('cors');
const path = require('path');
const fs = require('fs');
const crypto = require('crypto');

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
  const response = await fetch(url, {
    method: options.method || 'GET',
    headers: {
      'X-Shopify-Access-Token': SHOPIFY_ACCESS_TOKEN,
      'Content-Type': 'application/json',
      ...options.headers
    },
    body: options.body ? JSON.stringify(options.body) : undefined
  });
  if (!response.ok) {
    const error = await response.text();
    throw new Error(`Shopify API Error (${response.status}): ${error}`);
  }
  return await response.json();
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

  setCache(cacheKey, allProducts, 300);
  return allProducts;
}

// Fetch ALL Shopify orders with pagination
async function fetchAllShopifyOrders() {
  const cacheKey = 'shopify_all_orders';
  let cached = getCache(cacheKey);
  if (cached) return cached;

  let allOrders = [];
  let url = `/orders.json?limit=250&status=any`;

  while (url) {
    const response = await shopifyFetch(url);
    const orders = response.orders || [];
    allOrders = allOrders.concat(orders);
    if (orders.length < 250) break;
    const lastId = orders[orders.length - 1].id;
    url = `/orders.json?limit=250&status=any&since_id=${lastId}`;
  }

  setCache(cacheKey, allOrders, 300);
  return allOrders;
}

// ============================================================================
// AIRTABLE API UTILITIES
// ============================================================================
const AIRTABLE_API_KEY = process.env.AIRTABLE_API_KEY;
const AIRTABLE_BASE_ID = process.env.AIRTABLE_BASE_ID || 'appwCWGRd0jXOCxMA';
const AIRTABLE_TABLE_ID = process.env.AIRTABLE_TABLE_ID || 'tblnJO5Mf7EVmteRk';

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
      handle === 'corsi-ed-eventi-2024-25' || handle.startsWith('copy-of-')) {
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
// COURSES API - Returns ONLY actual courses with FULL Shopify data + orders
// ============================================================================
app.get('/api/courses', async (req, res) => {
  try {
    const products = await fetchAllShopifyProducts();
    const orders = await fetchAllShopifyOrders();

    // Filter to only course products
    const courseProducts = products.filter(isCourseProduct);

    // Fetch metafields for each course product (in parallel, batches of 5)
    // This gets the "Sake Educator" field from Shopify metafields
    const metafieldMap = {};
    const MFBATCH = 5;
    for (let i = 0; i < courseProducts.length; i += MFBATCH) {
      const batch = courseProducts.slice(i, i + MFBATCH);
      await Promise.all(batch.map(async (product) => {
        try {
          const mfResp = await shopifyFetch(`/products/${product.id}/metafields.json`);
          const metafields = mfResp.metafields || [];
          // Look for educator metafield (key contains 'educator' or 'sake_educator')
          const educatorMf = metafields.find(mf =>
            mf.key === 'sake_educator' || mf.key === 'educator' ||
            (mf.key && mf.key.toLowerCase().includes('educator'))
          );
          if (educatorMf && educatorMf.value) {
            metafieldMap[product.id] = educatorMf.value;
          }
        } catch (e) {
          // Silently skip metafield errors
        }
      }));
    }

    // Build enrollment data from orders
    const courseMap = new Map();
    courseProducts.forEach(product => {
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
        // Educator from Shopify metafield
        educatorName: metafieldMap[product.id] || '',
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

    // Apply phone overrides from saved config
    courses.forEach(course => {
      const overrides = courseCosts[course.handle]?.phoneOverrides;
      if (overrides && course.students) {
        course.students.forEach(st => {
          const emailKey = (st.email || '').toLowerCase().trim();
          if (emailKey && overrides[emailKey]) {
            st.phone = overrides[emailKey];
          }
        });
      }
    });

    res.json({ success: true, count: courses.length, data: courses });
  } catch (error) {
    console.error('Error in /api/courses:', error.message);
    res.status(500).json({ success: false, error: error.message });
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
  const { location, educator, food, sake, adv, program, lines, educatorName } = req.body;
  courseCosts[courseId] = {
    location: parseFloat(location) || 0,
    educator: parseFloat(educator) || 0,
    food: parseFloat(food) || 0,
    sake: parseFloat(sake) || 0,
    adv: parseFloat(adv) || 0
  };
  // Save program (groups with sakes) if provided
  if (program !== undefined) {
    courseCosts[courseId].program = program;
  }
  // Save lines array if provided
  if (lines !== undefined) {
    courseCosts[courseId].lines = lines;
  }
  // Save educator assignment if provided
  if (educatorName !== undefined) {
    courseCosts[courseId].educatorName = educatorName;
    // Also sync educator tag to Shopify (fire-and-forget)
    syncEducatorTagToShopify(courseId, educatorName).catch(e =>
      console.error(`Shopify educator tag sync failed for ${courseId}:`, e.message)
    );
  }
  saveCostsToFile(courseCosts);
  res.json({ success: true, costs: courseCosts[courseId] });
});

// Phone overrides: save corrected phone numbers (keyed by email)
app.post('/api/phone-overrides/:courseId', (req, res) => {
  const { courseId } = req.params;
  const { email, phone } = req.body;
  if (!email) return res.json({ success: false, error: 'email required' });
  if (!courseCosts[courseId]) courseCosts[courseId] = {};
  if (!courseCosts[courseId].phoneOverrides) courseCosts[courseId].phoneOverrides = {};
  courseCosts[courseId].phoneOverrides[email.toLowerCase().trim()] = phone;
  saveCostsToFile(courseCosts);
  res.json({ success: true });
});

// Sync educator:Name tag to Shopify product
async function syncEducatorTagToShopify(courseHandle, educatorName) {
  try {
    // Find product by handle
    const products = await fetchAllShopifyProducts();
    const product = products.find(p => p.handle === courseHandle);
    if (!product) {
      console.log(`Shopify sync: product not found for handle ${courseHandle}`);
      return;
    }
    // Parse existing tags, remove old educator: tag, add new one
    let tags = (product.tags || '').split(',').map(t => t.trim()).filter(t => t && !t.startsWith('educator:'));
    if (educatorName) {
      tags.push(`educator:${educatorName}`);
    }
    const newTags = tags.join(', ');
    // Update product via Shopify Admin API
    await shopifyFetch(`/products/${product.id}.json`, {
      method: 'PUT',
      body: { product: { id: product.id, tags: newTags } }
    });
    // Invalidate product cache so next fetch picks up the new tag
    setCache('shopify_all_products', null, 0);
    console.log(`Shopify educator tag synced: ${courseHandle} → educator:${educatorName}`);
  } catch (e) {
    console.error(`Shopify educator sync error:`, e.message);
  }
}

// ============================================================================
// WHATSAPP NUMBER CHECK (automatic, server-side)
// ============================================================================
const waCheckCache = {};        // { cleanPhone: true|false|null }
const WA_CACHE_TTL = 7 * 24 * 60 * 60 * 1000; // 7 days
const waCheckTimestamps = {};   // { cleanPhone: timestamp }

// Core: check a single phone number against wa.me
async function checkSingleWhatsApp(cleanPhone) {
  // Return cached result if still valid
  if (waCheckCache[cleanPhone] !== undefined && waCheckTimestamps[cleanPhone] &&
      Date.now() - waCheckTimestamps[cleanPhone] < WA_CACHE_TTL) {
    return waCheckCache[cleanPhone];
  }
  try {
    const resp = await fetch(`https://wa.me/${cleanPhone}`, {
      method: 'GET',
      redirect: 'manual',
      headers: { 'User-Agent': 'Mozilla/5.0 (compatible; SSA-Dashboard/1.0)' },
      signal: AbortSignal.timeout(8000)
    });
    const location = resp.headers.get('location') || '';
    const status = resp.status;
    const isOnWA = (status === 301 || status === 302) &&
                    location.includes('api.whatsapp.com');
    waCheckCache[cleanPhone] = isOnWA;
    waCheckTimestamps[cleanPhone] = Date.now();
    return isOnWA;
  } catch (e) {
    return null; // unknown
  }
}

// Batch check: enrich an array of students with hasWhatsApp field
// Runs in parallel batches of 5, non-blocking
async function enrichStudentsWithWhatsApp(courses) {
  // Collect unique phone numbers across all courses
  const phoneSet = new Map(); // cleanPhone → [{ course, studentIndex }]
  courses.forEach(course => {
    (course.students || []).forEach((st, idx) => {
      if (!st.phone) { st.hasWhatsApp = false; return; }
      const clean = st.phone.replace(/[^\d]/g, '');
      if (!clean || clean.length < 8) { st.hasWhatsApp = false; return; }
      // If cached, apply immediately
      if (waCheckCache[clean] !== undefined && waCheckTimestamps[clean] &&
          Date.now() - waCheckTimestamps[clean] < WA_CACHE_TTL) {
        st.hasWhatsApp = waCheckCache[clean];
        return;
      }
      if (!phoneSet.has(clean)) phoneSet.set(clean, []);
      phoneSet.get(clean).push({ course, idx });
    });
  });

  const toCheck = Array.from(phoneSet.keys());
  if (toCheck.length === 0) return;

  console.log(`WA check: ${toCheck.length} numbers to verify (${Object.keys(waCheckCache).length} cached)`);

  // Check in parallel batches of 5
  const BATCH = 5;
  for (let i = 0; i < toCheck.length; i += BATCH) {
    const batch = toCheck.slice(i, i + BATCH);
    const results = await Promise.all(batch.map(clean => checkSingleWhatsApp(clean)));
    batch.forEach((clean, j) => {
      const isOnWA = results[j];
      // Apply result to all students with this phone
      phoneSet.get(clean).forEach(({ course, idx }) => {
        course.students[idx].hasWhatsApp = isOnWA;
      });
    });
  }

  const okCount = toCheck.filter(p => waCheckCache[p] === true).length;
  const noCount = toCheck.filter(p => waCheckCache[p] === false).length;
  console.log(`WA check done: ${okCount} on WA, ${noCount} not on WA, ${toCheck.length - okCount - noCount} unknown`);
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
app.post('/api/share/:courseHandle', async (req, res) => {
  try {
    const { courseHandle } = req.params;
    const { ttlDays } = req.body;
    const ttl = (ttlDays || 7) * 24 * 60 * 60 * 1000; // Convert to milliseconds

    const token = generateShareToken();
    const now = new Date();
    const expiresAt = new Date(now.getTime() + ttl);

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

    // Fetch course data by handle
    const products = await fetchAllShopifyProducts();
    const orders = await fetchAllShopifyOrders();

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
    try {
      const mfResp = await shopifyFetch(`/products/${courseProduct.id}/metafields.json`);
      const educatorMf = (mfResp.metafields || []).find(mf =>
        mf.key === 'sake_educator' || mf.key === 'educator' ||
        (mf.key && mf.key.toLowerCase().includes('educator'))
      );
      if (educatorMf && educatorMf.value) shareEducator = educatorMf.value;
    } catch (e) { /* skip */ }
    if (!shareEducator) {
      const educatorTag = tagsArray.find(tag => tag.startsWith('educator:'));
      const savedEducator = courseCosts[courseProduct.handle]?.educatorName;
      shareEducator = educatorTag ? educatorTag.replace('educator:', '') : (savedEducator || '');
    }
    course.educator = shareEducator;

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

    // Deduplicate students (same logic as frontend)
    const seenOrders = new Set();
    const seenEmails = new Set();
    course.students = course.students.filter(st => {
      if (st.orderNumber && seenOrders.has(st.orderNumber)) return false;
      if (st.orderNumber) seenOrders.add(st.orderNumber);
      const emailKey = (st.email || '').toLowerCase().trim();
      if (emailKey && seenEmails.has(emailKey)) return false;
      if (emailKey) seenEmails.add(emailKey);
      return true;
    });
    course.enrollmentCount = course.students.length;

    res.json({ success: true, data: course });
  } catch (error) {
    console.error('Error in /api/shared/:token:', error.message);
    res.status(500).json({ success: false, error: error.message });
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
  </div>
  <div class="footer">Sake Sommelier Association &copy; 2026 &mdash; Link condiviso, sola lettura</div>

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
  });
}

startServer().catch(err => {
  console.error('Failed to start server:', err);
  // Fallback: start without Airtable persistence
  app.listen(PORT, () => {
    console.log(`SSA Backend Server running on http://localhost:${PORT} (without Airtable persistence)`);
  });
});
