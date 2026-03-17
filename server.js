require('dotenv').config();
const express = require('express');
const cors = require('cors');
const path = require('path');
const fs = require('fs');
const crypto = require('crypto');

// Course costs persistent storage
const COSTS_FILE = path.join(__dirname, 'data', 'course-costs.json');
const SHARE_FILE = path.join(__dirname, 'data', 'share-tokens.json');

function loadCosts() {
  try {
    if (fs.existsSync(COSTS_FILE)) {
      return JSON.parse(fs.readFileSync(COSTS_FILE, 'utf8'));
    }
  } catch (e) { console.error('Error loading costs:', e); }
  return {};
}

function saveCostsToFile(costs) {
  try {
    const dir = path.dirname(COSTS_FILE);
    if (!fs.existsSync(dir)) fs.mkdirSync(dir, { recursive: true });
    fs.writeFileSync(COSTS_FILE, JSON.stringify(costs, null, 2));
  } catch (e) { console.error('Error saving costs:', e); }
}

// Share tokens persistent storage
function loadShareTokens() {
  try {
    if (fs.existsSync(SHARE_FILE)) {
      return JSON.parse(fs.readFileSync(SHARE_FILE, 'utf8'));
    }
  } catch (e) { console.error('Error loading share tokens:', e); }
  return {};
}

function saveShareTokensToFile(tokens) {
  try {
    const dir = path.dirname(SHARE_FILE);
    if (!fs.existsSync(dir)) fs.mkdirSync(dir, { recursive: true });
    fs.writeFileSync(SHARE_FILE, JSON.stringify(tokens, null, 2));
  } catch (e) { console.error('Error saving share tokens:', e); }
}

function generateShareToken() {
  return crypto.randomUUID();
}

const courseCosts = loadCosts();
let shareTokens = loadShareTokens();

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
const AIRTABLE_BASE_ID = process.env.AIRTABLE_BASE_ID || 'appB0TjUHqfXr4ekq';
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
          course.enrollmentCount += item.quantity;
          course.revenue += parseFloat(item.price || 0) * item.quantity;

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

          course.students.push({
            name: customerName,
            email: order.customer?.email || '',
            phone: phone,
            orderId: order.id,
            orderNumber: order.name || `#${order.order_number}`,
            orderDate: order.created_at,
            financialStatus: order.financial_status,
            amount: parseFloat(item.price || 0) * item.quantity,
            variantTitle: item.variant_title || ''
          });
        }
      });
    });

    const courses = Array.from(courseMap.values());
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
            course.enrollmentCount += item.quantity;
            course.revenue += parseFloat(item.price || 0) * item.quantity;
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
  res.json(courseCosts);
});

app.get('/api/costs/:courseId', (req, res) => {
  const costs = courseCosts[req.params.courseId] || null;
  res.json(costs);
});

app.post('/api/costs/:courseId', (req, res) => {
  const { courseId } = req.params;
  const { location, educator, food, sake, adv, program, lines } = req.body;
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
  saveCostsToFile(courseCosts);
  res.json({ success: true, costs: courseCosts[courseId] });
});

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

    // Build course data without financial info
    const course = {
      id: courseProduct.id,
      title: courseProduct.title,
      handle: courseProduct.handle,
      body_html: courseProduct.body_html,
      images: courseProduct.images,
      students: []
    };

    // Get educator info from tags or vendor
    const educatorTag = courseProduct.tags?.find(tag => tag.startsWith('educator:'));
    course.educator = educatorTag ? educatorTag.replace('educator:', '') : courseProduct.vendor || '';

    // Get program info from costs (stored by handle)
    const costs = courseCosts[courseProduct.handle];
    if (costs && costs.program) {
      course.program = costs.program;
    }

    // Get student names only (no financial data)
    const studentSet = new Set();
    orders.forEach(order => {
      if (!order.line_items) return;
      order.line_items.forEach(item => {
        if (item.product_id === courseProduct.id) {
          const customerName = order.customer
            ? `${order.customer.first_name || ''} ${order.customer.last_name || ''}`.trim()
            : 'Sconosciuto';
          if (customerName) {
            studentSet.add(customerName);
          }
        }
      });
    });

    course.students = Array.from(studentSet).map(name => ({ name }));

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
  res.send(`
<!DOCTYPE html>
<html lang="it">
<head>
  <meta charset="UTF-8">
  <meta name="viewport" content="width=device-width, initial-scale=1.0">
  <title>SSA - Course Share</title>
  <style>
    * {
      margin: 0;
      padding: 0;
      box-sizing: border-box;
    }
    :root {
      --color-primary: #2a5f3f;
      --color-secondary: #d4a574;
      --color-accent: #8b4513;
      --color-light: #f5f1ed;
      --color-text: #333;
      --color-border: #ddd;
      --font-serif: Georgia, serif;
      --font-sans: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif;
    }
    body {
      font-family: var(--font-sans);
      color: var(--color-text);
      background-color: var(--color-light);
      line-height: 1.6;
    }
    .container {
      max-width: 900px;
      margin: 0 auto;
      padding: 2rem;
    }
    .header {
      text-align: center;
      margin-bottom: 3rem;
      border-bottom: 2px solid var(--color-secondary);
      padding-bottom: 2rem;
    }
    .header h1 {
      font-family: var(--font-serif);
      color: var(--color-primary);
      font-size: 2.5rem;
      margin-bottom: 0.5rem;
    }
    .header .ssa-brand {
      font-size: 0.9rem;
      color: var(--color-secondary);
      font-weight: 600;
      letter-spacing: 2px;
    }
    .status-error {
      background-color: #fee;
      border: 1px solid #fcc;
      color: #c33;
      padding: 1rem;
      border-radius: 4px;
      text-align: center;
      margin-bottom: 2rem;
    }
    .status-expired {
      background-color: #ffeaa7;
      border: 1px solid #ffc107;
      color: #856404;
      padding: 1.5rem;
      border-radius: 4px;
      text-align: center;
      font-size: 1.1rem;
      margin: 2rem 0;
    }
    .course-info {
      background: white;
      border: 1px solid var(--color-border);
      border-radius: 8px;
      padding: 2rem;
      margin-bottom: 2rem;
    }
    .course-title {
      font-family: var(--font-serif);
      font-size: 2rem;
      color: var(--color-primary);
      margin-bottom: 1rem;
    }
    .course-description {
      color: #666;
      margin-bottom: 1.5rem;
      line-height: 1.8;
    }
    .info-row {
      display: flex;
      justify-content: space-between;
      padding: 0.75rem 0;
      border-bottom: 1px solid var(--color-border);
    }
    .info-row:last-child {
      border-bottom: none;
    }
    .info-label {
      font-weight: 600;
      color: var(--color-primary);
      min-width: 150px;
    }
    .info-value {
      text-align: right;
    }
    .educator-section {
      background: white;
      border: 1px solid var(--color-border);
      border-radius: 8px;
      padding: 2rem;
      margin-bottom: 2rem;
    }
    .educator-title {
      font-family: var(--font-serif);
      font-size: 1.3rem;
      color: var(--color-primary);
      margin-bottom: 1rem;
      border-bottom: 2px solid var(--color-secondary);
      padding-bottom: 0.5rem;
    }
    .educator-info {
      color: #666;
      line-height: 1.8;
    }
    .program-section {
      background: white;
      border: 1px solid var(--color-border);
      border-radius: 8px;
      padding: 2rem;
      margin-bottom: 2rem;
    }
    .program-title {
      font-family: var(--font-serif);
      font-size: 1.3rem;
      color: var(--color-primary);
      margin-bottom: 1rem;
      border-bottom: 2px solid var(--color-secondary);
      padding-bottom: 0.5rem;
    }
    .program-content {
      color: #666;
    }
    .sake-group {
      background-color: #fafafa;
      border-left: 4px solid var(--color-secondary);
      padding: 1rem;
      margin-bottom: 1rem;
      border-radius: 4px;
    }
    .sake-group-name {
      font-weight: 600;
      color: var(--color-primary);
      margin-bottom: 0.5rem;
    }
    .sake-item {
      color: #666;
      margin-bottom: 0.3rem;
      padding-left: 1rem;
    }
    .students-section {
      background: white;
      border: 1px solid var(--color-border);
      border-radius: 8px;
      padding: 2rem;
    }
    .students-title {
      font-family: var(--font-serif);
      font-size: 1.3rem;
      color: var(--color-primary);
      margin-bottom: 1rem;
      border-bottom: 2px solid var(--color-secondary);
      padding-bottom: 0.5rem;
    }
    .student-list {
      list-style: none;
      columns: 2;
      gap: 2rem;
    }
    .student-list li {
      padding: 0.5rem 0;
      color: #666;
      break-inside: avoid;
    }
    @media (max-width: 600px) {
      .container { padding: 1rem; }
      .header h1 { font-size: 1.6rem; }
      .course-title { font-size: 1.4rem; }
      .info-row { flex-direction: column; gap: 4px; }
      .info-label { min-width: auto; }
      .info-value { text-align: left; }
      .student-list { columns: 1; }
      .course-info, .educator-section, .program-section, .students-section { padding: 1rem; }
    }
    .loading {
      text-align: center;
      padding: 2rem;
      color: #666;
    }
    .spinner {
      border: 4px solid var(--color-light);
      border-top: 4px solid var(--color-primary);
      border-radius: 50%;
      width: 40px;
      height: 40px;
      animation: spin 1s linear infinite;
      margin: 1rem auto;
    }
    @keyframes spin {
      0% { transform: rotate(0deg); }
      100% { transform: rotate(360deg); }
    }
  </style>
</head>
<body>
  <div class="container">
    <div class="header">
      <div class="ssa-brand">SAKE SOMMELIER ASSOCIATION</div>
      <h1>Course Information</h1>
    </div>
    <div id="content">
      <div class="loading">
        <div class="spinner"></div>
        <p>Loading course information...</p>
      </div>
    </div>
  </div>

  <script>
    async function loadCourseData() {
      try {
        const response = await fetch('/api/shared/${token}');
        const result = await response.json();

        if (!response.ok) {
          if (response.status === 401) {
            document.getElementById('content').innerHTML = '<div class="status-expired">Link scaduto</div>';
          } else {
            document.getElementById('content').innerHTML = '<div class="status-error">Errore: ' + (result.error || 'Course not found') + '</div>';
          }
          return;
        }

        const course = result.data;
        let html = '';

        // Course info
        html += '<div class="course-info">';
        html += '<div class="course-title">' + (course.title || 'Untitled') + '</div>';
        if (course.body_html) {
          html += '<div class="course-description">' + course.body_html + '</div>';
        }
        html += '</div>';

        // Educator
        if (course.educator) {
          html += '<div class="educator-section">';
          html += '<div class="educator-title">Educator</div>';
          html += '<div class="educator-info">' + course.educator + '</div>';
          html += '</div>';
        }

        // Program (sake details)
        if (course.program && Array.isArray(course.program)) {
          html += '<div class="program-section">';
          html += '<div class="program-title">Program</div>';
          html += '<div class="program-content">';
          course.program.forEach(group => {
            html += '<div class="sake-group">';
            html += '<div class="sake-group-name">' + (group.name || 'Group') + '</div>';
            if (Array.isArray(group.sakes)) {
              group.sakes.forEach(sake => {
                html += '<div class="sake-item">' + (sake.name || 'Unknown') + '</div>';
              });
            }
            html += '</div>';
          });
          html += '</div>';
          html += '</div>';
        }

        // Students
        if (course.students && Array.isArray(course.students) && course.students.length > 0) {
          html += '<div class="students-section">';
          html += '<div class="students-title">Students (' + course.students.length + ')</div>';
          html += '<ul class="student-list">';
          course.students.forEach(student => {
            html += '<li>' + (student.name || 'Unknown') + '</li>';
          });
          html += '</ul>';
          html += '</div>';
        }

        document.getElementById('content').innerHTML = html;
      } catch (error) {
        document.getElementById('content').innerHTML = '<div class="status-error">Error loading course: ' + error.message + '</div>';
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
// START SERVER
// ============================================================================
app.listen(PORT, () => {
  console.log(`SSA Backend Server running on http://localhost:${PORT}`);
  console.log(`Environment: ${process.env.NODE_ENV || 'development'}`);
  console.log(`Shopify Store: ${SHOPIFY_STORE}`);
  console.log(`Airtable Base: ${AIRTABLE_BASE_ID}`);
});
