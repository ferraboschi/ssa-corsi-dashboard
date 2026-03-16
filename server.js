require('dotenv').config();
const express = require('express');
const cors = require('cors');
const path = require('path');

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
    const cacheKey = 'shopify_products';
    let data = getCache(cacheKey);
    if (!data) {
      const response = await shopifyFetch('/products.json', { headers: { 'limit': '250' } });
      data = response.products || [];
      setCache(cacheKey, data, 300);
    }
    res.json({ success: true, count: data.length, data });
  } catch (error) {
    res.status(500).json({ success: false, error: error.message });
  }
});

app.get('/api/shopify/orders', async (req, res) => {
  try {
    const cacheKey = 'shopify_orders';
    let data = getCache(cacheKey);
    if (!data) {
      const response = await shopifyFetch('/orders.json', { headers: { 'status': 'any', 'limit': '250' } });
      data = response.orders || [];
      setCache(cacheKey, data, 300);
    }
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
      const response = await shopifyFetch('/customers.json', { headers: { 'limit': '250' } });
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
      const response = await airtableFetch(encodeURIComponent(AIRTABLE_TABLE_ID));
      data = response.records || [];
      setCache(cacheKey, data, 300);
    }
    res.json({ success: true, count: data.length, data });
  } catch (error) {
    res.status(500).json({ success: false, error: error.message });
  }
});

// ============================================================================
// INTERNAL ROUTES
// ============================================================================
app.get('/api/courses', async (req, res) => {
  try {
    const productsResponse = await shopifyFetch('/products.json');
    const products = productsResponse.products || [];
    const ordersResponse = await shopifyFetch('/orders.json');
    const orders = ordersResponse.orders || [];
    const courseMap = new Map();
    products.forEach(product => {
      courseMap.set(product.id, {
        id: product.id, name: product.title, handle: product.handle,
        description: product.body_html, variants: product.variants,
        enrollmentCount: 0, revenue: 0, students: [],
        sustainability: {
          organic: product.tags?.includes('organic') || false,
          fairtrade: product.tags?.includes('fair-trade') || false,
          biodynamic: product.tags?.includes('biodynamic') || false
        }
      });
    });
    orders.forEach(order => {
      if (order.line_items && order.line_items.length > 0) {
        order.line_items.forEach(item => {
          if (courseMap.has(item.product_id)) {
            const course = courseMap.get(item.product_id);
            course.enrollmentCount += item.quantity;
            course.revenue += parseFloat(item.total_price) || 0;
            course.students.push({
              name: order.customer?.first_name + ' ' + order.customer?.last_name || 'Unknown',
              email: order.customer?.email || 'N/A',
              orderId: order.id, orderDate: order.created_at, quantity: item.quantity
            });
          }
        });
      }
    });
    const courses = Array.from(courseMap.values());
    res.json({ success: true, count: courses.length, data: courses });
  } catch (error) {
    res.status(500).json({ success: false, error: error.message });
  }
});

app.get('/api/educator/:id', async (req, res) => {
  try {
    const educatorId = req.params.id;
    const productsResponse = await shopifyFetch('/products.json');
    const products = productsResponse.products || [];
    const ordersResponse = await shopifyFetch('/orders.json');
    const orders = ordersResponse.orders || [];
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
            course.revenue += parseFloat(item.total_price) || 0;
            if (order.customer?.email) {
              studentSet.add(JSON.stringify({ name: order.customer.first_name + ' ' + order.customer.last_name, email: order.customer.email }));
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

app.get('/api/export/corsisti', async (req, res) => {
  try {
    const customersResponse = await shopifyFetch('/customers.json');
    const customers = customersResponse.customers || [];
    let csv = 'ID,First Name,Last Name,Email,Phone,City,State,Country,Total Orders,Total Spent\n';
    customers.forEach(customer => {
      const id = customer.id;
      const firstName = customer.first_name?.replace(/"/g, '""') || '';
      const lastName = customer.last_name?.replace(/"/g, '""') || '';
      const email = customer.email || '';
      const phone = customer.phone || '';
      const city = customer.default_address?.city?.replace(/"/g, '""') || '';
      const state = customer.default_address?.province?.replace(/"/g, '""') || '';
      const country = customer.default_address?.country?.replace(/"/g, '""') || '';
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
// STATIC FILES & FALLBACK
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
