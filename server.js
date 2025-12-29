'use strict';
require('dotenv').config();


console.log('[DEBUG] Environment Variables:');
console.log(' MONGODB_URI:', process.env.MONGODB_URI ? '✅ Loaded' : '❌ Missing');
console.log(' REDIS_URL:', process.env.REDIS_URL ? '✅ Loaded' : '❌ Missing');
console.log(' DB_NAME:', process.env.DB_NAME ? '✅ Loaded' : '❌ Missing');
console.log(' CLUSTER_WORKERS:', process.env.CLUSTER_WORKERS || 'Not set');


const express = require('express');
const { MongoClient } = require('mongodb');
const cors = require('cors');
const rateLimit = require('express-rate-limit');
const app = express();
const PORT = process.env.PORT || 7000;
const MONGODB_URI = process.env.MONGODB_URI || 'mongodb+srv://samir_:fitara@cluster0.cmatn6k.mongodb.net/appdb?retryWrites=true&w=majority';
const DB_NAME = process.env.DB_NAME || 'appdb';
const axios = require('axios');
const Redis = require('ioredis');
const activeRequestsWithTimestamp = new Map();
const REQUEST_DEDUP_TTL = 5000; // 5 seconds
const requestDeduplication = new Map();
const DEDUP_WINDOW = 5000; // 5 seconds
// Global counters and cache
const dbOpCounters = { reads: 0, writes: 0, updates: 0, inserts: 0, deletes: 0, queries: 0, aggregations: 0 };
//const cache = { latestSlots: new Map(), userStatus: new Map(), maxIndexes: new Map(), ttl: 30 };
const router = express.Router();

const cache = {
latestSlots: new Map(),
userStatus: new Map(),
maxIndexes: new Map(),
ttl: 30000 // Increased to 30 seconds
};

const processedRequests = new Map();


const compression = require('compression');
const { promisify } = require('util');


// ===== CONFIGURATION VARIABLES - MODIFY THESE TO CHANGE SYSTEM BEHAVIOR =====
const MAX_CONTENT_PER_SLOT = 40; // Maximum content items per document before creating new slot
const DEFAULT_CONTENT_BATCH_SIZE = 10; // Default number of items to return per request
const MIN_CONTENT_FOR_FEED = 10; // Minimum content required for feed requests
// ============================================================================

console.log('[CONFIG] System Configuration:');
console.log(` MAX_CONTENT_PER_SLOT: ${MAX_CONTENT_PER_SLOT}`);
console.log(` DEFAULT_CONTENT_BATCH_SIZE: ${DEFAULT_CONTENT_BATCH_SIZE}`);
console.log(` MIN_CONTENT_FOR_FEED: ${MIN_CONTENT_FOR_FEED}`);

const CACHE_TTL_SHORT = 15000; // 15 seconds
const CACHE_TTL_MEDIUM = 60000; // 1 minute
const CACHE_TTL_LONG = 300000; // 5 minutes


setInterval(() => {
const now = Date.now();
const expiryTime = 60000; // 60 seconds
let cleanedCount = 0;

for (const [requestId, timestamp] of processedRequests.entries()) {
if (now - timestamp > expiryTime) {
processedRequests.delete(requestId);
cleanedCount++;
}
}

if (cleanedCount > 0) {
console.log(`[PROCESSED-REQUESTS-CLEANUP] Removed ${cleanedCount} expired entries | Active: ${processedRequests.size}`);
}
}, 60000);




class SimpleLRU {
  constructor(max = 1000, ttl = 5000) {
    this.max = max;
    this.ttl = ttl;
    this.cache = new Map(); // maintains insertion order
  }

  _cleanup() {
    const now = Date.now();
    for (const [key, { timestamp }] of this.cache.entries()) {
      if (now - timestamp > this.ttl) {
        this.cache.delete(key);
      }
    }
  }

  get(key) {
    this._cleanup();
    const item = this.cache.get(key);
    if (!item) return undefined;
    // refresh usage order
    this.cache.delete(key);
    this.cache.set(key, item);
    return item.value;
  }

  set(key, value) {
    this._cleanup();
    if (this.cache.size >= this.max) {
      const oldestKey = this.cache.keys().next().value;
      this.cache.delete(oldestKey);
    }
    this.cache.set(key, { value, timestamp: Date.now() });
  }

  has(key) {
    this._cleanup();
    return this.cache.has(key);
  }

  delete(key) {
    this.cache.delete(key);
  }

  size() {
    this._cleanup();
    return this.cache.size;
  }
}




const activeRequests = new SimpleLRU(1000, 5000);

const getOrCreateRequest = (key, requestFactory) => {
  if (activeRequests.has(key)) {
    console.log(`[REQUEST-DEDUP] ${key} - using existing request`);
    return activeRequests.get(key);
  }

  const promise = requestFactory().finally(() => {
    activeRequests.delete(key);
  });

  activeRequests.set(key, promise);
  return promise;
};

  

// CRITICAL: Enhanced logging with collection scan detection
function logDbOp(op, col, query = {}, result = null, time = 0, options = {}) {
const ts = new Date().toISOString();
let queryStr = '';
let scanWarning = '';

// Format query string
if (op.toLowerCase() === 'aggregate' && query.pipeline) {
if (typeof query.pipeline === 'string') {
queryStr = query.pipeline;
} else if (Array.isArray(query.pipeline)) {
const stages = query.pipeline.slice(0, 2).map(stage => {
const key = Object.keys(stage)[0];
return `{${key}}`;
}).join(' -> ');
queryStr = `[${stages}${query.pipeline.length > 2 ? ' ...' : ''}]`;
}
} else {
queryStr = JSON.stringify(query).length > 100
? JSON.stringify(query).substring(0, 100) + '...'
: JSON.stringify(query);
}

// CRITICAL: Detect collection scans (potential performance issues)
let docsScanned = 0;
let docsReturned = 0;
let isCollectionScan = false;

if (Array.isArray(result)) {
docsReturned = result.length;
docsScanned = options.docsExamined || result.length; // Will be passed from explain()

// WARN: If query has no indexed fields, it's likely a collection scan
if (!query._id && !Object.keys(query).some(k => k.includes('userId') || k.includes('postId'))) {
isCollectionScan = true;
scanWarning = '⚠️ POSSIBLE COLLECTION SCAN';
}
} else if (result?.matchedCount !== undefined) {
docsScanned = result.matchedCount;
docsReturned = result.modifiedCount || 0;
} else if (result?.insertedId) {
docsScanned = 1;
docsReturned = 1;
} else if (result !== null && typeof result === 'object') {
docsScanned = 1;
docsReturned = 1;
}

// Build result info with document details
let resultInfo = '';
if (docsReturned > 0 || docsScanned > 0) {
resultInfo = ` | scanned: ${docsScanned} docs, returned: ${docsReturned} docs`;

// Log document IDs for small result sets
if (Array.isArray(result) && result.length <= 3 && result.length > 0) {
const docIds = result.map(r => r._id || 'no-id').join(', ');
resultInfo += ` | docs: [${docIds}]`;
}
}

// CRITICAL: Log with enhanced details
console.log(`[DB-${op.toUpperCase()}] ${ts} | ${col}${resultInfo} | query: ${queryStr} | ${time}ms${scanWarning}`);

// **UPDATE COUNTERS AFTER LOGGING (not before)**
const opLower = op.toLowerCase();
if (['find', 'findone', 'count'].includes(opLower)) {
dbOpCounters.reads += docsScanned || 1; // Count actual docs scanned
dbOpCounters.queries++;
}
else if (opLower === 'aggregate') {
dbOpCounters.reads += docsScanned || 1;
dbOpCounters.aggregations++;
}
else if (['insertone', 'insertmany'].includes(opLower)) {
dbOpCounters.writes += docsReturned || 1;
dbOpCounters.inserts++;
}
else if (['updateone', 'updatemany', 'findoneandupdate', 'bulkwrite'].includes(opLower)) {
dbOpCounters.writes += docsScanned || 1; // Writes = docs matched
dbOpCounters.updates++;
}
else if (['deleteone', 'deletemany'].includes(opLower)) {
dbOpCounters.writes += docsScanned || 1;
dbOpCounters.deletes++;
}

// CRITICAL SCALE WARNING
if (docsScanned > 100) {
console.warn(`⚠️ [SCALE-WARNING] ${col}.${op} scanned ${docsScanned} documents - may cause issues at scale!`);
}

if (isCollectionScan && docsScanned > 10) {
console.error(`🚨 [CRITICAL-SCAN] ${col}.${op} performed COLLECTION SCAN on ${docsScanned} docs - NEEDS INDEX!`);
}
}

// Cache helpers
// Update the setCache function
const setCache = async (key, value, ttl = 30000) => {
if (redisClient) {
try {
await redisClient.setex(key, Math.floor(ttl / 1000), JSON.stringify(value));
console.log(`[CACHE-SET-REDIS] ${key} | TTL: ${ttl}ms`);
} catch (err) {
log('error', '[CACHE-SET-ERROR]', err.message);
}
} else {
// Fallback to in-memory (development only)
if (!cache[key]) cache[key] = new Map();
cache[key].set('value', value).set('timestamp', Date.now()).set('ttl', ttl);
console.log(`[CACHE-SET-MEMORY] ${key} | TTL: ${ttl}ms`);
}
};

// Update getCache to log hits/misses
const getCache = async (key) => {
if (redisClient) {
try {
const value = await redisClient.get(key);
if (value) {
console.log(`[CACHE-HIT-REDIS] ${key}`);
return JSON.parse(value);
}
console.log(`[CACHE-MISS-REDIS] ${key}`);
return null;
} catch (err) {
log('error', '[CACHE-GET-ERROR]', err.message);
return null;
}
} else {
// Fallback to in-memory
if (!cache[key]) {
console.log(`[CACHE-MISS-MEMORY] ${key} - not found`);
return null;
}
const timestamp = cache[key].get('timestamp'), ttl = cache[key].get('ttl'), value = cache[key].get('value');
if (!timestamp || (Date.now() - timestamp > ttl)) {
cache[key].clear();
console.log(`[CACHE-MISS-MEMORY] ${key} - expired`);
return null;
}
console.log(`[CACHE-HIT-MEMORY] ${key}`);
return value;
}
};

app.set('trust proxy', 1);

// Middleware
app.use(cors());
app.use(express.json({ limit: '50mb' }));
app.use((req, res, next) => { console.log(`[HTTP] ${new Date().toISOString()} ${req.method} ${req.originalUrl}`); next(); });
app.use('/api', rateLimit({ windowMs: 1000, max: 1000, standardHeaders: true, legacyHeaders: false }));

app.use(express.json());



app.use(compression({
level: 6, // Balance between speed and compression
threshold: 1024, // Only compress responses > 1KB
filter: (req, res) => {
if (req.headers['x-no-compression']) return false;
return compression.filter(req, res);
}
}));

// CRITICAL: Response time logging middleware
app.use((req, res, next) => {
const startHrTime = process.hrtime();

res.on('finish', () => {
const elapsedHrTime = process.hrtime(startHrTime);
const elapsedMs = elapsedHrTime[0] * 1000 + elapsedHrTime[1] / 1000000;

if (elapsedMs > 100) {
console.warn(`[SLOW-REQUEST] ${req.method} ${req.originalUrl} took ${elapsedMs.toFixed(0)}ms`);
} else {
console.log(`[FAST-REQUEST] ${req.method} ${req.originalUrl} in ${elapsedMs.toFixed(0)}ms`);
}
});

next();
});


// CRITICAL: Track and summarize DB activity per request
app.use((req, res, next) => {
req.dbActivityStart = {
reads: dbOpCounters.reads,
writes: dbOpCounters.writes,
queries: dbOpCounters.queries,
aggregations: dbOpCounters.aggregations
};

res.on('finish', () => {
const activity = {
reads: dbOpCounters.reads - req.dbActivityStart.reads,
writes: dbOpCounters.writes - req.dbActivityStart.writes,
queries: dbOpCounters.queries - req.dbActivityStart.queries,
aggregations: dbOpCounters.aggregations - req.dbActivityStart.aggregations
};

if (activity.reads > 0 || activity.writes > 0) {
console.log(`[REQUEST-DB-SUMMARY] ${req.method} ${req.originalUrl} | ` +
`Reads: ${activity.reads}, Writes: ${activity.writes}, ` +
`Queries: ${activity.queries}, Aggregations: ${activity.aggregations}`);

if (activity.reads > 20) {
console.warn(`⚠️ [HIGH-READ-COUNT] ${req.originalUrl} used ${activity.reads} reads - optimize!`);
}
}
});

next();
});




// Enhanced request logging with traffic metrics
const requestMetrics = {
totalRequests: 0,
activeConnections: 0,
endpointCounts: {},
avgResponseTime: 0,
requestTimes: []
};

app.use((req, res, next) => {
const startTime = Date.now();
requestMetrics.totalRequests++;
requestMetrics.activeConnections++;

const endpoint = `${req.method} ${req.originalUrl.split('?')[0]}`;
requestMetrics.endpointCounts[endpoint] = (requestMetrics.endpointCounts[endpoint] || 0) + 1;

console.log(`[REQUEST] Total: ${requestMetrics.totalRequests} | Active: ${requestMetrics.activeConnections} | ${endpoint}`);

res.on('finish', () => {
const duration = Date.now() - startTime;
requestMetrics.activeConnections--;
requestMetrics.requestTimes.push(duration);

// Keep only last 100 request times
if (requestMetrics.requestTimes.length > 100) {
requestMetrics.requestTimes.shift();
}

const avgTime = requestMetrics.requestTimes.reduce((a, b) => a + b, 0) / requestMetrics.requestTimes.length;
requestMetrics.avgResponseTime = avgTime.toFixed(0);

console.log(`[RESPONSE] ${endpoint} | Time: ${duration}ms | Avg: ${requestMetrics.avgResponseTime}ms | DB Reads: ${dbOpCounters.reads} | DB Writes: ${dbOpCounters.writes}`);
});

next();
});

app.use('/api', rateLimit({ windowMs: 1000, max: 1000, standardHeaders: true, legacyHeaders: false }));



let client, db, dbManager;


// ---------- logger shim (place near the top of server.js, after imports) ----------
const log = (level, ...args) => {
const ts = new Date().toISOString();
if (level === 'error') return console.error(`[${level.toUpperCase()}] ${ts}`, ...args);
if (level === 'warn') return console.warn(`[${level.toUpperCase()}] ${ts}`, ...args);
return console.log(`[${level.toUpperCase()}] ${ts}`, ...args);
};




// MongoDB initialization
async function initMongo() {
  console.log('[MONGO-INIT] Starting connection...');

  // Suppress non-critical warnings
  process.removeAllListeners('warning');
  process.on('warning', (warning) => {
    if (!warning.message?.includes('snappy') && 
        !warning.message?.includes('kerberos')) {
      console.warn('[NODE-WARNING]', warning.message);
    }
  });

  const MONGODB_URI = process.env.MONGODB_URI || 
    'mongodb+srv://samir_:fitara@cluster0.cmatn6k.mongodb.net/appdb?retryWrites=true&w=majority';

  console.log(`[MONGO-INIT] Connecting to: ${MONGODB_URI.substring(0, 30)}...`);

  // ✅ PRODUCTION-READY: Compression with fallback
  const compressors = ['zlib'];
  try {
    require.resolve('snappy');
    compressors.unshift('snappy');
    console.log('[MONGO-INIT] ✅ Snappy compression available');
  } catch (e) {
    console.log('[MONGO-INIT] ⚠️ Snappy not available, using zlib compression');
  }

  // ✅ OPTIMIZED CONNECTION SETTINGS (2024 best practices)
  client = new MongoClient(MONGODB_URI, {
    // Connection pool (FIXED - removed duplicate definitions)
    maxPoolSize: 50,                    // Reduced from 100 for stability
    minPoolSize: 5,                     // Reduced from 10 for faster cold starts
    maxConnecting: 10,                  // CRITICAL FIX: 20% of maxPoolSize (was 2)
    maxIdleTimeMS: 30000,
    waitQueueTimeoutMS: 10000,
    
    // Timeouts (aggressive for serverless)
    serverSelectionTimeoutMS: 5000,     // Fail fast (reduced from 10s)
    socketTimeoutMS: 45000,
    connectTimeoutMS: 10000,
    heartbeatFrequencyMS: 10000,
    
    // Read/Write preferences
    readPreference: 'nearest',
    retryWrites: true,
    retryReads: true,
    
    // Write concern (balanced durability + performance)
    w: 'majority',                      // Changed from w:1 for data safety
    journal: true,
    
    // Compression
    compressors: compressors,
    
    // Performance optimizations
    monitorCommands: false,             // Disable in production (reduces overhead)
    directConnection: false,
    autoEncryption: undefined,
    
    // ✅ NEW: 2024 MongoDB driver features
    serverMonitoringMode: 'stream',     // Faster health checks
  });

  console.log(`[MONGO-INIT] Compression: ${compressors.join(', ')}`);
  console.log('[MONGO-INIT] Connecting with production-optimized settings...');

  // ✅ CONNECTION POOL MONITORING (critical for debugging)
  client.on('connectionPoolCreated', (event) => {
    console.log(`[POOL-EVENT] Created pool for ${event.address}`);
  });
  
  client.on('connectionPoolReady', (event) => {
    console.log(`[POOL-EVENT] ✅ Pool ready for ${event.address}`);
  });
  
  client.on('connectionPoolCleared', (event) => {
    console.warn(`[POOL-WARNING] ⚠️ Pool cleared for ${event.address} - reason: ${event.reason || 'unknown'}`);
  });
  
  client.on('connectionCheckedOut', () => {
    // Check pool pressure (80% threshold)
    const activeConnections = client.topology?.s?.sessionPool?.sessions?.size || 0;
    if (activeConnections > 40) {  // 80% of maxPoolSize (50)
      console.warn(`⚠️ [POOL-PRESSURE] ${activeConnections}/50 connections active`);
    }
  });

  // Connect to database
  await client.connect();
  db = client.db(DB_NAME);
  console.log(`[MONGO-INIT] ✅ Connected to ${DB_NAME}`);

  // ✅ SHARDING SUPPORT (optional - only if enabled)
  if (process.env.ENABLE_SHARDING === 'true') {
    await enableSharding();
  }


  await createContribIndexes();
  // ✅ CREATE ALL INDEXES (consolidated function)
  await createAllProductionIndexes();

  // ✅ VERIFY CRITICAL INDEXES
  await verifyIndexes();

  // ✅ BACKGROUND METRICS SYNC (optional - can be disabled in production)
  if (process.env.ENABLE_PERIODIC_SYNC !== 'false') {
    setInterval(async () => {
      try {
        console.log('[PERIODIC-SYNC] Running background metrics sync check');
        
        const fiveMinutesAgo = new Date(Date.now() - 5 * 60 * 1000);
        const collections = ['posts', 'reels'];
        
        for (const collection of collections) {
          const arrayField = collection === 'reels' ? 'reelsList' : 'postList';
          const docs = await db.collection(collection).find({
            [`${arrayField}.lastSynced`]: { $lt: fiveMinutesAgo.toISOString() }
          }).limit(50).toArray();

          console.log(`[PERIODIC-SYNC] Found ${docs.length} outdated ${collection}`);
        }
      } catch (error) {
        console.error('[PERIODIC-SYNC-ERROR]', error);
      }
    }, 5 * 60 * 1000); // Every 5 minutes
  }

  console.log('[MONGO-INIT] ✅ Initialization complete');
}


async function createContribIndexes() {
  try {
    // ✅ CRITICAL: Compound index for FAST filtering
    await db.collection('contrib_posts').createIndex(
      { userId: 1, slotId: 1 },
      { 
        name: 'userId_slotId_lookup',
        background: true
      }
    );

    await db.collection('contrib_reels').createIndex(
      { userId: 1, slotId: 1 },
      { 
        name: 'userId_slotId_lookup',
        background: true
      }
    );

    console.log('[CONTRIB-INDEXES] ✅ Created compound indexes for userId + slotId');
  } catch (err) {
    if (err.code !== 85) { // Ignore "already exists"
      console.error('[CONTRIB-INDEX-ERROR]', err.message);
    }
  }
}


// ============================================================
// ✅ CONSOLIDATED INDEX CREATION (replaces all separate functions)
// ============================================================
async function createAllProductionIndexes() {
  console.log('[INDEXES] Creating production indexes...');
  
  const allIndexes = [


{
  collection: 'user_status',
  index: { _id: 1, reel_0_visits: 1, post_0_visits: 1 },
  options: { 
    name: 'slot_0_visit_tracking',
    background: true
  }
},


{
  collection: 'reels',
  index: { 'reelsList.postId': 1 },
  options: { 
    name: 'postId_to_slot_lookup_reels',
    background: true,
    sparse: true
  }
},
{
  collection: 'posts',
  index: { 'postList.postId': 1 },
  options: { 
    name: 'postId_to_slot_lookup_posts',
    background: true,
    sparse: true
  }
},
    
    
    {
      collection: 'reels',
      index: { 
        count: 1,                  // Filter: count > 0
        index: -1,                 // Sort: index DESC
        'reelsList.postId': 1      // Exclude viewed content
      },
      options: { 
        name: 'feed_query_optimized_reels',
        background: true,
        partialFilterExpression: { count: { $gt: 0 } }  // Only non-empty slots
      }
    },
    {
      collection: 'posts',
      index: { 
        count: 1, 
        index: -1,
        'postList.postId': 1
      },
      options: { 
        name: 'feed_query_optimized_posts',
        background: true,
        partialFilterExpression: { count: { $gt: 0 } }
      }
    },

    // ============================================================
    // CRITICAL: Contribution Collections (for viewed content lookup)
    // ============================================================
    {
      collection: 'contrib_reels',
      index: { userId: 1, slotId: 1 },
      options: { 
        name: 'contrib_lookup_reels',
        background: true,
        unique: true  // Prevent duplicate entries
      }
    },
    {
      collection: 'contrib_posts',
      index: { userId: 1, slotId: 1 },
      options: { 
        name: 'contrib_lookup_posts',
        background: true,
        unique: true
      }
    },

    // ============================================================
    // RANKING ALGORITHM: Retention + Engagement
    // ============================================================
    {
      collection: 'reels',
      index: {
        'reelsList.retention': -1,
        'reelsList.likeCount': -1,
        'reelsList.category': 1,
        'reelsList.postId': 1
      },
      options: {
        name: 'ranking_algorithm_reels',
        background: true,
        sparse: true  // Only items with retention data
      }
    },
    {
      collection: 'posts',
      index: {
        'postList.retention': -1,
        'postList.likeCount': -1,
        'postList.category': 1,
        'postList.postId': 1
      },
      options: {
        name: 'ranking_algorithm_posts',
        background: true,
        sparse: true
      }
    },

    // ============================================================
    // FAST POSTID LOOKUP (for exclusion and slot identification)
    // ============================================================
    {
      collection: 'reels',
      index: { 'reelsList.postId': 1 },
      options: { 
        name: 'postId_lookup_reels',
        background: true,
        sparse: true
      }
    },
    {
      collection: 'posts',
      index: { 'postList.postId': 1 },
      options: { 
        name: 'postId_lookup_posts',
        background: true,
        sparse: true
      }
    },

    // ============================================================
    // USER STATUS (primary key - MongoDB creates automatically but verify)
    // ============================================================
    {
      collection: 'user_status',
      index: { _id: 1 },
      options: { 
        name: 'user_status_primary',
        background: true
      }
    },

    // ============================================================
    // INTERACTION CACHE (for retention tracking)
    // ============================================================
    {
      collection: 'user_interaction_cache',
      index: { userId: 1, retentionContributed: 1 },
      options: { 
        name: 'retention_check_fast',
        background: true
      }
    },
    {
      collection: 'user_interaction_cache',
      index: { ttl: 1 },
      options: { 
        name: 'cache_ttl_cleanup',
        background: true,
        expireAfterSeconds: 0  // Auto-cleanup based on ttl field
      }
    },

    // ============================================================
    // SLOT ALLOCATION (prevents full collection scans)
    // ============================================================
    {
      collection: 'posts',
      index: { count: 1, index: -1 },
      options: { 
        name: 'slot_allocation_posts',
        background: true
      }
    },
    {
      collection: 'reels',
      index: { count: 1, index: -1 },
      options: { 
        name: 'slot_allocation_reels',
        background: true
      }
    },

    // ============================================================
    // INSTAGRAM-STYLE FEED (following + timestamp)
    // ============================================================
    {
      collection: 'posts',
      index: { 
        'postList.userId': 1, 
        'postList.timestamp': -1, 
        'postList.postId': 1 
      },
      options: { 
        name: 'feed_following_posts',
        background: true
      }
    },
    {
      collection: 'reels',
      index: { 
        'reelsList.userId': 1, 
        'reelsList.timestamp': -1, 
        'reelsList.postId': 1 
      },
      options: { 
        name: 'feed_following_reels',
        background: true
      }
    },

    // ============================================================
    // BASIC INDEXES (existing collections)
    // ============================================================
    {
      collection: 'posts',
      index: { index: 1 },
      options: { name: 'posts_index', background: true }
    },
    {
      collection: 'reels',
      index: { index: 1 },
      options: { name: 'reels_index', background: true }
    },
    {
      collection: 'user_posts',
      index: { userId: 1, postId: 1 },
      options: { 
        name: 'user_posts_unique',
        background: true,
        unique: true,
        sparse: true
      }
    },
  ];

  let successCount = 0;
  let skipCount = 0;
  let errorCount = 0;

  for (const { collection, index, options } of allIndexes) {
    try {
      await db.collection(collection).createIndex(index, options);
      successCount++;
      console.log(`[INDEX-CREATED] ${collection}.${options.name}`);
    } catch (err) {
      // Silently skip "already exists" errors
      if (err.code === 85 || err.code === 86 || 
          err.message?.includes('already exists') ||
          err.message?.includes('snappy')) {
        skipCount++;
      } else {
        errorCount++;
        console.error(`[INDEX-ERROR] ${collection}.${options.name}: ${err.message}`);
      }
    }
  }

  console.log(`[INDEXES] ✅ Created: ${successCount} | Existed: ${skipCount} | Errors: ${errorCount}`);
}

// ============================================================
// ✅ INDEX VERIFICATION (ensures critical indexes exist)
// ============================================================
async function verifyIndexes() {
  console.log('[INDEX-VERIFY] Checking critical indexes...');
  
  const criticalIndexes = [
    { collection: 'reels', fields: ['index', 'reelsList.postId'] },
    { collection: 'posts', fields: ['index', 'postList.postId'] },
    { collection: 'contrib_reels', fields: ['userId', 'slotId'] },
    { collection: 'contrib_posts', fields: ['userId', 'slotId'] },
    { collection: 'user_status', fields: ['_id'] }
  ];

  let allVerified = true;

  for (const { collection, fields } of criticalIndexes) {
    try {
      const indexes = await db.collection(collection).indexes();
      
      for (const field of fields) {
        const hasIndex = indexes.some(idx => {
          const keys = Object.keys(idx.key);
          return keys.includes(field) || keys.some(k => k.startsWith(field));
        });
        
        if (!hasIndex) {
          console.error(`🚨 [CRITICAL-MISSING-INDEX] ${collection}.${field} NOT INDEXED!`);
          allVerified = false;
        }
      }
    } catch (err) {
      console.error(`[INDEX-VERIFY-ERROR] ${collection}: ${err.message}`);
      allVerified = false;
    }
  }

  if (allVerified) {
    console.log('[INDEX-VERIFY] ✅ All critical indexes verified');
  } else {
    console.warn('[INDEX-VERIFY] ⚠️ Some indexes missing - performance may be degraded');
  }
}


async function initializeSlots() {
try {
console.log('[SLOT-INIT] Initializing first slots for posts and reels...');

const collections = ['posts', 'reels'];

for (const colName of collections) {
const coll = db.collection(colName);

// Check if collection has any documents
const count = await coll.countDocuments();

if (count === 0) {
console.log(`[SLOT-INIT] ${colName} is empty, creating first slot`);

try {
await coll.insertOne({
_id: `${colName.slice(0, -1)}_0`,
index: 0,
count: 0,
[colName === 'reels' ? 'reelsList' : 'postList']: [],
createdAt: new Date().toISOString()
});

console.log(`[SLOT-INIT-SUCCESS] Created initial slot for ${colName}`);
} catch (err) {
if (err.code === 11000) {
console.log(`[SLOT-INIT-EXISTS] ${colName} slot already exists`);
} else {
throw err;
}
}
} else {
console.log(`[SLOT-INIT-SKIP] ${colName} already has ${count} documents`);
}
}

console.log('[SLOT-INIT] Slot initialization complete');
} catch (error) {
console.error('[SLOT-INIT-ERROR]', error.message);
throw error;
}
}



// NEW FUNCTION: Enable sharding for production
async function enableSharding() {
try {
const adminDb = client.db('admin');

// Check if database is already sharded
const dbs = await adminDb.command({ listDatabases: 1 });
const appdb = dbs.databases.find(d => d.name === DB_NAME);

if (!appdb) {
log('info', '[SHARDING] Database does not exist yet, skipping sharding setup');
return;
}

// Enable sharding for database (safe to run multiple times)
try {
await adminDb.command({ enableSharding: DB_NAME });
log('info', '[SHARDING] Database sharding enabled');
} catch (err) {
if (err.code === 23 || err.codeName === 'AlreadyInitialized') {
log('info', '[SHARDING] Database already sharded');
} else {
throw err;
}
}

// Shard key strategies for your collections
const shardConfigs = [
// Shard posts by userId (queries always include userId)
{
collection: 'posts',
key: { index: 1, '_id': 1 }, // Compound shard key
unique: false
},
{
collection: 'reels',
key: { index: 1, '_id': 1 },
unique: false
},
// Shard contribution tracking by userId
{
collection: 'contrib_posts',
key: { userId: 'hashed' }, // Hashed for even distribution
unique: false
},
{
collection: 'contrib_reels',
key: { userId: 'hashed' },
unique: false
},
// Shard user interactions by userId + date
{
collection: 'user_reel_interactions',
key: { userId: 1, date: -1 },
unique: false
},
];

for (const config of shardConfigs) {
try {
await adminDb.command({
shardCollection: `${DB_NAME}.${config.collection}`,
key: config.key,
unique: config.unique
});
log('info', `[SHARDING] Sharded ${config.collection} on key: ${JSON.stringify(config.key)}`);
} catch (err) {
if (err.code !== 23) { // Ignore "already sharded" error
log('warn', `[SHARDING-WARN] ${config.collection}: ${err.message}`);
}
}
}

log('info', '[SHARDING] All collections configured for sharding');
} catch (error) {
log('error', '[SHARDING-ERROR]', error.message);
// Don't crash - continue with non-sharded setup for development
}
}



async function ensurePostIdUniqueness() {
try {
console.log('[UNIQUENESS-CHECK] Ensuring postId uniqueness...');

// Create unique index on postId within arrays (will prevent future duplicates)
await Promise.all([
db.collection('posts').createIndex(
{ 'postList.postId': 1 },
{
name: 'postId_uniqueness_check',
background: true,
sparse: true
}
),
db.collection('reels').createIndex(
{ 'reelsList.postId': 1 },
{
name: 'reelId_uniqueness_check',
background: true,
sparse: true
}
)
]);

console.log('[UNIQUENESS-CHECK] ✅ Indexes created');
} catch (error) {
if (error.code !== 85) { // Ignore "already exists"
console.error('[UNIQUENESS-CHECK-ERROR]', error.message);
}
}
}



class DatabaseManager {
constructor(db) { this.db = db; }


async getOptimizedFeedFixedReads(userId, contentType, minContentRequired = MIN_CONTENT_FOR_FEED) {
  const start = Date.now();
  const isReel = contentType === 'reels';
  const collection = isReel ? 'reels' : 'posts';
  const statusField = isReel ? 'latestReelSlotId' : 'latestPostSlotId';
  const normalField = isReel ? 'normalReelSlotId' : 'normalPostSlotId';
  const visitField = isReel ? 'reel_0_visits' : 'post_0_visits';
  const listKey = isReel ? 'reelsList' : 'postList';

  console.log(`\n${'='.repeat(80)}`);
  console.log(`[MONITORING SAMIR] [FEED-START] userId=${userId} | type=${contentType} | minRequired=${minContentRequired}`);
  console.log(`${'='.repeat(80)}`);

  // ============================================================
  // READ 1: Get user status - ALWAYS FRESH, NO CACHE
  // ============================================================
  console.log(`[MONITORING SAMIR] [READ-1] Checking user_status collection for userId=${userId}`);
  const userStatusReadStart = Date.now();
  
  const userStatus = await this.db.collection('user_status').findOne(
    { _id: userId },
    { projection: { [statusField]: 1, [normalField]: 1, [visitField]: 1 } }
  );
  
  console.log(`[MONITORING SAMIR] [READ-1-COMPLETE] Duration: ${Date.now() - userStatusReadStart}ms | Found: ${!!userStatus}`);
  
  const isNewUser = !userStatus || !userStatus[statusField];
  
  let slotsToRead = [];
  let newLatestSlot = null;
  let newNormalSlot = null;
  const documentsChecked = [];

  // ============================================================
  // CASE 1: FRESH USER (First Time)
  // ============================================================
  if (isNewUser) {
    console.log(`[MONITORING SAMIR] [CASE-1] FRESH USER - Finding latest slot`);
    
    const latestSlotCheckStart = Date.now();
    const latestSlot = await this.getLatestSlotOptimized(collection);
    console.log(`[MONITORING SAMIR] [LATEST-SLOT-CHECK] Duration: ${Date.now() - latestSlotCheckStart}ms | Found: ${latestSlot?._id || 'NONE'}`);
    
    if (!latestSlot) {
      console.warn(`[MONITORING SAMIR] [NO-SLOTS] ${collection} collection is empty`);
      return { content: [], latestDocumentId: null, normalDocumentId: null, isNewUser: true };
    }

    const latestIndex = latestSlot.index;
    
    // CASE 1 LOGIC: Read [latest, latest-1, latest-2]
    slotsToRead = [
      `${collection.slice(0, -1)}_${latestIndex}`,
      `${collection.slice(0, -1)}_${Math.max(latestIndex - 1, 0)}`,
      `${collection.slice(0, -1)}_${Math.max(latestIndex - 2, 0)}`
    ];

    newLatestSlot = slotsToRead[0];
    newNormalSlot = slotsToRead[2]; // latest-2

    console.log(`[MONITORING SAMIR] [CASE-1-PLAN] Will read slots: [${slotsToRead.join(', ')}]`);
    console.log(`[MONITORING SAMIR] [CASE-1-PLAN] Will save: latestSlot=${newLatestSlot}, normalSlot=${newNormalSlot}`);

  } else {
    // ============================================================
    // RETURNING USER LOGIC (Cases 2-6)
    // ============================================================
    console.log(`[MONITORING SAMIR] [RETURNING-USER] Analyzing current status`);
    
    const currentLatestSlot = userStatus[statusField];
    const currentNormalSlot = userStatus[normalField];
    const slot0Visits = userStatus[visitField] || 0;
    
    console.log(`[MONITORING SAMIR] [CURRENT-STATUS] latestSlot=${currentLatestSlot}, normalSlot=${currentNormalSlot}, slot_0_visits=${slot0Visits}`);
    
    const latestMatch = currentLatestSlot.match(/_(\d+)$/);
    const latestIndex = latestMatch ? parseInt(latestMatch[1]) : 0;

    const normalMatch = currentNormalSlot.match(/_(\d+)$/);
    const normalIndex = normalMatch ? parseInt(normalMatch[1]) : 0;

    // ============================================================
    // CRITICAL RULE: FORWARD CHECK PRIORITY
    // ============================================================
    const newerSlotId = `${collection.slice(0, -1)}_${latestIndex + 1}`;
    console.log(`[MONITORING SAMIR] [FORWARD-CHECK-PRIORITY] Looking for ${newerSlotId} (always check forward first)`);
    
    const newerCheckStart = Date.now();
    const newerSlotExists = await this.db.collection(collection).findOne(
      { _id: newerSlotId },
      { projection: { _id: 1 } }
    );
    documentsChecked.push({ slot: newerSlotId, exists: !!newerSlotExists, duration: Date.now() - newerCheckStart });
    console.log(`[MONITORING SAMIR] [CHECKED-DOCUMENT] ${newerSlotId} | Exists: ${!!newerSlotExists} | Duration: ${Date.now() - newerCheckStart}ms`);

    // ============================================================
    // CASE 2/3: New Forward Slot Exists
    // ============================================================
    if (newerSlotExists) {
      console.log(`[MONITORING SAMIR] [CASE-2/3] NEWER-SLOT-FOUND: ${newerSlotId}`);
      
      slotsToRead = [
        newerSlotId,
        currentNormalSlot,
        `${collection.slice(0, -1)}_${Math.max(normalIndex - 1, 0)}`
      ];

      newLatestSlot = newerSlotId;
      newNormalSlot = slotsToRead[2];

      console.log(`[MONITORING SAMIR] [CASE-2/3-PLAN] Will read: [${slotsToRead.join(', ')}]`);
      console.log(`[MONITORING SAMIR] [CASE-2/3-PLAN] Will save: latestSlot=${newLatestSlot}, normalSlot=${newNormalSlot}`);

    } else {
      console.log(`[MONITORING SAMIR] [NO-NEWER-SLOT] ${newerSlotId} doesn't exist`);
      
      // ============================================================
      // CHECK IF AT SLOT_0 (Cases 4, 5, 6)
      // ============================================================
if (normalIndex === 0) {
  console.log(`[MONITORING SAMIR] [AT-SLOT-0] normalSlot is at slot_0 | Visit count: ${slot0Visits}`);
  
  // ============================================================
  // CASE 4: FIRST TIME AT SLOT_0
  // ============================================================
  if (slot0Visits === 0) {
    console.log(`[MONITORING SAMIR] [CASE-4] FIRST TIME at slot_0 - checking forward slots`);
    
    const forwardSlots = [];
    
    // ✅ Check forward slots STARTING FROM latestIndex + 1
    for (let i = latestIndex + 1; i <= latestIndex + 3; i++) {
      const checkSlot = `${collection.slice(0, -1)}_${i}`;
      const checkStart = Date.now();
      const exists = await this.db.collection(collection).findOne(
        { _id: checkSlot },
        { projection: { _id: 1 } }
      );
      const checkDuration = Date.now() - checkStart;
      documentsChecked.push({ slot: checkSlot, exists: !!exists, duration: checkDuration });
      console.log(`[MONITORING SAMIR] [CASE-4-FORWARD-CHECK] ${checkSlot} | Exists: ${!!exists} | Duration: ${checkDuration}ms`);
      
      if (exists) {
        forwardSlots.push(checkSlot);
      } else {
        break; // ✅ Stop at first missing slot
      }
    }
    
    if (forwardSlots.length > 0) {
      // ✅ Found forward slots - read [slot_0, forward_1, forward_2]
      slotsToRead = [
        currentNormalSlot, // slot_0
        ...forwardSlots.slice(0, 2) // Take max 2 forward
      ].filter((v, i, a) => a.indexOf(v) === i).slice(0, 3); // ✅ Max 3 slots
      
      newLatestSlot = forwardSlots[forwardSlots.length - 1];
      newNormalSlot = currentNormalSlot; // ✅ Stay at slot_0
      
      console.log(`[MONITORING SAMIR] [CASE-4-FORWARD-FOUND] Will read: [${slotsToRead.join(', ')}]`);
      console.log(`[MONITORING SAMIR] [CASE-4-STRATEGY] Use MEDIUM engagement (50%+ retention OR 20+ likes)`);
    } else {
      // ✅ No forward slots - read [slot_0, latest, latest-1]
      slotsToRead = [
        currentNormalSlot, // slot_0
        currentLatestSlot,
        `${collection.slice(0, -1)}_${Math.max(latestIndex - 1, 0)}`
      ].filter((v, i, a) => a.indexOf(v) === i).slice(0, 3);
      
      newLatestSlot = currentLatestSlot; // ✅ Unchanged
      newNormalSlot = currentNormalSlot; // ✅ Stay at slot_0
      
      console.log(`[MONITORING SAMIR] [CASE-4-NO-FORWARD] Will read: [${slotsToRead.join(', ')}]`);
    }
    
  } 
  // ============================================================
  // CASE 5: SECOND TIME AT SLOT_0
  // ============================================================
  else if (slot0Visits === 1) {
    console.log(`[MONITORING SAMIR] [CASE-5] SECOND TIME at slot_0 - HIGH engagement only`);
    
    // ✅ Check forward first
    const forwardSlot = `${collection.slice(0, -1)}_${latestIndex + 1}`;
    const forwardCheckStart = Date.now();
    const forwardExists = await this.db.collection(collection).findOne(
      { _id: forwardSlot },
      { projection: { _id: 1 } }
    );
    documentsChecked.push({ slot: forwardSlot, exists: !!forwardExists, duration: Date.now() - forwardCheckStart });
    console.log(`[MONITORING SAMIR] [CASE-5-FORWARD-CHECK] ${forwardSlot} | Exists: ${!!forwardExists} | Duration: ${Date.now() - forwardCheckStart}ms`);
    
    if (forwardExists) {
      // ✅ Read [slot_0, forward, latest]
      slotsToRead = [
        currentNormalSlot, // slot_0
        forwardSlot,
        currentLatestSlot
      ].filter((v, i, a) => a.indexOf(v) === i).slice(0, 3);
      
      newLatestSlot = forwardSlot;
      newNormalSlot = currentNormalSlot; // ✅ Stay at slot_0
      
      console.log(`[MONITORING SAMIR] [CASE-5-FORWARD-FOUND] Will read: [${slotsToRead.join(', ')}]`);
    } else {
      // ✅ Read [slot_0, latest, latest-1]
      slotsToRead = [
        currentNormalSlot, // slot_0
        currentLatestSlot,
        `${collection.slice(0, -1)}_${Math.max(latestIndex - 1, 0)}`
      ].filter((v, i, a) => a.indexOf(v) === i).slice(0, 3);
      
      newLatestSlot = currentLatestSlot; // ✅ Unchanged
      newNormalSlot = currentNormalSlot; // ✅ Stay at slot_0
      
      console.log(`[MONITORING SAMIR] [CASE-5-NO-FORWARD] Will read: [${slotsToRead.join(', ')}]`);
    }
    
    console.log(`[MONITORING SAMIR] [CASE-5-STRATEGY] Use HIGH ENGAGEMENT (70%+ retention OR 50+ likes)`);
    
  } 
  // ============================================================
  // CASE 6: THIRD+ TIME AT SLOT_0
  // ============================================================
  else {
    console.log(`[MONITORING SAMIR] [CASE-6] THIRD+ TIME at slot_0 (visit ${slot0Visits + 1}) - load UNSEEN only`);
    
    // ✅ Check forward
    const forwardSlot = `${collection.slice(0, -1)}_${latestIndex + 1}`;
    const forwardCheckStart = Date.now();
    const forwardExists = await this.db.collection(collection).findOne(
      { _id: forwardSlot },
      { projection: { _id: 1 } }
    );
    documentsChecked.push({ slot: forwardSlot, exists: !!forwardExists, duration: Date.now() - forwardCheckStart });
    console.log(`[MONITORING SAMIR] [CASE-6-FORWARD-CHECK] ${forwardSlot} | Exists: ${!!forwardExists} | Duration: ${Date.now() - forwardCheckStart}ms`);
    
    if (forwardExists) {
      // ✅ Read [forward, latest, latest-1]
      slotsToRead = [
        forwardSlot,
        currentLatestSlot,
        `${collection.slice(0, -1)}_${Math.max(latestIndex - 1, 0)}`
      ].filter((v, i, a) => a.indexOf(v) === i).slice(0, 3);
      
      newLatestSlot = forwardSlot;
      newNormalSlot = `${collection.slice(0, -1)}_${Math.max(latestIndex - 1, 0)}`; // ✅ Move backward from latest
    } else {
      // ✅ Read [latest, latest-1, latest-2]
      slotsToRead = [
        currentLatestSlot,
        `${collection.slice(0, -1)}_${Math.max(latestIndex - 1, 0)}`,
        `${collection.slice(0, -1)}_${Math.max(latestIndex - 2, 0)}`
      ].filter((v, i, a) => a.indexOf(v) === i).slice(0, 3);
      
      newLatestSlot = currentLatestSlot; // ✅ Unchanged
      newNormalSlot = slotsToRead[slotsToRead.length - 1]; // ✅ Last slot read
    }
    
    console.log(`[MONITORING SAMIR] [CASE-6-PLAN] Will read: [${slotsToRead.join(', ')}]`);
    console.log(`[MONITORING SAMIR] [CASE-6-STRATEGY] Load UNSEEN CONTENT ONLY (no engagement filter)`);
  }
  
  
  
  
  
  
  
  
} else {
  // ============================================================
  // NORMAL BACKWARD TRAVERSAL (Case 3)
  // ============================================================
  console.log(`[MONITORING SAMIR] [CASE-3] NORMAL-BACKWARD: normalSlot at index ${normalIndex}`);
  
  // Before moving backward, recheck forward one more time
  const forwardRecheck = `${collection.slice(0, -1)}_${latestIndex + 1}`;
  console.log(`[MONITORING SAMIR] [FORWARD-RECHECK] Re-checking ${forwardRecheck} before backward movement`);
  
  const recheckStart = Date.now();
  const recheckExists = await this.db.collection(collection).findOne(
    { _id: forwardRecheck },
    { projection: { _id: 1 } }
  );
  documentsChecked.push({ slot: forwardRecheck, exists: !!recheckExists, duration: Date.now() - recheckStart });
  console.log(`[MONITORING SAMIR] [CHECKED-DOCUMENT] ${forwardRecheck} | Exists: ${!!recheckExists} | Duration: ${Date.now() - recheckStart}ms`);
  
  if (recheckExists) {
    console.log(`[MONITORING SAMIR] [FORWARD-FOUND-ON-RECHECK] ${forwardRecheck} exists! Prioritizing forward`);
    
    slotsToRead = [
      forwardRecheck,
      currentNormalSlot,
      `${collection.slice(0, -1)}_${Math.max(normalIndex - 1, 0)}`
    ];
    
    // ✅ CRITICAL: Remove duplicates PROPERLY
    slotsToRead = [...new Set(slotsToRead)];
    
    newLatestSlot = forwardRecheck;
    newNormalSlot = slotsToRead[slotsToRead.length - 1];
    
    console.log(`[MONITORING SAMIR] [CASE-3-FORWARD-PLAN] Will read: [${slotsToRead.join(', ')}]`);
  } else {
  // ✅ BACKWARD SEQUENCE - FIXED to prevent skipping slots
  const backwardSlots = [];
  
  // ✅ CRITICAL FIX: Build COMPLETE backward sequence from normalIndex
  // Example: normalIndex=2 → read [reel_2, reel_1, reel_0]
  let currentIndex = normalIndex;
  
  while (backwardSlots.length < 3 && currentIndex >= 0) {
    const slotId = `${collection.slice(0, -1)}_${currentIndex}`;
    
    // ✅ Check if slot actually exists before adding
    const slotExistsCheckStart = Date.now();
    const slotExists = await this.db.collection(collection).findOne(
      { _id: slotId },
      { projection: { _id: 1 } }
    );
    const slotExistsDuration = Date.now() - slotExistsCheckStart;
    
    documentsChecked.push({ 
      slot: slotId, 
      exists: !!slotExists, 
      duration: slotExistsDuration,
      reason: 'backward-sequence-check'
    });
    
    console.log(`[MONITORING SAMIR] [BACKWARD-CHECK] ${slotId} | Exists: ${!!slotExists} | Duration: ${slotExistsDuration}ms`);
    
    if (slotExists) {
      backwardSlots.push(slotId);
    }
    
    currentIndex--; // ✅ Move backward by 1
  }
  
  slotsToRead = backwardSlots;
  newLatestSlot = currentLatestSlot; // Unchanged
  newNormalSlot = backwardSlots.length > 0 ? backwardSlots[backwardSlots.length - 1] : currentNormalSlot;
  
  console.log(`[MONITORING SAMIR] [CASE-3-BACKWARD-PLAN] normalIndex=${normalIndex} | Building backward sequence`);
  console.log(`[MONITORING SAMIR] [CASE-3-BACKWARD-SEQUENCE] Found ${backwardSlots.length} existing slots: [${backwardSlots.join(', ')}]`);
  console.log(`[MONITORING SAMIR] [CASE-3-BACKWARD-PLAN] Will save: latestSlot=${newLatestSlot} (unchanged), normalSlot=${newNormalSlot}`);
  }
  
}
    }
  }

  // ============================================================
  // PHASE 2: Get user interests
  // ============================================================
  let userInterests = [];
  try {
    const interestFetchStart = Date.now();
    const userResponse = await axios.get(`https://server1-ki1x.onrender.com/api/users/${userId}`, { timeout: 1000 });
    if (userResponse.status === 200 && userResponse.data.success) {
      userInterests = userResponse.data.user.interests || [];
    }
    console.log(`[MONITORING SAMIR] [USER-INTERESTS] Fetched in ${Date.now() - interestFetchStart}ms | Interests: [${userInterests.join(', ')}] (${userInterests.length} total)`);
  } catch (e) {
    console.warn(`[MONITORING SAMIR] [USER-INTERESTS-FAILED] ${e.message} | Continuing without interests`);
  }

  // ============================================================
  // PHASE 3: FETCH CONTENT FROM DETERMINED SLOTS (MAX 3)
  // ============================================================
  console.log(`\n[MONITORING SAMIR] [PHASE-3] CONTENT FETCHING - Reading ${slotsToRead.length} slots`);
  
  const slotContents = [];
  for (const slotId of slotsToRead) {
    const slotReadStart = Date.now();
    const slotDoc = await this.db.collection(collection).findOne(
      { _id: slotId },
      { projection: { [listKey]: 1, index: 1, count: 1 } }
    );
    const slotReadDuration = Date.now() - slotReadStart;
    
    documentsChecked.push({ 
      slot: slotId, 
      exists: !!slotDoc, 
      duration: slotReadDuration, 
      hasContent: slotDoc && slotDoc[listKey] && slotDoc[listKey].length > 0 
    });

    if (slotDoc && slotDoc[listKey]) {
      slotContents.push({
        slotId: slotId,
        content: slotDoc[listKey]
      });
      console.log(`[MONITORING SAMIR] [READ-DOCUMENT] ${slotId} | Items: ${slotDoc[listKey].length} (count=${slotDoc.count}) | Duration: ${slotReadDuration}ms | Status: SUCCESS`);
    } else {
      console.log(`[MONITORING SAMIR] [READ-DOCUMENT] ${slotId} | Items: 0 | Duration: ${slotReadDuration}ms | Status: EMPTY/NOT-FOUND`);
    }
  }

  console.log(`[MONITORING SAMIR] [DOCUMENTS-READ-SUMMARY] Total documents checked: ${documentsChecked.length}`);
  documentsChecked.forEach((doc, idx) => {
    console.log(`[MONITORING SAMIR] [DOC-${idx + 1}] ${doc.slot} | Exists: ${doc.exists} | HasContent: ${doc.hasContent || false} | Duration: ${doc.duration}ms`);
  });

  // ============================================================
  // PHASE 4: FILTER VIEWED CONTENT (CONTRIB CHECK - MAX 3)
  // ============================================================
  console.log(`\n[MONITORING SAMIR] [PHASE-4] FILTERING VIEWED CONTENT - Checking contrib_${collection}`);
  
  const viewedIds = new Set();
  const contribDocsChecked = [];
  
  for (const { slotId } of slotContents) {
    const contribReadStart = Date.now();
    const contribDoc = await this.db.collection(`contrib_${collection}`).findOne(
      { 
        userId: userId,
        slotId: slotId
      },
      { projection: { ids: 1 } }
    );
    const contribReadDuration = Date.now() - contribReadStart;
    
    contribDocsChecked.push({ slotId, found: !!contribDoc, count: contribDoc?.ids?.length || 0, duration: contribReadDuration });

    if (contribDoc && contribDoc.ids) {
      contribDoc.ids.forEach(id => viewedIds.add(id));
      console.log(`[MONITORING SAMIR] [READ-CONTRIB] ${slotId} | Viewed IDs: ${contribDoc.ids.length} | Duration: ${contribReadDuration}ms | Status: FOUND`);
    } else {
      console.log(`[MONITORING SAMIR] [READ-CONTRIB] ${slotId} | Viewed IDs: 0 | Duration: ${contribReadDuration}ms | Status: NOT-FOUND`);
    }
  }

  console.log(`[MONITORING SAMIR] [CONTRIB-READ-SUMMARY] Total contrib docs checked: ${contribDocsChecked.length} | Total viewed IDs: ${viewedIds.size}`);

  // ============================================================
  // PHASE 5: INTEREST-BASED FILTERING
  // ============================================================
console.log(`\n[MONITORING SAMIR] [PHASE-5] INTEREST FILTERING - Matching with user interests`);

let interestedContent = [];
let totalItemsBeforeFilter = 0;
let interestMatchCount = 0;
let noInterestIncludeCount = 0;

for (const { slotId, content } of slotContents) {
  totalItemsBeforeFilter += content.length;
  
  for (const item of content) {
    if (viewedIds.has(item.postId)) continue; // ✅ Skip viewed first
    
    // ✅ CRITICAL FIX: Check if category field exists and matches
    const hasCategory = item.category && typeof item.category === 'string' && item.category.trim() !== '';
    const categoryMatches = hasCategory && userInterests.length > 0 && userInterests.includes(item.category);
    
    if (categoryMatches) {
      // ✅ PRIORITY 1: Category matches user interest
      interestedContent.push(item);
      interestMatchCount++;
      console.log(`[MONITORING SAMIR] [INTEREST-MATCH] ${item.postId.substring(0, 8)} | category="${item.category}" matches interests`);
    } else if (userInterests.length === 0) {
      // ✅ PRIORITY 2: No interests set, include all
      interestedContent.push(item);
      noInterestIncludeCount++;
    }
    // ✅ Items with non-matching categories are EXCLUDED (will be filled by engagement later)
  }
}

console.log(`[MONITORING SAMIR] [INTEREST-FILTER-RESULT] Total items: ${totalItemsBeforeFilter} | After viewed filter: ${totalItemsBeforeFilter - viewedIds.size} | Interest matches: ${interestMatchCount} | No-interest includes: ${noInterestIncludeCount} | Final: ${interestedContent.length}`);

if (interestMatchCount === 0 && userInterests.length > 0) {
  console.warn(`[MONITORING SAMIR] [NO-INTEREST-MATCHES] No content matched user interests [${userInterests.join(', ')}] - will use engagement fill`);
}

  // ============================================================
  // PHASE 6: ENGAGEMENT-BASED FILL (if insufficient)
  // ============================================================
  if (interestedContent.length < minContentRequired) {
    const slot0Visits = userStatus?.[visitField] || 0;
    const normalMatch = userStatus?.[normalField]?.match(/_(\d+)$/);
    const normalIndex = normalMatch ? parseInt(normalMatch[1]) : -1;
    const isAtSlot0 = normalIndex === 0;
    
    let engagementStrategy = 'HIGH';
    
    if (isAtSlot0) {
      if (slot0Visits === 0) {
        engagementStrategy = 'MEDIUM';
        console.log(`\n[MONITORING SAMIR] [PHASE-6] ENGAGEMENT FILL - FIRST TIME AT SLOT_0 - Using MEDIUM engagement strategy`);
      } else if (slot0Visits === 1) {
        engagementStrategy = 'HIGH';
        console.log(`\n[MONITORING SAMIR] [PHASE-6] ENGAGEMENT FILL - SECOND TIME AT SLOT_0 - Using HIGH engagement strategy`);
      } else {
        engagementStrategy = 'UNSEEN';
        console.log(`\n[MONITORING SAMIR] [PHASE-6] ENGAGEMENT FILL - THIRD+ TIME AT SLOT_0 - Using UNSEEN CONTENT strategy`);
      }
    } else {
      console.log(`\n[MONITORING SAMIR] [PHASE-6] ENGAGEMENT FILL - NORMAL POSITION - Using HIGH engagement strategy`);
    }
    
    console.log(`[MONITORING SAMIR] [ENGAGEMENT-STRATEGY] ${engagementStrategy} | Need ${minContentRequired - interestedContent.length} more items`);
    
    const existingIds = new Set(interestedContent.map(item => item.postId));
    const remainderContent = [];

    for (const { content } of slotContents) {
      for (const item of content) {
        if (!viewedIds.has(item.postId) && !existingIds.has(item.postId)) {
          let shouldInclude = false;
          
          if (engagementStrategy === 'HIGH') {
            shouldInclude = (item.retention || 0) > 70 || (item.likeCount || 0) > 50;
          } else if (engagementStrategy === 'MEDIUM') {
            shouldInclude = (item.retention || 0) > 40 || (item.likeCount || 0) > 20;
          } else if (engagementStrategy === 'UNSEEN') {
            shouldInclude = true;
          }
          
          if (shouldInclude) {
            remainderContent.push(item);
          }
        }
      }
    }

    remainderContent.sort((a, b) => {
      const retentionDiff = (b.retention || 0) - (a.retention || 0);
      if (Math.abs(retentionDiff) > 1) return retentionDiff;
      const likesDiff = (b.likeCount || 0) - (a.likeCount || 0);
      if (likesDiff !== 0) return likesDiff;
      const commentsDiff = (b.commentCount || 0) - (a.commentCount || 0);
      if (commentsDiff !== 0) return commentsDiff;
      return (b.viewCount || 0) - (a.viewCount || 0);
    });

    const needed = minContentRequired - interestedContent.length;
    const fillContent = remainderContent.slice(0, needed);
    
    console.log(`[MONITORING SAMIR] [ENGAGEMENT-FILL-RESULT] Strategy: ${engagementStrategy} | Available: ${remainderContent.length} | Taking: ${fillContent.length}`);
    
    fillContent.forEach((item, idx) => {
      if (idx < 3) {
        console.log(`[MONITORING SAMIR] [FILL-ITEM-${idx + 1}] postId=${item.postId.substring(0, 8)} | retention=${item.retention}% | likes=${item.likeCount} | Strategy: ${engagementStrategy}`);
      }
    });
    
    interestedContent.push(...fillContent);
  } else {
    console.log(`\n[MONITORING SAMIR] [PHASE-6] SKIP - Already have ${interestedContent.length} items (>= ${minContentRequired} required)`);
  }


  console.log(`\n[MONITORING SAMIR] [PHASE-7] FINAL RANKING - Sorting by engagement`);
  
// ============================================================
// PHASE 7: FINAL RANKING (UNCHANGED)
// ============================================================
interestedContent.sort((a, b) => {
  const retentionDiff = (b.retention || 0) - (a.retention || 0);
  if (Math.abs(retentionDiff) > 1) return retentionDiff;
  const likesDiff = (b.likeCount || 0) - (a.likeCount || 0);
  if (likesDiff !== 0) return likesDiff;
  const commentsDiff = (b.commentCount || 0) - (a.commentCount || 0);
  if (commentsDiff !== 0) return commentsDiff;
  return (b.viewCount || 0) - (a.viewCount || 0);
});

// ❌ REMOVE THIS ENTIRE BLOCK - NO UPDATE DURING FEED LOAD
// if (newLatestSlot && newNormalSlot) {
//   console.log(`\n[MONITORING SAMIR] [PHASE-8] UPDATING USER_STATUS...`);
//   await this.updateUserStatus(userId, updateData);
//   ...
// }

// ✅ NEW: Only log what WOULD be saved (for app exit to use)
console.log(`\n[MONITORING SAMIR] [PHASE-8] FEED-LOAD-COMPLETE - NO UPDATE`);
console.log(`[MONITORING SAMIR] [COMPUTED-NEXT-STATE] latestSlot=${newLatestSlot || 'UNCHANGED'}, normalSlot=${newNormalSlot || 'UNCHANGED'}`);
console.log(`[MONITORING SAMIR] [NOTE] user_status will be updated on APP EXIT, not during feed load`);

const duration = Date.now() - start;
const totalReads = 1 + documentsChecked.length + contribDocsChecked.length;

console.log(`\n${'='.repeat(80)}`);
console.log(`[MONITORING SAMIR] [FEED-COMPLETE]`);
console.log(`  User: ${userId} | Type: ${contentType} | User Type: ${isNewUser ? 'FRESH' : 'RETURNING'}`);
console.log(`  Content Returned: ${interestedContent.length} items`);
console.log(`  Total Reads: ${totalReads} = 1 user_status + ${documentsChecked.length} slot checks + ${contribDocsChecked.length} contrib checks`);
console.log(`  Slots Read: [${slotContents.map(s => s.slotId).join(', ')}]`);
console.log(`  Next State (for exit): latest=${newLatestSlot || 'UNCHANGED'}, normal=${newNormalSlot || 'UNCHANGED'}`);
console.log(`  Total Duration: ${duration}ms`);
console.log(`${'='.repeat(80)}\n`);

return {
  content: interestedContent.slice(0, minContentRequired * 2),
  latestDocumentId: newLatestSlot,  // ✅ Return for Android to save on exit
  normalDocumentId: newNormalSlot,  // ✅ Return for Android to save on exit
  isNewUser,
  hasNewContent: interestedContent.length > 0,
  metadata: {
    slotsChecked: documentsChecked.map(d => d.slot),
    slotsWithContent: slotContents.map(s => s.slotId),
    slotsRead: slotContents.map(s => s.slotId),
    contribDocsChecked: contribDocsChecked.map(c => c.slotId),
    interestFiltered: interestedContent.length,
    totalReads: totalReads,
    userInterests,
    duration,
    // ✅ NEW: Return computed next state for app exit
    computedNextState: {
      latestSlot: newLatestSlot,
      normalSlot: newNormalSlot,
      visitIncrement: (newNormalSlot === `${collection.slice(0, -1)}_0`) ? 1 : 0
    }
  }
};
if (newLatestSlot && newNormalSlot) {
  console.log(`\n[MONITORING SAMIR] [PHASE-8] UPDATING USER_STATUS - Saving new slot IDs`);
  const updateStart = Date.now();
  
  // ✅ Track slot_0 visits
  const updateData = {
    [statusField]: newLatestSlot,
    [normalField]: newNormalSlot
  };
  
  // ✅ CRITICAL: Increment visit counter when at slot_0
  if (newNormalSlot === `${collection.slice(0, -1)}_0`) {
    const currentVisits = userStatus?.[visitField] || 0;
    updateData[visitField] = currentVisits + 1;
    console.log(`[MONITORING SAMIR] [SLOT-0-VISIT-INCREMENT] Visit count: ${currentVisits} → ${currentVisits + 1}`);
  }
  
  await this.updateUserStatus(userId, updateData);
  
  // ✅ CRITICAL: Clear cache immediately after update
  const feedCacheKey = `feed_${contentType}_${userId}`;
  if (redisClient) {
    await redisClient.del(feedCacheKey).catch(() => {});
  }
  if (cache[feedCacheKey]) {
    cache[feedCacheKey].clear();
  }
  
  console.log(`[MONITORING SAMIR] [USER-STATUS-UPDATE] Saved in ${Date.now() - updateStart}ms | latest=${newLatestSlot}, normal=${newNormalSlot}`);
  console.log(`[MONITORING SAMIR] [CACHE-CLEARED] Feed cache invalidated for next request`);
} else {
  console.log(`\n[MONITORING SAMIR] [PHASE-8] SKIP USER_STATUS UPDATE - No changes needed`);
}

  const duration = Date.now() - start;
  const totalReads = 1 + documentsChecked.length + contribDocsChecked.length;

  console.log(`\n${'='.repeat(80)}`);
  console.log(`[MONITORING SAMIR] [FEED-COMPLETE]`);
  console.log(`  User: ${userId} | Type: ${contentType} | User Type: ${isNewUser ? 'FRESH' : 'RETURNING'}`);
  console.log(`  Content Returned: ${interestedContent.length} items`);
  console.log(`  Total Reads: ${totalReads} = 1 user_status + ${documentsChecked.length} slot checks + ${contribDocsChecked.length} contrib checks`);
  console.log(`  Documents Checked: ${documentsChecked.map(d => d.slot).join(', ')}`);
  console.log(`  Documents with Content: ${slotContents.map(s => s.slotId).join(', ')}`);
  console.log(`  Contrib Docs Checked: ${contribDocsChecked.map(c => c.slotId).join(', ')});   console.log(  Status Saved: latest=newLatestSlot,normal={newLatestSlot}, normal=
newLatestSlot,normal={newNormalSlot}
);   console.log(  Total Duration: ${duration}ms);   console.log(${'='.repeat(80)}\n`);
return {
  content: interestedContent.slice(0, minContentRequired * 2),
  latestDocumentId: newLatestSlot,
  normalDocumentId: newNormalSlot,
  isNewUser,
  hasNewContent: interestedContent.length > 0,
  metadata: {
    slotsChecked: documentsChecked.map(d => d.slot),
    slotsWithContent: slotContents.map(s => s.slotId),
    slotsRead: slotContents.map(s => s.slotId), // ✅ ADD THIS
    contribDocsChecked: contribDocsChecked.map(c => c.slotId),
    interestFiltered: interestedContent.length,
    totalReads: totalReads,
    userInterests,
    duration
  }
};
}





async getUserStatus(userId) {
  // ❌ REMOVE CACHE CHECK
  // const cacheKey = `user_status_${userId}`;
  // const cached = await getCache(cacheKey);
  // if (cached) {
  //   console.log(`[CACHE-HIT] user_status for ${userId} | Source: Redis/Memory`);
  //   return cached;
  // }

  const readsBefore = dbOpCounters.reads;
  const start = Date.now();

  const statusDoc = await this.db.collection('user_status').findOne({ _id: userId });

  const readsUsed = dbOpCounters.reads - readsBefore;
  const time = Date.now() - start;

  logDbOp('findOne', 'user_status', { _id: userId }, statusDoc, time, {
    docsExamined: statusDoc ? 1 : 0
  });

  console.log(`[USER-STATUS-QUERY] userId=${userId} | DB Reads: ${readsUsed} | Cache: DISABLED | Time: ${time}ms`);

  // ❌ REMOVE CACHE SET
  // if (statusDoc) {
  //   await setCache(cacheKey, statusDoc, CACHE_TTL_MEDIUM);
  // }
  
  return statusDoc || null;
}

async updateUserStatus(userId, updates) {
  const start = Date.now();
  
  console.log(`[MONITORING SAMIR] [UPDATE-USER-STATUS-START] userId=${userId} | updates=${JSON.stringify(updates)}`);
  
  const result = await this.db.collection('user_status').updateOne(
    { _id: userId },
    { 
      $set: { 
        ...updates, 
        updatedAt: new Date().toISOString() 
      } 
    },
    { upsert: true }
  );
  
  logDbOp('updateOne', 'user_status', { _id: userId }, result, Date.now() - start);

  console.log(`[MONITORING SAMIR] [UPDATE-USER-STATUS-COMPLETE] matched=${result.matchedCount} | modified=${result.modifiedCount} | upserted=${result.upsertedCount}`);

  // ❌ REMOVE THESE LINES - NO CACHING
  // const cacheKey = `user_status_${userId}`;
  // const updatedDoc = { _id: userId, ...updates, updatedAt: new Date().toISOString() };
  // await setCache(cacheKey, updatedDoc, CACHE_TTL_MEDIUM);
  
  console.log(`[MONITORING SAMIR] [NO-CACHE] user_status updated without caching`);
}

async getLatestSlotOptimized(collection) {
  const cacheKey = `latest_${collection}`;
  const cached = await getCache(cacheKey);
  if (cached) {
    return cached;
  }

  const start = Date.now();
  const latestSlot = await this.db.collection(collection).findOne(
    {},
    {
      sort: { index: -1 },
      projection: { _id: 1, index: 1, count: 1 }
    }
  );
  logDbOp('findOne', collection, { sort: { index: -1 } }, latestSlot, Date.now() - start);

  if (latestSlot) {
    await setCache(cacheKey, latestSlot, CACHE_TTL_MEDIUM);
  }
  return latestSlot;
}

async getAdditionalContent(userId, collection, contentType, needed, existingResult) {
try {
console.log(`[ADDITIONAL-CONTENT-START] ${userId} needs ${needed} more items | Current Reads: ${dbOpCounters.reads}`);

const listKey = contentType === 'reels' ? 'reelsList' : 'postList';
const existingIds = new Set(existingResult.content.map(item => item.postId));

// Single query for contributed views
const start1 = Date.now();
const contributedDoc = await this.db.collection(`contrib_${collection}`).findOne(
{ userId },
{ projection: { ids: 1 } }
);
logDbOp('findOne', `contrib_${collection}`, { userId }, contributedDoc, Date.now() - start1);
const viewedIds = new Set(contributedDoc?.ids || []);

// Limit to only 2 slots to minimize reads
const start2 = Date.now();
const slots = await this.db.collection(collection)
.find({}, { projection: { _id: 1, [listKey]: 1, index: 1 } })
.sort({ index: -1 })
.limit(2) // Reduced from 5 to 2
.toArray();
logDbOp('find', collection, { sort: { index: -1 }, limit: 2 }, slots, Date.now() - start2);

const additionalContent = [];

for (const slot of slots) {
if (additionalContent.length >= needed) break;

const items = slot[listKey] || [];
for (const item of items) {
if (additionalContent.length >= needed) break;

if (item.postId &&
!existingIds.has(item.postId) &&
!viewedIds.has(item.postId)) {
additionalContent.push(item);
}
}
}

console.log(`[ADDITIONAL-CONTENT-RESULT] Found ${additionalContent.length} items | Total Reads: ${dbOpCounters.reads}`);
return additionalContent;

} catch (error) {
console.error('[ADDITIONAL-CONTENT-ERROR]', error);
return [];
}
}

async getOptimizedFeedForNewUser(userId, collection, contentType, statusField, normalField, minContentRequired) {

const latestSlot = await this.getLatestSlotOptimized(collection);
if (!latestSlot) {
return { content: [], latestDocumentId: null, normalDocumentId: null, isNewUser: true };
}

const content = [];
const slotsToFetch = [latestSlot._id];

const additionalSlots = await this.db.collection(collection)
.find({ index: { $lt: latestSlot.index } }, { projection: { _id: 1, index: 1, count: 1 } })
.sort({ index: -1 })
.limit(3)
.toArray();

additionalSlots.forEach(slot => slotsToFetch.push(slot._id));

await this.fetchContentFromSlots(slotsToFetch, collection, contentType, content);

const qualityContent = content.filter(item => {
return item.postId &&
item.imageUrl &&
item.username &&
(item.likeCount || 0) >= 0;
});

const normalDocId = slotsToFetch.length > 1 ? slotsToFetch[1] : latestSlot._id;
await this.updateUserStatus(userId, {
[statusField]: latestSlot._id,
[normalField]: normalDocId
});

return {
content: qualityContent.slice(0, Math.max(minContentRequired, MIN_CONTENT_FOR_FEED)),
latestDocumentId: latestSlot._id,
normalDocumentId: normalDocId,
isNewUser: true
};
}

async getOptimizedFeedForReturningUser(userId, collection, contentType, userStatus, statusField, normalField, minContentRequired) {

const currentLatest = userStatus[statusField];
const currentNormal = userStatus[normalField];

if (!currentLatest) {
return await this.getOptimizedFeedForNewUser(userId, collection, contentType, statusField, normalField, minContentRequired);
}

// Extract current index
const match = currentLatest.match(/_(\d+)$/);
if (!match) {
return await this.getOptimizedFeedForNewUser(userId, collection, contentType, statusField, normalField, minContentRequired);
}
const currentIndex = parseInt(match[1]);

// Check cache first for recent requests
const cacheKey = `user_feed_${userId}_${contentType}_${currentIndex}`;
const cachedResult = getCache(cacheKey);
if (cachedResult) {
console.log(`[FEED-CACHE-HIT] ${userId} - ${contentType} | Reads: ${dbOpCounters.reads}`);
return cachedResult;
}

// SINGLE AGGREGATION QUERY instead of multiple separate queries
const listKey = contentType === 'reels' ? 'reelsList' : 'postList';
const nextSlotIds = [`${collection.slice(0, -1)}_${currentIndex + 1}`, `${collection.slice(0, -1)}_${currentIndex + 2}`];

const start = Date.now();
const pipeline = [
{
$match: {
$or: [
{ _id: { $in: nextSlotIds } }, // Check next 2 slots
{ _id: currentLatest }, // Current slot
{ _id: currentNormal } // Normal slot
]
}
},
{
$lookup: {
from: `contrib_${collection}`,
let: { slotContent: `$${listKey}` },
pipeline: [
{ $match: { userId: userId } },
{ $project: { ids: 1 } }
],
as: 'viewedData'
}
},
{
$project: {
_id: 1,
index: 1,
count: 1,
[listKey]: 1,
viewedIds: { $ifNull: [{ $arrayElemAt: ['$viewedData.ids', 0] }, []] }
}
},
{ $sort: { index: -1 } }
];

const results = await this.db.collection(collection).aggregate(pipeline).toArray();
logDbOp('aggregate', collection, { pipeline: 'optimized_feed_query' }, results, Date.now() - start);

const viewedIds = new Set(results.length > 0 && results[0].viewedIds ? results[0].viewedIds : []);
let content = [];
let newLatestSlot = currentLatest;

// Process results in order of preference: newest slots first
const sortedResults = results.sort((a, b) => (b.index || 0) - (a.index || 0));

for (const slot of sortedResults) {
const slotContent = slot[listKey] || [];
const isNewSlot = nextSlotIds.includes(slot._id);

// If this is a new slot with content, use it
if (isNewSlot && slotContent.length > 0) {
const freshContent = slotContent.filter(item =>
item.postId && !viewedIds.has(item.postId)
);

if (freshContent.length > 0) {
content = freshContent;
newLatestSlot = slot._id;

// Update user status in single operation
await this.updateUserStatus(userId, {
[statusField]: newLatestSlot,
[normalField]: currentNormal
});

const result = {
content: content.slice(0, Math.max(minContentRequired, content.length)),
latestDocumentId: newLatestSlot,
normalDocumentId: currentNormal,
isNewUser: false,
hasNewContent: true
};

// Cache the result for 15 seconds to prevent duplicate processing
setCache(cacheKey, result, 15000);

console.log(`[NEW-CONTENT-FOUND] ${userId} - ${contentType}: ${content.length} items | Total Reads: ${dbOpCounters.reads}`);
return result;
}
}
}

// If no new content, return filtered existing content
const existingSlots = sortedResults.filter(slot =>
slot._id === currentLatest || slot._id === currentNormal
);

for (const slot of existingSlots) {
const slotContent = slot[listKey] || [];
const filteredContent = slotContent.filter(item =>
item.postId && !viewedIds.has(item.postId)
);
content.push(...filteredContent);
}

const finalResult = {
content: content.slice(0, Math.max(minContentRequired, MIN_CONTENT_FOR_FEED)),
latestDocumentId: currentLatest,
normalDocumentId: currentNormal,
isNewUser: false,
hasNewContent: false
};

// Cache negative results too to prevent repeated queries
setCache(cacheKey, finalResult, 10000);

console.log(`[FILTERED-RESULT] ${userId} - ${contentType}: ${finalResult.content.length} items | Total Reads: ${dbOpCounters.reads}`);
return finalResult;
}

async fetchContentFromSlots(slots, collection, contentType, content) {
if (slots.length === 0) return;

const start = Date.now();
const listKey = contentType === 'reels' ? 'reelsList' : 'postList';
const slotDocs = await this.db.collection(collection).find({ _id: { $in: slots } }, { projection: { [listKey]: 1, _id: 1 } }).toArray();
logDbOp('find', collection, { _id: { $in: slots } }, slotDocs, Date.now() - start);

const slotMap = new Map();
slotDocs.forEach(doc => slotMap.set(doc._id, doc[listKey] || []));
slots.forEach(slotId => content.push(...(slotMap.get(slotId) || [])));
}

async getFilteredContentForReturningUser(userId, collection, contentType, latestSlotId, normalSlotId, minContentRequired) {
console.log(`[FILTERED-CONTENT] ${userId} - ${contentType}, minimum: ${minContentRequired} | Current Reads: ${dbOpCounters.reads}`);

// Single query to get contributed views
const start1 = Date.now();
const contributedDoc = await this.db.collection(`contrib_${collection}`)
.findOne({ userId }, { projection: { ids: 1 } });
logDbOp('findOne', `contrib_${collection}`, { userId }, contributedDoc, Date.now() - start1);

const viewedIds = new Set(contributedDoc?.ids || []);

const currentIndex = parseInt(latestSlotId.match(/_(\d+)$/)?.[1] || '0');

// Limit to only 2 slots for returning users to reduce reads
const slotIds = [latestSlotId];
if (normalSlotId && normalSlotId !== latestSlotId) {
slotIds.push(normalSlotId);
}

// Single query for content
const content = [];
const start2 = Date.now();
const slotDocs = await this.db.collection(collection)
.find({ _id: { $in: slotIds } }, {
projection: {
[contentType === 'reels' ? 'reelsList' : 'postList']: 1,
_id: 1
}
})
.toArray();
logDbOp('find', collection, { _id: { $in: slotIds } }, slotDocs, Date.now() - start2);

const listKey = contentType === 'reels' ? 'reelsList' : 'postList';
slotDocs.forEach(doc => {
const items = doc[listKey] || [];
content.push(...items);
});

const filteredContent = content.filter((item, index) => {
return item.postId &&
!viewedIds.has(item.postId) &&
item.imageUrl &&
item.username &&
index < minContentRequired * 2;
}).slice(0, Math.max(minContentRequired, 6));

console.log(`[FILTERED-RESULT] ${userId} - ${contentType}: ${filteredContent.length} items | Total Reads: ${dbOpCounters.reads}`);

return {
content: filteredContent,
latestDocumentId: latestSlotId,
normalDocumentId: normalSlotId,
isNewUser: false,
hasNewContent: false
};
}

async batchPutContributedViewsOptimized(userId, posts = [], reels = []) {
  console.log(`[MONITORING SAMIR] [BATCH-CONTRIB-START] userId=${userId} | ${posts.length} posts + ${reels.length} reels`);
  
  const results = [];
  const operations = [
    posts.length > 0 && { type: 'posts', collection: 'contrib_posts', ids: posts },
    reels.length > 0 && { type: 'reels', collection: 'contrib_reels', ids: reels }
  ].filter(Boolean);

  const reelSlotIds = new Set();
  const postSlotIds = new Set();

  for (const op of operations) {
    const start = Date.now();
    
    // Extract slotIds from items
    for (const item of op.ids) {
      const postId = typeof item === 'string' ? item : item.postId;
      const slotId = typeof item === 'object' ? item.slotId : null;
      
      if (slotId) {
        if (op.type === 'reels') {
          reelSlotIds.add(slotId);
        } else {
          postSlotIds.add(slotId);
        }
      }
    }

    // ✅ CRITICAL: Use ordered: true to ensure sequential execution
    const result = await this.db.collection(op.collection).bulkWrite([{
      updateOne: {
        filter: { userId, slotId: { $in: Array.from(op.type === 'reels' ? reelSlotIds : postSlotIds) } },
        update: {
          $addToSet: { ids: { $each: op.ids.map(i => typeof i === 'string' ? i : i.postId) } },
          $setOnInsert: { userId, createdAt: new Date().toISOString() },
          $set: { updatedAt: new Date().toISOString() }
        },
        upsert: true
      }
    }], { ordered: true }); // ✅ CHANGED: Force sequential execution
    
    const duration = Date.now() - start;
    logDbOp('bulkWrite', op.collection, { userId }, result, duration);
    console.log(`[MONITORING SAMIR] [BATCH-CONTRIB-SAVED] ${op.type} | ${op.ids.length} items | Duration: ${duration}ms`);
    results.push({ type: op.type, result });
  }

  // ✅ CRITICAL: IMMEDIATE user_status UPDATE (not async, wait for completion)
  if (reelSlotIds.size > 0 || postSlotIds.size > 0) {
    console.log(`[MONITORING SAMIR] [FORCE-USER-STATUS-UPDATE] Updating user_status IMMEDIATELY`);
    
    const updateData = {};
    
    if (reelSlotIds.size > 0) {
      const reelSlotArray = Array.from(reelSlotIds).sort((a, b) => {
        const numA = parseInt(a.split('_')[1]) || 0;
        const numB = parseInt(b.split('_')[1]) || 0;
        return numB - numA;
      });
      
      updateData.latestReelSlotId = reelSlotArray[0];
      updateData.normalReelSlotId = reelSlotArray[reelSlotArray.length - 1];
      
      console.log(`[MONITORING SAMIR] [USER-STATUS-REELS-UPDATE] latest=${updateData.latestReelSlotId}, normal=${updateData.normalReelSlotId}`);
    }
    
    if (postSlotIds.size > 0) {
      const postSlotArray = Array.from(postSlotIds).sort((a, b) => {
        const numA = parseInt(a.split('_')[1]) || 0;
        const numB = parseInt(b.split('_')[1]) || 0;
        return numB - numA;
      });
      
      updateData.latestPostSlotId = postSlotArray[0];
      updateData.normalPostSlotId = postSlotArray[postSlotArray.length - 1];
      
      console.log(`[MONITORING SAMIR] [USER-STATUS-POSTS-UPDATE] latest=${updateData.latestPostSlotId}, normal=${updateData.normalPostSlotId}`);
    }
    
    // ✅ CRITICAL: WAIT for update to complete (don't use Promise.all or async)
    await this.updateUserStatus(userId, updateData);
    
    console.log(`[MONITORING SAMIR] [FORCE-SYNC-COMPLETE] user_status updated IMMEDIATELY`);
  }

  return results;
}

async getDocument(col, id) {
const start = Date.now();
const result = await this.db.collection(col).findOne({ _id: id });
logDbOp('findOne', col, { _id: id }, result, Date.now() - start);
return result;
}

async saveDocument(col, id, data) {
const start = Date.now();
const result = await this.db.collection(col).updateOne({ _id: id }, { $set: data }, { upsert: true });
logDbOp('updateOne', col, { _id: id }, result, Date.now() - start);
}

async getMaxIndexCached(collection) {
const cacheKey = `max_index_${collection}`;
const lockKey = `${cacheKey}_lock`;

// Try cache first
const cached = await getCache(cacheKey);
if (cached !== null) {
console.log(`[CACHE-HIT] ${cacheKey} = ${cached}`);
return cached;
}

// CRITICAL FIX: Implement cache stampede prevention
if (redisClient) {
// Try to acquire lock
const lockAcquired = await redisClient.set(lockKey, '1', 'EX', 5, 'NX');

if (!lockAcquired) {
// Another process is fetching, wait and retry cache
console.log(`[CACHE-STAMPEDE-PREVENT] ${cacheKey} | Waiting for other process...`);
await new Promise(resolve => setTimeout(resolve, 100));

const retryCache = await getCache(cacheKey);
if (retryCache !== null) {
console.log(`[CACHE-HIT-RETRY] ${cacheKey} = ${retryCache}`);
return retryCache;
}
}
}

// Fetch from database
const start = Date.now();
const maxDoc = await this.db.collection(collection)
.find({}, { projection: { index: 1 } })
.sort({ index: -1 })
.limit(1)
.maxTimeMS(3000) // Prevent hanging queries
.next();

logDbOp('find', collection, { sort: { index: -1 }, limit: 1 }, maxDoc, Date.now() - start);

const maxIndex = maxDoc?.index || 0;

// Set cache with longer TTL for stable data
await setCache(cacheKey, maxIndex, 30000); // 30 seconds

// Release lock
if (redisClient) {
await redisClient.del(lockKey).catch(() => {});
}

console.log(`[MAX-INDEX-COMPUTED] ${collection} = ${maxIndex} | DB Reads: ${dbOpCounters.reads}`);
return maxIndex;
}

async allocateSlot(col, postData, maxAttempts = 3) {
const coll = this.db.collection(col);
const listKey = col === 'reels' ? 'reelsList' : 'postList';
const postId = postData.postId;

console.log(`[SLOT-ALLOCATION-START] ${col} | PostId: ${postId} | MAX_PER_SLOT: ${MAX_CONTENT_PER_SLOT}`);

// ✅ CRITICAL: Check if post already exists FIRST
const existingCheck = await coll.findOne(
{ [`${listKey}.postId`]: postId },
{ projection: { _id: 1, count: 1 } }
);

if (existingCheck) {
console.log(`[DUPLICATE-PREVENTED] ${postId} already exists in ${existingCheck._id} (count: ${existingCheck.count})`);
return existingCheck;
}

for (let attempt = 1; attempt <= maxAttempts; attempt++) {
try {
console.log(`[SLOT-ATTEMPT-${attempt}/${maxAttempts}] ${col} | PostId: ${postId}`);

// ✅ CHANGED: From count < 2 to count < MAX_CONTENT_PER_SLOT
const availableSlot = await coll.findOne(
{ count: { $lt: MAX_CONTENT_PER_SLOT } },
{
sort: { index: -1 },
projection: { _id: 1, index: 1, count: 1 }
}
);

if (availableSlot) {
console.log(`[SLOT-FOUND] ${availableSlot._id} (current count: ${availableSlot.count}/${MAX_CONTENT_PER_SLOT})`);

// ✅ CHANGED: Condition updated to use MAX_CONTENT_PER_SLOT
const updateResult = await coll.updateOne(
{
_id: availableSlot._id,
count: { $lt: MAX_CONTENT_PER_SLOT },
[`${listKey}.postId`]: { $ne: postId }
},
{
$push: { [listKey]: postData },
$inc: { count: 1 }
}
);

if (updateResult.matchedCount > 0 && updateResult.modifiedCount > 0) {
const newCount = availableSlot.count + 1;
console.log(`[SLOT-SUCCESS] ${postId} → ${availableSlot._id} | Count: ${availableSlot.count} → ${newCount}/${MAX_CONTENT_PER_SLOT}`);

await this.invalidateSlotCache(col);
if (redisClient) {
await redisClient.del(`max_index_${col}`).catch(() => {});
}

return {
_id: availableSlot._id,
index: availableSlot.index,
count: newCount
};
} else {
console.log(`[SLOT-TAKEN] ${availableSlot._id} filled by another process, retrying...`);
await new Promise(resolve => setTimeout(resolve, 50 * attempt));
continue;
}
}

// ✅ STEP 2: No available slot - create new one
console.log(`[NO-SLOTS-AVAILABLE] Creating new slot for ${postId} (all existing slots have ${MAX_CONTENT_PER_SLOT} items)`);

const maxDoc = await coll.findOne({}, {
sort: { index: -1 },
projection: { index: 1 }
});

const currentMaxIndex = maxDoc?.index ?? -1;
const nextIndex = currentMaxIndex + 1;
const newId = `${col.slice(0, -1)}_${nextIndex}`;

console.log(`[CREATE-NEW-SLOT-${attempt}] ${newId} (index: ${nextIndex})`);

try {
const newDoc = {
_id: newId,
index: nextIndex,
count: 1,
[listKey]: [postData],
createdAt: new Date().toISOString()
};

await coll.insertOne(newDoc);

console.log(`[SLOT-CREATED-SUCCESS] ${newId} | PostId: ${postId} | Capacity: 1/${MAX_CONTENT_PER_SLOT}`);

await setCache(`max_index_${col}`, nextIndex, 5000);
await this.invalidateSlotCache(col);

return newDoc;

} catch (insertErr) {
if (insertErr.code === 11000) {
console.log(`[DUPLICATE-KEY-${attempt}] ${newId} exists, retrying...`);

if (redisClient) {
await redisClient.del(`max_index_${col}`).catch(() => {});
}

await new Promise(resolve => setTimeout(resolve, 100 * attempt));
continue;
}

throw insertErr;
}

} catch (error) {
console.error(`[SLOT-ERROR-${attempt}] ${col} | ${error.message}`);

if (attempt === maxAttempts) {
throw new Error(`Slot allocation failed after ${maxAttempts} attempts: ${error.message}`);
}

await new Promise(resolve => setTimeout(resolve, 150 * attempt));
}
}

throw new Error(`Could not allocate slot for ${postId} after ${maxAttempts} attempts`);
}

// NEW: Helper to invalidate slot cache
async invalidateSlotCache(col) {
const cacheKey = `latest_${col}`;
if (redisClient) {
await redisClient.del(cacheKey).catch(() => {});
}
if (cache[cacheKey]) {
cache[cacheKey].clear();
}
console.log(`[CACHE-INVALIDATED] ${cacheKey}`);
}

async saveToUserPosts(userId, postData) {
const start = Date.now();
const result = await this.db.collection('user_posts').updateOne(
{ userId, postId: postData.postId },
{ $set: { ...postData, userId, postId: postData.postId, createdAt: new Date().toISOString() } },
{ upsert: true }
);
logDbOp('updateOne', 'user_posts', { userId, postId: postData.postId }, result, Date.now() - start);
await this.atomicIncrement(`user_post_count:${userId}`, 1);
}

async atomicIncrement(key, by = 1) {
const start = Date.now();
const res = await this.db.collection('counters').findOneAndUpdate({ _id: key }, { $inc: { value: by } }, { upsert: true, returnDocument: 'after' });
logDbOp('findOneAndUpdate', 'counters', { _id: key }, res, Date.now() - start);
return res.value?.value || null;
}

async getContributedViewsForUserType(userId, type) {
const start = Date.now();
const results = await this.db.collection(`contrib_${type}`).find({ userId }).toArray();
logDbOp('find', `contrib_${type}`, { userId }, results, Date.now() - start);

const out = {};
results.forEach(v => { if (v?.ids) out[v.session || ''] = v.ids; });
return out;
}

async getLatestDocId(col) {
const latestSlot = await this.getLatestSlotOptimized(col);
return latestSlot ? latestSlot._id : null;
}

async getContributedViewsStats(userId) {
const [postsStats, reelsStats] = await Promise.all([
this.getContribStatsForType(userId, 'contrib_posts'),
this.getContribStatsForType(userId, 'contrib_reels')
]);

return {
posts: postsStats[0] || { totalSessions: 0, totalViews: 0 },
reels: reelsStats[0] || { totalSessions: 0, totalViews: 0 }
};
}

async getContribStatsForType(userId, collection) {
const start = Date.now();
const aggregation = [
{ $match: { userId } },
{ $group: { _id: null, totalSessions: { $sum: 1 }, totalViews: { $sum: { $size: '$ids' } } } }
];
const stats = await this.db.collection(collection).aggregate(aggregation).toArray();
logDbOp('aggregate', collection, aggregation, stats, Date.now() - start);
return stats;
}

async getCollectionCount(collection) {
const start = Date.now();
const count = await this.db.collection(collection).countDocuments();
logDbOp('countDocuments', collection, {}, { count }, Date.now() - start);
return count;
}

async getDistinctUserIds() {
const start = Date.now();
const userIds = await this.db.collection('user_posts').distinct('userId');
logDbOp('distinct', 'user_posts', { field: 'userId' }, { count: userIds.length }, Date.now() - start);
return userIds;
}
}




let redisClient;

async function initRedis() {
  if (!process.env.REDIS_URL) {
    log('warn', '[REDIS] ⚠️ Not configured - using in-memory cache');
    return;
  }

  redisClient = new Redis(process.env.REDIS_URL, {
    maxRetriesPerRequest: 3,
    enableReadyCheck: true,
    connectTimeout: 10000,
    
    // ✅ PRODUCTION OPTIMIZATIONS
    lazyConnect: false,          // Connect immediately
    keepAlive: 30000,            // TCP keepalive
    
    // ✅ NEW: Connection pool (critical for performance)
    maxRetriesPerRequest: 3,
    enableOfflineQueue: true,    // Queue commands during reconnect
    
    retryStrategy(times) {
      if (times > 5) {
        log('error', '[REDIS] Max retries reached, falling back to in-memory');
        return null;
      }
      const delay = Math.min(times * 100, 3000);  // Max 3s backoff
      return delay;
    },
    
    // ✅ NEW: Reconnect strategy
    reconnectOnError(err) {
      const targetErrors = ['READONLY', 'ETIMEDOUT', 'ECONNRESET'];
      if (targetErrors.some(target => err.message.includes(target))) {
        return true;  // Reconnect
      }
      return false;   // Don't reconnect for other errors
    }
  });

  redisClient.on('error', (err) => {
    log('error', '[REDIS-ERROR]', err.message);
  });

  redisClient.on('connect', () => {
    log('info', '[REDIS] ✅ Connected successfully');
  });

  redisClient.on('ready', () => {
    log('info', '[REDIS] ✅ Ready to accept commands');
  });

  redisClient.on('close', () => {
    log('warn', '[REDIS] Connection closed');
  });

  redisClient.on('reconnecting', () => {
    log('info', '[REDIS] 🔄 Reconnecting...');
  });

  try {
    await redisClient.ping();
    console.log('[REDIS-INIT] ✅ Health check passed');
  } catch (err) {
    log('warn', `[REDIS] Health check failed: ${err.message}`);
    redisClient = null;
  }
}

// ✅ CACHE STAMPEDE PROTECTION
const getOrSetCache = async (key, fetchFunction, ttl = 30000) => {
  if (!redisClient) {
    return await fetchFunction();  // Fallback to direct fetch
  }

  const lockKey = `lock:${key}`;
  const lockTTL = 5;  // 5 seconds
  const maxWaitTime = 3000;  // 3 seconds max wait

  try {
    // Try to get from cache first
    const cached = await redisClient.get(key);
    if (cached) {
      console.log(`[CACHE-HIT] ${key}`);
      return JSON.parse(cached);
    }

    // ✅ STAMPEDE PROTECTION: Try to acquire lock
    const lockAcquired = await redisClient.set(
      lockKey, 
      '1', 
      'EX', 
      lockTTL, 
      'NX'
    );

    if (!lockAcquired) {
      // Another process is fetching, wait and retry
      console.log(`[CACHE-STAMPEDE-WAIT] ${key} - waiting for other process`);
      
      const startWait = Date.now();
      while (Date.now() - startWait < maxWaitTime) {
        await new Promise(resolve => setTimeout(resolve, 100));
        
        const retryCache = await redisClient.get(key);
        if (retryCache) {
          console.log(`[CACHE-HIT-AFTER-WAIT] ${key}`);
          return JSON.parse(retryCache);
        }
      }
      
      // Timeout reached, fetch anyway
      console.warn(`[CACHE-STAMPEDE-TIMEOUT] ${key} - fetching anyway`);
    }

    // Fetch data
    console.log(`[CACHE-MISS] ${key} - fetching`);
    const data = await fetchFunction();

    // Store in cache
    await redisClient.setex(key, Math.floor(ttl / 1000), JSON.stringify(data));
    
    // Release lock
    await redisClient.del(lockKey);

    return data;

  } catch (err) {
    log('error', '[CACHE-ERROR]', err.message);
    // Always release lock on error
    await redisClient.del(lockKey).catch(() => {});
    return await fetchFunction();  // Fallback
  }
};





async function start() {
console.log('[SERVER-START] Initializing...');
await initMongo();
await initializeSlots(); // ← ADD THIS LINE
await ensurePostIdUniqueness();
await initRedis();
dbManager = new DatabaseManager(db);
console.log('[SERVER-START] Ready');
}

start().catch(err => { console.error('[SERVER-START-ERROR]', err); process.exit(1); });

// --- ROUTES ---

// GET /api/debug/db-check
// Diagnostic endpoint to check database connection and Reels collection
app.get('/api/debug/db-check', async (req, res) => {
try {
const database = req.app.locals.db || global.db || db;

if (!database) {
return res.json({
success: false,
error: 'Database not connected'
});
}

// Get Reels collection stats
const reelDocs = await database.collection('Reels').find({}).limit(5).toArray();
const firstDoc = reelDocs[0];

const stats = {
success: true,
databaseConnected: true,
reelsDocumentsFound: reelDocs.length,
sampleDocument: firstDoc ? {
_id: firstDoc._id,
hasReelsList: !!firstDoc.reelsList,
reelsCount: firstDoc.reelsList ? firstDoc.reelsList.length : 0,
sampleReelIds: firstDoc.reelsList ?
firstDoc.reelsList.slice(0, 3).map(r => r.postId) : []
} : null
};

res.json(stats);
} catch (error) {
res.status(500).json({
success: false,
error: error.message,
stack: error.stack
});
}
});



app.get('/api/posts/single-reel/:postId', async (req, res) => {
const startTime = Date.now();
const { postId } = req.params;
const { userId } = req.query;

console.log('='.repeat(80));
console.log(`[SINGLE-REEL-REQUEST] postId=${postId} | length=${postId.length}`);
console.log('='.repeat(80));

try {
if (!postId || postId.trim() === '') {
return res.status(400).json({
success: false,
error: 'postId is required'
});
}

// ✅ CRITICAL FIX: Use aggregation instead of loading all docs into memory
// This is much more efficient and matches your other endpoints

const pipeline = [
{ $match: { 'reelsList': { $exists: true, $ne: [] } } },
{ $unwind: '$reelsList' },
{ $match: { 'reelsList.postId': postId } }, // ✅ Exact match on full UUID
{ $limit: 1 },
{
$project: {
postId: '$reelsList.postId',
userId: '$reelsList.userId',
username: '$reelsList.username',
caption: '$reelsList.caption',
description: '$reelsList.description',
category: '$reelsList.category',
hashtag: '$reelsList.hashtag',
imageUrl: {
$cond: {
if: { $ifNull: ['$reelsList.videoUrl', false] },
then: '$reelsList.videoUrl',
else: '$reelsList.imageUrl'
}
},
profilePicUrl: { $ifNull: ['$reelsList.profile_picture_url', '$reelsList.profilePicUrl', ''] },
likeCount: { $ifNull: ['$reelsList.likeCount', 0] },
commentCount: { $ifNull: ['$reelsList.commentCount', 0] },
viewCount: { $ifNull: ['$reelsList.viewCount', '$reelsList.viewcount', 0] },
retention: { $ifNull: ['$reelsList.retention', 0] },
timestamp: { $ifNull: ['$reelsList.timestamp', '$reelsList.serverTimestamp'] },
sourceDocument: '$_id',
multiple_posts: { $ifNull: ['$reelsList.multiple_posts', false] },
media_count: { $ifNull: ['$reelsList.media_count', 1] }
}
}
];

// ✅ FIXED: Use lowercase 'reels' collection name
const results = await db.collection('reels').aggregate(pipeline).toArray();

if (results.length === 0) {
const duration = Date.now() - startTime;
console.log(`[SINGLE-REEL-NOT-FOUND] ❌ postId=${postId} not found in reels collection`);

return res.status(404).json({
success: false,
error: 'Reel not found',
searchedFor: postId,
duration: duration
});
}

const reelData = results[0];

// Format response
const formattedReel = {
postId: reelData.postId,
userId: reelData.userId,
username: reelData.username || 'Unknown User',
caption: reelData.caption || '',
description: reelData.description || '',
category: reelData.category || '',
hashtag: reelData.hashtag || '',
imageUrl: reelData.imageUrl,
profilePicUrl: reelData.profilePicUrl,
likeCount: reelData.likeCount,
commentCount: reelData.commentCount,
viewCount: reelData.viewCount,
retention: reelData.retention,
timestamp: reelData.timestamp || Date.now(),
sourceDocument: reelData.sourceDocument,
isReel: true,
ratio: '9:16',
multiple_posts: reelData.multiple_posts,
media_count: reelData.media_count
};

const duration = Date.now() - startTime;

console.log(`[SINGLE-REEL-SUCCESS] ✅ postId=${formattedReel.postId.substring(0, 8)} | username=${formattedReel.username} | duration=${duration}ms`);

res.json({
success: true,
reel: formattedReel,
duration: duration
});

} catch (error) {
console.error('[SINGLE-REEL-ERROR] ❌ CRITICAL ERROR', error);
console.error('Stack trace:', error.stack);

res.status(500).json({
success: false,
error: 'Server error fetching reel',
message: error.message,
stack: process.env.NODE_ENV === 'development' ? error.stack : undefined
});
}
});


app.get('/health', (req, res) => res.json({ status: 'OK', ts: new Date().toISOString() }));

// ADDED: Comprehensive production health check
app.get('/health/detailed', async (req, res) => {
const health = {
status: 'healthy',
timestamp: new Date().toISOString(),
uptime: process.uptime(),
services: {}
};

// Check MongoDB
try {
await db.admin().ping();
health.services.mongodb = {
status: 'connected',
sharded: true,
compression: client.options?.compressors?.join(',') || 'none'
};
} catch (err) {
health.services.mongodb = { status: 'disconnected', error: err.message };
health.status = 'unhealthy';
}

// Check Redis
if (redisClient) {
try {
await redisClient.ping();
health.services.redis = { status: 'connected' };
} catch (err) {
health.services.redis = { status: 'disconnected', error: err.message };
health.status = 'degraded';
}
} else {
health.services.redis = { status: 'not_configured' };
health.status = 'degraded';
}

// System metrics
health.system = {
memory: {
used: `${Math.round(process.memoryUsage().heapUsed / 1024 / 1024)}MB`,
total: `${Math.round(process.memoryUsage().heapTotal / 1024 / 1024)}MB`
},
cpu: process.cpuUsage(),
pid: process.pid
};

// Database operations
health.operations = {
totalReads: dbOpCounters.reads,
totalWrites: dbOpCounters.writes,
totalQueries: dbOpCounters.queries,
totalAggregations: dbOpCounters.aggregations
};

res.json(health);
});

app.get('/api/db-stats', (req, res) => {
const uptime = process.uptime();
const stats = {
...dbOpCounters,
uptime,
operationsPerSecond: {
reads: (dbOpCounters.reads / uptime).toFixed(2),
writes: (dbOpCounters.writes / uptime).toFixed(2),
total: ((dbOpCounters.reads + dbOpCounters.writes) / uptime).toFixed(2)
},
timestamp: new Date().toISOString(),
cacheStats: { latestSlots: cache.latestSlots.size, userStatus: cache.userStatus.size, maxIndexes: cache.maxIndexes.size }
};
res.json(stats);
});


// Add after existing /health endpoint
app.get('/api/server-stats', (req, res) => {
const uptime = process.uptime();
const stats = {
server: {
uptime: `${Math.floor(uptime / 3600)}h ${Math.floor((uptime % 3600) / 60)}m`,
totalRequests: requestMetrics.totalRequests,
activeConnections: requestMetrics.activeConnections,
avgResponseTime: `${requestMetrics.avgResponseTime}ms`,
requestsPerSecond: (requestMetrics.totalRequests / uptime).toFixed(2)
},
database: {
totalReads: dbOpCounters.reads,
totalWrites: dbOpCounters.writes,
readsPerSecond: (dbOpCounters.reads / uptime).toFixed(2),
writesPerSecond: (dbOpCounters.writes / uptime).toFixed(2),
queries: dbOpCounters.queries,
aggregations: dbOpCounters.aggregations
},
topEndpoints: Object.entries(requestMetrics.endpointCounts)
.sort((a, b) => b[1] - a[1])
.slice(0, 10)
.map(([endpoint, count]) => ({ endpoint, count })),
timestamp: new Date().toISOString()
};

console.log(`[STATS-REQUEST] Total Traffic: ${stats.server.totalRequests} requests | DB Load: ${stats.database.totalReads} reads, ${stats.database.totalWrites} writes`);

res.json(stats);
});


// Ultra-fast retention contribution check - O(1) lookup
// Replace existing /api/retention/check/:userId/:postId endpoint
app.get('/api/retention/check/:userId/:postId', async (req, res) => {
try {
const { userId, postId } = req.params;

if (!userId || !postId) {
return res.status(400).json({ error: 'userId and postId required' });
}

const retentionCacheKey = `retention_${userId}_${postId}`;

console.log(`[RETENTION-CHECK] User: ${userId} | Post: ${postId} | Total Reads: ${dbOpCounters.reads}`);

// Check memory cache first
const cached = getCache(retentionCacheKey);
if (cached !== null) {
console.log(`[RETENTION-CACHE-HIT] Saved 1 DB read | Total Reads: ${dbOpCounters.reads}`);
return res.json({
success: true,
hasContributed: cached,
source: 'cache',
queryTime: 0
});
}

const today = new Date().toISOString().split('T')[0];
const dbReadsBefore = dbOpCounters.reads;
const start = Date.now();

const result = await db.collection('user_interaction_cache').findOne(
{
_id: `${userId}_session_${today}`,
retentionContributed: postId
},
{ projection: { _id: 1 } }
);

const queryTime = Date.now() - start;
const dbReadsUsed = dbOpCounters.reads - dbReadsBefore;
const hasContributed = !!result;

// Cache for 2 hours
setCache(retentionCacheKey, hasContributed, 7200000);

console.log(`[RETENTION-DB-CHECK] Result: ${hasContributed} | Used ${dbReadsUsed} DB reads | Time: ${queryTime}ms | Total Reads: ${dbOpCounters.reads}`);

return res.json({
success: true,
hasContributed,
source: 'database',
queryTime
});

} catch (error) {
console.error(`[RETENTION-CHECK-ERROR] ${error.message} | Total Reads: ${dbOpCounters.reads}`);
return res.status(500).json({ error: 'Failed to check retention contribution' });
}
});



// ✅ NEW: Update user_status ONLY on app exit
app.post('/api/user-status/exit-update/:userId', async (req, res) => {
  const startTime = Date.now();
  const { userId } = req.params;
  const { latestReelSlotId, normalReelSlotId, latestPostSlotId, normalPostSlotId, reel_0_visits, post_0_visits } = req.body;

  try {
    console.log(`[APP-EXIT-UPDATE] userId=${userId} | Updating user_status on app exit`);

    const updateData = { userId, updatedAt: new Date().toISOString() };

    if (latestReelSlotId) updateData.latestReelSlotId = latestReelSlotId;
    if (normalReelSlotId) updateData.normalReelSlotId = normalReelSlotId;
    if (latestPostSlotId) updateData.latestPostSlotId = latestPostSlotId;
    if (normalPostSlotId) updateData.normalPostSlotId = normalPostSlotId;
    if (typeof reel_0_visits === 'number') updateData.reel_0_visits = reel_0_visits;
    if (typeof post_0_visits === 'number') updateData.post_0_visits = post_0_visits;

    const result = await db.collection('user_status').updateOne(
      { _id: userId },
      { 
        $set: updateData,
        $setOnInsert: { createdAt: new Date().toISOString() }
      },
      { upsert: true }
    );

    const duration = Date.now() - startTime;

    console.log(`[APP-EXIT-UPDATE-SUCCESS] matched=${result.matchedCount} | modified=${result.modifiedCount} | upserted=${result.upsertedCount} | duration=${duration}ms`);

    return res.json({
      success: true,
      message: 'user_status updated on app exit',
      updated: updateData,
      duration
    });

  } catch (error) {
    console.error(`[APP-EXIT-UPDATE-ERROR] ${error.message}`);
    return res.status(500).json({
      success: false,
      error: 'Failed to update user_status on exit: ' + error.message,
      duration: Date.now() - startTime
    });
  }
});




// ✅ REPLACE - Forward to PORT 4000
app.post('/api/interactions/restore-like-states', async (req, res) => {
try {
const { userId, postIds } = req.body;

if (!userId || !Array.isArray(postIds) || postIds.length === 0) {
return res.status(400).json({ error: 'userId and postIds array required' });
}

console.log(`[RESTORE-LIKE-STATES-PROXY] User: ${userId} | Checking ${postIds.length} posts`);

// Forward to PORT 4000
const port4000Response = await axios.post(
'https://database-22io.onrender.com/api/posts/batch-check-liked',
{ userId, postIds },
{ timeout: 10000 }
);

if (port4000Response.data.success) {
const likes = port4000Response.data.likes;

// Convert format
const likeStates = {};
for (const [postId, likeInfo] of Object.entries(likes)) {
likeStates[postId] = likeInfo.isLiked || false;
}

return res.json({
success: true,
likeStates: likeStates,
optimization: {
postsChecked: postIds.length,
likedCount: Object.values(likeStates).filter(Boolean).length,
source: 'PORT_4000_post_likes'
}
});
}

return res.status(500).json({ success: false, error: 'PORT 4000 failed' });

} catch (error) {
console.error('[RESTORE-LIKE-STATES-ERROR]', error.message);
return res.status(500).json({ success: false, error: 'Failed to restore like states' });
}
});


// Sync metrics from PORT 4000 to PORT 2000
app.post('/api/sync/metrics', async (req, res) => {
try {
const { postId, metrics, isReel, sourceDocument, userId } = req.body;

if (!postId || !metrics) {
return res.status(400).json({ error: 'postId and metrics required' });
}

console.log(`[SYNC-METRICS] Updating ${postId} - isReel: ${isReel}`);

const collection = isReel ? 'reels' : 'posts';
const arrayField = isReel ? 'reelsList' : 'postList';

// Check if already synced recently (within last 5 seconds to prevent duplicate syncs)
const recentSync = await db.collection('sync_log').findOne({
postId: postId,
syncedAt: { $gte: new Date(Date.now() - 5000) }
});

if (recentSync) {
console.log(`[SYNC-SKIP] ${postId} already synced recently`);
return res.json({
success: true,
message: 'Already synced recently',
postId,
skipped: true
});
}

// Update in the main collection documents
const updateResult = await db.collection(collection).updateOne(
{ [`${arrayField}.postId`]: postId },
{
$set: {
[`${arrayField}.$.likeCount`]: metrics.likeCount || 0,
[`${arrayField}.$.commentCount`]: metrics.commentCount || 0,
[`${arrayField}.$.viewCount`]: metrics.viewCount || 0,
[`${arrayField}.$.retention`]: metrics.retention || 0,
[`${arrayField}.$.lastSynced`]: new Date().toISOString()
}
}
);

if (updateResult.matchedCount === 0) {
console.warn(`[SYNC-NOT-FOUND] ${postId} not found in ${collection} collection`);

// Try to find in user_slots if not in main collection
const userSlotUpdate = await db.collection('user_slots').updateOne(
{ [`${arrayField}.postId`]: postId },
{
$set: {
[`${arrayField}.$.likeCount`]: metrics.likeCount || 0,
[`${arrayField}.$.commentCount`]: metrics.commentCount || 0,
[`${arrayField}.$.viewCount`]: metrics.viewCount || 0,
[`${arrayField}.$.retention`]: metrics.retention || 0,
[`${arrayField}.$.lastSynced`]: new Date().toISOString()
}
}
);

if (userSlotUpdate.matchedCount === 0) {
return res.status(404).json({ error: 'Post not found' });
}

console.log(`[SYNC-SUCCESS-USERSLOT] Updated ${postId} in user_slots`);
} else {
console.log(`[SYNC-SUCCESS] Updated ${postId} in ${collection}`);
}

// Log successful sync to prevent duplicates
await db.collection('sync_log').insertOne({
postId: postId,
syncedAt: new Date(),
metrics: metrics
});

// Clean up old sync logs (older than 1 minute)
await db.collection('sync_log').deleteMany({
syncedAt: { $lt: new Date(Date.now() - 60000) }
});

res.json({
success: true,
message: 'Metrics synced successfully',
postId,
metrics
});
} catch (error) {
console.error('[SYNC-ERROR]', error);
res.status(500).json({ error: 'Failed to sync metrics' });
}
});


// Personalized reels feed with interest-based ranking
app.post('/api/feed/reels-personalized', async (req, res) => {
try {
const { userId, limit = DEFAULT_CONTENT_BATCH_SIZE, offset = 0 } = req.body;

if (!userId || userId === 'undefined' || userId === 'null') {
return res.status(400).json({ success: false, error: 'Valid userId required' });
}

const limitNum = parseInt(limit, 10) || DEFAULT_CONTENT_BATCH_SIZE;
const offsetNum = parseInt(offset, 10) || 0;

log('info', `[REELS-PERSONALIZED-START] userId=${userId}, limit=${limitNum}, offset=${offsetNum}`);

// Step 1: Get user interests
let userInterests = [];
try {
const userResponse = await axios.get(`https://server1-ki1x.onrender.com/api/users/${userId}`, {
timeout: 2000
});
if (userResponse.status === 200 && userResponse.data.success) {
userInterests = userResponse.data.user.interests || [];
log('info', `[USER-INTERESTS] ${userId}: [${userInterests.join(', ')}]`);
}
} catch (e) {
log('warn', `[USER-INTERESTS-SKIP] ${e.message} - proceeding without interests`);
}

// Step 2: Get viewed reels
const viewedReelsDoc = await db.collection('contrib_reels').findOne(
{ userId },
{ projection: { ids: 1 } }
);
const viewedReelIds = viewedReelsDoc?.ids || [];
log('info', `[VIEWED-FILTER] Excluding ${viewedReelIds.length} viewed reels`);

// **CRITICAL: First pass to get max values for normalization**
const maxValuesQuery = [
{ $match: { 'reelsList': { $exists: true, $ne: [] } } },
{ $unwind: '$reelsList' },
{ $match: { 'reelsList.postId': { $nin: viewedReelIds } }},
{
$group: {
_id: null,
maxLikes: { $max: { $toInt: { $ifNull: ['$reelsList.likeCount', 0] } } },
maxComments: { $max: { $toInt: { $ifNull: ['$reelsList.commentCount', 0] } } },
maxViews: { $max: { $toInt: { $ifNull: ['$reelsList.viewCount', 0] } } }
}
}
];

const maxValues = await db.collection('reels').aggregate(maxValuesQuery).toArray();
const maxLikes = maxValues[0]?.maxLikes || 1;
const maxComments = maxValues[0]?.maxComments || 1;
const maxViews = maxValues[0]?.maxViews || 1;

log('info', `[NORMALIZATION-VALUES] maxLikes=${maxLikes}, maxComments=${maxComments}, maxViews=${maxViews}`);

// Step 3: Build Instagram-style weighted scoring pipeline
const pipeline = [
{ $match: { 'reelsList': { $exists: true, $ne: [] } } },
{ $unwind: '$reelsList' },
{
$match: {
'reelsList.postId': { $nin: viewedReelIds }
}
},
{
$addFields: {
// Base metrics (converted to numbers)
retentionNum: { $toDouble: { $ifNull: ['$reelsList.retention', 0] } },
likeCountNum: { $toInt: { $ifNull: ['$reelsList.likeCount', 0] } },
commentCountNum: { $toInt: { $ifNull: ['$reelsList.commentCount', 0] } },
viewCountNum: { $toInt: { $ifNull: ['$reelsList.viewCount', 0] } },

// Interest match score (0, 50, or 100)
interestScore: {
$cond: {
if: {
$and: [
{ $gt: [{ $size: { $ifNull: [userInterests, []] } }, 0] },
{ $in: ['$reelsList.category', userInterests] }
]
},
then: 100,
else: {
$cond: {
if: { $eq: [{ $size: { $ifNull: [userInterests, []] } }, 0] },
then: 50,
else: 0
}
}
}
}
}
},
{
$addFields: {
// **INSTAGRAM-STYLE NORMALIZATION (0-100 scale)**
normalizedLikes: {
$multiply: [
{ $divide: ['$likeCountNum', maxLikes] },
100
]
},
normalizedComments: {
$multiply: [
{ $divide: ['$commentCountNum', maxComments] },
100
]
},
normalizedViews: {
$multiply: [
{ $divide: ['$viewCountNum', maxViews] },
100
]
}
}
},
{
$addFields: {
// **INSTAGRAM-STYLE WEIGHTED COMPOSITE SCORE**
// Retention: 50%, Likes: 25%, Comments: 12%, Interest: 10%, Views: 3%
compositeScore: {
$add: [
{ $multiply: ['$retentionNum', 0.50] }, // 50% weight
{ $multiply: ['$normalizedLikes', 0.25] }, // 25% weight
{ $multiply: ['$normalizedComments', 0.12] }, // 12% weight
{ $multiply: ['$interestScore', 0.10] }, // 10% weight
{ $multiply: ['$normalizedViews', 0.03] } // 3% weight
]
}
}
},
{
// **CRITICAL: Sort by composite score (Instagram algorithm)**
$sort: {
compositeScore: -1 // Highest score first
}
},
{ $skip: offsetNum },
{ $limit: limitNum * 2 },
{
$project: {
postId: '$reelsList.postId',
userId: '$reelsList.userId',
username: '$reelsList.username',
imageUrl: {
$cond: {
if: { $ifNull: ['$reelsList.videoUrl', false] },
then: '$reelsList.videoUrl',
else: '$reelsList.imageUrl'
}
},
videoUrl: '$reelsList.videoUrl',
caption: '$reelsList.caption',
description: '$reelsList.description',
category: '$reelsList.category',
profilePicUrl: { $ifNull: ['$reelsList.profile_picture_url', ''] },
timestamp: '$reelsList.timestamp',
likeCount: '$likeCountNum',
commentCount: '$commentCountNum',
viewCount: '$viewCountNum',
retention: '$retentionNum',
interestScore: '$interestScore',
compositeScore: '$compositeScore', // Include for debugging
sourceDocument: '$_id',
isReel: { $literal: true }
}
}
];

const startAgg = Date.now();
const reels = await db.collection('reels').aggregate(pipeline).toArray();
const aggTime = Date.now() - startAgg;

log('info', `[AGGREGATION-COMPLETE] ${reels.length} reels in ${aggTime}ms`);

// Step 4: Client-side deduplication + enhanced logging
const seenIds = new Set();
const uniqueReels = [];

for (const reel of reels) {
if (!seenIds.has(reel.postId)) {
seenIds.add(reel.postId);

// **ENHANCED DEBUG: Log Instagram-style ranking**
log('info', `[REEL-RANKED] ${reel.postId.substring(0, 8)} | ` +
`SCORE=${reel.compositeScore.toFixed(2)} | ` +
`retention=${reel.retention.toFixed(1)}% (${(reel.retention * 0.50).toFixed(1)}) | ` +
`likes=${reel.likeCount} (${(reel.likeCount/maxLikes*100*0.25).toFixed(1)}) | ` +
`comments=${reel.commentCount} (${(reel.commentCount/maxComments*100*0.12).toFixed(1)}) | ` +
`interest=${reel.interestScore} (${(reel.interestScore * 0.10).toFixed(1)}) | ` +
`views=${reel.viewCount} (${(reel.viewCount/maxViews*100*0.03).toFixed(1)}) | ` +
`category=${reel.category || 'none'}`);

uniqueReels.push(reel);

if (uniqueReels.length >= limitNum) break;
}
}

log('info', `[REELS-PERSONALIZED-COMPLETE] Returning ${uniqueReels.length}/${reels.length} unique reels`);
log('info', `[ALGORITHM-WEIGHTS] Retention=50%, Likes=25%, Comments=12%, Interest=10%, Views=3%`);

return res.json({
success: true,
content: uniqueReels,
hasMore: reels.length >= limitNum,
metadata: {
totalReturned: uniqueReels.length,
userInterests: userInterests,
viewedReelsFiltered: viewedReelIds.length,
aggregationTimeMs: aggTime,
offset: offsetNum,
normalization: {
maxLikes,
maxComments,
maxViews
},
algorithmWeights: {
retention: 50,
likes: 25,
comments: 12,
interest: 10,
views: 3
}
}
});

} catch (error) {
log('error', '[REELS-PERSONALIZED-ERROR]', error);
return res.status(500).json({
success: false,
error: 'Failed to load personalized reels',
details: error.message
});
}
});


// Calculate ranking score based on Instagram's algorithm
// Replace the existing calculateRankingScore function with this enhanced version
function calculateRankingScore(item) {
// Strict hierarchy: retention > likes > comments > views
const RETENTION_WEIGHT = 1000000; // Highest priority
const LIKE_WEIGHT = 10000; // Second priority
const COMMENT_WEIGHT = 100; // Third priority
const VIEW_WEIGHT = 1; // Lowest priority

const retention = parseFloat(item.retention) || 0;
const likes = parseInt(item.likeCount) || 0;
const comments = parseInt(item.commentCount) || 0;
const views = parseInt(item.viewCount) || 0;

// This ensures retention dominates, then likes, then comments, then views
const score = (
(retention * RETENTION_WEIGHT) +
(likes * LIKE_WEIGHT) +
(comments * COMMENT_WEIGHT) +
(views * VIEW_WEIGHT)
);

log('info', `[RANKING] ${item.postId}: retention=${retention}% → score=${score.toFixed(0)}`);
return score;
}



// ✅ REPLACE WITH THIS SIMPLIFIED VERSION
app.post('/api/interactions/contribution-like', async (req, res) => {
try {
const { userId, postId, action } = req.body;

if (!userId || !postId || !['like', 'unlike'].includes(action)) {
return res.status(400).json({
success: false,
error: 'userId, postId, and action (like/unlike) required'
});
}

const isLiking = action === 'like';
const today = new Date().toISOString().split('T')[0];

console.log(`[CONTRIBUTION-LIKE-PROXY] ${userId} ${action}ing ${postId} - forwarding to PORT 4000`);

// Forward directly to PORT 4000 (MongoDB server)
const port4000Response = await axios.post(
'https://database-22io.onrender.com/api/posts/toggle-like',
{
userId: userId,
postId: postId,
currentlyLiked: !isLiking, // PORT 4000 expects CURRENT state, not action
isReel: true
},
{ timeout: 5000 }
);

if (port4000Response.data.success) {
const isLiked = port4000Response.data.isLiked;
const likeCount = port4000Response.data.likeCount;

console.log(`[CONTRIBUTION-LIKE-SUCCESS] ${postId}: isLiked=${isLiked}, count=${likeCount}`);

return res.json({
success: true,
action: isLiked ? 'like' : 'unlike',
isLiked: isLiked,
likeCount: likeCount,
message: `Successfully ${isLiked ? 'liked' : 'unliked'}`,
duplicate: port4000Response.data.duplicate || false
});
} else {
return res.status(500).json({
success: false,
error: 'PORT 4000 like failed'
});
}

} catch (error) {
console.error('[CONTRIBUTION-LIKE-ERROR]', error.message);
return res.status(500).json({
success: false,
error: 'Failed to process like'
});
}
});




// Sync metrics from PORT 4000 to PORT 2000
app.post('/api/sync/metrics-from-mongodb', async (req, res) => {
try {
const { postId, metrics, isReel } = req.body;

if (!postId || !metrics) {
return res.status(400).json({ error: 'postId and metrics required' });
}

const collection = isReel ? 'reels' : 'posts';
const arrayField = isReel ? 'reelsList' : 'postList';

console.log(`[SYNC-METRICS] Updating ${postId} in ${collection}`);

// Update the post/reel in user_slots
const result = await db.collection('user_slots').updateOne(
{ [`${arrayField}.postId`]: postId },
{
$set: {
[`${arrayField}.$.likeCount`]: metrics.likeCount || 0,
[`${arrayField}.$.commentCount`]: metrics.commentCount || 0,
[`${arrayField}.$.viewCount`]: metrics.viewCount || 0,
[`${arrayField}.$.retention`]: metrics.retention || 0,
[`${arrayField}.$.lastSynced`]: new Date().toISOString()
}
}
);

if (result.matchedCount === 0) {
return res.status(404).json({ error: 'Post not found' });
}

console.log(`[SYNC-SUCCESS] Updated ${postId} metrics`);

res.json({
success: true,
message: 'Metrics synced successfully',
postId
});

} catch (error) {
console.error('[SYNC-ERROR]', error);
res.status(500).json({ error: 'Failed to sync metrics' });
}
});



// ✅ REPLACE WITH PORT 4000 PROXY VERSION
app.post('/api/interactions/check-likes', async (req, res) => {
try {
const { userId, postIds } = req.body;

if (!userId || !Array.isArray(postIds) || postIds.length === 0) {
return res.status(400).json({ error: 'userId and postIds array required' });
}

console.log(`[CHECK-LIKES-PROXY] User: ${userId} | Checking ${postIds.length} posts`);

// Forward to PORT 4000's batch check endpoint
const port4000Response = await axios.post(
'https://database-22io.onrender.com/api/posts/batch-check-liked',
{
userId: userId,
postIds: postIds
},
{ timeout: 10000 }
);

if (port4000Response.data.success) {
const likes = port4000Response.data.likes;

// Transform response format: { postId: { isLiked: true } } → { postId: true }
const simplifiedLikes = {};
for (const [postId, likeInfo] of Object.entries(likes)) {
simplifiedLikes[postId] = likeInfo.isLiked || false;
}

console.log(`[CHECK-LIKES-SUCCESS] Found ${Object.keys(simplifiedLikes).length} like states`);

return res.json({
success: true,
likes: simplifiedLikes,
optimization: {
postsChecked: postIds.length,
source: 'PORT_4000_post_likes'
}
});
} else {
return res.status(500).json({
success: false,
error: 'PORT 4000 check failed'
});
}

} catch (error) {
console.error('[CHECK-LIKES-ERROR]', error.message);
return res.status(500).json({
success: false,
error: 'Failed to check like states'
});
}
});






// Batch retention check for multiple posts
app.post('/api/retention/check-batch', async (req, res) => {
try {
const { userId, postIds } = req.body;

if (!userId || !Array.isArray(postIds) || postIds.length === 0) {
return res.status(400).json({ error: 'userId and postIds array required' });
}

const today = new Date().toISOString().split('T')[0];
const result = {};
const uncachedIds = [];

// Check cache first for all posts
postIds.forEach(postId => {
const cached = getCache(`retention_${userId}_${postId}`);
if (cached !== null) {
result[postId] = cached;
} else {
uncachedIds.push(postId);
}
});

// Query database only for uncached posts
if (uncachedIds.length > 0) {
const cacheDoc = await db.collection('user_interaction_cache').findOne(
{ _id: `${userId}_session_${today}` },
{ projection: { retentionContributed: 1 } }
);

const contributedSet = new Set(cacheDoc?.retentionContributed || []);

uncachedIds.forEach(postId => {
const hasContributed = contributedSet.has(postId);
result[postId] = hasContributed;
setCache(`retention_${userId}_${postId}`, hasContributed, 3600000);
});
}

return res.json({
success: true,
contributions: result,
cacheHits: postIds.length - uncachedIds.length,
dbQueries: uncachedIds.length > 0 ? 1 : 0
});

} catch (error) {
console.error('[BATCH-RETENTION-CHECK-ERROR]', error);
return res.status(500).json({ error: 'Failed to check batch retention' });
}
});


app.get('/api/feed/:contentType/:userId', async (req, res) => {
  try {
    const { contentType, userId } = req.params;
    const { minContent = MIN_CONTENT_FOR_FEED } = req.query;

    if (!['posts', 'reels'].includes(contentType)) {
      return res.status(400).json({ error: 'Invalid content type' });
    }

    const requestKey = `${contentType}_${userId}_${minContent}`;

    console.log(`[FEED-REQUEST] User: ${userId} | Type: ${contentType} | Min: ${minContent}`);

    // Check for duplicate requests
    if (activeRequests.has(requestKey)) {
      console.log(`[DUPLICATE-BLOCKED] ${requestKey}`);
      const result = await activeRequests.get(requestKey);
      return res.json({ ...result, servedFromDuplicatePrevention: true });
    }

    // ❌ REMOVE CACHE CHECK
    // const cacheKey = `feed_${contentType}_${userId}`;
    // const cached = await getCache(cacheKey);
    // if (cached && cached.content && cached.content.length >= parseInt(minContent)) {
    //   console.log(`[CACHE-SERVED] ${requestKey} | Items: ${cached.content.length}`);
    //   return res.json({ ...cached, servedFromCache: true });
    // }

    const requestPromise = (async () => {
      try {
        const dbReadsBefore = dbOpCounters.reads;

        const feedData = await dbManager.getOptimizedFeedFixedReads(
          userId, 
          contentType, 
          parseInt(minContent)
        );
        
        const dbReadsUsed = dbOpCounters.reads - dbReadsBefore;

        console.log(`[FEED-DB-QUERY] ${requestKey} | DB reads: ${dbReadsUsed} | Items: ${feedData.content?.length || 0}`);

        if (dbReadsUsed > 7) {
          console.warn(`⚠️ [READ-LIMIT-EXCEEDED] ${contentType} used ${dbReadsUsed} reads (limit: 7)`);
        }

        // ❌ REMOVE CACHE SET
        // if (feedData.content && feedData.content.length > 0) {
        //   await setCache(cacheKey, feedData, 30000);
        // }

        return {
          success: true,
          ...feedData,
          requestedMinimum: parseInt(minContent),
          actualDelivered: feedData.content ? feedData.content.length : 0,
          dbReadsUsed,
          readLimitCompliant: dbReadsUsed <= 7
        };
      } finally {
        activeRequests.delete(requestKey);
      }
    })();

    activeRequests.set(requestKey, requestPromise);
    const result = await requestPromise;
    return res.json(result);

  } catch (e) {
    console.error(`[FEED-ERROR] ${e.message}`);
    return res.status(500).json({ error: 'Failed to load feed', details: e.message });
  }
});

app.post('/api/contributed-views/batch-optimized', async (req, res) => {
  const requestId = req.body.requestId || req.headers['x-request-id'] || 'unknown';
  const startTime = Date.now();

  try {
    const { userId, posts = [], reels = [] } = req.body;

    if (!userId) {
      return res.status(400).json({
        success: false,
        error: 'userId required',
        requestId
      });
    }

    console.log(`[BATCH-START] userId=${userId} | ${posts.length}P + ${reels.length}R`);

    const postOperations = [];
    const reelOperations = [];
    
    // ✅ Track which slots we need to query
    const reelSlotLookups = [];
    const postSlotLookups = [];

    // ✅ Process reels - get slot for each postId
    for (const item of reels) {
      const postId = typeof item === 'string' ? item : item.postId;
      
      // Check if client provided slotId
      let slotId = typeof item === 'object' ? item.slotId : null;
      
      if (!slotId) {
        console.log(`[BATCH-REEL-LOOKUP] ${postId.substring(0, 8)} - client didn't provide slotId, looking up...`);
        slotId = await getSlotForReel(postId);
        reelSlotLookups.push(postId);
      }
      
      if (!slotId) {
        console.warn(`[BATCH-SKIP-REEL] ${postId} - slot not found in any document`);
        continue;
      }

      console.log(`[BATCH-REEL] ${postId.substring(0, 8)} → ${slotId}`);

      reelOperations.push({
        updateOne: {
          filter: { 
            userId: userId,
            slotId: slotId
          },
          update: {
            $addToSet: { ids: postId },
            $setOnInsert: {
              userId: userId,
              slotId: slotId,
              createdAt: new Date()
            },
            $set: { updatedAt: new Date() }
          },
          upsert: true
        }
      });
    }

    // ✅ Process posts (same logic)
    for (const item of posts) {
      const postId = typeof item === 'string' ? item : item.postId;
      let slotId = typeof item === 'object' ? item.slotId : null;
      
      if (!slotId) {
        console.log(`[BATCH-POST-LOOKUP] ${postId.substring(0, 8)} - client didn't provide slotId, looking up...`);
        slotId = await getSlotForPost(postId);
        postSlotLookups.push(postId);
      }
      
      if (!slotId) {
        console.warn(`[BATCH-SKIP-POST] ${postId} - slot not found`);
        continue;
      }

      console.log(`[BATCH-POST] ${postId.substring(0, 8)} → ${slotId}`);

      postOperations.push({
        updateOne: {
          filter: { 
            userId: userId,
            slotId: slotId
          },
          update: {
            $addToSet: { ids: postId },
            $setOnInsert: {
              userId: userId,
              slotId: slotId,
              createdAt: new Date()
            },
            $set: { updatedAt: new Date() }
          },
          upsert: true
        }
      });
    }

    // Execute bulk writes
    const [postResults, reelResults] = await Promise.all([
      postOperations.length > 0 
        ? db.collection('contrib_posts').bulkWrite(postOperations, { 
            ordered: false,
            writeConcern: { w: 1, j: false }
          })
        : Promise.resolve({ upsertedCount: 0, modifiedCount: 0 }),
      
      reelOperations.length > 0
        ? db.collection('contrib_reels').bulkWrite(reelOperations, { 
            ordered: false,
            writeConcern: { w: 1, j: false }
          })
        : Promise.resolve({ upsertedCount: 0, modifiedCount: 0 })
    ]);

    const duration = Date.now() - startTime;

    console.log(`[BATCH-COMPLETE] userId=${userId} | ${postOperations.length}P + ${reelOperations.length}R operations in ${duration}ms`);
    
    if (reelSlotLookups.length > 0 || postSlotLookups.length > 0) {
      console.warn(`[BATCH-PERFORMANCE] Had to lookup ${reelSlotLookups.length} reel slots + ${postSlotLookups.length} post slots - client should provide slotId`);
    }

    res.json({
      success: true,
      processed: {
        posts: posts.length,
        reels: reels.length
      },
      saved: {
        posts: postOperations.length,
        reels: reelOperations.length
      },
      results: {
        posts: {
          upserted: postResults.upsertedCount || 0,
          modified: postResults.modifiedCount || 0
        },
        reels: {
          upserted: reelResults.upsertedCount || 0,
          modified: reelResults.modifiedCount || 0
        }
      },
      performance: {
        reelSlotLookups: reelSlotLookups.length,
        postSlotLookups: postSlotLookups.length
      },
      requestId,
      duration
    });

  } catch (error) {
    const duration = Date.now() - startTime;
    console.error(`[BATCH-ERROR] requestId=${requestId} | duration=${duration}ms`, error);

    res.status(500).json({
      success: false,
      error: error.message,
      requestId,
      duration
    });
  }
});

const slotCache = new SimpleLRU(10000, 60000); // same config as before

async function getSlotForPost(postId) {
  const cacheKey = `slot:post:${postId}`;
  
  if (slotCache.has(cacheKey)) {
    return slotCache.get(cacheKey);
  }

  // ✅ CRITICAL FIX: Use aggregation to find exact slot
  const result = await db.collection('posts').aggregate([
    { $match: { 'postList.postId': postId } },
    { $limit: 1 },
    { $project: { _id: 1 } }
  ]).toArray();

  const slotId = result.length > 0 ? result[0]._id : null;
  
  if (slotId) {
    slotCache.set(cacheKey, slotId);
    console.log(`[SLOT-LOOKUP] ${postId.substring(0, 8)} found in ${slotId}`);
  } else {
    console.warn(`[SLOT-LOOKUP-FAILED] ${postId.substring(0, 8)} not found in any slot`);
  }
  
  return slotId;
}



async function getSlotForReel(reelId) {
  const cacheKey = `slot:reel:${reelId}`;
  
  if (slotCache.has(cacheKey)) {
    return slotCache.get(cacheKey);
  }

  // ✅ CRITICAL FIX: Use aggregation to find exact slot
  const result = await db.collection('reels').aggregate([
    { $match: { 'reelsList.postId': reelId } },
    { $limit: 1 },
    { $project: { _id: 1 } }
  ]).toArray();

  const slotId = result.length > 0 ? result[0]._id : null;
  
  if (slotId) {
    slotCache.set(cacheKey, slotId);
    console.log(`[SLOT-LOOKUP] ${reelId.substring(0, 8)} found in ${slotId}`);
  } else {
    console.warn(`[SLOT-LOOKUP-FAILED] ${reelId.substring(0, 8)} not found in any slot`);
  }
  
  return slotId;
}

app.get('/api/status/:userId', async (req, res) => {
try {
const userId = req.params.userId;
const status = await dbManager.getUserStatus(userId);

if (status) {
return res.json({
success: true, isNew: false,
posts: { latest: status.latestPostSlotId, normal: status.normalPostSlotId },
reels: { latest: status.latestReelSlotId, normal: status.normalReelSlotId },
status
});
}

const [postsLatest, reelsLatest] = await Promise.all([dbManager.getLatestDocId('posts'), dbManager.getLatestDocId('reels')]);
return res.json({
success: true, isNew: true,
posts: { latest: postsLatest, normal: postsLatest },
reels: { latest: reelsLatest, normal: reelsLatest },
computedAt: new Date().toISOString()
});
} catch (e) {
console.error('[STATUS-ERROR]', e);
return res.status(500).json({ error: 'Failed to fetch status' });
}
});

app.get('/api/latest/:collection', async (req, res) => {
try {
const col = req.params.collection;
if (!['posts','reels'].includes(col)) return res.status(400).json({ error: 'Invalid collection' });

const latest = await dbManager.getLatestDocId(col);
return res.json({ success: true, latest: latest || null });
} catch (e) {
console.error('[LATEST-ERROR]', e);
return res.status(500).json({ error: 'Failed to fetch latest id' });
}
});

app.get('/api/doc/:collection/:docId', async (req, res) => {
try {
const { collection, docId } = req.params;
if (!['posts','reels'].includes(collection)) return res.status(400).json({ error: 'Invalid collection' });

const doc = await dbManager.getDocument(collection, docId);
if (!doc) return res.status(404).json({ error: 'Not found' });

return res.json({ success: true, doc });
} catch (e) {
console.error('[DOC-ERROR]', e);
return res.status(500).json({ error: 'Failed to read doc' });
}
});

// REPLACE /api/contributed-views/:type/:userId endpoint
app.get('/api/contributed-views/:type/:userId', async (req, res) => {
try {
const { type, userId } = req.params;
if (!['posts', 'reels'].includes(type)) {
return res.status(400).json({ error: 'Invalid type' });
}

console.log(`[postId_debug] [GET-CONTRIB] userId=${userId} | type=${type}`);

const collection = type === 'posts' ? 'contrib_posts' : 'contrib_reels';

// ✅ Find all documents for this user (now _id is the document name)
const docs = await db.collection(collection)
.find({ userId })
.toArray();

console.log(`[postId_debug] [GET-CONTRIB-FOUND] ${docs.length} documents for userId=${userId}`);

// ✅ Build response object with document names as keys
const contributions = {};
for (const doc of docs) {
const docName = doc._id; // ✅ _id is now 'post_0', 'reels_1', etc.
contributions[docName] = doc.ids || [];
console.log(`[postId_debug] [GET-CONTRIB-DOC] _id=${docName} | count=${contributions[docName].length}`);
}

return res.json({ success: true, contributions });

} catch (e) {
console.error('[postId_debug] [CONTRIB-VIEWS-ERROR]', e);
return res.status(500).json({ error: 'Failed to read contributions' });
}
});

app.get('/api/contributed-views/stats/:userId', async (req, res) => {
try {
const { userId } = req.params;
const stats = await dbManager.getContributedViewsStats(userId);
return res.json({ success: true, stats });
} catch (e) {
console.error('[CONTRIB-STATS-ERROR]', e);
return res.status(500).json({ error: 'Failed to read contribution stats' });
}
});



app.use('/api/posts', (req, res, next) => {
if (req.method !== 'POST') return next();

const postId = req.body.postId;
if (!postId) return next();

const key = `post_${postId}`;
const now = Date.now();

// Check if same postId was processed recently
if (requestDeduplication.has(key)) {
const lastTime = requestDeduplication.get(key);
if (now - lastTime < DEDUP_WINDOW) {
console.log(`[REQUEST-DEDUP-BLOCKED] ${postId} - duplicate request within 5s window`);
return res.status(429).json({
success: false,
error: 'Duplicate request - post already processing',
postId
});
}
}

requestDeduplication.set(key, now);

// Cleanup old entries
if (requestDeduplication.size > 1000) {
for (const [k, time] of requestDeduplication.entries()) {
if (now - time > DEDUP_WINDOW) {
requestDeduplication.delete(k);
}
}
}

next();
});


// Global error handler for async routes
app.use(async (err, req, res, next) => {
if (err) {
log('error', '[ROUTE-ERROR]', {
url: req.originalUrl,
method: req.method,
error: err.message,
stack: err.stack
});

return res.status(500).json({
error: 'Internal server error',
message: process.env.NODE_ENV === 'development' ? err.message : undefined
});
}
next();
});



app.post('/api/posts', async (req, res) => {
const postData = req.body;

if (!postData.userId || !postData.postId) {
console.log('[POST-VALIDATION-FAILED] Missing userId or postId');
return res.status(400).json({ error: 'userId & postId required' });
}

const col = postData.isReel ? 'reels' : 'posts';
const postId = postData.postId;
const userId = postData.userId;
const listKey = col === 'reels' ? 'reelsList' : 'postList';

console.log(`[POST-CREATE-REQUEST] PostId: ${postId} | User: ${userId} | Type: ${col}`);

// ✅ CRITICAL: Global duplicate check BEFORE any processing
const globalCheck = await db.collection(col).findOne(
{ [`${listKey}.postId`]: postId },
{ projection: { _id: 1, count: 1 } }
);

if (globalCheck) {
console.log(`[DUPLICATE-BLOCKED-EARLY] ${postId} already exists in ${globalCheck._id}`);
return res.json({
success: true,
documentID: globalCheck._id,
postId: postId,
message: 'Post already exists',
duplicate: true
});
}

const requestKey = `create_${postId}_${col}`;

// Check in-flight requests
if (activeRequestsWithTimestamp.has(requestKey)) {
console.log(`[POST-IN-FLIGHT] ${postId} already processing`);
const existingPromise = activeRequestsWithTimestamp.get(requestKey).promise;
try {
const result = await existingPromise;
return res.json({ ...result, servedFromCache: true });
} catch (err) {
return res.status(500).json({ error: 'In-flight request failed', details: err.message });
}
}

// Create new processing promise
const processingPromise = (async () => {
try {
postData.serverTimestamp = new Date().toISOString();

console.log(`[POST-ALLOCATE-BEGIN] ${postId}`);

const allocatedDoc = await dbManager.allocateSlot(col, postData, 3);

if (!allocatedDoc || !allocatedDoc._id) {
throw new Error('Slot allocation failed: invalid document');
}

const documentId = allocatedDoc._id;
postData.documentID = documentId;

console.log(`[POST-ALLOCATED-FINAL] ${postId} → ${documentId} (count: ${allocatedDoc.count})`);

await dbManager.saveToUserPosts(userId, postData);

console.log(`[POST-COMPLETE] ${postId} saved successfully`);

return {
success: true,
documentID: documentId,
postId: postId,
message: 'Post created successfully',
slotCount: allocatedDoc.count
};

} catch (error) {
console.error(`[POST-CREATE-FAILED] ${postId} | Error: ${error.message}`);
throw error;
} finally {
activeRequestsWithTimestamp.delete(requestKey);
console.log(`[POST-CLEANUP] ${requestKey} removed from active requests`);
}
})();

activeRequestsWithTimestamp.set(requestKey, {
promise: processingPromise,
timestamp: Date.now()
});

try {
const result = await processingPromise;
return res.json(result);
} catch (error) {
return res.status(500).json({
error: 'Failed to create post',
message: error.message,
postId: postId
});
}
});




// Ultra-fast single reel interaction check
app.get('/api/interactions/check-single/:userId/:postId', async (req, res) => {
try {
const { userId, postId } = req.params;

if (!userId || !postId) {
return res.status(400).json({ error: 'userId and postId required' });
}

const today = new Date().toISOString().split('T')[0];

// Single optimized query using aggregation pipeline
const pipeline = [
{
$match: { userId: userId }
},
{
$lookup: {
from: 'user_reel_interactions',
let: { uid: '$userId' },
pipeline: [
{
$match: {
$expr: { $eq: ['$userId', '$$uid'] },
$or: [
{ "viewedReels.postId": postId },
{ "likedReels.postId": postId }
]
}
},
{
$project: {
viewedReels: {
$filter: {
input: '$viewedReels',
cond: { $eq: ['$$this.postId', postId] }
}
},
likedReels: {
$filter: {
input: '$likedReels',
cond: { $eq: ['$$this.postId', postId] }
}
}
}
}
],
as: 'interactions'
}
},
{
$project: {
userId: 1,
viewedToday: 1,
likedToday: 1,
retentionContributed: 1,
hasViewed: {
$or: [
{ $in: [postId, { $ifNull: ['$viewedToday', []] }] },
{ $gt: [{ $size: { $ifNull: [{ $arrayElemAt: ['$interactions.viewedReels', 0] }, []] }}, 0] }
]
},
hasLiked: {
$or: [
{ $in: [postId, { $ifNull: ['$likedToday', []] }] },
{ $gt: [{ $size: { $ifNull: [{ $arrayElemAt: ['$interactions.likedReels', 0] }, []] }}, 0] }
]
},
hasRetentionContributed: {
$or: [
{ $in: [postId, { $ifNull: ['$retentionContributed', []] }] },
{
$gt: [{
$size: {
$filter: {
input: { $ifNull: [{ $arrayElemAt: ['$interactions.viewedReels', 0] }, []] },
cond: { $eq: ['$$this.retentionContributed', true] }
}
}
}, 0]
}
]
}
}
}
];

const result = await db.collection('user_interaction_cache').aggregate(pipeline).toArray();

let interaction = {
viewed: false,
liked: false,
retentionContributed: false
};

if (result && result.length > 0) {
const data = result[0];
interaction = {
viewed: data.hasViewed || false,
liked: data.hasLiked || false,
retentionContributed: data.hasRetentionContributed || false
};
}

return res.json({
success: true,
postId,
interaction,
queryTime: Date.now()
});

} catch (error) {
console.error('[SINGLE-CHECK-ERROR]', error);
return res.status(500).json({ error: 'Failed to check interaction' });
}
});



// Add this to your existing Node.js server code

// Add new retention endpoint to handle audience retention updates
app.post('/api/retention/update', async (req, res) => {
try {
const { reelId, userId, retentionPercent, watchedDuration, totalDuration, sourceDocument, timestamp } = req.body;

// Validate required fields
if (!reelId || !userId || retentionPercent === undefined || !watchedDuration || !totalDuration) {
return res.status(400).json({
error: 'Missing required fields',
required: ['reelId', 'userId', 'retentionPercent', 'watchedDuration', 'totalDuration']
});
}

// Validate data types and ranges
if (typeof retentionPercent !== 'number' || retentionPercent < 0 || retentionPercent > 100) {
return res.status(400).json({ error: 'retentionPercent must be between 0 and 100' });
}

if (watchedDuration < 3000) { // Must watch at least 3 seconds
return res.status(400).json({ error: 'watchedDuration must be at least 3000ms' });
}

if (totalDuration < 5000) { // Video must be at least 5 seconds
return res.status(400).json({ error: 'totalDuration must be at least 5000ms' });
}

console.log(`[RETENTION-UPDATE] ${new Date().toISOString()} | User: ${userId} | Reel: ${reelId} | Retention: ${retentionPercent.toFixed(2)}% | Watched: ${watchedDuration}ms / ${totalDuration}ms`);

// Find which collection contains this reel and get the document name
const { collection, documentId } = await findReelLocation(reelId, sourceDocument);

if (!collection || !documentId) {
console.log(`[RETENTION-ERROR] Reel ${reelId} not found in any collection`);
return res.status(404).json({ error: 'Reel not found' });
}

// Update retention data in the correct document
const updateResult = await updateReelRetention(collection, documentId, reelId, {
userId,
retentionPercent: Math.round(retentionPercent * 100) / 100, // Round to 2 decimal places
watchedDuration,
totalDuration,
timestamp: timestamp || new Date().toISOString(),
watchDate: new Date().toISOString().split('T')[0] // YYYY-MM-DD format
});

if (updateResult.success) {
console.log(`[RETENTION-SUCCESS] Updated retention for reel ${reelId} in ${collection}/${documentId}`);
return res.json({
success: true,
message: 'Retention updated successfully',
reelId,
documentId,
collection,
retentionPercent,
totalViews: updateResult.totalViews,
averageRetention: updateResult.averageRetention
});
} else {
console.error(`[RETENTION-ERROR] Failed to update retention for reel ${reelId}:`, updateResult.error);
return res.status(500).json({ error: 'Failed to update retention', details: updateResult.error });
}

} catch (error) {
console.error('[RETENTION-UPDATE-ERROR]', error);
return res.status(500).json({ error: 'Internal server error', details: error.message });
}
});


function extractDocumentId(item) {
// Extract document ID from sourceDocument or _id field
if (item.sourceDocument) return item.sourceDocument;
if (item._id) return item._id;
return null;
}


app.post('/api/feed/instagram-ranked', async (req, res) => {
  const startTime = Date.now();
  const { userId, limit = DEFAULT_CONTENT_BATCH_SIZE, excludedPostIds = [], excludedReelIds = [] } = req.body;

  if (!userId) {
    return res.status(400).json({ success: false, error: 'userId required' });
  }

  try {
    console.log(`\n========== [FEED-REQUEST-START] ==========`);
    console.log(`User: ${userId} | Limit: ${limit} | Excluded: ${excludedPostIds.length}P + ${excludedReelIds.length}R`);

    // ✅ Use the CORRECT algorithm function
    const [reelsResult, postsResult] = await Promise.all([
      dbManager.getOptimizedFeedFixedReads(userId, 'reels', Math.ceil(limit * 0.6)),
      dbManager.getOptimizedFeedFixedReads(userId, 'posts', Math.ceil(limit * 0.4))
    ]);

    // Merge content
    const mixedContent = [...reelsResult.content, ...postsResult.content];
    
    // Sort by composite score
    mixedContent.sort((a, b) => {
      const retentionDiff = (b.retention || 0) - (a.retention || 0);
      if (Math.abs(retentionDiff) > 1) return retentionDiff;
      const likesDiff = (b.likeCount || 0) - (a.likeCount || 0);
      if (likesDiff !== 0) return likesDiff;
      return (b.commentCount || 0) - (a.commentCount || 0);
    });

    const duration = Date.now() - startTime;

    console.log(`\n========== [FEED-REQUEST-COMPLETE] ==========`);
    console.log(`User: ${userId} | Returned: ${mixedContent.length} items | Time: ${duration}ms`);
    console.log(`Slots Read: Reels=${reelsResult.metadata?.slotsRead?.length || 0}, Posts=${postsResult.metadata?.slotsRead?.length || 0}`);
    console.log(`=============================================\n`);

    return res.json({
      success: true,
      content: mixedContent.slice(0, limit),
      hasMore: mixedContent.length >= limit,
      metadata: {
        totalReturned: mixedContent.length,
        reelsCount: reelsResult.content.length,
        postsCount: postsResult.content.length,
        slotsRead: {
          reels: reelsResult.metadata?.slotsRead || [],
          posts: postsResult.metadata?.slotsRead || []
        },
        duration
      }
    });

  } catch (error) {
    console.error(`[FEED-ERROR] ${error.message}`);
    return res.status(500).json({ success: false, error: error.message });
  }
});

// Instagram feed builder (same as before)
function buildInstagramFeed(followingContent, globalContent, limit, offset) {
followingContent.sort((a, b) => b.rankingScore - a.rankingScore);
globalContent.sort((a, b) => b.rankingScore - a.rankingScore);

const feed = [];
const usedUserIds = new Set();
let followingIndex = 0;
let globalIndex = 0;

const followingRatio = 0.65;
const followingSlots = Math.ceil(limit * followingRatio);

// Phase 1: Following content with diversity
let followingAdded = 0;
while (followingAdded < followingSlots && followingIndex < followingContent.length) {
const item = followingContent[followingIndex++];

if (!usedUserIds.has(item.sourceUserId)) {
feed.push(item);
usedUserIds.add(item.sourceUserId);
followingAdded++;

if (feed.length % 3 === 0) {
usedUserIds.clear();
}
}
}

// Phase 2: Global content with diversity
while (feed.length < limit && globalIndex < globalContent.length) {
const item = globalContent[globalIndex++];

if (!usedUserIds.has(item.sourceUserId)) {
feed.push(item);
usedUserIds.add(item.sourceUserId);

if (feed.length % 3 === 0) {
usedUserIds.clear();
}
}
}

// Phase 3: Fill remaining if needed
if (feed.length < limit) {
const remaining = [...followingContent.slice(followingIndex), ...globalContent.slice(globalIndex)];
feed.push(...remaining.slice(0, limit - feed.length));
}

// Phase 4: Balance content types
const mixedFeed = balanceContentTypes(feed);

return mixedFeed.slice(offset, offset + limit);
}

// Instagram ranking algorithm
function calculateInstagramRankingScore(item, isFollowingContent) {
const FOLLOWING_BOOST = 10000000; // Massive boost for following content
const RECENCY_WEIGHT = 100000; // Recent content prioritized
const RETENTION_WEIGHT = 10000; // High engagement = high priority
const LIKE_WEIGHT = 100;
const COMMENT_WEIGHT = 50;
const VIEW_WEIGHT = 1;

// Calculate recency score (newer = higher)
const ageInHours = item.timestamp
? (Date.now() - new Date(item.timestamp).getTime()) / (1000 * 60 * 60)
: 999;
const recencyScore = Math.max(0, 168 - ageInHours); // 168 hours = 7 days

const retention = parseFloat(item.retention) || 0;
const likes = parseInt(item.likeCount) || 0;
const comments = parseInt(item.commentCount) || 0;
const views = parseInt(item.viewCount) || 0;

let score = (
(isFollowingContent ? FOLLOWING_BOOST : 0) +
(recencyScore * RECENCY_WEIGHT) +
(retention * RETENTION_WEIGHT) +
(likes * LIKE_WEIGHT) +
(comments * COMMENT_WEIGHT) +
(views * VIEW_WEIGHT)
);

return score;
}




// Add this endpoint after the /api/retention/analytics/:reelId endpoint
app.get('/api/posts/user-id/:postId', async (req, res) => {
try {
const { postId } = req.params;

if (!postId) {
return res.status(400).json({ success: false, error: 'postId required' });
}

log('info', `[USER-ID-RECOVERY] Searching for userId of postId: ${postId}`);

// Search in reels collection
const reelDoc = await db.collection('reels').findOne(
{ 'reelsList.postId': postId },
{ projection: { 'reelsList.$': 1 } }
);

if (reelDoc && reelDoc.reelsList && reelDoc.reelsList.length > 0) {
const reel = reelDoc.reelsList[0];
const userId = reel.userId || reel.uid || reel.user_id;

if (userId) {
log('info', `[USER-ID-FOUND] ${postId} -> ${userId}`);
return res.json({ success: true, userId, postId, source: 'reels' });
}
}

// Search in posts collection
const postDoc = await db.collection('posts').findOne(
{ 'postList.postId': postId },
{ projection: { 'postList.$': 1 } }
);

if (postDoc && postDoc.postList && postDoc.postList.length > 0) {
const post = postDoc.postList[0];
const userId = post.userId || post.uid || post.user_id;

if (userId) {
log('info', `[USER-ID-FOUND] ${postId} -> ${userId}`);
return res.json({ success: true, userId, postId, source: 'posts' });
}
}

// Search in user_posts as last resort
const userPostDoc = await db.collection('user_posts').findOne(
{ postId },
{ projection: { userId: 1 } }
);

if (userPostDoc && userPostDoc.userId) {
log('info', `[USER-ID-FOUND-FALLBACK] ${postId} -> ${userPostDoc.userId}`);
return res.json({ success: true, userId: userPostDoc.userId, postId, source: 'user_posts' });
}

log('warn', `[USER-ID-NOT-FOUND] ${postId} has no userId in any collection`);
return res.status(404).json({ success: false, error: 'userId not found for this post' });

} catch (error) {
log('error', '[USER-ID-RECOVERY-ERROR]', error);
return res.status(500).json({ success: false, error: 'Failed to recover userId' });
}
});




// Add analytics endpoint to get retention statistics
app.get('/api/retention/analytics/:reelId', async (req, res) => {
try {
const { reelId } = req.params;

if (!reelId) {
return res.status(400).json({ error: 'reelId is required' });
}

// Find the reel location
const { collection, documentId } = await findReelLocation(reelId);

if (!collection || !documentId) {
return res.status(404).json({ error: 'Reel not found' });
}

// Get retention analytics
const analytics = await getReelRetentionAnalytics(collection, documentId, reelId);

return res.json({
success: true,
reelId,
analytics
});

} catch (error) {
console.error('[RETENTION-ANALYTICS-ERROR]', error);
return res.status(500).json({ error: 'Failed to get retention analytics', details: error.message });
}
});


async function createInstagramFeedIndexes() {
try {
// Ultra-fast compound indexes for Instagram-style queries
const indexes = [
// Following queries optimization
{ collection: 'posts', index: { 'postList.userId': 1, 'postList.timestamp': -1 }, options: { background: true } },
{ collection: 'reels', index: { 'reelsList.userId': 1, 'reelsList.timestamp': -1 }, options: { background: true } },

// Ranking optimization
{ collection: 'posts', index: { 'postList.retention': -1, 'postList.likeCount': -1, 'postList.timestamp': -1 }, options: { background: true } },
{ collection: 'reels', index: { 'reelsList.retention': -1, 'reelsList.likeCount': -1, 'reelsList.timestamp': -1 }, options: { background: true } },

// User diversity optimization
{ collection: 'posts', index: { 'postList.userId': 1, 'postList.postId': 1 }, options: { background: true } },
{ collection: 'reels', index: { 'reelsList.userId': 1, 'reelsList.postId': 1 }, options: { background: true } }
];

for (const { collection, index, options } of indexes) {
await db.collection(collection).createIndex(index, options);
log('info', `Created Instagram feed index for ${collection}`);
}

log('info', '[INSTAGRAM-INDEXES] All feed optimization indexes created');
} catch (error) {
log('error', '[INSTAGRAM-INDEX-ERROR]', error.message);
}
}


// Helper function to find which collection and document contains a specific reel
async function findReelLocation(reelId, hintSourceDocument = null) {
try {
// If we have a hint about the source document, try that first
if (hintSourceDocument && hintSourceDocument !== 'unknown') {
const hintResult = await checkReelInDocument('reels', hintSourceDocument, reelId);
if (hintResult) {
return { collection: 'reels', documentId: hintSourceDocument };
}
}

// Search in reels collection
const reelsResult = await searchReelInCollection('reels', reelId);
if (reelsResult) {
return { collection: 'reels', documentId: reelsResult };
}

return { collection: null, documentId: null };

} catch (error) {
console.error('Error finding reel location:', error);
return { collection: null, documentId: null };
}
}

// Helper function to search for a reel in a specific collection
async function searchReelInCollection(collectionName, reelId) {
try {
const collection = db.collection(collectionName);

// Use aggregation to find the document containing this reel
const pipeline = [
{
$match: {
$or: [
{ "reelsList.postId": reelId },
{ "postList.postId": reelId } // Just in case it's misnamed
]
}
},
{
$project: {
_id: 1,
found: {
$cond: [
{ $in: [reelId, "$reelsList.postId"] },
true,
false
]
}
}
},
{ $limit: 1 }
];

const results = await collection.aggregate(pipeline).toArray();

if (results && results.length > 0) {
return results[0]._id;
}

return null;

} catch (error) {
console.error(`Error searching reel in ${collectionName}:`, error);
return null;
}
}

// Helper function to check if a reel exists in a specific document
async function checkReelInDocument(collectionName, documentId, reelId) {
try {
const collection = db.collection(collectionName);
const document = await collection.findOne(
{ _id: documentId },
{ projection: { "reelsList.postId": 1 } }
);

if (document && document.reelsList) {
return document.reelsList.some(reel => reel.postId === reelId);
}

return false;

} catch (error) {
console.error('Error checking reel in document:', error);
return false;
}
}

// Main function to update retention data for a specific reel
async function updateReelRetention(collectionName, documentId, reelId, retentionData) {
try {
const collection = db.collection(collectionName);

// First, try to update existing retention data for this user
const updateExistingResult = await collection.updateOne(
{
_id: documentId,
"reelsList.postId": reelId,
"reelsList.retention.userId": retentionData.userId
},
{
$set: {
"reelsList.$.retention.$[elem].retentionPercent": retentionData.retentionPercent,
"reelsList.$.retention.$[elem].watchedDuration": retentionData.watchedDuration,
"reelsList.$.retention.$[elem].totalDuration": retentionData.totalDuration,
"reelsList.$.retention.$[elem].timestamp": retentionData.timestamp,
"reelsList.$.retention.$[elem].watchDate": retentionData.watchDate
}
},
{
arrayFilters: [
{ "elem.userId": retentionData.userId }
]
}
);

// If no existing retention data was updated, add new retention data
if (updateExistingResult.modifiedCount === 0) {
const addNewResult = await collection.updateOne(
{
_id: documentId,
"reelsList.postId": reelId
},
{
$push: {
"reelsList.$.retention": retentionData
}
}
);

if (addNewResult.modifiedCount === 0) {
// If the reel doesn't have a retention array, create it
await collection.updateOne(
{
_id: documentId,
"reelsList.postId": reelId
},
{
$set: {
"reelsList.$.retention": [retentionData]
}
}
);
}
}

// Calculate updated analytics
const analytics = await getReelRetentionAnalytics(collectionName, documentId, reelId);

return {
success: true,
totalViews: analytics.totalViews,
averageRetention: analytics.averageRetention
};

} catch (error) {
console.error('Error updating reel retention:', error);
return {
success: false,
error: error.message
};
}
}

// Function to get comprehensive retention analytics for a reel
async function getReelRetentionAnalytics(collectionName, documentId, reelId) {
try {
const collection = db.collection(collectionName);

const pipeline = [
{ $match: { _id: documentId } },
{ $unwind: "$reelsList" },
{ $match: { "reelsList.postId": reelId } },
{
$project: {
retention: { $ifNull: ["$reelsList.retention", []] }
}
},
{
$unwind: {
path: "$retention",
preserveNullAndEmptyArrays: true
}
},
{
$group: {
_id: null,
totalViews: { $sum: 1 },
totalRetention: { $sum: "$retention.retentionPercent" },
averageRetention: { $avg: "$retention.retentionPercent" },
maxRetention: { $max: "$retention.retentionPercent" },
minRetention: { $min: "$retention.retentionPercent" },
retentionData: { $push: "$retention" }
}
}
];

const results = await collection.aggregate(pipeline).toArray();

if (results && results.length > 0) {
const data = results[0];

// Calculate retention distribution
const retentionRanges = {
"0-25%": 0,
"26-50%": 0,
"51-75%": 0,
"76-100%": 0
};

if (data.retentionData) {
data.retentionData.forEach(item => {
if (item && typeof item.retentionPercent === 'number') {
const percent = item.retentionPercent;
if (percent <= 25) retentionRanges["0-25%"]++;
else if (percent <= 50) retentionRanges["26-50%"]++;
else if (percent <= 75) retentionRanges["51-75%"]++;
else retentionRanges["76-100%"]++;
}
});
}

return {
totalViews: Math.max(0, data.totalViews - 1), // Subtract 1 because of the initial null item from unwind
averageRetention: Math.round((data.averageRetention || 0) * 100) / 100,
maxRetention: data.maxRetention || 0,
minRetention: data.minRetention || 0,
retentionDistribution: retentionRanges,
lastUpdated: new Date().toISOString()
};
} else {
return {
totalViews: 0,
averageRetention: 0,
maxRetention: 0,
minRetention: 0,
retentionDistribution: {
"0-25%": 0,
"26-50%": 0,
"51-75%": 0,
"76-100%": 0
},
lastUpdated: new Date().toISOString()
};
}

} catch (error) {
console.error('Error getting retention analytics:', error);
return {
totalViews: 0,
averageRetention: 0,
maxRetention: 0,
minRetention: 0,
retentionDistribution: {
"0-25%": 0,
"26-50%": 0,
"51-75%": 0,
"76-100%": 0
},
lastUpdated: new Date().toISOString(),
error: error.message
};
}
}






//



async function createOptimizedIndexes() {
const db = client.db(DB_NAME);

// user_reel_interactions indexes
await db.collection('user_reel_interactions').createIndex({ "_id": 1 }); // Primary key
await db.collection('user_reel_interactions').createIndex({ "userId": 1, "date": -1 });
await db.collection('user_reel_interactions').createIndex({ "viewedReels.postId": 1 });
await db.collection('user_reel_interactions').createIndex({ "likedReels.postId": 1 });

// reel_stats indexes
await db.collection('reel_stats').createIndex({ "_id": 1 }); // Primary key (postId)
await db.collection('reel_stats').createIndex({ "sourceDocument": 1 });
await db.collection('reel_stats').createIndex({ "likeCount": -1 }); // For trending

// user_interaction_cache indexes
await db.collection('user_interaction_cache').createIndex({ "_id": 1 });
await db.collection('user_interaction_cache').createIndex({ "userId": 1 });
await db.collection('user_interaction_cache').createIndex({ "ttl": 1 }, { expireAfterSeconds: 0 });

console.log('[INDEXES] All interaction indexes created successfully');
}



async function createRankingIndexes() {
try {
const rankingIndexes = [
// Posts collection
{ collection: 'posts', index: { retention: -1, likeCount: -1, commentCount: -1, viewCount: -1, createdAt: -1 }, options: { background: true } },
// Reels collection
{ collection: 'reels', index: { retention: -1, likeCount: -1, commentCount: -1, viewCount: -1, createdAt: -1 }, options: { background: true } },
// Compound indexes for efficient filtering
{ collection: 'posts', index: { '_id': 1, 'retention': -1 }, options: { background: true } },
{ collection: 'reels', index: { '_id': 1, 'retention': -1 }, options: { background: true } }
];

for (const { collection, index, options } of rankingIndexes) {
await db.collection(collection).createIndex(index, options);
log('info', `Created ranking index for ${collection}`);
}
} catch (error) {
log('warn', 'Ranking index creation error:', error.message);
}
}





app.post('/api/interactions/check', async (req, res) => {
try {
const { userId, reelIds } = req.body;

if (!userId || !Array.isArray(reelIds) || reelIds.length === 0) {
return res.status(400).json({ error: 'userId and reelIds array required' });
}

if (reelIds.length > 50) {
return res.status(400).json({ error: 'Maximum 50 reelIds per request' });
}

console.log(`[INTERACTION-CHECK] ${userId} checking ${reelIds.length} reels`);

// Step 1: Check today's cache first (fastest)
const today = new Date().toISOString().split('T')[0];
const cacheKey = `${userId}_session_${today}`;

const cacheDoc = await db.collection('user_interaction_cache').findOne({ _id: cacheKey });

let viewedReels = new Set();
let likedReels = new Set();
let retentionContributed = new Set();

if (cacheDoc) {
// Use cached data
viewedReels = new Set(cacheDoc.viewedToday || []);
likedReels = new Set(cacheDoc.likedToday || []);
retentionContributed = new Set(cacheDoc.retentionContributed || []);
} else {
// Step 2: Query recent interactions (last 7 days for comprehensive check)
const recentDates = [];
for (let i = 0; i < 7; i++) {
const date = new Date();
date.setDate(date.getDate() - i);
recentDates.push(`${userId}_${date.toISOString().split('T')[0]}`);
}

const recentInteractions = await db.collection('user_reel_interactions').find({
_id: { $in: recentDates }
}).toArray();

// Process recent interactions
recentInteractions.forEach(doc => {
if (doc.viewedReels) {
doc.viewedReels.forEach(item => {
viewedReels.add(item.postId);
if (item.retentionContributed) {
retentionContributed.add(item.postId);
}
});
}
if (doc.likedReels) {
doc.likedReels.forEach(item => likedReels.add(item.postId));
}
});

// Update cache for next requests
await db.collection('user_interaction_cache').updateOne(
{ _id: cacheKey },
{
$set: {
userId,
sessionStart: new Date(),
viewedToday: Array.from(viewedReels),
likedToday: Array.from(likedReels),
retentionContributed: Array.from(retentionContributed),
ttl: new Date(Date.now() + 24 * 60 * 60 * 1000) // 24 hours TTL
}
},
{ upsert: true }
);
}

// Step 3: Build response for requested reels
const result = {};
reelIds.forEach(reelId => {
result[reelId] = {
viewed: viewedReels.has(reelId),
liked: likedReels.has(reelId),
retentionContributed: retentionContributed.has(reelId)
};
});

return res.json({
success: true,
interactions: result,
cached: cacheDoc !== null
});

} catch (error) {
console.error('[INTERACTION-CHECK-ERROR]', error);
return res.status(500).json({ error: 'Failed to check interactions' });
}
});





// Update the existing /api/interactions/view endpoint
app.post('/api/interactions/view', async (req, res) => {
try {
const { userId, postId, sourceDocument, retentionData } = req.body;

if (!userId || !postId) {
return res.status(400).json({ error: 'userId and postId required' });
}

console.log(`[VIEW-START] ${userId} -> ${postId} ${retentionData ? '(WITH RETENTION)' : '(VIEW ONLY)'}`);

const today = new Date().toISOString().split('T')[0];
const cacheKey = `${userId}_session_${today}`;
const retentionCacheKey = `retention_${userId}_${postId}`;

if (retentionData) {
console.log(`[RETENTION-PRE-CHECK] ${userId} -> ${postId}`);

// Ultra-fast existence check using compound query
const existingDoc = await db.collection('user_interaction_cache').findOne(
{
_id: cacheKey,
retentionContributed: postId // Array contains check
},
{ projection: { _id: 1 } }
);

if (existingDoc) {
console.log(`[RETENTION-ALREADY-EXISTS] ${userId} already contributed to ${postId} - BLOCKED`);
setCache(retentionCacheKey, true, 7200000);
return res.json({
success: true,
message: 'Retention already contributed',
duplicate: true
});
}

// Use $addToSet to ensure uniqueness at database level
const updateResult = await db.collection('user_interaction_cache').updateOne(
{ _id: cacheKey },
{
$set: {
userId,
ttl: new Date(Date.now() + 24 * 60 * 60 * 1000),
updatedAt: new Date()
},
$addToSet: {
viewedToday: postId,
retentionContributed: postId // This ensures uniqueness
},
$setOnInsert: { createdAt: new Date() }
},
{ upsert: true }
);

// Check if the retention was actually added (not a duplicate)
if (updateResult.modifiedCount === 0 && updateResult.upsertedCount === 0) {
console.log(`[RETENTION-DUPLICATE-BLOCKED] ${userId} -> ${postId} - Database level duplicate`);
setCache(retentionCacheKey, true, 7200000);
return res.json({
success: true,
message: 'Retention already contributed',
duplicate: true
});
}

console.log(`[RETENTION-RECORDED] ${userId} -> ${postId} - Unique contribution saved`);
setCache(retentionCacheKey, true, 7200000);

} else {
// View only - simpler logic
await db.collection('user_interaction_cache').updateOne(
{ _id: cacheKey },
{
$set: {
userId,
ttl: new Date(Date.now() + 24 * 60 * 60 * 1000),
updatedAt: new Date()
},
$addToSet: { viewedToday: postId },
$setOnInsert: { createdAt: new Date() }
},
{ upsert: true }
);

console.log(`[VIEW-ONLY-RECORDED] ${userId} -> ${postId}`);
}

// Background detailed recording remains the same
const viewRecord = {
postId,
viewedAt: new Date(),
sourceDocument: sourceDocument || 'unknown',
retentionContributed: !!retentionData
};

if (retentionData) {
viewRecord.retentionPercent = Math.round(retentionData.retentionPercent * 100) / 100;
viewRecord.watchedDuration = retentionData.watchedDuration;
viewRecord.totalDuration = retentionData.totalDuration;
}

// Background recording
Promise.all([
db.collection('user_reel_interactions').updateOne(
{ _id: `${userId}_${today}` },
{
$set: { userId, date: today, updatedAt: new Date() },
$setOnInsert: { createdAt: new Date() },
$addToSet: { viewedReels: viewRecord }
},
{ upsert: true }
),

db.collection('reel_stats').updateOne(
{ _id: postId },
{
$inc: { viewCount: 1 },
$set: {
lastUpdated: new Date(),
sourceDocument: sourceDocument || 'unknown'
}
},
{ upsert: true }
)
]).then(() => {
console.log(`[DETAILED-RECORD-COMPLETE] ${userId} -> ${postId}`);
}).catch(error => {
console.error('[DETAILED-RECORD-ERROR]', error);
});

return res.json({ success: true, message: 'View recorded successfully' });

} catch (error) {
console.error('[VIEW-RECORD-ERROR]', error);
return res.status(500).json({ error: 'Failed to record view' });
}
});

// ✅ REPLACE THIS SECTION - Remove contributionToLike operations
app.post('/api/interactions/like', async (req, res) => {
try {
const { userId, postId, sourceDocument, action } = req.body;

if (!userId || !postId || !['like', 'unlike'].includes(action)) {
return res.status(400).json({ error: 'userId, postId, and action (like/unlike) required' });
}

const today = new Date().toISOString().split('T')[0];
const docId = `${userId}_${today}`;
const isLiking = action === 'like';

console.log(`[LIKE-${action.toUpperCase()}] ${userId} ${action}d ${postId} - forwarding to PORT 4000`);

// Forward to PORT 4000 (single source of truth)
const port4000Response = await axios.post(
'https://database-22io.onrender.com/api/posts/toggle-like',
{
userId: userId,
postId: postId,
currentlyLiked: !isLiking,
isReel: true
},
{ timeout: 5000 }
);

if (port4000Response.data.success) {
const isLiked = port4000Response.data.isLiked;
const likeCount = port4000Response.data.likeCount;

// Update local cache for quick access
const cacheKey = `${userId}_session_${today}`;
const cacheOperation = isLiked
? { $addToSet: { likedToday: postId } }
: { $pull: { likedToday: postId } };

await db.collection('user_interaction_cache').updateOne(
{ _id: cacheKey },
{
...cacheOperation,
$set: { ttl: new Date(Date.now() + 24 * 60 * 60 * 1000) }
},
{ upsert: true }
);

return res.json({
success: true,
action: isLiked ? 'like' : 'unlike',
likeCount: likeCount,
message: `Successfully ${isLiked ? 'liked' : 'unliked'} reel`
});
}

return res.status(500).json({ error: 'PORT 4000 like failed' });

} catch (error) {
console.error('[LIKE-ERROR]', error);
return res.status(500).json({ error: `Failed to ${req.body.action} reel` });
}
});

// Helper function to update like count in the main reels collection
async function updateLikeCountInReelsCollection(postId, sourceDocument, newLikeCount) {
try {
// Find which collection contains this reel
const collections = ['reels']; // Add other collections if needed

for (const collectionName of collections) {
const result = await db.collection(collectionName).updateOne(
{ "reelsList.postId": postId },
{ $set: { "reelsList.$.likeCount": newLikeCount } }
);

if (result.matchedCount > 0) {
console.log(`[LIKE-COUNT-UPDATE] Updated ${postId} in ${collectionName} to ${newLikeCount} likes`);
break;
}
}
} catch (error) {
console.error('[LIKE-COUNT-UPDATE-ERROR]', error);
// Don't throw error as this is secondary operation
}
}

// Endpoint to get reel statistics (like count, view count, etc.)
app.get('/api/interactions/stats/:postId', async (req, res) => {
try {
const { postId } = req.params;

if (!postId) {
return res.status(400).json({ error: 'postId required' });
}

const stats = await db.collection('reel_stats').findOne({ _id: postId });

if (!stats) {
return res.json({
success: true,
stats: {
postId,
likeCount: 0,
viewCount: 0,
commentCount: 0,
recentLikers: []
}
});
}

return res.json({
success: true,
stats: {
postId: stats._id,
likeCount: stats.likeCount || 0,
viewCount: stats.viewCount || 0,
commentCount: stats.commentCount || 0,
recentLikers: stats.recentLikers || [],
lastUpdated: stats.lastUpdated
}
});

} catch (error) {
console.error('[STATS-ERROR]', error);
return res.status(500).json({ error: 'Failed to get reel statistics' });
}
});


// =================================================================
// Database Cleanup and Maintenance
// =================================================================

// Clean up old interaction data (run weekly via cron job)
async function cleanupOldInteractions() {
try {
const thirtyDaysAgo = new Date();
thirtyDaysAgo.setDate(thirtyDaysAgo.getDate() - 30);

// Remove interaction records older than 30 days
const result = await db.collection('user_reel_interactions').deleteMany({
createdAt: { $lt: thirtyDaysAgo }
});

console.log(`[CLEANUP] Removed ${result.deletedCount} old interaction records`);

// Cache cleanup is automatic via TTL index

} catch (error) {
console.error('[CLEANUP-ERROR]', error);
}
}

// Initialize the interaction system
async function initializeInteractionSystem() {
try {
await createOptimizedIndexes();

// Schedule cleanup to run daily at 2 AM
setInterval(cleanupOldInteractions, 24 * 60 * 60 * 1000);

console.log('[INTERACTION-SYSTEM] Initialized successfully');
} catch (error) {
console.error('[INTERACTION-SYSTEM-ERROR]', error);
}
}

// Call this during server startup
initializeInteractionSystem();




app.get('/api/stats', async (req, res) => {
try {
const [postsCount, reelsCount, userIds, contribPostsCount, contribReelsCount] = await Promise.all([
dbManager.getCollectionCount('posts'),
dbManager.getCollectionCount('reels'),
dbManager.getDistinctUserIds(),
dbManager.getCollectionCount('contrib_posts'),
dbManager.getCollectionCount('contrib_reels')
]);

const stats = {
reelsDocuments: reelsCount,
postsDocuments: postsCount,
users: userIds.length,
contributedPostsSessions: contribPostsCount,
contributedReelsSessions: contribReelsCount,
timestamp: new Date().toISOString(),
operationCounts: dbOpCounters
};

return res.json(stats);
} catch (e) {
console.error('[STATS-ERROR]', e);
return res.status(500).json({ error: 'Failed to fetch stats' });
}
});



// === MongoDB Stress Test Endpoints ===

// Simulate random read: find a random document (or none)
app.get('/api/random-read', async (req, res) => {
try {
const db = mongoose.connection.db;
const collection = db.collection('stress_test');
const random = Math.random();
const doc = await collection.findOne({ random: { $gte: random } });
res.status(200).json({ success: true, random, found: !!doc });
} catch (err) {
console.error('Random read error:', err);
res.status(500).json({ success: false, error: err.message });
}
});

// Simulate random write: insert random data
app.post('/api/random-write', async (req, res) => {
try {
const db = mongoose.connection.db;
const collection = db.collection('stress_test');
const doc = {
random: Math.random(),
timestamp: new Date(),
payload: Math.random().toString(36).substring(2, 10)
};
await collection.insertOne(doc);
res.status(201).json({ success: true });
} catch (err) {
console.error('Random write error:', err);
res.status(500).json({ success: false, error: err.message });
}
});



app.get('/api/user-status/:userId', async (req, res) => {
  const startTime = Date.now();
  const { userId } = req.params;

  try {
    console.log(`[post_algorithm] [READ-1-START] user_status lookup for userId=${userId}`);

    const userStatus = await db.collection('user_status').findOne(
      { _id: userId },
      {
        projection: {
          _id: 1,
          userId: 1,
          latestReelSlotId: 1,
          normalReelSlotId: 1,
          latestPostSlotId: 1,
          normalPostSlotId: 1
        }
      }
    );

    const duration = Date.now() - startTime;

    if (userStatus && userStatus.latestReelSlotId) {
      // Case 1: User exists with correct fields
      let latestReelSlot = userStatus.latestReelSlotId || 'reel_0';
      let normalReelSlot = userStatus.normalReelSlotId || 'reel_0';

      if (latestReelSlot.startsWith('reels_')) {
        latestReelSlot = latestReelSlot.replace('reels_', 'reel_');
      }
      if (normalReelSlot.startsWith('reels_')) {
        normalReelSlot = normalReelSlot.replace('reels_', 'reel_');
      }

      console.log(`[post_algorithm] [READ-1-SUCCESS] user_status EXISTS | duration=${duration}ms | latestReel=${latestReelSlot} | normalReel=${normalReelSlot}`);

      return res.json({
        success: true,
        latestReelSlotId: latestReelSlot,
        normalReelSlotId: normalReelSlot,
        latestPostSlotId: userStatus.latestPostSlotId || 'post_0',
        normalPostSlotId: userStatus.normalPostSlotId || 'post_0',
        reads: 1,
        duration
      });
    } else if (userStatus && !userStatus.latestReelSlotId) {
      // Case 2: Document exists but missing fields
      console.log(`[post_algorithm] [READ-1-INCOMPLETE] Document exists but missing slot fields - updating`);

      const detectionStart = Date.now();

      const reelDocs = await db.collection('reels')
        .find({}, { projection: { _id: 1 } })
        .toArray();

      const postDocs = await db.collection('posts')
        .find({}, { projection: { _id: 1 } })
        .toArray();

      const reelIds = reelDocs
        .map(doc => doc._id)
        .filter(id => id.startsWith('reel_'))
        .sort((a, b) => {
          const numA = parseInt(a.replace('reel_', ''), 10) || 0;
          const numB = parseInt(b.replace('reel_', ''), 10) || 0;
          return numB - numA;
        });

      const postIds = postDocs
        .map(doc => doc._id)
        .filter(id => id.startsWith('post_'))
        .sort((a, b) => {
          const numA = parseInt(a.replace('post_', ''), 10) || 0;
          const numB = parseInt(b.replace('post_', ''), 10) || 0;
          return numB - numA;
        });

      // ✅ CRITICAL FIX: For fresh user, normal = latest - 2 (not latest - 1)
      const latestReelSlot = reelIds.length > 0 ? reelIds[0] : 'reel_0';
      const normalReelSlot = reelIds.length > 2 ? reelIds[2] : (reelIds[0] || 'reel_0');  // ✅ FIXED: index 2, not 1

      const latestPostSlot = postIds.length > 0 ? postIds[0] : 'post_0';
      const normalPostSlot = postIds.length > 2 ? postIds[2] : (postIds[0] || 'post_0');  // ✅ FIXED: index 2, not 1

      const detectionDuration = Date.now() - detectionStart;

      console.log(`[post_algorithm] [AUTO-DETECT-SUCCESS] duration=${detectionDuration}ms | Found ${reelIds.length} reel docs, ${postIds.length} post docs`);
      console.log(`[post_algorithm] [AUTO-DETECT-SLOTS] latestReel=${latestReelSlot} | normalReel=${normalReelSlot} | latestPost=${latestPostSlot} | normalPost=${normalPostSlot}`);

      await db.collection('user_status').updateOne(
        { _id: userId },
        {
          $set: {
            userId: userId,
            latestReelSlotId: latestReelSlot,
            normalReelSlotId: normalReelSlot,
            latestPostSlotId: latestPostSlot,
            normalPostSlotId: normalPostSlot,
            updatedAt: new Date()
          },
          $setOnInsert: {
            createdAt: new Date()
          }
        }
      );

      const totalDuration = Date.now() - startTime;

      console.log(`[post_algorithm] [READ-1-UPDATED] Document updated with detected slots | total_duration=${totalDuration}ms | reads=3`);

      return res.json({
        success: true,
        latestReelSlotId: latestReelSlot,
        normalReelSlotId: normalReelSlot,
        latestPostSlotId: latestPostSlot,
        normalPostSlotId: normalPostSlot,
        reads: 3,
        duration: totalDuration,
        wasIncomplete: true
      });
    } else {
      // Case 3: No document exists at all - CREATE new one
      console.log(`[post_algorithm] [READ-1-NEW-USER] User status doesn't exist - auto-detecting latest documents`);

      const detectionStart = Date.now();

      const reelDocs = await db.collection('reels')
        .find({}, { projection: { _id: 1 } })
        .toArray();

      const postDocs = await db.collection('posts')
        .find({}, { projection: { _id: 1 } })
        .toArray();

      const reelIds = reelDocs
        .map(doc => doc._id)
        .filter(id => id.startsWith('reel_'))
        .sort((a, b) => {
          const numA = parseInt(a.replace('reel_', ''), 10) || 0;
          const numB = parseInt(b.replace('reel_', ''), 10) || 0;
          return numB - numA;
        });

      const postIds = postDocs
        .map(doc => doc._id)
        .filter(id => id.startsWith('post_'))
        .sort((a, b) => {
          const numA = parseInt(a.replace('post_', ''), 10) || 0;
          const numB = parseInt(b.replace('post_', ''), 10) || 0;
          return numB - numA;
        });

      // ✅ CRITICAL FIX: For fresh user, normal = latest - 2 (not latest - 1)
      // Example: If latest is reel_10, read [reel_10, reel_9, reel_8]
      // So: latestReelSlotId = reel_10, normalReelSlotId = reel_8
      const latestReelSlot = reelIds.length > 0 ? reelIds[0] : 'reel_0';
      const normalReelSlot = reelIds.length > 2 ? reelIds[2] : (reelIds[0] || 'reel_0');  // ✅ FIXED: index 2, not 1

      const latestPostSlot = postIds.length > 0 ? postIds[0] : 'post_0';
      const normalPostSlot = postIds.length > 2 ? postIds[2] : (postIds[0] || 'post_0');  // ✅ FIXED: index 2, not 1

      const detectionDuration = Date.now() - detectionStart;

      console.log(`[post_algorithm] [AUTO-DETECT-SUCCESS] duration=${detectionDuration}ms | Found ${reelIds.length} reel docs, ${postIds.length} post docs`);
      console.log(`[post_algorithm] [AUTO-DETECT-SLOTS] latestReel=${latestReelSlot} | normalReel=${normalReelSlot} | latestPost=${latestPostSlot} | normalPost=${normalPostSlot}`);
      console.log(`[post_algorithm] [FRESH-USER-EXPLANATION] Will read slots: [${latestReelSlot}, ${reelIds[1] || 'N/A'}, ${normalReelSlot}]`);

const defaultStatus = {
  _id: userId,
  userId: userId,
  latestReelSlotId: latestReelSlot,
  normalReelSlotId: normalReelSlot,
  latestPostSlotId: latestPostSlot,
  normalPostSlotId: normalPostSlot,
  reel_0_visits: 0,  // ✅ ADD THIS
  post_0_visits: 0,  // ✅ ADD THIS
  createdAt: new Date(),
  updatedAt: new Date()
};

      await db.collection('user_status').insertOne(defaultStatus);

      const totalDuration = Date.now() - startTime;

      console.log(`[post_algorithm] [READ-1-CREATED] New user_status created | total_duration=${totalDuration}ms | reads=3`);

      return res.json({
        success: true,
        latestReelSlotId: latestReelSlot,
        normalReelSlotId: normalReelSlot,
        latestPostSlotId: latestPostSlot,
        normalPostSlotId: normalPostSlot,
        reads: 3,
        duration: totalDuration,
        isNewUser: true
      });
    }

  } catch (error) {
    console.error(`[post_algorithm] [READ-1-ERROR] ${error.message}`);
    return res.status(500).json({
      success: false,
      error: 'Failed to read user_status: ' + error.message,
      reads: 1,
      duration: Date.now() - startTime
    });
  }
});



app.post('/api/user-status/:userId', async (req, res) => {
  const startTime = Date.now();
  const { userId } = req.params;
  const { latestReelSlotId, normalReelSlotId, latestPostSlotId, normalPostSlotId } = req.body;

  try {

    if (!latestReelSlotId && !normalReelSlotId && !latestPostSlotId && !normalPostSlotId) {
      console.warn(`[post_algorithm] [UPDATE-USER-STATUS-SKIP] No slot IDs provided`);
      return res.status(400).json({
        success: false,
        error: 'At least one slot ID must be provided',
        received: { latestReelSlotId, normalReelSlotId, latestPostSlotId, normalPostSlotId }
      });
    }

    console.log(`[post_algorithm] [UPDATE-USER-STATUS] userId=${userId} | latestReel=${latestReelSlotId || 'not_provided'} | normalReel=${normalReelSlotId || 'not_provided'} | latestPost=${latestPostSlotId || 'not_provided'} | normalPost=${normalPostSlotId || 'not_provided'}`);

    const updateData = {
      userId: userId, // ✅ Always set userId
      updatedAt: new Date()
    };


    if (latestReelSlotId) updateData.latestReelSlotId = latestReelSlotId;
    if (normalReelSlotId) updateData.normalReelSlotId = normalReelSlotId;
    if (latestPostSlotId) updateData.latestPostSlotId = latestPostSlotId;
    if (normalPostSlotId) updateData.normalPostSlotId = normalPostSlotId;

    const result = await db.collection('user_status').updateOne(
      { _id: userId },
      {
        $set: updateData,
        $setOnInsert: { createdAt: new Date() }
      },
      { upsert: true }
    );

    const duration = Date.now() - startTime;

    console.log(`[post_algorithm] [UPDATE-SUCCESS] matched=${result.matchedCount} | modified=${result.modifiedCount} | upserted=${result.upsertedCount} | duration=${duration}ms | Updated fields: ${Object.keys(updateData).join(', ')}`);

    return res.json({
      success: true,
      message: 'Slot IDs updated',
      updated: updateData,
      writes: 1,
      duration
    });

  } catch (error) {
    console.error(`[post_algorithm] [UPDATE-ERROR] ${error.message}`);
    return res.status(500).json({
      success: false,
      error: 'Failed to update user_status: ' + error.message,
      writes: 0,
      duration: Date.now() - startTime
    });
  }
});



app.get('/api/contrib-check/:userId/:slotId/:type', async (req, res) => {
  const startTime = Date.now();
  const { userId, slotId, type } = req.params;

  if (!['posts', 'reels'].includes(type)) {
    return res.status(400).json({ success: false, error: 'Invalid type (must be posts or reels)' });
  }

  const collectionName = type === 'posts' ? 'contrib_posts' : 'contrib_reels';
  const readNum = req.headers['x-read-number'] || '?';

  try {
    console.log(`[post_algorithm] [READ-${readNum}-START] ${collectionName} lookup for userId=${userId} | slotId=${slotId}`);

    const contribDoc = await db.collection(collectionName).findOne(
      { 
        userId: userId,
        slotId: slotId
      },
      { projection: { ids: 1, slotId: 1, userId: 1 } }
    );

    const duration = Date.now() - startTime;

    if (contribDoc && contribDoc.ids) {
      const count = contribDoc.ids.length;

      console.log(`[post_algorithm] [READ-${readNum}-SUCCESS] ${collectionName} userId=${userId} slotId=${slotId} | found ${count} viewed IDs | duration=${duration}ms`);

      return res.json({
        success: true,
        slotId: slotId,
        userId: userId,
        ids: contribDoc.ids,
        count: count,
        reads: 1,
        duration
      });
    } else {
      console.log(`[post_algorithm] [READ-${readNum}-NOT-FOUND] ${collectionName} no contributions for userId=${userId} slotId=${slotId}`);

      return res.json({
        success: true,
        slotId: slotId,
        userId: userId,
        ids: [],
        count: 0,
        reads: 1,
        duration,
        isNewSlot: true
      });
    }

  } catch (error) {
    console.error(`[post_algorithm] [READ-${readNum}-ERROR] ${error.message}`);
    return res.status(500).json({
      success: false,
      error: `Failed to read ${collectionName}: ${error.message}`,
      reads: 1,
      duration: Date.now() - startTime
    });
  }
});



app.post('/api/feed/mixed-optimized', async (req, res) => {
  const startTime = Date.now();
  const { userId, limit = DEFAULT_CONTENT_BATCH_SIZE } = req.body;

  try {
    console.log(`[MIXED-FEED] START userId=${userId} | limit=${limit}`);

    const dbReadsBefore = dbOpCounters.reads;

    // ✅ Fetch reels and posts using strict algorithm (in parallel)
    const [reelsResult, postsResult] = await Promise.all([
      dbManager.getOptimizedFeedFixedReads(userId, 'reels', Math.ceil(limit * 0.6)),
      dbManager.getOptimizedFeedFixedReads(userId, 'posts', Math.ceil(limit * 0.4))
    ]);

    // Merge and sort by engagement
    const mixedContent = [...reelsResult.content, ...postsResult.content];
    
    mixedContent.sort((a, b) => {
      const retentionDiff = (b.retention || 0) - (a.retention || 0);
      if (Math.abs(retentionDiff) > 1) return retentionDiff;

      const likesDiff = (b.likeCount || 0) - (a.likeCount || 0);
      if (likesDiff !== 0) return likesDiff;

      return (b.commentCount || 0) - (a.commentCount || 0);
    });

    const dbReadsUsed = dbOpCounters.reads - dbReadsBefore;
    const duration = Date.now() - startTime;

    console.log(`[MIXED-FEED] COMPLETE: ${mixedContent.length} items (${reelsResult.content.length}R + ${postsResult.content.length}P) | ${duration}ms | Reads: ${dbReadsUsed}`);

    return res.json({
      success: true,
      content: mixedContent.slice(0, limit),
      metadata: {
        totalItems: mixedContent.length,
        reelsCount: reelsResult.content.length,
        postsCount: postsResult.content.length,
        dbReadsUsed,
        duration,
        readLimitCompliant: dbReadsUsed <= 14, 
        slotsRead: {
          reels: reelsResult.metadata?.slotsRead || [],
          posts: postsResult.metadata?.slotsRead || []
        }
      }
    });

  } catch (error) {
    console.error(`[MIXED-FEED] ERROR: ${error.message}`);
    return res.status(500).json({
      success: false,
      error: error.message,
      duration: Date.now() - startTime
    });
  }
});


function sendMixedResponse(res, mixedContent, startTime, phaseReached) {
const duration = Date.now() - startTime;


const sortedContent = mixedContent.sort((a, b) =>
(b.compositeScore || b.retention || 0) - (a.compositeScore || a.retention || 0)
);


const posts = sortedContent.filter(item => !item.isReel);
const reels = sortedContent.filter(item => item.isReel);

console.log(`[post_algorithm] [MIXED-FEED-RESPONSE] Total: ${sortedContent.length} | Posts: ${posts.length} | Reels: ${reels.length} | Phase: ${phaseReached} | Time: ${duration}ms`);

return res.json({
success: true,
content: sortedContent,
metadata: {
totalItems: sortedContent.length,
postsCount: posts.length,
reelsCount: reels.length,
phaseReached: phaseReached,
phasesChecked: ['latestReels', 'latestPosts', 'normalReels', 'normalPosts', 'previousSlots'].slice(0, phaseReached),
duration,
reads: dbOpCounters.reads
}
});
}


function calculatePreviousSlot(currentSlotId) {
const match = currentSlotId.match(/_(\d+)$/);
if (!match) return null;

const currentNum = parseInt(match[1]);
if (currentNum > 0) {
return currentSlotId.replace(/_\d+$/, `_${currentNum - 1}`);
}
return null;
}



app.post('/api/feed/optimized-posts', async (req, res) => {
const startTime = Date.now();
const { userId, slotId, excludedPostIds = [], limit = DEFAULT_CONTENT_BATCH_SIZE } = req.body;
const readNum = req.headers['x-read-number'] || '?';

try {
console.log(`[post_algorithm] [READ-${readNum}-START] posts content from slotId=${slotId} | excluding=${excludedPostIds.length} ids | limit=${limit}`);

if (!userId || !slotId) {
return res.status(400).json({
success: false,
error: 'userId and slotId are required'
});
}


let userInterests = [];
try {
const userResponse = await axios.get(`https://server1-ki1x.onrender.com/api/users/${userId}`, { timeout: 1000, validateStatus: s => s >= 200 && s < 500 });
if (userResponse.status === 200 && userResponse.data && userResponse.data.success && userResponse.data.user) {
userInterests = userResponse.data.user.interests || [];
}
} catch (e) {
console.warn(`[post_algorithm] [INTERESTS-SKIP] ${e && e.message ? e.message : e}`);
}

// Aggregation pipeline for optimized posts (includes multi-image fields)
const pipeline = [
{ $match: { _id: slotId, 'postList': { $exists: true, $ne: [] } } },
{ $unwind: '$postList' },
{ $match: { 'postList.postId': { $nin: excludedPostIds } } },
{
$addFields: {
likeCountNum: { $toInt: { $ifNull: ['$postList.likeCount', 0] } },
commentCountNum: { $toInt: { $ifNull: ['$postList.commentCount', 0] } },

interestScore: {
$cond: {
if: { $in: ['$postList.category', userInterests] },
then: 100,
else: { $cond: [{ $eq: [{ $size: { $ifNull: [userInterests, []] } }, 0] }, 50, 0] }
}
},
retentionNum: { $toDouble: { $ifNull: ['$postList.retention', 0] } },
categoryField: { $ifNull: ['$postList.category', ""] }
}
},
{
$addFields: {
compositeScore: {
$add: [
{ $multiply: ['$likeCountNum', 0.50] },
{ $multiply: ['$interestScore', 0.30] },
{ $multiply: ['$commentCountNum', 0.20] }
]
}
}
},
{ $sort: { compositeScore: -1 } },
{ $limit: parseInt(limit, 10) || DEFAULT_CONTENT_BATCH_SIZE },
{
$project: {
postId: '$postList.postId',
userId: '$postList.userId',
username: '$postList.username',

imageUrl: { $ifNull: ['$postList.imageUrl', '$postList.imageUrl1'] },

multiple_posts: { $ifNull: ['$postList.multiple_posts', false] },
media_count: { $ifNull: ['$postList.media_count', 1] },
imageUrl1: { $ifNull: ['$postList.imageUrl1', '$postList.imageUrl'] },
imageUrl2: '$postList.imageUrl2',
imageUrl3: '$postList.imageUrl3',
imageUrl4: '$postList.imageUrl4',
imageUrl5: '$postList.imageUrl5',
imageUrl6: '$postList.imageUrl6',
imageUrl7: '$postList.imageUrl7',
imageUrl8: '$postList.imageUrl8',
imageUrl9: '$postList.imageUrl9',
imageUrl10: '$postList.imageUrl10',
imageUrl11: '$postList.imageUrl11',
imageUrl12: '$postList.imageUrl12',
imageUrl13: '$postList.imageUrl13',
imageUrl14: '$postList.imageUrl14',
imageUrl15: '$postList.imageUrl15',
imageUrl16: '$postList.imageUrl16',
imageUrl17: '$postList.imageUrl17',
imageUrl18: '$postList.imageUrl18',
imageUrl19: '$postList.imageUrl19',
imageUrl20: '$postList.imageUrl20',
profilePicUrl: '$postList.profile_picture_url',
caption: '$postList.caption',
category: '$categoryField',
timestamp: '$postList.timestamp',
likeCount: '$likeCountNum',
commentCount: '$commentCountNum',
retention: '$retentionNum',
interestScore: '$interestScore',
compositeScore: '$compositeScore',
sourceDocument: slotId,
ratio: '$postList.ratio',
isReel: { $literal: false }
}
}
];

const posts = await db.collection('posts').aggregate(pipeline, { maxTimeMS: 3000 }).toArray();

const duration = Date.now() - startTime;
console.log(`[post_algorithm] [READ-${readNum}-SUCCESS] ✅ Fetched ${posts.length} posts from slotId=${slotId} | duration=${duration}ms`);

return res.json({
success: true,
content: posts,
slotUsed: slotId,
reads: 1,
duration
});

} catch (error) {
console.error(`[post_algorithm] [READ-${readNum}-ERROR] ${error && error.message ? error.message : error}`);
return res.status(500).json({
success: false,
error: 'Failed to fetch posts: ' + (error && error.message ? error.message : String(error)),
reads: 1,
duration: Date.now() - startTime
});
}
});



app.use((req, res) => {
  console.warn(`[404-NOT-FOUND] ${req.method} ${req.originalUrl} - No route handler exists`);
  
  res.status(404).json({
    success: false,
    error: 'Endpoint not found',
    requestedPath: req.originalUrl,
    method: req.method,
    message: 'This endpoint does not exist on this server'
  });
});

// Error handler (keep your existing one below this)
app.use((err, req, res, next) => {
  console.error('[UNHANDLED-ERROR]', err);
  res.status(500).json({ error: 'Internal server error' });
});


// Error handler
app.use((err, req, res, next) => {
console.error('[UNHANDLED-ERROR]', err);
res.status(500).json({ error: 'Internal server error' });
});

// Graceful shutdown
for (const sig of ['SIGINT','SIGTERM']) {
process.on(sig, async () => {
console.log('[SHUTDOWN] Shutting down gracefully...');
try {
if (client) { await client.close(); console.log('[SHUTDOWN] MongoDB closed'); }
} catch (e) { console.warn('[SHUTDOWN-ERROR]', e); }

console.log(`[SHUTDOWN] Final counts - R:${dbOpCounters.reads} W:${dbOpCounters.writes} Q:${dbOpCounters.queries}`);
process.exit(0);
});
}

app.listen(PORT, '0.0.0.0', () => {
console.log(`[SERVER-LISTENING] Port ${PORT}, DB: ${DB_NAME}, Optimizations: ENABLED`);
})
