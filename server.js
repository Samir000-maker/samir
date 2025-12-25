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
const activeRequests = new Map();
const app = express();
const PORT = process.env.PORT || 7000;
const MONGODB_URI = process.env.MONGODB_URI || 'mongodb+srv://samir_:fitara@cluster0.cmatn6k.mongodb.net/appdb?retryWrites=true&w=majority';
const DB_NAME = process.env.DB_NAME || 'appdb';
const axios = require('axios');
const Redis = require('ioredis');
const activeRequestsWithTimestamp = new Map();
const requestDeduplication = new Map();
const DEDUP_WINDOW = 5000; // 5 seconds

const RESULT_CACHE_TTL = 30000; // 30 seconds

const crypto = require('crypto');

const CACHE_TTL_SHORT = 15000; // 15 seconds
const CACHE_TTL_MEDIUM = 60000; // 1 minute
const CACHE_TTL_LONG = 300000; // 5 minutes

// ===== CONFIGURATION VARIABLES - MODIFY THESE TO CHANGE SYSTEM BEHAVIOR =====
const MAX_CONTENT_PER_SLOT = 40; // Maximum content items per document before creating new slot
const DEFAULT_CONTENT_BATCH_SIZE = 40; // Default number of items to return per request
const MIN_CONTENT_FOR_FEED = 40; // Minimum content required for feed requests
// ============================================================================

const MAX_ACTIVE_REQUESTS = 5000;  // Hard limit
const REQUEST_DEDUP_TTL = 5000;    // 5 seconds

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


console.log('[CONFIG] System Configuration:');
console.log(` MAX_CONTENT_PER_SLOT: ${MAX_CONTENT_PER_SLOT} (500 posts per slot)`);
console.log(` DEFAULT_CONTENT_BATCH_SIZE: ${DEFAULT_CONTENT_BATCH_SIZE} (40 items per request)`);
console.log(` MIN_CONTENT_FOR_FEED: ${MIN_CONTENT_FOR_FEED} (40 items minimum)`);
console.log(` DOCUMENT REDUCTION: 12.5x fewer documents vs 40-item slots`);




console.log('[CONFIG] System Configuration:');
console.log(` MAX_CONTENT_PER_SLOT: ${MAX_CONTENT_PER_SLOT}`);
console.log(` DEFAULT_CONTENT_BATCH_SIZE: ${DEFAULT_CONTENT_BATCH_SIZE}`);
console.log(` MIN_CONTENT_FOR_FEED: ${MIN_CONTENT_FOR_FEED}`);




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


const getOrCreateRequest = (key, requestFactory) => {
    const now = Date.now();

    // ✅ FIXED: Efficient cleanup - only check if over threshold
    if (activeRequestsWithTimestamp.size > MAX_ACTIVE_REQUESTS) {
        console.warn(`[REQUEST-DEDUP] Map size: ${activeRequestsWithTimestamp.size} - forcing cleanup`);
        
        // ✅ Use Array.from + filter for faster cleanup
        const expiredKeys = [];
        for (const [reqKey, reqData] of activeRequestsWithTimestamp.entries()) {
            if (now - reqData.timestamp > REQUEST_DEDUP_TTL) {
                expiredKeys.push(reqKey);
            }
            // ✅ Early exit if we've found enough to clean
            if (expiredKeys.length > 1000) break;
        }
        
        expiredKeys.forEach(k => activeRequestsWithTimestamp.delete(k));
        console.log(`[REQUEST-DEDUP-CLEANUP] Removed ${expiredKeys.length} expired entries`);
        
        // ✅ CRITICAL: If still over limit, remove oldest entries
        if (activeRequestsWithTimestamp.size > MAX_ACTIVE_REQUESTS) {
            const sortedEntries = Array.from(activeRequestsWithTimestamp.entries())
                .sort((a, b) => a[1].timestamp - b[1].timestamp);
            
            const toRemove = sortedEntries.slice(0, 1000);
            toRemove.forEach(([k]) => activeRequestsWithTimestamp.delete(k));
            
            console.warn(`[REQUEST-DEDUP-EVICTION] Evicted ${toRemove.length} oldest entries`);
        }
    }

    // Check if request already in-flight
    if (activeRequestsWithTimestamp.has(key)) {
        console.log(`[REQUEST-DEDUP-HIT] ${key.substring(0, 50)}`);
        return activeRequestsWithTimestamp.get(key).promise;
    }

    // Create new request
    const promise = requestFactory().finally(() => {
        activeRequestsWithTimestamp.delete(key);
    });

    activeRequestsWithTimestamp.set(key, {
        promise,
        timestamp: now
    });

    return promise;
};



// ✅ Alternative: If excluded IDs change frequently, use user slot-based caching
function generateSlotBasedCacheKey(userId, userStatus) {
    // Cache based on current slots - invalidates when user moves to new slot
    const reelSlot = userStatus?.latestReelSlotId || 'reel_0';
    const postSlot = userStatus?.latestPostSlotId || 'post_0';
    
    return `feed:${userId}:${reelSlot}:${postSlot}`;
}




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

// Middleware
app.use(cors());
app.use(express.json({ limit: '50mb' }));
app.use((req, res, next) => { console.log(`[HTTP] ${new Date().toISOString()} ${req.method} ${req.originalUrl}`); next(); });
app.use('/api', rateLimit({ windowMs: 1000, max: 1000, standardHeaders: true, legacyHeaders: false }));

app.use(express.json());


app.use(async (req, res, next) => {
    if (req.method !== 'POST' && req.method !== 'GET') return next();
    
    const requestKey = `req:${req.method}:${req.originalUrl}:${req.body ? JSON.stringify(req.body) : ''}`;
    
    if (redisClient) {
        try {
            // Check if identical request is in-flight
            const inFlight = await redisClient.get(requestKey);
            if (inFlight) {
                console.log(`[REQUEST-DEDUP-BLOCKED] ${requestKey.substring(0, 50)}`);
                return res.status(429).json({ 
                    success: false, 
                    error: 'Duplicate request in progress',
                    retryAfter: 3 
                });
            }
            
            // Mark request as in-flight
            await redisClient.setex(requestKey, Math.floor(REQUEST_DEDUP_TTL / 1000), '1');
            
            // Clear on response
            res.on('finish', () => {
                redisClient.del(requestKey).catch(() => {});
            });
        } catch (err) {
            console.error('[DEDUP-ERROR]', err);
        }
    }
    
    next();
});


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


async function createUltraFastRetentionIndexes() {
try {
// Remove the unique constraint on _id index (it's automatically unique)
// Just ensure the collection exists
await db.collection('user_interaction_cache').findOne({});

// Compound index for retention queries
await db.collection('user_interaction_cache').createIndex({
"userId": 1,
"retentionContributed": 1
});

// TTL index for automatic cleanup
await db.collection('user_interaction_cache').createIndex({
"ttl": 1
}, { expireAfterSeconds: 0 });

console.log('[ULTRA-FAST-RETENTION-INDEXES] Created successfully');
} catch (error) {
console.error('[INDEX-CREATION-ERROR]', error);
}
}




async function createProductionIndexes() {
try {
const indexes = [
// CRITICAL: Slot allocation optimization (prevents full collection scan)
{
collection: 'posts',
index: { count: 1, index: -1 },
options: { name: 'slot_allocation_posts', background: true }
},
{
collection: 'reels',
index: { count: 1, index: -1 },
options: { name: 'slot_allocation_reels', background: true }
},

// CRITICAL: Exact postId lookup (prevents array scans)
{
collection: 'posts',
index: { 'postList.postId': 1 },
options: { name: 'postList_postId_lookup', background: true, sparse: true }
},
{
collection: 'reels',
index: { 'reelsList.postId': 1 },
options: { name: 'reelsList_postId_lookup', background: true, sparse: true }
},

// Instagram feed optimization - compound indexes
{
collection: 'posts',
index: { 'postList.userId': 1, 'postList.timestamp': -1, 'postList.postId': 1 },
options: { name: 'feed_following_posts', background: true }
},
{
collection: 'reels',
index: { 'reelsList.userId': 1, 'reelsList.timestamp': -1, 'reelsList.postId': 1 },
options: { name: 'feed_following_reels', background: true }
},

// Global ranking indexes
{
collection: 'posts',
index: { index: -1, 'postList.retention': -1, 'postList.postId': 1 },
options: { name: 'feed_global_posts_ranked', background: true }
},
{
collection: 'reels',
index: { index: -1, 'reelsList.retention': -1, 'reelsList.postId': 1 },
options: { name: 'feed_global_reels_ranked', background: true }
},

// Like checking optimization - covering index
{
collection: 'contributionToLike',
index: { userId: 1, postId: 1 },
options: { name: 'like_check_covering', background: true }
},

// Contributed views optimization
{
collection: 'contrib_posts',
index: { userId: 1, ids: 1 },
options: { name: 'contrib_posts_lookup', background: true }
},
{
collection: 'contrib_reels',
index: { userId: 1, ids: 1 },
options: { name: 'contrib_reels_lookup', background: true }
},

// Retention check optimization
{
collection: 'user_interaction_cache',
index: { userId: 1, retentionContributed: 1 },
options: { name: 'retention_check_fast', background: true }
}
];

let successCount = 0;
let skipCount = 0;
let errorCount = 0;

for (const { collection, index, options } of indexes) {
try {
await db.collection(collection).createIndex(index, options);
successCount++;
console.log(`[INDEX-CREATED] ${collection}.${options.name}`);
} catch (err) {
// Silently skip "already exists" and compression warnings
if (err.code === 85 || err.code === 86 || err.message?.includes('snappy') || err.message?.includes('already exists')) {
skipCount++;
} else {
errorCount++;
console.log(`[INDEX-ERROR] ${collection}.${options.name}: ${err.message}`);
}
}
}

console.log(`[PROD-INDEXES] ✅ ${successCount} created, ${skipCount} already existed, ${errorCount} errors`);
} catch (error) {
log('error', '[PROD-INDEX-ERROR]', error.message);
}
}




async function createAggregationIndexes() {
try {
const aggIndexes = [
// CRITICAL: Covering indexes for aggregation pipelines
{
collection: 'posts',
index: {
'postList.postId': 1,
'postList.userId': 1,
'postList.retention': -1,
'postList.likeCount': -1,
'postList.timestamp': -1
},
options: { name: 'aggregation_posts_covering', background: true }
},
{
collection: 'reels',
index: {
'reelsList.postId': 1,
'reelsList.userId': 1,
'reelsList.retention': -1,
'reelsList.likeCount': -1,
'reelsList.timestamp': -1
},
options: { name: 'aggregation_reels_covering', background: true }
}
];

for (const { collection, index, options } of aggIndexes) {
try {
await db.collection(collection).createIndex(index, options);
log('info', `[AGG-INDEX] Created ${options.name}`);
} catch (err) {
if (err.code !== 85 && err.code !== 86) {
log('warn', `[AGG-INDEX-WARN] ${collection}: ${err.message}`);
}
}
}
} catch (error) {
log('error', '[AGG-INDEX-ERROR]', error.message);
}
}







// MongoDB initialization
async function initMongo() {
    console.log('[MONGO-INIT] Starting connection...');

    process.removeAllListeners('warning');
    process.on('warning', (warning) => {
        if (!warning.message?.includes('snappy') &&
            !warning.message?.includes('kerberos')) {
            console.warn('[NODE-WARNING]', warning.message);
        }
    });

    const MONGODB_URI = process.env.MONGODB_URI || 'mongodb+srv://samir_:fitara@cluster0.cmatn6k.mongodb.net/appdb?retryWrites=true&w=majority';

    console.log(`[MONGO-INIT] Connecting to MongoDB Atlas`);

    const compressors = ['zlib'];
    try {
        require.resolve('snappy');
        compressors.unshift('snappy');
        console.log('[MONGO-INIT] ✅ Snappy compression available');
    } catch (e) {
        console.log('[MONGO-INIT] ⚠️ Snappy not available, using zlib');
    }

    // ✅ CRITICAL FIX: Production-ready connection pool configuration
    client = new MongoClient(MONGODB_URI, {
        // ✅ FIXED: Increase pool size to handle high concurrency
        maxPoolSize: 200,           // Was 50 → Now 200 (supports 20,000+ req/sec)
        minPoolSize: 20,            // Was 5 → Now 20 (keep warm connections)
        
        // ✅ FIXED: Increase idle timeout to prevent connection churn
        maxIdleTimeMS: 120000,      // Was 30s → Now 2 minutes
        
        // Timeouts optimized for fast queries
        serverSelectionTimeoutMS: 5000,
        socketTimeoutMS: 45000,     // Increased from 30s for large aggregations
        connectTimeoutMS: 10000,
        
        // Connection strategy
        readPreference: 'nearest',
        retryWrites: true,
        retryReads: true,
        waitQueueTimeoutMS: 10000,  // Increased from 5s to handle bursts
        directConnection: false,
        
        // Compression
        compressors: compressors,
        
        // ✅ FIXED: Increase maxConnecting to prevent queue buildup
        maxConnecting: 10,          // Was 2 → Now 10
        
        // Write concern optimized for speed
        w: 1,
        journal: false,
        
        // ✅ NEW: Add heartbeat configuration for better connection health
        heartbeatFrequencyMS: 10000,  // Check connection health every 10s
        
        // Disable command monitoring in production (performance)
        monitorCommands: false,
        autoEncryption: undefined
    });

    // ✅ CRITICAL: Enhanced connection pool monitoring
    let poolWarningThreshold = 160; // Warn at 80% capacity (160/200)
    
    client.on('connectionPoolCreated', () => {
        console.log('[MONGO-POOL] Pool created | maxSize=200 | minSize=20');
    });

    client.on('connectionPoolReady', () => {
        console.log('[MONGO-POOL] ✅ Pool ready');
    });

    client.on('connectionCheckOutStarted', () => {
        const poolSize = client.topology?.s?.pool?.totalConnectionCount || 0;
        const availableConnections = client.topology?.s?.pool?.availableConnectionCount || 0;
        
        if (poolSize > poolWarningThreshold) {
            console.warn(`[MONGO-POOL-WARNING] ⚠️ High pool usage: ${poolSize}/200 connections | Available: ${availableConnections}`);
        }
    });

    // ✅ NEW: Monitor connection pool cleared events
    client.on('connectionPoolCleared', () => {
        console.warn('[MONGO-POOL] ⚠️ Pool cleared - connections will be recreated');
    });

    // ✅ NEW: Monitor checkout failures
    client.on('connectionCheckOutFailed', (event) => {
        console.error('[MONGO-POOL-ERROR] ❌ Connection checkout failed:', event.reason);
    });

    console.log(`[MONGO-INIT] Compression: ${compressors.join(', ')}`);
    console.log('[MONGO-INIT] Connecting with optimized pool settings...');

    await client.connect();
    db = client.db(DB_NAME);
    console.log(`[MONGO-INIT] ✅ Connected to ${DB_NAME}`);

    // Continue with index creation...
    await initializeSlots();
    await ensurePostIdUniqueness();

// CRITICAL: Enable sharding for collections (run once during setup)
if (process.env.ENABLE_SHARDING === 'true') {
await enableSharding();
}


await createInstagramFeedIndexes();


// Add after initMongo() in PORT 2000
// setInterval(async () => {
// try {
// console.log('[PERIODIC-SYNC] Running background metrics sync check');

// // Get all posts/reels that haven't been synced in the last 5 minutes
// const fiveMinutesAgo = new Date(Date.now() - 5 * 60 * 1000);

// const collections = ['posts', 'reels'];
// for (const collection of collections) {
// const arrayField = collection === 'reels' ? 'reelsList' : 'postList';
// const docs = await db.collection(collection).find({
// [`${arrayField}.lastSynced`]: { $lt: fiveMinutesAgo.toISOString() }
// }).limit(50).toArray();

// console.log(`[PERIODIC-SYNC] Found ${docs.length} outdated ${collection}`);
// }
// } catch (error) {
// console.error('[PERIODIC-SYNC-ERROR]', error);
// }
// }, 5 * 60 * 1000); // Every 5 minutes


await createUltraFastRetentionIndexes();



// Inside initMongo() after existing indexes:
const contributionLikeIndexes = [
    { 
        collection: 'contributionToLike', 
        index: { userId: 1, postId: 1 }, 
        options: { unique: true, background: true } 
    },
    { 
        collection: 'contributionToLike', 
        index: { postId: 1 }, 
        options: { background: true } 
    },
    { 
        collection: 'contributionToLike', 
        index: { userId: 1 }, 
        options: { background: true } 
    }
];

for (const { collection, index, options } of contributionLikeIndexes) {
    try {
        await db.collection(collection).createIndex(index, options);
        log('info', `Created index for ${collection}`);
    } catch (e) {
        log('warn', `Index error for ${collection}: ${e.message}`);
    }
}
    
await createRetentionOptimizedIndexes();






// ADD to initMongo() function after existing index creation

async function createContribCollectionIndexes() {
    try {
        console.log('[CONTRIB-INDEXES] Creating optimized contrib indexes...');

        const contribIndexes = [
            // ✅ Index for finding all contrib docs for a user
            {
                collection: 'contrib_posts',
                index: { userId: 1 },
                options: { name: 'user_contrib_posts', background: true }
            },
            {
                collection: 'contrib_reels',
                index: { userId: 1 },
                options: { name: 'user_contrib_reels', background: true }
            },
            
            // ✅ Index for finding contrib doc by slot
            {
                collection: 'contrib_posts',
                index: { slotId: 1 },
                options: { name: 'slot_contrib_posts', background: true, sparse: true }
            },
            {
                collection: 'contrib_reels',
                index: { slotId: 1 },
                options: { name: 'slot_contrib_reels', background: true, sparse: true }
            },
            
            // ✅ Compound index for userId + slotId queries
            {
                collection: 'contrib_posts',
                index: { userId: 1, slotId: 1 },
                options: { name: 'user_slot_contrib_posts', background: true }
            },
            {
                collection: 'contrib_reels',
                index: { userId: 1, slotId: 1 },
                options: { name: 'user_slot_contrib_reels', background: true }
            },
            
            // ✅ Index for checking if specific postId is in ids array
            {
                collection: 'contrib_posts',
                index: { 'ids': 1 },
                options: { name: 'postids_lookup', background: true }
            },
            {
                collection: 'contrib_reels',
                index: { 'ids': 1 },
                options: { name: 'reelids_lookup', background: true }
            }
        ];

        let created = 0;
        for (const { collection, index, options } of contribIndexes) {
            try {
                await db.collection(collection).createIndex(index, options);
                created++;
                console.log(`[CONTRIB-INDEX] ✅ ${options.name}`);
            } catch (err) {
                if (err.code !== 85 && err.code !== 86) {
                    console.warn(`[CONTRIB-INDEX-WARN] ${collection}: ${err.message}`);
                }
            }
        }

        console.log(`[CONTRIB-INDEXES] ✅ Created ${created}/${contribIndexes.length} indexes`);

    } catch (error) {
        console.error('[CONTRIB-INDEX-ERROR]', error.message);
    }
}

// Call in initMongo() after line ~655
await createContribCollectionIndexes();



async function createRetentionOptimizedIndexes() {
    try {
        console.log('[RETENTION-INDEXES] Creating optimized indexes...');

        // ✅ CRITICAL FIX: Create covering index for retention check
        // Query pattern: { _id: "userId_session_date", retentionContributed: "postId" }
        await db.collection('user_interaction_cache').createIndex(
            { _id: 1, retentionContributed: 1 },
            { 
                name: 'retention_check_covering',
                background: true,
                // ✅ COVERING INDEX: Query can be answered entirely from index
                // No document access needed
            }
        );

        // ✅ Also create index for array membership queries
        await db.collection('user_interaction_cache').createIndex(
            { retentionContributed: 1 },
            { 
                name: 'retention_array_lookup',
                background: true,
                sparse: true  // Only index documents with this field
            }
        );

        console.log('[RETENTION-INDEXES] ✅ Optimized indexes created');

    } catch (error) {
        if (error.code !== 85 && error.code !== 86) {
            console.error('[RETENTION-INDEX-ERROR]', error.message);
        }
    }
}


// ADD to initMongo() function
async function createProductionSpeedIndexes() {
try {
console.log('[SPEED-INDEXES] Creating Instagram-speed indexes...');

const speedIndexes = [
// CRITICAL: Covering index for feed query (no collection scan)
{
collection: 'posts',
index: {
'postList.userId': 1,
'postList.retention': -1,
'postList.likeCount': -1,
'postList.postId': 1
},
options: { name: 'feed_speed_posts', background: true }
},
{
collection: 'reels',
index: {
'reelsList.userId': 1,
'reelsList.retention': -1,
'reelsList.likeCount': -1,
'reelsList.postId': 1
},
options: { name: 'feed_speed_reels', background: true }
},

// CRITICAL: Exclude viewed content instantly
{
collection: 'posts',
index: { 'postList.postId': 1 },
options: { name: 'exclude_viewed_posts', background: true }
},
{
collection: 'reels',
index: { 'reelsList.postId': 1 },
options: { name: 'exclude_viewed_reels', background: true }
}
];

for (const { collection, index, options } of speedIndexes) {
try {
await db.collection(collection).createIndex(index, options);
console.log(`[SPEED-INDEX] ✅ ${options.name}`);
} catch (err) {
if (err.code !== 85 && err.code !== 86) {
console.warn(`[SPEED-INDEX-WARN] ${collection}: ${err.message}`);
}
}
}

console.log('[SPEED-INDEXES] ✅ All Instagram-speed indexes ready');
} catch (error) {
console.error('[SPEED-INDEX-ERROR]', error.message);
}
}

// Call in initMongo()
await createProductionSpeedIndexes();


// Add to your existing initMongo() function
async function createInteractionIndexes() {
// Compound indexes for ultra-fast lookups
await db.collection('user_interaction_cache').createIndex({
"userId": 1,
"sessionDate": 1
}, { unique: true });

// Fast lookup for specific user-reel combinations
await db.collection('user_reel_interactions').createIndex({
"userId": 1,
"date": 1
});

// Index for retention contributed lookup
await db.collection('user_reel_interactions').createIndex({
"userId": 1,
"viewedReels.postId": 1,
"viewedReels.retentionContributed": 1
});

console.log('[INTERACTION-INDEXES] Created optimized interaction indexes');
}


// Ensure indexes
const ensureIndex = async (col, spec, opts = {}) => {
try {
const coll = db.collection(col);
const existing = await coll.indexes().catch(() => []);
const found = existing.some(i => JSON.stringify(i.key) === JSON.stringify(spec));
if (!found) {
await coll.createIndex(spec, opts);
// Only log successful creation, not re-creation
}
} catch (e) {
// Suppress compression warnings and "already exists" errors
if (!e.message?.includes('snappy') && e.code !== 85 && e.code !== 86) {
console.warn(`[INDEX-WARN] ${col}:`, e.message);
}
}
};

await Promise.all([
ensureIndex('posts', { index: 1 }),
ensureIndex('reels', { index: 1 }),
ensureIndex('user_posts', { userId: 1, postId: 1 }, { unique: true, sparse: true }),
ensureIndex('user_status', { _id: 1 }, { unique: true }),
ensureIndex('contrib_posts', { userId: 1 }),
ensureIndex('contrib_reels', { userId: 1 }),
]);
console.log('[MONGO-INIT] All indexes ensured')





// Add these indexes for ultra-fast ranking queries
const rankingIndexes = [
{
collection: 'user_slots',
index: { 'postList.retention': -1, 'postList.likeCount': -1 },
options: { background: true }
},
{
collection: 'user_slots',
index: { 'reelsList.retention': -1, 'reelsList.likeCount': -1 },
options: { background: true }
}
];

for (const { collection, index, options } of rankingIndexes) {
try {
await db.collection(collection).createIndex(index, options);
//log('info', `Created ranking index for ${collection}`);
} catch (e) {
// log('warn', `Ranking index error for ${collection}: ${e.message}`);
}
}

// Add this line inside initMongo() after existing index creation
await createRankingIndexes();
await createProductionIndexes();

await createAggregationIndexes();

// Add this inside initMongo() function after other index creation
async function createPrecisionIndexes() {
try {
// CRITICAL: Compound indexes for EXACT document lookup
await db.collection('posts').createIndex(
{ 'postList.userId': 1, 'postList.postId': 1 },
{ name: 'exact_post_lookup', background: true }
);

await db.collection('reels').createIndex(
{ 'reelsList.userId': 1, 'reelsList.postId': 1 },
{ name: 'exact_reel_lookup', background: true }
);

// Index for finding ONLY documents with unviewed content
await db.collection('posts').createIndex(
{ 'postList.postId': 1 },
{ name: 'postId_direct_lookup', background: true }
);

await db.collection('reels').createIndex(
{ 'reelsList.postId': 1 },
{ name: 'reelId_direct_lookup', background: true }
);

log('info', '[PRECISION-INDEXES] Created exact lookup indexes');
} catch (error) {
log('error', '[PRECISION-INDEX-ERROR]', error.message);
}
}

// Call it in initMongo()
await createPrecisionIndexes();





// Ultra-fast personalized reels indexes
async function createPersonalizedReelsIndexes() {
try {
const personalizedIndexes = [
// Compound index for retention + category + engagement
{
collection: 'reels',
index: {
'reelsList.retention': -1,
'reelsList.category': 1,
'reelsList.likeCount': -1,
'reelsList.postId': 1
},
options: { name: 'personalized_reels_ranking', background: true }
},
// Fast postId lookup for exclusion
{
collection: 'reels',
index: { 'reelsList.postId': 1 },
options: { name: 'reels_postId_lookup', background: true }
}
];

for (const { collection, index, options } of personalizedIndexes) {
try {
await db.collection(collection).createIndex(index, options);
log('info', `[PERSONALIZED-INDEX] Created ${options.name}`);
} catch (err) {
if (err.code !== 85 && err.code !== 86) {
log('warn', `[INDEX-WARN] ${collection}: ${err.message}`);
}
}
}
} catch (error) {
log('error', '[PERSONALIZED-INDEX-ERROR]', error.message);
}
}

// Call it after existing index creation
await createPersonalizedReelsIndexes();



setInterval(() => {
    const now = Date.now();
    const expiredKeys = [];
    
    for (const [key, data] of activeRequestsWithTimestamp.entries()) {
        if (now - data.timestamp > REQUEST_DEDUP_TTL * 2) {  // 10 seconds
            expiredKeys.push(key);
        }
    }
    
    if (expiredKeys.length > 0) {
        expiredKeys.forEach(k => activeRequestsWithTimestamp.delete(k));
        console.log(`[REQUEST-DEDUP-BG-CLEANUP] Removed ${expiredKeys.length} stale entries | Active: ${activeRequestsWithTimestamp.size}`);
    }
}, 30000);  // Every 30 seconds


  setInterval(() => {
        if (client.topology?.s?.pool) {
            const pool = client.topology.s.pool;
            const total = pool.totalConnectionCount || 0;
            const available = pool.availableConnectionCount || 0;
            const pending = pool.pendingConnectionCount || 0;
            
            console.log(`[MONGO-POOL-HEALTH] Total: ${total}/200 | Available: ${available} | Pending: ${pending}`);
            
            // Alert if pool is consistently near capacity
            if (total > 180) {
                console.error(`[MONGO-POOL-CRITICAL] 🚨 Pool near capacity: ${total}/200 - Consider scaling!`);
            }
        }
    }, 60000); // Every 1 minute


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
    const listKey = isReel ? 'reelsList' : 'postList';
    
    // ✅ CRITICAL: Cache user status + content in single key
    const cacheKey = `feedData:${userId}:${contentType}`;
    const cached = await getCache(cacheKey);
    
    if (cached) {
        console.log(`[FEED-CACHE-HIT] ✅ ${contentType} for ${userId} (0 DB reads)`);
        return cached;
    }
    
    console.log(`[FEED-START] ${contentType} | userId=${userId} | min=${minContentRequired}`);
    
    // ✅ SINGLE AGGREGATION combining user_status lookup + content fetch
    const pipeline = [
        {
            $facet: {
                userStatus: [
                    { $limit: 1 },
                    {
                        $lookup: {
                            from: 'user_status',
                            pipeline: [{ $match: { _id: userId } }],
                            as: 'status'
                        }
                    }
                ],
                contentSlots: [
                    { $match: { count: { $gt: 0 } } },
                    { $sort: { index: -1 } },
                    { $limit: 2 }, // ✅ Only fetch 2 latest slots
                    {
                        $lookup: {
                            from: `contrib_${collection}`,
                            let: { slotContent: `$${listKey}` },
                            pipeline: [
                                { $match: { userId: userId } },
                                { $limit: 1 }, // ✅ Only need existence check
                                { $project: { ids: 1, _id: 0 } }
                            ],
                            as: 'viewedData'
                        }
                    },
                    { $limit: minContentRequired * 2 }, // ✅ Limit early
                    {
                        $project: {
                            _id: 1,
                            index: 1,
                            count: 1,
                            [listKey]: {
                                $slice: [`$${listKey}`, minContentRequired * 2] // ✅ Slice array
                            },
                            viewedIds: { $ifNull: [{ $arrayElemAt: ['$viewedData.ids', 0] }, []] }
                        }
                    }
                ]
            }
        }
    ];
    
    const aggregateStart = Date.now();
    const results = await this.db.collection(collection).aggregate(pipeline, { maxTimeMS: 2000 }).toArray();
    logDbOp('aggregate', collection, { pipeline: 'fixed_reads_cached' }, results, Date.now() - aggregateStart);
    
    let userStatus = null;
    let contentSlots = [];
    
    if (results.length > 0) {
        const facetResult = results[0];
        if (facetResult.userStatus?.[0]?.status?.[0]) {
            userStatus = facetResult.userStatus[0].status[0];
        }
        contentSlots = facetResult.contentSlots || [];
    }
    
    const viewedIds = new Set(contentSlots[0]?.viewedIds || []);
    let content = [];
    
    // Process content
    for (const slot of contentSlots) {
        const slotContent = slot[listKey] || [];
        for (const item of slotContent) {
            if (content.length >= minContentRequired) break;
            if (item.postId && !viewedIds.has(item.postId)) {
                content.push(item);
            }
        }
        if (content.length >= minContentRequired) break;
    }
    
    const result = {
        content: content.slice(0, minContentRequired),
        latestDocumentId: contentSlots[0]?._id || null,
        normalDocumentId: contentSlots[1]?._id || contentSlots[0]?._id || null,
        isNewUser: !userStatus
    };
    
    // ✅ Cache result for 30 seconds
    await setCache(cacheKey, result, 30000);
    
    console.log(`[FEED-COMPLETE] ${contentType} | ${result.content.length} items | ${Date.now() - start}ms | DB reads: ${dbOpCounters.reads}`);
    
    return result;
}





async getUserStatus(userId) {
const cacheKey = `user_status_${userId}`;
const cached = await getCache(cacheKey);
if (cached) {
console.log(`[CACHE-HIT] user_status for ${userId} | Source: Redis/Memory`);
return cached;
}

const readsBefore = dbOpCounters.reads;
const start = Date.now();

const statusDoc = await this.db.collection('user_status').findOne({ _id: userId });

const readsUsed = dbOpCounters.reads - readsBefore;
const time = Date.now() - start;

logDbOp('findOne', 'user_status', { _id: userId }, statusDoc, time, {
docsExamined: statusDoc ? 1 : 0
});

console.log(`[USER-STATUS-QUERY] userId=${userId} | DB Reads: ${readsUsed} | Cache: MISS | Time: ${time}ms`);

if (statusDoc) {
await setCache(cacheKey, statusDoc, CACHE_TTL_MEDIUM);
}
return statusDoc || null;
}

// Apply same pattern to ALL getCache/setCache calls throughout the code

async updateUserStatus(userId, updates) {
const start = Date.now();
const result = await this.db.collection('user_status').updateOne(
{ _id: userId },
{ $set: { ...updates, updatedAt: new Date().toISOString() } },
{ upsert: true }
);
logDbOp('updateOne', 'user_status', { _id: userId }, result, Date.now() - start);

// Update cache
setCache(`user_status_${userId}`, { _id: userId, ...updates, updatedAt: new Date().toISOString() });
}

async getLatestSlotOptimized(collection) {
const cacheKey = `latest_${collection}`;
const cached = getCache(cacheKey);
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
setCache(cacheKey, latestSlot, CACHE_TTL_LONG); // Extended to 15 minutes
console.log(`[DB-READ] latest_${collection} | Total Reads: ${dbOpCounters.reads}`);
}
return latestSlot;
}

async getOptimizedFeed(userId, contentType, minContentRequired = MIN_CONTENT_FOR_FEED) {
const start = Date.now();
const isReel = contentType === 'reels';
const collection = isReel ? 'reels' : 'posts';
const statusField = isReel ? 'latestReelSlotId' : 'latestPostSlotId';
const normalField = isReel ? 'normalReelSlotId' : 'normalPostSlotId';


const userStatus = await this.getUserStatus(userId);
const isNewUser = !userStatus || !userStatus[statusField];

let result;
if (isNewUser) {
result = await this.getOptimizedFeedForNewUser(userId, collection, contentType, statusField, normalField, minContentRequired);
} else {
result = await this.getOptimizedFeedForReturningUser(userId, collection, contentType, userStatus, statusField, normalField, minContentRequired);
}

// If we don't have enough content, try to get more from different slots
if (result.content.length < minContentRequired) {
console.log(`[FEED-INSUFFICIENT] Got ${result.content.length}, need ${minContentRequired}, fetching more...`);
const additionalContent = await this.getAdditionalContent(userId, collection, contentType, minContentRequired - result.content.length, result);
result.content = [...result.content, ...additionalContent];
}

console.log(`[FEED-COMPLETE] User: ${userId}, Time: ${Date.now() - start}ms, Items: ${result.content.length}`);
return result;
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

// ---- Moved here and corrected ----
async batchPutContributedViewsOptimized(userId, posts = [], reels = []) {
const results = [];
const operations = [
posts.length > 0 && { type: 'posts', collection: 'contrib_posts', ids: posts },
reels.length > 0 && { type: 'reels', collection: 'contrib_reels', ids: reels }
].filter(Boolean);

for (const op of operations) {
const start = Date.now();
const result = await this.db.collection(op.collection).bulkWrite([{
updateOne: {
filter: { userId },
update: {
$addToSet: { ids: { $each: op.ids } },
$setOnInsert: { userId, createdAt: new Date().toISOString() },
$set: { updatedAt: new Date().toISOString() }
},
upsert: true
}
}], { ordered: false });
logDbOp('bulkWrite', op.collection, { userId }, result, Date.now() - start);
results.push({ type: op.type, result });
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

async allocateSlot(col, postData, maxAttempts = 5) {
    const coll = this.db.collection(col);
    const listKey = col === 'reels' ? 'reelsList' : 'postList';
    const postId = postData.postId;

    console.log(`[SLOT-ALLOCATION] ${col} | PostId: ${postId} | MAX: ${MAX_CONTENT_PER_SLOT}`);

    // ✅ CRITICAL: Check for duplicates FIRST (prevents duplicate writes)
    const existingCheck = await coll.findOne(
        { [`${listKey}.postId`]: postId },
        { projection: { _id: 1, count: 1 } }
    );

    if (existingCheck) {
        console.log(`[DUPLICATE-PREVENTED] ${postId} exists in ${existingCheck._id}`);
        return existingCheck;
    }

    for (let attempt = 1; attempt <= maxAttempts; attempt++) {
        try {
            console.log(`[SLOT-ATTEMPT-${attempt}/${maxAttempts}] ${col} | PostId: ${postId}`);

            // ✅ CRITICAL FIX: Use findOneAndUpdate with ATOMIC increment
            // This prevents race conditions by making find+update atomic
            const result = await coll.findOneAndUpdate(
                {
                    count: { $lt: MAX_CONTENT_PER_SLOT },
                    [`${listKey}.postId`]: { $ne: postId }  // Extra duplicate check
                },
                {
                    $push: { [listKey]: postData },
                    $inc: { count: 1 }  // ✅ ATOMIC increment
                },
                {
                    sort: { index: -1 },  // Get latest slot first
                    returnDocument: 'after',  // Return updated document
                    projection: { _id: 1, index: 1, count: 1 }
                }
            );

            if (result) {
                console.log(`[SLOT-SUCCESS] ${postId} → ${result._id} | Count: ${result.count - 1} → ${result.count}/${MAX_CONTENT_PER_SLOT}`);

                // ✅ Invalidate cache
                await this.invalidateSlotCache(col);
                if (redisClient) {
                    await redisClient.del(`max_index_${col}`).catch(() => {});
                }

                return {
                    _id: result._id,
                    index: result.index,
                    count: result.count
                };
            }

            // ✅ No available slot - create new one
            console.log(`[NO-SLOTS] All slots full (${MAX_CONTENT_PER_SLOT} items) - creating new slot`);

            // ✅ Get max index atomically
            const maxDoc = await coll.findOne(
                {},
                {
                    sort: { index: -1 },
                    projection: { index: 1 }
                }
            );

            const currentMaxIndex = maxDoc?.index ?? -1;
            const nextIndex = currentMaxIndex + 1;
            const newId = `${col.slice(0, -1)}_${nextIndex}`;

            console.log(`[CREATE-SLOT-${attempt}] ${newId} (index: ${nextIndex})`);

            try {
                const newDoc = {
                    _id: newId,
                    index: nextIndex,
                    count: 1,
                    [listKey]: [postData],
                    createdAt: new Date().toISOString()
                };

                await coll.insertOne(newDoc);

                console.log(`[SLOT-CREATED] ${newId} | PostId: ${postId} | Capacity: 1/${MAX_CONTENT_PER_SLOT}`);

                await setCache(`max_index_${col}`, nextIndex, 5000);
                await this.invalidateSlotCache(col);

                return newDoc;

            } catch (insertErr) {
                if (insertErr.code === 11000) {
                    // ✅ Another process created this slot - retry with findOneAndUpdate
                    console.log(`[DUPLICATE-KEY-${attempt}] ${newId} exists, retrying...`);

                    if (redisClient) {
                        await redisClient.del(`max_index_${col}`).catch(() => {});
                    }

                    // ✅ Exponential backoff
                    await new Promise(resolve => setTimeout(resolve, 50 * Math.pow(2, attempt - 1)));
                    continue;
                }

                throw insertErr;
            }

        } catch (error) {
            console.error(`[SLOT-ERROR-${attempt}] ${col} | ${error.message}`);

            if (attempt === maxAttempts) {
                throw new Error(`Slot allocation failed after ${maxAttempts} attempts: ${error.message}`);
            }

            // ✅ Exponential backoff
            await new Promise(resolve => setTimeout(resolve, 100 * Math.pow(2, attempt - 1)));
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
if (process.env.REDIS_URL) {
redisClient = new Redis(process.env.REDIS_URL, {
maxRetriesPerRequest: 3,
enableReadyCheck: true,
connectTimeout: 10000,
retryStrategy(times) {
if (times > 5) {
log('error', '[REDIS] Max retries reached, falling back to in-memory cache');
return null; // Stop retrying
}
const delay = Math.min(times * 50, 2000);
return delay;
}
});

redisClient.on('error', (err) => {
log('error', '[REDIS-ERROR]', err.message);
// Don't crash the server, just log the error
});

redisClient.on('connect', () => {
log('info', '[REDIS] ✅ Connected successfully');
});

redisClient.on('close', () => {
log('warn', '[REDIS] Connection closed, falling back to in-memory cache');
});

// Test connection
try {
await redisClient.ping();
console.log('[REDIS-INIT] ✅ Health check passed');
} catch (err) {
log('warn', `[REDIS] Health check failed: ${err.message}, using in-memory cache`);
redisClient = null; // Fall back to in-memory
}
} else {
log('warn', '[REDIS] ⚠️ Not configured - using in-memory cache (NOT PRODUCTION READY)');
log('warn', '[REDIS] For Instagram-scale performance, install Redis and set REDIS_URL in .env');
}
}





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


app.get('/api/slot-stats', async (req, res) => {
    try {
        const [postStats, reelStats] = await Promise.all([
            db.collection('posts').aggregate([
                {
                    $group: {
                        _id: null,
                        totalSlots: { $sum: 1 },
                        avgItemsPerSlot: { $avg: '$count' },
                        maxItemsPerSlot: { $max: '$count' },
                        minItemsPerSlot: { $min: '$count' }
                    }
                }
            ]).toArray(),
            db.collection('reels').aggregate([
                {
                    $group: {
                        _id: null,
                        totalSlots: { $sum: 1 },
                        avgItemsPerSlot: { $avg: '$count' },
                        maxItemsPerSlot: { $max: '$count' },
                        minItemsPerSlot: { $min: '$count' }
                    }
                }
            ]).toArray()
        ]);

        res.json({
            success: true,
            config: {
                maxContentPerSlot: MAX_CONTENT_PER_SLOT,
                defaultBatchSize: DEFAULT_CONTENT_BATCH_SIZE
            },
            posts: postStats[0] || { totalSlots: 0, avgItemsPerSlot: 0 },
            reels: reelStats[0] || { totalSlots: 0, avgItemsPerSlot: 0 },
            timestamp: new Date().toISOString()
        });
    } catch (error) {
        res.status(500).json({ error: error.message });
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



// REMOVE the periodic sync interval completely:
// DELETE this entire block:
// setInterval(async () => { ... }, 5 * 60 * 1000);

// -------------------
// ADD new batch endpoint:
// -------------------
app.post('/api/sync/batch-metrics', async (req, res) => {
  try {
    const { metrics } = req.body;
    
    if (!metrics || typeof metrics !== 'object') {
      return res.status(400).json({ error: 'metrics object required' });
    }
    
    const postIds = Object.keys(metrics);
    console.log(`[BATCH-SYNC] Processing ${postIds.length} posts`);
    
    let updated = 0;
    let notFound = 0;
    
    // Update all in parallel
    await Promise.all(
      postIds.map(async (postId) => {
        const data = metrics[postId];
        const arrayField = data.isReel ? 'reelsList' : 'postList';
        
        const result = await db.collection('user_slots').updateOne(
          { [`${arrayField}.postId`]: postId },
          {
            $set: {
              [`${arrayField}.$.likeCount`]: data.likeCount || 0,
              [`${arrayField}.$.commentCount`]: data.commentCount || 0,
              [`${arrayField}.$.viewCount`]: data.viewCount || 0,
              [`${arrayField}.$.retention`]: data.retention || 0,
              [`${arrayField}.$.lastSynced`]: new Date().toISOString()
            }
          }
        );
        
        if (result.matchedCount > 0) {
          updated++;
        } else {
          notFound++;
          console.warn(`[BATCH-SYNC] Post not found: ${postId}`);
        }
      })
    );
    
    console.log(`[BATCH-SYNC-DONE] ${updated} updated, ${notFound} not found`);
    
    res.json({
      success: true,
      updated,
      notFound,
      total: postIds.length
    });
    
  } catch (error) {
    console.error('[BATCH-SYNC-ERROR]', error);
    res.status(500).json({ error: 'Batch sync failed' });
  }
});


// -------------------
// KEEP /api/sync/metrics but simplify it (for backward compatibility):
// -------------------
app.post('/api/sync/metrics', async (req, res) => {
  try {
    const { postId, metrics, isReel } = req.body;
    
    if (!postId || !metrics) {
      return res.status(400).json({ error: 'postId and metrics required' });
    }
    
    const arrayField = isReel ? 'reelsList' : 'postList';
    
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
    
    res.json({ success: true, message: 'Metrics synced successfully', postId });
    
  } catch (error) {
    console.error('[SYNC-ERROR]', error);
    res.status(500).json({ error: 'Failed to sync metrics' });
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



// ✅ ADD THIS NEW ENDPOINT - Receives like data from PORT 4000
app.post('/api/interactions/sync-from-port4000', async (req, res) => {
    try {
        const { likes, timestamp, trigger } = req.body;

        if (!Array.isArray(likes)) {
            return res.status(400).json({ error: 'likes array required' });
        }

        console.log(`[SYNC-FROM-4000] Received ${likes.length} likes, trigger: ${trigger || 'manual'}`);

        // Build a Set of current PORT 2000 likes for comparison
        const port2000CurrentLikes = await db.collection('user_interaction_cache').aggregate([
            { $match: { likedToday: { $exists: true, $ne: [] } } },
            { $unwind: '$likedToday' },
            { $group: { _id: null, allLikes: { $addToSet: '$likedToday' } } }
        ]).toArray();

        const currentLikesSet = new Set(port2000CurrentLikes[0]?.allLikes || []);
        const port4000LikesSet = new Set(likes.map(l => l.postId));

        // Determine what needs to be added/removed
        const toAdd = likes.filter(l => !currentLikesSet.has(l.postId));
        const toRemove = [...currentLikesSet].filter(postId => !port4000LikesSet.has(postId));

        let added = 0, removed = 0;

        // Add missing likes
        for (const like of toAdd) {
            const today = new Date().toISOString().split('T')[0];
            const cacheKey = `${like.userId}_session_${today}`;

            await db.collection('user_interaction_cache').updateOne(
                { _id: cacheKey },
                {
                    $addToSet: { likedToday: like.postId },
                    $set: { ttl: new Date(Date.now() + 24 * 60 * 60 * 1000) }
                },
                { upsert: true }
            );
            added++;
        }

        // Remove extra likes (unliked on PORT 4000)
        for (const postId of toRemove) {
            await db.collection('user_interaction_cache').updateMany(
                { likedToday: postId },
                { $pull: { likedToday: postId } }
            );
            removed++;
        }

        console.log(`[SYNC-FROM-4000-COMPLETE] Added: ${added}, Removed: ${removed}`);

        return res.json({
            success: true,
            added,
            removed,
            total: likes.length,
            timestamp: new Date().toISOString()
        });

    } catch (error) {
        console.error('[SYNC-FROM-4000-ERROR]', error);
        return res.status(500).json({ error: 'Sync failed' });
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

        console.log(`[BATCH-CONTRIB] userId=${userId} | ${posts.length}P + ${reels.length}R`);

        // ✅ CRITICAL FIX: Use aggregation instead of full collection scan
        const postDocumentMap = new Map();
        const reelDocumentMap = new Map();

        if (posts.length > 0) {
            console.log(`[BATCH-CONTRIB] Looking up ${posts.length} posts via aggregation`);
            
            // ✅ FIXED: Use aggregation to find documents efficiently
            const postAggregation = await db.collection('posts').aggregate([
                { $match: { 'postList.postId': { $in: posts } } },
                { $project: { _id: 1, postList: { postId: 1 } } },
                { $unwind: '$postList' },
                { $match: { 'postList.postId': { $in: posts } } },
                { $project: { _id: 1, postId: '$postList.postId' } }
            ]).toArray();

            postAggregation.forEach(doc => {
                postDocumentMap.set(doc.postId, doc._id);
                console.log(`[POST-MAPPED] ${doc.postId.substring(0, 8)} -> ${doc._id}`);
            });

            console.log(`[POST-LOOKUP] Mapped ${postDocumentMap.size}/${posts.length} posts`);
        }

        if (reels.length > 0) {
            console.log(`[BATCH-CONTRIB] Looking up ${reels.length} reels via aggregation`);
            
            // ✅ FIXED: Use aggregation for reels
            const reelAggregation = await db.collection('reels').aggregate([
                { $match: { 'reelsList.postId': { $in: reels } } },
                { $project: { _id: 1, reelsList: { postId: 1 } } },
                { $unwind: '$reelsList' },
                { $match: { 'reelsList.postId': { $in: reels } } },
                { $project: { _id: 1, postId: '$reelsList.postId' } }
            ]).toArray();

            reelAggregation.forEach(doc => {
                reelDocumentMap.set(doc.postId, doc._id);
                console.log(`[REEL-MAPPED] ${doc.postId.substring(0, 8)} -> ${doc._id}`);
            });

            console.log(`[REEL-LOOKUP] Mapped ${reelDocumentMap.size}/${reels.length} reels`);
        }

        // ✅ Group by document
        const postsByDocument = new Map();
        const reelsByDocument = new Map();

        for (const postId of posts) {
            const docId = postDocumentMap.get(postId);
            if (!docId) {
                console.warn(`[POST-NOT-FOUND] ${postId.substring(0, 8)} not in posts collection`);
                continue;
            }
            if (!postsByDocument.has(docId)) {
                postsByDocument.set(docId, []);
            }
            postsByDocument.get(docId).push(postId);
        }

        for (const reelId of reels) {
            const docId = reelDocumentMap.get(reelId);
            if (!docId) {
                console.warn(`[REEL-NOT-FOUND] ${reelId.substring(0, 8)} not in reels collection`);
                continue;
            }
            if (!reelsByDocument.has(docId)) {
                reelsByDocument.set(docId, []);
            }
            reelsByDocument.get(docId).push(reelId);
        }

        console.log(`[GROUPING] Posts: ${postsByDocument.size} docs | Reels: ${reelsByDocument.size} docs`);

        // ✅ Batch write operations
        const postResults = [];
        const reelResults = [];

        for (const [docId, postIds] of postsByDocument.entries()) {
            const uniqueDocId = `${userId}_${docId}`;

            const result = await db.collection('contrib_posts').updateOne(
                { _id: uniqueDocId },
                {
                    $addToSet: { ids: { $each: postIds } },
                    $setOnInsert: {
                        userId,
                        slotId: docId,
                        createdAt: new Date()
                    },
                    $set: { updatedAt: new Date() }
                },
                { upsert: true }
            );

            postResults.push({
                documentId: uniqueDocId,
                originalSlotId: docId,
                count: postIds.length,
                matched: result.matchedCount,
                modified: result.modifiedCount
            });
        }

        for (const [docId, reelIds] of reelsByDocument.entries()) {
            const uniqueDocId = `${userId}_${docId}`;

            const result = await db.collection('contrib_reels').updateOne(
                { _id: uniqueDocId },
                {
                    $addToSet: { ids: { $each: reelIds } },
                    $setOnInsert: {
                        userId,
                        slotId: docId,
                        createdAt: new Date()
                    },
                    $set: { updatedAt: new Date() }
                },
                { upsert: true }
            );

            reelResults.push({
                documentId: uniqueDocId,
                originalSlotId: docId,
                count: reelIds.length,
                matched: result.matchedCount,
                modified: result.modifiedCount
            });
        }

        const duration = Date.now() - startTime;

        console.log(`[BATCH-CONTRIB-COMPLETE] ${posts.length}P + ${reels.length}R in ${duration}ms`);

        res.json({
            success: true,
            message: 'Contributed views processed',
            processed: {
                posts: posts.length,
                reels: reels.length
            },
            postResults,
            reelResults,
            requestId,
            duration
        });

    } catch (error) {
        const duration = Date.now() - startTime;
        console.error(`[BATCH-CONTRIB-ERROR] ${error.message}`);

        res.status(500).json({
            success: false,
            error: error.message,
            requestId,
            duration
        });
    }
});

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




app.post('/api/feed/reels-personalized', async (req, res) => {
    const requestStart = Date.now();
    const readsBefore = dbOpCounters.reads;
    
    try {
        const { userId, limit = DEFAULT_CONTENT_BATCH_SIZE, offset = 0 } = req.body;

        if (!userId || userId === 'undefined' || userId === 'null') {
            return res.status(400).json({ success: false, error: 'Valid userId required' });
        }

        const limitNum = parseInt(limit, 10) || DEFAULT_CONTENT_BATCH_SIZE;
        const offsetNum = parseInt(offset, 10) || 0;

        log('info', `[REELS-PERSONALIZED] userId=${userId}, limit=${limitNum}, offset=${offsetNum}`);

        // ✅ Cache key includes offset for pagination
        const cacheKey = `reels:personalized:${userId}:${limitNum}:${offsetNum}`;
        
        if (redisClient) {
            try {
                const cached = await redisClient.get(cacheKey);
                if (cached) {
                    const cachedData = JSON.parse(cached);
                    console.log(`[REELS-CACHE-HIT] ✅ ${userId} offset=${offsetNum} (0 DB reads)`);
                    return res.json({ ...cachedData, servedFromCache: true });
                }
            } catch (e) {
                console.warn('[CACHE-ERROR]', e.message);
            }
        }

        // ✅ STEP 1: Get user's current reel slots
        const userStatus = await db.collection('user_status').findOne(
            { _id: userId },
            { projection: { latestReelSlotId: 1 } }
        );

        let reelSlotIds = ['reel_0'];
        
        if (userStatus) {
            const reelNum = parseInt(userStatus.latestReelSlotId?.match(/_(\d+)$/)?.[1] || '0');
            reelSlotIds = [
                `reel_${reelNum}`,
                reelNum > 0 ? `reel_${reelNum - 1}` : null,
                reelNum > 1 ? `reel_${reelNum - 2}` : null
            ].filter(Boolean);
        }

        // ✅ STEP 2: Fetch user interests
        let userInterests = [];
        try {
            const userResponse = await axios.get(
                `https://server1-ki1x.onrender.com/api/users/${userId}`,
                { timeout: 1000, validateStatus: s => s >= 200 && s < 500 }
            );
            if (userResponse.status === 200 && userResponse.data?.success) {
                userInterests = userResponse.data.user?.interests || [];
            }
        } catch (e) {
            console.warn(`[INTERESTS-SKIP] ${e.message}`);
        }

        // ✅ STEP 3: Get viewed reels
        const viewedReelsDocs = await db.collection('contrib_reels').find(
            { userId },
            { projection: { ids: 1 }, limit: 5 }
        ).toArray();

        const viewedReelIds = viewedReelsDocs.flatMap(doc => doc.ids || []);
        
        log('info', `[REELS-EXCLUSIONS] Excluding ${viewedReelIds.length} viewed reels`);

        // ✅ STEP 4: Get max values for normalization (from targeted slots only)
        const maxValues = await db.collection('reels').aggregate([
            { $match: { _id: { $in: reelSlotIds } } },
            { $unwind: '$reelsList' },
            { $match: { 'reelsList.postId': { $nin: viewedReelIds } } },
            {
                $group: {
                    _id: null,
                    maxLikes: { $max: { $toInt: { $ifNull: ['$reelsList.likeCount', 0] } } },
                    maxComments: { $max: { $toInt: { $ifNull: ['$reelsList.commentCount', 0] } } }
                }
            }
        ]).toArray();

        const maxLikes = maxValues[0]?.maxLikes || 1;
        const maxComments = maxValues[0]?.maxComments || 1;

        log('info', `[NORMALIZATION] maxLikes=${maxLikes}, maxComments=${maxComments}`);

        // ✅ STEP 5: TARGETED aggregation with pagination
        const pipeline = [
            // ✅ CRITICAL FIX: Target specific slot documents only
            { $match: { _id: { $in: reelSlotIds } } },
            { $unwind: '$reelsList' },
            { $match: { 'reelsList.postId': { $nin: viewedReelIds } } },
            {
                $addFields: {
                    retentionNum: { $toDouble: { $ifNull: ['$reelsList.retention', 0] } },
                    likeCountNum: { $toInt: { $ifNull: ['$reelsList.likeCount', 0] } },
                    commentCountNum: { $toInt: { $ifNull: ['$reelsList.commentCount', 0] } },
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
                    }
                }
            },
            {
                $addFields: {
                    compositeScore: {
                        $add: [
                            { $multiply: ['$retentionNum', 0.50] },
                            { $multiply: ['$normalizedLikes', 0.25] },
                            { $multiply: ['$interestScore', 0.15] },
                            { $multiply: ['$normalizedComments', 0.10] }
                        ]
                    }
                }
            },
            { $sort: { compositeScore: -1 } },
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
                    hashtag: '$reelsList.hashtag',
                    profilePicUrl: { $ifNull: ['$reelsList.profile_picture_url', ''] },
                    timestamp: '$reelsList.timestamp',
                    likeCount: '$likeCountNum',
                    commentCount: '$commentCountNum',
                    viewCount: { $toInt: { $ifNull: ['$reelsList.viewCount', 0] } },
                    retention: '$retentionNum',
                    interestScore: '$interestScore',
                    compositeScore: '$compositeScore',
                    sourceDocument: '$_id',
                    isReel: { $literal: true }
                }
            }
        ];

        const startAgg = Date.now();
        const reels = await db.collection('reels').aggregate(pipeline, { maxTimeMS: 3000 }).toArray();
        const aggTime = Date.now() - startAgg;

        log('info', `[AGGREGATION-COMPLETE] ${reels.length} reels in ${aggTime}ms`);

        // ✅ STEP 6: Client-side deduplication
        const seenIds = new Set();
        const uniqueReels = [];

        for (const reel of reels) {
            if (!seenIds.has(reel.postId)) {
                seenIds.add(reel.postId);

                log('info', `[REEL-RANKED] ${reel.postId.substring(0, 8)} | ` +
                    `SCORE=${reel.compositeScore.toFixed(2)} | ` +
                    `retention=${reel.retention.toFixed(1)}% | ` +
                    `likes=${reel.likeCount} | ` +
                    `category=${reel.category || 'none'}`);

                uniqueReels.push(reel);

                if (uniqueReels.length >= limitNum) break;
            }
        }

        const totalReads = dbOpCounters.reads - readsBefore;
        const totalTime = Date.now() - requestStart;

        const responseData = {
            success: true,
            content: uniqueReels,
            hasMore: reels.length >= limitNum,
            metadata: {
                totalReturned: uniqueReels.length,
                userInterests: userInterests,
                viewedReelsFiltered: viewedReelIds.length,
                aggregationTimeMs: aggTime,
                offset: offsetNum,
                normalization: { maxLikes, maxComments },
                algorithmWeights: {
                    retention: 50,
                    likes: 25,
                    interest: 15,
                    comments: 10
                },
                slotsQueried: reelSlotIds,
                dbActivity: { totalReads }
            }
        };

        // ✅ Cache for 30 seconds
        if (redisClient && uniqueReels.length > 0) {
            redisClient.setex(cacheKey, 30, JSON.stringify(responseData)).catch(e => {
                console.warn('[CACHE-SET-ERROR]', e.message);
            });
        }

        log('info', `[REELS-PERSONALIZED-COMPLETE] ${totalReads} reads | ${totalTime}ms | Returned ${uniqueReels.length} reels`);

        return res.json(responseData);

    } catch (error) {
        log('error', '[REELS-PERSONALIZED-ERROR]', error);
        return res.status(500).json({
            success: false,
            error: 'Failed to load personalized reels',
            details: error.message
        });
    }
});



app.post('/api/feed/instagram-ranked', async (req, res) => {
    const requestStart = Date.now();
    const readsBefore = dbOpCounters.reads;
    
    try {
        const { userId, limit = DEFAULT_CONTENT_BATCH_SIZE, excludedPostIds = [], excludedReelIds = [] } = req.body;
        
        if (!userId) {
            return res.status(400).json({ success: false, error: 'userId required' });
        }
        
        const limitNum = parseInt(limit, 10) || DEFAULT_CONTENT_BATCH_SIZE;
        const cacheKey = generateSlotBasedCacheKey(userId, userStatus);
        
        // ✅ CHECK REDIS CACHE FIRST
        if (redisClient) {
            try {
                const cached = await redisClient.get(cacheKey);
                if (cached) {
                    const cachedData = JSON.parse(cached);
                    console.log(`[FEED-CACHE-HIT] ✅ ${userId} served from Redis (0 DB reads)`);
                    return res.json({ ...cachedData, servedFromCache: true, cacheHit: true });
                }
            } catch (e) {
                console.warn('[CACHE-ERROR]', e.message);
            }
        }
        
        console.log(`[FEED-START] userId=${userId} | limit=${limitNum}`);
        
        // ✅ STEP 1: Get user's current slots (1 read)
        const userStatus = await db.collection('user_status').findOne(
            { _id: userId },
            { projection: { latestReelSlotId: 1, latestPostSlotId: 1 } }
        );
        
        // Default to latest slots if user is new
        let reelSlotIds = ['reel_0'];
        let postSlotIds = ['post_0'];
        
        if (userStatus) {
            // Extract slot numbers and get current + 2 previous slots
            const reelNum = parseInt(userStatus.latestReelSlotId?.match(/_(\d+)$/)?.[1] || '0');
            const postNum = parseInt(userStatus.latestPostSlotId?.match(/_(\d+)$/)?.[1] || '0');
            
            reelSlotIds = [
                `reel_${reelNum}`,
                reelNum > 0 ? `reel_${reelNum - 1}` : null,
                reelNum > 1 ? `reel_${reelNum - 2}` : null
            ].filter(Boolean);
            
            postSlotIds = [
                `post_${postNum}`,
                postNum > 0 ? `post_${postNum - 1}` : null,
                postNum > 1 ? `post_${postNum - 2}` : null
            ].filter(Boolean);
        }
        
        console.log(`[FEED-SLOTS] Reels: ${reelSlotIds} | Posts: ${postSlotIds}`);
        
        // ✅ STEP 2: Get viewed content (1 read each)
        const [viewedPostsDocs, viewedReelsDocs] = await Promise.all([
            db.collection('contrib_posts').find(
                { userId },
                { projection: { ids: 1 }, limit: 5 }
            ).toArray(),
            db.collection('contrib_reels').find(
                { userId },
                { projection: { ids: 1 }, limit: 5 }
            ).toArray()
        ]);
        
        const serverViewedPosts = viewedPostsDocs.flatMap(doc => doc.ids || []);
        const serverViewedReels = viewedReelsDocs.flatMap(doc => doc.ids || []);
        
        const allExcludedPosts = [...new Set([...serverViewedPosts, ...excludedPostIds])];
        const allExcludedReels = [...new Set([...serverViewedReels, ...excludedReelIds])];
        
        console.log(`[FEED-EXCLUSIONS] Posts: ${allExcludedPosts.length} | Reels: ${allExcludedReels.length}`);
        
        // ✅ STEP 3: Fetch user interests (optional, best-effort)
        let userInterests = [];
        try {
            const userResponse = await axios.get(
                `https://server1-ki1x.onrender.com/api/users/${userId}`,
                { timeout: 1000, validateStatus: s => s >= 200 && s < 500 }
            );
            if (userResponse.status === 200 && userResponse.data?.success) {
                userInterests = userResponse.data.user?.interests || [];
            }
        } catch (e) {
            console.warn(`[INTERESTS-SKIP] ${e.message}`);
        }
        
        // ✅ STEP 4: TARGETED aggregation - only query specific slot documents
        const buildSlotPipeline = (slotIds, arrayField, excludedIds) => [
            // ✅ CRITICAL FIX: Target specific documents by _id
            { $match: { _id: { $in: slotIds } } },
            { $unwind: `$${arrayField}` },
            { $match: { [`${arrayField}.postId`]: { $nin: excludedIds } } },
            { $limit: limitNum * 2 },
            {
                $addFields: {
                    retentionNum: { $toDouble: { $ifNull: [`$${arrayField}.retention`, 0] } },
                    likeCountNum: { $toInt: { $ifNull: [`$${arrayField}.likeCount`, 0] } },
                    commentCountNum: { $toInt: { $ifNull: [`$${arrayField}.commentCount`, 0] } },
                    interestScore: {
                        $cond: {
                            if: { $in: [`$${arrayField}.category`, userInterests] },
                            then: 100,
                            else: { $cond: [{ $eq: [{ $size: { $ifNull: [userInterests, []] } }, 0] }, 50, 0] }
                        }
                    }
                }
            },
            {
                $addFields: {
                    compositeScore: {
                        $add: [
                            { $multiply: ['$retentionNum', 0.50] },
                            { $multiply: ['$likeCountNum', 0.30] },
                            { $multiply: ['$interestScore', 0.15] },
                            { $multiply: ['$commentCountNum', 0.05] }
                        ]
                    }
                }
            },
            { $sort: { compositeScore: -1 } },
            { $limit: limitNum }
        ];
        
        const aggStart = Date.now();
        
        // ✅ Execute TARGETED aggregations (only on specific slot documents)
        const [allPosts, allReels] = await Promise.all([
            db.collection('posts').aggregate([
                ...buildSlotPipeline(postSlotIds, 'postList', allExcludedPosts),
                {
                    $project: {
                        postId: '$postList.postId',
                        userId: '$postList.userId',
                        username: '$postList.username',
                        imageUrl: { $ifNull: ['$postList.imageUrl', '$postList.imageUrl1'] },
                        multiple_posts: { $ifNull: ['$postList.multiple_posts', false] },
                        media_count: { $ifNull: ['$postList.media_count', 1] },
                        profilePicUrl: '$postList.profile_picture_url',
                        caption: '$postList.caption',
                        category: '$postList.category',
                        timestamp: '$postList.timestamp',
                        likeCount: '$likeCountNum',
                        commentCount: '$commentCountNum',
                        retention: '$retentionNum',
                        compositeScore: '$compositeScore',
                        sourceDocument: '$_id',
                        ratio: { $ifNull: ['$postList.ratio', '4:5'] },
                        isReel: { $literal: false }
                    }
                }
            ], { maxTimeMS: 3000 }).toArray(),
            
            db.collection('reels').aggregate([
                ...buildSlotPipeline(reelSlotIds, 'reelsList', allExcludedReels),
                {
                    $project: {
                        postId: '$reelsList.postId',
                        userId: '$reelsList.userId',
                        username: '$reelsList.username',
                        imageUrl: { $ifNull: ['$reelsList.videoUrl', '$reelsList.imageUrl'] },
                        videoUrl: '$reelsList.videoUrl',
                        profilePicUrl: '$reelsList.profile_picture_url',
                        caption: '$reelsList.caption',
                        category: '$reelsList.category',
                        timestamp: '$reelsList.timestamp',
                        likeCount: '$likeCountNum',
                        commentCount: '$commentCountNum',
                        retention: '$retentionNum',
                        compositeScore: '$compositeScore',
                        sourceDocument: '$_id',
                        ratio: '9:16',
                        isReel: { $literal: true }
                    }
                }
            ], { maxTimeMS: 3000 }).toArray()
        ]);
        
        console.log(`[FEED-AGGREGATION] ${Date.now() - aggStart}ms | Posts: ${allPosts.length} | Reels: ${allReels.length}`);
        
        // ✅ STEP 5: Deduplicate and merge
        const seenIds = new Set();
        const finalContent = [];
        const allContent = [...allPosts, ...allReels].sort((a, b) => (b.compositeScore || 0) - (a.compositeScore || 0));
        
        for (const item of allContent) {
            if (!item?.postId || seenIds.has(item.postId) || finalContent.length >= limitNum) continue;
            seenIds.add(item.postId);
            finalContent.push(item);
        }
        
        const totalReads = dbOpCounters.reads - readsBefore;
        const totalTime = Date.now() - requestStart;
        
        const responseData = {
            success: true,
            content: finalContent,
            hasMore: allContent.length >= limitNum,
            metadata: {
                totalReturned: finalContent.length,
                responseTime: totalTime,
                dbActivity: { totalReads },
                slotsQueried: { reels: reelSlotIds, posts: postSlotIds }
            }
        };
        
        // ✅ Cache response (30 seconds)
        if (redisClient && finalContent.length > 0) {
            redisClient.setex(cacheKey, 30, JSON.stringify(responseData)).catch(e => {
                console.warn('[CACHE-SET-ERROR]', e.message);
            });
        }
        
        console.log(`[FEED-COMPLETE] ✅ ${totalReads} reads | ${totalTime}ms | Cached for 30s`);
        
        return res.json(responseData);
        
    } catch (error) {
        console.error('[FEED-ERROR]', error);
        return res.status(500).json({ success: false, error: 'Failed to load feed' });
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





// REPLACE /api/interactions/view endpoint (line ~3487)

app.post('/api/interactions/view', async (req, res) => {
    try {
        const { userId, postId, sourceDocument, retentionData } = req.body;

        if (!userId || !postId) {
            return res.status(400).json({ error: 'userId and postId required' });
        }

        console.log(`[VIEW-START] ${userId} -> ${postId} ${retentionData ? '(WITH RETENTION)' : '(VIEW ONLY)'}`);

        const today = new Date().toISOString().split('T')[0];
        const cacheKey = `${userId}_session_${today}`;

        // ✅ CRITICAL FIX: Batch all operations together
        const operations = [];

        if (retentionData) {
            console.log(`[RETENTION-CHECK] ${userId} -> ${postId}`);

            // ✅ Single atomic operation with $addToSet (prevents duplicates at DB level)
            operations.push(
                db.collection('user_interaction_cache').updateOne(
                    { _id: cacheKey },
                    {
                        $set: {
                            userId,
                            ttl: new Date(Date.now() + 24 * 60 * 60 * 1000),
                            updatedAt: new Date()
                        },
                        $addToSet: {
                            viewedToday: postId,
                            retentionContributed: postId  // Atomic duplicate prevention
                        },
                        $setOnInsert: { createdAt: new Date() }
                    },
                    { upsert: true }
                )
            );

            // ✅ Background detailed recording (non-blocking)
            const viewRecord = {
                postId,
                viewedAt: new Date(),
                sourceDocument: sourceDocument || 'unknown',
                retentionContributed: true,
                retentionPercent: Math.round(retentionData.retentionPercent * 100) / 100,
                watchedDuration: retentionData.watchedDuration,
                totalDuration: retentionData.totalDuration
            };

            operations.push(
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
            );

        } else {
            // View only
            operations.push(
                db.collection('user_interaction_cache').updateOne(
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
                )
            );
        }

        // ✅ FIXED: Execute all operations in parallel
        const results = await Promise.all(operations);

        // Check if retention was actually added (not duplicate)
        if (retentionData && results[0].modifiedCount === 0 && results[0].upsertedCount === 0) {
            console.log(`[RETENTION-DUPLICATE] ${userId} -> ${postId} already contributed`);
            return res.json({
                success: true,
                message: 'Retention already contributed',
                duplicate: true
            });
        }

        console.log(`[VIEW-RECORDED] ${userId} -> ${postId} | Operations: ${operations.length}`);

        return res.json({ 
            success: true, 
            message: 'View recorded successfully',
            operations: operations.length
        });

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



// REPLACE GET /api/user-status/:userId (line ~3627)

app.get('/api/user-status/:userId', async (req, res) => {
    const startTime = Date.now();
    const { userId } = req.params;

    try {
        console.log(`[USER-STATUS] Lookup for userId=${userId}`);

        // ✅ FIXED: Read only necessary fields, no redundant userId field
        const userStatus = await db.collection('user_status').findOne(
            { _id: userId },
            {
                projection: {
                    _id: 1,
                    latestReelSlotId: 1,
                    normalReelSlotId: 1,
                    latestPostSlotId: 1,
                    normalPostSlotId: 1
                    // ❌ REMOVED: userId field (redundant with _id)
                }
            }
        );

        const duration = Date.now() - startTime;

        if (userStatus && userStatus.latestReelSlotId) {
            let latestReelSlot = userStatus.latestReelSlotId || 'reel_0';
            let normalReelSlot = userStatus.normalReelSlotId || 'reel_0';

            // Standardize naming
            if (latestReelSlot.startsWith('reels_')) {
                latestReelSlot = latestReelSlot.replace('reels_', 'reel_');
            }
            if (normalReelSlot.startsWith('reels_')) {
                normalReelSlot = normalReelSlot.replace('reels_', 'reel_');
            }

            console.log(`[USER-STATUS-SUCCESS] duration=${duration}ms | latestReel=${latestReelSlot}`);

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
            // Document exists but missing slot fields - auto-detect and update
            console.log(`[USER-STATUS-INCOMPLETE] Auto-detecting latest slots`);

            const [reelDocs, postDocs] = await Promise.all([
                db.collection('reels').find({}, { projection: { _id: 1 } }).sort({ _id: -1 }).limit(2).toArray(),
                db.collection('posts').find({}, { projection: { _id: 1 } }).sort({ _id: -1 }).limit(2).toArray()
            ]);

            const latestReelSlot = reelDocs[0]?._id || 'reel_0';
            const normalReelSlot = reelDocs[1]?._id || reelDocs[0]?._id || 'reel_0';
            const latestPostSlot = postDocs[0]?._id || 'post_0';
            const normalPostSlot = postDocs[1]?._id || postDocs[0]?._id || 'post_0';

            // ✅ FIXED: Update without redundant userId field
            await db.collection('user_status').updateOne(
                { _id: userId },
                {
                    $set: {
                        latestReelSlotId: latestReelSlot,
                        normalReelSlotId: normalReelSlot,
                        latestPostSlotId: latestPostSlot,
                        normalPostSlotId: normalPostSlot,
                        updatedAt: new Date()
                        // ❌ REMOVED: userId field
                    }
                }
            );

            const totalDuration = Date.now() - startTime;

            return res.json({
                success: true,
                latestReelSlotId: latestReelSlot,
                normalReelSlotId: normalReelSlot,
                latestPostSlotId: latestPostSlot,
                normalPostSlotId: normalPostSlot,
                reads: 3,
                duration: totalDuration
            });
        } else {
            // New user - create document
            console.log(`[USER-STATUS-NEW] Creating for userId=${userId}`);

            const [reelDocs, postDocs] = await Promise.all([
                db.collection('reels').find({}, { projection: { _id: 1 } }).sort({ _id: -1 }).limit(2).toArray(),
                db.collection('posts').find({}, { projection: { _id: 1 } }).sort({ _id: -1 }).limit(2).toArray()
            ]);

            const latestReelSlot = reelDocs[0]?._id || 'reel_0';
            const normalReelSlot = reelDocs[1]?._id || reelDocs[0]?._id || 'reel_0';
            const latestPostSlot = postDocs[0]?._id || 'post_0';
            const normalPostSlot = postDocs[1]?._id || postDocs[0]?._id || 'post_0';

            // ✅ FIXED: Insert without redundant userId field
            const defaultStatus = {
                _id: userId,  // Only store once as _id
                latestReelSlotId: latestReelSlot,
                normalReelSlotId: normalReelSlot,
                latestPostSlotId: latestPostSlot,
                normalPostSlotId: normalPostSlot,
                createdAt: new Date(),
                updatedAt: new Date()
                // ❌ REMOVED: userId field (redundant)
            };

            await db.collection('user_status').insertOne(defaultStatus);

            const totalDuration = Date.now() - startTime;

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
        console.error(`[USER-STATUS-ERROR] ${error.message}`);
        return res.status(500).json({
            success: false,
            error: 'Failed to read user_status: ' + error.message,
            reads: 1,
            duration: Date.now() - startTime
        });
    }
});

// ALSO FIX: POST /api/user-status/:userId (line ~3765)
app.post('/api/user-status/:userId', async (req, res) => {
    const startTime = Date.now();
    const { userId } = req.params;
    const { latestReelSlotId, normalReelSlotId, latestPostSlotId, normalPostSlotId } = req.body;

    try {
        const updateData = {
            updatedAt: new Date()
            // ❌ REMOVED: userId field (redundant)
        };

        if (latestReelSlotId) updateData.latestReelSlotId = latestReelSlotId;
        if (normalReelSlotId) updateData.normalReelSlotId = normalReelSlotId;
        if (latestPostSlotId) updateData.latestPostSlotId = latestPostSlotId;
        if (normalPostSlotId) updateData.normalPostSlotId = normalPostSlotId;

        const result = await db.collection('user_status').updateOne(
            { _id: userId },
            {
                $set: updateData,
                $setOnInsert: { createdAt: new Date() }  // ❌ REMOVED: userId from $setOnInsert
            },
            { upsert: true }
        );

        const duration = Date.now() - startTime;

        console.log(`[USER-STATUS-UPDATE] matched=${result.matchedCount} | modified=${result.modifiedCount} | duration=${duration}ms`);

        return res.json({
            success: true,
            message: 'Slot IDs updated',
            writes: 1,
            duration
        });

    } catch (error) {
        console.error(`[USER-STATUS-UPDATE-ERROR] ${error.message}`);
        return res.status(500).json({
            success: false,
            error: 'Failed to update user_status: ' + error.message,
            writes: 0,
            duration: Date.now() - startTime
        });
    }
});



/**
* POST /api/user-status/:userId
* Update slot IDs in user_status (WRITE only - no read)
*/
app.post('/api/user-status/:userId', async (req, res) => {
const startTime = Date.now();
const { userId } = req.params;
const { latestReelSlotId, normalReelSlotId, latestPostSlotId, normalPostSlotId } = req.body;

try {
console.log(`[post_algorithm] [UPDATE-USER-STATUS] userId=${userId} | latestReel=${latestReelSlotId} | normalReel=${normalReelSlotId} | latestPost=${latestPostSlotId} | normalPost=${normalPostSlotId}`);

const updateData = {
updatedAt: new Date()
};

// Only update fields that are provided
if (latestReelSlotId) updateData.latestReelSlotId = latestReelSlotId;
if (normalReelSlotId) updateData.normalReelSlotId = normalReelSlotId;
if (latestPostSlotId) updateData.latestPostSlotId = latestPostSlotId;
if (normalPostSlotId) updateData.normalPostSlotId = normalPostSlotId;

// ✅ CRITICAL: Write to document where _id = userId
const result = await db.collection('user_status').updateOne(
{ _id: userId },
{
$set: updateData,
$setOnInsert: { userId, createdAt: new Date() }
},
{ upsert: true }
);

const duration = Date.now() - startTime;

console.log(`[post_algorithm] [UPDATE-SUCCESS] matched=${result.matchedCount} | modified=${result.modifiedCount} | upserted=${result.upsertedCount} | duration=${duration}ms`);

return res.json({
success: true,
message: 'Slot IDs updated',
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



/**
* GET /api/contrib-check/:userId/:slotId/:type
* Returns ids array and count from contrib_posts or contrib_reels
* READ COUNT: 1 (single document read by _id = slotId)
*/
app.get('/api/contrib-check/:userId/:slotId/:type', async (req, res) => {
const startTime = Date.now();
const { userId, slotId, type } = req.params;

if (!['posts', 'reels'].includes(type)) {
return res.status(400).json({ success: false, error: 'Invalid type (must be posts or reels)' });
}

const collectionName = type === 'posts' ? 'contrib_posts' : 'contrib_reels';
const readNum = req.headers['x-read-number'] || '?';

try {
console.log(`[post_algorithm] [READ-${readNum}-START] ${collectionName} lookup for slotId=${slotId} | userId=${userId}`);

// ✅ FIXED: Use userId_slotId as _id
const uniqueDocId = `${userId}_${slotId}`;

const contribDoc = await db.collection(collectionName).findOne(
{ _id: uniqueDocId }, // Changed from { _id: slotId, userId: userId }
{ projection: { ids: 1, _id: 1, slotId: 1 } }
);

const duration = Date.now() - startTime;

if (contribDoc && contribDoc.ids) {
const count = contribDoc.ids.length;

console.log(`[post_algorithm] [READ-${readNum}-SUCCESS] ${collectionName} slotId=${slotId} | count=${count}/6 | duration=${duration}ms`);

return res.json({
success: true,
slotId: slotId,
ids: contribDoc.ids,
count: count,
reads: 1,
duration
});
} else {
console.log(`[post_algorithm] [READ-${readNum}-NOT-FOUND] ${collectionName} slotId=${slotId} doesn't exist for userId=${userId}`);

return res.json({
success: true,
slotId: slotId,
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

/**
* POST /api/feed/optimized-reels
* Fetches reels content from specific slot with ranking algorithm
* READ COUNT: 1 (aggregation on reels collection filtered by slotId)
*/
app.post('/api/feed/optimized-reels', async (req, res) => {
const startTime = Date.now();
const { userId, slotId, excludedReelIds = [], limit = DEFAULT_CONTENT_BATCH_SIZE } = req.body;
const readNum = req.headers['x-read-number'] || '?';

try {
console.log(`[post_algorithm] [READ-${readNum}-START] reels content from slotId=${slotId} | excluding=${excludedReelIds.length} ids | limit=${limit}`);

// ✅ Validate inputs
if (!userId || !slotId) {
return res.status(400).json({
success: false,
error: 'userId and slotId are required'
});
}

// Fetch user interests for ranking
let userInterests = [];
try {
const userResponse = await axios.get(`https://server1-ki1x.onrender.com/api/users/${userId}`, { timeout: 1000 });
if (userResponse.status === 200 && userResponse.data.success) {
userInterests = userResponse.data.user.interests || [];
}
} catch (e) {
console.warn(`[post_algorithm] [INTERESTS-SKIP] ${e.message}`);
}

// Get max values for normalization (quick aggregation on same document)
const maxValues = await db.collection('reels').aggregate([
{ $match: { _id: slotId, 'reelsList': { $exists: true, $ne: [] } } },
{ $unwind: '$reelsList' },
{ $match: { 'reelsList.postId': { $nin: excludedReelIds } } },
{
$group: {
_id: null,
maxLikes: { $max: { $toInt: { $ifNull: ['$reelsList.likeCount', 0] } } },
maxComments: { $max: { $toInt: { $ifNull: ['$reelsList.commentCount', 0] } } }
}
}
]).toArray();

const maxLikes = maxValues[0]?.maxLikes || 1;
const maxComments = maxValues[0]?.maxComments || 1;

console.log(`[post_algorithm] [NORMALIZATION] maxLikes=${maxLikes}, maxComments=${maxComments}, userInterests=${userInterests.length}`);

// ✅ CRITICAL: Aggregation reads ONLY from document with _id = slotId
const pipeline = [
{ $match: { _id: slotId, 'reelsList': { $exists: true, $ne: [] } } },
{ $unwind: '$reelsList' },
{ $match: { 'reelsList.postId': { $nin: excludedReelIds } } },
{
$addFields: {
retentionNum: { $toDouble: { $ifNull: ['$reelsList.retention', 0] } },
likeCountNum: { $toInt: { $ifNull: ['$reelsList.likeCount', 0] } },
commentCountNum: { $toInt: { $ifNull: ['$reelsList.commentCount', 0] } },
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
normalizedLikes: { $multiply: [{ $divide: ['$likeCountNum', maxLikes] }, 100] },
normalizedComments: { $multiply: [{ $divide: ['$commentCountNum', maxComments] }, 100] }
}
},
{
$addFields: {
compositeScore: {
$add: [
{ $multiply: ['$retentionNum', 0.50] },
{ $multiply: ['$normalizedLikes', 0.25] },
{ $multiply: ['$interestScore', 0.15] },
{ $multiply: ['$normalizedComments', 0.10] }
]
}
}
},
{ $sort: { compositeScore: -1 } },
{ $limit: limit },
{
$project: {
postId: '$reelsList.postId',
userId: '$reelsList.userId',
username: '$reelsList.username',
imageUrl: '$reelsList.imageUrl',
caption: '$reelsList.caption',
category: '$reelsList.category',
profilePicUrl: '$reelsList.profile_picture_url',
timestamp: '$reelsList.timestamp',
likeCount: '$likeCountNum',
commentCount: '$commentCountNum',
retention: '$retentionNum',
compositeScore: '$compositeScore',
sourceDocument: slotId,
ratio: '9:16',
isReel: { $literal: true }
}
}
];

const reels = await db.collection('reels').aggregate(pipeline).toArray();

const duration = Date.now() - startTime;

console.log(`[post_algorithm] [READ-${readNum}-SUCCESS] ✅ Fetched ${reels.length} reels from slotId=${slotId} | duration=${duration}ms`);

// Log ranking details (first 3 items only for brevity)
reels.slice(0, 3).forEach((reel, idx) => {
console.log(`[post_algorithm] [REEL-${idx}] ${reel.postId.substring(0, 8)} | score=${reel.compositeScore.toFixed(1)} | retention=${reel.retention.toFixed(1)}% | likes=${reel.likeCount} | category=${reel.category || 'none'}`);
});

if (reels.length === 0) {
console.warn(`[post_algorithm] [EMPTY-SLOT] slotId=${slotId} has no reels (or all excluded)`);
}

return res.json({
success: true,
content: reels,
slotUsed: slotId,
reads: 1, // Single aggregation counts as 1 read
duration,
metadata: {
userInterests,
maxLikes,
maxComments,
excludedCount: excludedReelIds.length,
returnedCount: reels.length
}
});

} catch (error) {
console.error(`[post_algorithm] [READ-${readNum}-ERROR] ${error.message}`);
console.error(`[post_algorithm] [READ-${readNum}-STACK]`, error.stack);
return res.status(500).json({
success: false,
error: 'Failed to fetch reels content: ' + error.message,
reads: 1,
duration: Date.now() - startTime
});
}
});






/**
* POST /api/feed/mixed-optimized
* Returns mixed posts + reels with 6-content guarantee
* Implements progressive slot fallback
*/
app.post('/api/feed/mixed-optimized', async (req, res) => {
const startTime = Date.now();
const { userId, limit = DEFAULT_CONTENT_BATCH_SIZE } = req.body;

try {
console.log(`[post_algorithm] [MIXED-FEED-START] userId=${userId} | target=${limit}`);

const mixedContent = [];
const TARGET_CONTENT = Math.max(limit, DEFAULT_CONTENT_BATCH_SIZE);

// Phase 1: Latest Reels
console.log(`[post_algorithm] [PHASE-1] Fetching latest reels`);
const latestReelsResult = await dbManager.getOptimizedFeedFixedReads(
userId, 'reels', TARGET_CONTENT
);

mixedContent.push(...latestReelsResult.content);
console.log(`[post_algorithm] [PHASE-1-COMPLETE] Got ${latestReelsResult.content.length} reels | Total: ${mixedContent.length}/${TARGET_CONTENT}`);

if (mixedContent.length >= TARGET_CONTENT) {
console.log(`[post_algorithm] [TARGET-REACHED-PHASE-1] ✅ Sufficient content from latest reels only`);
return sendMixedResponse(res, mixedContent, startTime, 1);
}

// Phase 2: Latest Posts
console.log(`[post_algorithm] [PHASE-2] Need ${TARGET_CONTENT - mixedContent.length} more - fetching latest posts`);
const latestPostsResult = await dbManager.getOptimizedFeedFixedReads(
userId, 'posts', TARGET_CONTENT - mixedContent.length
);

mixedContent.push(...latestPostsResult.content);
console.log(`[post_algorithm] [PHASE-2-COMPLETE] Got ${latestPostsResult.content.length} posts | Total: ${mixedContent.length}/${TARGET_CONTENT}`);

if (mixedContent.length >= TARGET_CONTENT) {
console.log(`[post_algorithm] [TARGET-REACHED-PHASE-2] ✅ Sufficient content from latest slots`);
return sendMixedResponse(res, mixedContent, startTime, 2);
}

// Phase 3: Normal Reels
console.log(`[post_algorithm] [PHASE-3] Need ${TARGET_CONTENT - mixedContent.length} more - fetching normal reels`);
const normalReelsResult = await dbManager.getOptimizedFeedFixedReads(
userId, 'reels', TARGET_CONTENT - mixedContent.length
);

const normalReelsFiltered = normalReelsResult.content.filter(item =>
!mixedContent.some(existing => existing.postId === item.postId)
);

mixedContent.push(...normalReelsFiltered);
console.log(`[post_algorithm] [PHASE-3-COMPLETE] Got ${normalReelsFiltered.length} new reels | Total: ${mixedContent.length}/${TARGET_CONTENT}`);

if (mixedContent.length >= TARGET_CONTENT) {
console.log(`[post_algorithm] [TARGET-REACHED-PHASE-3] ✅ Sufficient content including normal reels`);
return sendMixedResponse(res, mixedContent, startTime, 3);
}

// Phase 4: Normal Posts
console.log(`[post_algorithm] [PHASE-4] Need ${TARGET_CONTENT - mixedContent.length} more - fetching normal posts`);
const normalPostsResult = await dbManager.getOptimizedFeedFixedReads(
userId, 'posts', TARGET_CONTENT - mixedContent.length
);

const normalPostsFiltered = normalPostsResult.content.filter(item =>
!mixedContent.some(existing => existing.postId === item.postId)
);

mixedContent.push(...normalPostsFiltered);
console.log(`[post_algorithm] [PHASE-4-COMPLETE] Got ${normalPostsFiltered.length} new posts | Total: ${mixedContent.length}/${TARGET_CONTENT}`);

if (mixedContent.length >= TARGET_CONTENT) {
console.log(`[post_algorithm] [TARGET-REACHED-PHASE-4] ✅ Sufficient content from all normal slots`);
return sendMixedResponse(res, mixedContent, startTime, 4);
}

// Phase 5: Previous slots (if still insufficient)
console.log(`[post_algorithm] [PHASE-5] ⚠️ Still need ${TARGET_CONTENT - mixedContent.length} more - trying previous slots`);

// Get user status to find previous slot
const userStatus = await db.collection('user_status').findOne({ _id: userId });

if (userStatus && userStatus.normalReelSlotId) {
const previousSlotId = calculatePreviousSlot(userStatus.normalReelSlotId);

if (previousSlotId) {
console.log(`[post_algorithm] [PHASE-5-PREVIOUS] Trying slot: ${previousSlotId}`);

const previousResult = await db.collection('reels').aggregate([
{ $match: { _id: previousSlotId } },
{ $unwind: '$reelsList' },
{ $limit: TARGET_CONTENT - mixedContent.length },
{ $project: { content: '$reelsList' } }
]).toArray();

const previousContent = previousResult.map(r => r.content);
const previousFiltered = previousContent.filter(item =>
!mixedContent.some(existing => existing.postId === item.postId)
);

mixedContent.push(...previousFiltered);
console.log(`[post_algorithm] [PHASE-5-COMPLETE] Got ${previousFiltered.length} from previous | Total: ${mixedContent.length}/${TARGET_CONTENT}`);
}
}

// Final check
if (mixedContent.length < TARGET_CONTENT) {
console.warn(`[post_algorithm] [INSUFFICIENT-CONTENT] ⚠️ Only got ${mixedContent.length}/${TARGET_CONTENT} items after all phases`);
} else {
console.log(`[post_algorithm] [TARGET-REACHED-FINAL] ✅ Got ${mixedContent.length} items total`);
}

return sendMixedResponse(res, mixedContent, startTime, 5);

} catch (error) {
console.error(`[post_algorithm] [MIXED-FEED-ERROR]`, error);
return res.status(500).json({
success: false,
error: error.message,
duration: Date.now() - startTime
});
}
});

/**
* Helper: Send mixed content response with metadata
*/
function sendMixedResponse(res, mixedContent, startTime, phaseReached) {
const duration = Date.now() - startTime;

// Sort by composite score (if available)
const sortedContent = mixedContent.sort((a, b) =>
(b.compositeScore || b.retention || 0) - (a.compositeScore || a.retention || 0)
);

// Separate for metadata
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

/**
* Helper: Calculate previous slot ID
*/
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

// Fetch user interests (best-effort)
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
// interestScore logic: if user has interests and category matches -> 100, else 0; if user has no interests -> 50
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
// Primary image (fallbacks kept for compatibility)
imageUrl: { $ifNull: ['$postList.imageUrl', '$postList.imageUrl1'] },
// multi-image support
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
