'use strict';

const cluster = require('cluster');
const os = require('os');

const numCPUs = process.env.CLUSTER_WORKERS || os.cpus().length;

if (cluster.isMaster) {
  console.log(`[CLUSTER-MASTER] Starting ${numCPUs} workers...`);
  
  // Fork workers
  for (let i = 0; i < numCPUs; i++) {
    cluster.fork();
  }
  
  cluster.on('exit', (worker, code, signal) => {
    console.log(`[CLUSTER] Worker ${worker.process.pid} died. Restarting...`);
    cluster.fork();
  });
  
  cluster.on('online', (worker) => {
    console.log(`[CLUSTER] Worker ${worker.process.pid} is online`);
  });
  
} else {
  // Workers run the actual server
  require('./server.js');
}