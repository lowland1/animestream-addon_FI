const express = require('express');
const workerModule = require('./worker-github.js');  // Your working file

// Detect the fetch handler
let workerFetch;
if (typeof workerModule === 'function') {
  workerFetch = workerModule;
} else if (workerModule.fetch && typeof workerModule.fetch === 'function') {
  workerFetch = workerModule.fetch;
} else if (workerModule.default && typeof workerModule.default === 'function') {
  workerFetch = workerModule.default;
} else if (workerModule.default && workerModule.default.fetch) {
  workerFetch = workerModule.default.fetch;
} else {
  throw new Error('Could not find a valid fetch handler in worker-github.js. Check export style.');
}

console.log('Successfully loaded worker fetch handler from worker-github.js');

const app = express();
const port = process.env.PORT || 3000;

// Handle raw/binary bodies for proxy/stream requests
app.use(express.raw({ type: '*/*', limit: '200mb' }));
app.use((req, res, next) => {
  res.setHeader('Access-Control-Allow-Origin', '*');
  res.setHeader('Access-Control-Allow-Methods', 'GET, POST, OPTIONS');
  next();
});

app.options('*', (req, res) => res.sendStatus(200));

app.all('*', async (req, res) => {
  console.log(`[${new Date().toISOString()}] ${req.method} ${req.originalUrl}`);

  try {
    const url = new URL(req.originalUrl, `https://${req.headers.host || 'localhost'}`);
    const headers = new Headers();
    for (const [key, val] of Object.entries(req.headers)) {
      if (val) headers.append(key, Array.isArray(val) ? val.join(', ') : val);
    }

    // Create CF-like Request
    const request = new Request(url.toString(), {
      method: req.method,
      headers,
      body: req.method === 'GET' || req.method === 'HEAD' ? null : req.body,
      redirect: 'manual',
    });

    const env = {}; // If your script uses env vars/bindings, add them here e.g. env.API_KEY = 'xxx'
    const ctx = {
      waitUntil: (p) => p.catch(e => console.error('waitUntil error:', e)),
      passThroughOnException: () => {},
    };

    const response = await workerFetch(request, env, ctx);

    res.status(response.status);

    // Copy all headers
    response.headers.forEach((value, key) => {
      res.setHeader(key, value);
    });

    // Handle streaming response (critical for video proxy)
    if (response.body) {
      const reader = response.body.getReader();
      const stream = new ReadableStream({
        async pull(controller) {
          try {
            const { done, value } = await reader.read();
            if (done) {
              controller.close();
              return;
            }
            controller.enqueue(value);
          } catch (err) {
            controller.error(err);
          }
        },
        cancel() {
          reader.releaseLock();
        }
      });

      // Pipe to Express response
      stream.pipe(res);
    } else {
      res.send(await response.text());
    }
  } catch (error) {
    console.error('Worker execution failed:', error.stack || error);
    res.status(500).send(`Internal Server Error\n\nDetails: ${error.message || 'Unknown error'}\nCheck server logs for stack trace.`);
  }
});

app.listen(port, () => {
  console.log(`Worker proxy server running on port ${port}`);
});
