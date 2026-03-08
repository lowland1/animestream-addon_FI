const express = require('express');
const workerModule = require('./worker-github.js');  // Loads your working worker file

// Detect the fetch handler (covers different export styles)
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

// Support large raw bodies (for any POST or proxy needs)
app.use(express.raw({ type: '*/*', limit: '200mb' }));

// Add CORS headers globally
app.use((req, res, next) => {
  res.setHeader('Access-Control-Allow-Origin', '*');
  res.setHeader('Access-Control-Allow-Methods', 'GET, POST, OPTIONS');
  res.setHeader('Access-Control-Allow-Headers', 'Content-Type, Range');
  next();
});

// Handle OPTIONS preflight
app.options('*', (req, res) => res.sendStatus(200));

app.all('*', async (req, res) => {
  console.log(`[${new Date().toISOString()}] ${req.method} ${req.originalUrl} - from IP: ${req.ip}`);

  try {
    // Build full URL using host from request (Render sets headers.host)
    const url = new URL(req.originalUrl, `https://${req.headers.host || 'localhost:' + port}`);
    const headers = new Headers();
    for (const [key, val] of Object.entries(req.headers)) {
      if (val) headers.append(key, Array.isArray(val) ? val.join(', ') : val);
    }

    const request = new Request(url.toString(), {
      method: req.method,
      headers,
      body: req.method === 'GET' || req.method === 'HEAD' ? null : req.body,
      redirect: 'manual',
    });

    const env = {};  // Add any needed env vars here later if logs complain
    const ctx = {
      waitUntil: (p) => p.catch(e => console.error('waitUntil error:', e)),
      passThroughOnException: () => {},
    };

    const response = await workerFetch(request, env, ctx);

    console.log(`Worker returned status: ${response.status}`);

    res.status(response.status || 200);

    // Copy all headers from worker response
    response.headers.forEach((value, key) => {
      res.setHeader(key, value);
    });

    // Safe body handling – this fixes the pipe error
    if (response.body) {
      console.log('Response has body – attempting to stream');

      // Check if it's a web ReadableStream (has .getReader / .pipeTo)
      if (typeof response.body.getReader === 'function') {
        console.log('Detected web ReadableStream – using pipeTo fallback');
        await response.body.pipeTo(new WritableStream({
          write(chunk) {
            res.write(chunk);
          },
          close() {
            res.end();
          },
          abort(err) {
            console.error('Stream aborted:', err);
            res.end();
          }
        }));
      } else if (typeof response.body.pipe === 'function') {
        // Node.js Readable – direct pipe
        console.log('Detected Node Readable – using .pipe(res)');
        response.body.pipe(res);
        // Express handles end() on finish
      } else {
        // Fallback: consume as text (for small responses like JSON)
        console.log('Body not streamable – falling back to text');
        const text = await response.text();
        res.send(text);
      }
    } else {
      // No body at all (e.g. 204 No Content)
      console.log('No response body');
      res.send(await response.text() || '');
    }
  } catch (error) {
    console.error('Worker execution failed:', error.stack || error.message);
    res.status(500).send(
      `Internal Server Error\n\n` +
      `Details: ${error.message || 'Unknown error'}\n` +
      `Check Render logs for full stack trace.`
    );
  }
});

app.listen(port, () => {
  console.log(`Worker proxy server running on port ${port}`);
});
