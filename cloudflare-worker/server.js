const express = require('express');
const workerModule = require('./worker-github.js');  // Change to './worker-github.js' if that's the one with proxy

// Get the fetch handler – common patterns
let workerFetch;
if (typeof workerModule === 'function') {
  workerFetch = workerModule;  // if default export is fetch
} else if (workerModule.fetch) {
  workerFetch = workerModule.fetch;  // export { fetch }
} else if (workerModule.default && workerModule.default.fetch) {
  workerFetch = workerModule.default.fetch;
} else {
  throw new Error('No valid fetch handler found in worker.js');
}

const app = express();
const port = process.env.PORT || 3000;

app.use(express.raw({ type: '*/*', limit: '50mb' }));  // For large bodies if any

app.all('*', async (req, res) => {
  console.log(`Request: ${req.method} ${req.originalUrl}`);  // Log every request

  const url = new URL(req.originalUrl, `http://localhost:${port}`);
  const headers = new Headers();
  Object.entries(req.headers).forEach(([key, value]) => {
    if (value) headers.append(key, Array.isArray(value) ? value.join(', ') : value);
  });

  const request = new Request(url, {
    method: req.method,
    headers,
    body: req.method !== 'GET' && req.method !== 'HEAD' ? req.body : null,
    redirect: 'manual',
  });

  const env = {};  // Mock – add real if needed, e.g. env = { SOME_VAR: process.env.SOME_VAR }
  const ctx = {
    waitUntil: (promise) => promise.catch(console.error),
    passThroughOnException: () => {},
  };

  try {
    const response = await workerFetch(request, env, ctx);

    res.status(response.status || 200);

    for (const [key, value] of response.headers.entries()) {
      res.setHeader(key, value);
    }

    // Better streaming for videos
    if (response.body) {
      response.body.pipeTo(new WritableStream({
        write(chunk) {
          res.write(chunk);
        },
        close() {
          res.end();
        },
        abort(err) {
          console.error('Stream error:', err);
          res.end();
        }
      })).catch(err => console.error('Pipe error:', err));
    } else {
      res.send(await response.text());
    }
  } catch (error) {
    console.error('Worker error:', error);
    res.status(500).send('Internal Server Error: ' + (error.message || 'Unknown'));
  }
});

app.listen(port, () => {
  console.log(`Server running on port ${port}`);
});
