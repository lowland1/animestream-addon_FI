const express = require('express');
const { fetch } = require('./worker.js');  // Adjust if using worker-github.js instead

const app = express();
const port = process.env.PORT || 3000;

// Middleware to handle raw body for non-GET requests
app.use(express.raw({ type: '*/*' }));

app.all('*', async (req, res) => {
  const url = new URL(req.originalUrl, `https://${req.headers.host}`);
  const headers = new Headers();
  for (const [key, value] of Object.entries(req.headers)) {
    headers.append(key, value);
  }

  const request = new Request(url, {
    method: req.method,
    headers,
    body: req.method !== 'GET' && req.method !== 'HEAD' ? req.body : null,
    redirect: 'manual',
  });

  // Simulate env and ctx (adjust if your worker uses specific env vars like KV)
  const env = {};  // Add any env vars your worker needs, e.g., env.MY_KV = someBinding;
  const ctx = { waitUntil: () => Promise.resolve(), passThroughOnException: () => {} };

  try {
    const response = await fetch(request, env, ctx);
    res.status(response.status);
    response.headers.forEach((value, key) => {
      res.setHeader(key, value);
    });
    const body = await response.arrayBuffer();
    res.send(Buffer.from(body));
  } catch (error) {
    res.status(500).send('Internal Server Error');
  }
});

app.listen(port, () => {
  console.log(`Server running on port ${port}`);
});
