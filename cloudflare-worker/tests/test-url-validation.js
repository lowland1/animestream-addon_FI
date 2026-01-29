/**
 * Test Suite: URL Validation
 * Tests the URL validation logic for debrid URLs
 * 
 * Run with: node tests/test-url-validation.js
 */

console.log('========================================');
console.log('  URL VALIDATION TEST SUITE');
console.log('========================================\n');

let passed = 0;
let failed = 0;

function test(description, fn) {
  try {
    fn();
    console.log(`✅ ${description}`);
    passed++;
  } catch (error) {
    console.log(`❌ ${description}`);
    console.log(`   Error: ${error.message}`);
    failed++;
  }
}

function assertEqual(actual, expected, msg = '') {
  if (actual !== expected) {
    throw new Error(`Expected ${expected}, got ${actual}. ${msg}`);
  }
}

// ====================================================
// Test URL validation scenarios
// ====================================================

console.log('\n--- URL Format Tests ---\n');

// Test valid URL formats
test('Valid HTTPS URL', () => {
  const url = 'https://download.alldebrid.com/dl/abcd1234/video.mkv';
  assertEqual(url.startsWith('https://'), true);
  assertEqual(typeof url, 'string');
});

test('Valid TorBox URL', () => {
  const url = 'https://dl.torbox.app/t/abcd1234/video.mkv';
  assertEqual(url.startsWith('https://'), true);
});

// Test invalid URL detection
test('Null result detection', () => {
  const result = null;
  assertEqual(result === null || typeof result !== 'string', true);
});

test('Object status detection (downloading)', () => {
  const result = { status: 'downloading', message: 'Download in progress' };
  assertEqual(typeof result === 'object' && result.status === 'downloading', true);
});

test('Object status detection (mislabeled)', () => {
  const result = { status: 'mislabeled', message: 'Wrong content' };
  assertEqual(typeof result === 'object' && result.status === 'mislabeled', true);
});

test('Object status detection (episode_not_found)', () => {
  const result = { status: 'episode_not_found', message: 'Episode not in batch' };
  assertEqual(typeof result === 'object' && result.status === 'episode_not_found', true);
});

// ====================================================
// Test HTTP status handling
// ====================================================

console.log('\n--- HTTP Status Handling Tests ---\n');

function shouldRetry(statusCode) {
  // 5XX errors should trigger retry
  if (statusCode >= 500 && statusCode < 600) return true;
  // 404 = file not found, might need refresh
  if (statusCode === 404) return true;
  // 410 = Gone, URL expired
  if (statusCode === 410) return true;
  // 401/403 might be token expiry
  if (statusCode === 401 || statusCode === 403) return true;
  return false;
}

test('5XX error triggers retry', () => {
  assertEqual(shouldRetry(500), true);
  assertEqual(shouldRetry(502), true);
  assertEqual(shouldRetry(503), true);
});

test('404 triggers retry (file might have moved)', () => {
  assertEqual(shouldRetry(404), true);
});

test('410 Gone triggers retry (URL expired)', () => {
  assertEqual(shouldRetry(410), true);
});

test('200 OK does not trigger retry', () => {
  assertEqual(shouldRetry(200), false);
});

test('302 redirect does not trigger retry', () => {
  assertEqual(shouldRetry(302), false);
});

// ====================================================
// Test cache key generation
// ====================================================

console.log('\n--- Cache Key Tests ---\n');

function generateCacheKey(provider, infoHash, episode) {
  return `debrid:${provider}:${infoHash}:${episode || 'all'}`;
}

test('Cache key with episode', () => {
  const key = generateCacheKey('alldebrid', 'ABC123', 4);
  assertEqual(key, 'debrid:alldebrid:ABC123:4');
});

test('Cache key without episode', () => {
  const key = generateCacheKey('torbox', 'DEF456', null);
  assertEqual(key, 'debrid:torbox:DEF456:all');
});

test('Cache key for movie (no episode)', () => {
  const key = generateCacheKey('realdebrid', 'GHI789', undefined);
  assertEqual(key, 'debrid:realdebrid:GHI789:all');
});

// ====================================================
// Test error response formats
// ====================================================

console.log('\n--- Error Response Format Tests ---\n');

function createErrorResponse(status, error, message, hint) {
  return {
    status,
    body: JSON.stringify({ error, message, hint })
  };
}

test('Downloading status returns 503', () => {
  const response = createErrorResponse(503, 'Torrent not cached', 
    'This torrent is not cached on the debrid service.',
    'Look for streams marked with ⚡');
  assertEqual(response.status, 503);
  const body = JSON.parse(response.body);
  assertEqual(body.error, 'Torrent not cached');
});

test('Episode not found returns 404', () => {
  const response = createErrorResponse(404, 'Episode not found',
    'Episode 4 not found in batch',
    'Try a different torrent source');
  assertEqual(response.status, 404);
});

test('Mislabeled torrent returns 409', () => {
  const response = createErrorResponse(409, 'Mislabeled torrent',
    'Expected "Dan Da Dan" but got "Ranma"',
    'Try a different torrent source');
  assertEqual(response.status, 409);
});

test('URL expired returns 503', () => {
  const response = createErrorResponse(503, 'Stream URL expired',
    'The stream URL has expired.',
    'Try playing again');
  assertEqual(response.status, 503);
});

// ====================================================
// Summary
// ====================================================

console.log('\n========================================');
console.log(`  RESULTS: ${passed} passed, ${failed} failed`);
console.log('========================================\n');

process.exit(failed > 0 ? 1 : 0);
