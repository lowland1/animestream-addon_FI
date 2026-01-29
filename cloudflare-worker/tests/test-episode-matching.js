/**
 * Test Suite: Episode Matching Logic
 * Tests the extractEpisodeInfo function and loose pattern matching
 * 
 * Run with: node tests/test-episode-matching.js
 */

// Extract the functions from the worker file for testing
const fs = require('fs');
const path = require('path');

// Read and extract the function from worker-github.js
const workerCode = fs.readFileSync(path.join(__dirname, '../worker-github.js'), 'utf-8');

// Extract extractEpisodeInfo function
const extractEpisodeInfoMatch = workerCode.match(/function extractEpisodeInfo\(title\) \{[\s\S]*?^function /m);
if (!extractEpisodeInfoMatch) {
  console.error('Could not extract extractEpisodeInfo function');
  process.exit(1);
}
const extractEpisodeInfoCode = extractEpisodeInfoMatch[0].replace(/^function $/, '');
eval(extractEpisodeInfoCode);

console.log('========================================');
console.log('  EPISODE MATCHING TEST SUITE');
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

function assertMatch(obj, expected, msg = '') {
  for (const key of Object.keys(expected)) {
    if (obj[key] !== expected[key]) {
      throw new Error(`Expected ${key}=${expected[key]}, got ${obj[key]}. ${msg}`);
    }
  }
}

// ====================================================
// Test extractEpisodeInfo with various filename formats
// ====================================================

console.log('\n--- extractEpisodeInfo Tests ---\n');

// Standard anime format " - 05"
test('Anime standard: "[SubsPlease] Dan Da Dan - 04 (1080p).mkv"', () => {
  const info = extractEpisodeInfo('[SubsPlease] Dan Da Dan - 04 (1080p).mkv');
  assertMatch(info, { episode: 4, isAbsolute: true });
});

test('Anime standard: "[SubsPlease] Frieren - 28 (1080p).mkv"', () => {
  const info = extractEpisodeInfo('[SubsPlease] Frieren - 28 (1080p).mkv');
  assertMatch(info, { episode: 28, isAbsolute: true });
});

// S01E05 format
test('Western format: "Dan.Da.Dan.S01E04.1080p.WEB.x264.mkv"', () => {
  const info = extractEpisodeInfo('Dan.Da.Dan.S01E04.1080p.WEB.x264.mkv');
  assertMatch(info, { season: 1, episode: 4 });
});

test('Western format: "Fire.Force.S02E10.720p.mkv"', () => {
  const info = extractEpisodeInfo('Fire.Force.S02E10.720p.mkv');
  assertMatch(info, { season: 2, episode: 10 });
});

// Season with dash format
test('Season dash: "[Judas] Boku no Hero Academia S5 - 04.mkv"', () => {
  const info = extractEpisodeInfo('[Judas] Boku no Hero Academia S5 - 04.mkv');
  assertMatch(info, { season: 5, episode: 4 });
});

// Episode word format
test('Episode word: "Dan Da Dan Episode 04 [1080p].mkv"', () => {
  const info = extractEpisodeInfo('Dan Da Dan Episode 04 [1080p].mkv');
  assertMatch(info, { episode: 4 });
});

// Bracketed episode
test('Bracketed: "[Group] Anime Name [04] [1080p].mkv"', () => {
  const info = extractEpisodeInfo('[Group] Anime Name [04] [1080p].mkv');
  assertMatch(info, { episode: 4 });
});

// Batch detection
test('Batch range: "[Group] Dan Da Dan [01-12] [1080p].mkv"', () => {
  const info = extractEpisodeInfo('[Group] Dan Da Dan [01-12] [1080p].mkv');
  assertEqual(info.isBatch, true, 'Should be batch');
  assertEqual(info.batchRange[0], 1, 'Start should be 1');
  assertEqual(info.batchRange[1], 12, 'End should be 12');
});

test('Batch Season: "[Group] Dan Da Dan Season 1 Complete [1080p].mkv"', () => {
  const info = extractEpisodeInfo('[Group] Dan Da Dan Season 1 Complete [1080p].mkv');
  assertEqual(info.isBatch, true, 'Should be batch');
});

// Version numbers
test('Version: "[SubsPlease] Dan Da Dan - 04v2 (1080p).mkv"', () => {
  const info = extractEpisodeInfo('[SubsPlease] Dan Da Dan - 04v2 (1080p).mkv');
  assertMatch(info, { episode: 4, isAbsolute: true });
});

// Edge cases - should NOT match wrong episodes
test('No false positive: "Dan Da Dan 1080p" should not detect episode', () => {
  const info = extractEpisodeInfo('[Group] Dan Da Dan [1080p].mkv');
  // 1080 should NOT be treated as episode
  assertEqual(info.episode === 1080, false, '1080 should not be episode');
});

test('Year not episode: "Movie (2024) [1080p].mkv"', () => {
  const info = extractEpisodeInfo('Some Movie (2024) [1080p].mkv');
  assertEqual(info.episode === 2024, false, '2024 should not be episode');
});

// ====================================================
// Test loose pattern matching (file selection)
// ====================================================

console.log('\n--- Loose Pattern Matching Tests ---\n');

function testLoosePatterns(filename, targetEpisode, shouldMatch) {
  const episodePadded = String(targetEpisode).padStart(2, '0');
  const episodePadded3 = String(targetEpisode).padStart(3, '0');
  const fn = filename.toLowerCase();
  
  const loosePatterns = [
    new RegExp(`\\s-\\s${episodePadded}(?:v\\d+)?(?:\\s|\\.|\\[|\\(|$)`, 'i'),      // " - 04"
    new RegExp(`\\bE${episodePadded}(?:v\\d+)?(?:\\s|\\.|\\[|\\(|$)`, 'i'),       // "E04" with word boundary
    new RegExp(`\\bEp\\.?\\s*${episodePadded}(?:v\\d+)?(?:\\s|\\.|\\[|\\(|$)`, 'i'), // "Ep.04" or "Ep 04"
    new RegExp(`\\bEpisode\\s*${episodePadded}(?:v\\d+)?(?:\\s|\\.|\\[|\\(|$)`, 'i'), // "Episode 04"
    new RegExp(`\\[${episodePadded}\\]`, 'i'),                                    // "[04]"
    new RegExp(`_${episodePadded}(?:v\\d+)?(?:_|\\.)`, 'i'),                       // "_04_" or "_04."
    new RegExp(`\\.${episodePadded}(?:v\\d+)?\\.`, 'i'),                           // ".04."
    new RegExp(`[\\s_.-]${episodePadded}(?:v\\d+)?\\.[a-z0-9]{2,4}$`, 'i'),
    new RegExp(`[\\s_.-]${episodePadded3}(?:v\\d+)?\\.[a-z0-9]{2,4}$`, 'i'),
  ];
  
  const matched = loosePatterns.some(p => p.test(fn));
  return matched === shouldMatch;
}

// Positive matches
test('Loose match: " - 04.mkv" matches episode 4', () => {
  assertEqual(testLoosePatterns('Dan Da Dan - 04.mkv', 4, true), true);
});

test('Loose match: "E04.mkv" matches episode 4', () => {
  assertEqual(testLoosePatterns('Dan Da Dan E04.mkv', 4, true), true);
});

test('Loose match: "Episode 04.mkv" matches episode 4', () => {
  assertEqual(testLoosePatterns('Dan Da Dan Episode 04.mkv', 4, true), true);
});

test('Loose match: "[04].mkv" matches episode 4', () => {
  assertEqual(testLoosePatterns('Dan Da Dan [04].mkv', 4, true), true);
});

test('Loose match: "_04_.mkv" matches episode 4', () => {
  assertEqual(testLoosePatterns('Dan_Da_Dan_04_.mkv', 4, true), true);
});

test('Loose match: ".04.mkv" at end matches episode 4', () => {
  assertEqual(testLoosePatterns('Dan.Da.Dan.04.mkv', 4, true), true);
});

// Negative matches - CRITICAL: Should NOT match wrong episodes
test('NO false positive: "E40.mkv" should NOT match episode 4', () => {
  assertEqual(testLoosePatterns('Dan Da Dan E40.mkv', 4, false), true);
});

test('NO false positive: "Episode 41.mkv" should NOT match episode 4', () => {
  assertEqual(testLoosePatterns('Dan Da Dan Episode 41.mkv', 4, false), true);
});

test('NO false positive: " - 40.mkv" should NOT match episode 4', () => {
  assertEqual(testLoosePatterns('Dan Da Dan - 40.mkv', 4, false), true);
});

test('NO false positive: "[40].mkv" should NOT match episode 4', () => {
  assertEqual(testLoosePatterns('Dan Da Dan [40].mkv', 4, false), true);
});

test('NO false positive: "1080p" should NOT match episode 10', () => {
  assertEqual(testLoosePatterns('Dan Da Dan [1080p].mkv', 10, false), true);
});

// ====================================================
// Test batch torrent file selection scenarios
// ====================================================

console.log('\n--- Batch File Selection Tests ---\n');

const sampleBatchFiles = [
  { filename: '[SubsPlease] Dan Da Dan - 01 (1080p).mkv', size: 1400000000 },
  { filename: '[SubsPlease] Dan Da Dan - 02 (1080p).mkv', size: 1350000000 },
  { filename: '[SubsPlease] Dan Da Dan - 03 (1080p).mkv', size: 1380000000 },
  { filename: '[SubsPlease] Dan Da Dan - 04 (1080p).mkv', size: 1420000000 },
  { filename: '[SubsPlease] Dan Da Dan - 05 (1080p).mkv', size: 1390000000 },
  { filename: '[SubsPlease] Dan Da Dan - 06 (1080p).mkv', size: 1410000000 },
];

function selectFileFromBatch(files, targetEpisode) {
  // First: Try extractEpisodeInfo (exact match)
  for (const file of files) {
    const info = extractEpisodeInfo(file.filename);
    if (info.episode === targetEpisode) {
      return { file, method: 'exact' };
    }
  }
  
  // Second: Try loose patterns
  const episodePadded = String(targetEpisode).padStart(2, '0');
  const episodePadded3 = String(targetEpisode).padStart(3, '0');
  
  const loosePatterns = [
    new RegExp(`\\s-\\s${episodePadded}(?:v\\d+)?(?:\\s|\\.|\\[|\\(|$)`, 'i'),
    new RegExp(`\\bE${episodePadded}(?:v\\d+)?(?:\\s|\\.|\\[|\\(|$)`, 'i'),
    new RegExp(`\\[${episodePadded}\\]`, 'i'),
    new RegExp(`[\\s_.-]${episodePadded}(?:v\\d+)?\\.[a-z0-9]{2,4}$`, 'i'),
  ];
  
  for (const file of files) {
    const fn = file.filename.toLowerCase();
    for (const pattern of loosePatterns) {
      if (pattern.test(fn)) {
        return { file, method: 'loose' };
      }
    }
  }
  
  return null; // No match - should NOT fall back to largest file
}

test('Batch: Select episode 4 from batch', () => {
  const result = selectFileFromBatch(sampleBatchFiles, 4);
  assertEqual(result !== null, true, 'Should find episode 4');
  assertEqual(result.file.filename.includes('- 04'), true, 'Should be episode 4 file');
});

test('Batch: Select episode 6 from batch', () => {
  const result = selectFileFromBatch(sampleBatchFiles, 6);
  assertEqual(result !== null, true, 'Should find episode 6');
  assertEqual(result.file.filename.includes('- 06'), true, 'Should be episode 6 file');
});

test('Batch: Episode 10 not in batch returns null (not largest file)', () => {
  const result = selectFileFromBatch(sampleBatchFiles, 10);
  assertEqual(result, null, 'Should return null for missing episode');
});

test('Batch: Episode 40 not in batch returns null', () => {
  const result = selectFileFromBatch(sampleBatchFiles, 40);
  assertEqual(result, null, 'Should return null for episode 40');
});

// ====================================================
// Summary
// ====================================================

console.log('\n========================================');
console.log(`  RESULTS: ${passed} passed, ${failed} failed`);
console.log('========================================\n');

process.exit(failed > 0 ? 1 : 0);
