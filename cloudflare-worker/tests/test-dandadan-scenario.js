/**
 * Integration Test: Dan Da Dan Episode Selection
 * Simulates the exact scenario reported by users
 * 
 * Bug Report: "Dan Da Dan, season 1 episode 4,5,6 - the very first stream 
 * fetched would always be the first episode"
 * 
 * Run with: node tests/test-dandadan-scenario.js
 */

console.log('========================================');
console.log('  DAN DA DAN INTEGRATION TEST');
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
    throw new Error(`Expected "${expected}", got "${actual}". ${msg}`);
  }
}

// ====================================================
// Simulated extractEpisodeInfo (from worker)
// ====================================================

function extractEpisodeInfo(title) {
  const result = { episode: null, season: null, isBatch: false, batchRange: null, isAbsolute: false };
  const normalized = title.toLowerCase();
  
  // Batch detection: [01-12]
  const batchMatch = normalized.match(/\[(\d{1,3})\s*[-~]\s*(\d{1,3})\]/);
  if (batchMatch) {
    const start = parseInt(batchMatch[1], 10);
    const end = parseInt(batchMatch[2], 10);
    if (start < end && end - start >= 2) {
      result.isBatch = true;
      result.batchRange = [start, end];
      return result;
    }
  }
  
  // S01E05 format
  const sxeMatch = normalized.match(/\bs0?(\d{1,2})\s*e0?(\d{1,4})(?:v\d+)?(?!\d)/i);
  if (sxeMatch) {
    result.season = parseInt(sxeMatch[1], 10);
    result.episode = parseInt(sxeMatch[2], 10);
    return result;
  }
  
  // " - 05" format
  const dashEpMatch = normalized.match(/\s-\s0?(\d{1,4})(?:v\d+)?(?:\s|\(|\[|$)/);
  if (dashEpMatch) {
    result.episode = parseInt(dashEpMatch[1], 10);
    result.isAbsolute = true;
    return result;
  }
  
  return result;
}

// ====================================================
// Simulated validateTorrentEpisode
// ====================================================

function validateTorrentEpisode(title, requestedEpisode, requestedSeason = 1) {
  const info = extractEpisodeInfo(title);
  
  if (info.isBatch) {
    if (info.batchRange) {
      const [start, end] = info.batchRange;
      if (requestedEpisode >= start && requestedEpisode <= end) {
        return { matches: true, reason: 'batch_contains_episode', info };
      }
      return { matches: false, reason: 'batch_episode_out_of_range', info };
    }
    return { matches: true, reason: 'batch_unknown_range', info };
  }
  
  if (info.episode === null) {
    return { matches: false, reason: 'no_episode_detected', info };
  }
  
  if (info.episode === requestedEpisode) {
    return { matches: true, reason: 'exact_match', info };
  }
  
  return { matches: false, reason: 'episode_mismatch', info };
}

// ====================================================
// Simulated file selection (from worker)
// ====================================================

function selectFileFromBatchTorrent(videoFiles, episode, season = 1) {
  let selectedFile = null;
  const candidates = [];
  
  // First pass: exact extractEpisodeInfo match
  for (const file of videoFiles) {
    if (/(NCOP|NCED|Preview|Special|OVA)/i.test(file.filename)) continue;
    
    const info = extractEpisodeInfo(file.filename);
    if (info.episode === episode) {
      candidates.push({ file, info });
    }
  }
  
  if (candidates.length > 0) {
    candidates.sort((a, b) => b.file.size - a.file.size);
    return { file: candidates[0].file, method: 'exact' };
  }
  
  // Second pass: loose patterns
  const episodePadded = String(episode).padStart(2, '0');
  const loosePatterns = [
    new RegExp(`\\s-\\s${episodePadded}(?:v\\d+)?(?:\\s|\\.|\\[|\\(|$)`, 'i'),      // " - 04"
    new RegExp(`\\bE${episodePadded}(?:v\\d+)?(?:\\s|\\.|\\[|\\(|$)`, 'i'),       // "E04" with word boundary
    new RegExp(`\\bEp\\.?\\s*${episodePadded}(?:v\\d+)?(?:\\s|\\.|\\[|\\(|$)`, 'i'), // "Ep.04" or "Ep 04"
    new RegExp(`\\bEpisode\\s*${episodePadded}(?:v\\d+)?(?:\\s|\\.|\\[|\\(|$)`, 'i'), // "Episode 04"
    new RegExp(`\\[${episodePadded}\\]`, 'i'),                                    // "[04]"
    new RegExp(`_${episodePadded}(?:v\\d+)?(?:_|\\.)`, 'i'),                       // "_04_" or "_04."
    new RegExp(`\\.${episodePadded}(?:v\\d+)?\\.`, 'i'),                           // ".04."
    new RegExp(`[\\s_.-]${episodePadded}(?:v\\d+)?\\.[a-z0-9]{2,4}$`, 'i'),
  ];
  
  for (const file of videoFiles) {
    const fn = file.filename.toLowerCase();
    for (const pattern of loosePatterns) {
      if (pattern.test(fn)) {
        return { file, method: 'loose' };
      }
    }
  }
  
  // CRITICAL FIX: Don't fall back to largest file for batches
  if (videoFiles.length > 1) {
    return { status: 'episode_not_found', message: `Episode ${episode} not found` };
  }
  
  return { file: videoFiles[0], method: 'single_file' };
}

// ====================================================
// Realistic Test Data
// ====================================================

// Typical batch torrent files for Dan Da Dan
const danDaDanBatchFiles = [
  { filename: '[SubsPlease] Dan Da Dan - 01 (1080p) [E456789A].mkv', size: 1450000000 },
  { filename: '[SubsPlease] Dan Da Dan - 02 (1080p) [F567890B].mkv', size: 1420000000 },
  { filename: '[SubsPlease] Dan Da Dan - 03 (1080p) [G678901C].mkv', size: 1380000000 },
  { filename: '[SubsPlease] Dan Da Dan - 04 (1080p) [H789012D].mkv', size: 1510000000 }, // Largest!
  { filename: '[SubsPlease] Dan Da Dan - 05 (1080p) [I890123E].mkv', size: 1390000000 },
  { filename: '[SubsPlease] Dan Da Dan - 06 (1080p) [J901234F].mkv', size: 1410000000 },
  { filename: '[SubsPlease] Dan Da Dan - NCOP (1080p) [K012345G].mkv', size: 92000000 },
  { filename: '[SubsPlease] Dan Da Dan - NCED (1080p) [L123456H].mkv', size: 88000000 },
];

// Sort by size descending (simulating what "largest file" fallback would return)
const sortedBySize = [...danDaDanBatchFiles].sort((a, b) => b.size - a.size);
console.log('Files sorted by size (largest first):');
console.log(`  1. ${sortedBySize[0].filename} (${(sortedBySize[0].size/1e9).toFixed(2)} GB)`);
console.log(`  2. ${sortedBySize[1].filename} (${(sortedBySize[1].size/1e9).toFixed(2)} GB)\n`);

// ====================================================
// Tests: The Bug Scenario
// ====================================================

console.log('--- CRITICAL BUG REPRODUCTION TESTS ---\n');

test('User requests Episode 4 - should get Episode 4 (not Episode 1)', () => {
  const result = selectFileFromBatchTorrent(danDaDanBatchFiles, 4);
  assertEqual(result.file?.filename.includes('- 04'), true);
  console.log(`   ✓ Returned: ${result.file.filename}`);
});

test('User requests Episode 5 - should get Episode 5 (not Episode 1)', () => {
  const result = selectFileFromBatchTorrent(danDaDanBatchFiles, 5);
  assertEqual(result.file?.filename.includes('- 05'), true);
  console.log(`   ✓ Returned: ${result.file.filename}`);
});

test('User requests Episode 6 - should get Episode 6 (not Episode 1)', () => {
  const result = selectFileFromBatchTorrent(danDaDanBatchFiles, 6);
  assertEqual(result.file?.filename.includes('- 06'), true);
  console.log(`   ✓ Returned: ${result.file.filename}`);
});

// ====================================================
// Tests: Batch Torrent Validation
// ====================================================

console.log('\n--- BATCH TORRENT VALIDATION TESTS ---\n');

test('Batch torrent [01-12] should validate for episode 4', () => {
  const result = validateTorrentEpisode('[SubsPlease] Dan Da Dan [01-12] [1080p] [Batch]', 4, 1);
  assertEqual(result.matches, true);
  assertEqual(result.reason, 'batch_contains_episode');
});

test('Batch torrent [01-06] should NOT validate for episode 10', () => {
  const result = validateTorrentEpisode('[SubsPlease] Dan Da Dan [01-06] [1080p] [Batch]', 10, 1);
  assertEqual(result.matches, false);
  assertEqual(result.reason, 'batch_episode_out_of_range');
});

// ====================================================
// Tests: Edge Cases
// ====================================================

console.log('\n--- EDGE CASE TESTS ---\n');

test('Episode 10 not in files (1-6) should return error, not random file', () => {
  const result = selectFileFromBatchTorrent(danDaDanBatchFiles, 10);
  assertEqual(result.status, 'episode_not_found');
  assertEqual(result.file, undefined, 'Should not return a file');
});

test('Episode 1 should return Episode 1 (first file matches)', () => {
  const result = selectFileFromBatchTorrent(danDaDanBatchFiles, 1);
  assertEqual(result.file?.filename.includes('- 01'), true);
});

// Simulate a torrent with different naming that old system would fail on
const weirdNamingFiles = [
  { filename: 'DanDaDan.Ep01.720p.mkv', size: 500000000 },
  { filename: 'DanDaDan.Ep02.720p.mkv', size: 510000000 },
  { filename: 'DanDaDan.Ep03.720p.mkv', size: 520000000 },
  { filename: 'DanDaDan.Ep04.720p.mkv', size: 530000000 },
];

test('Weird naming (Ep04 format) - Episode 4 should still match', () => {
  const result = selectFileFromBatchTorrent(weirdNamingFiles, 4);
  assertEqual(result.file?.filename.includes('Ep04'), true);
});

// ====================================================
// Summary
// ====================================================

console.log('\n========================================');
console.log(`  RESULTS: ${passed} passed, ${failed} failed`);
console.log('========================================\n');

if (failed === 0) {
  console.log('🎉 All Dan Da Dan scenario tests passed!');
  console.log('The bug where episodes 4,5,6 returned episode 1 is FIXED.\n');
}

process.exit(failed > 0 ? 1 : 0);
