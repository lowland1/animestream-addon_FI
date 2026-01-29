/**
 * Test Suite: Debrid File Selection
 * Tests the batch torrent file selection logic for AllDebrid and TorBox
 * 
 * Run with: node tests/test-debrid-file-selection.js
 */

console.log('========================================');
console.log('  DEBRID FILE SELECTION TEST SUITE');
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
// Simulated extractEpisodeInfo (simplified for testing)
// ====================================================

function extractEpisodeInfo(title) {
  const result = { episode: null, season: null, isBatch: false };
  const normalized = title.toLowerCase();
  
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
    return result;
  }
  
  // Episode X format
  const epWordMatch = normalized.match(/\b(?:episode|ep\.?)\s*0?(\d{1,4})(?:v\d+)?(?!\d)/i);
  if (epWordMatch) {
    result.episode = parseInt(epWordMatch[1], 10);
    return result;
  }
  
  // [05] format
  const bracketMatch = normalized.match(/\[0?(\d{1,3})(?:v\d+)?\](?!\s*(?:p)\b)/);
  if (bracketMatch) {
    const num = parseInt(bracketMatch[1], 10);
    if (num > 0 && num < 500 && num !== 720 && num !== 1080) {
      result.episode = num;
      return result;
    }
  }
  
  return result;
}

// ====================================================
// Simulated file selection logic (matches worker code)
// ====================================================

function selectFileFromBatch(videoFiles, episode, season = 1) {
  let selectedFile = null;
  const candidates = [];
  
  // First pass: exact extractEpisodeInfo match
  for (const file of videoFiles) {
    // Skip obvious non-episode files
    if (/(NCOP|NCED|Preview|Special|SP[^a-z]|OVA|Menu|Trailer|PV|CM|Bonus)/i.test(file.filename)) {
      continue;
    }
    
    const info = extractEpisodeInfo(file.filename);
    
    if (info.episode === episode) {
      if (info.season !== null && info.season !== season) {
        continue; // Season mismatch
      }
      candidates.push({ file, info });
    }
  }
  
  if (candidates.length > 0) {
    // Sort by size descending
    candidates.sort((a, b) => b.file.size - a.file.size);
    return { file: candidates[0].file, method: 'exact' };
  }
  
  // Second pass: loose patterns
  const episodePadded = String(episode).padStart(2, '0');
  const episodePadded3 = String(episode).padStart(3, '0');
  
  const loosePatterns = [
    new RegExp(`\\s-\\s${episodePadded}(?:v\\d+)?(?:\\s|\\.|\\[|\\(|$)`, 'i'),
    new RegExp(`\\bE${episodePadded}(?:v\\d+)?(?:\\s|\\.|\\[|\\(|$)`, 'i'),
    new RegExp(`\\bEp\\.?\\s*${episodePadded}(?:v\\d+)?(?:\\s|\\.|\\[|\\(|$)`, 'i'),
    new RegExp(`\\bEpisode\\s*${episodePadded}(?:v\\d+)?(?:\\s|\\.|\\[|\\(|$)`, 'i'),
    new RegExp(`\\[${episodePadded}\\]`, 'i'),
    new RegExp(`_${episodePadded}(?:v\\d+)?(?:_|\\.)`, 'i'),
    new RegExp(`\\.${episodePadded}(?:v\\d+)?\\.`, 'i'),
    new RegExp(`[\\s_.-]${episodePadded}(?:v\\d+)?\\.[a-z0-9]{2,4}$`, 'i'),
    new RegExp(`[\\s_.-]${episodePadded3}(?:v\\d+)?\\.[a-z0-9]{2,4}$`, 'i'),
  ];
  
  for (const file of videoFiles) {
    const fn = file.filename.toLowerCase();
    for (const pattern of loosePatterns) {
      if (pattern.test(fn)) {
        return { file, method: 'loose', pattern: pattern.source };
      }
    }
  }
  
  // CRITICAL: For batch torrents (>1 file), do NOT fall back to largest file
  if (videoFiles.length > 1) {
    return { status: 'episode_not_found', message: `Episode ${episode} not found in batch` };
  }
  
  // Single file torrent - use the only available file
  return { file: videoFiles[0], method: 'single_file' };
}

// ====================================================
// Test Data: Dan Da Dan batch torrent
// ====================================================

const danDaDanBatch = [
  { filename: '[SubsPlease] Dan Da Dan - 01 (1080p) [HASH].mkv', size: 1400000000 },
  { filename: '[SubsPlease] Dan Da Dan - 02 (1080p) [HASH].mkv', size: 1350000000 },
  { filename: '[SubsPlease] Dan Da Dan - 03 (1080p) [HASH].mkv', size: 1380000000 },
  { filename: '[SubsPlease] Dan Da Dan - 04 (1080p) [HASH].mkv', size: 1420000000 },
  { filename: '[SubsPlease] Dan Da Dan - 05 (1080p) [HASH].mkv', size: 1390000000 },
  { filename: '[SubsPlease] Dan Da Dan - 06 (1080p) [HASH].mkv', size: 1410000000 },
  { filename: '[SubsPlease] Dan Da Dan - NCOP (1080p) [HASH].mkv', size: 90000000 },
  { filename: '[SubsPlease] Dan Da Dan - NCED (1080p) [HASH].mkv', size: 85000000 },
];

console.log('\n--- Dan Da Dan Batch Tests ---\n');

test('Episode 1 from Dan Da Dan batch', () => {
  const result = selectFileFromBatch(danDaDanBatch, 1);
  assertEqual(result.file?.filename.includes('- 01'), true);
  assertEqual(result.method, 'exact');
});

test('Episode 4 from Dan Da Dan batch (the main bug)', () => {
  const result = selectFileFromBatch(danDaDanBatch, 4);
  assertEqual(result.file?.filename.includes('- 04'), true);
  console.log(`   Found: ${result.file?.filename}`);
});

test('Episode 5 from Dan Da Dan batch', () => {
  const result = selectFileFromBatch(danDaDanBatch, 5);
  assertEqual(result.file?.filename.includes('- 05'), true);
});

test('Episode 6 from Dan Da Dan batch', () => {
  const result = selectFileFromBatch(danDaDanBatch, 6);
  assertEqual(result.file?.filename.includes('- 06'), true);
});

test('NCOP/NCED files are skipped', () => {
  // When looking for any episode, NCOP/NCED should be skipped
  const result = selectFileFromBatch(danDaDanBatch, 1);
  assertEqual(result.file?.filename.includes('NCOP'), false);
  assertEqual(result.file?.filename.includes('NCED'), false);
});

test('Episode 10 NOT in batch returns error (not largest file)', () => {
  const result = selectFileFromBatch(danDaDanBatch, 10);
  assertEqual(result.status, 'episode_not_found');
  assertEqual(result.file, undefined, 'Should not return a file');
});

// ====================================================
// Test Data: Frieren batch with different naming
// ====================================================

const frierenBatch = [
  { filename: 'Frieren S01E01 [1080p].mkv', size: 1500000000 },
  { filename: 'Frieren S01E02 [1080p].mkv', size: 1450000000 },
  { filename: 'Frieren S01E03 [1080p].mkv', size: 1480000000 },
  { filename: 'Frieren S01E04 [1080p].mkv', size: 1520000000 },
  { filename: 'Frieren S01E05 [1080p].mkv', size: 1490000000 },
];

console.log('\n--- Frieren S01E0X Format Tests ---\n');

test('Episode 3 from Frieren batch (S01E03 format)', () => {
  const result = selectFileFromBatch(frierenBatch, 3, 1);
  assertEqual(result.file?.filename.includes('S01E03'), true);
});

test('Episode 5 from Frieren batch', () => {
  const result = selectFileFromBatch(frierenBatch, 5, 1);
  assertEqual(result.file?.filename.includes('S01E05'), true);
});

test('Season 2 episode from Season 1 batch returns error', () => {
  const result = selectFileFromBatch(frierenBatch, 1, 2);
  // S01 files should not match when looking for S02
  assertEqual(result.status, 'episode_not_found');
});

// ====================================================
// Test Data: Mixed naming conventions
// ====================================================

const mixedBatch = [
  { filename: 'Anime_01_720p.mkv', size: 500000000 },
  { filename: 'Anime_02_720p.mkv', size: 510000000 },
  { filename: 'Anime_03_720p.mkv', size: 520000000 },
  { filename: 'Anime_04_720p.mkv', size: 530000000 },
];

console.log('\n--- Underscore Format Tests ---\n');

test('Episode 2 from underscore batch (_02_)', () => {
  const result = selectFileFromBatch(mixedBatch, 2);
  assertEqual(result.file?.filename.includes('_02_'), true);
});

test('Episode 4 from underscore batch', () => {
  const result = selectFileFromBatch(mixedBatch, 4);
  assertEqual(result.file?.filename.includes('_04_'), true);
});

// ====================================================
// Test Data: Episode with versions
// ====================================================

const versionBatch = [
  { filename: '[Group] Anime - 01v2 (1080p).mkv', size: 1400000000 },
  { filename: '[Group] Anime - 02 (1080p).mkv', size: 1350000000 },
  { filename: '[Group] Anime - 03v3 (1080p).mkv', size: 1380000000 },
];

console.log('\n--- Version Suffix Tests ---\n');

test('Episode 1 with version suffix (01v2)', () => {
  const result = selectFileFromBatch(versionBatch, 1);
  assertEqual(result.file?.filename.includes('- 01v2'), true);
});

test('Episode 3 with version suffix (03v3)', () => {
  const result = selectFileFromBatch(versionBatch, 3);
  assertEqual(result.file?.filename.includes('- 03v3'), true);
});

// ====================================================
// Test: Single file torrents (should always work)
// ====================================================

console.log('\n--- Single File Torrent Tests ---\n');

test('Single file torrent uses the file regardless of episode match', () => {
  const singleFile = [{ filename: 'Some.Random.Anime.mkv', size: 1000000000 }];
  const result = selectFileFromBatch(singleFile, 5);
  assertEqual(result.method, 'single_file');
  assertEqual(result.file.filename, 'Some.Random.Anime.mkv');
});

// ====================================================
// Test: No false positives (critical for the bug)
// ====================================================

console.log('\n--- No False Positive Tests ---\n');

const tricky = [
  { filename: '[SubsPlease] Anime - 40 (1080p).mkv', size: 1400000000 },
  { filename: '[SubsPlease] Anime - 41 (1080p).mkv', size: 1350000000 },
  { filename: '[SubsPlease] Anime - 42 (1080p).mkv', size: 1380000000 },
];

test('Episode 4 should NOT match episode 40, 41, or 42', () => {
  const result = selectFileFromBatch(tricky, 4);
  assertEqual(result.status, 'episode_not_found');
});

test('Episode 40 should match correctly', () => {
  const result = selectFileFromBatch(tricky, 40);
  assertEqual(result.file?.filename.includes('- 40'), true);
});

test('Episode 1 should NOT match episode 41', () => {
  const result = selectFileFromBatch(tricky, 1);
  assertEqual(result.status, 'episode_not_found');
});

// ====================================================
// Summary
// ====================================================

console.log('\n========================================');
console.log(`  RESULTS: ${passed} passed, ${failed} failed`);
console.log('========================================\n');

process.exit(failed > 0 ? 1 : 0);
