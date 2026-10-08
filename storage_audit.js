/**
 * Storage Audit & Cleaner
 * Runtime: Node.js (modul native bawaan: http, fs, path, crypto, child_process)
 * Berjalan langsung tanpa dependensi eksternal (Zero npm install)
 */

const http = require('http');
const fs = require('fs');
const path = require('path');
const crypto = require('crypto');
const { exec } = require('child_process');

const DEFAULT_PORT = 3000;
const DEFAULT_TARGET_DIR = './Bahan Latihan P12';
const GIANT_FILE_THRESHOLD_BYTES = 2 * 1024 * 1024; // 2 MB (2.048 KB = 2.097.152 bytes)

/**
 * Format representasi ukuran byte ke format terbaca manusia (B, KB, MB, GB)
 */
function formatBytes(bytes, decimals = 2) {
  if (bytes === 0) return '0 B';
  const k = 1024;
  const dm = decimals < 0 ? 0 : decimals;
  const sizes = ['B', 'KB', 'MB', 'GB', 'TB'];
  const i = Math.floor(Math.log(bytes) / Math.log(k));
  return parseFloat((bytes / Math.pow(k, i)).toFixed(dm)) + ' ' + sizes[i];
}

/**
 * Menghitung hash SHA-256 berkas menggunakan streaming data
 */
function calculateFileHash(filePath) {
  return new Promise((resolve, reject) => {
    const hash = crypto.createHash('sha256');
    const stream = fs.createReadStream(filePath);

    stream.on('data', chunk => hash.update(chunk));
    stream.on('end', () => resolve(hash.digest('hex')));
    stream.on('error', err => reject(err));
  });
}

/**
 * Memindai direktori target secara rekursif dan menghitung metrik audit
 */
async function scanDirectory(targetInput) {
  const resolvedTarget = path.resolve(process.cwd(), targetInput || DEFAULT_TARGET_DIR);

  if (!fs.existsSync(resolvedTarget)) {
    throw new Error(`Folder tidak ditemukan: "${resolvedTarget}"`);
  }

  const stat = await fs.promises.stat(resolvedTarget);
  if (!stat.isDirectory()) {
    throw new Error(`Path bukan sebuah folder / direktori: "${resolvedTarget}"`);
  }

  const filesFound = [];

  // Penelusuran rekursif seluruh subfolder
  async function walk(currentDir) {
    let entries;
    try {
      entries = await fs.promises.readdir(currentDir, { withFileTypes: true });
    } catch (err) {
      console.warn(`Gagal membaca folder ${currentDir}:`, err.message);
      return;
    }

    for (const entry of entries) {
      const fullPath = path.join(currentDir, entry.name);
      if (entry.isDirectory()) {
        await walk(fullPath);
      } else if (entry.isFile()) {
        try {
          const fileStat = await fs.promises.stat(fullPath);
          const ext = path.extname(entry.name).toLowerCase();
          const relativePath = path.relative(resolvedTarget, fullPath);
          const hash = await calculateFileHash(fullPath);

          filesFound.push({
            name: entry.name,
            fullPath: fullPath,
            relativePath: relativePath,
            sizeBytes: fileStat.size,
            sizeFormatted: formatBytes(fileStat.size),
            hash: hash,
            mtimeMs: fileStat.mtimeMs,
            isGiant: fileStat.size >= GIANT_FILE_THRESHOLD_BYTES,
            isTmp: ext === '.tmp'
          });
        } catch (err) {
          console.warn(`Gagal membaca file ${fullPath}:`, err.message);
        }
      }
    }
  }

  await walk(resolvedTarget);

  // Mengelompokkan berkas berdasarkan SHA-256 hash (isi persis sama)
  const hashMap = {};
  for (const file of filesFound) {
    if (!hashMap[file.hash]) {
      hashMap[file.hash] = [];
    }
    hashMap[file.hash].push(file);
  }

  const duplicateGroups = [];
  let duplicateWasteBytes = 0;
  const duplicateCopiesToDelete = [];

  for (const [hash, groupFiles] of Object.entries(hashMap)) {
    if (groupFiles.length > 1) {
      // Urutkan file terlama (atau path relatif terpendek) sebagai file ASLI yang dipertahankan
      groupFiles.sort((a, b) => a.mtimeMs - b.mtimeMs || a.relativePath.length - b.relativePath.length);

      const originalFile = groupFiles[0];
      const copies = groupFiles.slice(1);

      let groupWasteBytes = 0;
      copies.forEach(copy => {
        groupWasteBytes += copy.sizeBytes;
        duplicateCopiesToDelete.push(copy);
      });
      duplicateWasteBytes += groupWasteBytes;

      duplicateGroups.push({
        hash: hash,
        fileCount: groupFiles.length,
        originalFile: originalFile,
        copies: copies,
        fileSize: originalFile.sizeBytes,
        fileSizeFormatted: originalFile.sizeFormatted,
        groupWasteBytes: groupWasteBytes,
        groupWasteFormatted: formatBytes(groupWasteBytes)
      });
    }
  }

  // Filter File Raksasa (>= 2 MB / 2.048 KB)
  const giantFiles = filesFound
    .filter(f => f.isGiant)
    .sort((a, b) => b.sizeBytes - a.sizeBytes);

  let giantTotalBytes = 0;
  giantFiles.forEach(f => giantTotalBytes += f.sizeBytes);

  // Filter File Sampah Sementara (.tmp)
  const tmpFiles = filesFound
    .filter(f => f.isTmp)
    .sort((a, b) => b.sizeBytes - a.sizeBytes);

  let tmpTotalBytes = 0;
  tmpFiles.forEach(f => tmpTotalBytes += f.sizeBytes);

  // Potensi Hemat: File salinan kembar + file .tmp (tanpa double counting)
  const filesToDeleteSet = new Set();
  duplicateCopiesToDelete.forEach(f => filesToDeleteSet.add(f.fullPath));
  tmpFiles.forEach(f => filesToDeleteSet.add(f.fullPath));

  let potentialSavingsBytes = 0;
  const allCandidateFiles = [];
  for (const file of filesFound) {
    if (filesToDeleteSet.has(file.fullPath)) {
      potentialSavingsBytes += file.sizeBytes;
      allCandidateFiles.push(file);
    }
  }

  let totalCapacityBytes = 0;
  filesFound.forEach(f => totalCapacityBytes += f.sizeBytes);

  return {
    scannedFolder: resolvedTarget,
    displayFolder: targetInput || DEFAULT_TARGET_DIR,
    metrics: {
      totalFiles: filesFound.length,
      totalCapacityBytes: totalCapacityBytes,
      totalCapacityFormatted: formatBytes(totalCapacityBytes),
      giantFilesCount: giantFiles.length,
      giantTotalBytes: giantTotalBytes,
      giantTotalFormatted: formatBytes(giantTotalBytes),
      duplicateGroupsCount: duplicateGroups.length,
      duplicateCopiesCount: duplicateCopiesToDelete.length,
      duplicateWasteBytes: duplicateWasteBytes,
      duplicateWasteFormatted: formatBytes(duplicateWasteBytes),
      tmpFilesCount: tmpFiles.length,
      tmpTotalBytes: tmpTotalBytes,
      tmpTotalFormatted: formatBytes(tmpTotalBytes),
      potentialSavingsBytes: potentialSavingsBytes,
      potentialSavingsFormatted: formatBytes(potentialSavingsBytes)
    },
    giantFiles: giantFiles,
    duplicateGroups: duplicateGroups,
    tmpFiles: tmpFiles,
    cleanupCandidates: allCandidateFiles
  };
}

/**
 * Eksekusi pembersihan in-place langsung di folder target
 * Hanya menghapus salinan kembar dan file .tmp, serta mempertahankan 1 file asli per grup
 */
async function cleanStorage(targetInput, requestedPaths) {
  const scanData = await scanDirectory(targetInput);
  const resolvedTarget = scanData.scannedFolder;

  // Petakan kandidat yang sah
  const validCandidateMap = new Map();
  for (const cand of scanData.cleanupCandidates) {
    validCandidateMap.set(path.resolve(cand.fullPath), cand);
  }

  const pathsToDelete = Array.isArray(requestedPaths) && requestedPaths.length > 0
    ? requestedPaths.map(p => path.resolve(p)).filter(p => validCandidateMap.has(p))
    : Array.from(validCandidateMap.keys());

  let deletedCount = 0;
  let freedBytes = 0;
  const deletedFiles = [];
  const errors = [];

  for (const fullPath of pathsToDelete) {
    const normalizedTarget = path.normalize(resolvedTarget);
    const normalizedFile = path.normalize(fullPath);

    // Keamanan: pastikan file berada di dalam folder target
    if (!normalizedFile.startsWith(normalizedTarget)) {
      errors.push({ path: fullPath, error: 'Di luar target folder (Ditolak demi keamanan)' });
      continue;
    }

    const item = validCandidateMap.get(fullPath);
    if (!item) {
      errors.push({ path: fullPath, error: 'File bukan salinan duplikat atau .tmp yang diizinkan' });
      continue;
    }

    try {
      if (fs.existsSync(fullPath)) {
        await fs.promises.unlink(fullPath); // Hapus in-place
        deletedCount++;
        freedBytes += item.sizeBytes;
        deletedFiles.push({
          name: item.name,
          relativePath: item.relativePath,
          fullPath: item.fullPath,
          sizeFormatted: item.sizeFormatted,
          type: item.isTmp ? 'File Sementara (.tmp)' : 'Salinan Duplikat'
        });
      }
    } catch (err) {
      errors.push({ path: fullPath, error: err.message });
    }
  }

  return {
    success: true,
    deletedCount,
    freedBytes,
    freedFormatted: formatBytes(freedBytes),
    deletedFiles,
    errors
  };
}

/**
 * Template Web UI Dashboard responsif
 */
function getHtmlDashboard() {
  return `<!DOCTYPE html>
<html lang="id">
<head>
  <meta charset="UTF-8">
  <meta name="viewport" content="width=device-width, initial-scale=1.0">
  <title>Storage Audit & Cleaner — Node.js</title>
  <link rel="preconnect" href="https://fonts.googleapis.com">
  <link rel="preconnect" href="https://fonts.gstatic.com" crossorigin>
  <link href="https://fonts.googleapis.com/css2?family=Plus+Jakarta+Sans:wght@400;500;600;700;800&family=JetBrains+Mono:wght@400;500;600&display=swap" rel="stylesheet">
  <style>
    :root {
      --bg-dark: #0a0e17;
      --bg-card: rgba(17, 24, 39, 0.85);
      --bg-card-hover: rgba(28, 38, 58, 0.95);
      --border-color: rgba(255, 255, 255, 0.08);
      --border-glow: rgba(99, 102, 241, 0.35);
      --text-main: #f1f5f9;
      --text-muted: #94a3b8;
      --text-sub: #64748b;
      --primary: #6366f1;
      --primary-hover: #4f46e5;
      --primary-gradient: linear-gradient(135deg, #6366f1 0%, #8b5cf6 50%, #ec4899 100%);
      --emerald: #10b981;
      --emerald-bg: rgba(16, 185, 129, 0.12);
      --emerald-border: rgba(16, 185, 129, 0.3);
      --rose: #f43f5e;
      --rose-bg: rgba(244, 63, 94, 0.12);
      --rose-border: rgba(244, 63, 94, 0.3);
      --amber: #f59e0b;
      --amber-bg: rgba(245, 158, 11, 0.12);
      --amber-border: rgba(245, 158, 11, 0.3);
      --cyan: #06b6d4;
      --cyan-bg: rgba(6, 182, 212, 0.12);
      --cyan-border: rgba(6, 182, 212, 0.3);
      --radius-sm: 8px;
      --radius-md: 14px;
      --radius-lg: 20px;
      --shadow-card: 0 10px 30px -10px rgba(0, 0, 0, 0.5), 0 0 1px 1px rgba(255, 255, 255, 0.05);
    }

    * {
      box-sizing: border-box;
      margin: 0;
      padding: 0;
    }

    body {
      font-family: 'Plus Jakarta Sans', -apple-system, BlinkMacSystemFont, sans-serif;
      background-color: var(--bg-dark);
      background-image: 
        radial-gradient(circle at 12% 15%, rgba(99, 102, 241, 0.18) 0%, transparent 40%),
        radial-gradient(circle at 88% 85%, rgba(236, 72, 153, 0.12) 0%, transparent 45%),
        linear-gradient(180deg, #0a0e17 0%, #06090f 100%);
      background-attachment: fixed;
      color: var(--text-main);
      min-height: 100vh;
      line-height: 1.5;
      padding: 28px 24px 60px;
    }

    .container {
      max-width: 1240px;
      margin: 0 auto;
    }

    header {
      display: flex;
      justify-content: space-between;
      align-items: center;
      margin-bottom: 28px;
      flex-wrap: wrap;
      gap: 16px;
    }

    .brand {
      display: flex;
      align-items: center;
      gap: 14px;
    }

    .brand-icon {
      width: 48px;
      height: 48px;
      border-radius: var(--radius-md);
      background: var(--primary-gradient);
      display: flex;
      align-items: center;
      justify-content: center;
      box-shadow: 0 8px 22px rgba(99, 102, 241, 0.35);
      color: white;
    }

    .brand h1 {
      font-size: 1.45rem;
      font-weight: 800;
      letter-spacing: -0.02em;
      background: linear-gradient(120deg, #ffffff 0%, #cbd5e1 100%);
      -webkit-background-clip: text;
      -webkit-text-fill-color: transparent;
    }

    .brand p {
      font-size: 0.84rem;
      color: var(--text-muted);
    }

    .badge-runtime {
      display: inline-flex;
      align-items: center;
      gap: 8px;
      background: rgba(255, 255, 255, 0.05);
      border: 1px solid var(--border-color);
      padding: 6px 14px;
      border-radius: 999px;
      font-size: 0.78rem;
      font-weight: 600;
      color: #a5b4fc;
    }

    .badge-runtime span.dot {
      width: 8px;
      height: 8px;
      background: var(--emerald);
      border-radius: 50%;
      box-shadow: 0 0 10px var(--emerald);
    }

    .search-panel {
      background: var(--bg-card);
      backdrop-filter: blur(16px);
      border: 1px solid var(--border-color);
      border-radius: var(--radius-lg);
      padding: 22px;
      box-shadow: var(--shadow-card);
      margin-bottom: 28px;
    }

    .search-row {
      display: flex;
      gap: 12px;
      flex-wrap: wrap;
    }

    .input-wrapper {
      flex: 1;
      min-width: 280px;
    }

    .input-wrapper label {
      display: block;
      font-size: 0.78rem;
      font-weight: 700;
      text-transform: uppercase;
      letter-spacing: 0.05em;
      color: var(--text-muted);
      margin-bottom: 8px;
    }

    .input-wrapper input {
      width: 100%;
      background: rgba(10, 15, 26, 0.8);
      border: 1px solid var(--border-color);
      color: var(--text-main);
      padding: 13px 18px;
      border-radius: var(--radius-md);
      font-size: 0.95rem;
      font-family: 'JetBrains Mono', monospace;
      outline: none;
      transition: all 0.2s ease;
    }

    .input-wrapper input:focus {
      border-color: var(--primary);
      box-shadow: 0 0 0 3px rgba(99, 102, 241, 0.25);
      background: rgba(15, 23, 42, 0.95);
    }

    .btn-group {
      display: flex;
      align-items: flex-end;
      gap: 10px;
      flex-wrap: wrap;
    }

    button {
      cursor: pointer;
      font-family: inherit;
      font-weight: 600;
      border: none;
      outline: none;
      border-radius: var(--radius-md);
      transition: all 0.2s cubic-bezier(0.4, 0, 0.2, 1);
      display: inline-flex;
      align-items: center;
      justify-content: center;
      gap: 8px;
      font-size: 0.92rem;
      padding: 13px 22px;
    }

    .btn-primary {
      background: var(--primary-gradient);
      color: white;
      box-shadow: 0 4px 15px rgba(99, 102, 241, 0.35);
    }

    .btn-primary:hover {
      box-shadow: 0 6px 22px rgba(99, 102, 241, 0.5);
      transform: translateY(-1px);
    }

    .btn-danger {
      background: linear-gradient(135deg, #f43f5e 0%, #e11d48 100%);
      color: white;
      box-shadow: 0 4px 15px rgba(244, 63, 94, 0.3);
    }

    .btn-danger:hover:not(:disabled) {
      box-shadow: 0 6px 20px rgba(244, 63, 94, 0.45);
      transform: translateY(-1px);
    }

    .btn-danger:disabled {
      opacity: 0.4;
      cursor: not-allowed;
      filter: grayscale(0.5);
    }

    .btn-secondary {
      background: rgba(255, 255, 255, 0.08);
      border: 1px solid var(--border-color);
      color: var(--text-main);
    }

    .btn-secondary:hover {
      background: rgba(255, 255, 255, 0.12);
    }

    .quick-tags {
      margin-top: 14px;
      display: flex;
      align-items: center;
      gap: 8px;
      flex-wrap: wrap;
      font-size: 0.78rem;
      color: var(--text-sub);
    }

    .tag-btn {
      background: rgba(255, 255, 255, 0.04);
      border: 1px solid var(--border-color);
      color: var(--text-muted);
      padding: 3px 10px;
      border-radius: 999px;
      cursor: pointer;
      font-family: 'JetBrains Mono', monospace;
      font-size: 0.75rem;
      transition: all 0.2s;
    }

    .tag-btn:hover {
      background: rgba(99, 102, 241, 0.15);
      border-color: rgba(99, 102, 241, 0.4);
      color: #a5b4fc;
    }

    /* 4 Metric Cards Grid */
    .metrics-grid {
      display: grid;
      grid-template-columns: repeat(auto-fit, minmax(260px, 1fr));
      gap: 18px;
      margin-bottom: 28px;
    }

    .metric-card {
      background: var(--bg-card);
      backdrop-filter: blur(14px);
      border: 1px solid var(--border-color);
      border-radius: var(--radius-lg);
      padding: 22px;
      position: relative;
      overflow: hidden;
      box-shadow: var(--shadow-card);
      transition: all 0.25s ease;
    }

    .metric-card:hover {
      transform: translateY(-3px);
      border-color: var(--border-glow);
      background: var(--bg-card-hover);
    }

    .metric-header {
      display: flex;
      align-items: center;
      justify-content: space-between;
      margin-bottom: 12px;
    }

    .metric-title {
      font-size: 0.82rem;
      font-weight: 700;
      text-transform: uppercase;
      letter-spacing: 0.05em;
      color: var(--text-muted);
    }

    .metric-icon-wrap {
      width: 42px;
      height: 42px;
      border-radius: var(--radius-sm);
      display: flex;
      align-items: center;
      justify-content: center;
    }

    .metric-value {
      font-size: 2rem;
      font-weight: 800;
      letter-spacing: -0.02em;
      line-height: 1.1;
      margin-bottom: 6px;
    }

    .metric-sub {
      font-size: 0.82rem;
      color: var(--text-muted);
      display: flex;
      align-items: center;
      gap: 6px;
    }

    .card-total .metric-icon-wrap { background: var(--cyan-bg); border: 1px solid var(--cyan-border); color: var(--cyan); }
    .card-total .metric-value { color: #e0f2fe; }

    .card-capacity .metric-icon-wrap { background: rgba(99, 102, 241, 0.15); border: 1px solid rgba(99, 102, 241, 0.3); color: #818cf8; }
    .card-capacity .metric-value { color: #e0e7ff; }

    .card-giant .metric-icon-wrap { background: var(--rose-bg); border: 1px solid var(--rose-border); color: var(--rose); }
    .card-giant .metric-value { color: #ffe4e6; }

    .card-savings .metric-icon-wrap { background: var(--emerald-bg); border: 1px solid var(--emerald-border); color: var(--emerald); }
    .card-savings .metric-value { color: #d1fae5; }

    /* Sections */
    .section-card {
      background: var(--bg-card);
      backdrop-filter: blur(14px);
      border: 1px solid var(--border-color);
      border-radius: var(--radius-lg);
      margin-bottom: 28px;
      overflow: hidden;
      box-shadow: var(--shadow-card);
    }

    .section-head {
      padding: 20px 24px;
      border-bottom: 1px solid var(--border-color);
      display: flex;
      align-items: center;
      justify-content: space-between;
      flex-wrap: wrap;
      gap: 12px;
      background: rgba(255, 255, 255, 0.02);
    }

    .section-title {
      display: flex;
      align-items: center;
      gap: 10px;
      font-size: 1.12rem;
      font-weight: 700;
    }

    .chip-counter {
      font-size: 0.74rem;
      font-weight: 700;
      padding: 3px 10px;
      border-radius: 999px;
      background: rgba(255, 255, 255, 0.08);
      color: var(--text-muted);
    }

    .table-container {
      width: 100%;
      overflow-x: auto;
    }

    table {
      width: 100%;
      border-collapse: collapse;
      text-align: left;
      font-size: 0.88rem;
    }

    th {
      background: rgba(10, 15, 26, 0.6);
      color: var(--text-muted);
      font-weight: 700;
      font-size: 0.76rem;
      text-transform: uppercase;
      letter-spacing: 0.05em;
      padding: 14px 20px;
      border-bottom: 1px solid var(--border-color);
    }

    td {
      padding: 15px 20px;
      border-bottom: 1px solid rgba(255, 255, 255, 0.04);
      vertical-align: middle;
    }

    tr:last-child td {
      border-bottom: none;
    }

    tbody tr:hover {
      background: rgba(255, 255, 255, 0.02);
    }

    .file-name-cell {
      display: flex;
      align-items: center;
      gap: 10px;
      font-weight: 600;
      color: #f8fafc;
    }

    .file-path {
      font-family: 'JetBrains Mono', monospace;
      font-size: 0.78rem;
      color: var(--text-sub);
      display: block;
      margin-top: 2px;
      word-break: break-all;
    }

    .mono-hash {
      font-family: 'JetBrains Mono', monospace;
      font-size: 0.75rem;
      color: #94a3b8;
      background: rgba(0, 0, 0, 0.35);
      padding: 3px 8px;
      border-radius: 4px;
      border: 1px solid rgba(255, 255, 255, 0.05);
      max-width: 140px;
      overflow: hidden;
      text-overflow: ellipsis;
      white-space: nowrap;
      display: inline-block;
    }

    .badge {
      display: inline-flex;
      align-items: center;
      gap: 5px;
      padding: 3px 9px;
      border-radius: 999px;
      font-size: 0.74rem;
      font-weight: 700;
    }

    .badge-giant {
      background: var(--rose-bg);
      color: #f43f5e;
      border: 1px solid var(--rose-border);
    }

    .badge-original {
      background: var(--emerald-bg);
      color: #10b981;
      border: 1px solid var(--emerald-border);
    }

    .badge-duplicate {
      background: var(--rose-bg);
      color: #f43f5e;
      border: 1px solid var(--rose-border);
    }

    .badge-tmp {
      background: var(--amber-bg);
      color: #f59e0b;
      border: 1px solid var(--amber-border);
    }

    /* Accordion */
    .accordion-item {
      border-bottom: 1px solid var(--border-color);
    }

    .accordion-item:last-child {
      border-bottom: none;
    }

    .accordion-header {
      padding: 18px 24px;
      cursor: pointer;
      display: flex;
      align-items: center;
      justify-content: space-between;
      transition: background 0.2s;
      user-select: none;
    }

    .accordion-header:hover {
      background: rgba(255, 255, 255, 0.02);
    }

    .accordion-info {
      display: flex;
      align-items: center;
      gap: 14px;
      flex-wrap: wrap;
    }

    .accordion-title {
      font-weight: 700;
      font-size: 0.95rem;
      color: #f1f5f9;
    }

    .accordion-chevron {
      transition: transform 0.25s ease;
      color: var(--text-muted);
    }

    .accordion-item.active .accordion-chevron {
      transform: rotate(180deg);
      color: var(--primary);
    }

    .accordion-body {
      display: none;
      padding: 0 24px 20px;
      background: rgba(10, 15, 26, 0.4);
    }

    .accordion-item.active .accordion-body {
      display: block;
    }

    .dup-table {
      margin-top: 10px;
      border-radius: var(--radius-sm);
      overflow: hidden;
      border: 1px solid rgba(255, 255, 255, 0.05);
    }

    .empty-state {
      padding: 44px 24px;
      text-align: center;
      color: var(--text-muted);
    }

    /* Modal */
    .modal-backdrop {
      position: fixed;
      inset: 0;
      background: rgba(5, 8, 15, 0.82);
      backdrop-filter: blur(8px);
      display: flex;
      align-items: center;
      justify-content: center;
      padding: 20px;
      z-index: 999;
      opacity: 0;
      visibility: hidden;
      transition: all 0.25s ease;
    }

    .modal-backdrop.open {
      opacity: 1;
      visibility: visible;
    }

    .modal-box {
      background: #111827;
      border: 1px solid rgba(255, 255, 255, 0.1);
      border-radius: var(--radius-lg);
      width: 100%;
      max-width: 680px;
      max-height: 85vh;
      display: flex;
      flex-direction: column;
      box-shadow: 0 25px 50px -12px rgba(0, 0, 0, 0.8), 0 0 0 1px rgba(244, 63, 94, 0.2);
      transform: scale(0.95);
      transition: transform 0.25s ease;
    }

    .modal-backdrop.open .modal-box {
      transform: scale(1);
    }

    .modal-head {
      padding: 20px 24px;
      border-bottom: 1px solid var(--border-color);
      display: flex;
      align-items: center;
      justify-content: space-between;
    }

    .modal-head h3 {
      font-size: 1.15rem;
      font-weight: 800;
      color: #ffe4e6;
      display: flex;
      align-items: center;
      gap: 10px;
    }

    .modal-content {
      padding: 20px 24px;
      overflow-y: auto;
      flex: 1;
    }

    .modal-warning-box {
      background: rgba(244, 63, 94, 0.1);
      border: 1px solid rgba(244, 63, 94, 0.25);
      border-radius: var(--radius-md);
      padding: 14px 18px;
      margin-bottom: 18px;
      font-size: 0.86rem;
      color: #fecdd3;
      line-height: 1.45;
    }

    .candidate-list {
      display: flex;
      flex-direction: column;
      gap: 8px;
      max-height: 260px;
      overflow-y: auto;
      padding-right: 6px;
    }

    .candidate-item {
      display: flex;
      align-items: center;
      justify-content: space-between;
      padding: 10px 14px;
      background: rgba(255, 255, 255, 0.03);
      border: 1px solid var(--border-color);
      border-radius: var(--radius-sm);
      font-size: 0.82rem;
    }

    .candidate-item .info {
      overflow: hidden;
      text-overflow: ellipsis;
      white-space: nowrap;
      margin-right: 12px;
    }

    .candidate-item .info .c-name {
      font-weight: 600;
      color: #fff;
    }

    .candidate-item .info .c-path {
      font-size: 0.74rem;
      color: var(--text-muted);
      font-family: 'JetBrains Mono', monospace;
    }

    .modal-foot {
      padding: 16px 24px;
      border-top: 1px solid var(--border-color);
      display: flex;
      align-items: center;
      justify-content: space-between;
      background: rgba(10, 15, 26, 0.5);
    }

    .toast-container {
      position: fixed;
      bottom: 24px;
      right: 24px;
      z-index: 1000;
      display: flex;
      flex-direction: column;
      gap: 10px;
    }

    .toast {
      background: #1e293b;
      border: 1px solid var(--border-color);
      box-shadow: 0 10px 25px rgba(0, 0, 0, 0.6);
      border-radius: var(--radius-md);
      padding: 14px 20px;
      min-width: 300px;
      max-width: 420px;
      display: flex;
      align-items: center;
      gap: 12px;
      font-size: 0.88rem;
      animation: slideIn 0.3s forwards cubic-bezier(0.16, 1, 0.3, 1);
    }

    .toast.success { border-color: var(--emerald-border); background: #064e3b; color: #a7f3d0; }
    .toast.error { border-color: var(--rose-border); background: #881337; color: #fecdd3; }
    .toast.info { border-color: var(--cyan-border); background: #083344; color: #a5f3fc; }

    @keyframes slideIn {
      from { transform: translateX(100%); opacity: 0; }
      to { transform: translateX(0); opacity: 1; }
    }

    .spinner {
      width: 18px;
      height: 18px;
      border: 2px solid rgba(255, 255, 255, 0.3);
      border-top-color: white;
      border-radius: 50%;
      animation: spin 0.8s linear infinite;
    }

    @keyframes spin {
      to { transform: rotate(360deg); }
    }

    @media (max-width: 768px) {
      body { padding: 16px 12px 40px; }
      .metrics-grid { grid-template-columns: 1fr; }
      .search-row { flex-direction: column; }
      .btn-group { width: 100%; }
      .btn-group button { flex: 1; }
    }
  </style>
</head>
<body>
  <div class="container">
    <!-- Header -->
    <header>
      <div class="brand">
        <div class="brand-icon">
          <svg width="26" height="26" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2.2" stroke-linecap="round" stroke-linejoin="round">
            <path d="M21 16V8a2 2 0 0 0-1-1.73l-7-4a2 2 0 0 0-2 0l-7 4A2 2 0 0 0 3 8v8a2 2 0 0 0 1 1.73l7 4a2 2 0 0 0 2 0l7-4A2 2 0 0 0 21 16z"></path>
            <polyline points="3.27 6.96 12 12.01 20.73 6.96"></polyline>
            <line x1="12" y1="22.08" x2="12" y2="12"></line>
          </svg>
        </div>
        <div>
          <h1>Storage Audit & Cleaner</h1>
          <p>Sistem Audit Penyimpanan Dinamis, Deteksi Hash SHA-256 & Pembersihan In-Place</p>
        </div>
      </div>
      <div class="badge-runtime">
        <span class="dot"></span>
        <span>Node.js Native Runtime (Zero Dependency)</span>
      </div>
    </header>

    <!-- Dynamic Path Input Panel -->
    <section class="search-panel">
      <div class="search-row">
        <div class="input-wrapper">
          <label for="folderInput">Target Path Folder (Dinamis / Tidak Hardcoded)</label>
          <input type="text" id="folderInput" value="./Bahan Latihan P12" placeholder="Contoh: ./Bahan Latihan P12 atau D:/Documents/Proyek">
        </div>
        <div class="btn-group">
          <button id="btnScan" class="btn-primary">
            <svg width="18" height="18" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2.2">
              <circle cx="11" cy="11" r="8"></circle>
              <line x1="21" y1="21" x2="16.65" y2="16.65"></line>
            </svg>
            <span id="scanBtnText">Pindai Folder</span>
          </button>
          <button id="btnOpenCleanModal" class="btn-danger" disabled>
            <svg width="18" height="18" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2.2">
              <polyline points="3 6 5 6 21 6"></polyline>
              <path d="M19 6v14a2 2 0 0 1-2 2H7a2 2 0 0 1-2-2V6m3 0V4a2 2 0 0 1 2-2h4a2 2 0 0 1 2 2v2"></path>
              <line x1="10" y1="11" x2="10" y2="17"></line>
              <line x1="14" y1="11" x2="14" y2="17"></line>
            </svg>
            <span>Bersihkan Duplikat &amp; Sampah</span>
          </button>
        </div>
      </div>
      <div class="quick-tags">
        <span>Quick Paths:</span>
        <button class="tag-btn" onclick="setFolderPath('./Bahan Latihan P12')">./Bahan Latihan P12 (Default)</button>
        <button class="tag-btn" onclick="setFolderPath('./')">./ (Root Workspace)</button>
      </div>
    </section>

    <!-- 4 Storage Metric Cards -->
    <div class="metrics-grid">
      <!-- Card 1: Total File -->
      <div class="metric-card card-total">
        <div class="metric-header">
          <span class="metric-title">Total File</span>
          <div class="metric-icon-wrap">
            <svg width="22" height="22" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2">
              <path d="M13 2H6a2 2 0 0 0-2 2v16a2 2 0 0 0 2 2h12a2 2 0 0 0 2-2V9z"></path>
              <polyline points="13 2 13 9 20 9"></polyline>
            </svg>
          </div>
        </div>
        <div class="metric-value" id="metricTotalFiles">0</div>
        <div class="metric-sub" id="metricTotalSub">File terpindai secara rekursif</div>
      </div>

      <!-- Card 2: Total Kapasitas -->
      <div class="metric-card card-capacity">
        <div class="metric-header">
          <span class="metric-title">Total Kapasitas</span>
          <div class="metric-icon-wrap">
            <svg width="22" height="22" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2">
              <rect x="2" y="2" width="20" height="8" rx="2" ry="2"></rect>
              <rect x="2" y="14" width="20" height="8" rx="2" ry="2"></rect>
              <line x1="6" y1="6" x2="6.01" y2="6"></line>
              <line x1="6" y1="18" x2="6.01" y2="18"></line>
            </svg>
          </div>
        </div>
        <div class="metric-value" id="metricTotalCapacity">0 B</div>
        <div class="metric-sub">Ukuran akumulatif seluruh berkas</div>
      </div>

      <!-- Card 3: File Raksasa (> 2 MB) -->
      <div class="metric-card card-giant">
        <div class="metric-header">
          <span class="metric-title">File Raksasa (&gt; 2 MB)</span>
          <div class="metric-icon-wrap">
            <svg width="22" height="22" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2">
              <path d="M10.29 3.86L1.82 18a2 2 0 0 0 1.71 3h16.94a2 2 0 0 0 1.71-3L13.71 3.86a2 2 0 0 0-3.42 0z"></path>
              <line x1="12" y1="9" x2="12" y2="13"></line>
              <line x1="12" y1="17" x2="12.01" y2="17"></line>
            </svg>
          </div>
        </div>
        <div class="metric-value" id="metricGiantCount">0</div>
        <div class="metric-sub" id="metricGiantSub">0 berkas melebihi 2.048 KB</div>
      </div>

      <!-- Card 4: Potensi Hemat -->
      <div class="metric-card card-savings">
        <div class="metric-header">
          <span class="metric-title">Potensi Hemat</span>
          <div class="metric-icon-wrap">
            <svg width="22" height="22" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2">
              <polyline points="23 4 23 10 17 10"></polyline>
              <polyline points="1 20 1 14 7 14"></polyline>
              <path d="M3.51 9a9 9 0 0 1 14.85-3.36L23 10M1 14l4.64 4.36A9 9 0 0 0 20.49 15"></path>
            </svg>
          </div>
        </div>
        <div class="metric-value" id="metricSavings">0 B</div>
        <div class="metric-sub" id="metricSavingsSub">Salinan kembar &amp; file .tmp</div>
      </div>
    </div>

    <!-- Section 1: Tabel File Raksasa (> 2 MB) -->
    <section class="section-card">
      <div class="section-head">
        <div class="section-title">
          <svg width="20" height="20" viewBox="0 0 24 24" fill="none" stroke="#f43f5e" stroke-width="2.2">
            <polygon points="12 2 15.09 8.26 22 9.27 17 14.14 18.18 21.02 12 17.77 5.82 21.02 7 14.14 2 9.27 8.91 8.26 12 2"></polygon>
          </svg>
          <span>Daftar File Raksasa (Melebihi Ambang Batas 2 MB)</span>
          <span class="chip-counter" id="giantCountChip">0 File</span>
        </div>
      </div>
      <div class="table-container">
        <table>
          <thead>
            <tr>
              <th>Nama File &amp; Path</th>
              <th>Ukuran</th>
              <th>Status</th>
              <th>Hash SHA-256</th>
            </tr>
          </thead>
          <tbody id="giantTableBody">
            <tr>
              <td colspan="4" class="empty-state">
                <p>Belum ada data pemindaian. Klik tombol "Pindai Folder" di atas.</p>
              </td>
            </tr>
          </tbody>
        </table>
      </div>
    </section>

    <!-- Section 2: Accordion Grup Duplikat (SHA-256 Identik) -->
    <section class="section-card">
      <div class="section-head">
        <div class="section-title">
          <svg width="20" height="20" viewBox="0 0 24 24" fill="none" stroke="#8b5cf6" stroke-width="2.2">
            <rect x="9" y="9" width="13" height="13" rx="2" ry="2"></rect>
            <path d="M5 15H4a2 2 0 0 1-2-2V4a2 2 0 0 1 2-2h9a2 2 0 0 1 2 2v1"></path>
          </svg>
          <span>Grup File Duplikat (Hash SHA-256 Identik)</span>
          <span class="chip-counter" id="dupGroupsChip">0 Grup</span>
        </div>
      </div>
      <div id="duplicateAccordionContainer">
        <div class="empty-state">
          <p>Belum ada data duplikat yang dipindai.</p>
        </div>
      </div>
    </section>

    <!-- Section 3: File Sampah Sementara (.tmp) -->
    <section class="section-card">
      <div class="section-head">
        <div class="section-title">
          <svg width="20" height="20" viewBox="0 0 24 24" fill="none" stroke="#f59e0b" stroke-width="2.2">
            <path d="M21 15v4a2 2 0 0 1-2 2H5a2 2 0 0 1-2-2v-4"></path>
            <polyline points="7 10 12 15 17 10"></polyline>
            <line x1="12" y1="15" x2="12" y2="3"></line>
          </svg>
          <span>File Sampah Sementara (.tmp)</span>
          <span class="chip-counter" id="tmpCountChip">0 File</span>
        </div>
      </div>
      <div class="table-container">
        <table>
          <thead>
            <tr>
              <th>Nama File &amp; Path</th>
              <th>Ukuran</th>
              <th>Tipe</th>
              <th>Rekomendasi</th>
            </tr>
          </thead>
          <tbody id="tmpTableBody">
            <tr>
              <td colspan="4" class="empty-state">
                <p>Tidak ditemukan file sementara berakhiran .tmp</p>
              </td>
            </tr>
          </tbody>
        </table>
      </div>
    </section>
  </div>

  <!-- Interactive Confirmation Modal -->
  <div class="modal-backdrop" id="cleanModal">
    <div class="modal-box">
      <div class="modal-head">
        <h3>
          <svg width="22" height="22" viewBox="0 0 24 24" fill="none" stroke="#f43f5e" stroke-width="2.2">
            <path d="M10.29 3.86L1.82 18a2 2 0 0 0 1.71 3h16.94a2 2 0 0 0 1.71-3L13.71 3.86a2 2 0 0 0-3.42 0z"></path>
            <line x1="12" y1="9" x2="12" y2="13"></line>
            <line x1="12" y1="17" x2="12.01" y2="17"></line>
          </svg>
          Konfirmasi Pembersihan In-Place
        </h3>
        <button class="btn-secondary" style="padding: 6px 12px; font-size: 0.8rem;" onclick="closeCleanModal()">✕</button>
      </div>
      <div class="modal-content">
        <div class="modal-warning-box">
          <strong>Perhatian:</strong> Pembersihan dilakukan <u>langsung di tempat</u> pada folder target (in-place execution). Hanya file salinan kembar dan file .tmp yang akan dihapus secara permanen. <strong>1 file asli per grup duplikat dipastikan tetap aman dan dipertahankan.</strong>
        </div>
        <p style="font-size: 0.88rem; color: #cbd5e1; margin-bottom: 12px;">
          Rincian file yang akan dibersihkan (<span id="modalFileCount">0</span> file, potensi ruang bebas: <strong id="modalFreedSpace" style="color: #34d399;">0 B</strong>):
        </p>
        <div class="candidate-list" id="modalCandidateList"></div>
      </div>
      <div class="modal-foot">
        <button class="btn-secondary" onclick="closeCleanModal()">Batal</button>
        <button id="btnConfirmClean" class="btn-danger">
          <svg width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2.2">
            <polyline points="3 6 5 6 21 6"></polyline>
            <path d="M19 6v14a2 2 0 0 1-2 2H7a2 2 0 0 1-2-2V6"></path>
          </svg>
          <span id="confirmBtnText">Eksekusi Hapus Sekarang</span>
        </button>
      </div>
    </div>
  </div>

  <div class="toast-container" id="toastContainer"></div>

  <script>
    let currentScanData = null;

    function setFolderPath(pathStr) {
      document.getElementById('folderInput').value = pathStr;
      performScan();
    }

    function showToast(message, type = 'info') {
      const container = document.getElementById('toastContainer');
      const toast = document.createElement('div');
      toast.className = 'toast ' + type;
      toast.innerHTML = '<span>' + message + '</span>';
      container.appendChild(toast);
      setTimeout(() => {
        toast.style.opacity = '0';
        toast.style.transform = 'translateY(10px)';
        toast.style.transition = 'all 0.3s';
        setTimeout(() => toast.remove(), 300);
      }, 4000);
    }

    async function performScan() {
      const folderPath = document.getElementById('folderInput').value.trim();
      const btnScan = document.getElementById('btnScan');
      const scanText = document.getElementById('scanBtnText');

      if (!folderPath) {
        showToast('Silakan masukkan target path folder.', 'error');
        return;
      }

      btnScan.disabled = true;
      scanText.innerHTML = '<span class="spinner"></span> Memindai...';

      try {
        const response = await fetch('/api/scan', {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({ folderPath })
        });

        const data = await response.json();

        if (!response.ok) {
          throw new Error(data.error || 'Gagal memindai folder');
        }

        currentScanData = data;
        renderDashboard(data);
        showToast('Pemindaian berhasil! Ditemukan ' + data.metrics.totalFiles + ' berkas.', 'success');
      } catch (err) {
        showToast(err.message, 'error');
      } finally {
        btnScan.disabled = false;
        scanText.innerHTML = 'Pindai Folder';
      }
    }

    function renderDashboard(data) {
      const m = data.metrics;

      // 4 Metric Cards
      const remainingUnique = m.totalFiles - m.duplicateCopiesCount - m.tmpFilesCount;
      document.getElementById('metricTotalFiles').textContent = m.totalFiles.toLocaleString();
      document.getElementById('metricTotalSub').innerHTML = 
        m.duplicateCopiesCount > 0 || m.tmpFilesCount > 0
          ? '<strong style="color:#a5b4fc;">' + remainingUnique + ' Berkas Unik</strong> (' + (m.duplicateCopiesCount + m.tmpFilesCount) + ' sampah/duplikat)'
          : 'Semua berkas unik terpindai';

      document.getElementById('metricTotalCapacity').textContent = m.totalCapacityFormatted;

      document.getElementById('metricGiantCount').textContent = m.giantFilesCount.toLocaleString();
      document.getElementById('metricGiantSub').textContent = m.giantFilesCount + ' berkas (' + m.giantTotalFormatted + ')';

      document.getElementById('metricSavings').textContent = m.potentialSavingsFormatted;
      document.getElementById('metricSavingsSub').textContent = 
        m.duplicateCopiesCount + ' salinan duplikat & ' + m.tmpFilesCount + ' file .tmp';

      // Tombol Bersihkan Duplikat & Sampah
      const btnClean = document.getElementById('btnOpenCleanModal');
      const canClean = (m.duplicateCopiesCount > 0 || m.tmpFilesCount > 0);
      btnClean.disabled = !canClean;

      // Section 1: File Raksasa Table
      const giantTableBody = document.getElementById('giantTableBody');
      document.getElementById('giantCountChip').textContent = data.giantFiles.length + ' File';

      if (data.giantFiles.length === 0) {
        giantTableBody.innerHTML = '<tr><td colspan="4" class="empty-state"><p>Tidak ada file yang melebihi ambang batas 2 MB (2.048 KB).</p></td></tr>';
      } else {
        giantTableBody.innerHTML = data.giantFiles.map(f => {
          return '<tr>' +
            '<td><div class="file-name-cell">' +
              '<svg width="18" height="18" viewBox="0 0 24 24" fill="none" stroke="#f43f5e" stroke-width="2"><path d="M13 2H6a2 2 0 0 0-2 2v16a2 2 0 0 0 2 2h12a2 2 0 0 0 2-2V9z"></path></svg>' +
              '<div>' +
                '<span>' + escapeHtml(f.name) + '</span>' +
                '<span class="file-path">' + escapeHtml(f.relativePath) + '</span>' +
              '</div>' +
            '</div></td>' +
            '<td><strong>' + f.sizeFormatted + '</strong></td>' +
            '<td><span class="badge badge-giant">File Raksasa (&gt;2MB)</span></td>' +
            '<td><span class="mono-hash" title="' + f.hash + '">' + f.hash.substring(0, 16) + '...</span></td>' +
          '</tr>';
        }).join('');
      }

      // Section 2: Duplicate Accordion
      const accordionContainer = document.getElementById('duplicateAccordionContainer');
      document.getElementById('dupGroupsChip').textContent = data.duplicateGroups.length + ' Grup';

      if (data.duplicateGroups.length === 0) {
        accordionContainer.innerHTML = '<div class="empty-state"><p>Tidak ditemukan berkas duplikat dengan hash SHA-256 yang sama.</p></div>';
      } else {
        accordionContainer.innerHTML = data.duplicateGroups.map((g, idx) => {
          const original = g.originalFile;
          const copies = g.copies;

          return '<div class="accordion-item ' + (idx === 0 ? 'active' : '') + '">' +
            '<div class="accordion-header" onclick="toggleAccordion(this)">' +
              '<div class="accordion-info">' +
                '<span class="accordion-title">Grup #' + (idx + 1) + ' — ' + escapeHtml(original.name) + '</span>' +
                '<span class="badge badge-giant" style="background: rgba(99,102,241,0.15); color: #a5b4fc; border-color: rgba(99,102,241,0.3);">' + g.fileCount + ' File Kembar</span>' +
                '<span style="font-size:0.8rem; color:#94a3b8;">Ukuran: ' + g.fileSizeFormatted + ' | Terbuang: <strong style="color:#f43f5e">' + g.groupWasteFormatted + '</strong></span>' +
              '</div>' +
              '<svg class="accordion-chevron" width="18" height="18" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2.2"><polyline points="6 9 12 15 18 9"></polyline></svg>' +
            '</div>' +
            '<div class="accordion-body">' +
              '<div style="font-size:0.78rem; color:#94a3b8; margin-bottom:8px;">SHA-256 Hash: <code style="color:#38bdf8;">' + g.hash + '</code></div>' +
              '<div class="dup-table">' +
                '<table>' +
                  '<thead><tr><th>Status &amp; Peran</th><th>Nama Berkas</th><th>Path Relatif</th><th>Ukuran</th></tr></thead>' +
                  '<tbody>' +
                    '<tr>' +
                      '<td><span class="badge badge-original">✓ ASLI (DIPERTAHANKAN)</span></td>' +
                      '<td><strong>' + escapeHtml(original.name) + '</strong></td>' +
                      '<td><span class="file-path">' + escapeHtml(original.relativePath) + '</span></td>' +
                      '<td>' + original.sizeFormatted + '</td>' +
                    '</tr>' +
                    copies.map(c => 
                      '<tr>' +
                        '<td><span class="badge badge-duplicate">✕ SALINAN (AKAN DIHAPUS)</span></td>' +
                        '<td>' + escapeHtml(c.name) + '</td>' +
                        '<td><span class="file-path">' + escapeHtml(c.relativePath) + '</span></td>' +
                        '<td>' + c.sizeFormatted + '</td>' +
                      '</tr>'
                    ).join('') +
                  '</tbody>' +
                '</table>' +
              '</div>' +
            '</div>' +
          '</div>';
        }).join('');
      }

      // Section 3: Tmp Files Table
      const tmpTableBody = document.getElementById('tmpTableBody');
      document.getElementById('tmpCountChip').textContent = data.tmpFiles.length + ' File';

      if (data.tmpFiles.length === 0) {
        tmpTableBody.innerHTML = '<tr><td colspan="4" class="empty-state"><p>Tidak ditemukan file sementara berakhiran .tmp.</p></td></tr>';
      } else {
        tmpTableBody.innerHTML = data.tmpFiles.map(f => {
          return '<tr>' +
            '<td><div class="file-name-cell">' +
              '<svg width="18" height="18" viewBox="0 0 24 24" fill="none" stroke="#f59e0b" stroke-width="2"><polyline points="21 8 21 21 3 21 3 8"></polyline><line x1="1" y1="3" x2="23" y2="3"></line></svg>' +
              '<div>' +
                '<span>' + escapeHtml(f.name) + '</span>' +
                '<span class="file-path">' + escapeHtml(f.relativePath) + '</span>' +
              '</div>' +
            '</div></td>' +
            '<td><strong>' + f.sizeFormatted + '</strong></td>' +
            '<td><span class="badge badge-tmp">File Sampah .tmp</span></td>' +
            '<td><span style="color:#f43f5e; font-size:0.8rem; font-weight:600;">Direkomendasikan Dihapus</span></td>' +
          '</tr>';
        }).join('');
      }
    }

    function toggleAccordion(el) {
      const item = el.closest('.accordion-item');
      item.classList.toggle('active');
    }

    function openCleanModal() {
      if (!currentScanData || currentScanData.cleanupCandidates.length === 0) {
        showToast('Tidak ada file duplikat atau .tmp yang perlu dibersihkan.', 'info');
        return;
      }

      const modal = document.getElementById('cleanModal');
      const listEl = document.getElementById('modalCandidateList');
      const candidates = currentScanData.cleanupCandidates;

      document.getElementById('modalFileCount').textContent = candidates.length;
      document.getElementById('modalFreedSpace').textContent = currentScanData.metrics.potentialSavingsFormatted;

      listEl.innerHTML = candidates.map(c => {
        const typeLabel = c.isTmp ? 'File .tmp' : 'Salinan Duplikat';
        const typeColor = c.isTmp ? '#f59e0b' : '#f43f5e';

        return '<div class="candidate-item">' +
          '<div class="info">' +
            '<div class="c-name">' + escapeHtml(c.name) + ' <span style="font-size:0.7rem; color:' + typeColor + ';">(' + typeLabel + ')</span></div>' +
            '<div class="c-path">' + escapeHtml(c.relativePath) + '</div>' +
          '</div>' +
          '<div style="font-weight:700; font-size:0.82rem; color:#f1f5f9; white-space:nowrap;">' + c.sizeFormatted + '</div>' +
        '</div>';
      }).join('');

      modal.classList.add('open');
    }

    function closeCleanModal() {
      document.getElementById('cleanModal').classList.remove('open');
    }

    async function executeClean() {
      if (!currentScanData) return;

      const folderPath = document.getElementById('folderInput').value.trim();
      const btnConfirm = document.getElementById('btnConfirmClean');
      const confirmText = document.getElementById('confirmBtnText');

      btnConfirm.disabled = true;
      confirmText.innerHTML = '<span class="spinner"></span> Menghapus Langsung...';

      try {
        const response = await fetch('/api/clean', {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({ folderPath })
        });

        const resData = await response.json();

        if (!response.ok) {
          throw new Error(resData.error || 'Gagal membersihkan penyimpanan');
        }

        closeCleanModal();
        showToast('Berhasil membersihkan ' + resData.deletedCount + ' berkas! Kapasitas dihemat: ' + resData.freedFormatted, 'success');

        await performScan();
      } catch (err) {
        showToast(err.message, 'error');
      } finally {
        btnConfirm.disabled = false;
        confirmText.textContent = 'Eksekusi Hapus Sekarang';
      }
    }

    function escapeHtml(text) {
      if (!text) return '';
      return text
        .replace(/&/g, '&amp;')
        .replace(/</g, '&lt;')
        .replace(/>/g, '&gt;')
        .replace(/"/g, '&quot;')
        .replace(/'/g, '&#039;');
    }

    document.getElementById('btnScan').addEventListener('click', performScan);
    document.getElementById('btnOpenCleanModal').addEventListener('click', openCleanModal);
    document.getElementById('btnConfirmClean').addEventListener('click', executeClean);
    document.getElementById('folderInput').addEventListener('keydown', (e) => {
      if (e.key === 'Enter') performScan();
    });

    window.addEventListener('DOMContentLoaded', () => {
      performScan();
    });
  </script>
</body>
</html>`;
}

/**
 * Server HTTP Request Handler
 */
async function handleRequest(req, res) {
  const parsedUrl = new URL(req.url, `http://${req.headers.host}`);
  const pathname = parsedUrl.pathname;

  res.setHeader('Access-Control-Allow-Origin', '*');
  res.setHeader('Access-Control-Allow-Methods', 'GET, POST, OPTIONS');
  res.setHeader('Access-Control-Allow-Headers', 'Content-Type');

  if (req.method === 'OPTIONS') {
    res.writeHead(204);
    res.end();
    return;
  }

  // GET / -> Web UI Dashboard
  if (req.method === 'GET' && pathname === '/') {
    res.writeHead(200, { 'Content-Type': 'text/html; charset=utf-8' });
    res.end(getHtmlDashboard());
    return;
  }

  // POST /api/scan -> Pindai folder target
  if (req.method === 'POST' && pathname === '/api/scan') {
    let body = '';
    req.on('data', chunk => { body += chunk; });
    req.on('end', async () => {
      try {
        const payload = body ? JSON.parse(body) : {};
        const folderPath = payload.folderPath || DEFAULT_TARGET_DIR;
        const result = await scanDirectory(folderPath);

        res.writeHead(200, { 'Content-Type': 'application/json; charset=utf-8' });
        res.end(JSON.stringify(result));
      } catch (err) {
        res.writeHead(400, { 'Content-Type': 'application/json; charset=utf-8' });
        res.end(JSON.stringify({ error: err.message }));
      }
    });
    return;
  }

  // POST /api/clean -> Eksekusi pembersihan in-place
  if (req.method === 'POST' && pathname === '/api/clean') {
    let body = '';
    req.on('data', chunk => { body += chunk; });
    req.on('end', async () => {
      try {
        const payload = body ? JSON.parse(body) : {};
        const folderPath = payload.folderPath || DEFAULT_TARGET_DIR;
        const result = await cleanStorage(folderPath, payload.targets);

        res.writeHead(200, { 'Content-Type': 'application/json; charset=utf-8' });
        res.end(JSON.stringify(result));
      } catch (err) {
        res.writeHead(400, { 'Content-Type': 'application/json; charset=utf-8' });
        res.end(JSON.stringify({ error: err.message }));
      }
    });
    return;
  }

  res.writeHead(404, { 'Content-Type': 'application/json' });
  res.end(JSON.stringify({ error: 'Endpoint tidak ditemukan' }));
}

/**
 * Menjalankan server HTTP dan membuka browser secara otomatis
 */
function startServer(port = DEFAULT_PORT) {
  const server = http.createServer(handleRequest);

  server.listen(port, () => {
    const url = `http://localhost:${port}`;
    console.log(`\n======================================================`);
    console.log(`🚀 Storage Audit & Cleaner Server Berhasil Berjalan!`);
    console.log(`📡 URL Web UI: ${url}`);
    console.log(`📁 Target Default: ${path.resolve(process.cwd(), DEFAULT_TARGET_DIR)}`);
    console.log(`⚡ Runtime: Node.js ${process.version} (Native Modules Only)`);
    console.log(`======================================================\n`);

    const startCmd = process.platform === 'win32'
      ? `start ${url}`
      : process.platform === 'darwin'
      ? `open ${url}`
      : `xdg-open ${url}`;

    exec(startCmd, (err) => {
      if (err) {
        console.log(`Buka tautan secara manual di browser: ${url}`);
      } else {
        console.log(`Browser terbuka otomatis pada ${url}`);
      }
    });
  });

  server.on('error', (err) => {
    if (err.code === 'EADDRINUSE') {
      console.warn(`Port ${port} sedang digunakan, mencoba port ${port + 1}...`);
      startServer(port + 1);
    } else {
      console.error('Terjadi kesalahan pada server:', err);
    }
  });
}

startServer(DEFAULT_PORT);
