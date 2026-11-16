'use strict';

const fs = require('node:fs/promises');
const path = require('node:path');
const crypto = require('node:crypto');

const DEFAULT_PARTITION = '_default';
const MANIFEST_NAME = 'manifest.json';

function keyAccessor(key) {
  if (typeof key === 'function') return key;
  return (record) => record[key];
}

function compareValues(a, b) {
  if (a === b) return 0;
  if (a === undefined || a === null) return -1;
  if (b === undefined || b === null) return 1;
  if (typeof a === 'number' && typeof b === 'number') return a - b;
  const sa = String(a);
  const sb = String(b);
  return sa < sb ? -1 : 1;
}

function sha256(buffer) {
  return crypto.createHash('sha256').update(buffer).digest('hex');
}

async function pathExists(filePath) {
  try {
    await fs.access(filePath);
    return true;
  } catch {
    return false;
  }
}

async function writeFileAtomic(dir, name, content) {
  const tmpName = `${name}.tmp-${process.pid}-${crypto.randomBytes(6).toString('hex')}`;
  const tmpPath = path.join(dir, tmpName);
  await fs.writeFile(tmpPath, content);
  await fs.rename(tmpPath, path.join(dir, name));
}

function partitionDirName(partition) {
  return `part=${encodeURIComponent(partition)}`;
}

function snapshotDirName(snapshotId) {
  return `snapshot=${encodeURIComponent(snapshotId)}`;
}

async function exportSnapshot(options) {
  const {
    outputDir,
    snapshotId,
    sortKey,
    partitionKey,
    targetFileSize,
    records,
    hooks = {},
  } = options;

  if (!outputDir) throw new Error('outputDir is required');
  if (!snapshotId) throw new Error('snapshotId is required');
  if (!sortKey) throw new Error('sortKey is required');
  if (!partitionKey) throw new Error('partitionKey is required');
  if (!Number.isInteger(targetFileSize) || targetFileSize <= 0) {
    throw new Error('targetFileSize must be a positive integer');
  }
  if (!records) throw new Error('records is required');

  const sortFn = keyAccessor(sortKey);
  const partFn = keyAccessor(partitionKey);

  const collected = [];
  for await (const record of records) {
    collected.push(record);
  }

  const decorated = collected.map((record, index) => ({ record, index }));
  decorated.sort((x, y) => {
    const cmp = compareValues(sortFn(x.record), sortFn(y.record));
    return cmp !== 0 ? cmp : x.index - y.index;
  });

  const partitions = new Map();
  for (const { record } of decorated) {
    let value = partFn(record);
    if (value === null || value === undefined || value === '') {
      value = DEFAULT_PARTITION;
    }
    const partition = String(value);
    if (!partitions.has(partition)) partitions.set(partition, []);
    partitions.get(partition).push(record);
  }

  const snapshotDir = path.join(outputDir, snapshotDirName(snapshotId));
  const shards = [];

  for (const [partition, partitionRecords] of partitions) {
    const partDir = path.join(snapshotDir, partitionDirName(partition));
    await fs.mkdir(partDir, { recursive: true });

    let seq = 0;
    let buffer = [];
    let bufferBytes = 0;

    const flush = async () => {
      if (buffer.length === 0) return;
      const content = Buffer.from(
        buffer.map((r) => JSON.stringify(r)).join('\n') + '\n',
        'utf8',
      );
      const name = `shard-${String(seq).padStart(5, '0')}.ndjson`;
      const digest = sha256(content);
      const filePath = path.join(partDir, name);

      let reused = false;
      if (await pathExists(filePath)) {
        const existing = await fs.readFile(filePath);
        if (sha256(existing) === digest) reused = true;
      }
      if (!reused) {
        await writeFileAtomic(partDir, name, content);
        if (hooks.onShardWritten) await hooks.onShardWritten(filePath);
      }

      shards.push({
        partition,
        seq,
        path: path.relative(outputDir, filePath),
        records: buffer.length,
        bytes: content.length,
        sha256: digest,
      });
      seq += 1;
      buffer = [];
      bufferBytes = 0;
    };

    for (const record of partitionRecords) {
      const lineBytes = Buffer.byteLength(JSON.stringify(record), 'utf8') + 1;
      if (buffer.length > 0 && bufferBytes + lineBytes > targetFileSize) {
        await flush();
      }
      buffer.push(record);
      bufferBytes += lineBytes;
    }
    await flush();
  }

  for (const shard of shards) {
    const data = await fs.readFile(path.join(outputDir, shard.path));
    const actual = sha256(data);
    if (actual !== shard.sha256) {
      throw new Error(
        `checksum mismatch for ${shard.path}: expected ${shard.sha256}, got ${actual}`,
      );
    }
  }

  const manifest = {
    version: 1,
    snapshotId,
    createdAt: new Date().toISOString(),
    targetFileSize,
    sortKey: typeof sortKey === 'string' ? sortKey : null,
    partitionKey: typeof partitionKey === 'string' ? partitionKey : null,
    shards,
  };

  await fs.mkdir(snapshotDir, { recursive: true });
  await writeFileAtomic(snapshotDir, MANIFEST_NAME, JSON.stringify(manifest, null, 2));
  return manifest;
}

async function verifyExport(outputDir, snapshotId) {
  const snapshotDir = path.join(outputDir, snapshotDirName(snapshotId));
  const manifest = JSON.parse(
    await fs.readFile(path.join(snapshotDir, MANIFEST_NAME), 'utf8'),
  );
  for (const shard of manifest.shards) {
    const data = await fs.readFile(path.join(outputDir, shard.path));
    const actual = sha256(data);
    if (actual !== shard.sha256) {
      throw new Error(
        `checksum mismatch for ${shard.path}: expected ${shard.sha256}, got ${actual}`,
      );
    }
    if (data.length !== shard.bytes) {
      throw new Error(
        `size mismatch for ${shard.path}: expected ${shard.bytes}, got ${data.length}`,
      );
    }
  }
  return manifest;
}

module.exports = {
  exportSnapshot,
  verifyExport,
  DEFAULT_PARTITION,
  MANIFEST_NAME,
};
