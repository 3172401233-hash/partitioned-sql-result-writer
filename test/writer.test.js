'use strict';

const test = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs/promises');
const os = require('node:os');
const path = require('node:path');

const {
  exportSnapshot,
  verifyExport,
  DEFAULT_PARTITION,
  MANIFEST_NAME,
} = require('../src/writer');

async function makeTempDir() {
  return fs.mkdtemp(path.join(os.tmpdir(), 'partitioned-writer-'));
}

function snapshotDir(outputDir, snapshotId) {
  return path.join(outputDir, `snapshot=${snapshotId}`);
}

async function readShardLines(outputDir, shard) {
  const content = await fs.readFile(path.join(outputDir, shard.path), 'utf8');
  return content
    .trim()
    .split('\n')
    .map((line) => JSON.parse(line));
}

async function* toStream(records) {
  for (const record of records) yield record;
}

test('单行记录超过目标大小时仍独占一个 shard', async () => {
  const dir = await makeTempDir();
  const bigRecord = { k: 1, region: 'us', payload: 'x'.repeat(1024) };

  const manifest = await exportSnapshot({
    outputDir: dir,
    snapshotId: 'snap-oversize',
    sortKey: 'k',
    partitionKey: 'region',
    targetFileSize: 64,
    records: [bigRecord],
  });

  assert.equal(manifest.shards.length, 1);
  const shard = manifest.shards[0];
  assert.ok(shard.bytes > 64, 'shard 体积应超过目标大小');
  assert.equal(shard.records, 1);
  const lines = await readShardLines(dir, shard);
  assert.deepEqual(lines, [bigRecord]);
  await verifyExport(dir, 'snap-oversize');
});

test('空记录流生成空 manifest，空分区键归入默认分区', async () => {
  const dir = await makeTempDir();

  const emptyManifest = await exportSnapshot({
    outputDir: dir,
    snapshotId: 'snap-empty',
    sortKey: 'k',
    partitionKey: 'region',
    targetFileSize: 128,
    records: [],
  });
  assert.deepEqual(emptyManifest.shards, []);
  const manifestPath = path.join(snapshotDir(dir, 'snap-empty'), MANIFEST_NAME);
  const onDisk = JSON.parse(await fs.readFile(manifestPath, 'utf8'));
  assert.deepEqual(onDisk.shards, []);
  await verifyExport(dir, 'snap-empty');

  const nullKeyManifest = await exportSnapshot({
    outputDir: dir,
    snapshotId: 'snap-null-partition',
    sortKey: 'k',
    partitionKey: 'region',
    targetFileSize: 128,
    records: [
      { k: 1, region: null },
      { k: 2 },
      { k: 3, region: '' },
    ],
  });
  assert.equal(nullKeyManifest.shards.length, 1);
  assert.equal(nullKeyManifest.shards[0].partition, DEFAULT_PARTITION);
  const lines = await readShardLines(dir, nullKeyManifest.shards[0]);
  assert.equal(lines.length, 3);
});

test('排序键重复时保持稳定的输入相对顺序', async () => {
  const dir = await makeTempDir();
  const records = [
    { k: 2, region: 'us', tag: 'a' },
    { k: 1, region: 'us', tag: 'b' },
    { k: 2, region: 'us', tag: 'c' },
    { k: 1, region: 'us', tag: 'd' },
    { k: 2, region: 'us', tag: 'e' },
  ];

  const manifest = await exportSnapshot({
    outputDir: dir,
    snapshotId: 'snap-dup-keys',
    sortKey: 'k',
    partitionKey: 'region',
    targetFileSize: 1 << 20,
    records,
  });

  const lines = await readShardLines(dir, manifest.shards[0]);
  assert.deepEqual(
    lines.map((r) => r.tag),
    ['b', 'd', 'a', 'c', 'e'],
  );

  const again = await exportSnapshot({
    outputDir: dir,
    snapshotId: 'snap-dup-keys-2',
    sortKey: 'k',
    partitionKey: 'region',
    targetFileSize: 1 << 20,
    records: [...records],
  });
  const linesAgain = await readShardLines(dir, again.shards[0]);
  assert.deepEqual(linesAgain, lines);
});

test('中断后重跑复用已完成的 shard 并最终发布 manifest', async () => {
  const dir = await makeTempDir();
  const records = [];
  for (let i = 0; i < 6; i += 1) records.push({ k: i, region: 'aa', v: `a${i}` });
  for (let i = 6; i < 12; i += 1) records.push({ k: i, region: 'bb', v: `b${i}` });

  async function* failingStream() {
    for (let i = 0; i < records.length; i += 1) {
      if (i === 8) throw new Error('simulated crash mid-export');
      yield records[i];
    }
  }

  await assert.rejects(
    exportSnapshot({
      outputDir: dir,
      snapshotId: 'snap-resume',
      sortKey: 'k',
      partitionKey: 'region',
      targetFileSize: 40,
      records: failingStream(),
    }),
    /simulated crash/,
  );
  const manifestPath = path.join(snapshotDir(dir, 'snap-resume'), MANIFEST_NAME);
  await assert.rejects(fs.access(manifestPath), '中断后不应存在 manifest');

  let firstRunWrites = 0;
  const firstManifest = await exportSnapshot({
    outputDir: dir,
    snapshotId: 'snap-resume',
    sortKey: 'k',
    partitionKey: 'region',
    targetFileSize: 40,
    records: toStream(records),
    hooks: {
      onShardWritten: () => {
        firstRunWrites += 1;
      },
    },
  });
  assert.ok(firstManifest.shards.length > 0);

  let secondRunWrites = 0;
  const secondManifest = await exportSnapshot({
    outputDir: dir,
    snapshotId: 'snap-resume',
    sortKey: 'k',
    partitionKey: 'region',
    targetFileSize: 40,
    records: toStream(records),
    hooks: {
      onShardWritten: () => {
        secondRunWrites += 1;
      },
    },
  });

  assert.equal(secondRunWrites, 0, '已完成且校验一致的 shard 不应重写');
  assert.deepEqual(
    secondManifest.shards.map((s) => [s.path, s.sha256]),
    firstManifest.shards.map((s) => [s.path, s.sha256]),
    '重试必须覆盖同一组逻辑 shard',
  );
  await verifyExport(dir, 'snap-resume');
});

test('摘要校验失败时不发布 manifest，verifyExport 能发现篡改', async () => {
  const dir = await makeTempDir();
  const records = [
    { k: 1, region: 'us' },
    { k: 2, region: 'us' },
  ];

  let corrupted = false;
  await assert.rejects(
    exportSnapshot({
      outputDir: dir,
      snapshotId: 'snap-corrupt',
      sortKey: 'k',
      partitionKey: 'region',
      targetFileSize: 1 << 20,
      records: toStream(records),
      hooks: {
        onShardWritten: async (filePath) => {
          if (!corrupted) {
            corrupted = true;
            await fs.appendFile(filePath, 'tampered\n');
          }
        },
      },
    }),
    /checksum mismatch/,
  );
  const manifestPath = path.join(snapshotDir(dir, 'snap-corrupt'), MANIFEST_NAME);
  await assert.rejects(fs.access(manifestPath), '校验失败时不得发布 manifest');

  const good = await exportSnapshot({
    outputDir: dir,
    snapshotId: 'snap-good',
    sortKey: 'k',
    partitionKey: 'region',
    targetFileSize: 1 << 20,
    records: toStream(records),
  });
  await verifyExport(dir, 'snap-good');

  const shardPath = path.join(dir, good.shards[0].path);
  await fs.appendFile(shardPath, 'tampered\n');
  await assert.rejects(verifyExport(dir, 'snap-good'), /mismatch/);
});

test('并发导出不同快照互不干扰', async () => {
  const dir = await makeTempDir();

  const makeRecords = (prefix) =>
    Array.from({ length: 50 }, (_, i) => ({
      k: i,
      region: i % 2 === 0 ? 'us' : 'eu',
      v: `${prefix}-${i}`,
    }));

  const [manifestA, manifestB] = await Promise.all([
    exportSnapshot({
      outputDir: dir,
      snapshotId: 'snap-concurrent-a',
      sortKey: 'k',
      partitionKey: 'region',
      targetFileSize: 128,
      records: toStream(makeRecords('a')),
    }),
    exportSnapshot({
      outputDir: dir,
      snapshotId: 'snap-concurrent-b',
      sortKey: 'k',
      partitionKey: 'region',
      targetFileSize: 128,
      records: toStream(makeRecords('b')),
    }),
  ]);

  assert.ok(manifestA.shards.length > 1);
  assert.ok(manifestB.shards.length > 1);
  await verifyExport(dir, 'snap-concurrent-a');
  await verifyExport(dir, 'snap-concurrent-b');

  const entries = await fs.readdir(dir, { recursive: true });
  const leftovers = entries.filter((name) => String(name).includes('.tmp-'));
  assert.deepEqual(leftovers, [], '不应残留临时文件');
});
