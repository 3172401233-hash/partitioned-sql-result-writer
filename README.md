# partitioned-sql-result-writer
JavaScript 查询结果分区写入库，处理稳定排序、多文件输出、续写和完整性清单

## 用法

```js
const { exportSnapshot, verifyExport } = require('./src/writer');

const manifest = await exportSnapshot({
  outputDir: '/data/exports',
  snapshotId: 'snap-2026-11-16', // 固定快照标识，重试使用同一标识
  sortKey: 'id',                 // 排序键（字段名或函数），重复键保持输入相对顺序
  partitionKey: 'region',        // 分区键（字段名或函数），空值归入 _default
  targetFileSize: 64 * 1024 * 1024,
  records: recordStream,         // 数组或异步迭代器
});
```

输出布局：

```
<outputDir>/
  snapshot=<snapshotId>/
    part=<partition>/shard-00000.ndjson   # 带 sha256 摘要的数据分片
    manifest.json                         # 全部分片校验通过后才原子发布
```

## 行为约定

- 每个 shard 先写临时文件再原子 rename，单行超过目标大小时独占一个 shard。
- 重试使用相同 `snapshotId` 会覆盖同一组逻辑 shard；已存在且摘要一致的
  shard 直接复用（断点续写），不会重写。
- manifest 只在所有 shard 写入并重新校验 sha256 通过后，才通过
  临时文件 + rename 原子发布；任何一步失败都不会留下 manifest。

## 消费端约定

- **只读取 manifest**：消费者只允许以 `snapshot=<id>/manifest.json` 的
  出现作为导出完成的信号，然后按 manifest 中的 `shards` 列表读取数据，
  并逐个校验 `sha256`（可用 `verifyExport`）。
- 不要直接扫描输出目录或读取 shard 文件：写入中的分区和临时文件
  （`*.tmp-*`）对消费者不可见也不应被假设为完整数据。
- 没有 manifest 的快照目录视为未完成导出，应整体忽略。

## 测试

```sh
node --test
```
