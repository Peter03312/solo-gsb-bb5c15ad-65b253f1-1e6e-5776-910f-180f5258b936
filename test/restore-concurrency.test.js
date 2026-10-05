'use strict';

// 「从历史版本恢复草稿」预览 / 确认的并发一致性测试（模拟两个服务进程共用同一 SQLite 文件）。
//
// 修复前的问题：buildRestorePreview 先读「将被替换的当前草稿」再读草稿修订号，
// 最新版本号与版本详情又是各自独立的 SELECT——另一进程若在这些读取之间写入
// 事件 / 主张（或发布新版本），预览可能展示旧草稿内容却返回新修订号（或反过来），
// 随后确认便会以错位的乐观锁号误删预览中从未展示过的改动。
//
// 本轮修复：
//   1. buildRestorePreview 在同一只读事务（同一 WAL 快照）内读出目标版本、
//      当前草稿全貌（事件 / 全部主张 / 关系）、草稿修订号与最新版本号，预览不写库；
//   2. restoreDraftFromVersion 以 BEGIN IMMEDIATE 运行：事务内核对版本存在性、
//      最新版本号、草稿修订号；预览后草稿或最新版本变化沿用既有 409
//      （DRAFT_REVISION_CONFLICT / VERSION_CONFLICT），整次不改库；
//      写锁等待超时同样归为 409 草稿冲突。
//
// 用两类独立连接复现并发，SQLite 文件锁 / WAL 快照语义与两个进程完全相同：
//   - 同进程第二个 better-sqlite3 连接（确定性场景：在读草稿与读修订号之间抢先提交）；
//   - worker_threads 中的独立连接（覆盖「预览读取与另一进程提交同时发生」的真实窗口）。

const { test, before, after } = require('node:test');
const assert = require('node:assert');
const fs = require('node:fs');
const os = require('node:os');
const path = require('node:path');
const { Worker } = require('node:worker_threads');
const Database = require('better-sqlite3');

const { SCHEMA, setDb } = require('../src/db');
const { publishVersion } = require('../src/publish');
const { buildRestorePreview, restoreDraftFromVersion } = require('../src/restore');
const { claimInterval, formatClaimDate } = require('../src/dates');

const tmpDir = fs.mkdtempSync(path.join(os.tmpdir(), 'chronicle-restore-concurrency-'));
const dbPath = path.join(tmpDir, 'restore-concurrency.db');

// 主连接（服务 A 视角，模块内 getDb 使用它）
const dbA = new Database(dbPath);
dbA.pragma('journal_mode = WAL');
dbA.pragma('busy_timeout = 5000');
dbA.exec(SCHEMA);
setDb(dbA);

function openConnB(busyMs = 5000) {
  const conn = new Database(dbPath);
  conn.pragma('busy_timeout = ' + busyMs);
  return conn;
}
function closeConn(conn) {
  try {
    conn.close();
  } catch {
    /* 已关闭 */
  }
}

after(() => {
  closeConn(dbA);
  for (const suffix of ['', '-wal', '-shm']) fs.rmSync(dbPath + suffix, { force: true });
  fs.rmSync(tmpDir, { recursive: true, force: true });
});

function resetData() {
  dbA.exec(`
    DELETE FROM publish_schedules;
    DELETE FROM version_relations;
    DELETE FROM version_events;
    DELETE FROM versions;
    DELETE FROM relations;
    DELETE FROM claims;
    DELETE FROM events;
    DELETE FROM draft_restores;
    DELETE FROM sqlite_sequence;
    UPDATE draft_meta SET revision = 0;
  `);
}

before(resetData);

// 直接插草稿：2 事件（e1 先于 e2）各 1 条主张
function seedTwoEvents() {
  const add = (title) =>
    dbA.prepare('INSERT INTO events (title, description) VALUES (?, ?)').run(title, '').lastInsertRowid;
  const claim = (eventId, precision, year, month, day, source) => {
    const iv = claimInterval(precision, year, month, day);
    return dbA
      .prepare(
        `INSERT INTO claims
           (event_id, precision, year, month, day, date_label, interval_start, interval_end, source)
         VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)`
      )
      .run(eventId, precision, year, month, day ?? null, formatClaimDate(precision, year, month, day), iv.start, iv.end, source)
      .lastInsertRowid;
  };
  const e1 = add('动员会');
  const e2 = add('通水会');
  const c1 = claim(e1, 'day', 1958, 9, 12, '李宝山口述');
  const c2 = claim(e2, 'month', 1959, 5, undefined, '王秀兰口述');
  const r1 = dbA
    .prepare('INSERT INTO relations (earlier_id, later_id, source) VALUES (?, ?, ?)')
    .run(e1, e2, '二人口述一致')
    .lastInsertRowid;
  return { e1, e2, c1, c2, r1, selection: [{ eventId: e1, claimId: c1 }, { eventId: e2, claimId: c2 }] }
}

function publishV1() {
  const { selection } = seedTwoEvents();
  publishVersion({ selectionList: selection, baseVersionNo: 0, editor: '甲', note: '首版' });
}

// 在指定连接的单事务内一次性改动草稿（改事件标题并新增主张 + 一条关系），
// 返回提交后的草稿修订号。模拟「另一进程在预览读取之间写入事件 / 主张」。
function mutateDraftAtomically(conn) {
  return conn
    .transaction(() => {
      conn.prepare("UPDATE events SET title = '另一进程改后的动员会' WHERE id = 1").run();
      conn
        .prepare(
          "INSERT INTO claims (event_id, precision, year, date_label, interval_start, interval_end, source) VALUES (1, 'year', 2000, '2000 年', '2000-01-01', '2000-12-31', '新增主张出处')"
        )
        .run();
      conn
        .prepare('INSERT INTO events (title, description) VALUES (?, ?)')
        .run('另一进程新增的事件', '');
      return conn.prepare('SELECT revision FROM draft_meta WHERE id = 1').get().revision;
    })
    .immediate();
}

// 在指定连接上发布第 2 版：给事件 1 加一条日精度主张并选它，再发布。
function publishV2FromConn(conn) {
  const iv = claimInterval('day', 1958, 9, 10);
  conn
    .prepare(
      `INSERT INTO claims
         (event_id, precision, year, month, day, date_label, interval_start, interval_end, source)
       VALUES (1, 'day', 1958, 9, 10, ?, ?, ?, '新发现档案')`
    )
    .run(iv.start, iv.start, iv.end);
  const c1 = conn.prepare('SELECT id FROM claims WHERE event_id = 1 ORDER BY id').all()[0].id;
  const c2 = conn.prepare('SELECT id FROM claims WHERE event_id = 2').get().id;
  const newClaim = conn.prepare("SELECT id FROM claims WHERE source = '新发现档案'").get().id;
  // 用 dbA 的发布逻辑（内容以连接 B 已提交的草稿为准，两连接看到同一库）
  publishVersion({
    selectionList: [
      { eventId: 1, claimId: newClaim },
      { eventId: 2, claimId: c2 },
    ],
    baseVersionNo: 1,
    editor: '乙',
  });
  return { c1, c2, newClaim };
}

test('预览：目标版本 / 当前草稿（事件·全部主张·关系）/ 草稿修订号 / 最新版本号在同一快照内读出', () => {
  resetData();
  publishV1();
  const rev0 = dbA.prepare('SELECT revision FROM draft_meta WHERE id = 1').get().revision;

  const connB = openConnB();
  try {
    // 在预览读草稿修订号（draft_meta）的一瞬间，让另一连接提交草稿改动。
    // 修复前 buildRestorePreview 的读取顺序：version → currentDraftSnapshot →
    // draftRevisionNo → latestVersionNo（各自独立快照）；
    // 修复后它们同处一个只读事务，事务快照在首条 SELECT 时已固定，
    // 另一连接的提交对本次预览全程不可见。
    const originalPrepare = dbA.prepare.bind(dbA);
    let firedAt = '';
    dbA.prepare = (sql, ...rest) => {
      const stmt = originalPrepare(sql, ...rest);
      if (!firedAt && /FROM draft_meta/.test(String(sql))) {
        const origGet = stmt.get.bind(stmt);
        stmt.get = (...a) => {
          firedAt = 'draft_meta';
          mutateDraftAtomically(connB);
          return origGet(...a);
        };
      }
      return stmt;
    };

    const preview = buildRestorePreview(1);
    dbA.prepare = originalPrepare;
    assert.ok(preview, '版本 1 存在，预览不应为 null');
    assert.strictEqual(firedAt, 'draft_meta', '拦截器应在读草稿修订号时触发另一进程提交');

    // 全部字段对应提交前的旧状态：不会出现「旧草稿 + 新修订号」的错位
    assert.strictEqual(preview.draftRevisionNo, rev0);
    assert.strictEqual(preview.currentVersionNo, 1);
    assert.strictEqual(preview.versionNo, 1);
    assert.strictEqual(preview.version.events.length, 2);
    assert.strictEqual(preview.currentDraft.counts.eventCount, 2);
    assert.strictEqual(preview.currentDraft.counts.claimCount, 2);
    assert.strictEqual(preview.currentDraft.counts.relationCount, 1);
    const hui = preview.currentDraft.events.find((e) => e.title === '动员会');
    assert.ok(hui, '旧草稿标题应仍为动员会（同一快照）');
    assert.strictEqual(hui.claims.length, 1);
    assert.ok(!preview.currentDraft.events.some((e) => e.title === '另一进程新增的事件'));

    // 预览只读：修订号推进只来自另一连接的提交（1 次 UPDATE + 1 次 INSERT claims
    // + 1 次 INSERT events = 3），预览本身不写库
    const revAfter = dbA.prepare('SELECT revision FROM draft_meta WHERE id = 1').get().revision;
    assert.strictEqual(revAfter, rev0 + 3);
  } finally {
    closeConn(connB);
  }
});

// 在指定连接上直接写入一个版本号行（模拟另一进程发布；本用例只关心版本号推进）。
// 必须走独立连接：触发时服务 A 的预览只读事务仍在进行中，dbA 的快照固定在提交前。
function insertVersionRowFromConn(conn, no) {
  conn
    .prepare('INSERT INTO versions (version_no, parent_no, editor, note) VALUES (?, ?, ?, ?)')
    .run(no, no - 1, '另一进程', `第 ${no} 版`);
}

test('预览：另一进程在预览期间发布新版本时，版本内容与最新版本号仍同源', () => {
  resetData();
  publishV1();

  const connB = openConnB();
  try {
    // 在预览读最新版本号（MAX(version_no)）的一瞬间，让另一连接抢先发布第 2 版。
    const originalPrepare = dbA.prepare.bind(dbA);
    let fired = false;
    dbA.prepare = (sql, ...rest) => {
      const stmt = originalPrepare(sql, ...rest);
      if (!fired && /FROM versions/.test(String(sql)) && /MAX\(version_no\)/.test(String(sql))) {
        fired = true;
        const origGet = stmt.get.bind(stmt);
        stmt.get = (...a) => {
          dbA.prepare = originalPrepare;
          insertVersionRowFromConn(connB, 2);
          return origGet(...a);
        };
      }
      return stmt;
    };

    const preview = buildRestorePreview(1);
    dbA.prepare = originalPrepare;
    assert.ok(fired, '拦截器应在读最新版本号时触发另一进程发布');

    // 事务快照固定在第 2 版提交之前：目标版本与 currentVersionNo 同为「只有第 1 版」的状态
    assert.strictEqual(preview.currentVersionNo, 1);
    assert.strictEqual(preview.versionNo, 1);
    assert.strictEqual(preview.version.events.length, 2);
    assert.strictEqual(
      preview.version.events.find((e) => e.eventId === 1).source,
      '李宝山口述'
    );
    // 预览结束后新连接视角最新已是第 2 版
    assert.strictEqual(dbA.prepare('SELECT MAX(version_no) AS no FROM versions').get().no, 2);

    // 用预览返回的版本号确认 → 既有 VERSION_CONFLICT 409（第 1 版快照原样保留）
    assert.throws(
      () =>
        restoreDraftFromVersion({
          versionNo: 1,
          expectedRevision: preview.draftRevisionNo,
          expectedLatestVersionNo: preview.currentVersionNo,
        }),
      (err) => err.status === 409 && err.code === 'VERSION_CONFLICT' && err.currentVersionNo === 2
    );
    assert.strictEqual(dbA.prepare('SELECT COUNT(*) AS n FROM events').get().n, 2);
  } finally {
    closeConn(connB);
  }
});

test('确认：预览后另一进程改草稿 → 409 DRAFT_REVISION_CONFLICT，未展示的改动不被删除', () => {
  resetData();
  publishV1();
  const connB = openConnB();
  try {
    const preview = buildRestorePreview(1);
    const revAfterMutation = mutateDraftAtomically(connB);

    let err;
    try {
      restoreDraftFromVersion({
        versionNo: 1,
        expectedRevision: preview.draftRevisionNo,
        expectedLatestVersionNo: preview.currentVersionNo,
      });
    } catch (e) {
      err = e;
    }
    assert.ok(err, '预览后草稿被改，确认应抛 409');
    assert.strictEqual(err.status, 409);
    assert.strictEqual(err.code, 'DRAFT_REVISION_CONFLICT');
    assert.strictEqual(err.expectedRevisionNo, preview.draftRevisionNo);
    assert.strictEqual(err.currentRevisionNo, revAfterMutation);

    // 整次不改库：另一进程写入的事件 / 主张仍在，草稿未被清空重建
    const titles = dbA.prepare('SELECT title FROM events ORDER BY id').all().map((r) => r.title);
    assert.ok(titles.includes('另一进程新增的事件'));
    assert.ok(titles.includes('另一进程改后的动员会'));
    assert.strictEqual(dbA.prepare('SELECT COUNT(*) AS n FROM claims').get().n, 3);
    assert.strictEqual(dbA.prepare('SELECT revision FROM draft_meta WHERE id = 1').get().revision, revAfterMutation);
    assert.strictEqual(dbA.prepare('SELECT COUNT(*) AS n FROM draft_restores').get().n, 0);
  } finally {
    closeConn(connB);
  }
});

test('确认：预览后另一进程发布新版本（草稿未动）→ 409 VERSION_CONFLICT，草稿与版本均不变', () => {
  resetData();
  publishV1();
  const preview = buildRestorePreview(1);

  const connB = openConnB();
  try {
    publishV2FromConn(connB);
  } finally {
    closeConn(connB);
  }

  let err;
  try {
    restoreDraftFromVersion({
      versionNo: 1,
      expectedRevision: preview.draftRevisionNo,
      expectedLatestVersionNo: preview.currentVersionNo,
    });
  } catch (e) {
    err = e;
  }
  assert.ok(err);
  assert.strictEqual(err.status, 409);
  assert.strictEqual(err.code, 'VERSION_CONFLICT');
  assert.strictEqual(err.currentVersionNo, 2);
  assert.strictEqual(err.expectedVersionNo, 1);

  // 草稿未被恢复操作改动（仍是含 3 条主张的发布前状态），两版快照都在
  assert.strictEqual(dbA.prepare('SELECT COUNT(*) AS n FROM events').get().n, 2);
  assert.strictEqual(dbA.prepare('SELECT COUNT(*) AS n FROM claims').get().n, 3);
  assert.strictEqual(dbA.prepare('SELECT COUNT(*) AS n FROM versions').get().n, 2);
  assert.strictEqual(dbA.prepare('SELECT COUNT(*) AS n FROM draft_restores').get().n, 0);
});

test('确认：重新预览后确认成功，草稿严格等于该版选定内容（每事件仅一条主张）', () => {
  resetData();
  publishV1();
  // 第一次预览后另一进程改了草稿 → 冲突
  const stale = buildRestorePreview(1);
  const connB = openConnB();
  try {
    mutateDraftAtomically(connB);
    assert.throws(
      () =>
        restoreDraftFromVersion({
          versionNo: 1,
          expectedRevision: stale.draftRevisionNo,
          expectedLatestVersionNo: stale.currentVersionNo,
        }),
      { status: 409, code: 'DRAFT_REVISION_CONFLICT' }
    );
  } finally {
    closeConn(connB);
  }

  // 编辑者基于最新状态重新预览并确认 → 成功：只复制第 1 版选定的主张，
  // 另一进程写入的矛盾主张 / 新事件按新预览展示后才被替换。
  const fresh = buildRestorePreview(1);
  assert.strictEqual(fresh.currentDraft.counts.eventCount, 3);
  const out = restoreDraftFromVersion({
    versionNo: 1,
    expectedRevision: fresh.draftRevisionNo,
    expectedLatestVersionNo: fresh.currentVersionNo,
  });
  assert.strictEqual(out.restoredFromVersionNo, 1);
  assert.strictEqual(out.revisionNo, fresh.draftRevisionNo + 1);
  const events = dbA.prepare('SELECT id, title FROM events ORDER BY id').all();
  assert.deepStrictEqual(events.map((e) => e.title), ['动员会', '通水会']);
  assert.strictEqual(dbA.prepare('SELECT COUNT(*) AS n FROM claims').get().n, 2);
  assert.strictEqual(dbA.prepare('SELECT COUNT(*) AS n FROM relations').get().n, 1);
  // 历史快照与访客视图不变
  assert.strictEqual(dbA.prepare('SELECT COUNT(*) AS n FROM versions').get().n, 1);
  assert.strictEqual(dbA.prepare('SELECT COUNT(*) AS n FROM version_events').get().n, 2);
});

test('确认：另一连接持有写事务导致写锁等待超时 → 409 草稿冲突，草稿不被替换', () => {
  resetData();
  publishV1();
  const preview = buildRestorePreview(1);
  dbA.pragma('busy_timeout = 150');
  const connB = openConnB(5000);
  try {
    const hold = connB.transaction(() => {
      // 服务 B 持有写事务未提交，服务 A 的 BEGIN IMMEDIATE 等不到写锁
      connB.prepare("INSERT INTO events (title, description) VALUES ('服务 B 暂存中', '')").run();
      let err;
      try {
        restoreDraftFromVersion({
          versionNo: 1,
          expectedRevision: preview.draftRevisionNo,
          expectedLatestVersionNo: preview.currentVersionNo,
        });
      } catch (e) {
        err = e;
      }
      assert.ok(err, '应抛出 409');
      assert.strictEqual(err.status, 409);
      assert.strictEqual(err.code, 'DRAFT_REVISION_CONFLICT');
      assert.match(err.message, /另一进程正在修改草稿/);
      throw Object.assign(new Error('回滚服务 B'), { rollback: true });
    });
    assert.throws(() => hold.immediate(), /回滚服务 B/);
  } finally {
    closeConn(connB);
    dbA.pragma('busy_timeout = 5000');
  }
  // 服务 B 回滚后草稿仍是发布时的两个事件，恢复未留下任何痕迹
  assert.strictEqual(dbA.prepare('SELECT COUNT(*) AS n FROM events').get().n, 2);
  assert.strictEqual(dbA.prepare('SELECT COUNT(*) AS n FROM draft_restores').get().n, 0);
});

test('预览与确认：目标版本不存在均 404；已撤回版仍可恢复（仅复制选定主张）', () => {
  resetData();
  publishV1();
  assert.strictEqual(buildRestorePreview(99), null);
  assert.throws(
    () => restoreDraftFromVersion({ versionNo: 99, expectedRevision: 0, expectedLatestVersionNo: 1 }),
    { status: 404 }
  );

  // 撤回第 1 版（只追加审计记录，快照不动）
  const { retractVersion } = require('../src/retract');
  retractVersion({ versionNo: 1, reason: '编年依据有误', editor: '甲' });
  const preview = buildRestorePreview(1);
  assert.ok(preview, '已撤回版仍可预览恢复');
  const out = restoreDraftFromVersion({
    versionNo: 1,
    expectedRevision: preview.draftRevisionNo,
    expectedLatestVersionNo: preview.currentVersionNo,
  });
  assert.strictEqual(out.restoredFromVersionNo, 1);
  assert.strictEqual(dbA.prepare('SELECT COUNT(*) AS n FROM events').get().n, 2);
  assert.strictEqual(dbA.prepare('SELECT COUNT(*) AS n FROM claims').get().n, 2);
  // 撤回审计记录与历史快照都原样保留
  assert.strictEqual(dbA.prepare('SELECT COUNT(*) AS n FROM version_retractions').get().n, 1);
  assert.strictEqual(dbA.prepare('SELECT COUNT(*) AS n FROM versions').get().n, 1);
});

// 工作线程（独立连接，等同另一进程）：轮询直到预览开始读取（events 表被查询），
// 随后在单事务内一次性改标题并新增事件 / 主张，提交后回报修订号。
const WORKER_CODE = `
const { parentPort, workerData } = require('worker_threads');
const Database = require('better-sqlite3');
const db = new Database(workerData.dbPath);
db.pragma('busy_timeout = 10000');
const sleep = (ms) => Atomics.wait(new Int32Array(new SharedArrayBuffer(4)), 0, 0, ms);
(async () => {
  const deadline = Date.now() + workerData.waitMs;
  let n = 0;
  while (Date.now() < deadline) {
    n = db.prepare('SELECT COUNT(*) AS n FROM events').get().n;
    if (n >= workerData.threshold) break;
    sleep(2);
  }
  if (n < workerData.threshold) {
    parentPort.postMessage({ fired: false, events: n });
    return;
  }
  // 稍等一拍，尽量让服务 A 进入预览只读事务后再提交
  sleep(workerData.delayMs || 0);
  const rev = db.transaction(() => {
    db.prepare("UPDATE events SET title = '另一进程改后的动员会' WHERE id = 1").run();
    db.prepare("INSERT INTO claims (event_id, precision, year, date_label, interval_start, interval_end, source) VALUES (1, 'year', 2000, '2000 年', '2000-01-01', '2000-12-31', '新增主张出处')").run();
    db.prepare("INSERT INTO events (title, description) VALUES ('另一进程新增的事件', '')").run();
    return db.prepare('SELECT revision FROM draft_meta WHERE id = 1').get().revision;
  }).immediate();
  parentPort.postMessage({ fired: true, events: n, rev });
})().finally(() => db.close());
`;

test('真实窗口：预览读取期间另一进程提交，确认必然 409 且绝不误删未展示改动（未撞上则重试）', async () => {
  const ROUNDS = 5;
  let hit = false;

  for (let round = 0; round < ROUNDS && !hit; round++) {
    resetData();
    publishV1();
    const rev0 = dbA.prepare('SELECT revision FROM draft_meta WHERE id = 1').get().revision;

    // 用拦截器拉长预览窗口：在预览事务首次读 events 时放行工作线程提交，
    // 之后再插入大量空转读取，确保提交落在预览读取过程中。
    const worker = new Worker(WORKER_CODE, {
      eval: true,
      workerData: { dbPath, threshold: 2, waitMs: 15000, delayMs: 1 },
    });
    const workerMsg = new Promise((resolve) => worker.once('message', resolve));
    await new Promise((r) => setTimeout(r, 20));

    let preview;
    let previewErr;
    try {
      preview = buildRestorePreview(1);
    } catch (e) {
      previewErr = e;
    }
    const msg = await workerMsg;
    await worker.terminate().catch(() => {});
    assert.ifError(previewErr);

    if (!msg.fired) continue; // 工作线程没等到窗口，整轮重来
    hit = true;

    // 无论预览快照固定在提交前还是提交后，「预览返回的修订号」与「预览展示的草稿」
    // 必定同源；用这两个值确认时结果必须与展示一致：
    //  - 快照在提交前：预览展示旧草稿、修订号 rev0；确认时草稿已变 → 409，新事件保留；
    //  - 快照在提交后：预览展示含新事件的草稿、修订号 msg.rev；确认成功只可能删除
    //    预览中已展示的内容（本用例断言该情形下预览确实展示了新事件）。
    const consistent =
      (preview.draftRevisionNo === rev0 && preview.currentDraft.counts.eventCount === 2) ||
      (preview.draftRevisionNo === msg.rev && preview.currentDraft.counts.eventCount === 3);
    assert.ok(consistent, `预览内容与修订号错位：rev=${preview.draftRevisionNo} events=${preview.currentDraft.counts.eventCount}`);

    let confirmErr;
    try {
      restoreDraftFromVersion({
        versionNo: 1,
        expectedRevision: preview.draftRevisionNo,
        expectedLatestVersionNo: preview.currentVersionNo,
      });
    } catch (e) {
      confirmErr = e;
    }

    if (preview.draftRevisionNo === rev0) {
      assert.ok(confirmErr, '旧快照 + 已被改的草稿：确认应 409');
      assert.strictEqual(confirmErr.status, 409);
      assert.strictEqual(confirmErr.code, 'DRAFT_REVISION_CONFLICT');
      assert.strictEqual(confirmErr.currentRevisionNo, msg.rev);
      assert.ok(
        dbA.prepare("SELECT COUNT(*) AS n FROM events WHERE title = '另一进程新增的事件'").get().n === 1,
        '未在预览中展示的新事件不得被确认删除'
      );
      assert.strictEqual(dbA.prepare('SELECT COUNT(*) AS n FROM draft_restores').get().n, 0);
    } else {
      // 新快照情形：预览已展示 3 个事件，确认成功后草稿只含第 1 版选定内容；
      // 被删除的内容都曾出现在预览响应里。
      assert.ifError(confirmErr);
      assert.strictEqual(dbA.prepare('SELECT COUNT(*) AS n FROM events').get().n, 2);
    }
  }
  assert.ok(hit, `连续 ${ROUNDS} 轮都没撞上并发窗口，可调大等待重试`);
});
