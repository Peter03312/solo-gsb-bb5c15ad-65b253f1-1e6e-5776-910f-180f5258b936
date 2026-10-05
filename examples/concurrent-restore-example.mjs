#!/usr/bin/env node
'use strict';

// 两个服务进程共用同一数据库时，「从历史版本恢复草稿（预览 → 确认）」的并发端到端示例。
//
// 背景：生产上可能起两个 node src/server.js 进程（容器副本 / 双机热备）共用同一个
// SQLite 文件（better-sqlite3 + WAL）。本轮修复前，恢复预览的多次读取各自取独立快照：
// 先读「将被替换的草稿」，再读草稿修订号 / 最新版本号——另一进程若在其间写入事件 /
// 主张（或发布新版本），预览可能展示旧草稿却返回新修订号，随后确认便会误删预览中
// 从未展示过的改动。
//
// 本脚本自动拉起两个共享同一 DB_PATH 的服务进程（端口不同），只通过编辑者 HTTP API
// 操作，演示修复后的三类并发裁决：
//
//   场景一：草稿冲突（既有语义 DRAFT_REVISION_CONFLICT）
//     甲在进程 A 取得恢复预览（draftRevisionNo / currentVersionNo 与展示的草稿同源），
//     乙在进程 B 于确认前新增事件 + 主张。甲用预览返回的修订号确认 → 409，
//     草稿一字未动（乙的新事件保留）；甲重新预览（此时预览已展示新事件）后确认成功。
//
//   场景二：版本冲突（既有语义 VERSION_CONFLICT）
//     甲预览后、确认前，乙在进程 B 发布了更新的版本。甲确认 → 409，
//     currentVersionNo 指向新版本；草稿与历史快照均未改动。
//
//   场景三：预览只读且各字段同源（本轮修复点）
//     在甲的预览请求进行期间，让乙并发改草稿。预览绝不会返回「旧草稿内容 + 新修订号」
//     的错位组合：响应里 currentDraft 的事件数与 draftRevisionNo 必定对应同一数据库
//     状态；随后用该响应确认，结果与展示严格一致（旧快照 → 409；新快照 → 成功，
//     且被替换的内容都曾出现在预览响应中）。预览本身不写库。
//
// 依赖：仅 Node >= 20（内置 fetch 与 child_process）；脚本结束自动关掉两个服务并删临时库。
//
// 用法：
//   node examples/concurrent-restore-example.mjs
//   可选：PORT_A=4201 PORT_B=4202、KEEP=1（结束时保留临时数据库与服务，便于人工复查）。

import { spawn } from 'node:child_process';
import os from 'node:os';
import fs from 'node:fs';
import path from 'node:path';
import { fileURLToPath } from 'node:url';

const ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), '..');
const PORT_A = Number(process.env.PORT_A || 4201);
const PORT_B = Number(process.env.PORT_B || 4202);
const TOKEN = 'example-token';
const KEEP = process.env.KEEP === '1';
const TS = Date.now();

const tmpDir = fs.mkdtempSync(path.join(os.tmpdir(), 'chronicle-restore-cc-'));
const dbPath = path.join(tmpDir, 'shared.db');

const say = (t) => console.log(`\n\x1b[1;36m== ${t} ==\x1b[0m`);
const ok = (m) => console.log(`  \x1b[32mOK\x1b[0m ${m}`);
const info = (m) => console.log(`  ${m}`);

async function call(base, method, url, body, token = TOKEN) {
  const res = await fetch(base + url, {
    method,
    headers: { 'Content-Type': 'application/json', ...(token ? { 'X-Editor-Token': token } : {}) },
    body: body === undefined ? undefined : JSON.stringify(body),
  });
  const json = await res.json().catch(() => ({}));
  return { status: res.status, json };
}

function startServer(port) {
  const child = spawn(process.execPath, [path.join(ROOT, 'src', 'server.js')], {
    cwd: ROOT,
    env: {
      ...process.env,
      PORT: String(port),
      HOST: '127.0.0.1',
      DB_PATH: dbPath,
      EDITOR_TOKEN: TOKEN,
      SCHEDULE_TICK_MS: '60000',
      SEED_DEMO: '0',
    },
    stdio: ['ignore', 'pipe', 'pipe'],
  });
  child.stdout.on('data', () => {});
  child.stderr.on('data', () => {});
  return child;
}

async function waitReady(base, timeoutMs = 15000) {
  const deadline = Date.now() + timeoutMs;
  while (Date.now() < deadline) {
    try {
      const r = await fetch(base + '/api/public/chronicle');
      if (r.ok) return;
    } catch {
      /* 尚未监听 */
    }
    await new Promise((r) => setTimeout(r, 100));
  }
  throw new Error(`服务 ${base} 在 ${timeoutMs}ms 内未就绪`);
}

const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

async function addEvent(base, title, claim) {
  const ev = await call(base, 'POST', '/api/editor/events', { title, description: '' });
  if (ev.status !== 201) throw new Error(`建事件失败：${JSON.stringify(ev.json)}`);
  const eventId = ev.json.event.id;
  const cl = await call(base, 'POST', `/api/editor/events/${eventId}/claims`, { ...claim });
  if (cl.status !== 201) throw new Error(`建主张失败：${JSON.stringify(cl.json)}`);
  return { eventId, claimId: cl.json.claim.id };
}

async function addRelation(base, earlierId, laterId, source) {
  const r = await call(base, 'POST', '/api/editor/relations', { earlierId, laterId, source });
  if (r.status !== 201) throw new Error(`建关系失败：${r.status} ${JSON.stringify(r.json)}`);
  return r.json.relation.id;
}

async function publish(base, selection, baseVersionNo, editor) {
  const r = await call(base, 'POST', '/api/editor/publish', { selection, baseVersionNo, editor });
  if (r.status !== 201) throw new Error(`发布失败：${r.status} ${JSON.stringify(r.json).slice(0, 400)}`);
  return r.json.versionNo;
}

async function state(base) {
  const r = await call(base, 'GET', '/api/editor/state');
  if (r.status !== 200) throw new Error('读取草稿状态失败');
  return r.json;
}

// 铺好两事件（动员会 1958-09-12 先于 通水会 1959-05）并发布第 1 版
async function seedV1(base) {
  const a = await addEvent(base, `动员会 ${TS}`, { precision: 'day', year: 1958, month: 9, day: 12, source: '李宝山口述' });
  const b = await addEvent(base, `通水会 ${TS}`, { precision: 'month', year: 1959, month: 5, source: '王秀兰口述' });
  await addRelation(base, a.eventId, b.eventId, '二人口述一致');
  const no = await publish(base, [a, b], 0, '甲');
  return { a, b, v1: no };
}

// 把草稿恢复成第 1 版的两事件状态（确认成功），返回新草稿里两事件的 selection
async function restoreToV1(base, versionNo = 1) {
  const pv = await call(base, 'GET', `/api/editor/restore-preview/${versionNo}`);
  if (pv.status !== 200) throw new Error(`预览失败：${pv.status}`);
  const done = await call(base, 'POST', '/api/editor/restore', {
    versionNo,
    draftRevisionNo: pv.json.draftRevisionNo,
    currentVersionNo: pv.json.currentVersionNo,
  });
  if (done.status !== 200) throw new Error(`恢复失败：${done.status} ${JSON.stringify(done.json)}`);
  return done.json.pendingSelection;
}

async function scenarioDraftConflict(baseA, baseB) {
  say('场景一：预览后乙改草稿 → 409 DRAFT_REVISION_CONFLICT，未展示改动保留；重新预览后成功');
  await seedV1(baseA);

  // 甲取得恢复预览
  const pv = await call(baseA, 'GET', '/api/editor/restore-preview/1');
  if (pv.status !== 200) throw new Error('甲预览失败');
  info(
    `甲的预览：目标第 ${pv.json.versionNo} 版（${pv.json.version.events.length} 事件）、` +
    `当前草稿 ${pv.json.currentDraft.counts.eventCount} 事件 / ${pv.json.currentDraft.counts.claimCount} 主张、` +
    `draftRevisionNo=${pv.json.draftRevisionNo}、currentVersionNo=${pv.json.currentVersionNo}`
  );
  const revPreview = pv.json.draftRevisionNo;

  // 乙在确认前新增事件 + 主张
  const extra = await addEvent(baseB, `乙在确认前加入的事件 ${TS}`, {
    precision: 'year',
    year: 1962,
    source: '张德福口述',
  });
  const stAfterB = await state(baseA);
  info(`乙已提交新事件 #${extra.eventId}，草稿修订号 ${revPreview} → ${stAfterB.draftRevisionNo}`);

  // 甲用预览返回的修订号确认 → 409，整次不改库
  const blocked = await call(baseA, 'POST', '/api/editor/restore', {
    versionNo: 1,
    draftRevisionNo: pv.json.draftRevisionNo,
    currentVersionNo: pv.json.currentVersionNo,
  });
  if (blocked.status !== 409 || blocked.json.code !== 'DRAFT_REVISION_CONFLICT') {
    throw new Error(`期望 409 DRAFT_REVISION_CONFLICT，实际 ${blocked.status} ${JSON.stringify(blocked.json)}`);
  }
  info(`  409 ${blocked.json.code}：${blocked.json.message}`);
  const stBlocked = await state(baseA);
  if (!stBlocked.events.some((e) => e.id === extra.eventId)) throw new Error('乙的新事件不应被删除');
  if (stBlocked.draftRevisionNo !== blocked.json.currentRevisionNo) throw new Error('冲突时草稿修订号不应变化');
  ok(`乙的新事件 #${extra.eventId} 保留，草稿修订号仍为 ${stBlocked.draftRevisionNo}，整次未改库`);

  // 甲重新预览（新事件出现在「将被替换的草稿」中）后确认 → 成功
  const pv2 = await call(baseA, 'GET', '/api/editor/restore-preview/1');
  if (!pv2.json.currentDraft.events.some((e) => e.id === extra.eventId)) {
    throw new Error('重新预览应展示乙加入的事件');
  }
  const done = await call(baseA, 'POST', '/api/editor/restore', {
    versionNo: 1,
    draftRevisionNo: pv2.json.draftRevisionNo,
    currentVersionNo: pv2.json.currentVersionNo,
  });
  if (done.status !== 200) throw new Error(`重新预览后确认应成功：${done.status} ${JSON.stringify(done.json)}`);
  const finalSt = await state(baseA);
  if (finalSt.events.some((e) => e.id === extra.eventId)) throw new Error('确认成功后新事件应已被替换');
  if (finalSt.events.length !== 2 || finalSt.events.some((e) => e.claims.length !== 1)) {
    throw new Error('恢复后草稿应只剩第 1 版两事件、每事件一条被选主张');
  }
  ok(`重新预览后确认成功：草稿只剩第 1 版两事件（每事件仅一条选定主张），修订号 ${done.json.revisionNo}`);
}

async function scenarioVersionConflict(baseA, baseB) {
  say('场景二：预览后乙发布新版本 → 409 VERSION_CONFLICT，草稿与快照均不变');
  // 从第 1 版恢复出干净草稿，再加一个事件由乙发布第 2 版
  const selV1 = await restoreToV1(baseA);
  const third = await addEvent(baseA, `抽水机到站 ${TS}`, { precision: 'year', year: 1962, source: '张德福口述' });
  await addRelation(baseA, selV1[1].eventId, third.eventId, '先有水渠后有抽水机');
  await publish(baseA, [...selV1, third], 1, '乙');
  // 再恢复回第 1 版两事件，制造「最新=2、草稿=第1版内容」的预览局面
  await restoreToV1(baseA);

  const pv = await call(baseA, 'GET', '/api/editor/restore-preview/1');
  if (pv.json.currentVersionNo !== 2) throw new Error('准备状态有误：最新应为第 2 版');
  info(`甲的预览：currentVersionNo=${pv.json.currentVersionNo}，draftRevisionNo=${pv.json.draftRevisionNo}`);

  // 乙在确认前发布第 3 版（内容可与第 2 版相同，只是推进版本号）
  const cur = await state(baseB);
  const selectionNow = cur.events.map((e) => ({ eventId: e.id, claimId: e.claims[e.claims.length - 1].id }));
  // 草稿当前是第 1 版两事件，直接以基准 2 发布即可生成第 3 版
  await publish(baseB, selectionNow, 2, '乙');
  info('乙已发布第 3 版');

  const blocked = await call(baseA, 'POST', '/api/editor/restore', {
    versionNo: 1,
    draftRevisionNo: pv.json.draftRevisionNo,
    currentVersionNo: pv.json.currentVersionNo,
  });
  if (blocked.status !== 409 || blocked.json.code !== 'VERSION_CONFLICT') {
    throw new Error(`期望 409 VERSION_CONFLICT，实际 ${blocked.status} ${JSON.stringify(blocked.json)}`);
  }
  info(`  409 ${blocked.json.code}：${blocked.json.message}`);
  if (blocked.json.currentVersionNo !== 3 || blocked.json.expectedVersionNo !== 2) {
    throw new Error('冲突体的版本号字段不符');
  }
  const st = await state(baseA);
  if (st.events.length !== 2 || st.currentVersionNo !== 3) throw new Error('冲突后草稿 / 最新版本不应被改动');
  ok('草稿仍是两事件、最新版本号为 3；三个历史版本快照原样保留');

  // 重新预览后可正常恢复（撤回版同样可恢复，见 test/restore.test.js；此处演示普通重试）
  const pv2 = await call(baseA, 'GET', '/api/editor/restore-preview/1');
  const done = await call(baseA, 'POST', '/api/editor/restore', {
    versionNo: 1,
    draftRevisionNo: pv2.json.draftRevisionNo,
    currentVersionNo: pv2.json.currentVersionNo,
  });
  if (done.status !== 200) throw new Error(`重新预览后确认应成功：${done.status}`);
  ok(`重新预览后确认成功，访客年表仍指向第 ${done.json.currentVersionNo} 版（恢复不改历史快照）`);
}

// 场景三：在预览 HTTP 请求进行中让乙并发写草稿，阶梯延迟多轮撞窗口。
// 断言预览内容与修订号同源、预览不写库，以及随之确认的结果与展示一致。
async function scenarioPreviewConsistency(baseA, baseB) {
  say('场景三：预览读取期间乙并发写草稿 → 预览内容与修订号同源，绝不「旧草稿 + 新修订号」');
  await restoreToV1(baseA);
  const before = await state(baseA);
  const revBefore = before.draftRevisionNo;
  const eventsBefore = before.events.length;

  const delays = [0, 2, 5, 10, 20, 40, 80];
  let demonstrated = null;

  for (let attempt = 0; attempt < delays.length && !demonstrated; attempt++) {
    const s0 = await state(baseA);
    const reqPreview = call(baseA, 'GET', '/api/editor/restore-preview/1');
    await sleep(delays[attempt]);
    // 乙并发写入：新增事件 + 主张（2 次修订）
    const reqWrite = addEvent(baseB, `乙在预览期间写入 ${TS}-${attempt}`, {
      precision: 'year',
      year: 2000 + attempt,
      source: `并发出处 ${attempt}`,
    });
    const [pv, extra] = await Promise.all([reqPreview, reqWrite]);
    if (pv.status !== 200) throw new Error('预览不应失败');
    const revShown = pv.json.draftRevisionNo;
    const eventsShown = pv.json.currentDraft.counts.eventCount;
    const claimsShown = pv.json.currentDraft.counts.claimCount;
    const s1 = await state(baseA);

    // 同源断言（无论快照在提交前还是提交后都必须成立）：
    //   旧快照：2 事件 / 2 主张 + 旧修订号；新快照：3 事件 / 3 主张 + 新修订号。
    const oldView = eventsShown === 2 && claimsShown === 2 && revShown === s0.draftRevisionNo;
    const newView = eventsShown === 3 && claimsShown === 3 && revShown === s1.draftRevisionNo;
    if (!oldView && !newView) {
      throw new Error(
        `预览错位：展示 ${eventsShown} 事件 / ${claimsShown} 主张，却返回修订号 ${revShown}（预览前 ${s0.draftRevisionNo} / 预览后 ${s1.draftRevisionNo}）`
      );
    }

    // 预览不写库：修订号只应来自乙的两次写入（事件 + 主张），绝无预览自身的写入痕迹
    if (s1.draftRevisionNo !== s0.draftRevisionNo + 2) {
      throw new Error(`预览疑似写库：修订号增量 ${s1.draftRevisionNo - s0.draftRevisionNo}（应为 2）`);
    }

    // 用该预览直接确认：结果必须与展示一致
    const confirm = await call(baseA, 'POST', '/api/editor/restore', {
      versionNo: 1,
      draftRevisionNo: revShown,
      currentVersionNo: pv.json.currentVersionNo,
    });

    if (oldView) {
      // 乙的提交落在预览快照之后：确认必须 409，未展示的新事件保留
      if (confirm.status !== 409 || confirm.json.code !== 'DRAFT_REVISION_CONFLICT') {
        throw new Error(`旧快照确认应 409，实际 ${confirm.status} ${JSON.stringify(confirm.json)}`);
      }
      const s2 = await state(baseA);
      if (!s2.events.some((e) => e.id === extra.eventId)) throw new Error('未展示的新事件不得被删除');
      info(`第 ${attempt + 1} 轮（延迟 ${delays[attempt]}ms）：预览固定在乙提交前 → 确认 409，事件 #${extra.eventId} 保留`);
      demonstrated = 'old-view';
    } else {
      // 预览快照已包含乙的提交（新事件就在预览响应里）：确认成功不构成误删
      if (confirm.status !== 200) throw new Error(`新快照确认应成功：${confirm.status} ${JSON.stringify(confirm.json)}`);
      const s2 = await state(baseA);
      if (s2.events.length !== 2) throw new Error('确认成功后草稿应只剩第 1 版两事件');
      info(`第 ${attempt + 1} 轮（延迟 ${delays[attempt]}ms）：预览已展示乙的新事件 → 确认成功（被替换内容均曾展示）`);
      demonstrated = 'new-view';
      // 恢复回两事件后下一轮无法再制造「2 → 3」，到此即可结束
    }
  }

  if (!demonstrated) throw new Error('多轮都未与乙的写入交错，可重试本脚本');
  ok(`预览各字段对应同一数据库状态，且确认结果与展示一致（本轮命中：${demonstrated}）`);

  // 收尾：恢复成两事件干净状态（若上一轮停在 409）
  const sEnd = await state(baseA);
  if (sEnd.events.length !== 2 || sEnd.draftRevisionNo === revBefore) {
    await restoreToV1(baseA);
  }
}

async function scenarioNotFound(baseA) {
  say('附加：目标版本不存在 → 预览与确认均 404（API 字段保持兼容）');
  const pv = await call(baseA, 'GET', '/api/editor/restore-preview/999');
  if (pv.status !== 404) throw new Error(`不存在的版本预览应 404，实际 ${pv.status}`);
  const st = await state(baseA);
  const cf = await call(baseA, 'POST', '/api/editor/restore', {
    versionNo: 999,
    draftRevisionNo: st.draftRevisionNo,
    currentVersionNo: st.currentVersionNo,
  });
  if (cf.status !== 404) throw new Error(`不存在的版本确认应 404，实际 ${cf.status}`);
  ok('404 语义不变，草稿未改动');
}

async function main() {
  say(`启动两个共享同一数据库的服务进程：A=:${PORT_A}  B=:${PORT_B}\n     库文件 ${dbPath}`);
  const serverA = startServer(PORT_A);
  const serverB = startServer(PORT_B);
  const baseA = `http://127.0.0.1:${PORT_A}`;
  const baseB = `http://127.0.0.1:${PORT_B}`;
  let exitCode = 0;
  try {
    await waitReady(baseA);
    await waitReady(baseB);
    ok('两个进程均已就绪（共用 DB_PATH，WAL 模式）');

    const denied = await fetch(baseA + '/api/editor/state', { headers: { 'X-Editor-Token': 'wrong' } });
    if (denied.status !== 401) throw new Error('编辑者口令校验异常');
    ok('编辑者 HTTP API 保持原有口令校验（错误口令 401）');

    await scenarioDraftConflict(baseA, baseB);
    await scenarioVersionConflict(baseA, baseB);
    await scenarioPreviewConsistency(baseA, baseB);
    await scenarioNotFound(baseA);

    say('全部裁决符合预期：预览同源且只读；冲突整次不改库；成功仅复制选定主张');
    if (KEEP) info(`KEEP=1：服务仍在运行（A=${baseA} B=${baseB}），临时库保留在 ${tmpDir}`);
  } catch (err) {
    console.error('\x1b[31m示例失败：\x1b[0m', err.message);
    exitCode = 1;
  } finally {
    if (!KEEP) {
      serverA.kill('SIGTERM');
      serverB.kill('SIGTERM');
      await new Promise((r) => setTimeout(r, 300));
      fs.rmSync(tmpDir, { recursive: true, force: true });
    }
  }
  process.exit(exitCode);
}

main();
