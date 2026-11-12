#!/usr/bin/env node
/**
 * 存证独立验证工具
 *
 * 用途：任何人拿到一条存证证明，都能不信任服务端地复核它。
 * 验证三件事：
 *   1. 两个节点的签名都是真的（用它们自己的公钥验）
 *   2. 证明里的记录内容与叶子哈希一致（记录没被改过）
 *   3. Merkle 路径能推出当天的根（这条记录确实在当天的公开结果里）
 *
 * 用法：
 *   # 从枢纽拉一条证明并验证
 *   node scripts/verify-attestation.js --url http://localhost:3000 --id 3f2c...
 *
 *   # 验证本地保存的证明 JSON 文件
 *   node scripts/verify-attestation.js proof.json
 *
 *   # 只核对某天的根（把别人存的根和枢纽的比）
 *   node scripts/verify-attestation.js --url http://localhost:3000 --roots
 */
'use strict';

const fs = require('fs');
const path = require('path');
const attest = require(path.join(__dirname, '..', 'lib', 'attest.js'));

function usage() {
  console.log(`用法:
  node scripts/verify-attestation.js <proof.json>                       验证本地证明文件
  node scripts/verify-attestation.js --url <hub> --id <attestId>        从枢纽拉取并验证
  node scripts/verify-attestation.js --url <hub> --roots                列出某天/全部根哈希
  node scripts/verify-attestation.js --selftest                         自检（构造假数据验证算法）`);
  process.exit(1);
}

function parseArgs(argv) {
  const out = { _: [] };
  for (let i = 0; i < argv.length; i++) {
    const a = argv[i];
    if (a === '--url') out.url = argv[++i];
    else if (a === '--id') out.id = argv[++i];
    else if (a === '--roots') out.roots = true;
    else if (a === '--selftest') out.selftest = true;
    else out._.push(a);
  }
  return out;
}

const mark = ok => (ok ? '\x1b[32m✓\x1b[0m' : '\x1b[31m✗\x1b[0m');

function verifyProofObject(data) {
  const { record, peers, leaf, path: merklePath, root } = data;
  let ok = true;

  if (!record || !Array.isArray(record.nodes) || record.nodes.length < 2 || !record.sigs) {
    console.log(`${mark(false)} 证明缺少 record.nodes（参与节点）或签名`);
    return false;
  }
  if (!peers || record.nodes.some(nid => !peers[nid])) {
    console.log(`${mark(false)} 证明缺少参与方公钥（peers），无法验签`);
    return false;
  }

  // 1. 每个参与节点的签名都要有效
  for (const nid of record.nodes) {
    const good = attest.verify(peers[nid], record, record.sigs[nid]);
    console.log(`${mark(good)} 节点 ${nid.slice(0, 12)}… 的签名${good ? '有效' : '无效'}`);
    ok = ok && good;
  }

  // 2. 叶子哈希
  const recomputed = attest.leafOf(record);
  const leafOk = recomputed === leaf;
  console.log(`${mark(leafOk)} 记录哈希与证明一致 ${leafOk ? '' : `(算出 ${recomputed.slice(0, 16)}… 证明给 ${String(leaf).slice(0, 16)}…)`}`);
  ok = ok && leafOk;

  // 3. Merkle 路径
  const merkleOk = attest.verifyProof(leaf, merklePath || [], root);
  console.log(`${mark(merkleOk)} Merkle 路径可推出根 ${String(root).slice(0, 16)}…`);
  ok = ok && merkleOk;

  if (ok) {
    console.log(`\n\x1b[32m存证有效\x1b[0m — 游戏 ${record.game}，时间 ${new Date(record.ts).toLocaleString('zh-CN')}`);
    console.log(`${record.nodes.length} 个节点共同签署：`);
    for (const nid of record.nodes) console.log(`  ${nid}`);
  } else {
    console.log('\n\x1b[31m存证无效\x1b[0m — 记录被改动、签名伪造，或不在当日根里');
  }
  return ok;
}

async function fetchJson(url) {
  const r = await fetch(url);
  if (!r.ok) throw new Error(`HTTP ${r.status}`);
  return r.json();
}

(async () => {
  const args = parseArgs(process.argv.slice(2));

  if (args.selftest) return selftest();

  if (args.roots) {
    if (!args.url) usage();
    const data = await fetchJson(args.url.replace(/\/$/, '') + '/api/attest/roots');
    if (!data.days || !data.days.length) return console.log('该枢纽还没有任何已完成存证');
    console.log(`共 ${data.total} 条存证\n`);
    for (const d of data.days) {
      console.log(`${d.day}  ${String(d.count).padStart(5)} 条  root=${d.root}`);
    }
    return;
  }

  if (args.url && args.id) {
    const data = await fetchJson(`${args.url.replace(/\/$/, '')}/api/attest/proof?id=${encodeURIComponent(args.id)}`);
    process.exit(verifyProofObject(data) ? 0 : 1);
  }

  if (args._.length) {
    const data = JSON.parse(fs.readFileSync(args._[0], 'utf8'));
    process.exit(verifyProofObject(data) ? 0 : 1);
  }

  usage();
})().catch(err => {
  console.error('验证失败:', err.message);
  process.exit(1);
});

/** 自检：不依赖任何服务，验证签名与 Merkle 算法本身没写错 */
function selftest() {
  const crypto = require('crypto');
  const mkId = () => {
    const { publicKey, privateKey } = crypto.generateKeyPairSync('ed25519');
    const der = publicKey.export({ type: 'spki', format: 'der' });
    return { nodeId: attest.nodeIdFromPubDer(der), pubKey: der.toString('base64'), privateKey };
  };
  const A = mkId(), B = mkId(), C = mkId();
  const rec = { id: crypto.randomUUID(), game: 'ittakestwo', ts: Date.now(), nodes: [A.nodeId, B.nodeId, C.nodeId], sigs: {} };
  for (const n of [A, B, C]) {
    rec.sigs[n.nodeId] = crypto.sign(null, Buffer.from(attest.message(rec)), n.privateKey).toString('base64');
  }

  const peers = { [A.nodeId]: A.pubKey, [B.nodeId]: B.pubKey, [C.nodeId]: C.pubKey };
  console.log(`${mark(attest.verifyRecord(rec, peers))} 三方多签互验通过`);

  // 少一方签名就无效
  const missing = JSON.parse(JSON.stringify(rec));
  delete missing.sigs[C.nodeId];
  console.log(`${mark(!attest.verifyRecord(missing, peers))} 缺一方签名被检出`);

  // 篡改检测
  const tampered = JSON.parse(JSON.stringify(rec));
  tampered.game = 'palworld';
  console.log(`${mark(!attest.verifyRecord(tampered, peers))} 篡改游戏名后被检出`);

  // 参与方重复也要拒绝
  const dup = JSON.parse(JSON.stringify(rec));
  dup.nodes = [A.nodeId, A.nodeId];
  console.log(`${mark(!attest.verifyRecord(dup, peers))} 参与方重复被拒绝`);

  // Merkle + proof
  const leaves = Array.from({ length: 7 }, () => crypto.randomBytes(32).toString('hex'));
  const root = attest.buildRoot(leaves);
  let allOk = true;
  for (let i = 0; i < leaves.length; i++) {
    const p = attest.buildProof(leaves, i);
    if (!attest.verifyProof(p.leaf, p.path, root)) { allOk = false; console.log(`${mark(false)} 第 ${i} 条 proof 验证失败`); }
  }
  console.log(`${mark(allOk)} 7 条叶子的 Merkle proof 全部可验证（含奇数补位）`);

  const bad = attest.buildProof(leaves, 3);
  bad.path[0].hash = crypto.randomBytes(32).toString('hex');
  console.log(`${mark(!attest.verifyProof(bad.leaf, bad.path, root))} 篡改 proof 后被检出`);

  const allPass = allOk;
  console.log(allPass ? '\n自检通过' : '\n自检失败');
  process.exit(allPass ? 0 : 1);
}
