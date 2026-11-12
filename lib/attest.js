/**
 * 节点密码学身份 + 配对存证（ATTESTATION）
 *
 * 目的：让「节点促成了多少局跨节点配对」这件事不再由枢纽单方面说了算。
 *
 * 三个机制：
 *   1. 身份：每个节点首次启动生成 Ed25519 密钥对，持久化到 data/node-key.json。
 *      nodeId = 公钥 DER 的 sha256 前 32 位十六进制，即公钥指纹（无法伪造）。
 *   2. 双向签名：一次跨节点配对，参与的每个节点都用自己私钥对同一条消息签名。
 *      任何人都能验签，但伪造需要某个节点的私钥。
 *   3. Merkle 存证：当日所有存证记录按顺序算出一棵 Merkle 树，取根哈希公开。
 *      单条记录可凭 Merkle proof 证明「我确实在当天的结果里」，而无需公开全部记录。
 *
 * 隐私边界：存证记录里**只有** 记录ID / 游戏ID / 时间戳 / 两个节点ID / 签名，
 *          不含任何玩家信息（昵称、好友ID、平台、时段一律不入记录）。
 *
 * 本模块不依赖任何第三方库，只用 Node 内置 crypto。
 * 未设 SIGNING=1 时整个机制关闭，服务端行为与之前完全一致。
 */
'use strict';

const fs = require('fs');
const path = require('path');
const crypto = require('crypto');

/** 是否启用（环境变量 SIGNING=1/on/true） */
const ENABLED = /^(1|on|true|yes)$/i.test(String(process.env.SIGNING || ''));

/** 参与方标识：'self' 代表本进程自己，其余为远端节点 ws 对象 */
const SELF = 'self';

let identity = null; // { nodeId, pubKeyB64, privateKey, publicKey }

/* ------------------------------------------------------------------ */
/* 一、身份                                                            */
/* ------------------------------------------------------------------ */

function nodeIdFromPubDer(der) {
  return crypto.createHash('sha256').update(der).digest('hex').slice(0, 32);
}

/**
 * 载入或生成节点身份。失败（无写权限等）时返回 null，调用方应降级为不签名。
 * @param {string} dataDir 存放密钥的目录
 */
function init(dataDir) {
  if (!ENABLED) return null;
  try {
    fs.mkdirSync(dataDir, { recursive: true });
    const keyFile = path.join(dataDir, 'node-key.json');

    let privateKey, publicKey;
    if (fs.existsSync(keyFile)) {
      const saved = JSON.parse(fs.readFileSync(keyFile, 'utf8'));
      privateKey = crypto.createPrivateKey(saved.privatePem);
      publicKey = crypto.createPublicKey(saved.publicPem);
    } else {
      const pair = crypto.generateKeyPairSync('ed25519');
      privateKey = pair.privateKey;
      publicKey = pair.publicKey;
      fs.writeFileSync(keyFile, JSON.stringify({
        privatePem: privateKey.export({ type: 'pkcs8', format: 'pem' }),
        publicPem: publicKey.export({ type: 'spki', format: 'pem' }),
        createdAt: new Date().toISOString(),
        note: '这是本节点的身份私钥，请勿公开或提交到代码仓库。删除后节点身份将不可恢复。',
      }, null, 2), { mode: 0o600 });
    }

    const der = publicKey.export({ type: 'spki', format: 'der' });
    identity = {
      nodeId: nodeIdFromPubDer(der),
      pubKeyB64: der.toString('base64'),
      privateKey,
      publicKey,
    };
    return identity;
  } catch (err) {
    console.warn('[存证] 初始化失败，已降级为不签名模式:', err.message);
    return null;
  }
}

function getIdentity() { return identity; }
function isEnabled() { return ENABLED && !!identity; }

/* ------------------------------------------------------------------ */
/* 二、签名                                                            */
/* ------------------------------------------------------------------ */

/**
 * 待签名消息的规范形式。两端必须算出完全相同的字符串。
 * 参与节点 ID 排序后拼接，避免「谁在前谁在后」造成签名不一致。
 */
function message(rec) {
  const ids = rec.nodes.slice().sort();
  return `ATTEST_V2|${rec.id}|${rec.game}|${rec.ts}|${ids.join(',')}`;
}

function sign(rec) {
  if (!identity) throw new Error('未初始化身份');
  return crypto.sign(null, Buffer.from(message(rec)), identity.privateKey).toString('base64');
}

/** @param {string} pubKeyB64 对端公钥（SPKI DER 的 base64） */
function verify(pubKeyB64, rec, sig) {
  try {
    const key = crypto.createPublicKey({
      key: Buffer.from(pubKeyB64, 'base64'),
      format: 'der',
      type: 'spki',
    });
    return crypto.verify(null, Buffer.from(message(rec)), key, Buffer.from(sig, 'base64'));
  } catch {
    return false;
  }
}

/** 校验一条记录的签名是否齐全且全部有效（peers: nodeId -> pubKeyB64） */
function verifyRecord(rec, peers) {
  if (!rec || !Array.isArray(rec.nodes) || rec.nodes.length < 2 || !rec.sigs) return false;
  if (new Set(rec.nodes).size !== rec.nodes.length) return false; // 参与方不允许重复
  for (const nid of rec.nodes) {
    const sig = rec.sigs[nid];
    const pk = peers[nid];
    if (!sig || !pk) return false;
    if (!verify(pk, rec, sig)) return false;
  }
  return true;
}

/* ------------------------------------------------------------------ */
/* 三、Merkle                                                          */
/* ------------------------------------------------------------------ */

function sha256hex(buf) {
  return crypto.createHash('sha256').update(buf).digest('hex');
}

function hashPair(h1, h2) {
  return sha256hex(Buffer.concat([Buffer.from(h1, 'hex'), Buffer.from(h2, 'hex')]));
}

/**
 * 记录 → 叶子哈希。字段固定顺序，签名按 nodeId 排序后拼接，
 * 保证同一条记录在任何实现下算出的叶子都相同。
 */
function leafOf(rec) {
  const ids = rec.nodes.slice().sort();
  const sigs = Object.keys(rec.sigs || {}).sort()
    .map(k => `${k}:${rec.sigs[k]}`).join(',');
  return sha256hex(Buffer.from(`RECORD_V2|${rec.id}|${rec.game}|${rec.ts}|${ids.join(',')}|${sigs}`));
}

/** 空树也有确定根，避免「当天没有存证」和「算不出来」混淆 */
const EMPTY_ROOT = sha256hex(Buffer.from('EMPTY_ATTEST_TREE'));

/** 构建 Merkle 根。奇数个时复制末项（Bitcoin 风格），验证端无需额外标记。 */
function buildRoot(leaves) {
  if (!leaves.length) return EMPTY_ROOT;
  let level = leaves.slice();
  while (level.length > 1) {
    if (level.length % 2) level.push(level[level.length - 1]);
    const next = [];
    for (let i = 0; i < level.length; i += 2) next.push(hashPair(level[i], level[i + 1]));
    level = next;
  }
  return level[0];
}

/**
 * 生成某片叶子的 Merkle proof。
 * @returns {{leaf:string, index:number, path:{hash:string,pos:'left'|'right'}[], root:string}}
 */
function buildProof(leaves, index) {
  if (index < 0 || index >= leaves.length) return null;
  let level = leaves.slice();
  let idx = index;
  const proof = [];
  while (level.length > 1) {
    if (level.length % 2) level.push(level[level.length - 1]);
    const siblingIdx = idx % 2 === 0 ? idx + 1 : idx - 1;
    proof.push({ hash: level[siblingIdx], pos: idx % 2 === 0 ? 'right' : 'left' });
    const next = [];
    for (let i = 0; i < level.length; i += 2) next.push(hashPair(level[i], level[i + 1]));
    level = next;
    idx = Math.floor(idx / 2);
  }
  return { leaf: leaves[index], index, path: proof, root: level[0] };
}

/** 验证 proof：不信任调用方之外的任何信息，纯重算 */
function verifyProof(leaf, proof, root) {
  let acc = leaf;
  for (const step of proof) {
    acc = step.pos === 'left' ? hashPair(step.hash, acc) : hashPair(acc, step.hash);
  }
  return acc === root;
}

/* ------------------------------------------------------------------ */
/* 四、本地存证落盘（节点侧）                                           */
/* ------------------------------------------------------------------ */

const LOCAL_LOG_MAX_BYTES = 2 * 1024 * 1024;
const LOCAL_LOG_KEEP_LINES = 1000;

/** 节点把收到的 attest_ok 追加到本地，枢纽消失也能自证 */
function appendLocalRecord(dataDir, rec) {
  if (!ENABLED) return;
  try {
    fs.mkdirSync(dataDir, { recursive: true });
    const file = path.join(dataDir, 'attestations.jsonl');
    fs.appendFileSync(file, JSON.stringify(rec) + '\n');
    // 超限时滚动，只保留最近若干条
    const st = fs.statSync(file);
    if (st.size > LOCAL_LOG_MAX_BYTES) {
      const lines = fs.readFileSync(file, 'utf8').trim().split('\n');
      fs.writeFileSync(file, lines.slice(-LOCAL_LOG_KEEP_LINES).join('\n') + '\n');
    }
  } catch (err) {
    console.warn('[存证] 本地记录写入失败:', err.message);
  }
}

/** 读回本地存证（给 /api/attest/local 用） */
function readLocalRecords(dataDir, limit = 200) {
  try {
    const file = path.join(dataDir, 'attestations.jsonl');
    if (!fs.existsSync(file)) return [];
    const lines = fs.readFileSync(file, 'utf8').trim().split('\n').filter(Boolean);
    return lines.slice(-limit).map(l => { try { return JSON.parse(l); } catch { return null; } }).filter(Boolean);
  } catch { return []; }
}

module.exports = {
  SELF,
  init,
  getIdentity,
  isEnabled,
  message,
  sign,
  verify,
  verifyRecord,
  leafOf,
  buildRoot,
  buildProof,
  verifyProof,
  emptyRoot: () => EMPTY_ROOT,
  appendLocalRecord,
  readLocalRecords,
  nodeIdFromPubDer,
};
