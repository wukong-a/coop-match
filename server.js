/**
 * 游戏搭子随机匹配工具 - 服务端（多游戏 + 联邦 + 防滥用）
 *
 * 队列键 = 游戏 × 平台，凑满该游戏的开局人数即配对。
 * 每个实例既是独立的匹配服务，也可以作为节点接入联邦网络：
 *   - 不设 HUB_URL：独立运行
 *   - 设 HUB_URL：本地凑不齐人时，玩家进入该枢纽的跨节点队列
 * 任何实例都可以被别的实例指为枢纽，网络中没有特权服务器。
 *
 * 环境变量：
 *   PORT       监听端口（默认 3000）
 *   NODE_NAME  节点显示名（默认随机生成）
 *   HUB_URL    上游枢纽地址（可选）
 */
const express = require('express');
const http = require('http');
const path = require('path');
const fs = require('fs');
const crypto = require('crypto');
const { WebSocketServer, WebSocket } = require('ws');
const attest = require('./lib/attest');

const PORT = process.env.PORT || 3000;
const NODE_NAME = process.env.NODE_NAME || ('节点-' + crypto.randomBytes(2).toString('hex'));
const HUB_URL = process.env.HUB_URL || '';
const RELAY_TOKEN = process.env.RELAY_TOKEN || ''; // 枢纽可选：要求节点携带相同令牌才能接入中继
const BIND = process.env.BIND || '0.0.0.0';        // 部署在反代后时建议 BIND=127.0.0.1，不直接对外
const DATA_DIR = process.env.DATA_DIR || path.join(__dirname, 'data');

// 节点密码学身份（SIGNING=1 才启用；失败则自动降级，不影响匹配）
const IDENT = attest.init(DATA_DIR);
const SIGNING = !!IDENT;

/* ================= 游戏库 ================= */
const ALL_PLATFORMS = ['steam', 'ea', 'epic', 'ps', 'xbox', 'switch'];
const BASE_GAMES = [
  { id: 'ittakestwo',   name: '双人成行',    size: 2, platforms: ['steam', 'ea', 'ps', 'xbox', 'switch'] },
  { id: 'splitfiction', name: '双影奇境',    size: 2, platforms: ['steam', 'ea', 'ps', 'xbox', 'switch'] },
  { id: 'awayout',      name: '逃出生天',    size: 2, platforms: ['steam', 'ea', 'ps', 'xbox'] },
  { id: 'portal2',      name: '传送门2',     size: 2, platforms: ['steam'] },
  { id: 'overcooked2',  name: '胡闹厨房2',   size: 4, platforms: ['steam', 'epic', 'ps', 'xbox', 'switch'] },
  { id: 'drg',          name: '深岩银河',    size: 4, platforms: ['steam', 'epic', 'ps', 'xbox'] },
  { id: 'phasmophobia', name: '恐鬼症',      size: 4, platforms: ['steam', 'epic', 'ps', 'xbox'] },
  { id: 'sonsoftforest',name: '森林之子',    size: 4, platforms: ['steam'] },
  { id: 'dst',          name: '饥荒联机版',  size: 4, platforms: ['steam'] },
  { id: 'valheim',      name: '英灵神殿',    size: 4, platforms: ['steam', 'xbox'] },
  { id: 'palworld',     name: '幻兽帕鲁',    size: 4, platforms: ['steam', 'xbox'] },
  { id: 'bg3',          name: '博德之门3',   size: 4, platforms: ['steam', 'ps', 'xbox'] },
  { id: 'l4d2',         name: '求生之路2',   size: 4, platforms: ['steam'] },
];
/* ---- 提名收录：节点主人把「已达标」的候选游戏加进匹配库，落盘在 data/games-extra.json，重启不丢 ---- */
const EXTRA_GAMES_FILE = path.join(DATA_DIR, 'games-extra.json');
function loadExtraGames() {
  try {
    const arr = JSON.parse(fs.readFileSync(EXTRA_GAMES_FILE, 'utf8'));
    if (!Array.isArray(arr)) return [];
    return arr.filter(g => g && typeof g.id === 'string' && typeof g.name === 'string'
      && Number.isInteger(g.size) && g.size >= 2 && g.size <= 8
      && Array.isArray(g.platforms) && g.platforms.length > 0
      && g.platforms.every(p => ALL_PLATFORMS.includes(p)));
  } catch { return []; }
}
const GAMES = [...BASE_GAMES, ...loadExtraGames()];
const GAME_MAP = new Map(GAMES.map(g => [g.id, g]));
const qkey = (game, platform) => game + '|' + platform;

/* ================= 候选游戏池（提名制） =================
 * 为什么不做「玩家自己输入游戏名」：
 *   1) 那是唯一能让任意文字进展示位的口子 —— 广告/辱骂会从这里灌进来
 *   2) 同一个游戏多种写法（"双人成行"/"双人成形"/"it takes two "）会把匹配池切成碎片
 *   3) 自定义游戏别的节点不认识，联邦跨节点配对必然配不上，体验更差
 * 所以改成：固定候选池 → 玩家投票 → 达标后由节点主人一键收录（收录后进入本地匹配库）
 */
const CANDIDATES = [
  { id: 'terraria',  name: '泰拉瑞亚',      size: 4, platforms: ['steam', 'switch', 'ps', 'xbox'] },
  { id: 'stardew',   name: '星露谷物语',    size: 4, platforms: ['steam', 'switch', 'ps', 'xbox'] },
  { id: 'minecraft', name: '我的世界',      size: 4, platforms: ['steam'] },
  { id: 'naraka',    name: '永劫无间',      size: 3, platforms: ['steam', 'ps', 'xbox'] },
  { id: 'valorant',  name: '无畏契约',      size: 5, platforms: ['steam', 'ps', 'xbox'] },
  { id: 'cs2',       name: 'CS2',           size: 5, platforms: ['steam'] },
  { id: 'lol',       name: '英雄联盟',      size: 5, platforms: ['steam'] },
  { id: 'r6',        name: '彩虹六号',      size: 5, platforms: ['steam', 'ps', 'xbox'] },
  { id: 'destiny2',  name: '命运2',         size: 3, platforms: ['steam', 'ps', 'xbox'] },
  { id: 'payday2',   name: '收获日2',       size: 4, platforms: ['steam'] },
  { id: 'forzah5',   name: '极限竞速地平线5', size: 4, platforms: ['steam', 'xbox'] },
  { id: 'gtfo',      name: 'GTFO',          size: 4, platforms: ['steam'] },
];
const NOMINATE_VOTES = 3;   // 达标票数
const NOMINATE_IPS = 2;     // 至少来自几个不同 IP（一人换 token 刷不动）
const NOMINATE_PER_IP_DAY = 20;
const nominateVotes = new Map(); // candidateId -> { tokens:Set, ips:Set }
const nominateIpLog = new Map(); // ip -> [时间戳]
const ADMIN_TOKEN = process.env.ADMIN_TOKEN || ''; // 配置后节点主人可在线收录游戏

/* ================= 防滥用状态 ================= */
const REPORTS_TO_BAN = 3;
const BAN_MS = 24 * 60 * 60 * 1000;
const tokenStrikes = new Map();
const bannedUntil = new Map();
const joinLog = new Map();       // ip -> [时间戳]
const tokenQueuing = new Map();  // token -> ws
const hubStrikes = new Map();    // 枢纽: token -> 举报次数

function isBanned(token) {
  const until = bannedUntil.get(token);
  if (!until) return false;
  if (Date.now() > until) { bannedUntil.delete(token); return false; }
  return true;
}
function strikeToken(token) {
  const n = (tokenStrikes.get(token) || 0) + 1;
  tokenStrikes.set(token, n);
  if (n >= REPORTS_TO_BAN) bannedUntil.set(token, Date.now() + BAN_MS);
  return n;
}

/* ---- 防举报武器化 ----
 * token 是前端生成的匿名身份，清缓存 = 换新身份，所以"3 个不同 token"本身不构成可信证据。
 * 四道闸：①身份必须早于被举报内容存在（挡临时小号）②单 IP 举报频率上限
 *        ③同一 IP 只计一次（挡住"一人换三个 token"）④下架要求 ≥2 个不同 IP 同时举报
 */
const tokenFirstSeen = new Map(); // token -> 首次出现时间
const reportIpLog = new Map();     // ip -> [举报时间戳]
const REPORT_PER_IP_DAY = 10;      // 单 IP 每天最多举报次数
const REPORT_MIN_IPS = 2;          // 下架至少需要几个不同 IP

function touchToken(token) {
  if (!token) return;
  if (tokenFirstSeen.has(token)) return;
  tokenFirstSeen.set(token, Date.now());
  // 简单防膨胀：超过 2 万条时淘汰最早的 5000 条
  if (tokenFirstSeen.size > 20000) {
    let i = 0;
    for (const k of tokenFirstSeen.keys()) { tokenFirstSeen.delete(k); if (++i >= 5000) break; }
  }
}
function allowReport(ip) {
  const now = Date.now();
  const arr = (reportIpLog.get(ip) || []).filter(t => now - t < 24 * 60 * 60 * 1000);
  if (arr.length >= REPORT_PER_IP_DAY) return false;
  arr.push(now);
  reportIpLog.set(ip, arr);
  return true;
}

const URL_RE = /(https?:\/\/|www\.)\S+/gi;
const DOMAIN_RE = /[\w\u4e00-\u9fa5-]+\.(com|cn|net|org|top|xyz|vip|cc|io|me|tv|club|shop|site|online|link|app|fun|icu)(\/|\b)/gi;
function sanitize(s) {
  // 去链接/去域名/去尖括号：昵称、好友 ID、进度里 < > 没有任何合法用途，
  // 剥掉它们，入库的数据就永远拼不出 HTML 标签（防 XSS / 防注入第一道闸）
  return String(s || '').replace(URL_RE, '').replace(DOMAIN_RE, '').replace(/[<>]/g, '').trim();
}

/* ---- 敏感内容过滤：防辱骂 / 防色情 / 防政治敏感词 / 防个人隐私泄露 ----
 * 只拦明确的高危词，避免误伤正常讨论（比如「小姐姐」「这游戏真垃圾」不拦）。
 * 命中即整条拒绝（400），不做星号替换——替换后的半截话照样能看懂，没意义。 */
const SENSITIVE_WORDS = [
  // 辱骂脏话（明确的人身攻击词）
  '傻逼', '傻比', '沙比', '煞笔', '操你', '草你', '艹你', '你妈', '尼玛', '你妹',
  '滚蛋', '去死', '脑残', '智障', '白痴', '弱智', '死全家', '操蛋', '狗东西',
  // 色情低俗（明确的性相关词）
  '做爱', '性交', '约炮', '裸聊', '色情', '黄片', '嫖娼', '卖淫', '援交',
  // 政治敏感（仅最明确的高危词）
  '六四', '天安门事件', '法轮功', '法轮大法', '台独', '藏独', '疆独', '港独',
  // 广告引流
  '加微信', '加v ', '加q', '加QQ', '刷单', '代购', '兼职赚钱',
];
const SENSITIVE_PATTERNS = [
  /1[3-9]\d{9}/,      // 手机号（11 位）
  /\d{17}[\dXx]/,     // 身份证号（18 位）
];
function findSensitive(s) {
  const t = String(s || '');
  if (!t) return null;
  const low = t.toLowerCase();
  for (const w of SENSITIVE_WORDS) {
    if (low.includes(w.toLowerCase())) return w;
  }
  for (const re of SENSITIVE_PATTERNS) {
    if (re.test(t)) return '个人隐私信息（手机号/身份证号）';
  }
  return null;
}

const app = express();
app.disable('x-powered-by');
app.set('trust proxy', 1); // 部署在 nginx 等反代后时，用 X-Forwarded-For 取真实 IP
app.use(express.json({ limit: '4kb' })); // 限制请求体大小，防超大 payload 打内存

/* ---- 记录匿名身份首次出现时间：用于识别"临时生成的小号"（刚出现就举报一律可疑） ---- */
app.use((req, res, next) => {
  const t = req.body && req.body.token;
  if (t) touchToken(String(t).slice(0, 64));
  next();
});

/* ---- 安全响应头：防 XSS 借道、防 iframe 嵌套、防 MIME 嗅探 ---- */
app.use((req, res, next) => {
  res.setHeader('X-Content-Type-Options', 'nosniff');
  res.setHeader('X-Frame-Options', 'DENY');
  res.setHeader('Referrer-Policy', 'no-referrer');
  res.setHeader('Content-Security-Policy', "default-src 'self'; script-src 'self' 'unsafe-inline'; style-src 'self' 'unsafe-inline'; connect-src 'self' ws: wss:");
  next();
});

/* ---- 跨源防护：非 GET 的 API 请求必须来自本站页面（防别的网站替用户发请求） ---- */
app.use('/api', (req, res, next) => {
  if (req.method !== 'GET') {
    const origin = req.headers.origin;
    if (origin && req.headers.host && !origin.endsWith('//' + req.headers.host)) {
      return res.status(403).json({ error: '拒绝跨站请求' });
    }
  }
  next();
});

/* ---- 全局 API 限频：单 IP 每分钟最多 60 次 ---- */
const apiLog = new Map(); // ip -> [时间戳]
setInterval(() => {
  const cutoff = Date.now() - 60 * 1000;
  for (const [ip, arr] of apiLog) {
    const kept = arr.filter(t => t > cutoff);
    if (kept.length) apiLog.set(ip, kept); else apiLog.delete(ip);
  }
}, 5 * 60 * 1000).unref();
app.use('/api', (req, res, next) => {
  const ip = req.ip || 'unknown';
  const now = Date.now();
  const arr = (apiLog.get(ip) || []).filter(t => now - t < 60 * 1000);
  arr.push(now);
  apiLog.set(ip, arr);
  if (arr.length > 60) return res.status(429).json({ error: '请求太频繁' });
  next();
});

/* ---- 静态资源：HTML 永远重新校验（更新后浏览器立即生效，不留旧缓存），其余默认协商缓存 ---- */
app.use(express.static(path.join(__dirname, 'public'), {
  etag: true,
  setHeaders(res, filePath) {
    if (filePath.endsWith('.html')) res.setHeader('Cache-Control', 'no-cache');
  },
}));

/* ---- JSON 解析失败等错误统一兜底，不向攻击者泄露堆栈 ---- */
app.use((err, req, res, next) => {
  if (err && err.type === 'entity.too.large') return res.status(413).json({ error: '请求体过大' });
  if (err && err.type === 'entity.parse.failed') return res.status(400).json({ error: '请求格式错误' });
  console.error('[server error]', err && err.message);
  res.status(500).json({ error: '服务出错' });
});

app.get('/api/info', (req, res) => {
  res.json({ node: NODE_NAME, federated: !!HUB_URL, version: 3, signing: SIGNING, nodeId: IDENT ? IDENT.nodeId : null });
});
app.get('/api/games', (req, res) => {
  res.json(GAMES);
});

/* ---- 游戏提名：列表 / 投票 ---- */
function nomOf(id) {
  let v = nominateVotes.get(id);
  if (!v) { v = { tokens: new Set(), ips: new Set() }; nominateVotes.set(id, v); }
  return v;
}
function nomReady(v) { return v.tokens.size >= NOMINATE_VOTES && v.ips.size >= NOMINATE_IPS; }

app.get('/api/nominations', (req, res) => {
  const token = String(req.query.token || '').slice(0, 64);
  const list = CANDIDATES.map(c => {
    const v = nomOf(c.id);
    return {
      id: c.id, name: c.name, size: c.size, platforms: c.platforms,
      votes: v.tokens.size,
      ready: nomReady(v),
      added: GAME_MAP.has(c.id),
      voted: token ? v.tokens.has(token) : false,
    };
  }).sort((a, b) => (b.votes - a.votes) || a.name.localeCompare(b.name, 'zh'));
  res.json({ list, threshold: NOMINATE_VOTES, minIps: NOMINATE_IPS });
});

app.post('/api/nominations/:id/vote', (req, res) => {
  const c = CANDIDATES.find(x => x.id === req.params.id);
  if (!c) return res.status(404).json({ error: '没有这个候选游戏' });
  const v = nomOf(c.id);
  if (GAME_MAP.has(c.id)) return res.json({ ok: true, added: true, votes: v.tokens.size });
  const token = String((req.body || {}).token || '').slice(0, 64);
  if (!token) return res.status(400).json({ error: '身份校验失败，请刷新页面' });
  if (v.tokens.has(token)) {
    return res.json({ ok: true, already: true, votes: v.tokens.size, ready: nomReady(v) });
  }
  const ip = (req.socket && req.socket.remoteAddress) || 'unknown';
  const now = Date.now();
  const arr = (nominateIpLog.get(ip) || []).filter(t => now - t < 24 * 60 * 60 * 1000);
  if (arr.length >= NOMINATE_PER_IP_DAY) return res.status(429).json({ error: '今天的提名次数已用完，明天再来' });
  arr.push(now); nominateIpLog.set(ip, arr);

  v.tokens.add(token);
  v.ips.add(ip);
  res.json({ ok: true, votes: v.tokens.size, ready: nomReady(v) });
});

/* ---- 节点主人收录（需配置 ADMIN_TOKEN，curl 调用即可，不需要改代码重启） ---- */
app.post('/api/admin/promote', (req, res) => {
  if (!ADMIN_TOKEN) return res.status(403).json({ error: '未配置 ADMIN_TOKEN，无法在线收录（可改 server.js 的 GAMES 或设置该环境变量）' });
  if (String(req.headers['x-admin-token'] || '') !== ADMIN_TOKEN) return res.status(403).json({ error: '管理令牌错误' });
  const id = String((req.body || {}).id || '');
  const c = CANDIDATES.find(x => x.id === id);
  if (!c) return res.status(404).json({ error: '没有这个候选游戏' });
  if (GAME_MAP.has(c.id)) return res.json({ ok: true, already: true, game: GAME_MAP.get(c.id) });
  const game = { id: c.id, name: c.name, size: c.size, platforms: c.platforms.slice() };
  GAMES.push(game);
  GAME_MAP.set(game.id, game);
  try {
    const extra = GAMES.filter(g => !BASE_GAMES.some(b => b.id === g.id));
    fs.mkdirSync(DATA_DIR, { recursive: true });
    fs.writeFileSync(EXTRA_GAMES_FILE, JSON.stringify(extra, null, 2));
  } catch (e) { console.error('[promote] 写盘失败', e && e.message); }
  console.log(`[promote] 已收录「${c.name}」，当前游戏库 ${GAMES.length} 款`);
  res.json({ ok: true, game });
});
app.get('/api/status', (req, res) => {
  const queued = {};
  for (const [k, m] of rQueue) queued[k] = m.size;
  const board = [...nodeConns.entries()]
    .map(([ws, info]) => ({
      name: info.name,
      helped: nodeContrib.get(ws) || 0,
      verified: info.nodeId ? (verifiedByNodeId.get(info.nodeId) || 0) : 0,
      signed: !!info.nodeId,
      nodeId: info.nodeId || null,
    }))
    .filter(e => e.helped > 0)
    .sort((a, b) => b.helped - a.helped)
    .slice(0, 5);
  res.json({
    role: 'hub-capable', node: NODE_NAME, nodes: nodeConns.size, queued,
    honor: {
      startedAt: nodeStats.startedAt,
      localMatches: nodeStats.localMatches,
      crossMatches: nodeStats.crossMatches,
      matchedPlayers: nodeStats.matchedPlayers,
      verified: nodeStats.verified,
      board,
      signing: SIGNING,
      nodeId: IDENT ? IDENT.nodeId : null,
      attested: attestLog.length,
    },
  });
});

/* ---- 存证（签名 + Merkle）公开查询：任何人都可复核 ---- */
app.get('/api/attest/identity', (req, res) => {
  res.json({
    signing: SIGNING,
    node: NODE_NAME,
    nodeId: IDENT ? IDENT.nodeId : null,
    pubKey: IDENT ? IDENT.pubKeyB64 : null,
  });
});

app.get('/api/attest/roots', (req, res) => {
  const days = [...attestDays().entries()].sort((a, b) => (a[0] < b[0] ? -1 : 1))
    .map(([day, list]) => ({ day, count: list.length, root: attest.buildRoot(list.map(attest.leafOf)) }));
  res.json({ signing: SIGNING, total: attestLog.length, days });
});

app.get('/api/attest/records', (req, res) => {
  const day = String(req.query.day || '');
  const list = day ? (attestDays().get(day) || []) : attestLog.slice(-200);
  // 只公开存证本身，不含任何玩家信息（记录里本来就没有）
  res.json(list.map(r => ({ id: r.id, game: r.game, ts: r.ts, nodes: r.nodes, sigs: r.sigs })));
});

app.get('/api/attest/proof', (req, res) => {
  const id = String(req.query.id || '');
  const rec = attestLog.find(r => r.id === id);
  if (!rec) return res.status(404).json({ error: '未找到该存证' });
  const day = dayKey(rec.ts);
  const list = attestDays().get(day) || [];
  const proof = attest.buildProof(list.map(attest.leafOf), list.indexOf(rec));
  if (!proof) return res.status(500).json({ error: '生成证明失败' });
  // 附上参与方公钥，让证明可以离线独立验（公钥是公开信息）
  const peers = {};
  for (const nid of rec.nodes) peers[nid] = knownKeys.get(nid) || null;
  res.json({ day, ...proof, record: rec, peers });
});

app.get('/api/attest/nodes', (req, res) => {
  res.json([...knownKeys.entries()].map(([nodeId, pubKey]) => ({ nodeId, pubKey })));
});

/* ---- 节点本地存证（本节点自己保存的那份，枢纽挂了也能自证） ---- */
app.get('/api/attest/local', (req, res) => {
  if (!SIGNING) return res.json({ signing: false, records: [] });
  res.json({ signing: true, nodeId: IDENT.nodeId, records: attest.readLocalRecords(DATA_DIR, 200) });
});

/* ============================================================
 * 预约留言板：零自由文本，全部来自固定选项池
 * ============================================================ */
const SLOTS = ['今晚 20-23 点', '今晚 23 点后', '明天白天', '明天晚上', '本周六', '本周日', '工作日随时', '时间灵活'];
const TAGS = ['休闲不肝', '能带萌新', '不骂人', '有麦沟通', '佛系试错', '进度同步', '速通冲关', '只玩一局', '女生局'];
const BOOKING_TTL = 24 * 60 * 60 * 1000; // 24 小时自动过期
const MAX_ACTIVE_PER_TOKEN = 1;          // 同一身份同时只能有 1 条预约
const MAX_PER_DAY = 3;                   // 每天最多发 3 条
const bookings = new Map();              // id -> booking
const bookingDayLog = new Map();         // token -> [发布时间戳]
const bookingIpLog = new Map();          // ip -> [发布时间戳]

/* ---- 预约落盘：永久数据，重启/更新不丢；过期预约进档案，不删除 ---- */
const BOOKINGS_FILE = path.join(DATA_DIR, 'bookings.json');
const BOOKINGS_ARCHIVE_FILE = path.join(DATA_DIR, 'bookings-archive.json');
const BOOKINGS_ARCHIVE_MAX = 200; // 档案最多保留 200 条，超了删最旧的
const bookingsArchive = []; // [{...bookingView, archivedAt, archiveReason}]
function bookingToDisk(b) {
  return {
    id: b.id, game: b.game, platform: b.platform, slot: b.slot,
    have: b.have, need: b.need, mic: b.mic, tags: b.tags, own: b.own || 'own',
    owner: { nick: b.owner.nick, fid: b.owner.fid }, ownerToken: b.ownerToken,
    members: b.members.map(m => ({ nick: m.nick, fid: m.fid, token: m.token })),
    reports: [...(b.reports || [])], reportIps: [...(b.reportIps || [])],
    createdAt: b.createdAt, expiresAt: b.expiresAt,
  };
}
function bookingFromDisk(d) {
  return {
    id: d.id, game: d.game, platform: d.platform, slot: d.slot,
    have: d.have, need: d.need, mic: d.mic, tags: d.tags || [], own: d.own || 'own',
    owner: d.owner || { nick: '玩家', fid: '' }, ownerToken: d.ownerToken || '',
    members: Array.isArray(d.members) ? d.members : [],
    reports: new Set(Array.isArray(d.reports) ? d.reports : []),
    reportIps: new Set(Array.isArray(d.reportIps) ? d.reportIps : []),
    createdAt: d.createdAt || Date.now(), expiresAt: d.expiresAt || (Date.now() + BOOKING_TTL),
  };
}
function saveBookingsToDisk() {
  try {
    fs.mkdirSync(DATA_DIR, { recursive: true });
    fs.writeFileSync(BOOKINGS_FILE, JSON.stringify([...bookings.values()].map(bookingToDisk), null, 2));
  } catch (e) { console.error('[bookings] 写盘失败', e && e.message); }
}
function saveBookingsArchiveToDisk() {
  try {
    fs.mkdirSync(DATA_DIR, { recursive: true });
    fs.writeFileSync(BOOKINGS_ARCHIVE_FILE, JSON.stringify(bookingsArchive, null, 2));
  } catch (e) { console.error('[bookings-archive] 写盘失败', e && e.message); }
}
function loadBookingsFromDisk() {
  try {
    const arr = JSON.parse(fs.readFileSync(BOOKINGS_FILE, 'utf8'));
    if (!Array.isArray(arr)) return;
    for (const d of arr) {
      if (!d || typeof d.id !== 'string') continue;
      bookings.set(d.id, bookingFromDisk(d));
    }
  } catch { /* 首次运行无文件，正常 */ }
  try {
    const arr = JSON.parse(fs.readFileSync(BOOKINGS_ARCHIVE_FILE, 'utf8'));
    if (Array.isArray(arr)) bookingsArchive.push(...arr.slice(-BOOKINGS_ARCHIVE_MAX));
  } catch { /* 首次运行无文件，正常 */ }
}
// 过期/被举报下架的预约进档案（不删除，只搬家）；reason: 'expired' | 'reported'
function archiveBooking(b, reason) {
  bookings.delete(b.id);
  const entry = { ...bookingView(b), archivedAt: Date.now(), archiveReason: reason };
  bookingsArchive.push(entry);
  if (bookingsArchive.length > BOOKINGS_ARCHIVE_MAX) {
    bookingsArchive.splice(0, bookingsArchive.length - BOOKINGS_ARCHIVE_MAX);
  }
  saveBookingsToDisk();
  saveBookingsArchiveToDisk();
}
// 每小时清一次：把过期预约搬进档案
setInterval(() => {
  const now = Date.now();
  for (const b of [...bookings.values()]) {
    if (b.expiresAt <= now) archiveBooking(b, 'expired');
  }
}, 60 * 60 * 1000).unref();
loadBookingsFromDisk();

function bookingView(b) {
  return {
    id: b.id, game: b.game, platform: b.platform, slot: b.slot,
    have: b.have, need: b.need, mic: b.mic, tags: b.tags, own: b.own || 'own',
    owner: { nick: b.owner.nick, fid: b.owner.fid },
    members: b.members.map(m => ({ nick: m.nick, fid: m.fid })),
    createdAt: b.createdAt, expiresAt: b.expiresAt,
    full: b.members.length >= b.need,
  };
}

app.get('/api/bookings', (req, res) => {
  const now = Date.now();
  const list = [...bookings.values()]
    .filter(b => b.expiresAt > now)
    .sort((a, b) => b.createdAt - a.createdAt)
    .map(bookingView);
  res.json(list);
});

// 过期预约档案：只读，给节点主人看历史留底（最多 200 条）
app.get('/api/bookings/archive', (req, res) => {
  res.json([...bookingsArchive].reverse());
});

app.post('/api/bookings', (req, res) => {
  const b = req.body || {};
  const token = String(b.token || '').slice(0, 64);
  if (!token) return res.status(400).json({ error: '身份校验失败，请刷新页面' });
  if (isBanned(token)) return res.status(403).json({ error: '你的身份已被多次举报，暂时无法发布' });

  const game = GAME_MAP.get(b.game);
  const platform = ALL_PLATFORMS.includes(b.platform) ? b.platform : null;
  if (!game || !platform || !game.platforms.includes(platform)) return res.status(400).json({ error: '游戏或平台无效' });
  if (!SLOTS.includes(b.slot)) return res.status(400).json({ error: '时段无效' });
  const have = parseInt(b.have, 10), need = parseInt(b.need, 10);
  // 人数必须自洽：已有 + 还缺 = 该游戏开局人数（如双人成行 2 人局，已有 1 就只能缺 1）
  if (!Number.isInteger(have) || !Number.isInteger(need) || have < 1 || need < 1) {
    return res.status(400).json({ error: '人数无效' });
  }
  if (have + need !== game.size) {
    return res.status(400).json({ error: `${game.name} 是 ${game.size} 人局，已有 ${have} 人时还缺应为 ${game.size - have} 人` });
  }
  const tags = Array.isArray(b.tags) ? b.tags.filter(t => TAGS.includes(t)).slice(0, 4) : [];
  const mic = !!b.mic;
  const nick = sanitize(b.nick).slice(0, 12) || '玩家';
  const hitNick = findSensitive(nick);
  if (hitNick) return res.status(400).json({ error: '昵称包含敏感内容，请修改后再发' });
  const fidRaw = String(b.fid || '');
  const fid = sanitize(fidRaw).slice(0, 40);
  if (!fid) return res.status(400).json({ error: '好友 ID 不能为空' });
  if (fid !== fidRaw.trim()) return res.status(400).json({ error: '好友 ID 不能包含链接或网址' });
  const hitFid = findSensitive(fid);
  if (hitFid) return res.status(400).json({ error: '好友 ID 包含敏感内容，请修改后再发' });

  const ip = (req.socket && req.socket.remoteAddress) || 'unknown';
  const now = Date.now();
  // 频率限制：单 token 1 条在架 + 每天 3 条；单 IP 每天 6 条
  const active = [...bookings.values()].filter(x => x.ownerToken === token && x.expiresAt > now);
  if (active.length >= MAX_ACTIVE_PER_TOKEN) return res.status(429).json({ error: '你已有一条进行中的预约，先等它过期或满员吧' });
  const dayArr = (bookingDayLog.get(token) || []).filter(t => now - t < 24 * 60 * 60 * 1000);
  if (dayArr.length >= MAX_PER_DAY) return res.status(429).json({ error: '今天发布的预约已达上限（3 条/天）' });
  dayArr.push(now); bookingDayLog.set(token, dayArr);
  const ipArr = (bookingIpLog.get(ip) || []).filter(t => now - t < 24 * 60 * 60 * 1000);
  if (ipArr.length >= 6) return res.status(429).json({ error: '发布太频繁，请明天再试' });
  ipArr.push(now); bookingIpLog.set(ip, ipArr);

  const booking = {
    id: crypto.randomUUID(), game: game.id, platform, slot: b.slot,
    have, need, mic, tags,
    own: b.own === 'pass' ? 'pass' : 'own', // 发起人是否有本体
    owner: { nick, fid }, ownerToken: token,
    members: [], reports: new Set(), reportIps: new Set(),
    createdAt: now, expiresAt: now + BOOKING_TTL,
  };
  bookings.set(booking.id, booking);
  saveBookingsToDisk();
  res.json(bookingView(booking));
});

app.post('/api/bookings/:id/join', (req, res) => {
  const b = bookings.get(req.params.id);
  if (!b || b.expiresAt < Date.now()) return res.status(404).json({ error: '预约不存在或已过期' });
  const token = String((req.body || {}).token || '').slice(0, 64);
  if (!token) return res.status(400).json({ error: '身份校验失败' });
  if (isBanned(token)) return res.status(403).json({ error: '你的身份已被多次举报，暂时无法应约' });
  if (b.ownerToken === token) return res.status(400).json({ error: '这是你自己的预约呀' });
  if (b.members.some(m => m.token === token)) return res.status(400).json({ error: '你已经应约过了' });
  if (b.members.length >= b.need) return res.status(400).json({ error: '这个预约已经满员啦' });
  const nick = sanitize((req.body || {}).nick).slice(0, 12) || '玩家';
  const fid = sanitize((req.body || {}).fid).slice(0, 40);
  const hitNick = findSensitive(nick);
  if (hitNick) return res.status(400).json({ error: '昵称包含敏感内容，请修改后再发' });
  const hitFid = findSensitive(fid);
  if (hitFid) return res.status(400).json({ error: '好友 ID 包含敏感内容，请修改后再发' });
  if (!fid) return res.status(400).json({ error: '请先在匹配页填好好友 ID（不能含链接）' });
  b.members.push({ nick, fid, token });
  saveBookingsToDisk();
  res.json(bookingView(b));
});

app.post('/api/bookings/:id/report', (req, res) => {
  const b = bookings.get(req.params.id);
  if (!b || b.expiresAt < Date.now()) return res.status(404).json({ error: '预约不存在或已过期' });
  const token = String((req.body || {}).token || '').slice(0, 64);
  if (!token) return res.status(400).json({ error: '身份校验失败' });
  if (b.ownerToken === token) return res.status(400).json({ error: '不能举报自己的预约' });
  if (b.reports.has(token)) return res.json({ ok: true, removed: false, already: true }); // 重复举报不计数

  // 闸①：举报身份必须早于该预约出现（临时清缓存生成的小号没有历史，直接拒）
  const seen = tokenFirstSeen.get(token);
  if (!seen || seen > b.createdAt) {
    return res.status(403).json({ error: '举报前请先正常使用本服务一段时间（防止小号刷举报）' });
  }
  // 闸②：单 IP 举报频率
  const ip = (req.socket && req.socket.remoteAddress) || 'unknown';
  if (!allowReport(ip)) return res.status(429).json({ error: '你的举报太频繁了，请明天再试' });

  b.reports.add(token);
  b.reportIps.add(ip); // 闸③：同一 IP 只计一次，换 token 也刷不动
  // 闸④：下架需同时满足"3 个不同身份"且"来自 ≥2 个不同 IP"
  if (b.reports.size >= REPORTS_TO_BAN && b.reportIps.size >= REPORT_MIN_IPS) {
    archiveBooking(b, 'reported'); // 进档案留底，不直接删
    bannedUntil.set(b.ownerToken, Date.now() + BAN_MS); // 直接拉黑，与匹配黑名单联动
    return res.json({ ok: true, removed: true });
  }
  res.json({ ok: true, removed: false, needMore: b.reports.size < REPORTS_TO_BAN || b.reportIps.size < REPORT_MIN_IPS });
});

/* ============================================================
 * 节点留言板：玩家 → 节点部署者的互动窗口
 * 短文本（60字）+ 严格限频 + 可举报隐藏 + 自动过期 + 全文消毒
 * ============================================================ */
const NODE_MOTD = process.env.NODE_MOTD || ''; // 节点主人的公告位（部署者回话用）
const MSG_MAX_LEN = 60;
const MSG_TTL = 14 * 24 * 60 * 60 * 1000; // 14 天过期
const MSG_KEEP_MAX = 50;                  // 最多保留 50 条
const MSG_PER_TOKEN_MS = 60 * 60 * 1000;  // 同身份每小时 1 条
const MSG_PER_IP_DAY = 5;                 // 单 IP 每天 5 条
const messages = new Map();               // id -> message
const msgTokenLog = new Map();            // token -> 上次留言时间
const msgIpLog = new Map();               // ip -> [时间戳]

/* ---- 留言落盘：重启不丢（之前只放内存，服务一重启留言全没了） ---- */
const MESSAGES_FILE = path.join(DATA_DIR, 'messages.json');
function saveMessagesToDisk() {
  try {
    fs.mkdirSync(DATA_DIR, { recursive: true });
    const arr = [...messages.values()].map(m => ({
      id: m.id, nick: m.nick, text: m.text, token: m.token,
      reports: [...(m.reports || [])], reportIps: [...(m.reportIps || [])],
      createdAt: m.createdAt, expiresAt: m.expiresAt,
    }));
    fs.writeFileSync(MESSAGES_FILE, JSON.stringify(arr, null, 2));
  } catch (e) { console.error('[messages] 写盘失败', e && e.message); }
}
function loadMessagesFromDisk() {
  try {
    const arr = JSON.parse(fs.readFileSync(MESSAGES_FILE, 'utf8'));
    if (!Array.isArray(arr)) return;
    const now = Date.now();
    for (const m of arr) {
      if (!m || typeof m.id !== 'string' || typeof m.text !== 'string') continue;
      if (m.expiresAt && m.expiresAt <= now) continue; // 已过期不恢复
      messages.set(m.id, {
        id: m.id, nick: m.nick || '玩家', text: m.text, token: m.token || '',
        reports: new Set(Array.isArray(m.reports) ? m.reports : []),
        reportIps: new Set(Array.isArray(m.reportIps) ? m.reportIps : []),
        createdAt: m.createdAt || now, expiresAt: m.expiresAt || (now + MSG_TTL),
      });
    }
    if (messages.size > MSG_KEEP_MAX) {
      const sorted = [...messages.values()].sort((a, b) => a.createdAt - b.createdAt);
      for (const m of sorted.slice(0, messages.size - MSG_KEEP_MAX)) messages.delete(m.id);
    }
  } catch { /* 首次运行无文件，正常 */ }
}
loadMessagesFromDisk();

function msgView(m) {
  return { id: m.id, nick: m.nick, text: m.text, createdAt: m.createdAt, motdReply: !!m.motdReply };
}

app.get('/api/messages', (req, res) => {
  const now = Date.now();
  const list = [...messages.values()]
    .filter(m => m.expiresAt > now && m.reports.size < REPORTS_TO_BAN)
    .sort((a, b) => b.createdAt - a.createdAt)
    .slice(0, 20)
    .map(msgView);
  res.json({ motd: NODE_MOTD, list });
});

app.post('/api/messages', (req, res) => {
  const b = req.body || {};
  const token = String(b.token || '').slice(0, 64);
  if (!token) return res.status(400).json({ error: '身份校验失败，请刷新页面' });
  if (isBanned(token)) return res.status(403).json({ error: '你的身份已被多次举报，暂时无法留言' });

  const ip = (req.socket && req.socket.remoteAddress) || 'unknown';
  const now = Date.now();
  // 限频：同身份每小时 1 条，单 IP 每天 5 条
  const last = msgTokenLog.get(token) || 0;
  if (now - last < MSG_PER_TOKEN_MS) {
    const wait = Math.ceil((MSG_PER_TOKEN_MS - (now - last)) / 60000);
    return res.status(429).json({ error: `留言太频繁，约 ${wait} 分钟后再试` });
  }
  const ipArr = (msgIpLog.get(ip) || []).filter(t => now - t < 24 * 60 * 60 * 1000);
  if (ipArr.length >= MSG_PER_IP_DAY) return res.status(429).json({ error: '今天留言数已达上限，明天再来吧' });

  // 内容：含链接直接拒绝（留言板唯一文本入口，比剥离更严格）；截断 60 字；纯空白拒绝
  const raw = String(b.text || '').trim();
  if (!raw) return res.status(400).json({ error: '留言内容不能为空' });
  const cleaned = sanitize(raw);
  if (cleaned !== raw) return res.status(400).json({ error: '留言不能包含链接或网址' });
  const hitText = findSensitive(raw);
  if (hitText) return res.status(400).json({ error: '留言包含敏感内容，请修改后再发' });
  const text = raw.slice(0, MSG_MAX_LEN);
  const nick = sanitize(b.nick).slice(0, 12) || '玩家';
  const hitNick = findSensitive(nick);
  if (hitNick) return res.status(400).json({ error: '昵称包含敏感内容，请修改后再发' });

  msgTokenLog.set(token, now);
  ipArr.push(now); msgIpLog.set(ip, ipArr);
  const msg = { id: crypto.randomUUID(), nick, text, token, reports: new Set(), createdAt: now, expiresAt: now + MSG_TTL };
  messages.set(msg.id, msg);
  // 超量时淘汰最旧的
  if (messages.size > MSG_KEEP_MAX) {
    const oldest = [...messages.values()].sort((a, b) => a.createdAt - b.createdAt)[0];
    if (oldest) messages.delete(oldest.id);
  }
  saveMessagesToDisk();
  res.json(msgView(msg));
});

app.post('/api/messages/:id/report', (req, res) => {
  const m = messages.get(req.params.id);
  if (!m) return res.status(404).json({ error: '留言不存在' });
  const token = String((req.body || {}).token || '').slice(0, 64);
  if (!token) return res.status(400).json({ error: '身份校验失败' });
  if (m.token === token) return res.status(400).json({ error: '不能举报自己的留言' });
  if (m.reports.has(token)) return res.json({ ok: true, removed: false, already: true });

  // 与预约举报同一套四道闸：身份早于留言、IP 频率、IP 去重、多 IP 同时举报
  const seen = tokenFirstSeen.get(token);
  if (!seen || seen > m.createdAt) {
    return res.status(403).json({ error: '举报前请先正常使用本服务一段时间（防止小号刷举报）' });
  }
  const ip = (req.socket && req.socket.remoteAddress) || 'unknown';
  if (!allowReport(ip)) return res.status(429).json({ error: '你的举报太频繁了，请明天再试' });

  m.reports.add(token);
  if (!m.reportIps) m.reportIps = new Set();
  m.reportIps.add(ip);
  saveMessagesToDisk();
  if (m.reports.size >= REPORTS_TO_BAN && m.reportIps.size >= REPORT_MIN_IPS) {
    messages.delete(m.id);
    saveMessagesToDisk();
    bannedUntil.set(m.token, Date.now() + BAN_MS); // 恶意留言者与匹配黑名单联动
    return res.json({ ok: true, removed: true });
  }
  res.json({ ok: true, removed: false });
});

const server = http.createServer(app);

/* ============================================================
 * 一、本地玩家匹配（队列键 = 游戏 × 平台）
 * ============================================================ */
const wss = new WebSocketServer({ noServer: true, maxPayload: 8 * 1024 }); // 单条消息最大 8KB
const clients = new Map(); // ws -> info
const queues = new Map();  // "game|platform" -> [ws]

/* ============================================================
 * 二、枢纽端：接收其他节点的玩家，跨节点撮合
 * ============================================================ */
const relayWss = new WebSocketServer({ noServer: true, maxPayload: 8 * 1024 });
const nodeConns = new Map();   // ws -> { name, nodeId, pubKey }  节点名 + 密码学身份
const nodeContrib = new Map(); // 荣誉榜：节点ws -> 促成的跨节点配对数（含未签名）
const verifiedByNodeId = new Map(); // 存证计数：nodeId -> 已双向签名的配对数
const pendingAttest = new Map();    // id -> { rec, need:Set<nodeId>, peers, timer }
const attestLog = [];               // 已完成的存证记录（按时间追加，供 Merkle 与审计）
const ATTEST_LOG_MAX = 20000;
const ATTEST_TIMEOUT_MS = 8000;
const knownKeys = new Map(); // nodeId -> pubKeyB64（节点身份公钥，公开信息，供任何人验签）
if (IDENT) knownKeys.set(IDENT.nodeId, IDENT.pubKeyB64);
const rQueue = new Map();  // "game|platform" -> Map(key -> {ws, player})
const rooms = new Map();   // key -> peerKey
// 节点贡献统计（自建节点荣誉机制）
/* ---- 节点统计落盘：重启不丢（之前只放内存，服务一重启「运行 X 小时」和「已促成 X 局」全归零） ---- */
const NODE_STATS_FILE = path.join(DATA_DIR, 'node-stats.json');
function saveNodeStatsToDisk() {
  try {
    fs.mkdirSync(DATA_DIR, { recursive: true });
    fs.writeFileSync(NODE_STATS_FILE, JSON.stringify(nodeStats, null, 2));
  } catch (e) { console.error('[nodeStats] 写盘失败', e && e.message); }
}
const nodeStats = (() => {
  try {
    const s = JSON.parse(fs.readFileSync(NODE_STATS_FILE, 'utf8'));
    if (s && Number.isFinite(s.startedAt)) {
      return {
        startedAt: s.startedAt,
        localMatches: Number(s.localMatches) || 0,
        crossMatches: Number(s.crossMatches) || 0,
        verified: Number(s.verified) || 0,
        matchedPlayers: Number(s.matchedPlayers) || 0,
      };
    }
  } catch { /* 首次运行无文件，正常 */ }
  return { startedAt: Date.now(), localMatches: 0, crossMatches: 0, verified: 0, matchedPlayers: 0 };
})();
saveNodeStatsToDisk(); // 首次运行落盘一份；已存在时写回同值，无害

function getRQueue(k) {
  if (!rQueue.has(k)) rQueue.set(k, new Map());
  return rQueue.get(k);
}

function hubBroadcastStats() {
  const queued = {};
  let total = 0;
  for (const [k, m] of rQueue) {
    queued[k] = m.size;
    total += m.size;
  }
  // 节点荣誉榜：按促成的跨节点配对数取前 5（verified = 双向签名已存证的部分）
  const board = [...nodeConns.entries()]
    .map(([ws, info]) => ({
      name: info.name,
      helped: nodeContrib.get(ws) || 0,
      verified: info.nodeId ? (verifiedByNodeId.get(info.nodeId) || 0) : 0,
      signed: !!info.nodeId,
      nodeId: info.nodeId || null,
    }))
    .filter(e => e.helped > 0)
    .sort((a, b) => b.helped - a.helped)
    .slice(0, 5);
  const payload = JSON.stringify({
    type: 'hub_stats', nodes: nodeConns.size, queued, total, board,
    signing: SIGNING,
  });
  for (const ws of nodeConns.keys()) {
    if (ws.readyState === ws.OPEN) ws.send(payload);
  }
}

function hubRemove(key, notifyPeer = true) {
  for (const m of rQueue.values()) m.delete(key);
  const peerKey = rooms.get(key);
  if (peerKey) {
    rooms.delete(key);
    rooms.delete(peerKey);
    if (notifyPeer) {
      const peer = keyInfo.get(peerKey);
      if (peer && peer.ws.readyState === peer.ws.OPEN) {
        peer.ws.send(JSON.stringify({ type: 'partner_left', key: peerKey }));
      }
    }
  }
  hubBroadcastStats();
}

/* ---- 存证：跨节点配对的双向签名与每日 Merkle 根 ---- */

/** 交易日（按服务器本地时区切日） */
function dayKey(ts) {
  const d = new Date(ts);
  const p = n => String(n).padStart(2, '0');
  return `${d.getFullYear()}-${p(d.getMonth() + 1)}-${p(d.getDate())}`;
}

/** 按天分组所有已完成的存证 */
function attestDays() {
  const map = new Map();
  for (const r of attestLog) {
    const d = dayKey(r.ts);
    if (!map.has(d)) map.set(d, []);
    map.get(d).push(r);
  }
  return map;
}

/** 某个参与方的身份；枢纽自己的本地玩家用本进程身份，null 表示该方不支持签名 */
function partyOf(ws) {
  if (ws && ws.isLocalClient) {
    return SIGNING ? { nodeId: IDENT.nodeId, pubKey: IDENT.pubKeyB64, ws: null } : null;
  }
  const info = nodeConns.get(ws);
  if (info && info.nodeId && info.pubKey) return { nodeId: info.nodeId, pubKey: info.pubKey, ws };
  return null;
}

/**
 * 一局跨节点配对 = 一张存证，所有参与节点各自签名（多签）。
 * 同一节点有多个玩家在同一局里，只算一个参与方（按 nodeId 去重），
 * 否则 4 人局里枢纽的 3 个本地玩家会被算成 3 个独立节点、把计数刷成 3 倍。
 */
function requestAttestations(pickedNodes, gameId) {
  if (!SIGNING) return;
  const byId = new Map();
  for (const ws of pickedNodes) {
    const p = partyOf(ws);
    if (p && !byId.has(p.nodeId)) byId.set(p.nodeId, p);
  }
  const parties = [...byId.values()];
  if (parties.length < 2) return; // 只有一方（全在同一节点）无需存证
  createAttestation(parties, gameId);
}

function createAttestation(parties, gameId) {
  const rec = {
    id: crypto.randomUUID(),
    game: gameId,
    ts: Date.now(),
    nodes: parties.map(p => p.nodeId),
    sigs: {},
  };
  const peers = {};
  for (const p of parties) peers[p.nodeId] = p.pubKey;
  const need = new Set(rec.nodes);

  // 枢纽自己也是参与方时，本地直接签，不必问自己
  for (const p of parties) {
    if (p.ws === null) {
      rec.sigs[p.nodeId] = attest.sign(rec);
      need.delete(p.nodeId);
    }
  }
  if (!need.size) return finalizeAttestation(rec, peers);

  const timer = setTimeout(() => {
    pendingAttest.delete(rec.id);
    console.warn(`[存证] ${rec.id.slice(0, 8)} 超时未收齐签名（缺 ${need.size} 方），本次配对不计入已验证`);
  }, ATTEST_TIMEOUT_MS);
  if (timer.unref) timer.unref();
  pendingAttest.set(rec.id, { rec, need, peers, timer });

  const payload = JSON.stringify({ type: 'attest_req', ...rec, peers });
  for (const p of parties) {
    if (p.ws && p.ws.readyState === p.ws.OPEN) {
      try { p.ws.send(payload); } catch {}
    }
  }
}

/** 签名齐了：验签 → 计数 → 落日志 → 通知参与节点各自保存 */
function finalizeAttestation(rec, peers) {
  const pending = pendingAttest.get(rec.id);
  if (pending) { clearTimeout(pending.timer); pendingAttest.delete(rec.id); peers = peers || pending.peers; }
  if (!attest.verifyRecord(rec, peers)) {
    console.warn(`[存证] ${rec.id.slice(0, 8)} 验签失败，丢弃`);
    return false;
  }
  // 每个参与节点记一局（一局一张存证，不重复计数）
  for (const nid of rec.nodes) verifiedByNodeId.set(nid, (verifiedByNodeId.get(nid) || 0) + 1);
  // 枢纽自己也是参与方时，本地计数 + 留档
  if (IDENT && rec.nodes.includes(IDENT.nodeId)) {
    nodeStats.verified++; saveNodeStatsToDisk();
    attest.appendLocalRecord(DATA_DIR, rec);
  }
  attestLog.push(rec);
  if (attestLog.length > ATTEST_LOG_MAX) attestLog.shift();

  const payload = JSON.stringify({ type: 'attest_ok', ...rec });
  for (const [ws, info] of nodeConns) {
    if (info.nodeId && rec.nodes.includes(info.nodeId) && ws.readyState === ws.OPEN) {
      try { ws.send(payload); } catch {}
    }
  }
  hubBroadcastStats();
  return true;
}

const keyInfo = new Map(); // key -> { ws, qkey, player, token }

const MAX_RELAY_NODES = 100; // 枢纽最多接受 100 个节点连接
relayWss.on('connection', (ws) => {
  if (relayWss.clients.size > MAX_RELAY_NODES) { try { ws.close(1008, 'too many nodes'); } catch {} return; }
  ws.on('message', (raw) => {
    let msg;
    try { msg = JSON.parse(raw); } catch { return; }

    if (msg.type === 'hello') {
      const name = String(msg.name || '未命名节点').slice(0, 24);
      let nodeId = null, pubKey = null;
      // 身份可选：带公钥才认，且 nodeId 必须等于公钥指纹（否则视为冒充）
      if (SIGNING && msg.nodeId && msg.pubKey) {
        try {
          const der = Buffer.from(String(msg.pubKey), 'base64');
          const derived = attest.nodeIdFromPubDer(der);
          if (derived === String(msg.nodeId)) { nodeId = derived; pubKey = String(msg.pubKey); knownKeys.set(nodeId, pubKey); }
        } catch {}
      }
      nodeConns.set(ws, { name, nodeId, pubKey });
      hubBroadcastStats();
      return;
    }

    // 节点对自己的存证签名
    if (msg.type === 'attest_sig' && msg.id) {
      const p = pendingAttest.get(msg.id);
      const info = nodeConns.get(ws);
      if (!p || !info || !info.nodeId) return;
      if (String(msg.nodeId) !== info.nodeId) return;              // 只能用自己身份签
      const sig = String(msg.sig || '');
      if (!attest.verify(info.pubKey, p.rec, sig)) return;         // 签名必须有效
      p.rec.sigs[info.nodeId] = sig;
      p.need.delete(info.nodeId);
      if (!p.need.size) finalizeAttestation(p.rec, p.peers);
      return;
    }

    if (msg.type === 'join') {
      const game = GAME_MAP.get(msg.game);
      const platform = ALL_PLATFORMS.includes(msg.platform) ? msg.platform : null;
      if (!game || !platform || !msg.key) return;
      if (!game.platforms.includes(platform)) return;
      const token = String(msg.token || '').slice(0, 64);
      if (hubStrikes.get(token) >= REPORTS_TO_BAN) {
        ws.send(JSON.stringify({ type: 'banned', key: msg.key }));
        return;
      }
      const k = qkey(game.id, platform);
      // 跨节点来的数据也要过一遍敏感词闸（防恶意节点绕过本站过滤直接投毒）
      if (findSensitive(msg.nick) || findSensitive(msg.id) || findSensitive(msg.chapter)) return;
      const entry = { ws, player: { nick: sanitize(msg.nick).slice(0, 12) || '玩家', game: game.id, platform, id: sanitize(msg.id).slice(0, 40), chapter: sanitize(msg.chapter).slice(0, 20), mic: !!msg.mic, own: msg.own === 'pass' ? 'pass' : 'own', key: msg.key } };
      getRQueue(k).set(msg.key, entry);
      keyInfo.set(msg.key, { ws, qkey: k, player: entry.player, token });
      hubTryMatch(k);
      return;
    }

    if (msg.type === 'cancel' && msg.key) {
      hubRemove(msg.key, false);
      return;
    }

    if (msg.type === 'report' && msg.key) {
      const info = keyInfo.get(msg.key);
      if (info && info.token) {
        const n = (hubStrikes.get(info.token) || 0) + 1;
        hubStrikes.set(info.token, n);
        if (n >= REPORTS_TO_BAN) hubRemove(msg.key, false);
      }
      return;
    }

    if (msg.type === 'leave' && msg.key) {
      hubRemove(msg.key, true);
    }
  });

  ws.on('close', () => {
    for (const m of rQueue.values()) {
      for (const key of [...m.keys()]) {
        if (m.get(key).ws === ws) m.delete(key);
      }
    }
    for (const [key, peerKey] of [...rooms.entries()]) {
      const info = keyInfo.get(key);
      if (info && info.ws === ws) {
        rooms.delete(key); rooms.delete(peerKey);
        const peer = keyInfo.get(peerKey);
        if (peer && peer.ws.readyState === peer.ws.OPEN) {
          peer.ws.send(JSON.stringify({ type: 'partner_left', key: peerKey }));
        }
      }
    }
    const goneNodeId = nodeConns.get(ws)?.nodeId || null;
    nodeConns.delete(ws);
    // 该节点掉线：它参与的未完成存证一律作废，避免只算一半
    if (goneNodeId) {
      for (const [id, p] of [...pendingAttest.entries()]) {
        if (p.need.has(goneNodeId) || p.rec.nodes.includes(goneNodeId)) {
          clearTimeout(p.timer);
          pendingAttest.delete(id);
        }
      }
    }
    hubBroadcastStats();
  });
  ws.on('error', () => {});
});

function hubTryMatch(k) {
  const m = getRQueue(k);
  const game = GAME_MAP.get(k.split('|')[0]);
  if (!game || m.size < game.size) return;
  const keys = [...m.keys()];
  const picked = [];
  let lastNode = null;
  for (const key of keys) {
    const entry = m.get(key);
    if (entry.ws.readyState !== entry.ws.OPEN) { m.delete(key); continue; }
    if (picked.length && entry.ws === lastNode) continue; // 同节点不跨节点撮合
    picked.push(key);
    lastNode = entry.ws;
    if (picked.length === game.size) break;
  }
  if (picked.length < game.size) return;
  // 本体的硬性要求：组里至少要有一个有本体的玩家，否则谁也开不了房
  const hasOwner = picked.some(key => (m.get(key).player.own || 'own') === 'own');
  if (!hasOwner) {
    const ownerKey = keys.find(k2 => !picked.includes(k2) && (m.get(k2).player.own || 'own') === 'own');
    if (!ownerKey) return;                   // 全是通行证，等他处有人来
    picked[picked.length - 1] = ownerKey;    // 换一个进来（可能影响节点分布，下面再校验）
  }
  const players = picked.map(key => m.get(key).player);
  // 不同节点的玩家才在枢纽撮合
  const pickedNodes = new Set(picked.map(key => m.get(key).ws));
  if (pickedNodes.size < 2) return;
  for (const key of picked) m.delete(key);
  // 荣誉记账：每个参与节点 +1
  for (const nws of pickedNodes) nodeContrib.set(nws, (nodeContrib.get(nws) || 0) + 1);
  nodeStats.crossMatches++; saveNodeStatsToDisk();
  // 只统计落在本节点上的玩家，避免跨节点重复计数
  nodeStats.matchedPlayers += picked.filter(k => {
    const w = keyInfo.get(k) && keyInfo.get(k).ws;
    return w && w.isLocalClient;
  }).length;
  saveNodeStatsToDisk();
  // 开存证：参与的每两个节点互签，签名齐全才计入「已验证」
  requestAttestations(pickedNodes, game.id);
  picked.forEach((key, i) => {
    rooms.set(key, picked[(i + 1) % picked.length]);
    const entryWs = keyInfo.get(key).ws;
    entryWs.send(JSON.stringify({ type: 'matched', key, partner: players[(i + 1) % picked.length] }));
    // 枢纽本地玩家：同步服务端状态
    if (entryWs.isLocalClient) hubMatchLocal(entryWs, key);
  });
  hubBroadcastStats();
}

function hubMatchLocal(ws, key) {
  const info = clients.get(ws);
  if (!info) return;
  info.state = 'matched';
  info.remoteMatch = true;
  info.remotePartnerKey = key;
  info.partner = null;
  const lq = queues.get(info.qkey);
  if (lq) {
    const li = lq.indexOf(ws);
    if (li !== -1) lq.splice(li, 1);
  }
  if (info.token) tokenQueuing.delete(info.token);
  localByKey.set(key, ws);
}

/* ============================================================
 * 三、节点端：上报枢纽、接收跨节点结果
 * ============================================================ */
let hubWs = null;
let hubReady = false;
let networkStats = null;
const localByKey = new Map();

function hubConnect() {
  if (!HUB_URL) return;
  try {
    const url = HUB_URL.replace(/^http/, 'ws') + '/relay';
    hubWs = new WebSocket(url, { headers: RELAY_TOKEN ? { 'x-relay-token': RELAY_TOKEN } : {} });
    hubWs.on('open', () => {
      hubReady = true;
      const hello = { type: 'hello', name: NODE_NAME };
      if (SIGNING) { hello.nodeId = IDENT.nodeId; hello.pubKey = IDENT.pubKeyB64; }
      hubWs.send(JSON.stringify(hello));
      for (const [k, arr] of queues) {
        for (const ws of arr) {
          const info = clients.get(ws);
          if (info && info.state === 'queued') relayJoin(info, ws);
        }
      }
      console.log(`[联邦] 已接入枢纽 ${HUB_URL}`);
    });
    hubWs.on('message', (raw) => {
      let msg;
      try { msg = JSON.parse(raw); } catch { return; }
      if (msg.type === 'matched') {
        const ws = localByKey.get(msg.key);
        const info = ws ? clients.get(ws) : null;
        if (!info) { if (hubReady) hubWs.send(JSON.stringify({ type: 'leave', key: msg.key })); return; }
        info.state = 'matched';
        info.remoteMatch = true;
        info.remotePartnerKey = msg.key;
        info.partner = null;
        const lq = queues.get(info.qkey);
        if (lq) {
          const li = lq.indexOf(ws);
          if (li !== -1) lq.splice(li, 1);
        }
        if (info.token) tokenQueuing.delete(info.token);
        info.relayed = false;
        nodeStats.crossMatches++; saveNodeStatsToDisk();
        nodeStats.matchedPlayers++; saveNodeStatsToDisk();   // 这个本地玩家被跨节点配对成功
        ws.send(JSON.stringify({ type: 'matched', partner: msg.partner, crossNode: true }));
        broadcastStats();
      }
      if (msg.type === 'partner_left') {
        const ws = localByKey.get(msg.key);
        if (ws && ws.readyState === ws.OPEN) {
          const info = clients.get(ws);
          if (info) { info.state = 'idle'; info.remoteMatch = false; }
          ws.send(JSON.stringify({ type: 'partner_left' }));
        }
      }
      if (msg.type === 'banned') {
        const ws = localByKey.get(msg.key);
        if (ws && ws.readyState === ws.OPEN) {
          ws.send(JSON.stringify({ type: 'error', message: '你的身份已被多处举报，暂时无法匹配' }));
        }
        const info = ws ? clients.get(ws) : null;
        if (info) {
          removeFromQueue(ws);
          if (info.state === 'queued') info.state = 'idle';
        }
      }
      if (msg.type === 'hub_stats') {
        networkStats = { nodes: msg.nodes, total: msg.total, board: msg.board || [] };
        broadcastStats();
      }
      // 枢纽要求共同签署一条存证：先验已有的签名，再签自己的，回传
      if (msg.type === 'attest_req') {
        if (!SIGNING) return;
        if (!Array.isArray(msg.nodes) || !msg.nodes.includes(IDENT.nodeId)) return;
        const rec = { id: String(msg.id), game: String(msg.game), ts: Number(msg.ts), nodes: msg.nodes.map(String), sigs: msg.sigs || {} };
        const peers = msg.peers || {};
        // 已有签名必须全部有效，否则拒绝参与（防止被塞进伪造记录）
        for (const [nid, sig] of Object.entries(rec.sigs)) {
          if (!peers[nid] || !attest.verify(peers[nid], rec, sig)) return;
        }
        if (rec.sigs[IDENT.nodeId]) return; // 已经签过
        const sig = attest.sign(rec);
        if (hubWs.readyState === WebSocket.OPEN) {
          hubWs.send(JSON.stringify({ type: 'attest_sig', id: rec.id, nodeId: IDENT.nodeId, sig }));
        }
      }
      // 存证完成：本节点留一份，枢纽不在了也能自证
      if (msg.type === 'attest_ok') {
        if (!SIGNING) return;
        const rec = { id: msg.id, game: msg.game, ts: msg.ts, nodes: msg.nodes, sigs: msg.sigs };
        nodeStats.verified++; saveNodeStatsToDisk();
        attest.appendLocalRecord(DATA_DIR, rec);
        broadcastStats();
      }
    });
    hubWs.on('close', () => {
      hubReady = false;
      networkStats = null;
      broadcastStats();
      setTimeout(hubConnect, 5000);
    });
    hubWs.on('error', () => {});
  } catch {
    setTimeout(hubConnect, 5000);
  }
}

function relayJoin(info, ws) {
  localByKey.set(info.key, ws);
  if (!HUB_URL) {
    // 枢纽即节点：本地玩家直接进入自己的跨节点队列
    const entry = { ws, player: { nick: info.nick, game: info.game, platform: info.platform, id: info.id, chapter: info.chapter, mic: info.mic, own: info.own || 'own', key: info.key } };
    getRQueue(info.qkey).set(info.key, entry);
    keyInfo.set(info.key, { ws, qkey: info.qkey, player: entry.player, token: info.token });
    hubTryMatch(info.qkey);
    return;
  }
  if (!hubReady || !hubWs || hubWs.readyState !== WebSocket.OPEN) return;
  info.relayed = true;
  hubWs.send(JSON.stringify({ type: 'join', key: info.key, game: info.game, platform: info.platform, nick: info.nick, id: info.id, chapter: info.chapter, mic: info.mic, own: info.own || 'own', token: info.token }));
}

function relayCancel(info) {
  localByKey.delete(info.key);
  if (!HUB_URL) {
    getRQueue(info.qkey).delete(info.key);
    hubBroadcastStats();
    return;
  }
  if (!hubReady || !hubWs || hubWs.readyState !== WebSocket.OPEN) return;
  hubWs.send(JSON.stringify({ type: 'cancel', key: info.key }));
}

function relayLeave(info) {
  localByKey.delete(info.key);
  if (!HUB_URL) {
    hubRemove(info.key, true);
    return;
  }
  if (!hubReady || !hubWs || hubWs.readyState !== WebSocket.OPEN) return;
  hubWs.send(JSON.stringify({ type: 'leave', key: info.key }));
}

/* ============================================================
 * 四、本地客户端消息处理
 * ============================================================ */
// 统一路由升级请求
server.on('upgrade', (req, socket, head) => {
  let pathname = '/';
  try { pathname = new URL(req.url, 'http://localhost').pathname; } catch { socket.destroy(); return; }
  if (pathname === '/relay' && RELAY_TOKEN) {
    // 枢纽设置了令牌：节点必须带相同令牌才能接入，防陌生人冒充节点
    if (req.headers['x-relay-token'] !== RELAY_TOKEN) { socket.destroy(); return; }
  }
  if (pathname === '/ws') {
    wss.handleUpgrade(req, socket, head, ws => wss.emit('connection', ws, req));
  } else if (pathname === '/relay') {
    relayWss.handleUpgrade(req, socket, head, ws => relayWss.emit('connection', ws, req));
  } else {
    socket.destroy();
  }
});

function broadcastStats() {
  const queue = {};
  for (const [k, arr] of queues) {
    if (arr.length) queue[k] = arr.length;
  }
  const payload = JSON.stringify({
    type: 'stats',
    online: clients.size,
    queue,
    node: NODE_NAME,
    network: networkStats,
    honor: {
      startedAt: nodeStats.startedAt,
      localMatches: nodeStats.localMatches,
      crossMatches: nodeStats.crossMatches,
      matchedPlayers: nodeStats.matchedPlayers,
      verified: nodeStats.verified,
      signing: SIGNING,
      nodeId: IDENT ? IDENT.nodeId : null,
    },
  });
  for (const ws of clients.keys()) {
    if (ws.readyState === ws.OPEN) ws.send(payload);
  }
}

function removeFromQueue(ws) {
  const info = clients.get(ws);
  if (!info) return;
  if (info.state === 'queued') {
    const q = queues.get(info.qkey);
    if (q) {
      const idx = q.indexOf(ws);
      if (idx !== -1) q.splice(idx, 1);
    }
    if (info.relayed || !HUB_URL) relayCancel(info);
    info.relayed = false;
    if (info.token) tokenQueuing.delete(info.token);
  }
  broadcastStats();
}

/**
 * 从队列里挑一组能开局的玩家：
 *   - 必须凑满该游戏的开局人数；
 *   - 组里至少要有一个人「有本体」，否则两个通行证玩家谁也开不了房。
 * 全是通行证时返回 null（继续等有本体的人进来），不占用队列顺序。
 */
function pickGroup(q, size) {
  const group = [];
  for (const ws of q) {
    if (group.length >= size) break;
    if (ws.readyState === ws.OPEN) group.push(ws);
  }
  if (group.length < size) return null;
  const isOwner = ws => (clients.get(ws)?.own || 'own') === 'own';
  if (group.some(isOwner)) return group;
  // 全是通行证：从队列后面找一个有本体的人替换进来
  const ownerWs = q.find(ws => ws.readyState === ws.OPEN && !group.includes(ws) && isOwner(ws));
  if (!ownerWs) return null;
  group[group.length - 1] = ownerWs;
  return group;
}

function tryMatch(k) {
  const game = GAME_MAP.get(k.split('|')[0]);
  const q = queues.get(k);
  if (!game || !q || q.length < game.size) return;
  while (q.length >= game.size) {
    const group = pickGroup(q, game.size);
    if (!group) return;
    for (const ws of group) {
      const i = q.indexOf(ws);
      if (i !== -1) q.splice(i, 1);
    }
    nodeStats.localMatches++; saveNodeStatsToDisk();
    nodeStats.matchedPlayers += group.length; saveNodeStatsToDisk();   // 一局 N 人 → 帮到 N 位玩家
    const infos = group.map(ws => clients.get(ws));
    for (const info of infos) {
      info.state = 'matched';
      if (info.relayed || !HUB_URL) relayCancel(info);
      info.relayed = false;
      if (info.token) tokenQueuing.delete(info.token);
    }
    const seq = nodeStats.localMatches + nodeStats.crossMatches;   // 本节点累计第几局
    for (let i = 0; i < infos.length; i++) {
      const me = infos[i], partner = infos[(i + 1) % infos.length];
      me.partner = group[(i + 1) % group.length];
      me.remoteMatch = false;
      group[i].send(JSON.stringify({ type: 'matched', seq, size: group.length, partner: { ...pub(partner), key: partner.key }, self: pub(me) }));
    }
    broadcastStats();
  }
}

function pub(info) {
  return {
    nick: info.nick, game: info.game, platform: info.platform, id: info.id,
    chapter: info.chapter, mic: info.mic, own: info.own || 'own',
  };
}

// 反代后取真实 IP（nginx 的 X-Forwarded-For）
function wsIp(req) {
  const xff = req.headers['x-forwarded-for'];
  if (typeof xff === 'string' && xff.length) return xff.split(',')[0].trim();
  return (req.socket && req.socket.remoteAddress) || 'unknown';
}

// 单 IP 并发玩家连接上限（防连接洪水占满内存）
const MAX_WS_PER_IP = 8;
const wsPerIp = new Map(); // ip -> Set<ws>
setInterval(() => { // 定期清理已断开的
  for (const [ip, set] of wsPerIp) {
    for (const ws of set) if (ws.readyState !== ws.OPEN && ws.readyState !== ws.CONNECTING) set.delete(ws);
    if (!set.size) wsPerIp.delete(ip);
  }
}, 5 * 60 * 1000).unref();

wss.on('connection', (ws, req) => {
  ws.isLocalClient = true;
  const ip = wsIp(req);
  const set = wsPerIp.get(ip) || new Set();
  if (set.size >= MAX_WS_PER_IP) { ws.close(1008, 'too many connections'); return; }
  set.add(ws);
  wsPerIp.set(ip, set);
  ws._ip = ip;
  clients.set(ws, { state: 'idle', key: crypto.randomUUID() });
  broadcastStats();

  // 每连接消息频率限制：10 秒内超过 40 条直接断开（防消息洪水）
  let msgTimes = [];
  function floodCheck() {
    const now = Date.now();
    msgTimes = msgTimes.filter(t => now - t < 10 * 1000);
    msgTimes.push(now);
    return msgTimes.length > 40;
  }

  ws.on('message', (raw) => {
    if (floodCheck()) { try { ws.close(1008, 'flood'); } catch {} return; }
    let msg;
    try { msg = JSON.parse(raw); } catch { return; }
    const info = clients.get(ws);
    if (!info) return;

    if (msg.type === 'join') {
      const game = GAME_MAP.get(msg.game);
      const platform = ALL_PLATFORMS.includes(msg.platform) ? msg.platform : null;
      if (!game) { ws.send(JSON.stringify({ type: 'error', message: '未收录的游戏' })); return; }
      if (!platform || !game.platforms.includes(platform)) { ws.send(JSON.stringify({ type: 'error', message: '该游戏不支持此平台联机' })); return; }

      const token = String(msg.token || '').slice(0, 64);
      touchToken(token); // 记录身份首见时间（举报防小号用）
      if (!token) { ws.send(JSON.stringify({ type: 'error', message: '身份校验失败，请刷新页面' })); return; }
      if (isBanned(token)) { ws.send(JSON.stringify({ type: 'error', message: '你的身份已被多次举报，暂时无法匹配' })); return; }

      const now = Date.now();
      const arr = (joinLog.get(ip) || []).filter(t => now - t < 5 * 60 * 1000);
      arr.push(now);
      joinLog.set(ip, arr);
      if (arr.length > 12) { ws.send(JSON.stringify({ type: 'error', message: '操作太频繁，请稍后再试' })); return; }

      const occupant = tokenQueuing.get(token);
      if (occupant && occupant !== ws && occupant.readyState === occupant.OPEN) {
        ws.send(JSON.stringify({ type: 'error', message: '这个身份已经在别的页面排队啦' }));
        return;
      }

      const nick = sanitize(msg.nick).slice(0, 12) || '玩家';
      const id = sanitize(msg.id).slice(0, 40);
      if (!id) { ws.send(JSON.stringify({ type: 'error', message: '好友 ID 不能为空或包含链接' })); return; }
      const hitNick = findSensitive(nick);
      if (hitNick) { ws.send(JSON.stringify({ type: 'error', message: '昵称包含敏感内容，请修改后再排队' })); return; }
      const hitId = findSensitive(id);
      if (hitId) { ws.send(JSON.stringify({ type: 'error', message: '好友 ID 包含敏感内容，请修改后再排队' })); return; }
      const hitChapter = findSensitive(sanitize(msg.chapter).slice(0, 20));
      if (hitChapter) { ws.send(JSON.stringify({ type: 'error', message: '进度包含敏感内容，请修改后再排队' })); return; }

      removeFromQueue(ws);
      info.state = 'queued';
      info.game = game.id;
      info.platform = platform;
      info.qkey = qkey(game.id, platform);
      info.nick = nick;
      info.id = id;
      info.chapter = sanitize(msg.chapter).slice(0, 20);
      info.mic = !!msg.mic;
      // 是否有游戏本体：own = 有本体，pass = 只有好友通行证
      // 两个人都只有通行证是开不了局的，所以配对时必须至少有一个本体
      info.own = msg.own === 'pass' ? 'pass' : 'own';
      info.token = token;
      tokenQueuing.set(token, ws);
      if (!queues.has(info.qkey)) queues.set(info.qkey, []);
      queues.get(info.qkey).push(ws);
      ws.send(JSON.stringify({ type: 'queued', game: game.id, platform }));
      tryMatch(info.qkey);
      const cur = clients.get(ws);
      if (cur && cur.state === 'queued' && !cur.relayed) relayJoin(cur, ws);
      broadcastStats();
    }

    if (msg.type === 'cancel') {
      if (info.remoteMatch) relayLeave(info);
      removeFromQueue(ws);
      if (info.state === 'queued') info.state = 'idle';
      ws.send(JSON.stringify({ type: 'cancelled' }));
    }

    if (msg.type === 'report' && msg.key) {
      const partnerKey = String(msg.key);
      const partnerWs = info.partner;
      const pi = partnerWs ? clients.get(partnerWs) : null;
      if (pi && pi.token && pi.key === partnerKey) {
        strikeToken(pi.token);
      } else if (!HUB_URL) {
        const ki = keyInfo.get(partnerKey);
        if (ki && ki.token) {
          const n = (hubStrikes.get(ki.token) || 0) + 1;
          hubStrikes.set(ki.token, n);
          if (n >= REPORTS_TO_BAN) hubRemove(partnerKey, false);
        }
      } else if (info.remoteMatch) {
        if (hubReady && hubWs && hubWs.readyState === WebSocket.OPEN) {
          hubWs.send(JSON.stringify({ type: 'report', key: partnerKey }));
        }
      }
      ws.send(JSON.stringify({ type: 'reported' }));
    }

    if (msg.type === 'leave_pair') {
      if (info.remoteMatch) relayLeave(info);
      if (info.partner && clients.has(info.partner)) {
        const partnerWs = info.partner;
        const pi = clients.get(partnerWs);
        partnerWs.send(JSON.stringify({ type: 'partner_left' }));
        pi.state = 'idle';
        pi.partner = null;
      }
      info.state = 'idle';
      info.partner = null;
      info.remoteMatch = false;
    }
  });

  ws.on('close', () => {
    const info = clients.get(ws);
    if (info) {
      if (info.remoteMatch) relayLeave(info);
      removeFromQueue(ws);
      if (info.partner && clients.has(info.partner)) {
        const partnerWs = info.partner;
        const pi = clients.get(partnerWs);
        if (pi) {
          pi.state = 'idle';
          pi.partner = null;
          try { partnerWs.send(JSON.stringify({ type: 'partner_left' })); } catch {}
        }
      }
      localByKey.delete(info.key);
      if (info.token && tokenQueuing.get(info.token) === ws) tokenQueuing.delete(info.token);
      clients.delete(ws);
    }
    broadcastStats();
  });

  ws.on('error', () => {});
});

if (HUB_URL) hubConnect();

server.listen(PORT, BIND, () => {
  console.log(`游戏搭子匹配节点已启动: http://localhost:${PORT} (监听 ${BIND})`);
  console.log(`节点名称: ${NODE_NAME} | 收录游戏: ${GAMES.length} 款`);
  console.log(HUB_URL ? `联邦模式: 已指向枢纽 ${HUB_URL}` : '独立模式: 未接入联邦（设置 HUB_URL 可加入网络）');
  if (SIGNING) {
    console.log(`存证模式: 已启用 | 节点身份 ${IDENT.nodeId}`);
    console.log(`          私钥文件 ${path.join(DATA_DIR, 'node-key.json')}（勿公开）`);
  } else {
    console.log('存证模式: 未启用（SIGNING=1 可开启节点签名与 Merkle 存证）');
  }
});
