/**
 * 电商运营主管工作台 —— 云同步后端（零依赖 Node.js 版）
 *
 * 运行方式：
 *   node server.js                 # 默认端口 3001
 *   PORT=8080 node server.js       # 自定义端口
 *
 * 端点：
 *   POST /init        幂等初始化文档（不存在则创建，已存在则校验写令牌）
 *   POST /push        增量写入（写令牌 + 乐观锁 base 版本号，冲突返回 snapshot）
 *   GET  /pull        读取快照（只读令牌或写令牌均可）
 *   POST /share       主设备签发只读分享链接
 *   GET  /subscribe   SSE 长连接：文档版本变化时实时推送 {"version": n}
 *   GET  /health      健康检查
 *
 * 数据存储：同目录下 data.json 文件（自动创建），重启不丢数据。
 */

const http = require('http');
const fs = require('fs');
const path = require('path');

const PORT = process.env.PORT || 3001;
const DATA_FILE = path.join(__dirname, 'data.json');
const CORS = {
  'Access-Control-Allow-Origin': '*',
  'Access-Control-Allow-Methods': 'GET,POST,OPTIONS',
  'Access-Control-Allow-Headers': 'Content-Type,Access-Control-Request-Private-Network',
  'Access-Control-Allow-Private-Network': 'true',
  'Vary': 'Origin',
  'Cache-Control': 'no-store'
};

// ---------- 持久化存储 ----------
function loadStore() {
  try {
    return JSON.parse(fs.readFileSync(DATA_FILE, 'utf8'));
  } catch (_) {
    return {}; // { "doc:default": {version, items, writeToken, readTokens} }
  }
}
function saveStore(store) {
  fs.writeFileSync(DATA_FILE, JSON.stringify(store, null, 2));
}
function loadDoc(docId) {
  if (!docId) return null;
  const store = loadStore();
  return store['doc:' + docId] || null;
}
function saveDoc(docId, doc) {
  const store = loadStore();
  store['doc:' + docId] = doc;
  saveStore(store);
}
function authorized(doc, token) {
  if (!token || !doc) return false;
  if (token === doc.writeToken) return true;
  return (doc.readTokens || []).some(r => r.token === token);
}
function snapshot(doc) { return { version: doc.version, items: doc.items }; }

// ---------- SSE 广播中心 ----------
const subscribers = new Map(); // docId -> Set<res>

function broadcast(docId, version) {
  const subs = subscribers.get(docId);
  if (!subs || subs.size === 0) return;
  const msg = 'event: version\ndata: ' + JSON.stringify({ doc: docId, version }) + '\n\n';
  for (const res of subs) {
    try { res.write(msg); } catch (_) { subs.delete(res); }
  }
}

// ---------- HTTP 响应工具 ----------
function json(res, data, status = 200) {
  res.writeHead(status, { 'Content-Type': 'application/json', ...CORS });
  res.end(JSON.stringify(data));
}
function readBody(req) {
  return new Promise(resolve => {
    let buf = '';
    req.on('data', c => buf += c);
    req.on('end', () => {
      try { resolve(JSON.parse(buf || '{}')); }
      catch (_) { resolve({}); }
    });
  });
}

// ---------- 路由 ----------
const server = http.createServer(async (req, res) => {
  // CORS 预检
  if (req.method === 'OPTIONS') {
    res.writeHead(204, CORS); res.end(); return;
  }

  const url = new URL(req.url, 'http://localhost');
  const p = url.pathname;

  // ---------- GET /health ----------
  if (p === '/health') { json(res, { ok: true, time: new Date().toISOString() }); return; }

  // ---------- POST /init ----------
  if (p.endsWith('/init') && req.method === 'POST') {
    try {
      const body = await readBody(req);
      if (!body.doc || !body.token) { json(res, { ok: false, error: '缺少 doc 或 token' }, 400); return; }
      let doc = loadDoc(body.doc);
      let created = false;
      if (!doc) {
        doc = { version: 0, items: {}, writeToken: body.token, readTokens: [] };
        created = true;
      } else if (body.token !== doc.writeToken) {
        json(res, { ok: false, error: '令牌与文档不匹配' }, 403); return;
      }
      saveDoc(body.doc, doc);
      json(res, { ok: true, created, version: doc.version });
    } catch (e) { json(res, { ok: false, error: e.message }, 500); }
    return;
  }

  // ---------- POST /push ----------
  if (p.endsWith('/push') && req.method === 'POST') {
    try {
      const body = await readBody(req);
      let doc = loadDoc(body.doc);
      if (!doc) {
        // 兜底：文档未初始化时，首次 push 自动创建
        if (!body.token) { json(res, { ok: false, error: '文档不存在，请先调用 /init 初始化' }, 404); return; }
        doc = { version: 0, items: {}, writeToken: body.token, readTokens: [] };
      }
      if (body.token !== doc.writeToken) { json(res, { ok: false, error: '无写权限' }, 403); return; }
      if (body.base !== doc.version) { json(res, { ok: false, conflict: true, snapshot: snapshot(doc) }); return; }

      let opsCount = 0;
      for (const col of Object.keys(body.batch || {})) {
        doc.items[col] = doc.items[col] || {};
        for (const op of body.batch[col]) {
          if (op.t === 'set') doc.items[col][op.id] = { value: op.value, ts: op.ts || Date.now() };
          else if (op.t === 'del') delete doc.items[col][op.id];
          opsCount++;
        }
      }
      doc.version += opsCount;
      saveDoc(body.doc, doc);
      broadcast(body.doc, doc.version);
      json(res, { ok: true, version: doc.version });
    } catch (e) { json(res, { ok: false, error: e.message }, 500); }
    return;
  }

  // ---------- GET /pull ----------
  if (p.endsWith('/pull')) {
    const doc = loadDoc(url.searchParams.get('doc'));
    if (!doc) { json(res, { ok: false, error: '文档不存在' }, 404); return; }
    if (!authorized(doc, url.searchParams.get('token'))) { json(res, { ok: false, error: '无权限' }, 403); return; }
    json(res, { ok: true, version: doc.version, items: doc.items });
    return;
  }

  // ---------- POST /share ----------
  if (p.endsWith('/share') && req.method === 'POST') {
    try {
      const body = await readBody(req);
      const doc = loadDoc(body.doc);
      if (!doc) { json(res, { ok: false, error: '文档不存在' }, 404); return; }
      if (body.token !== doc.writeToken) { json(res, { ok: false, error: '无权限' }, 403); return; }
      const readToken = 'r_' + Math.random().toString(36).slice(2, 10) + Date.now().toString(36);
      doc.readTokens = doc.readTokens || [];
      doc.readTokens.push({ token: readToken, createdAt: Date.now() });
      saveDoc(body.doc, doc);
      const base = process.env.WORKBENCH_URL || '';
      const shareUrl = base + '?doc=' + encodeURIComponent(body.doc) + '&share=' + readToken;
      json(res, { ok: true, shareUrl, token: readToken });
    } catch (e) { json(res, { ok: false, error: e.message }, 500); }
    return;
  }

  // ---------- GET /subscribe（SSE） ----------
  if (p.endsWith('/subscribe')) {
    const docId = url.searchParams.get('doc');
    const token = url.searchParams.get('token');
    const doc = loadDoc(docId);
    if (!doc || !authorized(doc, token)) { json(res, { ok: false, error: '无权限' }, 403); return; }

    res.writeHead(200, {
      'Content-Type': 'text/event-stream',
      'Cache-Control': 'no-store',
      'Connection': 'keep-alive',
      ...CORS
    });
    res.write('retry: 3000\n\n');
    res.write('event: version\ndata: ' + JSON.stringify({ doc: docId, version: doc.version }) + '\n\n');

    if (!subscribers.has(docId)) subscribers.set(docId, new Set());
    subscribers.get(docId).add(res);

    // 心跳保活（25s）
    const heartbeat = setInterval(() => {
      try { res.write(': hb\n\n'); } catch (_) { clearInterval(heartbeat); }
    }, 25000);

    req.on('close', () => {
      clearInterval(heartbeat);
      const subs = subscribers.get(docId);
      if (subs) { subs.delete(res); if (subs.size === 0) subscribers.delete(docId); }
    });
    return;
  }

  json(res, { ok: false, error: '未找到路由: ' + p }, 404);
});

server.listen(PORT, () => {
  console.log('=== 电商运营主管工作台 · 云同步后端 ===');
  console.log('监听端口: ' + PORT);
  console.log('数据文件: ' + DATA_FILE);
  console.log('端点:');
  console.log('  POST /init        初始化文档');
  console.log('  POST /push        增量写入');
  console.log('  GET  /pull        读取快照');
  console.log('  POST /share       签发分享链接');
  console.log('  GET  /subscribe   SSE 实时推送');
  console.log('  GET  /health      健康检查');
  console.log('======================================');
  console.log('就绪。前端配置示例:');
  console.log("  localStorage.setItem('wb_sync_endpoint','http://localhost:" + PORT + "')");
  console.log("  localStorage.setItem('wb_sync_write_token','your-secret-token')");
});
