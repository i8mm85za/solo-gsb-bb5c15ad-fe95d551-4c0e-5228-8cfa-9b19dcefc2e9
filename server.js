'use strict';

/**
 * 发言席位（Speak Seat）
 * 线下讨论投屏用的多人发言席位服务：Express 静态托管 + WebSocket 实时同步。
 *
 * 并发模型：所有队列命令都携带客户端所见版本 baseVersion，
 * 服务端在单线程事件循环内「校验版本 -> 应用变更 -> 递增版本 -> 广播快照」，
 * 该序列天然原子（无中断点）；版本不匹配则拒绝并回发最新快照（stale）。
 */

const path = require('path');
const http = require('http');
const crypto = require('crypto');
const express = require('express');
const { WebSocketServer } = require('ws');

// ---------- 配置项（均可通过环境变量覆盖） ----------
const PORT = parseInt(process.env.PORT || '3000', 10);
const ROOM_IDLE_TTL_MINUTES = parseInt(process.env.ROOM_IDLE_TTL_MINUTES || '120', 10);
const MAX_LOG_ENTRIES = parseInt(process.env.MAX_LOG_ENTRIES || '200', 10);
const LOG_PAGE = 50; // 每份快照携带的最近操作记录条数
const MAX_NAME_LEN = 24;
const CODE_ALPHABET = 'ABCDEFGHJKLMNPQRSTUVWXYZ23456789'; // 去掉易混淆的 0/O/1/I

// ---------- 数据模型 ----------
/**
 * Room = {
 *   code: string,            // 房间码
 *   hostToken: string,       // 主持人密钥（仅创建者持有，不广播）
 *   hostId: string,          // 主持人参与者 id（公开，用于界面标记）
 *   version: number,         // 单调递增的状态版本
 *   createdAt/lastActiveAt: number,
 *   participants: Map<id, {id, name, connected, joinedAt}>,
 *   queue: string[],         // 发言队列（参与者 id 有序数组）
 *   current: {participantId, startedAt} | null,  // 当前发言
 *   log: [{t, text}],        // 操作记录
 *   sockets: Set<ws>         // 房间内所有连接
 * }
 */
const rooms = new Map();

function genCode() {
  let code;
  do {
    code = Array.from({ length: 6 }, () => CODE_ALPHABET[crypto.randomInt(CODE_ALPHABET.length)]).join('');
  } while (rooms.has(code));
  return code;
}

function cleanName(name) {
  if (typeof name !== 'string') return '';
  return name.trim().slice(0, MAX_NAME_LEN);
}

function nameOf(room, id) {
  const p = room.participants.get(id);
  return p ? p.name : '未知成员';
}

function addLog(room, text) {
  room.log.push({ t: Date.now(), text });
  if (room.log.length > MAX_LOG_ENTRIES) {
    room.log.splice(0, room.log.length - MAX_LOG_ENTRIES);
  }
}

function snapshot(room) {
  return {
    code: room.code,
    version: room.version,
    serverNow: Date.now(), // 客户端用它对齐时钟，恢复发言计时
    hostId: room.hostId,
    participants: [...room.participants.values()].map((p) => ({
      id: p.id,
      name: p.name,
      connected: p.connected,
    })),
    queue: room.queue.slice(),
    current: room.current
      ? { participantId: room.current.participantId, startedAt: room.current.startedAt }
      : null,
    log: room.log.slice(-LOG_PAGE),
  };
}

function send(ws, obj) {
  if (ws.readyState === ws.OPEN) ws.send(JSON.stringify(obj));
}

function sendError(ws, message, code) {
  send(ws, { type: 'error', message, code });
}

function broadcast(room) {
  const msg = JSON.stringify({ type: 'snapshot', ...snapshot(room) });
  for (const ws of room.sockets) {
    if (ws.readyState === ws.OPEN) ws.send(msg);
  }
}

/** 提交一次已接受的变更：递增版本、记录日志、广播快照。 */
function commit(room, logText) {
  room.version += 1;
  room.lastActiveAt = Date.now();
  if (logText) addLog(room, logText);
  broadcast(room);
}

/**
 * 原子校验命令版本：与当前版本不一致时拒绝并回发最新快照。
 * 校验与随后的变更在同一事件循环 tick 内完成，不会被其他命令插入。
 */
function checkVersion(room, msg, ws) {
  if (!Number.isInteger(msg.baseVersion) || msg.baseVersion !== room.version) {
    send(ws, { type: 'stale', ...snapshot(room) });
    return false;
  }
  return true;
}

function requireHost(room, msg, ws) {
  if (typeof msg.hostToken !== 'string' || msg.hostToken !== room.hostToken) {
    sendError(ws, '需要主持人权限');
    return false;
  }
  return true;
}

function requireContext(ws) {
  const room = ws._room;
  const participantId = ws._participantId;
  if (!room || !participantId || !room.participants.has(participantId)) {
    sendError(ws, '尚未加入房间');
    return null;
  }
  return { room, participantId };
}

function attach(ws, room, participantId) {
  ws._room = room;
  ws._participantId = participantId;
  room.sockets.add(ws);
  const p = room.participants.get(participantId);
  if (p) p.connected = true;
}

function detach(ws) {
  const room = ws._room;
  if (!room) return;
  room.sockets.delete(ws);
  room.lastActiveAt = Date.now();
  const p = room.participants.get(ws._participantId);
  ws._room = null;
  ws._participantId = null;
  // 同一参与者可能有多个连接，全部断开才标记离线
  if (p && ![...room.sockets].some((s) => s._participantId === p.id)) {
    p.connected = false;
    commit(room, null);
  }
}

function fmtDurationCn(ms) {
  const s = Math.max(0, Math.round(ms / 1000));
  const h = Math.floor(s / 3600);
  const m = Math.floor((s % 3600) / 60);
  const sec = s % 60;
  if (h > 0) return `${h} 小时 ${m} 分 ${sec} 秒`;
  if (m > 0) return `${m} 分 ${sec} 秒`;
  return `${sec} 秒`;
}

// ---------- 消息处理 ----------
function handleMessage(ws, msg) {
  switch (msg.type) {
    case 'create': {
      const name = cleanName(msg.name);
      if (!name) return sendError(ws, '请输入有效的昵称');
      const code = genCode();
      const hostToken = crypto.randomBytes(16).toString('hex');
      const hostId = crypto.randomUUID();
      const room = {
        code,
        hostToken,
        hostId,
        version: 0,
        createdAt: Date.now(),
        lastActiveAt: Date.now(),
        participants: new Map(),
        queue: [],
        current: null,
        log: [],
        sockets: new Set(),
      };
      room.participants.set(hostId, { id: hostId, name, connected: false, joinedAt: Date.now() });
      rooms.set(code, room);
      attach(ws, room, hostId);
      send(ws, { type: 'created', code, participantId: hostId, hostToken });
      commit(room, `房间已创建，${name} 成为主持人`);
      break;
    }

    case 'join': {
      const room = rooms.get(String(msg.code || '').toUpperCase().trim());
      if (!room) return sendError(ws, '房间不存在或已关闭');
      const name = cleanName(msg.name);
      if (!name) return sendError(ws, '请输入有效的昵称');
      const id = crypto.randomUUID();
      room.participants.set(id, { id, name, connected: false, joinedAt: Date.now() });
      attach(ws, room, id);
      send(ws, { type: 'joined', code: room.code, participantId: id });
      commit(room, `${name} 加入了房间`);
      break;
    }

    case 'rejoin': {
      const room = rooms.get(String(msg.code || '').toUpperCase().trim());
      if (!room) return sendError(ws, '房间不存在或已关闭', 'session_expired');
      const p = room.participants.get(msg.participantId);
      if (!p) return sendError(ws, '身份已失效，请重新加入', 'session_expired');
      attach(ws, room, p.id);
      const isHost = typeof msg.hostToken === 'string' && msg.hostToken === room.hostToken;
      send(ws, { type: 'rejoined', code: room.code, participantId: p.id, isHost });
      commit(room, null); // 在线状态变化，广播快照
      break;
    }

    case 'leave': {
      const ctx = requireContext(ws);
      if (!ctx) return;
      const { room, participantId } = ctx;
      const name = nameOf(room, participantId);
      const idx = room.queue.indexOf(participantId);
      if (idx !== -1) room.queue.splice(idx, 1);
      if (room.current && room.current.participantId === participantId) {
        const dur = Date.now() - room.current.startedAt;
        room.current = null;
        addLog(room, `${name} 发言结束（用时 ${fmtDurationCn(dur)}）`);
      }
      room.participants.delete(participantId);
      room.sockets.delete(ws);
      ws._room = null;
      ws._participantId = null;
      commit(room, `${name} 离开了房间`);
      break;
    }

    // ---- 以下为队列命令：一律先原子校验 baseVersion ----

    case 'request': {
      const ctx = requireContext(ws);
      if (!ctx) return;
      const { room, participantId } = ctx;
      if (!checkVersion(room, msg, ws)) return;
      if (room.queue.includes(participantId)) return sendError(ws, '你已在发言队列中');
      if (room.current && room.current.participantId === participantId) {
        return sendError(ws, '你正在发言');
      }
      room.queue.push(participantId);
      commit(room, `${nameOf(room, participantId)} 申请发言（第 ${room.queue.length} 位）`);
      break;
    }

    case 'cancel': {
      const ctx = requireContext(ws);
      if (!ctx) return;
      const { room, participantId } = ctx;
      if (!checkVersion(room, msg, ws)) return;
      const idx = room.queue.indexOf(participantId);
      if (idx === -1) return sendError(ws, '你不在发言队列中');
      room.queue.splice(idx, 1);
      commit(room, `${nameOf(room, participantId)} 取消了发言申请`);
      break;
    }

    case 'reorder': {
      const ctx = requireContext(ws);
      if (!ctx) return;
      const { room } = ctx;
      if (!requireHost(room, msg, ws)) return;
      if (!checkVersion(room, msg, ws)) return;
      const order = msg.order;
      const valid =
        Array.isArray(order) &&
        order.length === room.queue.length &&
        new Set(order).size === order.length &&
        order.every((id) => typeof id === 'string' && room.queue.includes(id));
      if (!valid) {
        // 队列结构在拖拽期间已变化，按过期处理，回发最新快照
        return send(ws, { type: 'stale', ...snapshot(room) });
      }
      room.queue = order.slice();
      commit(room, '主持人调整了发言顺序');
      break;
    }

    case 'start': {
      const ctx = requireContext(ws);
      if (!ctx) return;
      const { room } = ctx;
      if (!requireHost(room, msg, ws)) return;
      if (!checkVersion(room, msg, ws)) return;
      if (room.current) return sendError(ws, '当前有正在进行的发言，请先结束');
      const id = typeof msg.participantId === 'string' ? msg.participantId : room.queue[0];
      const idx = room.queue.indexOf(id);
      if (idx === -1) return sendError(ws, '该成员不在发言队列中');
      room.queue.splice(idx, 1);
      room.current = { participantId: id, startedAt: Date.now() };
      commit(room, `${nameOf(room, id)} 开始发言`);
      break;
    }

    case 'end': {
      const ctx = requireContext(ws);
      if (!ctx) return;
      const { room } = ctx;
      if (!requireHost(room, msg, ws)) return;
      if (!checkVersion(room, msg, ws)) return;
      if (!room.current) return sendError(ws, '当前没有发言者');
      const dur = Date.now() - room.current.startedAt;
      const name = nameOf(room, room.current.participantId);
      room.current = null;
      commit(room, `${name} 发言结束（用时 ${fmtDurationCn(dur)}）`);
      break;
    }

    case 'remove': {
      const ctx = requireContext(ws);
      if (!ctx) return;
      const { room } = ctx;
      if (!requireHost(room, msg, ws)) return;
      if (!checkVersion(room, msg, ws)) return;
      const idx = room.queue.indexOf(msg.participantId);
      if (idx === -1) return sendError(ws, '该成员不在发言队列中');
      room.queue.splice(idx, 1);
      commit(room, `主持人将 ${nameOf(room, msg.participantId)} 移出队列`);
      break;
    }

    case 'clear': {
      const ctx = requireContext(ws);
      if (!ctx) return;
      const { room } = ctx;
      if (!requireHost(room, msg, ws)) return;
      if (!checkVersion(room, msg, ws)) return;
      room.queue = [];
      room.current = null;
      commit(room, '主持人清空了发言队列');
      break;
    }

    default:
      sendError(ws, '未知命令');
  }
}

// ---------- HTTP / WebSocket 服务 ----------
const app = express();
app.use(express.static(path.join(__dirname, 'public')));
app.get('/healthz', (req, res) => res.json({ ok: true, rooms: rooms.size }));

const server = http.createServer(app);
const wss = new WebSocketServer({ server, maxPayload: 64 * 1024 });

wss.on('connection', (ws) => {
  ws.isAlive = true;
  ws.on('pong', () => {
    ws.isAlive = true;
  });
  ws.on('message', (data) => {
    let msg;
    try {
      msg = JSON.parse(data);
    } catch {
      return sendError(ws, '无法解析的消息');
    }
    try {
      handleMessage(ws, msg);
    } catch (err) {
      console.error('[handleMessage]', err);
      sendError(ws, '服务器内部错误');
    }
  });
  ws.on('close', () => detach(ws));
  ws.on('error', () => {});
});

// 心跳：清理静默断开的连接
const heartbeat = setInterval(() => {
  for (const ws of wss.clients) {
    if (ws.isAlive === false) {
      detach(ws);
      ws.terminate();
      continue;
    }
    ws.isAlive = false;
    ws.ping();
  }
}, 30000);
wss.on('close', () => clearInterval(heartbeat));

// 房间回收：无任何连接且空闲超过 TTL 的房间将被删除
const sweeper = setInterval(() => {
  const now = Date.now();
  for (const [code, room] of rooms) {
    const hasOpenSocket = [...room.sockets].some((s) => s.readyState === s.OPEN);
    if (!hasOpenSocket && now - room.lastActiveAt > ROOM_IDLE_TTL_MINUTES * 60 * 1000) {
      rooms.delete(code);
      console.log(`[cleanup] 房间 ${code} 已回收`);
    }
  }
}, 60 * 1000);
sweeper.unref();

server.listen(PORT, () => {
  console.log(`发言席位已启动: http://localhost:${PORT}`);
});
