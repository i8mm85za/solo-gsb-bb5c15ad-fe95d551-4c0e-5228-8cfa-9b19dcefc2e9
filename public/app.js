'use strict';

/* 发言席位前端：WebSocket 同步 + 版本化命令 + 拖拽排序 + 发言计时 */

const $ = (sel) => document.querySelector(sel);

let ws = null;
let state = null;        // 最新快照
let serverOffset = 0;    // serverNow - 本地时间，用于恢复发言计时
let myId = null;
let isHost = false;
let reconnectTimer = null;
let toastTimer = null;
// 申请/取消发言的待处理意图：版本过期时按最新快照自动重试（这两种操作与版本无关，重试安全）
let pendingIntent = null;
let intentRetries = 0;

const SESSION_KEY = 'speak-seat-session';

function loadSession() {
  try {
    return JSON.parse(localStorage.getItem(SESSION_KEY));
  } catch {
    return null;
  }
}
let session = loadSession();

function saveSession(s) {
  session = s;
  localStorage.setItem(SESSION_KEY, JSON.stringify(s));
}
function clearSession() {
  session = null;
  localStorage.removeItem(SESSION_KEY);
}

// ---------- 连接 ----------

function connect() {
  clearTimeout(reconnectTimer);
  ws = new WebSocket(`${location.protocol === 'https:' ? 'wss' : 'ws'}://${location.host}`);

  ws.onopen = () => {
    renderConn(true);
    // 断线重连：用本地会话恢复身份，服务端会回发最新快照（含顺序与计时）
    if (session) {
      send({
        type: 'rejoin',
        code: session.code,
        participantId: session.participantId,
        hostToken: session.hostToken,
      });
    }
  };

  ws.onmessage = (ev) => {
    let msg;
    try {
      msg = JSON.parse(ev.data);
    } catch {
      return;
    }
    handleMessage(msg);
  };

  ws.onclose = () => {
    renderConn(false);
    reconnectTimer = setTimeout(connect, 800 + Math.random() * 800);
  };
}

function send(obj) {
  if (ws && ws.readyState === WebSocket.OPEN) ws.send(JSON.stringify(obj));
}

/** 队列命令统一入口：自动携带客户端所见版本 */
function cmd(type, extra = {}) {
  if (!state) return;
  send({ type, baseVersion: state.version, ...extra });
}

function whenOpen(fn) {
  if (ws && ws.readyState === WebSocket.OPEN) return fn();
  toast('正在连接服务器，请稍候…');
  const timer = setInterval(() => {
    if (ws && ws.readyState === WebSocket.OPEN) {
      clearInterval(timer);
      fn();
    }
  }, 200);
  setTimeout(() => clearInterval(timer), 5000);
}

// ---------- 消息处理 ----------

function handleMessage(msg) {
  switch (msg.type) {
    case 'created':
      myId = msg.participantId;
      isHost = true;
      saveSession({ code: msg.code, participantId: msg.participantId, hostToken: msg.hostToken });
      showScreen('room');
      break;

    case 'joined':
      myId = msg.participantId;
      isHost = false;
      saveSession({ code: msg.code, participantId: msg.participantId });
      showScreen('room');
      break;

    case 'rejoined':
      myId = msg.participantId;
      isHost = !!msg.isHost;
      showScreen('room');
      break;

    case 'snapshot':
      pendingIntent = null;
      applySnapshot(msg);
      break;

    case 'stale':
      // 命令因版本过期被拒绝：以最新快照为准重新渲染
      applySnapshot(msg);
      if (!retryIntent()) toast('队列已有更新，已同步最新状态');
      break;

    case 'error':
      toast(msg.message || '出错了');
      if (msg.code === 'session_expired') {
        clearSession();
        state = null;
        showScreen('home');
      }
      break;
  }
}

function applySnapshot(msg) {
  state = msg;
  serverOffset = msg.serverNow - Date.now();
  render();
}

/** 申请/取消发言被 stale 拒绝时，若意图仍未达成则用最新版本自动重试一次 */
function retryIntent() {
  if (!pendingIntent || !state || intentRetries >= 3) return false;
  const inQueue = state.queue.includes(myId);
  const isCurrent = state.current && state.current.participantId === myId;
  const intent = pendingIntent;
  if (intent === 'request' && !inQueue && !isCurrent) {
    intentRetries++;
    cmd('request');
    return true;
  }
  if (intent === 'cancel' && inQueue) {
    intentRetries++;
    cmd('cancel');
    return true;
  }
  pendingIntent = null;
  return false;
}

function sendIntent(type) {
  pendingIntent = type;
  intentRetries = 0;
  cmd(type);
}

// ---------- 渲染 ----------

function showScreen(name) {
  $('#screen-home').hidden = name !== 'home';
  $('#screen-room').hidden = name !== 'room';
}

function renderConn(on) {
  const el = $('#conn');
  el.textContent = on ? '已连接' : '连接断开，重连中…';
  el.className = `conn ${on ? 'conn-on' : 'conn-off'}`;
}

function esc(s) {
  return String(s).replace(/[&<>"']/g, (c) => ({
    '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;', "'": '&#39;',
  }[c]));
}

function participant(id) {
  return state.participants.find((p) => p.id === id);
}

function render() {
  if (!state) return;
  $('#room-code').textContent = state.code;
  $('#room-version').textContent = `v${state.version}`;
  $('#room-count').textContent = `${state.participants.length} 人`;
  document.title = `发言席位 · ${state.code}`;
  renderCurrent();
  renderQueue();
  renderMyControls();
  renderHostControls();
  renderParticipants();
  renderLog();
}

function renderCurrent() {
  const panel = $('#current-panel');
  const cur = state.current;
  panel.classList.toggle('current-idle', !cur);
  $('#current-name').textContent = cur ? (participant(cur.participantId)?.name || '未知成员') : '暂无';
  if (!cur) $('#current-timer').textContent = '00:00';
  // 有发言时由 tick() 每 250ms 刷新计时
}

function renderQueue() {
  const ol = $('#queue');
  ol.innerHTML = '';
  $('#queue-count').textContent = state.queue.length ? `（${state.queue.length} 人等待）` : '';
  $('#queue-empty').hidden = state.queue.length > 0;

  state.queue.forEach((pid, i) => {
    const p = participant(pid);
    const li = document.createElement('li');
    li.className = 'q-item';
    li.dataset.pid = pid;

    let html = `<span class="pos">${i + 1}</span><span class="name">${esc(p ? p.name : '?')}</span>`;
    if (p && !p.connected) html += '<span class="badge">离线</span>';
    if (isHost) html += '<span class="grip" title="拖动排序">⠿</span>';
    li.innerHTML = html;

    if (isHost) {
      const startBtn = document.createElement('button');
      startBtn.className = 'item-btn start';
      startBtn.textContent = '开始';
      startBtn.disabled = !!state.current;
      startBtn.onclick = () => cmd('start', { participantId: pid });
      li.appendChild(startBtn);

      const rmBtn = document.createElement('button');
      rmBtn.className = 'item-btn remove';
      rmBtn.textContent = '移出';
      rmBtn.onclick = () => cmd('remove', { participantId: pid });
      li.appendChild(rmBtn);

      li.draggable = true;
      li.addEventListener('dragstart', (e) => {
        e.dataTransfer.effectAllowed = 'move';
        li.classList.add('dragging');
      });
      li.addEventListener('dragend', () => {
        li.classList.remove('dragging');
        ol.querySelectorAll('.drag-over').forEach((x) => x.classList.remove('drag-over'));
        submitOrder();
      });
    }
    ol.appendChild(li);
  });
}

/** 拖拽过程中实时移动 DOM，松手时读取顺序提交 */
$('#queue').addEventListener('dragover', (e) => {
  if (!isHost) return;
  e.preventDefault();
  const ol = $('#queue');
  const dragging = ol.querySelector('.dragging');
  if (!dragging) return;
  const after = [...ol.querySelectorAll('.q-item:not(.dragging)')].reduce(
    (closest, el) => {
      const box = el.getBoundingClientRect();
      const offset = e.clientY - box.top - box.height / 2;
      return offset < 0 && offset > closest.offset ? { offset, el } : closest;
    },
    { offset: -Infinity, el: null }
  ).el;
  if (after == null) ol.appendChild(dragging);
  else ol.insertBefore(dragging, after);
});

function submitOrder() {
  const order = [...$('#queue').querySelectorAll('.q-item')].map((li) => li.dataset.pid);
  const same = order.length === state.queue.length && order.every((id, i) => id === state.queue[i]);
  if (!same) cmd('reorder', { order }); // 若版本过期，服务端回 stale 快照自动还原
}

function renderMyControls() {
  const div = $('#my-controls');
  div.innerHTML = '';
  const inQueue = state.queue.includes(myId);
  const isCurrent = state.current && state.current.participantId === myId;

  if (isCurrent) {
    const note = document.createElement('div');
    note.className = 'speaking-note';
    note.textContent = '🎤 你正在发言';
    div.appendChild(note);
  } else if (inQueue) {
    const btn = document.createElement('button');
    btn.className = 'btn btn-ghost';
    btn.textContent = `取消申请（当前第 ${state.queue.indexOf(myId) + 1} 位）`;
    btn.onclick = () => sendIntent('cancel');
    div.appendChild(btn);
  } else {
    const btn = document.createElement('button');
    btn.className = 'btn btn-primary';
    btn.textContent = '✋ 申请发言';
    btn.onclick = () => sendIntent('request');
    div.appendChild(btn);
  }
}

function renderHostControls() {
  const box = $('#host-controls');
  box.hidden = !isHost;
  if (!isHost) return;
  $('#btn-start-next').disabled = !!state.current || state.queue.length === 0;
  $('#btn-end').disabled = !state.current;
  $('#btn-clear').disabled = !state.current && state.queue.length === 0;
}

function renderParticipants() {
  const ul = $('#participants');
  ul.innerHTML = '';
  for (const p of state.participants) {
    const li = document.createElement('li');
    let html = `<span class="dot ${p.connected ? 'on' : ''}"></span><span class="name">${esc(p.name)}</span>`;
    if (p.id === state.hostId) html += '<span class="role">主持人</span>';
    if (p.id === myId) html += '<span class="role me">我</span>';
    li.innerHTML = html;
    ul.appendChild(li);
  }
}

function renderLog() {
  const ul = $('#log');
  ul.innerHTML = '';
  for (const entry of state.log) {
    const li = document.createElement('li');
    const d = new Date(entry.t);
    const hh = String(d.getHours()).padStart(2, '0');
    const mm = String(d.getMinutes()).padStart(2, '0');
    const ss = String(d.getSeconds()).padStart(2, '0');
    li.innerHTML = `<time>${hh}:${mm}:${ss}</time>${esc(entry.text)}`;
    ul.appendChild(li);
  }
  ul.scrollTop = ul.scrollHeight;
}

// ---------- 发言计时（基于 serverNow 对齐，重连后自动恢复） ----------

function fmtClock(ms) {
  const s = Math.max(0, Math.floor(ms / 1000));
  const h = Math.floor(s / 3600);
  const m = Math.floor((s % 3600) / 60);
  const sec = s % 60;
  const mmss = `${String(m).padStart(2, '0')}:${String(sec).padStart(2, '0')}`;
  return h > 0 ? `${h}:${String(m).padStart(2, '0')}:${String(sec).padStart(2, '0')}` : mmss;
}

setInterval(() => {
  if (!state || !state.current) return;
  $('#current-timer').textContent = fmtClock(Date.now() + serverOffset - state.current.startedAt);
}, 250);

// ---------- 提示 ----------

function toast(text) {
  const el = $('#toast');
  el.textContent = text;
  el.hidden = false;
  clearTimeout(toastTimer);
  toastTimer = setTimeout(() => {
    el.hidden = true;
  }, 2600);
}

// ---------- 事件绑定 ----------

$('#btn-create').onclick = () => {
  const name = $('#create-name').value.trim();
  if (!name) return toast('请输入昵称');
  whenOpen(() => send({ type: 'create', name }));
};

$('#btn-join').onclick = () => {
  const code = $('#join-code').value.trim().toUpperCase();
  const name = $('#join-name').value.trim();
  if (!code) return toast('请输入房间码');
  if (!name) return toast('请输入昵称');
  whenOpen(() => send({ type: 'join', code, name }));
};

$('#join-code').addEventListener('input', (e) => {
  e.target.value = e.target.value.toUpperCase();
});

$('#room-code').onclick = async () => {
  try {
    await navigator.clipboard.writeText(state ? state.code : '');
    toast('房间码已复制');
  } catch {
    toast('复制失败，请手动复制');
  }
};

$('#btn-start-next').onclick = () => cmd('start');
$('#btn-end').onclick = () => cmd('end');
$('#btn-clear').onclick = () => {
  if (confirm('确定清空发言队列和当前发言？')) cmd('clear');
};

$('#btn-leave').onclick = () => {
  if (!confirm('确定离开房间？')) return;
  send({ type: 'leave' });
  clearSession();
  state = null;
  showScreen('home');
};

// ---------- 启动 ----------

renderConn(false);
connect();
