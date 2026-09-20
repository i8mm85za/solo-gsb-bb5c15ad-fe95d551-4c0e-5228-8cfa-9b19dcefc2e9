# 发言席位（Speak Seat）

适合线下讨论投屏使用的多人发言席位 Web 应用。创建者生成房间码，参与者凭昵称加入并申请 / 取消发言；主持人可拖拽排序、开始与结束发言，并查看操作记录、一键清空队列。

技术栈：Node.js + Express + WebSocket（`ws`），前端为原生 HTML / CSS / JavaScript，无任何构建步骤。

## 快速开始（Docker）

```bash
docker compose up --build
```

启动后在浏览器访问：

```
http://localhost:3000
```

停止：`docker compose down`。

## 本地开发（不用 Docker）

```bash
npm install
npm start          # 或 npm run dev（文件变动自动重启）
```

同样访问 `http://localhost:3000`。要求 Node.js ≥ 18。

## 使用方式

1. **主持人**：首页输入昵称 →「创建房间」，得到 6 位房间码（点击可复制），投屏展示给现场。
2. **参与者**：首页输入房间码 + 昵称 →「加入房间」，点击「✋ 申请发言」进入队列，可随时取消。
3. **主持人**：
   - 拖动队列项调整顺序（松手即提交）；
   - 「▶ 开始下一位」或点某项的「开始」指定发言人，大屏开始计时；
   - 「■ 结束发言」停止计时并记录用时；
   - 「移出」可将单个成员移出队列；「清空队列」一键清空队列与当前发言。
4. 右侧实时显示成员在线状态与操作记录。

刷新或断网重连后自动恢复身份，队列顺序与当前发言计时保持一致。

## 配置项（环境变量）

| 变量 | 默认值 | 说明 |
| --- | --- | --- |
| `PORT` | `3000` | HTTP / WebSocket 监听端口 |
| `ROOM_IDLE_TTL_MINUTES` | `120` | 房间无任何连接且空闲超过该时长后自动回收 |
| `MAX_LOG_ENTRIES` | `200` | 每个房间保留的操作记录上限（快照携带最近 50 条） |

在 `compose.yaml` 的 `environment` 中修改即可。

## 一致性与版本机制

- 房间状态（队列、当前发言、成员、日志）由服务端单点持有，每次变更产生**单调递增的版本号**。
- 所有队列命令（`request` / `cancel` / `reorder` / `start` / `end` / `remove` / `clear`）都携带客户端所见版本 `baseVersion`。
- 服务端在单线程事件循环内**原子地**完成「校验版本 → 应用变更 → 递增版本 → 广播快照」，不存在竞态。
- 版本不匹配时命令被拒绝，服务端立即回发 `stale` 最新快照，客户端以此为准重新渲染（例如拖拽排序期间队列被他人变更）。
- 快照携带 `serverNow` 与当前发言的 `startedAt`，客户端对齐时钟后本地走秒，因此**断线重连后顺序与发言计时都能精确恢复**。
- 主持人操作需携带创建房间时签发的 `hostToken`（仅存于创建者浏览器的 localStorage）。

## 项目结构

```
├── server.js          # Express 静态托管 + WebSocket 房间/队列逻辑
├── public/
│   ├── index.html     # 单页界面（首页 + 房间页）
│   ├── style.css      # 投屏友好的深色大字号样式
│   └── app.js         # 连接管理、快照渲染、拖拽排序、计时
├── package.json
├── Dockerfile
├── compose.yaml
└── .dockerignore
```

## WebSocket 协议速览

客户端 → 服务端：`create` / `join` / `rejoin` / `leave`，以及携带 `baseVersion` 的队列命令（主持人命令另需 `hostToken`）。
服务端 → 客户端：`created` / `joined` / `rejoined`（含身份凭证）、`snapshot`（全量状态）、`stale`（版本过期，附最新快照）、`error`。
