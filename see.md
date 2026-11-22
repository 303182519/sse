## 使用方法

```typescript
// 创建SSE客户端
const client = new SSEClient({
  url: "/api/chat/stream",
  method: "POST",
  // headers 支持函数形式：每次（重）连接前重新求值，token 刷新后重连自动使用新凭证
  headers: () => ({
    Authorization: `Bearer ${getLatestToken()}`,
    "Content-Type": "application/json",
  }),
  body: JSON.stringify({
    model: "gpt-4",
    messages: [{ role: "user", content: "介绍一下SSE" }],
  }),
  // 断点续传：页面刷新后从上次位置继续
  lastEventId: localStorage.getItem("sse_last_event_id") ?? undefined,
  // 心跳超时：45s 无任何字节（含注释行心跳）判定连接死亡并主动重连，防代理静默断链
  heartbeatTimeout: 45000,
  // 连接建立阶段超时
  requestTimeout: 30000,
  reconnect: {
    maxRetries: 10,
    baseDelay: 500,
    maxDelay: 60000,
    jitter: true,
    stableDuration: 30000, // 连接稳定 30s 后才清零重试计数，防止抖动导致无限重连
  },
  // 日志注入：接入统一日志体系
  logger: myLogger,
  // 可观测性钩子：接入监控（重连次数、错误分布、连接时长等）
  metrics: {
    onReconnectAttempt: (attempt, delay) =>
      reportMetric("sse_reconnect", { attempt, delay }),
    onError: (error) => reportMetric("sse_error", { type: error.type, status: error.status }),
  },
});

// 注册中间件
client.use(new LoggingMiddleware());
client.use(
  new AuthMiddleware(() => {
    console.log("Token过期，跳转登录页");
    window.location.href = "/login";
  }),
);

// 监听事件
client
  .on("token", (event) => {
    const data = JSON.parse(event.data);
    appendToUI(data.content);
  })
  .on("done", () => {
    console.log("生成完成");
  })
  .on("message", (event) => {
    // 默认消息类型的兜底处理
    appendToUI(event.data);
  })
  .once("done", () => {
    // once：只响应一次
  });

// 错误事件携带结构化信息（type / status / retryAfterMs），可编程化处理
client.on("auth_error", (event) => {
  console.error("鉴权失败:", event.error?.status, event.error?.message);
});
client.on("reconnect_failed", (event) => {
  showOfflineBanner();
});

// 监听连接状态
const onState = (state) => updateConnectionIndicator(state);
client.onStateChange(onState);
// 组件卸载时记得解绑，防止内存泄漏
// client.offStateChange(onState);

// 建立连接
client.connect();

// 持久化断点位置，供下次会话续传
window.addEventListener("beforeunload", () => {
  const id = client.getLastEventId();
  if (id) localStorage.setItem("sse_last_event_id", id);
  client.disconnect();
});
```

## 企业级特性说明

- **心跳活性检测**：`heartbeatTimeout` 内未收到任何字节（含服务端注释行心跳）即主动断开重连，解决代理/NAT 静默断链导致的「假活」连接；页面从后台恢复前台时也会主动校验活性。
- **网络感知**：监听 `online` / `offline` 事件，断网时暂停重连，网络恢复后立即重连并重置重试预算（可用 `reconnectOnOnline: false` 关闭）。
- **服务端节奏协商**：自动遵循 SSE 规范的 `retry:` 字段和 HTTP `Retry-After` 响应头（429/503），延迟优先级：Retry-After > retry: 字段 > 本地指数退避。
- **Content-Type 校验**：防止网关错误页（200 + HTML）被当作事件流消费。
- **结构化错误**：`auth_error` / `forbidden` / `client_error` / `reconnect_failed` 事件均携带 `event.error`（type、status、retryAfterMs、cause）。
- **动态凭证**：`headers` 传函数即可在每次连接前重新求值，配合 token 刷新机制使用。
