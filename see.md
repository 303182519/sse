## 使用方法

```typescript
// 创建SSE客户端
const client = new SSEClient({
  url: "/api/chat/stream",
  method: "POST",
  headers: {
    Authorization: `Bearer ${token}`,
    "Content-Type": "application/json",
  },
  body: JSON.stringify({
    model: "gpt-4",
    messages: [{ role: "user", content: "介绍一下SSE" }],
  }),
  reconnect: {
    maxRetries: 10,
    baseDelay: 500,
    maxDelay: 60000,
    jitter: true,
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
  });

// 监听连接状态
client.onStateChange((state) => {
  updateConnectionIndicator(state);
});

// 建立连接
client.connect();

// 用户关闭页面时主动断开
window.addEventListener("beforeunload", () => {
  client.disconnect();
});
```
