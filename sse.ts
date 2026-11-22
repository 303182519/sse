interface SSEEvent {
	id?: string;
	event?: string;
	data: string;
	retry?: number;
}

function parseSSEEvent(text: string): SSEEvent {
	const result: SSEEvent = { data: '' };
	const lines = text.split(/\r\n|\r|\n/);

	for (const line of lines) {
		if (line.startsWith(':')) continue; // 注释行，忽略

		const colonIndex = line.indexOf(':');
		if (colonIndex === -1) continue;

		const field = line.slice(0, colonIndex);
		let value = line.slice(colonIndex + 1);
		if (value.startsWith(' ')) value = value.slice(1); // 去除前导空格

		switch (field) {
			case 'id':
				result.id = value;
				break;
			case 'event':
				result.event = value;
				break;
			case 'data':
				result.data = result.data ? result.data + '\n' + value : value;
				break;
			case 'retry':
				result.retry = parseInt(value, 10);
				break;
		}
	}

	return result;
}


enum SSEState {
	IDLE = 'idle',
	CONNECTING = 'connecting',
	OPEN = 'open',
	RECONNECTING = 'reconnecting',
	CLOSED = 'closed',
}


type SSEEventHandler = (event: SSEEvent) => void;
type StateChangeHandler = (state: SSEState) => void;


interface ReconnectStrategy {
	maxRetries: number;        // 最大重试次数
	baseDelay: number;         // 基础延迟（ms）
	maxDelay: number;          // 最大延迟（ms）
	jitter: boolean;           // 是否添加随机抖动
	stableDuration: number;    // 连接稳定多久后清零重试计数（ms），<=0 表示连上即清零
}

function calculateDelay(
	retryCount: number,
	strategy: ReconnectStrategy
): number {
	// 指数退避：baseDelay * 2^retryCount
	const exponentialDelay = strategy.baseDelay * Math.pow(2, retryCount);

	// 限制最大延迟
	const cappedDelay = Math.min(exponentialDelay, strategy.maxDelay);

	// 添加随机抖动，防止重连风暴
	if (strategy.jitter) {
		return cappedDelay * (0.5 + Math.random() * 0.5);
	}

	return cappedDelay;
}

// 示例：baseDelay=1000, maxDelay=30000
// 重试1: 1000ms → 1s
// 重试2: 2000ms → 2s
// 重试3: 4000ms → 4s
// 重试4: 8000ms → 8s
// 重试5: 16000ms → 16s
// 重试6+: 30000ms → 30s（封顶）


interface SSEClientOptions {
	url: string;
	method?: string;
	headers?: Record<string, string>;
	body?: string;
	reconnect?: Partial<ReconnectStrategy>;
	withCredentials?: boolean;
}

class SSEClient {
	private state: SSEState = SSEState.IDLE;
	private retryCount = 0;
	private lastEventId: string | null = null;
	private abortController: AbortController | null = null;
	private reconnectTimer: ReturnType<typeof setTimeout> | null = null;
	private stableTimer: ReturnType<typeof setTimeout> | null = null;

	private eventHandlers = new Map<string, Set<SSEEventHandler>>();
	private stateHandlers = new Set<StateChangeHandler>();
	private middlewares: SSEMiddleware[] = [];

	private reconnectStrategy: ReconnectStrategy;

	constructor(private options: SSEClientOptions) {
		this.reconnectStrategy = {
			maxRetries: 5,
			baseDelay: 1000,
			maxDelay: 30000,
			jitter: true,
			stableDuration: 30000,
			...options.reconnect,
		};
	}

	// === 连接管理 ===

	async connect(): Promise<void> {
		if (this.state === SSEState.OPEN || this.state === SSEState.CONNECTING) {
			return; // 防止重复连接
		}

		// 从 CLOSED 状态手动重连时，重置重试预算
		if (this.state === SSEState.CLOSED) {
			this.retryCount = 0;
		}

		this.transition(SSEState.CONNECTING);
		this.abortController = new AbortController();

		try {
			const headers: Record<string, string> = {
				Accept: 'text/event-stream',
				...this.options.headers,
			};

			if (this.lastEventId) {
				headers['Last-Event-ID'] = this.lastEventId;
			}

			const response = await fetch(this.options.url, {
				method: this.options.method ?? 'GET',
				headers,
				body: this.options.body,
				signal: this.abortController.signal,
				credentials: this.options.withCredentials ? 'include' : 'same-origin',
			});

			if (!response.ok) {
				const error: Error & { status?: number } = new Error(
					`HTTP ${response.status}: ${response.statusText}`
				);
				error.status = response.status;
				throw error;
			}

			this.transition(SSEState.OPEN);

			// 连接刚建立时不清零重试计数：只有稳定保持一段时间才视为恢复，
			// 避免服务端「接受连接后立刻断开」导致重试计数被反复清零、无限重连
			this.startStableTimer();

			await this.consumeStream(response);
		} catch (error: any) {
			if (error.name === 'AbortError') {
				// 主动断开，不需要重连
				return;
			}

			this.handleError(error);
		}
	}

	disconnect(): void {
		this.clearReconnectTimer();
		this.abortController?.abort();
		this.abortController = null;
		this.transition(SSEState.CLOSED);
	}

	// === 流消费 ===

	private async consumeStream(response: Response): Promise<void> {
		const reader = response.body!.getReader();
		const decoder = new TextDecoder();
		let buffer = '';

		try {
			while (true) {
				const { done, value } = await reader.read();

				if (done) {
					// 服务端关闭了事件流（EOF）。SSE 是长连接，不应主动结束，视为意外断开，触发重连
					this.tryReconnect(new Error('Stream ended unexpectedly'));
					break;
				}

				buffer += decoder.decode(value, { stream: true });
				const events = buffer.split(/\r\n\r\n|\r\r|\n\n/);
				buffer = events.pop()!;

				for (const eventText of events) {
					if (eventText.trim()) {
						const sseEvent = parseSSEEvent(eventText);
						this.lastEventId = sseEvent.id ?? this.lastEventId;
						this.dispatchEvent(sseEvent);
					}
				}
			}
		} catch (error: any) {
			if (error.name !== 'AbortError') {
				this.tryReconnect(error);
			}
		}
	}

	// === 重连逻辑 ===

	private tryReconnect(error: Error): void {
		if (this.retryCount >= this.reconnectStrategy.maxRetries) {
			this.transition(SSEState.CLOSED);
			this.emitError('reconnect_failed', error);
			return;
		}

		this.transition(SSEState.RECONNECTING);

		const delay = calculateDelay(this.retryCount, this.reconnectStrategy);
		this.retryCount++;

		console.warn(
			`[SSE] 连接断开，${delay}ms 后进行第 ${this.retryCount} 次重连...`,
			error.message
		);

		this.reconnectTimer = setTimeout(() => {
			this.connect();
		}, delay);
	}

	private clearReconnectTimer(): void {
		if (this.reconnectTimer) {
			clearTimeout(this.reconnectTimer);
			this.reconnectTimer = null;
		}
	}

	// 连接稳定计时：持续 stableDuration 后才清零重试计数
	private startStableTimer(): void {
		this.clearStableTimer();

		const { stableDuration } = this.reconnectStrategy;
		if (stableDuration <= 0) {
			this.retryCount = 0;
			return;
		}

		this.stableTimer = setTimeout(() => {
			this.retryCount = 0;
			this.stableTimer = null;
		}, stableDuration);
	}

	private clearStableTimer(): void {
		if (this.stableTimer) {
			clearTimeout(this.stableTimer);
			this.stableTimer = null;
		}
	}

	// === 事件系统 ===

	on(event: string, handler: SSEEventHandler): this {
		if (!this.eventHandlers.has(event)) {
			this.eventHandlers.set(event, new Set());
		}
		this.eventHandlers.get(event)!.add(handler);
		return this;
	}

	off(event: string, handler: SSEEventHandler): this {
		this.eventHandlers.get(event)?.delete(handler);
		return this;
	}

	onStateChange(handler: StateChangeHandler): this {
		this.stateHandlers.add(handler);
		return this;
	}

	private dispatchEvent(event: SSEEvent): void {
		// 执行中间件管道
		let current = event;
		for (const middleware of this.middlewares) {
			let next: SSEEvent | null;
			try {
				next = middleware.onEvent(current);
			} catch (e) {
				console.error('[SSE] 中间件异常:', e);
				continue;
			}
			if (!next) return; // 中间件可以吞掉事件
			current = next;
		}

		this.emit(current.event ?? 'message', current);
	}

	// 向指定事件的处理器分发事件
	private emit(event: string, sseEvent: SSEEvent): void {
		const handlers = this.eventHandlers.get(event);
		if (!handlers) return;

		for (const handler of handlers) {
			try {
				handler(sseEvent);
			} catch (e) {
				console.error('[SSE] 事件处理器异常:', e);
			}
		}
	}

	// 从错误对象中读取 HTTP 状态码
	private extractHttpStatus(error: Error): number | undefined {
		return (error as Error & { status?: number }).status;
	}

	// 以错误事件的形式通知处理器
	private emitError(event: string, error: Error): void {
		this.emit(event, { event, data: error.message });
	}

	private handleError(error: Error): void {
		const status = this.extractHttpStatus(error);

		if (status) {
			switch (true) {
				// 4xx错误：客户端问题，重连无意义
				case status === 401:
					// 鉴权过期，需要刷新token
					this.transition(SSEState.CLOSED);
					this.emitError('auth_error', error);
					break;

				case status === 403:
					// 禁止访问，彻底放弃
					this.transition(SSEState.CLOSED);
					this.emitError('forbidden', error);
					break;

				case status === 429:
					// 限流，等更长时间再重试
					this.retryCount += 2; // 跳过一些重试次数以增加延迟
					this.tryReconnect(error);
					break;

				case status >= 400 && status < 500:
					// 其他4xx，不重连
					this.transition(SSEState.CLOSED);
					this.emitError('client_error', error);
					break;

				// 5xx错误：服务端问题，可以重连
				case status >= 500:
					this.tryReconnect(error);
					break;
			}
		} else {
			// 网络错误（无HTTP状态码），适合重连
			this.tryReconnect(error);
		}
	}


	// === 状态管理 ===

	private transition(newState: SSEState): void {
		const oldState = this.state;
		this.state = newState;

		// 稳定性计时只在 OPEN 状态下有效，离开 OPEN 立即取消
		if (oldState === SSEState.OPEN && newState !== SSEState.OPEN) {
			this.clearStableTimer();
		}

		if (oldState !== newState) {
			for (const handler of this.stateHandlers) {
				handler(newState);
			}
		}
	}

	getState(): SSEState {
		return this.state;
	}

	// === 中间件 ===

	use(middleware: SSEMiddleware): this {
		this.middlewares.push(middleware);
		return this;
	}
}

// 中间件接口
interface SSEMiddleware {
	onEvent(event: SSEEvent): SSEEvent | null;
}

// 日志中间件
class LoggingMiddleware implements SSEMiddleware {
	onEvent(event: SSEEvent): SSEEvent {
		console.log(`[SSE Event] type=${event.event ?? 'message'} data=${event.data.slice(0, 100)}`);
		return event;
	}
}

// 鉴权中间件（检查事件中的token过期信号）
class AuthMiddleware implements SSEMiddleware {
	constructor(private onAuthExpired: () => void) { }

	onEvent(event: SSEEvent): SSEEvent | null {
		if (event.event === 'error' && event.data.includes('token_expired')) {
			this.onAuthExpired();
			return null; // 吞掉事件，不再传播
		}
		return event;
	}
}
