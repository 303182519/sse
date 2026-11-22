interface SSEEvent {
	id?: string;
	event?: string;
	data: string;
	retry?: number;
	/** 结构化错误信息（仅错误类事件携带，如 auth_error / reconnect_failed） */
	error?: SSEError;
}

type SSEErrorType = 'http' | 'network' | 'stream' | 'timeout' | 'heartbeat';

interface SSEError {
	type: SSEErrorType;
	/** HTTP 状态码（网络层错误时为空） */
	status?: number;
	message: string;
	/** 服务端通过 Retry-After 响应头建议的等待时长（ms） */
	retryAfterMs?: number;
	cause?: unknown;
}

function parseSSEEvent(text: string): SSEEvent {
	const result: SSEEvent = { data: '' };
	const lines = text.split(/\r\n|\r|\n/);

	for (const line of lines) {
		if (line.startsWith(':')) continue; // 注释行（通常用作心跳），忽略内容，仅用于活性检测

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

// Retry-After 支持两种格式：秒数 或 HTTP 日期
function parseRetryAfter(value: string): number | undefined {
	const seconds = Number(value);
	if (!Number.isNaN(seconds)) return Math.max(0, seconds * 1000);
	const date = Date.parse(value);
	if (!Number.isNaN(date)) return Math.max(0, date - Date.now());
	return undefined;
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

interface SSELogger {
	debug(message: string, ...args: unknown[]): void;
	info(message: string, ...args: unknown[]): void;
	warn(message: string, ...args: unknown[]): void;
	error(message: string, ...args: unknown[]): void;
}

// 可观测性钩子：全部可选，用于接入监控系统（Prometheus、Sentry 等）
interface SSEMetrics {
	onOpen?(): void;
	onClose?(): void;
	onReconnectAttempt?(attempt: number, delay: number): void;
	onEvent?(event: SSEEvent): void;
	onError?(error: SSEError): void;
}


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


// 动态凭证：每次（重）连接前重新求值，解决 token 刷新后重连仍携带旧凭证的问题
type HeaderFactory = () => Record<string, string> | Promise<Record<string, string>>;

interface SSEClientOptions {
	url: string;
	method?: string;
	headers?: Record<string, string> | HeaderFactory;
	body?: string;
	reconnect?: Partial<ReconnectStrategy>;
	withCredentials?: boolean;
	/** 初始 Last-Event-ID，用于跨页面刷新的断点续传 */
	lastEventId?: string;
	/**
	 * 心跳超时（ms）：超过该时长未收到任何字节（含注释行心跳），
	 * 判定连接已被中间设备静默断链，主动断开并重连。<=0 关闭。默认 45000
	 */
	heartbeatTimeout?: number;
	/** 连接建立阶段（收到响应头之前）的超时（ms），<=0 关闭。默认 30000 */
	requestTimeout?: number;
	/** 日志注入，默认 console，用于接入统一日志体系 */
	logger?: SSELogger;
	/** 可观测性钩子 */
	metrics?: SSEMetrics;
	/** 网络恢复（online 事件）时是否立即重连，默认 true */
	reconnectOnOnline?: boolean;
}

class SSEClient {
	private state: SSEState = SSEState.IDLE;
	private retryCount = 0;
	private lastEventId: string | null = null;
	private abortController: AbortController | null = null;
	/** 区分 abort 的来源：用户主动断开 / 心跳超时 / 连接超时，决定是否自动重连 */
	private abortReason: 'manual' | 'heartbeat' | 'timeout' | null = null;
	private reconnectTimer: ReturnType<typeof setTimeout> | null = null;
	private stableTimer: ReturnType<typeof setTimeout> | null = null;
	private heartbeatTimer: ReturnType<typeof setTimeout> | null = null;
	private lastActivityAt = 0;
	/** 服务端通过 retry: 字段下发的重连间隔（ms），优先级高于本地退避计算 */
	private serverRetryMs: number | null = null;
	private networkListenersBound = false;

	private eventHandlers = new Map<string, Set<SSEEventHandler>>();
	private stateHandlers = new Set<StateChangeHandler>();
	private middlewares: SSEMiddleware[] = [];

	private reconnectStrategy: ReconnectStrategy;
	private readonly logger: SSELogger;
	private readonly metrics?: SSEMetrics;
	private readonly heartbeatTimeout: number;
	private readonly requestTimeout: number;
	private readonly reconnectOnOnline: boolean;

	constructor(private options: SSEClientOptions) {
		this.reconnectStrategy = {
			maxRetries: 5,
			baseDelay: 1000,
			maxDelay: 30000,
			jitter: true,
			stableDuration: 30000,
			...options.reconnect,
		};
		this.logger = options.logger ?? console;
		this.metrics = options.metrics;
		this.heartbeatTimeout = options.heartbeatTimeout ?? 45000;
		this.requestTimeout = options.requestTimeout ?? 30000;
		this.reconnectOnOnline = options.reconnectOnOnline ?? true;
		this.lastEventId = options.lastEventId ?? null;
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

		this.abortReason = null;
		this.transition(SSEState.CONNECTING);
		this.abortController = new AbortController();
		this.bindEnvironmentListeners();

		// 连接建立阶段超时：fetch 在收到响应头之前可能无限挂起
		let requestTimer: ReturnType<typeof setTimeout> | null = null;
		if (this.requestTimeout > 0) {
			requestTimer = setTimeout(() => this.abort('timeout'), this.requestTimeout);
		}

		try {
			let response: Response;
			try {
				// 每次连接前动态求值，保证重连时使用最新凭证。
				// HeaderFactory 与 fetch 纳入同一个 try/finally：
				// 否则 HeaderFactory 抛异常时 requestTimer 残留，
				// 之后触发会误 abort 重连后的新连接
				const dynamicHeaders =
					typeof this.options.headers === 'function'
						? await this.options.headers()
						: this.options.headers;

				const headers: Record<string, string> = {
					Accept: 'text/event-stream',
					...dynamicHeaders,
				};

				if (this.lastEventId) {
					headers['Last-Event-ID'] = this.lastEventId;
				}

				response = await fetch(this.options.url, {
					method: this.options.method ?? 'GET',
					headers,
					body: this.options.body,
					signal: this.abortController.signal,
					credentials: this.options.withCredentials ? 'include' : 'same-origin',
				});
			} finally {
				// 响应头到达（或任意异常）后，连接建立阶段结束，超时定时器必须清理
				if (requestTimer) {
					clearTimeout(requestTimer);
					requestTimer = null;
				}
			}

			if (!response.ok) {
				const error: Error & { status?: number; retryAfterMs?: number } = new Error(
					`HTTP ${response.status}: ${response.statusText}`
				);
				error.status = response.status;
				const retryAfter = response.headers.get('retry-after');
				if (retryAfter) {
					error.retryAfterMs = parseRetryAfter(retryAfter);
				}
				throw error;
			}

			// 校验 Content-Type，防止网关/负载均衡返回的错误页（200 + HTML）被当作事件流消费
			const contentType = response.headers.get('content-type') ?? '';
			if (!contentType.includes('text/event-stream')) {
				throw new Error(`Unexpected Content-Type: ${contentType || '(missing)'}`);
			}

			this.transition(SSEState.OPEN);

			// 连接刚建立时不清零重试计数：只有稳定保持一段时间才视为恢复，
			// 避免服务端「接受连接后立刻断开」导致重试计数被反复清零、无限重连
			this.startStableTimer();

			// 启动心跳活性检测，防止代理/NAT 静默断链后无限挂起
			this.resetHeartbeat();

			await this.consumeStream(response);
		} catch (error: any) {
			if (error?.name === 'AbortError') {
				if (this.abortReason === 'manual') {
					// 主动断开，不需要重连
					return;
				}
				this.handleError({
					type: 'timeout',
					message: `连接建立超时（${this.requestTimeout}ms）`,
					cause: error,
				});
				return;
			}

			this.handleError(this.normalizeError(error));
		}
	}

	disconnect(): void {
		this.clearReconnectTimer();
		this.clearStableTimer();
		this.clearHeartbeat();

		this.unbindEnvironmentListeners();
		this.abort('manual');
		this.abortController = null;

		this.transition(SSEState.CLOSED);
	}

	private abort(reason: 'manual' | 'heartbeat' | 'timeout'): void {
		this.abortReason = reason;
		this.abortController?.abort();
	}

	private normalizeError(error: unknown): SSEError {
		const err = error as Error & { status?: number; retryAfterMs?: number };
		return {
			type: err?.status ? 'http' : 'network',
			status: err?.status,
			retryAfterMs: err?.retryAfterMs,
			message: err?.message ?? String(error),
			cause: error,
		};
	}

	// === 流消费 ===

	private async consumeStream(response: Response): Promise<void> {
		const body = response.body;
		if (!body) {
			this.tryReconnect({ type: 'stream', message: 'Response body is null' });
			return;
		}

		const reader = body.getReader();
		const decoder = new TextDecoder();
		let buffer = '';

		try {
			while (true) {
				const { done, value } = await reader.read();

				if (done) {
					// 多字节 UTF-8 字符可能被拆在不同 chunk，
					// EOF 时 flush 解码器内部残留的字节
					buffer += decoder.decode();

					// 用户主动断开后 EOF 到达：正常结束，静默退出，不误报为意外断开
					if (this.abortReason === 'manual' || this.state === SSEState.CLOSED) {
						break;
					}

					// 连接异常结束时，buffer 里可能残留未以空行结束的半条事件。
					// 按协议不完整事件只能丢弃并记录，不能解析半条 JSON 交给业务层
					if (buffer.trim()) {
						this.logger.warn(
							'[SSE] 连接异常结束，丢弃未完整结束的事件:',
							buffer.slice(0, 200)
						);
						buffer = '';
					}

					// 服务端关闭了事件流（EOF）。SSE 是长连接，不应主动结束，视为意外断开，触发重连
					this.tryReconnect({ type: 'stream', message: 'Stream ended unexpectedly' });
					break;
				}

				// 收到任意字节即刷新活性时间戳（含注释行心跳）
				this.resetHeartbeat();

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
			if (error?.name === 'AbortError') {
				// 心跳触发的主动 abort：连接已被判定死亡，走重连
				if (this.abortReason === 'heartbeat') {
					this.tryReconnect({
						type: 'heartbeat',
						message: `心跳超时（${this.heartbeatTimeout}ms 无数据），连接已死亡`,
						cause: error,
					});
				}
				// manual：用户主动断开，静默结束
				return;
			}
			this.tryReconnect(this.normalizeError(error));
		}
	}

	// === 心跳活性检测 ===

	private resetHeartbeat(): void {
		if (this.heartbeatTimeout <= 0) return;
		this.lastActivityAt = Date.now();
		this.clearHeartbeat();

		this.heartbeatTimer = setTimeout(() => {
			this.heartbeatTimer = null;
			this.logger.warn(
				`[SSE] 超过 ${this.heartbeatTimeout}ms 未收到任何数据，判定连接已死，主动重连`
			);
			this.abort('heartbeat'); // 中止挂起的 read，由 consumeStream 的 catch 触发重连
		}, this.heartbeatTimeout);
	}

	private clearHeartbeat(): void {
		if (this.heartbeatTimer) {
			clearTimeout(this.heartbeatTimer);
			this.heartbeatTimer = null;
		}
	}

	// === 网络环境监听 ===

	private bindEnvironmentListeners(): void {
		if (this.networkListenersBound || typeof window === 'undefined') return;
		window.addEventListener('offline', this.onOffline);
		window.addEventListener('online', this.onOnline);
		if (typeof document !== 'undefined') {
			document.addEventListener('visibilitychange', this.onVisibilityChange);
		}
		this.networkListenersBound = true;
	}

	private unbindEnvironmentListeners(): void {
		if (!this.networkListenersBound || typeof window === 'undefined') return;
		window.removeEventListener('offline', this.onOffline);
		window.removeEventListener('online', this.onOnline);
		if (typeof document !== 'undefined') {
			document.removeEventListener('visibilitychange', this.onVisibilityChange);
		}
		this.networkListenersBound = false;
	}

	private onOffline = (): void => {
		this.logger.warn('[SSE] 网络已离线，暂停重连，等待网络恢复');
		this.clearReconnectTimer();
		// 中止当前连接但不触发自动重连，等待 online 事件驱动
		this.abort('manual');
		if (this.state === SSEState.OPEN || this.state === SSEState.CONNECTING) {
			this.transition(SSEState.RECONNECTING);
		}
	};

	private onOnline = (): void => {
		if (!this.reconnectOnOnline || this.state !== SSEState.RECONNECTING) return;
		this.logger.info('[SSE] 网络恢复，立即重连');
		this.retryCount = 0; // 网络环境变化属于新场景，恢复完整重试预算
		void this.connect();
	};

	private onVisibilityChange = (): void => {
		if (typeof document === 'undefined' || document.visibilityState !== 'visible') return;
		// 后台标签页中定时器会被节流，心跳定时器可能无法按时触发；
		// 回到前台时主动校验活性，避免「假活」连接
		if (this.state !== SSEState.OPEN || this.heartbeatTimeout <= 0) return;
		if (Date.now() - this.lastActivityAt > this.heartbeatTimeout) {
			this.logger.warn('[SSE] 页面恢复前台，连接已超心跳期限，主动重连');
			this.abort('heartbeat');
		}
	};

	// === 重连逻辑 ===

	private tryReconnect(error: SSEError, overrideDelay?: number): void {
		// 用户已主动关闭（如收到业务 done 后 disconnect），不再调度重连
		if (this.state === SSEState.CLOSED) {
			return;
		}

		if (this.retryCount >= this.reconnectStrategy.maxRetries) {
			this.transition(SSEState.CLOSED);
			this.emitError('reconnect_failed', error);
			return;
		}

		this.transition(SSEState.RECONNECTING);

		// 延迟优先级：显式指定 > 服务端 Retry-After > 服务端 retry: 字段 > 本地指数退避
		const delay = Math.min(
			overrideDelay ??
			error.retryAfterMs ??
			this.serverRetryMs ??
			calculateDelay(this.retryCount, this.reconnectStrategy),
			this.reconnectStrategy.maxDelay
		);
		this.retryCount++;

		this.metrics?.onReconnectAttempt?.(this.retryCount, delay);
		this.logger.warn(
			`[SSE] 连接断开，${delay}ms 后进行第 ${this.retryCount} 次重连...`,
			error.message
		);

		this.reconnectTimer = setTimeout(() => {
			void this.connect();
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

	once(event: string, handler: SSEEventHandler): this {
		const wrapper: SSEEventHandler = (e) => {
			this.off(event, wrapper);
			handler(e);
		};
		return this.on(event, wrapper);
	}

	off(event: string, handler: SSEEventHandler): this {
		this.eventHandlers.get(event)?.delete(handler);
		return this;
	}

	onStateChange(handler: StateChangeHandler): this {
		this.stateHandlers.add(handler);
		return this;
	}

	offStateChange(handler: StateChangeHandler): this {
		this.stateHandlers.delete(handler);
		return this;
	}

	private dispatchEvent(event: SSEEvent): void {
		// SSE 规范：服务端的 retry: 字段用于建议重连间隔
		if (typeof event.retry === 'number' && !Number.isNaN(event.retry)) {
			this.serverRetryMs = event.retry;
		}

		// 执行中间件管道
		let current = event;
		for (const middleware of this.middlewares) {
			let next: SSEEvent | null;
			try {
				next = middleware.onEvent(current);
			} catch (e) {
				this.logger.error('[SSE] 中间件异常:', e);
				continue;
			}
			if (!next) return; // 中间件可以吞掉事件
			current = next;
		}

		this.metrics?.onEvent?.(current);
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
				this.logger.error('[SSE] 事件处理器异常:', e);
			}
		}
	}

	// 以错误事件的形式通知处理器，保留结构化错误信息（type/status/retryAfterMs）
	private emitError(event: string, error: SSEError): void {
		this.emit(event, { event, data: error.message, error });
	}

	private handleError(error: SSEError): void {
		this.metrics?.onError?.(error);
		const status = error.status;

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
					// 限流：优先遵循服务端 Retry-After，否则走常规退避
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
			// 网络错误 / 超时 / Content-Type 异常（可能是网关瞬态故障），适合重连
			this.tryReconnect(error);
		}
	}


	// === 状态管理 ===

	private transition(newState: SSEState): void {
		const oldState = this.state;
		this.state = newState;

		// 稳定性计时与心跳检测只在 OPEN 状态下有效，离开 OPEN 立即取消
		if (oldState === SSEState.OPEN && newState !== SSEState.OPEN) {
			this.clearStableTimer();
			this.clearHeartbeat();
		}

		if (newState === SSEState.OPEN) this.metrics?.onOpen?.();
		if (newState === SSEState.CLOSED) this.metrics?.onClose?.();

		if (oldState !== newState) {
			for (const handler of this.stateHandlers) {
				// 与事件处理器一致的异常隔离：一个订阅者抛异常
				// 不能影响其他订阅者，也不能传播到 connect() 被误归类为连接错误
				try {
					handler(newState);
				} catch (e) {
					this.logger.error('[SSE] 状态监听器异常:', e);
				}
			}
		}
	}

	getState(): SSEState {
		return this.state;
	}

	/** 当前断点位置，可持久化后通过 options.lastEventId 注入实现跨刷新续传 */
	getLastEventId(): string | null {
		return this.lastEventId;
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
	constructor(private logger: SSELogger = console) { }

	onEvent(event: SSEEvent): SSEEvent {
		this.logger.info(`[SSE Event] type=${event.event ?? 'message'} data=${event.data.slice(0, 100)}`);
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
