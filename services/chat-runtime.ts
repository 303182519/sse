
import { SSEClient } from '@/services/sse/SSEClient';
import { useChatStore } from '../stores/chat.store';
import type {
	ChatMessage,
	ChatRun,
	RunStatus,
} from '../types/chat.types';

interface RuntimeOptions {
	getAccessToken: () => Promise<string>;
}

interface ActiveRun {
	run: ChatRun;
	client: SSEClient;

	buffer: string;
	rafId: number | null;

	terminal: boolean;
	handlers: {
		token: (event: any) => void;
		done: (event: any) => void;
		appError: (event: any) => void;
		reconnectFailed: (event: any) => void;
		authError: (event: any) => void;
		stateChange: (state: any) => void;
	};
}

function parseEventData<T>(data: string): T {
	return JSON.parse(data) as T;
}

function createId() {
	return crypto.randomUUID();
}

export class ChatRuntime {
	private activeRuns = new Map<string, ActiveRun>();

	constructor(private options: RuntimeOptions) { }

	// 开始一次新的生成
	async sendMessage(
		conversationId: string,
		content: string,
	): Promise<string> {
		const store = useChatStore.getState();

		// 同一个会话只允许一个活动 Run
		const existingRunId =
			store.activeRunByConversation[conversationId];

		if (existingRunId) {
			const existing = store.runs[existingRunId];

			if (
				existing &&
				['connecting', 'streaming', 'reconnecting'].includes(
					existing.status,
				)
			) {
				throw new Error('当前会话正在生成，请先停止或等待完成');
			}
		}

		const userMessageId = createId();
		const assistantMessageId = createId();
		const runId = createId();

		const now = Date.now();

		const userMessage: ChatMessage = {
			id: userMessageId,
			conversationId,
			role: 'user',
			content,
			status: 'completed',
			createdAt: now,
		};

		const assistantMessage: ChatMessage = {
			id: assistantMessageId,
			conversationId,
			role: 'assistant',
			content: '',
			status: 'pending',
			createdAt: now,
			runId,
		};

		const run: ChatRun = {
			id: runId,
			conversationId,
			userMessageId,
			assistantMessageId,
			status: 'connecting',
			startedAt: now,
			updatedAt: now,
		};

		store.addMessage(userMessage);
		store.addMessage(assistantMessage);
		store.addRun(run);

		try {
			await this.startRun(run, content);
		} catch (error) {
			this.finish(
				runId,
				'error',
				error instanceof Error ? error.message : '连接失败',
			);
		}

		return runId;
	}

	// 建立 SSE 连接
	private async startRun(
		run: ChatRun,
		prompt: string,
		lastEventId?: string,
	): Promise<void> {
		const body = JSON.stringify({
			conversationId: run.conversationId,
			runId: run.id,
			message: prompt,
		});

		const client = new SSEClient({
			url: '/api/chat/stream',
			method: 'POST',

			headers: async () => ({
				'Content-Type': 'application/json',
				Authorization: `Bearer ${await this.options.getAccessToken()}`,
				'Idempotency-Key': run.id,
			}),

			body,

			// 只有服务端支持幂等 Run + 事件重放时，才允许自动重连
			reconnect: {
				maxRetries: 3,
				baseDelay: 1000,
				maxDelay: 10000,
				jitter: true,
				stableDuration: 30000,
			},

			lastEventId,

			heartbeatTimeout: 45000,
			requestTimeout: 30000,
			reconnectOnOnline: true,
		});

		const active: ActiveRun = {
			run,
			client,
			buffer: '',
			rafId: null,
			terminal: false,
			handlers: {
				token: () => { },
				done: () => { },
				appError: () => { },
				reconnectFailed: () => { },
				authError: () => { },
				stateChange: () => { },
			},
		};

		// Token 事件
		active.handlers.token = (event) => {
			if (active.terminal) return;

			try {
				const data = parseEventData<{ content: string }>(
					event.data,
				);

				if (!data.content) return;

				active.buffer += data.content;
				this.scheduleFlush(active);
			} catch {
				this.finish(run.id, 'error', 'Token 事件格式错误');
			}
		};

		// 生成成功
		active.handlers.done = () => {
			if (active.terminal) return;

			this.flush(active);

			this.finish(run.id, 'completed');

			// 主动断开，阻止 EOF 触发重连
			client.disconnect();
		};

		// 业务错误
		active.handlers.appError = (event) => {
			if (active.terminal) return;

			let message = 'AI 生成失败';

			try {
				const data = parseEventData<{
					code?: string;
					message?: string;
				}>(event.data);

				message = data.message || message;
			} catch {
				// 使用默认错误
			}

			this.finish(run.id, 'error', message);
			client.disconnect();
		};

		// 自动重连次数耗尽
		active.handlers.reconnectFailed = (event) => {
			if (active.terminal) return;

			this.finish(
				run.id,
				'error',
				event.error?.message || '连接中断，重连失败',
			);
		};

		// 身份认证失败
		active.handlers.authError = () => {
			if (active.terminal) return;

			this.finish(run.id, 'error', '登录状态失效，请重新登录');
			client.disconnect();
		};

		// 连接状态变化
		active.handlers.stateChange = (state) => {
			if (active.terminal) return;

			const statusMap: Record<string, RunStatus | undefined> = {
				connecting: 'connecting',
				open: 'streaming',
				reconnecting: 'reconnecting',
			};

			const status = statusMap[state];

			if (status) {
				useChatStore.getState().updateRun(run.id, {
					status,
				});

				useChatStore.getState().updateMessage(
					run.conversationId,
					run.assistantMessageId,
					{
						status:
							status === 'connecting' ? 'pending' : 'streaming',
					},
				);
			}
		};

		client
			.on('token', active.handlers.token)
			.on('done', active.handlers.done)
			.on('app_error', active.handlers.appError)
			.on('reconnect_failed', active.handlers.reconnectFailed)
			.on('auth_error', active.handlers.authError)
			.onStateChange(active.handlers.stateChange);

		this.activeRuns.set(run.id, active);

		await client.connect();
	}

	// 合并 Token，降低 Zustand 更新频率
	private scheduleFlush(active: ActiveRun) {
		if (active.rafId !== null) return;

		active.rafId = requestAnimationFrame(() => {
			active.rafId = null;
			this.flush(active);
		});
	}

	private flush(active: ActiveRun) {
		if (!active.buffer) return;

		const content = active.buffer;
		active.buffer = '';

		useChatStore.getState().appendMessageContent(
			active.run.conversationId,
			active.run.assistantMessageId,
			content,
		);
	}

	// 停止指定会话的生成
	stopConversation(conversationId: string) {
		const runId =
			useChatStore.getState().activeRunByConversation[
			conversationId
			];

		if (!runId) return;

		this.stopRun(runId);
	}

	stopRun(runId: string) {
		const active = this.activeRuns.get(runId);
		if (!active || active.terminal) return;

		// 先标记 terminal，避免 abort 触发的状态事件覆盖 cancelled
		this.flush(active);

		this.finish(runId, 'cancelled', '用户停止了生成');

		active.client.disconnect();

		// 再通知服务端取消
		/* try {
			await fetch(`/api/chat/runs/${runId}/cancel`, {
				method: 'POST',
				credentials: 'include',
			});
		} catch {
			// 前端已停止接收，但服务端取消状态尚未确认
			// 生产环境可以保留一个 cancel-pending 状态并允许查询任务状态
		} */
	}


	async resumeRun(runId: string) {
		const store = useChatStore.getState();
		const run = store.runs[runId];

		if (!run) {
			throw new Error('找不到生成任务');
		}

		if (!['error'].includes(run.status)) {
			throw new Error('当前任务状态不允许恢复');
		}

		const messages =
			store.messagesByConversation[run.conversationId] ?? [];

		const userMessage = messages.find(
			(item) => item.id === run.userMessageId,
		);

		if (!userMessage) {
			throw new Error('找不到原始用户消息');
		}

		const resumedRun: ChatRun = {
			...run,
			status: 'connecting',
			error: undefined,
			updatedAt: Date.now(),
		};

		store.addRun(resumedRun);

		store.updateMessage(
			run.conversationId,
			run.assistantMessageId,
			{
				status: 'streaming',
				error: undefined,
			},
		);

		await this.startRun(
			resumedRun,
			userMessage.content,
			run.lastEventId,
		);
	}


	async retryRun(oldRunId: string) {
		const store = useChatStore.getState();
		const oldRun = store.runs[oldRunId];

		if (!oldRun) {
			throw new Error('找不到原始生成任务');
		}

		const messages =
			store.messagesByConversation[oldRun.conversationId] ?? [];

		const userMessage = messages.find(
			(item) => item.id === oldRun.userMessageId,
		);

		if (!userMessage) {
			throw new Error('找不到原始用户消息');
		}

		const newRunId = createId();
		const newAssistantMessageId = createId();
		const now = Date.now();

		const assistantMessage: ChatMessage = {
			id: newAssistantMessageId,
			conversationId: oldRun.conversationId,
			role: 'assistant',
			content: '',
			status: 'pending',
			createdAt: now,
			runId: newRunId,
			retryOf: oldRun.assistantMessageId,
		};

		const newRun: ChatRun = {
			id: newRunId,
			conversationId: oldRun.conversationId,
			userMessageId: oldRun.userMessageId,
			assistantMessageId: newAssistantMessageId,
			status: 'connecting',
			startedAt: now,
			updatedAt: now,
		};

		store.addMessage(assistantMessage);
		store.addRun(newRun);

		await this.startRun(newRun, userMessage.content);

		return newRunId;
	}

	// 统一完成、失败、取消
	private finish(
		runId: string,
		status: RunStatus,
		error?: string,
	) {
		const active = this.activeRuns.get(runId);

		if (active?.terminal) return;

		if (active) {
			active.terminal = true;

			if (active.rafId !== null) {
				cancelAnimationFrame(active.rafId);
				active.rafId = null;
			}

			this.flush(active);

			this.detach(active);
			this.activeRuns.delete(runId);
		}

		useChatStore.getState().finishRun(runId, status, error);
	}

	private detach(active: ActiveRun) {
		const { client, handlers } = active;

		client
			.off('token', handlers.token)
			.off('done', handlers.done)
			.off('app_error', handlers.appError)
			.off('reconnect_failed', handlers.reconnectFailed)
			.off('auth_error', handlers.authError)
			.offStateChange(handlers.stateChange);
	}

	// 应用真正退出时调用，而不是聊天组件卸载时调用
	dispose() {
		for (const runId of this.activeRuns.keys()) {
			this.stopRun(runId);
		}
	}
}
