import { useCallback, useRef } from 'react';

import { SSEClient } from '@/services/sse';

import { useChatStore } from '../stores/chat.store';

import type { ChatMessage } from '../types/chat.types';

const API_URL = '/api/chat/stream';

export function useChatStream() {
	const clientRef = useRef<SSEClient | null>(null);

	// 缓冲区：避免每收到一个 Token 就更新 Zustand
	const bufferRef = useRef('');

	const rafRef = useRef<number | null>(null);

	const targetRef = useRef<{
		conversationId: string;
		messageId: string;
	} | null>(null);

	const {
		addMessage,
		appendContent,
		updateMessageStatus,
		setActiveMessage,
	} = useChatStore.getState();

	// 将缓冲区内容批量刷新到 Zustand
	const flushBuffer = useCallback(() => {
		const target = targetRef.current;

		if (!target || !bufferRef.current) return;

		const content = bufferRef.current;

		bufferRef.current = '';

		appendContent(
			target.conversationId,
			target.messageId,
			content,
		);
	}, []);

	// 收到 Token 后不立即更新状态
	const scheduleFlush = useCallback(() => {
		if (rafRef.current !== null) return;

		rafRef.current = requestAnimationFrame(() => {
			rafRef.current = null;
			flushBuffer();
		});
	}, [flushBuffer]);

	// 停止当前生成
	const stop = useCallback(() => {
		const target = targetRef.current;

		if (target) {
			flushBuffer();

			updateMessageStatus(
				target.conversationId,
				target.messageId,
				'cancelled',
			);
		}

		// 主动断开 SSE
		clientRef.current?.disconnect();

		clientRef.current = null;

		targetRef.current = null;

		setActiveMessage(null);

		if (rafRef.current !== null) {
			cancelAnimationFrame(rafRef.current);
			rafRef.current = null;
		}

		bufferRef.current = '';
	}, [flushBuffer]);

	// 发送消息
	const sendMessage = useCallback(
		async (
			conversationId: string,
			content: string,
		) => {
			// 同一个前端只允许一个生成任务
			if (clientRef.current) {
				stop();
			}

			const userMessageId = crypto.randomUUID();

			const assistantMessageId = crypto.randomUUID();

			// 创建用户消息
			const userMessage: ChatMessage = {
				id: userMessageId,
				conversationId,
				role: 'user',
				content,
				status: 'completed',
				createdAt: Date.now(),
			};

			// 创建 AI 占位消息
			const assistantMessage: ChatMessage = {
				id: assistantMessageId,
				conversationId,
				role: 'assistant',
				content: '',
				status: 'streaming',
				createdAt: Date.now(),
			};

			addMessage(conversationId, userMessage);

			addMessage(conversationId, assistantMessage);

			targetRef.current = {
				conversationId,
				messageId: assistantMessageId,
			};

			setActiveMessage(assistantMessageId);

			bufferRef.current = '';

			// 每次生成都使用独立请求 ID
			const requestId = crypto.randomUUID();

			const client = new SSEClient({
				url: API_URL,
				method: 'POST',

				headers: async () => ({
					'Content-Type': 'application/json',
					Authorization: `Bearer ${getAccessToken()}`,
					'X-Request-ID': requestId,
				}),

				body: JSON.stringify({
					conversationId,
					message: content,
					requestId,
				}),

				// 一次性 AI 生成，默认不盲目重放 POST
				reconnect: {
					maxRetries: 0,
				},
			});

			clientRef.current = client;

			// 1. 收到 Token
			client.on('token', (event) => {
				const data = JSON.parse(event.data);

				bufferRef.current += data.content;

				scheduleFlush();
			});

			// 2. 正常完成
			client.on('done', () => {
				flushBuffer();

				updateMessageStatus(
					conversationId,
					assistantMessageId,
					'completed',
				);

				client.disconnect();

				if (clientRef.current === client) {
					clientRef.current = null;
					targetRef.current = null;
					setActiveMessage(null);
				}
			});

			// 3. 模型业务错误
			client.on('app_error', (event) => {
				flushBuffer();

				const data = JSON.parse(event.data);

				updateMessageStatus(
					conversationId,
					assistantMessageId,
					'error',
				);

				client.disconnect();

				if (clientRef.current === client) {
					clientRef.current = null;
					targetRef.current = null;
					setActiveMessage(null);
				}

				console.error('AI 生成失败:', data.message);
			});

			// 4. SSE 连接最终失败
			client.on('reconnect_failed', () => {
				flushBuffer();

				updateMessageStatus(
					conversationId,
					assistantMessageId,
					'error',
				);

				if (clientRef.current === client) {
					clientRef.current = null;
					targetRef.current = null;
					setActiveMessage(null);
				}
			});

			// 5. 连接建立
			await client.connect();
		},
		[
			addMessage,
			flushBuffer,
			scheduleFlush,
			setActiveMessage,
			stop,
			updateMessageStatus,
		],
	);

	return {
		sendMessage,
		stop,
	};
}

// 示例：替换成你项目里的认证模块
function getAccessToken() {
	return localStorage.getItem('access_token') ?? '';
}