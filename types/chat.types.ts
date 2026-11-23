
export type MessageRole = 'user' | 'assistant';

export type MessageStatus =
	| 'pending'
	| 'streaming'
	| 'completed'
	| 'cancelled'
	| 'error';

export type RunStatus =
	| 'connecting'
	| 'streaming'
	| 'reconnecting'
	| 'completed'
	| 'cancelled'
	| 'error';

export interface ChatMessage {
	id: string;
	conversationId: string;
	role: MessageRole;
	content: string;
	status: MessageStatus;
	createdAt: number;
	error?: string;
	runId?: string;
	retryOf?: string;
}

export interface ChatRun {
	id: string;
	conversationId: string;
	userMessageId: string;
	assistantMessageId: string;

	status: RunStatus;

	// 最后收到的 SSE 事件 ID
	lastEventId?: string;

	error?: string;

	startedAt: number;
	updatedAt: number;
}

export interface SendMessageParams {
	conversationId: string;
	content: string;
}
