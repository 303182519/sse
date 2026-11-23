
import { create } from 'zustand';
import type {
	ChatMessage,
	ChatRun,
	MessageStatus,
	RunStatus,
} from '../types/chat.types';

interface ChatState {
	messagesByConversation: Record<string, ChatMessage[]>;
	runs: Record<string, ChatRun>;
	activeRunByConversation: Record<string, string | undefined>;

	hydrateMessages: (
		conversationId: string,
		messages: ChatMessage[],
	) => void;

	addMessage: (message: ChatMessage) => void;

	appendMessageContent: (
		conversationId: string,
		messageId: string,
		content: string,
	) => void;

	updateMessage: (
		conversationId: string,
		messageId: string,
		patch: Partial<ChatMessage>,
	) => void;

	addRun: (run: ChatRun) => void;

	updateRun: (
		runId: string,
		patch: Partial<ChatRun>,
	) => void;

	finishRun: (
		runId: string,
		status: RunStatus,
		error?: string,
	) => void;
}

export const useChatStore = create<ChatState>((set, get) => ({
	messagesByConversation: {},
	runs: {},
	activeRunByConversation: {},

	hydrateMessages: (conversationId, messages) => {
		set((state) => ({
			messagesByConversation: {
				...state.messagesByConversation,
				[conversationId]: messages,
			},
		}));
	},

	addMessage: (message) => {
		set((state) => {
			const current =
				state.messagesByConversation[message.conversationId] ?? [];

			return {
				messagesByConversation: {
					...state.messagesByConversation,
					[message.conversationId]: [...current, message],
				},
			};
		});
	},

	appendMessageContent: (
		conversationId,
		messageId,
		content,
	) => {
		set((state) => {
			const current =
				state.messagesByConversation[conversationId] ?? [];

			return {
				messagesByConversation: {
					...state.messagesByConversation,
					[conversationId]: current.map((message) =>
						message.id === messageId
							? {
								...message,
								content: message.content + content,
							}
							: message,
					),
				},
			};
		});
	},

	updateMessage: (conversationId, messageId, patch) => {
		set((state) => {
			const current =
				state.messagesByConversation[conversationId] ?? [];

			return {
				messagesByConversation: {
					...state.messagesByConversation,
					[conversationId]: current.map((message) =>
						message.id === messageId
							? { ...message, ...patch }
							: message,
					),
				},
			};
		});
	},

	addRun: (run) => {
		set((state) => ({
			runs: {
				...state.runs,
				[run.id]: run,
			},
			activeRunByConversation: {
				...state.activeRunByConversation,
				[run.conversationId]: run.id,
			},
		}));
	},

	updateRun: (runId, patch) => {
		set((state) => {
			const run = state.runs[runId];
			if (!run) return state;

			return {
				runs: {
					...state.runs,
					[runId]: {
						...run,
						...patch,
						updatedAt: Date.now(),
					},
				},
			};
		});
	},

	finishRun: (runId, status, error) => {
		const run = get().runs[runId];
		if (!run) return;

		const messageStatus: MessageStatus =
			status === 'completed'
				? 'completed'
				: status === 'cancelled'
					? 'cancelled'
					: 'error';

		get().updateRun(runId, {
			status,
			error,
		});

		get().updateMessage(
			run.conversationId,
			run.assistantMessageId,
			{
				status: messageStatus,
				error,
			},
		);

		set((state) => {
			const active = {
				...state.activeRunByConversation,
			};

			if (active[run.conversationId] === runId) {
				delete active[run.conversationId];
			}

			return {
				activeRunByConversation: active,
			};
		});
	},
}));
