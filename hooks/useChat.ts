
import { useChatRuntime } from '../providers/chat-provider';
import { useChatStore } from '../stores/chat.store';

export function useChat(conversationId: string) {
	const runtime = useChatRuntime();

	const messages = useChatStore((state) =>
		state.messagesByConversation[conversationId] ?? [],
	);

	const activeRunId = useChatStore((state) =>
		state.activeRunByConversation[conversationId],
	);

	const activeRun = useChatStore((state) =>
		activeRunId ? state.runs[activeRunId] : undefined,
	);

	const sendMessage = async (content: string) => {
		const value = content.trim();

		if (!value) return;

		return runtime.sendMessage(conversationId, value);
	};

	const stop = () => {
		runtime.stopConversation(conversationId);
	};

	return {
		messages,
		activeRun,
		isGenerating:
			activeRun?.status === 'connecting' ||
			activeRun?.status === 'streaming' ||
			activeRun?.status === 'reconnecting',

		sendMessage,
		stop,
	};
}

/* 
  
function ChatWindow({
	conversationId,
}: {
	conversationId: string;
}) {
	const {
		messages,
		activeRun,
		isGenerating,
		sendMessage,
		stop,
	} = useChat(conversationId);

	return (
		<div>
			<ChatMessageList messages={messages} />

			{activeRun?.status === 'reconnecting' && (
				<div>连接中断，正在尝试恢复...</div>
			)}

			<ChatInput
				onSend={sendMessage}
				onStop={stop}
				isGenerating={isGenerating}
			/>
		</div>
	);
}
 */