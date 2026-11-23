
import type { ChatMessage } from './types/chat.types';
import { MarkdownRenderer } from './MarkdownRenderer';

export function ChatMessageItem({
  message,
}: {
  message: ChatMessage;
}) {
  const isAssistant = message.role === 'assistant';

  return (
    <div className="chat-message">
      <div className="chat-message__content">
        {isAssistant ? (
          <MarkdownRenderer content={message.content} />
        ) : (
          <div className="whitespace-pre-wrap">
            {message.content}
          </div>
        )}
      </div>

      {message.status === 'pending' && (
        <div>正在准备生成...</div>
      )}

      {message.status === 'streaming' && (
        <span className="animate-pulse">▍</span>
      )}

      {message.status === 'cancelled' && (
        <div className="text-gray-500">
          已停止生成
        </div>
      )}

      {message.status === 'error' && (
        <div className="text-red-500">
          {message.error || '生成失败，请重试'}
        </div>
      )}
    </div>
  );
}


function MessageErrorActions({
  runId,
}: {
  runId: string;
}) {
  const runtime = useChatRuntime();

  return (
    <div className="flex gap-2">
      <button onClick={() => runtime.resumeRun(runId)}>
        继续恢复
      </button>

      <button onClick={() => runtime.retryRun(runId)}>
        重新生成
      </button>
    </div>
  );
}