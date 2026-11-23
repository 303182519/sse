function ChatMessageList({
  messages,
}: {
  messages: ChatMessage[];
}) {
  const content = messages
    .map((item) => item.content)
    .join('');

  const {
    containerRef,
    handleScroll,
  } = useChatScroll(content);

  return (
    <div
      ref={containerRef}
      onScroll={handleScroll}
      className="chat-message-list"
    >
      {messages.map((message) => (
        <ChatMessageItem
          key={message.id}
          message={message}
        />
      ))}
    </div>
  );
}