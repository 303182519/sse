
import { createContext, useContext } from 'react';
import type { ReactNode } from 'react';

import { chatRuntime } from '../chat-runtime-instance';

const ChatRuntimeContext = createContext(chatRuntime);

export function ChatProvider({
  children,
}: {
  children: ReactNode;
}) {
  return (
    <ChatRuntimeContext.Provider value={chatRuntime}>
      {children}
    </ChatRuntimeContext.Provider>
  );
}

export function useChatRuntime() {
  return useContext(ChatRuntimeContext);
}
