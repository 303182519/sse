
import { ChatRuntime } from './services/chat-runtime';
import { authService } from '@/services/auth';

export const chatRuntime = new ChatRuntime({
    getAccessToken: () => authService.getAccessToken(),
});
