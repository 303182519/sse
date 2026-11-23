import {
	useEffect,
	useRef,
} from 'react';

export function useChatScroll(
	content: string,
) {
	const containerRef =
		useRef<HTMLDivElement>(null);

	const shouldAutoScrollRef = useRef(true);

	useEffect(() => {
		const element = containerRef.current;

		if (!element) return;

		if (shouldAutoScrollRef.current) {
			element.scrollTop = element.scrollHeight;
		}
	}, [content]);

	function handleScroll() {
		const element = containerRef.current;

		if (!element) return;

		const distance =
			element.scrollHeight -
			element.scrollTop -
			element.clientHeight;

		shouldAutoScrollRef.current = distance < 100;
	}

	return {
		containerRef,
		handleScroll,
	};
}