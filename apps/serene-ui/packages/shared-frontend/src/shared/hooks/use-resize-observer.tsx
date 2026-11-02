import { useCallback, useEffect, useRef, useState } from "react";

export interface Size {
    width: number;
    height: number;
}

export const useResizeObserver = <T extends HTMLElement>() => {
    const ref = useRef<T | null>(null);
    const [size, setSize] = useState<Size>({ width: 0, height: 0 });

    const disconnect = useRef<(() => void) | null>(null);

    const setRef = useCallback((node: T | null) => {
        if (disconnect.current) {
            disconnect.current();
            disconnect.current = null;
        }

        if (node) {
            ref.current = node;
            const observer = new ResizeObserver(([entry]) => {
                if (entry?.contentRect) {
                    const { width, height } = entry.contentRect;
                    setSize({ width, height });
                }
            });
            observer.observe(node);

            disconnect.current = () => observer.disconnect();
        } else {
            ref.current = null;
        }
    }, []);

    useEffect(() => {
        return () => {
            if (disconnect.current) {
                disconnect.current();
            }
        };
    }, []);

    return { ref: setRef, size };
};
