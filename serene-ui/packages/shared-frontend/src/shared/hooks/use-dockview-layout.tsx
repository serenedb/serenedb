import { useLayoutEffect } from "react";

import { useResizeObserver } from "./use-resize-observer";

type DockviewLayoutApi = {
    layout?: (width: number, height: number) => void;
} | null | undefined;

export const useDockviewLayoutSync = <T extends HTMLElement>(
    api: DockviewLayoutApi,
) => {
    const { ref, size } = useResizeObserver<T>();

    useLayoutEffect(() => {
        if (!api || size.width <= 0 || size.height <= 0) {
            return;
        }

        api.layout?.(size.width, size.height);
    }, [api, size.width, size.height]);

    return ref;
};
