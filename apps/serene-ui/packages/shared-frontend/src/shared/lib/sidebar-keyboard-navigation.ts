import type { KeyboardEvent } from "react";

export const SIDEBAR_FOCUS_SELECTOR = "[data-sidebar-focus-id]";
export const SIDEBAR_SECTION_SELECTOR = "[data-sidebar-section-id]";
export const SIDEBAR_PRIMARY_ACTION_SELECTOR =
    "[data-sidebar-primary-action='true']";
const activeSidebarFocusRestorers = new Map<string, () => void>();

interface FocusSidebarElementOptions {
    shouldScrollIntoView?: boolean;
}

const isFocusableElement = (element: HTMLElement | null) => {
    if (!element || !element.isConnected) {
        return false;
    }

    if (
        element.getAttribute("disabled") !== null ||
        element.getAttribute("aria-hidden") === "true"
    ) {
        return false;
    }

    return element.getClientRects().length > 0;
};

const isScrollableElement = (element: HTMLElement) => {
    const { overflowY } = window.getComputedStyle(element);

    return (
        /auto|scroll|overlay/.test(overflowY) &&
        element.scrollHeight > element.clientHeight
    );
};

const getNearestScrollableAncestor = (element: HTMLElement) => {
    if (typeof window === "undefined") {
        return null;
    }

    let currentElement = element.parentElement;

    while (currentElement) {
        if (isScrollableElement(currentElement)) {
            return currentElement;
        }

        currentElement = currentElement.parentElement;
    }

    return null;
};

export const scrollSidebarElementIntoView = (element: HTMLElement | null) => {
    if (!element || typeof window === "undefined") {
        return false;
    }

    const scrollContainer = getNearestScrollableAncestor(element);

    if (!scrollContainer) {
        element.scrollIntoView({
            block: "nearest",
            inline: "nearest",
        });
        return true;
    }

    const elementRect = element.getBoundingClientRect();
    const containerRect = scrollContainer.getBoundingClientRect();

    if (elementRect.top < containerRect.top) {
        scrollContainer.scrollTop -= containerRect.top - elementRect.top;
    } else if (elementRect.bottom > containerRect.bottom) {
        scrollContainer.scrollTop +=
            elementRect.bottom - containerRect.bottom;
    }

    return true;
};

export const focusSidebarElement = (
    element: HTMLElement | null,
    options: FocusSidebarElementOptions = {},
) => {
    if (!element || !isFocusableElement(element)) {
        return false;
    }

    try {
        element.focus({ preventScroll: true });
    } catch {
        element.focus();
    }

    if (options.shouldScrollIntoView) {
        scrollSidebarElementIntoView(element);
    }

    return (
        document.activeElement === element ||
        element.contains(document.activeElement)
    );
};

export const getSidebarSectionId = (element: HTMLElement | null) =>
    element?.closest<HTMLElement>(SIDEBAR_SECTION_SELECTOR)?.dataset
        .sidebarSectionId ?? null;

export const getSidebarFocusableElementsInSection = (
    element: HTMLElement | null,
) => {
    const sectionId = getSidebarSectionId(element);
    if (!sectionId || typeof document === "undefined") {
        return [];
    }

    return Array.from(
        document.querySelectorAll<HTMLElement>(SIDEBAR_FOCUS_SELECTOR),
    ).filter(
        (candidate) =>
            candidate.dataset.sidebarSectionId === sectionId &&
            isFocusableElement(candidate),
    );
};

export const focusSidebarRelativeItem = (
    element: HTMLElement,
    direction: "next" | "previous",
) => {
    const focusableElements = getSidebarFocusableElementsInSection(element);
    const currentIndex = focusableElements.indexOf(element);

    if (currentIndex < 0) {
        return false;
    }

    const nextIndex =
        direction === "next" ? currentIndex + 1 : currentIndex - 1;

    if (nextIndex < 0 || nextIndex >= focusableElements.length) {
        return false;
    }

    return focusSidebarElement(focusableElements[nextIndex], {
        shouldScrollIntoView: true,
    });
};

export const focusSidebarSectionHeader = (element: HTMLElement) => {
    const focusableElements = getSidebarFocusableElementsInSection(element);
    return focusSidebarElement(focusableElements[0] ?? null, {
        shouldScrollIntoView: true,
    });
};

export const focusSidebarRelativeSection = (
    element: HTMLElement,
    direction: "next" | "previous",
) => {
    if (typeof document === "undefined") {
        return false;
    }

    const focusableElements = Array.from(
        document.querySelectorAll<HTMLElement>(SIDEBAR_FOCUS_SELECTOR),
    ).filter((candidate) => isFocusableElement(candidate));
    const headersBySection = new Map<string, HTMLElement>();

    focusableElements.forEach((candidate) => {
        const sectionId = candidate.dataset.sidebarSectionId;
        if (!sectionId || headersBySection.has(sectionId)) {
            return;
        }

        headersBySection.set(sectionId, candidate);
    });

    const sectionHeaders = Array.from(headersBySection.values());
    const currentSectionId = getSidebarSectionId(element);
    const currentIndex = sectionHeaders.findIndex(
        (candidate) =>
            candidate.dataset.sidebarSectionId === currentSectionId,
    );

    if (currentIndex < 0) {
        return false;
    }

    const nextIndex =
        direction === "next" ? currentIndex + 1 : currentIndex - 1;

    if (nextIndex < 0 || nextIndex >= sectionHeaders.length) {
        return false;
    }

    return focusSidebarElement(sectionHeaders[nextIndex], {
        shouldScrollIntoView: true,
    });
};

export const focusSidebarItemByFocusId = (
    focusId: string,
    options: FocusSidebarElementOptions = {},
) => {
    if (typeof document === "undefined") {
        return false;
    }

    return focusSidebarElement(
        Array.from(
            document.querySelectorAll<HTMLElement>(SIDEBAR_FOCUS_SELECTOR),
        ).find((element) => element.dataset.sidebarFocusId === focusId) ??
            null,
        options,
    );
};

const getFocusedSidebarFocusId = (element: Element | null) =>
    element
        ?.closest<HTMLElement>(SIDEBAR_FOCUS_SELECTOR)
        ?.dataset.sidebarFocusId ?? null;

const shouldStopSidebarFocusRestore = (focusId: string) => {
    if (typeof document === "undefined") {
        return true;
    }

    const activeElement = document.activeElement;

    if (
        !(activeElement instanceof HTMLElement) ||
        activeElement === document.body ||
        activeElement === document.documentElement
    ) {
        return false;
    }

    return getFocusedSidebarFocusId(activeElement) !== focusId;
};

export const activateSidebarPrimaryAction = (element: HTMLElement) => {
    const primaryAction = element.matches(SIDEBAR_PRIMARY_ACTION_SELECTOR)
        ? element
        : element.querySelector<HTMLElement>(SIDEBAR_PRIMARY_ACTION_SELECTOR);

    if (!primaryAction) {
        return false;
    }

    primaryAction.click();
    return true;
};

export const restoreSidebarFocusById = (
    focusId: string,
    timeoutMs: number = 10_000,
) => {
    if (typeof window === "undefined" || typeof document === "undefined") {
        return;
    }

    activeSidebarFocusRestorers.get(focusId)?.();

    const startedAt = Date.now();
    let animationFrameId: number | null = null;
    let intervalId: number | null = null;
    let timeoutId: number | null = null;
    let observer: MutationObserver | null = null;
    let isDisposed = false;

    const cleanup = () => {
        if (isDisposed) {
            return;
        }

        isDisposed = true;

        if (animationFrameId !== null) {
            window.cancelAnimationFrame(animationFrameId);
        }

        if (intervalId !== null) {
            window.clearInterval(intervalId);
        }

        if (timeoutId !== null) {
            window.clearTimeout(timeoutId);
        }

        observer?.disconnect();
        document.removeEventListener("focusin", handleFocusIn, true);

        if (activeSidebarFocusRestorers.get(focusId) === cleanup) {
            activeSidebarFocusRestorers.delete(focusId);
        }
    };

    const tryRestore = () => {
        if (isDisposed) {
            return;
        }

        if (Date.now() - startedAt >= timeoutMs) {
            cleanup();
            return;
        }

        if (shouldStopSidebarFocusRestore(focusId)) {
            cleanup();
            return;
        }

        if (getFocusedSidebarFocusId(document.activeElement) === focusId) {
            return;
        }

        focusSidebarItemByFocusId(focusId);
    };

    const queueRestore = () => {
        if (isDisposed || animationFrameId !== null) {
            return;
        }

        animationFrameId = window.requestAnimationFrame(() => {
            animationFrameId = null;
            tryRestore();
        });
    };

    const handleFocusIn = () => {
        if (shouldStopSidebarFocusRestore(focusId)) {
            cleanup();
        }
    };

    activeSidebarFocusRestorers.set(focusId, cleanup);

    if (document.body) {
        observer = new MutationObserver(() => {
            queueRestore();
        });
        observer.observe(document.body, {
            childList: true,
            subtree: true,
        });
    }

    document.addEventListener("focusin", handleFocusIn, true);
    intervalId = window.setInterval(queueRestore, 150);
    timeoutId = window.setTimeout(cleanup, timeoutMs);

    queueRestore();

    return cleanup;
};

export const handleSidebarSectionHotkey = (
    event: KeyboardEvent<HTMLElement>,
) => {
    if (!(event.altKey && (event.ctrlKey || event.metaKey))) {
        return false;
    }

    const key = event.key.toLowerCase();
    const isNextSectionKey = event.key === "ArrowDown" || key === "j";
    const isPreviousSectionKey = event.key === "ArrowUp" || key === "k";

    if (!isNextSectionKey && !isPreviousSectionKey) {
        return false;
    }

    event.preventDefault();
    event.stopPropagation();

    return focusSidebarRelativeSection(
        event.currentTarget,
        isNextSectionKey ? "next" : "previous",
    );
};

export const handleSidebarListItemKeyDown = (
    event: KeyboardEvent<HTMLElement>,
    onActivate: () => void,
) => {
    if (handleSidebarSectionHotkey(event)) {
        return;
    }

    if (event.target !== event.currentTarget) {
        return;
    }

    const key = event.key.toLowerCase();

    if (event.key === "ArrowDown" || key === "j") {
        event.preventDefault();
        focusSidebarRelativeItem(event.currentTarget, "next");
        return;
    }

    if (event.key === "ArrowUp" || key === "k") {
        event.preventDefault();
        focusSidebarRelativeItem(event.currentTarget, "previous");
        return;
    }

    if (event.key === "ArrowLeft" || key === "h") {
        event.preventDefault();
        focusSidebarSectionHeader(event.currentTarget);
        return;
    }

    if (
        event.key === "ArrowRight" ||
        key === "l" ||
        event.key === "Enter" ||
        event.key === " "
    ) {
        event.preventDefault();
        onActivate();
    }
};
