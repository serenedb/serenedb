import { useCallback, useEffect, useRef } from "react";

const SIDEBAR_FOCUS_SELECTOR = "[data-sidebar-focus-id]";
const SIDEBAR_SECTION_SELECTOR = "[data-sidebar-section-id]";
const EDITOR_FOCUSABLE_SELECTOR = [
    "textarea:not([disabled])",
    "input:not([disabled])",
    "button:not([disabled])",
    "select:not([disabled])",
    "a[href]",
    "[tabindex]:not([tabindex='-1'])",
    "[contenteditable='true']",
].join(", ");

interface SidebarFocusControllerOptions {
    sidebarRootSelector: string;
    editorRootSelectors: string[];
    sectionOrder: string[];
}

interface SidebarFocusMetadata {
    focusId: string | null;
    sectionId: string | null;
}

interface FocusElementOptions {
    shouldScrollIntoView?: boolean;
}

const canFocusElement = (element: HTMLElement | null) => {
    if (!element || !element.isConnected) {
        return false;
    }

    if (
        element.getAttribute("disabled") !== null ||
        element.getAttribute("aria-hidden") === "true"
    ) {
        return false;
    }

    return true;
};

const isVisibleSidebarElement = (element: HTMLElement | null) => {
    if (!element || !canFocusElement(element)) {
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

const scrollElementIntoView = (element: HTMLElement | null) => {
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

const focusElement = (
    element: HTMLElement | null,
    options: FocusElementOptions = {},
) => {
    if (!element || !canFocusElement(element)) {
        return false;
    }

    try {
        element.focus({ preventScroll: true });
    } catch {
        element.focus();
    }

    if (options.shouldScrollIntoView) {
        scrollElementIntoView(element);
    }

    return (
        document.activeElement === element ||
        element.contains(document.activeElement)
    );
};

const getSidebarFocusMetadata = (
    target: HTMLElement | null,
): SidebarFocusMetadata => {
    if (!target) {
        return {
            focusId: null,
            sectionId: null,
        };
    }

    const focusTarget = target.closest<HTMLElement>(SIDEBAR_FOCUS_SELECTOR);
    const sectionTarget = target.closest<HTMLElement>(SIDEBAR_SECTION_SELECTOR);

    return {
        focusId: focusTarget?.dataset.sidebarFocusId ?? null,
        sectionId:
            focusTarget?.dataset.sidebarSectionId ??
            sectionTarget?.dataset.sidebarSectionId ??
            null,
    };
};

const findSidebarElementByFocusId = (
    root: HTMLElement,
    focusId: string,
) =>
    Array.from(
        root.querySelectorAll<HTMLElement>(SIDEBAR_FOCUS_SELECTOR),
    ).find(
        (element) =>
            element.dataset.sidebarFocusId === focusId &&
            isVisibleSidebarElement(element),
    ) ?? null;

const findFirstSidebarElementInSection = (
    root: HTMLElement,
    sectionId: string,
) =>
    Array.from(
        root.querySelectorAll<HTMLElement>(SIDEBAR_FOCUS_SELECTOR),
    ).find(
        (element) =>
            element.dataset.sidebarSectionId === sectionId &&
            isVisibleSidebarElement(element),
    ) ?? null;

const findFirstFocusableEditorElement = (root: HTMLElement) => {
    if (root.matches(EDITOR_FOCUSABLE_SELECTOR) && canFocusElement(root)) {
        return root;
    }

    return (
        Array.from(
            root.querySelectorAll<HTMLElement>(EDITOR_FOCUSABLE_SELECTOR),
        ).find((element) => canFocusElement(element)) ?? null
    );
};

export const useSidebarFocusController = ({
    sidebarRootSelector,
    editorRootSelectors,
    sectionOrder,
}: SidebarFocusControllerOptions) => {
    const lastSidebarFocusIdRef = useRef<string | null>(null);
    const lastSidebarSectionIdRef = useRef<string | null>(null);
    const lastSidebarFocusBySectionRef = useRef<Record<string, string>>({});
    const lastEditorFocusedElementRef = useRef<HTMLElement | null>(null);
    const lastEditorRootSelectorRef = useRef<string | null>(null);

    const getSidebarRoot = useCallback(() => {
        if (typeof document === "undefined") {
            return null;
        }

        return document.querySelector<HTMLElement>(sidebarRootSelector);
    }, [sidebarRootSelector]);

    const getEditorRootSelectorsInPriorityOrder = useCallback(() => {
        const selectors = lastEditorRootSelectorRef.current
            ? [lastEditorRootSelectorRef.current, ...editorRootSelectors]
            : editorRootSelectors;

        return selectors.filter(
            (selector, index) => selectors.indexOf(selector) === index,
        );
    }, [editorRootSelectors]);

    const focusSidebarSection = useCallback(
        (sectionId: string, options: FocusElementOptions = {}) => {
            const sidebarRoot = getSidebarRoot();
            if (!sidebarRoot) {
                return false;
            }

            const rememberedFocusId =
                lastSidebarFocusBySectionRef.current[sectionId];
            if (rememberedFocusId) {
                const rememberedElement = findSidebarElementByFocusId(
                    sidebarRoot,
                    rememberedFocusId,
                );

                if (focusElement(rememberedElement, options)) {
                    return true;
                }
            }

            return focusElement(
                findFirstSidebarElementInSection(sidebarRoot, sectionId),
                options,
            );
        },
        [getSidebarRoot],
    );

    const focusSidebar = useCallback(() => {
        const sidebarRoot = getSidebarRoot();
        if (!sidebarRoot) {
            return false;
        }

        if (lastSidebarFocusIdRef.current) {
            const rememberedElement = findSidebarElementByFocusId(
                sidebarRoot,
                lastSidebarFocusIdRef.current,
            );

            if (focusElement(rememberedElement)) {
                return true;
            }
        }

        if (
            lastSidebarSectionIdRef.current &&
            focusSidebarSection(lastSidebarSectionIdRef.current)
        ) {
            return true;
        }

        for (const sectionId of sectionOrder) {
            if (focusSidebarSection(sectionId)) {
                return true;
            }
        }

        return focusElement(
            Array.from(
                sidebarRoot.querySelectorAll<HTMLElement>(SIDEBAR_FOCUS_SELECTOR),
            ).find((element) => isVisibleSidebarElement(element)) ?? null,
        );
    }, [focusSidebarSection, getSidebarRoot, sectionOrder]);

    const restoreSidebarFocusAfterRender = useCallback(() => {
        if (typeof window === "undefined") {
            return;
        }

        let attempts = 0;

        const tryFocus = () => {
            attempts += 1;

            if (focusSidebar() || attempts >= 12) {
                return;
            }

            window.requestAnimationFrame(tryFocus);
        };

        window.requestAnimationFrame(tryFocus);
    }, [focusSidebar]);

    const isSidebarFocused = useCallback(() => {
        if (typeof document === "undefined") {
            return false;
        }

        const sidebarRoot = getSidebarRoot();
        return !!sidebarRoot && sidebarRoot.contains(document.activeElement);
    }, [getSidebarRoot]);

    const focusLastEditor = useCallback(() => {
        if (typeof document === "undefined") {
            return false;
        }

        if (focusElement(lastEditorFocusedElementRef.current)) {
            return true;
        }

        for (const selector of getEditorRootSelectorsInPriorityOrder()) {
            const editorRoot = document.querySelector<HTMLElement>(selector);
            if (!editorRoot) {
                continue;
            }

            if (focusElement(findFirstFocusableEditorElement(editorRoot))) {
                lastEditorRootSelectorRef.current = selector;
                return true;
            }
        }

        return false;
    }, [getEditorRootSelectorsInPriorityOrder]);

    const focusRelativeSidebarSection = useCallback(
        (direction: "next" | "previous") => {
            if (sectionOrder.length === 0) {
                return false;
            }

            const activeElement =
                typeof document === "undefined"
                    ? null
                    : document.activeElement instanceof HTMLElement
                      ? document.activeElement
                      : null;
            const currentSectionId =
                getSidebarFocusMetadata(activeElement).sectionId ??
                lastSidebarSectionIdRef.current;
            const currentIndex =
                currentSectionId === null
                    ? -1
                    : sectionOrder.indexOf(currentSectionId);

            const nextIndex =
                direction === "next"
                    ? currentIndex + 1
                    : currentIndex >= 0
                      ? currentIndex - 1
                      : sectionOrder.length - 1;

            if (nextIndex < 0 || nextIndex >= sectionOrder.length) {
                return false;
            }

            return focusSidebarSection(sectionOrder[nextIndex], {
                shouldScrollIntoView: true,
            });
        },
        [focusSidebarSection, sectionOrder],
    );

    const focusNextSidebarSection = useCallback(
        () => focusRelativeSidebarSection("next"),
        [focusRelativeSidebarSection],
    );

    const focusPreviousSidebarSection = useCallback(
        () => focusRelativeSidebarSection("previous"),
        [focusRelativeSidebarSection],
    );

    useEffect(() => {
        if (typeof document === "undefined") {
            return;
        }

        const handleFocusIn = (event: FocusEvent) => {
            const target = event.target;
            if (!(target instanceof HTMLElement)) {
                return;
            }

            const sidebarRoot = getSidebarRoot();
            if (sidebarRoot?.contains(target)) {
                const { focusId, sectionId } = getSidebarFocusMetadata(target);

                if (focusId) {
                    lastSidebarFocusIdRef.current = focusId;
                }

                if (sectionId) {
                    lastSidebarSectionIdRef.current = sectionId;

                    if (focusId) {
                        lastSidebarFocusBySectionRef.current[sectionId] =
                            focusId;
                    }
                }

                return;
            }

            for (const selector of editorRootSelectors) {
                const editorRoot = document.querySelector<HTMLElement>(selector);
                if (!editorRoot?.contains(target)) {
                    continue;
                }

                lastEditorFocusedElementRef.current = target;
                lastEditorRootSelectorRef.current = selector;
                return;
            }
        };

        document.addEventListener("focusin", handleFocusIn, true);

        return () => {
            document.removeEventListener("focusin", handleFocusIn, true);
        };
    }, [editorRootSelectors, getSidebarRoot]);

    return {
        focusLastEditor,
        focusNextSidebarSection,
        focusPreviousSidebarSection,
        focusSidebar,
        isSidebarFocused,
        restoreSidebarFocusAfterRender,
    };
};
