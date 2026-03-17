import React, { createContext, useCallback, useContext, useState } from "react";

export type DashboardsMenuSectionId =
    | "favorites"
    | "dashboards"
    | "blockTemplates"
    | "savedQueries";

interface DashboardsMenuSectionState {
    isOpen: boolean;
    sizeUnits: number;
}

interface DashboardsMenuContextType {
    sections: Record<DashboardsMenuSectionId, DashboardsMenuSectionState>;
    toggleSection: (sectionId: DashboardsMenuSectionId) => void;
    setSectionSizeUnits: (
        updates: Partial<Record<DashboardsMenuSectionId, number>>,
    ) => void;
}

export const DASHBOARDS_MENU_SECTION_HEADER_HEIGHT = 36;
export const DASHBOARDS_MENU_SECTION_MIN_BODY_HEIGHT = 96;
export const DASHBOARDS_MENU_SECTION_HANDLE_HEIGHT = 6;
const DASHBOARDS_MENU_STATE_STORAGE_KEY = "serene-ui:dashboards-menu:state:v1";
const DASHBOARDS_MENU_SECTION_IDS: DashboardsMenuSectionId[] = [
    "favorites",
    "dashboards",
    "blockTemplates",
    "savedQueries",
];

const DEFAULT_SECTION_SIZE_UNITS = 1;

const createInitialSections = (): Record<
    DashboardsMenuSectionId,
    DashboardsMenuSectionState
> => ({
    favorites: {
        isOpen: false,
        sizeUnits: DEFAULT_SECTION_SIZE_UNITS,
    },
    dashboards: {
        isOpen: true,
        sizeUnits: DEFAULT_SECTION_SIZE_UNITS,
    },
    blockTemplates: {
        isOpen: false,
        sizeUnits: DEFAULT_SECTION_SIZE_UNITS,
    },
    savedQueries: {
        isOpen: false,
        sizeUnits: DEFAULT_SECTION_SIZE_UNITS,
    },
});

const readPersistedSections = () => {
    if (typeof window === "undefined") {
        return {};
    }

    try {
        const rawState = window.localStorage.getItem(
            DASHBOARDS_MENU_STATE_STORAGE_KEY,
        );

        if (!rawState) {
            return {};
        }

        const parsedState = JSON.parse(rawState);

        if (!parsedState || typeof parsedState !== "object") {
            return {};
        }

        return DASHBOARDS_MENU_SECTION_IDS.reduce<
            Partial<
                Record<
                    DashboardsMenuSectionId,
                    Partial<DashboardsMenuSectionState>
                >
            >
        >((acc, sectionId) => {
            const sectionState = parsedState[sectionId];

            if (!sectionState || typeof sectionState !== "object") {
                return acc;
            }

            const isOpen =
                typeof sectionState.isOpen === "boolean"
                    ? sectionState.isOpen
                    : undefined;
            const sizeUnits =
                typeof sectionState.sizeUnits === "number" &&
                Number.isFinite(sectionState.sizeUnits) &&
                sectionState.sizeUnits > 0
                    ? sectionState.sizeUnits
                    : undefined;

            if (isOpen === undefined && sizeUnits === undefined) {
                return acc;
            }

            acc[sectionId] = {
                ...(isOpen !== undefined ? { isOpen } : {}),
                ...(sizeUnits !== undefined ? { sizeUnits } : {}),
            };

            return acc;
        }, {});
    } catch {
        return {};
    }
};

const DashboardsMenuContext = createContext<
    DashboardsMenuContextType | undefined
>(undefined);

export const DashboardsMenuProvider = ({
    children,
}: {
    children: React.ReactNode;
}) => {
    const [sections, setSections] = useState(() => {
        const initialSections = createInitialSections();
        const persistedSections = readPersistedSections();

        if (!Object.keys(persistedSections).length) {
            return initialSections;
        }

        const nextSections = { ...initialSections };

        for (const sectionId of DASHBOARDS_MENU_SECTION_IDS) {
            const persistedSection = persistedSections[sectionId];

            if (!persistedSection) {
                continue;
            }

            nextSections[sectionId] = {
                isOpen: persistedSection.isOpen ?? nextSections[sectionId].isOpen,
                sizeUnits:
                    persistedSection.sizeUnits ??
                    nextSections[sectionId].sizeUnits,
            };
        }

        return nextSections;
    });

    React.useEffect(() => {
        if (typeof window === "undefined") {
            return;
        }

        const sectionState = DASHBOARDS_MENU_SECTION_IDS.reduce<
            Record<DashboardsMenuSectionId, DashboardsMenuSectionState>
        >((acc, sectionId) => {
            acc[sectionId] = {
                isOpen: sections[sectionId].isOpen,
                sizeUnits: sections[sectionId].sizeUnits,
            };
            return acc;
        }, {} as Record<DashboardsMenuSectionId, DashboardsMenuSectionState>);

        try {
            window.localStorage.setItem(
                DASHBOARDS_MENU_STATE_STORAGE_KEY,
                JSON.stringify(sectionState),
            );
        } catch {
            // Ignore localStorage write failures (e.g. privacy mode / quota).
        }
    }, [sections]);

    const toggleSection = useCallback((sectionId: DashboardsMenuSectionId) => {
        setSections((prevSections) => ({
            ...prevSections,
            [sectionId]: {
                ...prevSections[sectionId],
                isOpen: !prevSections[sectionId].isOpen,
            },
        }));
    }, []);

    const setSectionSizeUnits = useCallback(
        (updates: Partial<Record<DashboardsMenuSectionId, number>>) => {
            setSections((prevSections) => {
                const nextSections = { ...prevSections };

                for (const [sectionId, nextSizeUnits] of Object.entries(
                    updates,
                )) {
                    if (nextSizeUnits === undefined) {
                        continue;
                    }

                    nextSections[sectionId as DashboardsMenuSectionId] = {
                        ...nextSections[sectionId as DashboardsMenuSectionId],
                        sizeUnits: Math.max(nextSizeUnits, 0.01),
                    };
                }

                return nextSections;
            });
        },
        [],
    );

    return (
        <DashboardsMenuContext.Provider
            value={{
                sections,
                toggleSection,
                setSectionSizeUnits,
            }}>
            {children}
        </DashboardsMenuContext.Provider>
    );
};

export const useDashboardsMenu = () => {
    const context = useContext(DashboardsMenuContext);

    if (!context) {
        throw new Error(
            "useDashboardsMenu must be used within a DashboardsMenuProvider",
        );
    }

    return context;
};
