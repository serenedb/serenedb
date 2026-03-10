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
        isOpen: false,
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

const DashboardsMenuContext = createContext<
    DashboardsMenuContextType | undefined
>(undefined);

export const DashboardsMenuProvider = ({
    children,
}: {
    children: React.ReactNode;
}) => {
    const [sections, setSections] = useState(createInitialSections);

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

                for (const [sectionId, nextSizeUnits] of Object.entries(updates)) {
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
