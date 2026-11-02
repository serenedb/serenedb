import { createContext, useContext, useState } from "react";
import { SettingsModal } from "../ui";
import { SettingsTabs } from "./consts";
import {
    ConsoleIcon,
    HomeIcon,
    NotificationsIcon,
    PalleteIcon,
} from "@serene-ui/shared-frontend/shared";
import { KeyboardIcon } from "@serene-ui/shared-frontend/shared";

interface SettingsModalTab {
    name: SettingsTabs;
    label: string;
    icon?: React.ReactNode;
}

interface SettingsModalContextType {
    open: boolean;
    setOpen: React.Dispatch<React.SetStateAction<boolean>>;
    selectedTab: string;
    setSelectedTab: React.Dispatch<React.SetStateAction<string>>;
    tabs: SettingsModalTab[];
}

const SettingsModalContext = createContext<
    SettingsModalContextType | undefined
>(undefined);

export const SettingsModalProvider = ({
    children,
}: {
    children: React.ReactNode;
}) => {
    const [open, setOpen] = useState(false);
    const [selectedTab, setSelectedTab] = useState<string>(
        SettingsTabs.General,
    );

    const tabs: SettingsModalTab[] = [
        {
            name: SettingsTabs.General,
            label: "General",
            icon: <HomeIcon />,
        },
        {
            name: SettingsTabs.Editor,
            label: "Editor",
            icon: <ConsoleIcon />,
        },
        {
            name: SettingsTabs.Notifications,
            label: "Notifications",
            icon: <NotificationsIcon />,
        },
        {
            name: SettingsTabs.Appearance,
            label: "Appearance",
            icon: <PalleteIcon />,
        },
        {
            name: SettingsTabs.Shortcuts,
            label: "Shortcuts",
            icon: <KeyboardIcon />,
        },
    ];

    return (
        <SettingsModalContext.Provider
            value={{
                open,
                setOpen,
                selectedTab,
                setSelectedTab,
                tabs,
            }}>
            {children}
            <SettingsModal />
        </SettingsModalContext.Provider>
    );
};

export const useSettingsModal = () => {
    const context = useContext(SettingsModalContext);
    if (!context) {
        throw new Error(
            "useSettingsModal must be used within a SettingsModalProvider",
        );
    }
    return context;
};
