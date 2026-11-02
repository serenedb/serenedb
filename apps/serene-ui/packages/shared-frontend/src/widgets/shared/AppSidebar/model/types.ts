import type { JSX } from "react";

export interface SidebarButton {
    title: string;
    icon: JSX.Element;
    link?: string;
    action?: () => void;
}
