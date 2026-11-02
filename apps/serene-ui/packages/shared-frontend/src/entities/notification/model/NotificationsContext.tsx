import { createContext, useContext, useState } from "react";
import { INotification } from "./types";

interface NotificationsContextType {
    notifications: INotification[];
    addNotification: (notification: INotification) => void;
    removeNotification: (notificationId: INotification["id"]) => void;
    clearNotifications: () => void;
}

export const NotificationsContext = createContext<
    NotificationsContextType | undefined
>(undefined);

export const useNotifications = () => {
    const context = useContext(NotificationsContext);
    if (!context) {
        throw new Error(
            "useNotifications must be used within a NotificationsProvider",
        );
    }
    return context;
};
