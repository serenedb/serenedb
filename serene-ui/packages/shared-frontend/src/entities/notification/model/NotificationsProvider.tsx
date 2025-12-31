import { createContext, useContext, useState } from "react";
import { INotification } from "./types";
import { NotificationsContext } from "./NotificationsContext";

export const NotificationsProvider = ({
    children,
}: {
    children: React.ReactNode;
}) => {
    const [notifications, setNotifications] = useState<INotification[]>([]);

    const addNotification = (notification: INotification) => {
        setNotifications((prev) => [...prev, notification]);
    };

    const removeNotification = (notificationId: INotification["id"]) => {
        setNotifications((prev) => prev.filter((n) => n.id !== notificationId));
    };

    const clearNotifications = () => {
        setNotifications([]);
    };

    return (
        <NotificationsContext.Provider
            value={{
                notifications,
                addNotification,
                removeNotification,
                clearNotifications,
            }}>
            {children}
        </NotificationsContext.Provider>
    );
};
