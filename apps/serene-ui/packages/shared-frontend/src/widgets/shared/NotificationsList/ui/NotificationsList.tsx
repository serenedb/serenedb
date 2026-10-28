import { useNotifications } from "@serene-ui/shared-frontend/entities";
import {
    Button,
    CheckIcon,
    CrossIcon,
} from "@serene-ui/shared-frontend/shared";

export const NotificationsList = () => {
    const { notifications, removeNotification } = useNotifications();

    return (
        <div className="flex flex-col overflow-y-auto max-h-[200px]">
            {notifications.map((notification) => (
                <div
                    key={notification.id}
                    className={`p-2 flex justify-between items-center ${
                        notification.id !==
                        notifications[notifications.length - 1].id
                            ? "border-b border-border"
                            : ""
                    }`}>
                    <div className="flex gap-2">
                        <div className="flex gap-2">
                            <div className="flex mt-0.5 items-center gap-2 border border-green-500/30 h-4 w-4 rounded-full justify-center">
                                {notification.type === "success" && (
                                    <CheckIcon className="w-3 text-green-500/30" />
                                )}
                                {notification.type === "error" && (
                                    <CrossIcon className="w-3 text-red-500/30" />
                                )}
                            </div>
                        </div>
                        <div className="flex flex-col gap-1">
                            <p className="text-xs">{notification.message}</p>
                            <p className="text-xs">
                                {new Date(
                                    notification.createdAt,
                                ).toLocaleString()}
                            </p>
                        </div>
                    </div>
                    <Button
                        variant="ghost"
                        size="iconSmall"
                        onClick={() => removeNotification(notification.id)}>
                        <CrossIcon className="size-2.5" />
                    </Button>
                </div>
            ))}
        </div>
    );
};
