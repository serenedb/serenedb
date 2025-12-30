import {
    Button,
    ButtonCard,
    ButtonCardButtonContent,
    ButtonCardContent,
    NotificationsIcon,
} from "@serene-ui/shared-frontend/shared";
import { NotificationsList } from "./NotificationsList";
import { useNotifications } from "@serene-ui/shared-frontend/entities";

export const NotificationsListButton = () => {
    const { notifications, clearNotifications } = useNotifications();
    return (
        <ButtonCard>
            <ButtonCardButtonContent
                asChild
                size={"iconSmall"}
                variant="ghost"
                className="relative">
                <div>
                    <NotificationsIcon />
                    {notifications.length > 0 && (
                        <div className="absolute top-0 right-0 min-w-3.5 h-3.5 bg-red-500 rounded-full flex items-center justify-center">
                            <p className="text-white text-xs">
                                {notifications.length}
                            </p>
                        </div>
                    )}
                </div>
            </ButtonCardButtonContent>
            {notifications.length > 0 && (
                <ButtonCardContent className="p-0">
                    <div className="flex items-center border-b border-border p-2 justify-between">
                        <div className="flex items-center gap-2">
                            <p className="text-sm">Notifications</p>
                            <div className="w-3.5 h-3.5 flex gap-2 bg-red-500 rounded-full items-center justify-center">
                                <p className="text-white text-xs">
                                    {notifications.length}
                                </p>
                            </div>
                        </div>
                        <Button
                            variant={"outline"}
                            size={"small"}
                            onClick={clearNotifications}>
                            Clear
                        </Button>
                    </div>
                    <NotificationsList />
                </ButtonCardContent>
            )}
        </ButtonCard>
    );
};
