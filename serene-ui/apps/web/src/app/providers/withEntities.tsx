import {
    ConnectionProvider,
    DatabasesProvider,
} from "@serene-ui/shared-frontend/entities";
import { NotificationsProvider } from "@serene-ui/shared-frontend/entities";
import { QueryHistoryProvider } from "@serene-ui/shared-frontend/entities";

export const WithEntities = ({ children }: { children: React.ReactNode }) => {
    return (
        <NotificationsProvider>
            <ConnectionProvider>
                <DatabasesProvider>
                    <QueryHistoryProvider>{children}</QueryHistoryProvider>
                </DatabasesProvider>
            </ConnectionProvider>
        </NotificationsProvider>
    );
};
