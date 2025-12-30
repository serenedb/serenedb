import { useEffect, useRef } from "react";
import { useNotifications } from "@serene-ui/shared-frontend/entities";
import { toast } from "sonner";
import type { ConsoleTab } from "@serene-ui/shared-frontend/widgets";

export interface UseConsoleNotificationsProps {
    tabs: ConsoleTab[];
    selectedTabId: number;
}

type NotifyStatus = "success" | "failed";

const isNotifyStatus = (status: string): status is NotifyStatus =>
    status === "success" || status === "failed";

export const useConsoleNotifications = ({
    tabs,
    selectedTabId,
}: UseConsoleNotificationsProps): void => {
    const { addNotification } = useNotifications();
    const notifiedJobIds = useRef(new Set<number>());

    useEffect(() => {
        const activeJobIds = new Set<number>();

        const notify = (
            jobId: number,
            status: NotifyStatus,
            tabIndex: number,
        ) => {
            const tabNumber = tabIndex + 1;
            const isFailed = status === "failed";

            const message = isFailed
                ? `Query ${tabNumber} failed`
                : `Query ${tabNumber} executed successfully`;

            if (isFailed) {
                toast.error(message);
            } else toast.success(message);

            addNotification({
                id: jobId,
                type: isFailed ? "error" : "success",
                message,
                createdAt: Date.now(),
            });

            notifiedJobIds.current.add(jobId);
        };

        tabs.forEach((tab, tabIndex) => {
            tab.results.forEach((result) => {
                activeJobIds.add(result.jobId);

                if (!isNotifyStatus(result.status)) {
                    return;
                }

                if (!notifiedJobIds.current.has(result.jobId)) {
                    notifiedJobIds.current.add(result.jobId);
                    if (tab.id !== selectedTabId) {
                        notify(result.jobId, result.status, tabIndex);
                    }
                }
            });
        });

        notifiedJobIds.current.forEach((jobId) => {
            if (!activeJobIds.has(jobId)) {
                notifiedJobIds.current.delete(jobId);
            }
        });
    }, [tabs, selectedTabId, addNotification]);
};
