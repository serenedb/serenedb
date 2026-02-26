import { DownloadResultsButton } from "@serene-ui/shared-frontend/features";
import {
    Button,
    Tabs,
    TabsList,
    TabsTrigger,
    TreeColumnsIcon,
} from "@serene-ui/shared-frontend/shared";
import { TimelineCard, type TimelineItem } from "../../TimelineCard";

interface QueryResultsFooterProps {
    children: React.ReactNode;
    rows?: Record<string, unknown>[];
    created_at?: string;
    execution_started_at?: string;
    execution_finished_at?: string;
    received_at?: string;
}

export const QueryResultsFooter: React.FC<QueryResultsFooterProps> = ({
    children,
    rows,
    created_at,
    execution_started_at,
    execution_finished_at,
    received_at,
}) => {
    const queueTime =
        created_at && execution_started_at
            ? new Date(execution_started_at).getTime() -
              new Date(created_at).getTime()
            : 0;
    const execTime =
        execution_started_at && execution_finished_at
            ? new Date(execution_finished_at).getTime() -
              new Date(execution_started_at).getTime()
            : 0;
    const transferTime =
        execution_finished_at && received_at
            ? new Date(received_at).getTime() -
              new Date(execution_finished_at).getTime()
            : 0;

    const executionTime = execTime > 0 ? execTime : null;

    const timelineItems: TimelineItem[] = [
        { name: "Queue", time: queueTime, color: "rgb(234, 179, 8)" },
        { name: "Execution", time: execTime, color: "rgb(34, 197, 94)" },
        { name: "Transfer", time: transferTime, color: "rgb(59, 130, 246)" },
    ];
    return (
        <Tabs defaultValue="viewer" className="h-full flex flex-col min-h-0">
            <div className="flex-1 min-h-0">{children}</div>
            <TabsList className="mt-0 h-max px-0">
                <div className="flex w-full p-2 border-t border-border justify-between">
                    <div className="flex gap-1">
                        <Button variant="thirdly">
                            <TreeColumnsIcon />
                            {rows?.length || 0}{" "}
                            {rows?.length === 1 ? "element" : "elements"}
                        </Button>
                        <TimelineCard
                            title="Query Execution Timeline"
                            items={timelineItems}
                            displayTime={executionTime}
                            disabled={true}
                        />
                    </div>
                    <div className="flex gap-1">
                        <TabsTrigger value="viewer">Viewer</TabsTrigger>
                        <TabsTrigger className="px-2.5 w-max" value="json">
                            JSON
                        </TabsTrigger>
                    </div>
                    <div className="flex gap-1">
                        <DownloadResultsButton rows={rows} />
                    </div>
                </div>
            </TabsList>
        </Tabs>
    );
};
