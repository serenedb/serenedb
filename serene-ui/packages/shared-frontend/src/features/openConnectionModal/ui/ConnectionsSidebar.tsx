import {
    useConnection,
    useDeleteConnection,
    useGetConnections,
} from "@serene-ui/shared-frontend/entities";
import {
    Button,
    cn,
    ConnectionIcon,
    DeleteIcon,
    PlusIcon,
} from "@serene-ui/shared-frontend/shared";
import { useConnectionsModal } from "../model/ConnectionsModalContext";
import { EMPTY_CONNECTION } from "../model/ConnectionsModalProvider";

export const ConnectionsSidebar = () => {
    const { setCurrentConnection, currentConnection, isGlobalDisabled } =
        useConnectionsModal();
    const { data: connections } = useGetConnections();
    const { mutateAsync: deleteConnection } = useDeleteConnection();
    const {
        setCurrentConnection: setConnection,
        currentConnection: connection,
    } = useConnection();

    return (
        <div
            className={cn(
                "w-[260px] border-r flex flex-col",
                isGlobalDisabled ? "opacity-50 pointer-events-none" : "",
            )}>
            <div className="pl-4 pr-2 py-2 border-b flex items-center justify-between">
                <p className="text-sm font-medium">Connections</p>
                <Button
                    onClick={() => setCurrentConnection(EMPTY_CONNECTION)}
                    variant="secondary"
                    size="iconSmall"
                    aria-label="Add connection">
                    <PlusIcon className="size-3.5" />
                </Button>
            </div>
            {!connections?.length ? (
                <div className="flex flex-1 items-center justify-center">
                    <div className="px-6 py-4 bg-secondary rounded-md w-max flex flex-col items-center">
                        <p>No connections yet!</p>
                        <p className="text-sm font-light opacity-30">
                            Want to add some?
                        </p>
                    </div>
                </div>
            ) : (
                <div className="flex flex-1 flex-col gap-0">
                    {connections?.map((conn) => (
                        <Button
                            asChild
                            className={cn(
                                "flex justify-start items-start h-auto w-full rounded-none pl-4 pr-2 cursor-pointer",
                                {
                                    "bg-accent":
                                        conn.id === currentConnection.id,
                                },
                            )}
                            variant="ghost"
                            key={conn.id}
                            onClick={() => {
                                setCurrentConnection({
                                    ...conn,
                                    ssl: !!conn.ssl,
                                });
                            }}>
                            <div className="flex w-full min-w-0">
                                <div className="flex items-center gap-2 min-w-0 flex-1">
                                    <ConnectionIcon className="mt-0.5 flex-shrink-0" />
                                    <div className="flex flex-col min-w-0 flex-1">
                                        <p className="text-md font-medium truncate">
                                            {conn.name}
                                        </p>
                                        <p className="text-xs text-secondary-foreground/50 truncate">
                                            {conn.type}
                                        </p>
                                    </div>
                                </div>
                                <div className="flex h-full items-center justify-center ml-auto">
                                    <Button
                                        variant="ghost"
                                        size="iconSmall"
                                        aria-label="Delete connection"
                                        onClick={(e) => {
                                            e.stopPropagation();
                                            if (
                                                connection.connectionId ===
                                                    conn.id &&
                                                connection.database ===
                                                    conn.database
                                            ) {
                                                setConnection({
                                                    connectionId: -1,
                                                    database: "",
                                                });
                                            }
                                            deleteConnection({
                                                id: conn.id,
                                            });
                                            setCurrentConnection(
                                                EMPTY_CONNECTION,
                                            );
                                        }}>
                                        <DeleteIcon className="size-3.5" />
                                    </Button>
                                </div>
                            </div>
                        </Button>
                    ))}
                </div>
            )}
        </div>
    );
};
