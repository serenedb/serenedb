import { createContext, useContext } from "react";

export type CurrentConnection = {
    connectionId: number;
    database?: string;
};
interface ConnectionContextType {
    currentConnection: CurrentConnection;
    setCurrentConnection: React.Dispatch<
        React.SetStateAction<CurrentConnection>
    >;
}

export const ConnectionContext = createContext<ConnectionContextType | undefined>(
    undefined,
);

export const useConnection = () => {
    const context = useContext(ConnectionContext);
    if (!context) {
        throw new Error(
            "useConnection must be used within a ConnectionProvider",
        );
    }
    return context;
};
