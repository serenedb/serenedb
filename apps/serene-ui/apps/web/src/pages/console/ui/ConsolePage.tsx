import { Console } from "@serene-ui/shared-frontend";
import { ConsoleErrorBoundary } from "./ConsoleErrorBoundary";

const ConsoleContent = () => {
    return <Console />;
};

export const ConsolePage = () => {
    return (
        <ConsoleErrorBoundary>
            <ConsoleContent />
        </ConsoleErrorBoundary>
    );
};
