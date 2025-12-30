import { Component, type ReactNode } from "react";
import { AlertTriangle } from "lucide-react";

interface ConsoleErrorBoundaryProps {
    children: ReactNode;
}

interface ErrorBoundaryState {
    hasError: boolean;
    error: Error | null;
}

export class ConsoleErrorBoundary extends Component<
    ConsoleErrorBoundaryProps,
    ErrorBoundaryState
> {
    state: ErrorBoundaryState = {
        hasError: false,
        error: null,
    };

    static getDerivedStateFromError(error: Error): ErrorBoundaryState {
        return {
            hasError: true,
            error,
        };
    }

    componentDidCatch(error: Error, errorInfo: React.ErrorInfo) {
        console.error("Console Error:", error, errorInfo);
    }

    render() {
        if (this.state.hasError) {
            return (
                <div className="flex h-full w-full items-center justify-center bg-background">
                    <div className="flex max-w-md flex-col items-center gap-4 rounded-lg border border-destructive/50 bg-destructive/10 p-6 text-center">
                        <AlertTriangle className="h-12 w-12 text-destructive" />
                        <div className="flex flex-col gap-2">
                            <h2 className="text-lg font-semibold text-destructive">
                                Something went wrong in Console
                            </h2>
                            <p className="text-sm text-muted-foreground">
                                {this.state.error?.message ||
                                    "An unexpected error occurred"}
                            </p>
                        </div>
                        <button
                            onClick={() => {
                                this.setState({ hasError: false, error: null });
                                window.location.reload();
                            }}
                            className="rounded-md bg-primary px-4 py-2 text-sm font-medium text-primary-foreground hover:bg-primary/90">
                            Reload Console
                        </button>
                    </div>
                </div>
            );
        }

        return this.props.children;
    }
}
