import { queryClient } from "@serene-ui/shared-frontend/shared";
import { QueryClientProvider } from "@tanstack/react-query";
import React from "react";

interface WithQueryProps {
    children: React.ReactNode;
}

export const WithQuery: React.FC<WithQueryProps> = ({ children }) => {
    return (
        <QueryClientProvider client={queryClient}>
            {children}
        </QueryClientProvider>
    );
};
