import {
    ConnectionsModalProvider,
    SavedQueriesModalProvider,
    CommandModalProvider,
    SettingsModalProvider,
    SupportModalProvider,
    AuthorizeGithubProvider,
} from "@serene-ui/shared-frontend/features";
import { QueryResultsProvider } from "@serene-ui/shared-frontend/features";

interface WithFeaturesProps {
    children: React.ReactNode;
}

export const WithFeatures = ({ children }: WithFeaturesProps) => {
    return (
        <SettingsModalProvider>
            <AuthorizeGithubProvider>
                <SupportModalProvider>
                    <QueryResultsProvider>
                        <ConnectionsModalProvider>
                            <SavedQueriesModalProvider>
                                <CommandModalProvider>
                                    {children}
                                </CommandModalProvider>
                            </SavedQueriesModalProvider>
                        </ConnectionsModalProvider>
                    </QueryResultsProvider>
                </SupportModalProvider>
            </AuthorizeGithubProvider>
        </SettingsModalProvider>
    );
};
