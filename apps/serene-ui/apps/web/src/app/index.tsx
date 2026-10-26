import {
    WithLanguage,
    WithQuery,
    WithRouter,
    WithTheme,
} from "@/app/providers";

export const App = () => {
    return (
        <WithQuery>
            <WithTheme>
                <WithLanguage>
                    <WithRouter />
                </WithLanguage>
            </WithTheme>
        </WithQuery>
    );
};
