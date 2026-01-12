import type { Preview, Decorator } from "@storybook/react-vite";
import { WithQuery, WithTheme, WithLanguage } from "../src/app/providers";
import { AppLayout } from "../src/app/layouts";
import { createMemoryRouter, RouterProvider } from "react-router-dom";
import "../src/index.css";

const preview: Preview = {
    parameters: {
        controls: {
            matchers: {
                color: /(background|color)$/i,
                date: /Date$/i,
            },
        },

        a11y: {
            // 'todo' - show a11y violations in the test UI only
            // 'error' - fail CI on a11y violations
            // 'off' - skip a11y checks entirely
            test: "error",
        },
    },
    decorators: [
        ((Story, context) => {
            const route = context.parameters.route;

            if (route) {
                const router = createMemoryRouter(
                    [
                        {
                            path: route,
                            element: (
                                <AppLayout>
                                    <Story />
                                </AppLayout>
                            ),
                        },
                    ],
                    {
                        initialEntries: [route],
                    },
                );

                return (
                    <WithQuery>
                        <WithTheme>
                            <WithLanguage>
                                <RouterProvider router={router} />
                            </WithLanguage>
                        </WithTheme>
                    </WithQuery>
                );
            }

            return (
                <WithQuery>
                    <WithTheme>
                        <WithLanguage>
                            <AppLayout>
                                <Story />
                            </AppLayout>
                        </WithLanguage>
                    </WithTheme>
                </WithQuery>
            );
        }) as Decorator,
    ],
};

export default preview;
