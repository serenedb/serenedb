import { ConsoleProvider } from "@/pages/console/model";

export const WithPages = ({ children }: { children: React.ReactNode }) => {
    return <ConsoleProvider>{children}</ConsoleProvider>;
};
