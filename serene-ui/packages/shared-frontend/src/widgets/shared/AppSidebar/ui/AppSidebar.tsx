import {
    Sidebar,
    SidebarHeader,
    SidebarContent,
    SidebarFooter,
    SidebarRail,
} from "@serene-ui/shared-frontend/shared";
import { NavMain } from "./parts/NavMain";
import { NavSerene } from "./parts/NavSerene";
import { NavTop } from "./parts/NavTop";
import { NavFooter } from "./parts/NavFooter";
import { NavBottom } from "./parts/NavBottom";

export const AppSidebar = ({
    ...props
}: React.ComponentProps<typeof Sidebar>) => {
    return (
        <Sidebar className="page-fade" collapsible="icon" {...props}>
            <SidebarHeader>
                <NavTop />
            </SidebarHeader>
            <SidebarContent>
                <NavMain />
                <NavSerene />
                <NavBottom />
            </SidebarContent>
            <SidebarFooter>
                <NavFooter />
            </SidebarFooter>
            <SidebarRail />
        </Sidebar>
    );
};
