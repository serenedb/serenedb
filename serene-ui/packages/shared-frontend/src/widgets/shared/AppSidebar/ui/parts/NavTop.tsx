import {
    SidebarMenu,
    SidebarMenuButton,
    SidebarMenuItem,
    SmallLogoIcon,
} from "@serene-ui/shared-frontend/shared";
import { NotificationsListButton } from "../../../NotificationsList";

export const NavTop = () => {
    return (
        <SidebarMenu>
            <SidebarMenuItem>
                <SidebarMenuButton
                    size="lg"
                    className="flex justify-between active:bg-transparent hover:text-sidebar-primary-foreground hover:bg-transparent cursor-default data-[state=open]:bg-sidebar-accent data-[state=open]:text-sidebar-accent-foreground">
                    <div className="flex items-center gap-2">
                        <div className=" text-sidebar-primary-foreground flex aspect-square size-8 items-center justify-center rounded-lg">
                            <SmallLogoIcon className="size-8 group-data-[collapsible=icon]:ml-[1px]" />
                        </div>
                        <span className="text-white text-[16px] font-black">
                            SereneDB
                        </span>
                    </div>
                    <NotificationsListButton />
                </SidebarMenuButton>
            </SidebarMenuItem>
        </SidebarMenu>
    );
};
