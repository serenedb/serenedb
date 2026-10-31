"use client";

import {
    Breadcrumb,
    BreadcrumbItem,
    BreadcrumbLink,
    BreadcrumbList,
    BreadcrumbPage,
    BreadcrumbSeparator,
    DEFAULT_HOTKEYS,
    Dialog,
    DialogContent,
    DialogDescription,
    DialogTitle,
    Sidebar,
    SidebarContent,
    SidebarGroup,
    SidebarGroupContent,
    SidebarMenu,
    SidebarMenuButton,
    SidebarMenuItem,
    SidebarProvider,
    useAppHotkey,
} from "@serene-ui/shared-frontend/shared";
import { useSettingsModal } from "../model";

export const SettingsModal = () => {
    const { open, setOpen, tabs, selectedTab } = useSettingsModal();

    useAppHotkey(DEFAULT_HOTKEYS.SETTINGS_TOGGLE, () => setOpen(true));

    return (
        <Dialog open={open} onOpenChange={setOpen}>
            <DialogContent className="overflow-hidden p-0 md:max-h-[500px] md:max-w-[700px] lg:max-w-[800px]">
                <DialogTitle className="sr-only">Settings</DialogTitle>
                <DialogDescription className="sr-only">
                    Customize your settings here.
                </DialogDescription>
                <SidebarProvider className="items-start">
                    <Sidebar
                        collapsible="none"
                        className="hidden md:flex border-r-1">
                        <SidebarContent>
                            <SidebarGroup className="py-2 px-2">
                                <SidebarGroupContent>
                                    <SidebarMenu>
                                        {tabs.map((item) => (
                                            <SidebarMenuItem key={item.label}>
                                                <SidebarMenuButton
                                                    asChild
                                                    isActive={
                                                        item.name ===
                                                        selectedTab
                                                    }>
                                                    <a href="#">
                                                        {item.icon}
                                                        <span>
                                                            {item.label}
                                                        </span>
                                                    </a>
                                                </SidebarMenuButton>
                                            </SidebarMenuItem>
                                        ))}
                                    </SidebarMenu>
                                </SidebarGroupContent>
                            </SidebarGroup>
                        </SidebarContent>
                    </Sidebar>
                    <main className="flex h-[480px] flex-1 flex-col overflow-hidden">
                        <header className="flex h-11.5 shrink-0 items-center gap-2 transition-[width,height] ease-linear group-has-data-[collapsible=icon]/sidebar-wrapper:h-12">
                            <div className="flex items-center gap-2 px-4">
                                <Breadcrumb>
                                    <BreadcrumbList>
                                        <BreadcrumbItem className="hidden md:block">
                                            <BreadcrumbLink className="text-secondary-foreground">
                                                Settings
                                            </BreadcrumbLink>
                                        </BreadcrumbItem>
                                        <BreadcrumbSeparator className="hidden md:block" />
                                        <BreadcrumbItem>
                                            <BreadcrumbPage>
                                                {
                                                    tabs.find(
                                                        (tab) =>
                                                            tab.name ===
                                                            selectedTab,
                                                    )?.label
                                                }
                                            </BreadcrumbPage>
                                        </BreadcrumbItem>
                                    </BreadcrumbList>
                                </Breadcrumb>
                            </div>
                        </header>
                        <div className="flex flex-1 flex-col gap-4 overflow-y-auto p-4 pt-0"></div>
                    </main>
                </SidebarProvider>
            </DialogContent>
        </Dialog>
    );
};
