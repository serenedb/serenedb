import React from "react";
import { ChevronDownIcon } from "lucide-react";
import type { AddDashboardCardInput } from "../../../../entities/dashboard-card";
import { useAddDashboardCard } from "../../../../entities/dashboard-card";

import {
    Button,
    Popover,
    PopoverContent,
    PopoverTrigger,
} from "../../../../shared/ui";

type DashboardCardBounds = AddDashboardCardInput["bounds"];

interface DashboardAddCardButtonProps {
    dashboardId: number;
    nextBounds: DashboardCardBounds;
}

type DashboardCardOption = {
    key: string;
    title: string;
    description: string;
    createCard: (bounds: DashboardCardBounds) => AddDashboardCardInput;
};

const createQueryCardBase = () => ({
    custom_refresh_interval_enabled: false,
    custom_refresh_interval: 60,
    custom_row_limit_enabled: false,
    custom_row_limit: 1000,
});

const DASHBOARD_CARD_OPTIONS: DashboardCardOption[] = [
    {
        key: "text",
        title: "Text",
        description: "Simple note card",
        createCard: (bounds) => ({
            type: "text",
            bounds,
            text: "Add your note here",
        }),
    },
    {
        key: "bar-interactive",
        title: "Bar interactive",
        description: "Switch between series",
        createCard: (bounds) => ({
            type: "bar_chart",
            bounds,
            ...createQueryCardBase(),
            query: "",
            name: "Interactive bar chart",
            description: "Switch between series",
            category_key: "label",
            default_active_key: "value",
            value_label: "Value",
            variant: "interactive",
            is_stacked: false,
            series: [
                {
                    key: "value",
                    label: "Value",
                    color: "var(--chart-1)",
                },
                {
                    key: "value_2",
                    label: "Value 2",
                    color: "var(--chart-2)",
                },
            ],
        }),
    },
    {
        key: "bar-vertical",
        title: "Bar vertical",
        description: "Grouped columns",
        createCard: (bounds) => ({
            type: "bar_chart",
            bounds,
            ...createQueryCardBase(),
            query: "",
            name: "Vertical bar chart",
            description: "Grouped columns",
            category_key: "label",
            value_label: "Value",
            variant: "vertical",
            is_stacked: false,
            series: [
                {
                    key: "value",
                    label: "Value",
                    color: "var(--chart-1)",
                },
                {
                    key: "value_2",
                    label: "Value 2",
                    color: "var(--chart-2)",
                },
                {
                    key: "value_3",
                    label: "Value 3",
                    color: "var(--chart-3)",
                },
            ],
        }),
    },
    {
        key: "bar-horizontal",
        title: "Bar horizontal",
        description: "Horizontal comparison",
        createCard: (bounds) => ({
            type: "bar_chart",
            bounds,
            ...createQueryCardBase(),
            query: "",
            name: "Horizontal bar chart",
            description: "Horizontal comparison",
            category_key: "label",
            value_label: "Value",
            variant: "horizontal",
            is_stacked: false,
            series: [
                {
                    key: "value",
                    label: "Value",
                    color: "var(--chart-1)",
                },
                {
                    key: "value_2",
                    label: "Value 2",
                    color: "var(--chart-2)",
                },
                {
                    key: "value_3",
                    label: "Value 3",
                    color: "var(--chart-3)",
                },
            ],
        }),
    },
    {
        key: "line-interactive",
        title: "Line interactive",
        description: "Selectable time series",
        createCard: (bounds) => ({
            type: "line_chart",
            bounds,
            ...createQueryCardBase(),
            query: "",
            name: "Interactive line chart",
            description: "Selectable time series",
            x_axis_key: "label",
            default_active_key: "value",
            value_label: "Value",
            variant: "interactive",
            line_type: "natural",
            series: [
                {
                    key: "value",
                    label: "Value",
                    color: "var(--chart-4)",
                },
                {
                    key: "value_2",
                    label: "Value 2",
                    color: "var(--chart-2)",
                },
            ],
        }),
    },
    {
        key: "line-default",
        title: "Line default",
        description: "Multi-line comparison",
        createCard: (bounds) => ({
            type: "line_chart",
            bounds,
            ...createQueryCardBase(),
            query: "",
            name: "Line chart",
            description: "Multi-line comparison",
            x_axis_key: "label",
            variant: "default",
            line_type: "natural",
            value_label: "Value",
            series: [
                {
                    key: "value",
                    label: "Value",
                    color: "var(--chart-4)",
                },
                {
                    key: "value_2",
                    label: "Value 2",
                    color: "var(--chart-2)",
                },
            ],
        }),
    },
    {
        key: "pie-interactive",
        title: "Pie interactive",
        description: "Active slice summary",
        createCard: (bounds) => ({
            type: "pie_chart",
            bounds,
            ...createQueryCardBase(),
            query: "",
            name: "Interactive pie chart",
            description: "Active slice summary",
            interactive: true,
            variant: "donut",
            name_key: "label",
            value_key: "value",
            value_label: "Value",
            color_key: "fill",
            show_labels: false,
            show_center_label: false,
        }),
    },
    {
        key: "pie",
        title: "Pie",
        description: "Simple pie distribution",
        createCard: (bounds) => ({
            type: "pie_chart",
            bounds,
            ...createQueryCardBase(),
            query: "",
            name: "Pie chart",
            description: "Simple pie distribution",
            interactive: false,
            variant: "pie",
            name_key: "label",
            value_key: "value",
            value_label: "Value",
            color_key: "fill",
            show_labels: false,
            show_center_label: false,
        }),
    },
    {
        key: "donut",
        title: "Donut",
        description: "Centered donut chart",
        createCard: (bounds) => ({
            type: "pie_chart",
            bounds,
            ...createQueryCardBase(),
            query: "",
            name: "Donut chart",
            description: "Centered donut chart",
            interactive: false,
            variant: "donut",
            name_key: "label",
            value_key: "value",
            value_label: "Value",
            color_key: "fill",
            show_labels: false,
            show_center_label: true,
            center_label: "Value",
        }),
    },
];

export const DashboardAddCardButton: React.FC<DashboardAddCardButtonProps> = ({
    dashboardId,
    nextBounds,
}) => {
    const [open, setOpen] = React.useState(false);
    const { mutateAsync: addDashboardCard, isPending } = useAddDashboardCard();

    const handleAddCard = async (option: DashboardCardOption) => {
        setOpen(false);

        await addDashboardCard({
            dashboardId,
            card: option.createCard(nextBounds),
        });
    };

    return (
        <Popover open={open} onOpenChange={setOpen}>
            <PopoverTrigger asChild>
                <Button
                    variant="default"
                    className="h-9 min-w-20 justify-between rounded-lg px-3"
                    data-testid="dashboardAddCardButton-trigger"
                    title="Add card">
                    <span>Add card</span>
                    <span>|</span>
                    <ChevronDownIcon className="size-4" />
                </Button>
            </PopoverTrigger>
            <PopoverContent
                align="end"
                side="top"
                variant="secondary"
                className="min-w-72 p-1 shadow-none">
                <div className="flex flex-col gap-1">
                    {DASHBOARD_CARD_OPTIONS.map((option) => (
                        <Button
                            key={option.key}
                            type="button"
                            variant="ghost"
                            disabled={isPending}
                            data-testid={`dashboardAddCardButton-option-${option.key}`}
                            className="h-auto w-full flex-col items-start gap-0.5 rounded-md px-3 py-2 text-left"
                            onClick={() => void handleAddCard(option)}>
                            <span className="text-sm text-primary-foreground">
                                {option.title}
                            </span>
                            <span className="text-xs text-muted-foreground">
                                {option.description}
                            </span>
                        </Button>
                    ))}
                </div>
            </PopoverContent>
        </Popover>
    );
};
