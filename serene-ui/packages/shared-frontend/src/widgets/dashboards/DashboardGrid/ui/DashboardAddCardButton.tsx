import React from "react";
import type { AddDashboardCardInput } from "../../../../entities/dashboard-card";
import { useAddDashboardCard } from "../../../../entities/dashboard-card";
import {
    BarChart3Icon,
    ChevronDownIcon,
    FileTextIcon,
    LineChartIcon,
    PieChartIcon,
} from "../../../../shared/ui/icons";

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
    icon: React.ElementType;
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
        icon: FileTextIcon,
        createCard: (bounds) => ({
            type: "text",
            bounds,
            text: "Add your note here",
        }),
    },
    {
        key: "bar",
        title: "Bar",
        icon: BarChart3Icon,
        createCard: (bounds) => ({
            type: "bar_chart",
            bounds,
            ...createQueryCardBase(),
            query: "",
            name: "Bar chart",
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
        key: "line",
        title: "Line",
        icon: LineChartIcon,
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
        key: "pie",
        title: "Pie",
        icon: PieChartIcon,
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
                            className="h-auto w-full justify-start gap-2 rounded-md px-3 py-2 text-left"
                            onClick={() => void handleAddCard(option)}>
                            <option.icon className="size-4 text-muted-foreground" />
                            <span className="text-xs text-primary-foreground">
                                {option.title}
                            </span>
                        </Button>
                    ))}
                </div>
            </PopoverContent>
        </Popover>
    );
};
