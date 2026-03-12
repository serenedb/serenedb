interface DashboardChartCardBaseProps {
    children: React.ReactNode;
    name?: string;
    description?: string;
}

export const DashboardChartCardBase: React.FC<DashboardChartCardBaseProps> = ({
    children,
    name,
    description,
}) => {
    return (
        <div className="bg-background border-1 rounded-xs flex min-h-0 flex-1 flex-col overflow-hidden">
            <div className="flex min-h-0 flex-1 flex-col">{children}</div>
            <div className="flex flex-col border-t-1 p-3">
                <p className="uppercase text-xs font-extrabold text-primary-foreground">
                    {name}
                </p>
                <p className="text-xs text-muted-foreground">{description}</p>
            </div>
        </div>
    );
};
