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
        <div className="bg-background border-1 rounded-xs flex-1 flex flex-col pt-4">
            {children}
            <div className="flex flex-col border-t-1 p-3 mt-4">
                <p className="uppercase text-xs font-extrabold text-primary-foreground">
                    {name}
                </p>
                <p className="text-xs text-muted-foreground">{description}</p>
            </div>
        </div>
    );
};
