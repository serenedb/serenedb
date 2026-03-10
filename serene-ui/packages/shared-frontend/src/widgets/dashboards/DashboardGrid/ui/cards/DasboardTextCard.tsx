interface DashboardTextCardProps {
    text: string;
}

export const DashboardTextCard: React.FC<DashboardTextCardProps> = ({
    text,
}) => {
    return (
        <div className="bg-background border-1 rounded-xs flex-1 px-4 py-3 overflow-auto">
            <p className="text-sm select-none">{text}</p>
        </div>
    );
};
