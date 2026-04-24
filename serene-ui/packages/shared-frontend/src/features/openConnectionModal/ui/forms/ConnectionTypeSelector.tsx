import { PostgresIcon, SmallLogoIcon } from "@serene-ui/shared-frontend/shared";
import { useState } from "react";
import { ConnectionTypeButton } from "../buttons";
import { useConnectionsModal } from "../../model/ConnectionsModalContext";

type ConnectionUiType = "serenedb" | "postgres";

type TypeButton = {
    type: ConnectionUiType;
    tooltip: React.ReactNode;
    icon: React.ReactNode;
};

const TYPE_BUTTONS: TypeButton[] = [
    {
        type: "serenedb",
        tooltip: (
            <>
                <p>SereneDB</p>
            </>
        ),
        icon: <SmallLogoIcon />,
    },
    {
        type: "postgres",
        tooltip: (
            <>
                <p>PostgreSQL</p>
            </>
        ),
        icon: <PostgresIcon />,
    },
];

export const ConnectionTypeSelector = () => {
    const { handleSelectChange } = useConnectionsModal();
    const [selectedType, setSelectedType] = useState<ConnectionUiType>("serenedb");

    const handleTypeSelect = (type: ConnectionUiType) => {
        setSelectedType(type);
        handleSelectChange("type", "postgres");
    };

    return (
        <div className="flex gap-2 p-4 pb-0 ">
            {TYPE_BUTTONS.map((button) => (
                <ConnectionTypeButton
                    key={button.type}
                    type={button.type}
                    isActive={selectedType === button.type}
                    onSelect={handleTypeSelect}
                    tooltipContent={button.tooltip}
                    icon={button.icon}
                />
            ))}
        </div>
    );
};
