import {
    Button,
    PostgresIcon,
    SmallLogoIcon,
    Tooltip,
    TooltipContent,
    TooltipTrigger,
} from "@serene-ui/shared-frontend/shared";
import { ConnectionTypeButton } from "../buttons";
import { ConnectionSchema } from "@serene-ui/shared-core";

type TypeButton = {
    type: ConnectionSchema["type"];
    tooltip: React.ReactNode;
    icon: React.ReactNode;
};

const TYPE_BUTTONS: TypeButton[] = [
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
    return (
        <div className="flex gap-2 p-4 pb-0 ">
            <Tooltip>
                <TooltipTrigger asChild>
                    <Button
                        className="opacity-50 bg-primary/30 hover:bg-primary/30"
                        size="icon"
                        aria-label="More connection types coming soon">
                        <SmallLogoIcon />
                    </Button>
                </TooltipTrigger>
                <TooltipContent
                    className="flex flex-col items-center "
                    side="top"
                    align="center">
                    <span className="text-sm">We're working on it!</span>
                    <a
                        href="https://github.com/serenedb/serenedb"
                        target="_blank">
                        <p className="text-sm underline">Stay tuned</p>
                    </a>
                </TooltipContent>
            </Tooltip>
            {TYPE_BUTTONS.map((button) => (
                <ConnectionTypeButton
                    key={button.type}
                    type={button.type}
                    tooltipContent={button.tooltip}
                    icon={button.icon}
                />
            ))}
        </div>
    );
};
