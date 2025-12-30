import {
    ButtonCard,
    ButtonCardButtonContent,
    ButtonCardContent,
    ClockIcon,
} from "@serene-ui/shared-frontend/shared";
import { TimelineItem } from "../model";

interface TimelineCardProps {
    title: string;
    items: TimelineItem[];
    displayTime?: number | null;
    disabled?: boolean;
}

export const TimelineCard: React.FC<TimelineCardProps> = ({
    title,
    items,
    displayTime,
    disabled,
}) => {
    const totalTime = items.reduce((sum, item) => sum + item.time, 0);

    const itemsWithPercent = items.map((item) => ({
        ...item,
        percent: totalTime > 0 ? (item.time / totalTime) * 100 : 0,
    }));

    return (
        <ButtonCard>
            <ButtonCardButtonContent variant="thirdly">
                <ClockIcon />
                {displayTime !== null && displayTime !== undefined
                    ? `${Math.round(displayTime)} ms`
                    : "-- ms"}
            </ButtonCardButtonContent>
            {!disabled && (
                <ButtonCardContent className="w-80">
                    <div className="space-y-3">
                        <div className="text-sm font-medium">{title}</div>
                        <div className="space-y-2">
                            {items.map((item, index) => (
                                <div
                                    key={index}
                                    className="flex items-center gap-2 text-xs">
                                    <div
                                        className="w-3 h-3 rounded-full"
                                        style={{ backgroundColor: item.color }}
                                    />
                                    <span>
                                        {item.name}: {Math.round(item.time)} ms
                                    </span>
                                </div>
                            ))}
                        </div>
                        <div className="h-2 w-full bg-muted rounded-full overflow-hidden flex">
                            {itemsWithPercent.map(
                                (item, index) =>
                                    item.percent > 0 && (
                                        <div
                                            key={index}
                                            className="h-full"
                                            style={{
                                                width: `${item.percent}%`,
                                                backgroundColor: item.color,
                                            }}
                                        />
                                    ),
                            )}
                        </div>
                        <div className="text-xs text-muted-foreground">
                            Total: {Math.round(totalTime)} ms
                        </div>
                    </div>
                </ButtonCardContent>
            )}
        </ButtonCard>
    );
};
