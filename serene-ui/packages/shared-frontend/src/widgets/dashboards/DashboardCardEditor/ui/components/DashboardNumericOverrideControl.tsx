import React from "react";
import {
    Checkbox,
    Input,
    Label,
} from "@serene-ui/shared-frontend";

interface DashboardNumericOverrideControlProps {
    checkboxId: string;
    inputId: string;
    title: string;
    description: string;
    valueLabel: string;
    enabled: boolean;
    value: number;
    onEnabledChange: (enabled: boolean) => void;
    onValueChange: (value: number) => void;
    checkboxTestId: string;
    inputTestId: string;
}

export const DashboardNumericOverrideControl: React.FC<
    DashboardNumericOverrideControlProps
> = ({
    checkboxId,
    inputId,
    title,
    description,
    valueLabel,
    enabled,
    value,
    onEnabledChange,
    onValueChange,
    checkboxTestId,
    inputTestId,
}) => {
    return (
        <div className="flex flex-col gap-3">
            <div className="flex items-start gap-3">
                <Checkbox
                    id={checkboxId}
                    data-testid={checkboxTestId}
                    checked={enabled}
                    onCheckedChange={(checked) => {
                        onEnabledChange(checked === true);
                    }}
                />
                <div className="flex min-w-0 flex-1 flex-col gap-1">
                    <Label htmlFor={checkboxId}>{title}</Label>
                    <p className="text-xs text-muted-foreground">{description}</p>
                </div>
            </div>
            {enabled ? (
                <div className="flex flex-col gap-2 pl-7">
                    <Label htmlFor={inputId}>{valueLabel}</Label>
                    <Input
                        id={inputId}
                        type="number"
                        min={1}
                        step={1}
                        data-testid={inputTestId}
                        value={String(value)}
                        onChange={(event) => {
                            const nextValue = Number(event.target.value);

                            if (Number.isFinite(nextValue) && nextValue > 0) {
                                onValueChange(nextValue);
                            }
                        }}
                    />
                </div>
            ) : null}
        </div>
    );
};
