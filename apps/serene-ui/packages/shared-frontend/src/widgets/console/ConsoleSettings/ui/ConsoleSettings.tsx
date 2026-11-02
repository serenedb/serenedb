import { useCallback, useEffect, useState, type ReactNode } from "react";
import {
    Button,
    Checkbox,
    Input,
    Label,
    Select,
    SelectContent,
    SelectItem,
    SelectTrigger,
    SelectValue,
    SupportIcon,
    Tooltip,
    TooltipContent,
    TooltipTrigger,
    cn,
} from "@serene-ui/shared-frontend/shared";
import { useConsole } from "../../Console/model";
import type { ConsoleExecutionAlertMode } from "../../Console/model";
import { ConsoleSettingsTopbar } from "./ConsoleSettingsTopbar";

const LIMIT_PRESETS = [1000, 10000, 100000];

interface ConsoleSettingFieldProps {
    children: ReactNode;
    description: string;
    disabled?: boolean;
    htmlFor?: string;
    title: string;
}

const ConsoleSettingField = ({
    children,
    description,
    disabled = false,
    htmlFor,
    title,
}: ConsoleSettingFieldProps) => (
    <div className={cn("space-y-2", disabled && "opacity-60")}>
        <div className="flex items-start justify-between gap-2">
            <Label
                htmlFor={htmlFor}
                className={cn(
                    "text-xs leading-5 font-medium text-foreground opacity-100 dark:opacity-80 dark:text-primary-foreground",
                    disabled && "cursor-not-allowed",
                )}>
                {title}
            </Label>
            <Tooltip>
                <TooltipTrigger asChild>
                    <button
                        type="button"
                        tabIndex={0}
                        className="mt-[2px] text-foreground/80 dark:text-secondary-foreground/70 hover:text-foreground dark:hover:text-secondary-foreground transition-colors"
                        aria-label={`Help: ${title}`}>
                        <SupportIcon className="size-3.5" />
                    </button>
                </TooltipTrigger>
                <TooltipContent
                    side="left"
                    className="max-w-72 text-xs leading-5 text-primary-foreground">
                    {description}
                </TooltipContent>
            </Tooltip>
        </div>
        {children}
    </div>
);

interface ConsoleSettingCheckboxRowProps {
    checked: boolean;
    description: string;
    disabled?: boolean;
    htmlFor: string;
    onCheckedChange: (checked: boolean | "indeterminate") => void;
    title: string;
}

const ConsoleSettingCheckboxRow = ({
    checked,
    description,
    disabled = false,
    htmlFor,
    onCheckedChange,
    title,
}: ConsoleSettingCheckboxRowProps) => (
    <div
        className={cn(
            "flex items-center gap-2.5",
            disabled && "opacity-60",
        )}>
        <Checkbox
            id={htmlFor}
            checked={checked}
            disabled={disabled}
            onCheckedChange={onCheckedChange}
        />
        <Label
            htmlFor={htmlFor}
            className={cn(
                "text-xs leading-5 font-medium text-foreground opacity-100 dark:opacity-80 dark:text-primary-foreground",
                disabled && "cursor-not-allowed",
            )}>
            {title}
        </Label>
        <Tooltip>
            <TooltipTrigger asChild>
                <button
                    type="button"
                    tabIndex={0}
                    className="text-foreground/80 dark:text-secondary-foreground/70 hover:text-foreground dark:hover:text-secondary-foreground transition-colors"
                    aria-label={`Help: ${title}`}>
                    <SupportIcon className="size-3.5" />
                </button>
            </TooltipTrigger>
            <TooltipContent
                side="left"
                className="max-w-72 text-xs leading-5 text-primary-foreground">
                {description}
            </TooltipContent>
        </Tooltip>
    </div>
);

const toBoolean = (value: boolean | "indeterminate") => value === true;

export const ConsoleSettings = () => {
    const {
        alertOnExecution,
        colorfulTypesInResults,
        executeInNewTabByDefault,
        executeSequentiallyByDefault,
        limit,
        selectRelatedResultOnTabChange,
        setAlertOnExecution,
        setColorfulTypesInResults,
        setExecuteInNewTabByDefault,
        setExecuteSequentiallyByDefault,
        setLimit,
        setSelectRelatedResultOnTabChange,
        setSettingsSidebarCollapsed,
        setShowAutocomplete,
        setShowExecutionHistoryInAutocomplete,
        setShowJsonByDefault,
        setShowSavedQueriesInAutocomplete,
        setSpawnResultsInFirstTab,
        showAutocomplete,
        showExecutionHistoryInAutocomplete,
        showJsonByDefault,
        showSavedQueriesInAutocomplete,
        spawnResultsInFirstTab,
    } = useConsole();
    const [limitValue, setLimitValue] = useState(() => String(limit));

    useEffect(() => {
        setLimitValue(String(limit));
    }, [limit]);

    const handleLimitInputChange = useCallback(
        (value: string) => {
            setLimitValue(value);

            const parsedValue = Number(value);
            if (Number.isFinite(parsedValue) && parsedValue > 0) {
                setLimit(parsedValue);
            }
        },
        [setLimit],
    );

    const handleLimitBlur = useCallback(() => {
        setLimitValue(String(limit));
    }, [limit]);

    const handleAlertOnExecutionChange = useCallback(
        (value: string) => {
            setAlertOnExecution(value as ConsoleExecutionAlertMode);
        },
        [setAlertOnExecution],
    );

    return (
        <div className="flex h-full w-full flex-col">
            <ConsoleSettingsTopbar
                onClose={() => setSettingsSidebarCollapsed(true)}
            />
            <div className="flex-1 min-h-0 overflow-auto px-4 py-3">
                <div className="space-y-4 pb-2">
                    <ConsoleSettingField
                        htmlFor="console-settings-limit"
                        title="Limit"
                        description="Maximum number of rows returned per statement execution. Use preset buttons for quick values or type a custom positive number.">
                        <Input
                            id="console-settings-limit"
                            type="number"
                            min={1}
                            step={1}
                            value={limitValue}
                            onBlur={handleLimitBlur}
                            onChange={(event) => {
                                handleLimitInputChange(event.target.value);
                            }}
                        />
                        <div className="flex flex-wrap gap-1.5">
                            {LIMIT_PRESETS.map((preset) => (
                                <Button
                                    key={preset}
                                    type="button"
                                    variant={
                                        limit === preset ? "default" : "outline"
                                    }
                                    size="small"
                                    className="h-6 min-w-16 px-2"
                                    onClick={() => {
                                        setLimit(preset);
                                        setLimitValue(String(preset));
                                    }}>
                                    {preset}
                                </Button>
                            ))}
                        </div>
                    </ConsoleSettingField>

                    <ConsoleSettingField
                        title="Alert On Execution"
                        description="Controls when execution notifications are shown: always, only when results are not currently visible or the page is hidden, or never.">
                        <Select
                            value={alertOnExecution}
                            onValueChange={handleAlertOnExecutionChange}>
                            <SelectTrigger
                                id="console-settings-alert-on-execution"
                                className="w-full">
                                <SelectValue />
                            </SelectTrigger>
                            <SelectContent>
                                <SelectItem value="always">Always</SelectItem>
                                <SelectItem value="onlyUnseen">
                                    Only unseen
                                </SelectItem>
                                <SelectItem value="never">Never</SelectItem>
                            </SelectContent>
                        </Select>
                    </ConsoleSettingField>

                    <div className="border-b-[0.5px] border-border" />

                    <ConsoleSettingCheckboxRow
                        htmlFor="console-settings-spawn-first-results-tab"
                        title="Spawn Results In First Tab"
                        description="When enabled, new results from any query tab are shown in the first created results pane. Otherwise each query tab uses its own related results pane."
                        checked={spawnResultsInFirstTab}
                        onCheckedChange={(checked) => {
                            setSpawnResultsInFirstTab(toBoolean(checked));
                        }}
                    />

                    <ConsoleSettingCheckboxRow
                        htmlFor="console-settings-colorful-types"
                        title="Colorful Types In Results"
                        description="Colors values by type in the results viewer. Disable this to render all non-null values with one neutral color while keeping null slightly dimmer."
                        checked={colorfulTypesInResults}
                        onCheckedChange={(checked) => {
                            setColorfulTypesInResults(toBoolean(checked));
                        }}
                    />

                    <ConsoleSettingCheckboxRow
                        htmlFor="console-settings-select-related-result"
                        title="On Tab Change Select Related Result"
                        description="When enabled and query/results are in different panes, selecting a query tab also selects its related results tab in the results pane."
                        checked={selectRelatedResultOnTabChange}
                        onCheckedChange={(checked) => {
                            setSelectRelatedResultOnTabChange(
                                toBoolean(checked),
                            );
                        }}
                    />

                    <ConsoleSettingCheckboxRow
                        htmlFor="console-settings-show-json-default"
                        title="Show JSON By Default"
                        description="When enabled, result sets open in JSON view by default instead of table viewer mode."
                        checked={showJsonByDefault}
                        onCheckedChange={(checked) => {
                            setShowJsonByDefault(toBoolean(checked));
                        }}
                    />

                    <ConsoleSettingCheckboxRow
                        htmlFor="console-settings-show-autocomplete"
                        title="Show Autocomplete"
                        description="Enables SQL autocompletion and inline suggestions in the editor. Turning this off disables and clears saved/history autocomplete sources."
                        checked={showAutocomplete}
                        onCheckedChange={(checked) => {
                            setShowAutocomplete(toBoolean(checked));
                        }}
                    />

                    <ConsoleSettingCheckboxRow
                        htmlFor="console-settings-show-saved-autocomplete"
                        title="Show Saved Queries In Autocomplete"
                        description="Includes saved queries in inline autocomplete suggestions at the end of the editor text."
                        disabled={!showAutocomplete}
                        checked={showSavedQueriesInAutocomplete}
                        onCheckedChange={(checked) => {
                            setShowSavedQueriesInAutocomplete(
                                toBoolean(checked),
                            );
                        }}
                    />

                    <ConsoleSettingCheckboxRow
                        htmlFor="console-settings-show-history-autocomplete"
                        title="Show Query Execution History In Autocomplete"
                        description="Includes recently executed queries in inline autocomplete suggestions at the end of the editor text."
                        disabled={!showAutocomplete}
                        checked={showExecutionHistoryInAutocomplete}
                        onCheckedChange={(checked) => {
                            setShowExecutionHistoryInAutocomplete(
                                toBoolean(checked),
                            );
                        }}
                    />

                    <ConsoleSettingCheckboxRow
                        htmlFor="console-settings-sequential-default"
                        title="Execute Sequentially By Default"
                        description="Changes the default run action to sequential execution instead of transaction execution."
                        checked={executeSequentiallyByDefault}
                        onCheckedChange={(checked) => {
                            setExecuteSequentiallyByDefault(
                                toBoolean(checked),
                            );
                        }}
                    />

                    <ConsoleSettingCheckboxRow
                        htmlFor="console-settings-new-tab-default"
                        title="Execute In New Tab By Default"
                        description="Changes the default run action to execute in a newly created query tab."
                        checked={executeInNewTabByDefault}
                        onCheckedChange={(checked) => {
                            setExecuteInNewTabByDefault(toBoolean(checked));
                        }}
                    />
                </div>
            </div>
        </div>
    );
};
