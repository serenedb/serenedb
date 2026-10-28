import React from "react";
import { Checkbox, Label, Table, TableBody, TableCell, TableHead, TableHeader, TableRow } from "@serene-ui/shared-frontend";
import {
    type DashboardChartBlock,
    getDashboardColumnRoleState,
} from "../../model/dashboardChartColumns";
import { ConnectionsCombobox } from "../../../shared/ConnectionsCombobox";
import { useDashboardSelectChartParams } from "../model";
import { DatabasesCombobox } from "./components";

interface DashboardSelectChartParamsProps {
    block: DashboardChartBlock;
    onBlockChange?: (block: DashboardChartBlock) => void;
}

export const DashboardSelectChartParams: React.FC<
    DashboardSelectChartParamsProps
> = ({ block, onBlockChange }) => {
    const {
        columns,
        currentDatabase,
        databases,
        error,
        isDatabasesPending,
        isRowsPending,
        handleConnectionChange,
        handleDatabaseChange,
        handleXChange,
        handleYChange,
        handleDimensionChange,
    } = useDashboardSelectChartParams({
        block,
        onBlockChange,
    });

    return (
        <div className="flex flex-col gap-2 px-4" data-testid="dashboardSelectChartParams-root">
            <div className="flex flex-col gap-2">
                <Label>Connection</Label>
                <ConnectionsCombobox
                    currentConnectionId={block.connection_id ?? -1}
                    setCurrentConnectionId={handleConnectionChange}
                />
            </div>
            <div className="flex flex-col gap-2">
                <Label>Database</Label>
                <DatabasesCombobox
                    databases={databases}
                    currentDatabase={currentDatabase}
                    isLoading={isDatabasesPending}
                    setCurrentDatabase={handleDatabaseChange}
                />
            </div>
            <div className="rounded-md border mt-2" data-testid="dashboardSelectChartParams-columnsTable">
                {isRowsPending ? (
                    <div className="p-4 text-sm text-muted-foreground">
                        Executing query...
                    </div>
                ) : error ? (
                    <div className="p-4 text-sm text-destructive">{error}</div>
                ) : columns.length === 0 ? (
                    <div className="p-4 text-sm text-muted-foreground">
                        Query returned no rows, so column types could not be
                        inferred yet.
                    </div>
                ) : (
                    <Table className="text-xs">
                        <TableHeader>
                            <TableRow>
                                <TableHead className="w-12">Color</TableHead>
                                <TableHead>Column</TableHead>
                                <TableHead>Type</TableHead>
                                <TableHead className="w-12 text-center">
                                    X
                                </TableHead>
                                <TableHead className="w-12 text-center">
                                    Y
                                </TableHead>
                                <TableHead className="w-24 text-center">
                                    Dimension
                                </TableHead>
                            </TableRow>
                        </TableHeader>
                        <TableBody>
                            {columns.map((column, index) => {
                                const roleState = getDashboardColumnRoleState(
                                    block,
                                    column,
                                );

                                return (
                                    <TableRow key={column.name}>
                                        <TableCell>
                                            <div
                                                className="size-2.5 rounded-full"
                                                style={{
                                                    backgroundColor:
                                                        column.color,
                                                }}
                                            />
                                        </TableCell>
                                        <TableCell className="font-medium">
                                            {column.name}
                                        </TableCell>
                                        <TableCell className="text-muted-foreground">
                                            {column.type}
                                        </TableCell>
                                        <TableCell className="text-center">
                                            <Checkbox
                                                checked={roleState.isXChecked}
                                                disabled={roleState.isXDisabled}
                                                data-testid={`dashboardSelectChartParams-xCheckbox-${index}`}
                                                onCheckedChange={(checked) => {
                                                    handleXChange(
                                                        column.name,
                                                        checked === true,
                                                    );
                                                }}
                                            />
                                        </TableCell>
                                        <TableCell className="text-center">
                                            <Checkbox
                                                checked={roleState.isYChecked}
                                                disabled={roleState.isYDisabled}
                                                data-testid={`dashboardSelectChartParams-yCheckbox-${index}`}
                                                onCheckedChange={(checked) => {
                                                    handleYChange(
                                                        column.name,
                                                        checked === true,
                                                    );
                                                }}
                                            />
                                        </TableCell>
                                        <TableCell className="text-center">
                                            <Checkbox
                                                checked={
                                                    roleState.isDimensionChecked
                                                }
                                                disabled={
                                                    roleState.isDimensionDisabled
                                                }
                                                data-testid={`dashboardSelectChartParams-dimensionCheckbox-${index}`}
                                                onCheckedChange={(checked) => {
                                                    handleDimensionChange(
                                                        column.name,
                                                        checked === true,
                                                    );
                                                }}
                                            />
                                        </TableCell>
                                    </TableRow>
                                );
                            })}
                        </TableBody>
                    </Table>
                )}
            </div>
        </div>
    );
};
