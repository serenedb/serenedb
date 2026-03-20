import React from "react";
import { cn } from "@serene-ui/shared-frontend";
import { useDropZone, type UseDropZoneOptions } from "../model";

export interface DropZoneProps extends UseDropZoneOptions {
    className?: string;
    children: React.ReactNode;
}

export const DropZone = ({
    supportedExtensions,
    onFilesDrop,
    onRejectedFiles,
    className,
    children,
}: DropZoneProps) => {
    const { isDragging, normalizedExtensions, rootProps } = useDropZone({
        supportedExtensions,
        onFilesDrop,
        onRejectedFiles,
    });

    return (
        <div
            className={cn(
                "relative h-full w-full border-1 border-transparent flex",
                isDragging &&
                    "border-1 border-dashed border-primary bg-primary/10",
                className,
            )}
            {...rootProps}>
            {isDragging && (
                <div className="pointer-events-none absolute z-1000 flex h-full w-full items-center justify-center">
                    <div className="flex flex-col bg-[#303030] items-center px-6 py-4 rounded-md border-dashed border-1 border-secondary">
                        <p className="text-md font-medium">Drop files here</p>
                        {normalizedExtensions.length > 0 && (
                            <p className="text-muted-foreground mt-1 text-xs">
                                Supported:{" "}
                                {normalizedExtensions
                                    .map((extension) => `.${extension}`)
                                    .join(", ")}
                            </p>
                        )}
                    </div>
                </div>
            )}
            {children}
        </div>
    );
};
