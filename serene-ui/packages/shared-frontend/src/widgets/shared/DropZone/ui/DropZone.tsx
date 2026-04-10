import React from "react";
import { cn } from "@serene-ui/shared-frontend";
import { useDropZone, type UseDropZoneOptions } from "../model";

export interface DropZoneProps extends UseDropZoneOptions {
    className?: string;
    children: React.ReactNode;
    customDropLabel?: string;
}

export const DropZone = ({
    supportedExtensions,
    onFilesDrop,
    onRejectedFiles,
    isCustomDragEvent,
    onCustomDrop,
    className,
    children,
    customDropLabel = "Drop item here",
}: DropZoneProps) => {
    const { isDragging, activeDragKind, normalizedExtensions, rootProps } =
        useDropZone({
            supportedExtensions,
            onFilesDrop,
            onRejectedFiles,
            isCustomDragEvent,
            onCustomDrop,
        });

    const isFileDrag = activeDragKind === "files";
    const isCustomDrag = activeDragKind === "custom";

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
                        <p className="text-md font-medium">
                            {isCustomDrag ? customDropLabel : "Drop files here"}
                        </p>
                        {isFileDrag && normalizedExtensions.length > 0 && (
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
