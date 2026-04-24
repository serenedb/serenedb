import React, { useMemo, useState } from "react";

export type DropZoneDragKind = "none" | "files" | "custom";

export interface UseDropZoneOptions {
    supportedExtensions: readonly string[];
    onFilesDrop: (files: File[]) => void;
    onRejectedFiles?: (files: File[]) => void;
    isCustomDragEvent?: (event: React.DragEvent<HTMLDivElement>) => boolean;
    onCustomDrop?: (event: React.DragEvent<HTMLDivElement>) => void;
}

interface SplitFilesResult {
    allowedFiles: File[];
    rejectedFiles: File[];
}

const normalizeExtension = (extension: string) =>
    extension.trim().toLowerCase().replace(/^\./, "");

const getFileExtension = (fileName: string) => {
    const lastDotIndex = fileName.lastIndexOf(".");

    if (lastDotIndex <= 0 || lastDotIndex === fileName.length - 1) {
        return null;
    }

    return fileName.slice(lastDotIndex + 1).toLowerCase();
};

const isFileDragEvent = (event: React.DragEvent<HTMLDivElement>) =>
    Array.from(event.dataTransfer.types).includes("Files");

const splitFilesByExtension = (
    fileList: FileList,
    supportedExtensions: ReadonlySet<string>,
): SplitFilesResult => {
    const files = Array.from(fileList);

    if (!supportedExtensions.size) {
        return {
            allowedFiles: files,
            rejectedFiles: [],
        };
    }

    return files.reduce<SplitFilesResult>(
        (result, file) => {
            const extension = getFileExtension(file.name);

            if (extension && supportedExtensions.has(extension)) {
                result.allowedFiles.push(file);
                return result;
            }

            result.rejectedFiles.push(file);
            return result;
        },
        {
            allowedFiles: [],
            rejectedFiles: [],
        },
    );
};

export const useDropZone = ({
    supportedExtensions,
    onFilesDrop,
    onRejectedFiles,
    isCustomDragEvent,
    onCustomDrop,
}: UseDropZoneOptions) => {
    const [dragDepth, setDragDepth] = useState(0);
    const [activeDragKind, setActiveDragKind] = useState<DropZoneDragKind>("none");

    const normalizedExtensions = useMemo(
        () =>
            supportedExtensions
                .map(normalizeExtension)
                .filter((extension) => extension.length > 0),
        [supportedExtensions],
    );

    const supportedExtensionSet = useMemo(
        () => new Set(normalizedExtensions),
        [normalizedExtensions],
    );

    const handleDragOver = (event: React.DragEvent<HTMLDivElement>) => {
        if (!isFileDragEvent(event)) {
            if (!isCustomDragEvent?.(event)) {
                return;
            }
        }

        event.preventDefault();
        event.dataTransfer.dropEffect = "copy";
    };

    const handleDragEnter = (event: React.DragEvent<HTMLDivElement>) => {
        const isFileDrag = isFileDragEvent(event);
        const isCustomDrag = !isFileDrag && Boolean(isCustomDragEvent?.(event));

        if (!isFileDrag && !isCustomDrag) {
            return;
        }

        event.preventDefault();
        setActiveDragKind((currentKind) => {
            if (currentKind === "files" || isFileDrag) {
                return "files";
            }

            return "custom";
        });
        setDragDepth((currentDepth) => currentDepth + 1);
    };

    const handleDragLeave = (event: React.DragEvent<HTMLDivElement>) => {
        if (dragDepth === 0) {
            return;
        }

        event.preventDefault();

        if (event.relatedTarget === null) {
            setDragDepth(0);
            setActiveDragKind("none");
            return;
        }

        setDragDepth((currentDepth) => {
            const nextDepth = Math.max(currentDepth - 1, 0);

            if (nextDepth === 0) {
                setActiveDragKind("none");
            }

            return nextDepth;
        });
    };

    const handleDrop = (event: React.DragEvent<HTMLDivElement>) => {
        const isFileDrag = isFileDragEvent(event);
        const isCustomDrag =
            !isFileDrag &&
            (Boolean(isCustomDragEvent?.(event)) || activeDragKind === "custom");

        if (!isFileDrag && !isCustomDrag && activeDragKind === "none") {
            return;
        }

        event.preventDefault();
        setDragDepth(0);
        setActiveDragKind("none");

        if (isCustomDrag) {
            onCustomDrop?.(event);
            return;
        }

        const { allowedFiles, rejectedFiles } = splitFilesByExtension(
            event.dataTransfer.files,
            supportedExtensionSet,
        );

        if (allowedFiles.length > 0) {
            onFilesDrop(allowedFiles);
        }

        if (rejectedFiles.length > 0) {
            onRejectedFiles?.(rejectedFiles);
        }
    };

    return {
        isDragging: dragDepth > 0,
        activeDragKind,
        normalizedExtensions,
        rootProps: {
            onDragEnter: handleDragEnter,
            onDragOver: handleDragOver,
            onDragLeave: handleDragLeave,
            onDrop: handleDrop,
        },
    };
};
