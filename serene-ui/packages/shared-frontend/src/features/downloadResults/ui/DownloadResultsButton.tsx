import {
    ArrowDownIcon,
    Button,
    HoverCard,
    HoverCardContent,
    HoverCardTrigger,
} from "@serene-ui/shared-frontend/shared";
import { useDownloadResults } from "../model/useDownloadResults";
import { useCallback, useState } from "react";

interface DownloadResultsButtonProps {
    rows?: Record<string, unknown>[];
}

export const DownloadResultsButton: React.FC<DownloadResultsButtonProps> = ({
    rows,
}) => {
    const { downloadCSV, downloadJSON, copyCSV, copyJSON, isLoading } =
        useDownloadResults();
    const [isOpen, setIsOpen] = useState(false);

    const disabled = !rows || rows.length === 0;

    const handleDownloadCSV = useCallback(async () => {
        if (rows) {
            await downloadCSV(rows);
            setIsOpen(false);
        }
    }, [rows, downloadCSV]);

    const handleDownloadJSON = useCallback(async () => {
        if (rows) {
            await downloadJSON(rows);
            setIsOpen(false);
        }
    }, [rows, downloadJSON]);

    const handleCopyCSV = useCallback(async () => {
        if (rows) {
            await copyCSV(rows);
            setIsOpen(false);
        }
    }, [rows, copyCSV]);

    const handleCopyJSON = useCallback(async () => {
        if (rows) {
            await copyJSON(rows);
            setIsOpen(false);
        }
    }, [rows, copyJSON]);

    return (
        <HoverCard openDelay={0} open={isOpen} onOpenChange={setIsOpen}>
            <HoverCardTrigger asChild>
                <Button
                    variant="thirdly"
                    disabled={disabled || isLoading}
                    aria-label={`Download results (${rows?.length || 0} items)`}
                    aria-expanded={isOpen}
                    aria-haspopup="menu">
                    Download <ArrowDownIcon />
                </Button>
            </HoverCardTrigger>
            <HoverCardContent
                align="end"
                className="w-56 p-0 overflow-hidden"
                role="menu"
                aria-label="Download/Copy options">
                <div className="flex flex-col p-1 gap-1">
                    <Button
                        className="justify-between flex-1"
                        variant="ghost"
                        disabled={disabled || isLoading}
                        onClick={handleDownloadJSON}
                        role="menuitem"
                        aria-label="Download as JSON"
                        title="Download data as JSON file">
                        <span className="flex items-center gap-2">
                            <span>Download as JSON</span>
                        </span>
                    </Button>
                    <Button
                        className="justify-between flex-1"
                        variant="ghost"
                        disabled={disabled || isLoading}
                        onClick={handleDownloadCSV}
                        role="menuitem"
                        aria-label="Download as CSV"
                        title="Download data as CSV file (comma-separated values)">
                        <span className="flex items-center gap-2">
                            <span>Download as CSV</span>
                        </span>
                    </Button>
                    <Button
                        className="justify-between flex-1"
                        variant="ghost"
                        disabled={disabled || isLoading}
                        onClick={handleCopyJSON}
                        role="menuitem"
                        aria-label="Copy as JSON"
                        title="Copy data as JSON to clipboard">
                        <span className="flex items-center gap-2">
                            <span>Copy as JSON</span>
                        </span>
                    </Button>
                    <Button
                        className="justify-between flex-1"
                        variant="ghost"
                        disabled={disabled || isLoading}
                        onClick={handleCopyCSV}
                        role="menuitem"
                        aria-label="Copy as CSV"
                        title="Copy data as CSV to clipboard">
                        <span className="flex items-center gap-2">
                            <span>Copy as CSV</span>
                        </span>
                    </Button>
                </div>
            </HoverCardContent>
        </HoverCard>
    );
};
