import { useState } from "react";
import { Button, CheckIcon, CopyIcon } from "@serene-ui/shared-frontend/shared";

interface CopyTextButtonProps {
    text: string;
}

export const CopyTextButton = ({ text }: CopyTextButtonProps) => {
    const [copied, setCopied] = useState(false);

    const handleCopy = async () => {
        await navigator.clipboard.writeText(text);
        setCopied(true);
        setTimeout(() => setCopied(false), 2000);
    };

    return (
        <Button
            variant="outline"
            size="icon"
            className="size-9 bg-secondary"
            title={copied ? "Copied" : "Copy to clipboard"}
            onClick={handleCopy}>
            {copied ? <CheckIcon /> : <CopyIcon />}
        </Button>
    );
};
