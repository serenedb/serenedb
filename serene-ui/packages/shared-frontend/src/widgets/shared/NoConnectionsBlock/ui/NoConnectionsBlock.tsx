import { OpenConnectionsModalButton } from "@serene-ui/shared-frontend/features";
import { Button, UploadIcon } from "@serene-ui/shared-frontend/shared";
import React from "react";

interface NoConnectionsBlockProps {}

export const NoConnectionsBlock: React.FC<NoConnectionsBlockProps> = () => {
    return (
        <div className="flex flex-col gap-1 ">
            <div className="bg-secondary rounded-md px-8 py-4 w-max">
                <p>No connections yet!</p>
                <p className="text-sm font-light opacity-30">
                    Want to add some?
                </p>
            </div>
            <div className="flex gap-1">
                <OpenConnectionsModalButton className="flex-1" />
                <Button
                    variant="secondary"
                    size="icon"
                    title="Import connections">
                    <UploadIcon className="size-3.5 mt-0.5" />
                </Button>
            </div>
        </div>
    );
};
