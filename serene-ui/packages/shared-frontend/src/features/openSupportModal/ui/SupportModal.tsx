"use client";

import { useState } from "react";
import {
    Dialog,
    DialogContent,
    DialogHeader,
    DialogTitle,
    DialogDescription,
    Label,
    Input,
    Textarea,
    Button,
} from "@serene-ui/shared-frontend/shared";
import { useSupportModal } from "../model";
import {
    AuthorizeGithubButton,
    useAuthorizeGithub,
} from "@serene-ui/shared-frontend/features";
import { useCreateIssue } from "@serene-ui/shared-frontend/entities";
import { toast } from "sonner";

export const SupportModal = () => {
    const { open, setOpen } = useSupportModal();
    const { authorized } = useAuthorizeGithub();
    const [title, setTitle] = useState("");
    const [description, setDescription] = useState("");
    const createIssueMutation = useCreateIssue();

    return (
        <Dialog open={open} onOpenChange={setOpen}>
            <DialogContent className="w-max">
                <div className="flex flex-col gap-2 max-w-[396px]">
                    <DialogHeader className="items-start text-left gap-1">
                        <DialogTitle className="text-2xl font-bold">
                            Support
                        </DialogTitle>
                        <DialogDescription className="text-sm text-primary-foreground">
                            If you have any questions, need help, or want to
                            suggest a feature, feel free to open an issue on our
                            GitHub repository.
                        </DialogDescription>
                    </DialogHeader>
                    <p className="text-sm text-primary-foreground">
                        {authorized
                            ? "You are already logged in with your GitHub account, so you can open an issue directly in the app."
                            : "You can also do this directly in the app by logging in with your GitHub account."}
                    </p>
                    <AuthorizeGithubButton />
                    {authorized && (
                        <div className="flex flex-col gap-2 mt-2">
                            <div className="flex flex-col gap-1">
                                <Label>Title</Label>
                                <Input
                                    value={title}
                                    onChange={(e) => setTitle(e.target.value)}
                                    placeholder="Brief description of your issue"
                                />
                            </div>
                            <div className="flex flex-col gap-1">
                                <Label>Description</Label>
                                <Textarea
                                    className="resize-none"
                                    value={description}
                                    onChange={(e) =>
                                        setDescription(e.target.value)
                                    }
                                    placeholder="Provide more details about your issue"
                                    rows={4}
                                />
                            </div>
                            <Button
                                onClick={async () => {
                                    if (!title.trim() || !description.trim()) {
                                        toast.error(
                                            "Please fill in both title and description",
                                        );
                                        return;
                                    }
                                    try {
                                        const result =
                                            await createIssueMutation.mutateAsync(
                                                {
                                                    title,
                                                    body: description,
                                                },
                                            );
                                        toast.success(
                                            "Issue created successfully!",
                                        );
                                        setTitle("");
                                        setDescription("");
                                        setOpen(false);
                                        if (result.issue_url) {
                                            window.open(
                                                result.issue_url,
                                                "_blank",
                                            );
                                        }
                                    } catch (error) {
                                        console.error(error);
                                        toast.error("Failed to create issue");
                                    }
                                }}
                                disabled={createIssueMutation.isPending}>
                                {createIssueMutation.isPending
                                    ? "Submitting..."
                                    : "Submit"}
                            </Button>
                        </div>
                    )}
                </div>
            </DialogContent>
        </Dialog>
    );
};
