import { Checkbox, Label } from "@serene-ui/shared-frontend/shared";

interface StarAgreementProps {
    checked: boolean;
    onCheckedChange: (checked: boolean) => void;
}

/**
 * Checkbox component for user agreement to leave a star on GitHub
 * after successful authorization.
 */
export const StarAgreement = ({
    checked,
    onCheckedChange,
}: StarAgreementProps) => {
    return (
        <div className="flex items-center gap-3">
            <Checkbox
                checked={checked}
                onCheckedChange={(e) => onCheckedChange(e === true)}
                id="github-star"
            />
            <Label htmlFor="github-star" className="cursor-pointer">
                I agree to leave a star on GitHub
            </Label>
        </div>
    );
};
