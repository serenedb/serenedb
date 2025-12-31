import {
    Button,
    InputOTP,
    InputOTPGroup,
    InputOTPSeparator,
    InputOTPSlot,
    Label,
} from "@serene-ui/shared-frontend/shared";
import { CopyTextButton } from "@serene-ui/shared-frontend/features";

interface AuthorizationCodeFormProps {
    code: string;
    authLink: string;
    onVerify: () => void;
}

/**
 * Form component for entering and verifying the GitHub device authorization code.
 * Displays the authorization link and OTP input slots for the user to enter the code.
 */
export const AuthorizationCodeForm = ({
    code,
    authLink,
    onVerify,
}: AuthorizationCodeFormProps) => {
    return (
        <div className="flex flex-col gap-3">
            <div className="flex flex-col gap-1">
                <Label>1. Go to link</Label>
                <a
                    href={authLink}
                    target="_blank"
                    rel="noreferrer"
                    className="text-blue-600 hover:underline break-all">
                    {authLink}
                </a>
            </div>

            <div className="flex flex-col gap-2">
                <Label>2. Enter code</Label>
                <div className="flex gap-2">
                    <InputOTP
                        value={code}
                        maxLength={9}
                        onComplete={onVerify}
                        className="gap-2 mt-2">
                        <InputOTPGroup>
                            <InputOTPSlot index={0} />
                            <InputOTPSlot index={1} />
                            <InputOTPSlot index={2} />
                            <InputOTPSlot index={3} />
                        </InputOTPGroup>
                        <InputOTPSeparator />
                        {/* Github sends code in XXXX-XXXX format, that's why we need to skip 4th slot */}
                        <InputOTPGroup>
                            <InputOTPSlot index={5} />
                            <InputOTPSlot index={6} />
                            <InputOTPSlot index={7} />
                            <InputOTPSlot index={8} />
                        </InputOTPGroup>
                    </InputOTP>
                    <CopyTextButton text={code} />
                </div>
            </div>

            <div className="flex flex-col gap-1">
                <Label>3. Verify</Label>
                <Button onClick={onVerify}>Verify</Button>
            </div>
        </div>
    );
};
