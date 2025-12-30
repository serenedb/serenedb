interface AuthorizationTimerProps {
    codeTimeLeft: number | null;
}

/**
 * Displays the countdown timer for authorization code expiration
 */
export const AuthorizationTimer = ({
    codeTimeLeft,
}: AuthorizationTimerProps) => {
    const formatTime = (seconds: number) => {
        const minutes = Math.floor(seconds / 60);
        const remainingSeconds = seconds % 60;
        return `${minutes}:${remainingSeconds.toString().padStart(2, "0")}`;
    };

    if (codeTimeLeft === null) {
        return null;
    }

    return (
        <div className="flex items-center justify-between px-3 py-2 bg-secondary rounded-md">
            <span className="text-sm text-primary-foreground">
                Code expires in:
            </span>
            <span className="text-sm font-mono font-semibold">
                {formatTime(codeTimeLeft)}
            </span>
        </div>
    );
};
