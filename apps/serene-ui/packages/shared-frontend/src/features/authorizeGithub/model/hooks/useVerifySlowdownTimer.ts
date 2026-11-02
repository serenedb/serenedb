import { useEffect, useState } from "react";

interface UseVerifySlowdownTimerProps {
    initialValue: number | null;
}

/**
 * Hook that manages the slow-down timer for GitHub authorization verification.
 * When the GitHub API responds with 'slow_down', this timer prevents excessive
 * verification attempts by waiting the specified interval before allowing retries.
 */
export const useVerifySlowdownTimer = ({
    initialValue,
}: UseVerifySlowdownTimerProps) => {
    const [verifySlowDownTimer, setVerifySlowDownTimer] = useState<
        number | null
    >(initialValue);

    useEffect(() => {
        if (verifySlowDownTimer === null) return;

        const timer = setInterval(() => {
            setVerifySlowDownTimer((prev) => {
                if (prev === null || prev <= 1) {
                    return null;
                }
                return prev - 1;
            });
        }, 1000);

        return () => clearInterval(timer);
    }, [verifySlowDownTimer]);

    return {
        verifySlowDownTimer,
        setVerifySlowDownTimer,
    };
};
