import { useEffect, useState } from "react";

interface UseDeviceAuthorizationTimerProps {
    codeTimeLeft: number | null;
    onTimeExpired: () => void;
}

/**
 * Hook that manages the device authorization code expiration timer.
 * Decrements the time left by 1 second on each interval until it reaches 0,
 * then calls the onTimeExpired callback to refresh the authorization code.
 */
export const useDeviceAuthorizationTimer = ({
    codeTimeLeft,
    onTimeExpired,
}: UseDeviceAuthorizationTimerProps) => {
    const [timeLeft, setTimeLeft] = useState<number | null>(codeTimeLeft);

    useEffect(() => {
        setTimeLeft(codeTimeLeft);
    }, [codeTimeLeft]);

    useEffect(() => {
        if (timeLeft === null || timeLeft <= 0) return;

        const timer = setInterval(() => {
            setTimeLeft((prev) => {
                if (prev === null || prev <= 1) {
                    onTimeExpired();
                    return null;
                }
                return prev - 1;
            });
        }, 1000);

        return () => clearInterval(timer);
    }, [onTimeExpired]);
};
