import React, { useEffect } from "react";
import {
    Button,
    cn,
    ErrorIcon,
    LoaderIcon,
    SuccessIcon,
} from "@serene-ui/shared-frontend/shared";
import { useConnectionsModal } from "../../model/ConnectionsModalContext";

const AnimationStage = {
    Finished: "finished",
    Spinning: "spinning",
    SuccessBg: "success-bg",
    SuccessText: "success-text",
    ErrorBg: "error-bg",
    ErrorText: "error-text",
    FinishedBg: "finished-bg",
};

type AnimationStageType = (typeof AnimationStage)[keyof typeof AnimationStage];

interface TestConnectionButtonProps {
    children?: React.ReactNode;
}

export const AddConnectionButton: React.FC<TestConnectionButtonProps> = ({
    children,
}) => {
    const [animationStage, setAnimationStage] =
        React.useState<AnimationStageType>(AnimationStage.Finished);

    const {
        handleAddConnection,
        handleUpdateConnection,
        isAddPending: isPending,
        currentConnection,
        setIsGlobalDisabled,
    } = useConnectionsModal();
    const requestFired = React.useRef(false);

    function nextStage(
        stage: AnimationStageType,
        success: boolean,
    ): [AnimationStageType, number] | null {
        switch (stage) {
            case AnimationStage.Spinning:
                return [
                    success ? AnimationStage.SuccessBg : AnimationStage.ErrorBg,
                    1000,
                ];
            case AnimationStage.SuccessBg:
                return [AnimationStage.SuccessText, 1];
            case AnimationStage.SuccessText:
                return [AnimationStage.FinishedBg, 1000];
            case AnimationStage.ErrorBg:
                return [AnimationStage.ErrorText, 1];
            case AnimationStage.ErrorText:
                return [AnimationStage.FinishedBg, 1000];
            case AnimationStage.FinishedBg:
                return [AnimationStage.Finished, 100];
            default:
                return null;
        }
    }

    useEffect(() => {
        let timer: NodeJS.Timeout | null = null;

        if (
            animationStage === AnimationStage.Spinning &&
            !requestFired.current
        ) {
            requestFired.current = true;
            timer = setTimeout(() => {
                (async () => {
                    const isSuccess =
                        currentConnection.id === -1
                            ? await handleAddConnection()
                            : await handleUpdateConnection();
                    const transition = nextStage(animationStage, isSuccess);
                    if (transition) {
                        const [next] = transition;
                        setAnimationStage(next);
                    }
                })();
            }, 1000);
        } else if (!isPending) {
            const transition = nextStage(animationStage, true);
            if (animationStage === AnimationStage.Finished) {
                requestFired.current = false;
            }
            if (transition) {
                const [next, delay] = transition;
                timer = setTimeout(() => setAnimationStage(next), delay);
            }
        }

        return () => (timer ? clearTimeout(timer) : undefined);
    }, [isPending, animationStage, handleAddConnection]);

    const renderContent = () => {
        switch (animationStage) {
            case AnimationStage.Spinning:
                return <LoaderIcon className="animate-spin" />;
            case AnimationStage.SuccessBg:
            case AnimationStage.SuccessText:
                return (
                    <SuccessIcon
                        className={cn(
                            "duration-300",
                            animationStage === AnimationStage.SuccessText
                                ? "opacity-100"
                                : "opacity-0",
                        )}
                    />
                );
            case AnimationStage.ErrorBg:
            case AnimationStage.ErrorText:
                return (
                    <ErrorIcon
                        className={cn(
                            "duration-300",
                            animationStage === AnimationStage.ErrorText
                                ? "opacity-100"
                                : "opacity-0",
                        )}
                    />
                );
            default:
                return (
                    <div
                        className={cn(
                            "duration-300",
                            animationStage === AnimationStage.Finished
                                ? "opacity-100"
                                : "opacity-0",
                        )}>
                        {children || currentConnection.id === -1
                            ? "Connect & Add"
                            : "Connect & Update"}
                    </div>
                );
        }
    };

    return (
        <Button
            aria-label={
                currentConnection.id === -1
                    ? "Connect & Add"
                    : "Connect & Update"
            }
            className={cn(
                "duration-300",
                [AnimationStage.SuccessBg, AnimationStage.SuccessText].includes(
                    animationStage,
                ) && "bg-green-950  opacity-70 text-[#b5beb9]",
                [AnimationStage.ErrorBg, AnimationStage.ErrorText].includes(
                    animationStage,
                ) && "bg-red-950  opacity-70 text-[#b5beb9]",
                [
                    AnimationStage.SuccessBg,
                    AnimationStage.SuccessText,
                    AnimationStage.ErrorBg,
                    AnimationStage.ErrorText,
                    AnimationStage.Spinning,
                ].includes(animationStage) && "has-[>svg]:px-[9px]",
            )}
            onClick={() => {
                if (isPending) return;
                setIsGlobalDisabled(true);
                setAnimationStage("spinning");
            }}>
            {renderContent()}
        </Button>
    );
};
