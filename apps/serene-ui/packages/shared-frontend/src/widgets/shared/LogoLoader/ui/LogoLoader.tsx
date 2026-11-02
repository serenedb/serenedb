import React, { useEffect } from "react";
import { useNavigate } from "react-router-dom";
import {
    navigationMap,
    SmallLogoIcon,
} from "@serene-ui/shared-frontend/shared";

interface LogoLoaderProps {
    circleCount?: number;
    animationDelayStep?: number;
    animationDuration?: number;
}

export const LogoLoader: React.FC<LogoLoaderProps> = ({
    circleCount = 4,
    animationDelayStep = 0.75,
    animationDuration = 2.75,
}) => {
    const loaderRef = React.createRef<HTMLDivElement>();
    const navigate = useNavigate();

    const waitForLoad = async () => {
        setTimeout(() => {
            loaderRef.current?.classList.add("opacity-0");
            setTimeout(() => {
                navigate(navigationMap.console);
            }, 300);
        }, 500);
    };

    useEffect(() => {
        waitForLoad();
    }, []);

    return (
        <div className="duration-300" ref={loaderRef}>
            <div className="relative flex justify-center items-center">
                <SmallLogoIcon className="z-10 w-19 h-19" />

                {Array.from({ length: circleCount }).map((_, index) => (
                    <span
                        key={index}
                        className={`absolute rounded-full border border-purple-400 opacity-50`}
                        style={{
                            animationName: "growShrink",
                            animationDuration: `${animationDuration}s`,
                            animationTimingFunction: "linear",
                            animationIterationCount: "infinite",
                            animationDelay: `${index * animationDelayStep}s`,
                        }}
                    />
                ))}

                <style>{`
        @keyframes growShrink {
          0% {
            width: 76px;
            height: 76px;
            border-width: 5px;
            opacity: 0.5;
          }
          100% {
            width: 250px;
            height: 250px;
            border-width: 2px;
            opacity: 0;
          }
        }
      `}</style>
            </div>
        </div>
    );
};
