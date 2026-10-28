import * as React from "react";
import { HoverCard, HoverCardTrigger, HoverCardContent } from "./hover-card";
import { Button, buttonVariants } from "./button";
import { type VariantProps } from "class-variance-authority";

interface ButtonCardProps {
    children: React.ReactNode;
}

const ButtonCard = ({ children }: ButtonCardProps) => {
    return <HoverCard>{children}</HoverCard>;
};

type ButtonCardButtonContentProps = React.ComponentProps<"button"> &
    VariantProps<typeof buttonVariants> & {
        asChild?: boolean;
    };

const ButtonCardButtonContent = ({
    children,
    asChild = false,
    className,
    variant,
    size,
    ...props
}: ButtonCardButtonContentProps) => {
    return (
        <HoverCardTrigger asChild>
            <Button
                asChild={asChild}
                className={className}
                variant={variant}
                size={size}
                {...props}>
                {children}
            </Button>
        </HoverCardTrigger>
    );
};

interface ButtonCardContentProps {
    children: React.ReactNode;
    className?: string;
}

const ButtonCardContent = ({
    children,
    className = "w-80",
}: ButtonCardContentProps) => {
    return (
        <HoverCardContent align="start" className={className}>
            {children}
        </HoverCardContent>
    );
};

export { ButtonCard, ButtonCardButtonContent, ButtonCardContent };
