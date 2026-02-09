import * as React from "react";

import { cn } from "../lib/utils";
import { cva, VariantProps } from "class-variance-authority";

const inputVariants = cva(
    cn(
        "file:text-primary-foreground border-color-[#444444] placeholder:text-secondary-foreground selection:bg-primary text-primary-foreground selection:text-primary-foreground dark:bg-input/30 border-input flex h-8 w-full min-w-0 rounded-md border bg-transparent px-3 py-1 text-base transition-[color,box-shadow] outline-none file:inline-flex file:h-7 file:border-0 file:bg-transparent file:text-sm file:font-medium disabled:pointer-events-none disabled:cursor-not-allowed disabled:opacity-50 md:text-sm",
        "focus-visible:border-ring focus-visible:ring-ring/50 focus-visible:ring-[3px]",
        "aria-invalid:ring-destructive/20 dark:aria-invalid:ring-destructive/40 aria-invalid:border-destructive",
    ),
    {
        variants: {
            variant: {
                default:
                    "border-border placeholder:text-secondary-foreground/70 focus-visible:ring-ring/50",
                secondary:
                    "dark:bg-transparent !border-border outline-none !ring-0",
                ghost: "dark:bg-transparent !border-transparent outline-none !ring-0 w-max disabled:pointer-events-none disabled:cursor-not-allowed disabled:opacity-100",
            },
        },
        defaultVariants: {
            variant: "default",
        },
    },
);

function Input({
    className,
    type,
    variant,
    ...props
}: React.ComponentProps<"input"> & VariantProps<typeof inputVariants>) {
    return (
        <input
            type={type}
            data-slot="input"
            className={cn(inputVariants({ variant, className }))}
            {...props}
        />
    );
}

export { Input };
