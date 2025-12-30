import { BindVarSchema } from "@serene-ui/shared-core";
import { cn, Input, Label } from "@serene-ui/shared-frontend";

interface BindVariablesProps {
    bind_vars?: BindVarSchema[];
    setBindVars?: (bind_vars: BindVarSchema[]) => void;
    className?: string;
}

export const BindVariables: React.FC<BindVariablesProps> = ({
    bind_vars,
    setBindVars,
    className,
}) => {
    const handleInputChange = (e: React.ChangeEvent<HTMLInputElement>) => {
        if (!bind_vars) return;
        const newBindVars = bind_vars.map((bind_var) => {
            if (bind_var.name === e.target.name) {
                return {
                    ...bind_var,
                    value: e.target.value,
                };
            }
            return bind_var;
        });
        setBindVars?.(newBindVars);
    };

    return (
        <div className={cn("w-65 h-full bg-popover rounded-md", className)}>
            <div className="pl-4 pr-2 py-3 border-b border-border flex items-center justify-between">
                <p className="text-sm font-medium">Bind variables</p>
            </div>
            <div className="flex flex-col gap-2 p-2">
                {bind_vars?.map((bind_var) => (
                    <div key={bind_var.name} className="flex flex-col gap-2">
                        <Label>{bind_var.name}</Label>
                        <Input
                            name={bind_var.name}
                            value={bind_var.value || ""}
                            onChange={handleInputChange}
                        />
                    </div>
                ))}
            </div>
        </div>
    );
};
