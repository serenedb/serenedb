import { BindVarSchema } from "@serene-ui/shared-core";

export const syncBindVars = (query: string, bind_vars?: BindVarSchema[]) => {
    const variablePattern = /\$([a-zA-Z_][a-zA-Z0-9_]*)/g;
    const foundVariables = new Set<string>();
    let match;

    while ((match = variablePattern.exec(query)) !== null) {
        foundVariables.add(match[1]);
    }

    const currentBindVars = bind_vars || [];

    const existingBindVarsMap = new Map(
        currentBindVars.map((bv) => [bv.name, bv]),
    );

    const newBindVars = Array.from(foundVariables).map((varName) => {
        return (
            existingBindVarsMap.get(varName) || {
                name: varName,
                default_value: "",
                description: "",
                value: "",
            }
        );
    });

    return newBindVars;
};

export const prepareQuery = (query: string, bind_vars?: BindVarSchema[]) => {
    if (!bind_vars || bind_vars.length === 0) {
        return { query, values: [] };
    }

    const bindVarMap = new Map(
        bind_vars.map((bv) => [bv.name, bv.value || bv.default_value || ""]),
    );

    const variablePattern = /\$([a-zA-Z_][a-zA-Z0-9_]*)/g;
    const orderedValues: string[] = [];
    const seenVariables = new Map<string, number>();

    const preparedQuery = query.replace(variablePattern, (_match, varName) => {
        if (seenVariables.has(varName)) {
            return `$${seenVariables.get(varName)}`;
        }

        const value = bindVarMap.get(varName);

        orderedValues.push(value || "");
        const position = orderedValues.length;
        seenVariables.set(varName, position);

        return `$${position}`;
    });

    return { query: preparedQuery, values: orderedValues };
};
