import { MonacoEditor } from "@serene-ui/shared-frontend/shared";
import React from "react";

interface JSONEditorProps {
    value: string;
    onChange: (value: string) => void;
    readOnly?: boolean;
}

export const JSONEditor: React.FC<JSONEditorProps> = ({
    value,
    onChange,
    readOnly = false,
}) => {
    const tryPretty = (input: string): string | null => {
        try {
            const parsed = JSON.parse(input);
            return JSON.stringify(parsed, null, 2);
        } catch {
            return null;
        }
    };
    const handleChange = (v: string) => {
        const p = tryPretty(v);
        onChange(p ?? v);
    };

    return (
        <MonacoEditor
            language="json"
            options={{
                suggestOnTriggerCharacters: true,
                quickSuggestions: true,
                wordBasedSuggestions: "allDocuments",
                minimap: {
                    enabled: false,
                },
                readOnly,
                automaticLayout: true,
                lineNumbersMinChars: 4,
            }}
            value={value}
            onChange={handleChange}
        />
    );
};
