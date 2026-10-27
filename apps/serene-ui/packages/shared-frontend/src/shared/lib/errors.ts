import {
    ORPCError,
    createORPCErrorFromJson,
    isORPCErrorJson,
} from "@orpc/client";

const DEFAULT_ERROR_MESSAGE = "An unexpected error occurred.";

const isNonEmptyString = (value: unknown): value is string =>
    typeof value === "string" && value.trim().length > 0;

const getMessageFromData = (data: unknown): string | undefined => {
    if (isNonEmptyString(data)) return data;

    if (data && typeof data === "object" && "message" in data) {
        const message = (data as { message?: unknown }).message;
        if (isNonEmptyString(message)) return message;
    }

    return undefined;
};

const getCauseMessage = (error: unknown): string | undefined => {
    if (!error || typeof error !== "object") return undefined;

    const cause = (error as { cause?: unknown }).cause;

    if (isNonEmptyString(cause)) return cause;
    if (cause instanceof Error && isNonEmptyString(cause.message)) {
        return cause.message;
    }

    if (cause && typeof cause === "object" && "message" in cause) {
        const message = (cause as { message?: unknown }).message;
        if (isNonEmptyString(message)) return message;
    }

    return undefined;
};

const coerceOrpcError = (error: unknown): ORPCError<any, any> | undefined => {
    if (error instanceof ORPCError) return error;
    if (isORPCErrorJson(error)) return createORPCErrorFromJson(error);
    return undefined;
};

const parseJsonErrorString = (value: string): Record<string, unknown> | null => {
    const trimmed = value.trim();
    if (!trimmed.startsWith("{") || !trimmed.endsWith("}")) return null;

    try {
        const parsed = JSON.parse(trimmed);
        if (parsed && typeof parsed === "object") {
            return parsed as Record<string, unknown>;
        }
    } catch {
        return null;
    }

    return null;
};

const buildMessageWithDetails = (
    base: string,
    details?: string,
    hint?: string,
    raw?: string,
) => {
    let message = base;

    if (details && !message.includes(details)) {
        message = `${message} Details: ${details}`;
    }

    if (hint && !message.includes(hint)) {
        message = `${message} Hint: ${hint}`;
    }

    if (raw && !message.includes(raw) && raw !== base) {
        message = `${message} Details: ${raw}`;
    }

    return message;
};

export const getErrorMessage = (
    error: unknown,
    fallback = DEFAULT_ERROR_MESSAGE,
): string => {
    if (typeof error === "string") {
        const parsed = parseJsonErrorString(error);
        if (parsed) {
            const message = getMessageFromData(
                (parsed as { userMessage?: unknown }).userMessage,
            );
            const raw = getMessageFromData(parsed);
            const detail = getMessageFromData(
                (parsed as { detail?: unknown }).detail,
            );
            const hint = getMessageFromData(
                (parsed as { hint?: unknown }).hint,
            );

            if (message) {
                return buildMessageWithDetails(message, detail, hint, raw);
            }

            if (raw) {
                return buildMessageWithDetails(raw, detail, hint);
            }
        }
    }

    const orpcError = coerceOrpcError(error);

    if (orpcError) {
        const data = orpcError.data as
            | { userMessage?: unknown; detail?: unknown; hint?: unknown }
            | undefined;
        const userMessage = getMessageFromData(data?.userMessage);
        const detail = getMessageFromData(data?.detail);
        const hint = getMessageFromData(data?.hint);
        const rawMessage = getMessageFromData(orpcError.data);

        if (userMessage) {
            return buildMessageWithDetails(
                userMessage,
                detail,
                hint,
                rawMessage,
            );
        }

        return (
            getMessageFromData(orpcError.data) ||
            (isNonEmptyString(orpcError.message) ? orpcError.message : undefined) ||
            getCauseMessage(orpcError) ||
            fallback
        );
    }

    if (error instanceof Error) {
        const raw = isNonEmptyString(error.message) ? error.message : undefined;
        return raw || getCauseMessage(error) || fallback;
    }

    if (isNonEmptyString(error)) return error;

    return fallback;
};
