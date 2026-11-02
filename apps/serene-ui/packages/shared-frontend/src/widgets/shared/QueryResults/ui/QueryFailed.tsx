import { PG_ERROR_CODES } from "../model/consts";

interface QueryFailedProps {
    error: string;
}

interface PostgresError {
    code?: string;
    severity?: string;
    message?: string;
    position?: string;
    [key: string]: any;
}

const getErrorDetails = (errorCode: string) => {
    const errorInfo = PG_ERROR_CODES.find((e) => e.code === errorCode);
    return errorInfo;
};

const formatErrorMessage = (errorObj: PostgresError): string => {
    const errorDetails = errorObj.code ? getErrorDetails(errorObj.code) : null;

    if (errorDetails) {
        const readableMessage = errorDetails.message
            .split("_")
            .map((word) => word.charAt(0).toUpperCase() + word.slice(1))
            .join(" ");
        return readableMessage;
    }

    return errorObj.message || "An unknown error occurred";
};

export const QueryFailed = ({ error }: QueryFailedProps) => {
    let errorObject: PostgresError;
    try {
        errorObject = JSON.parse(error);
    } catch {
        errorObject = { message: error };
    }

    const errorDetails = errorObject.code
        ? getErrorDetails(errorObject.code)
        : null;
    const userFriendlyMessage = formatErrorMessage(errorObject);

    return (
        <div className="p-4 flex-1">
            <div className="flex items-start gap-3">
                <div className="flex-shrink-0 text-red-500">
                    <svg
                        className="w-6 h-6"
                        fill="none"
                        stroke="currentColor"
                        viewBox="0 0 24 24">
                        <path
                            strokeLinecap="round"
                            strokeLinejoin="round"
                            strokeWidth={2}
                            d="M12 8v4m0 4h.01M21 12a9 9 0 11-18 0 9 9 0 0118 0z"
                        />
                    </svg>
                </div>
                <div className="flex-1 min-w-0">
                    <h3 className="text-sm font-semibold text-red-800 mb-2">
                        Query Failed
                    </h3>
                    <div className="space-y-2">
                        <p className="text-sm text-red-700 font-medium">
                            {userFriendlyMessage}
                        </p>
                        {errorDetails && (
                            <p className="text-xs text-red-600">
                                {errorDetails.class}
                                {errorObject.code && (
                                    <span className="ml-2 text-red-500">
                                        (Code: {errorObject.code})
                                    </span>
                                )}
                            </p>
                        )}
                        {errorObject.position && (
                            <p className="text-xs text-red-600">
                                Position: {errorObject.position}
                            </p>
                        )}
                        <details className="text-xs">
                            <summary className="cursor-pointer text-red-600 hover:text-red-700">
                                Technical Details
                            </summary>
                            <pre className="mt-2 bg-secondary border-1 border-border rounded p-3 overflow-x-auto">
                                <code className="text-red-700">
                                    {JSON.stringify(errorObject, null, 2)}
                                </code>
                            </pre>
                        </details>
                    </div>
                </div>
            </div>
        </div>
    );
};
