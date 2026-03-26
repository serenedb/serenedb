interface QuerySucceededProps {
    message?: string;
}

export const QuerySucceeded = ({ message }: QuerySucceededProps) => {
    return (
        <div className="p-4 flex-1">
            <div className="flex items-start gap-3">
                <div className="flex-shrink-0 text-green-600">
                    <svg
                        className="w-6 h-6"
                        fill="none"
                        stroke="currentColor"
                        viewBox="0 0 24 24">
                        <path
                            strokeLinecap="round"
                            strokeLinejoin="round"
                            strokeWidth={2}
                            d="M9 12.75 11.25 15 15 9.75M21 12a9 9 0 1 1-18 0 9 9 0 0 1 18 0Z"
                        />
                    </svg>
                </div>
                <div className="flex-1 min-w-0">
                    <h3 className="text-sm font-semibold text-green-800 mb-1">
                        Query Executed
                    </h3>
                    <p className="text-sm text-green-700">
                        {message || "Query successfully executed."}
                    </p>
                </div>
            </div>
        </div>
    );
};
