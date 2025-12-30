import { useEffect, useRef, useState, useMemo } from "react";
import { QueryResult, useQueryResults } from "../..";

const isFinalResult = (r: QueryResult) =>
    r.status === "success" || r.status === "failed";

/**
 * Custom hook to manage query result subscriptions.
 * Handles subscription lifecycle and cleanup automatically.
 *
 * Supports both single job ID and multiple job IDs:
 * - Single job: useQuerySubscription(jobId) returns QueryResult | undefined
 * - Multiple jobs: useQuerySubscription([jobId1, jobId2]) returns Map<number, QueryResult>
 *
 * @param jobIds - Single job ID, array of job IDs, or undefined
 * @param onResultReceived - Optional callback when results are received
 * @returns Query result(s) - single result or map of results
 */

export function useQuerySubscription(
    jobIds: number | undefined,
    onResultReceived?: (jobId: number, result: QueryResult) => void,
): QueryResult | undefined;

export function useQuerySubscription(
    jobIds: number[] | undefined,
    onResultReceived?: (jobId: number, result: QueryResult) => void,
): Map<number, QueryResult>;

export function useQuerySubscription(
    jobIds: number | number[] | undefined,
    onResultReceived?: (jobId: number, result: QueryResult) => void,
): QueryResult | undefined | Map<number, QueryResult> {
    const { subscribe } = useQueryResults();
    const [singleResult, setSingleResult] = useState<QueryResult | undefined>(
        undefined,
    );
    const [multipleResults, setMultipleResults] = useState<
        Map<number, QueryResult>
    >(new Map());
    const unsubscribersRef = useRef<Map<number, () => void>>(new Map());
    const callbackRef = useRef(onResultReceived);

    useEffect(() => {
        callbackRef.current = onResultReceived;
    }, [onResultReceived]);

    const isSingleMode = typeof jobIds === "number";

    const normalizedJobIds = useMemo<readonly number[]>(
        () => (Array.isArray(jobIds) ? jobIds : jobIds != null ? [jobIds] : []),
        [jobIds],
    );

    useEffect(() => {
        if (isSingleMode) {
            setMultipleResults(new Map());
        } else {
            setSingleResult(undefined);
        }
    }, [isSingleMode]);

    useEffect(() => {
        const current = unsubscribersRef.current;

        const jobIdSet = new Set<number>(normalizedJobIds);

        current.forEach((unsubscribe, jobId) => {
            if (!jobIdSet.has(jobId)) {
                unsubscribe();
                current.delete(jobId);
            }
        });

        normalizedJobIds.forEach((jobId) => {
            if (!current.has(jobId)) {
                const unsubscribe = subscribe(jobId, (queryResult) => {
                    if (isSingleMode) {
                        setSingleResult(queryResult);
                    } else {
                        setMultipleResults((prev) => {
                            const updated = new Map(prev);
                            updated.set(jobId, queryResult);
                            return updated;
                        });
                    }

                    if (isFinalResult(queryResult)) {
                        callbackRef.current?.(jobId, queryResult);

                        if (unsubscribersRef.current.has(jobId)) {
                            const unsub = unsubscribersRef.current.get(jobId);
                            unsub?.();
                            unsubscribersRef.current.delete(jobId);
                        }
                    }
                });
                current.set(jobId, unsubscribe);
            }
        });

        return () => {
            current.forEach((unsub) => unsub());
            current.clear();
        };
    }, [normalizedJobIds, subscribe]);

    if (isSingleMode) {
        return singleResult;
    }

    return multipleResults;
}
