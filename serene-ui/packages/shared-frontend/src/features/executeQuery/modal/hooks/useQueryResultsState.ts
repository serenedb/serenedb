import { useRef, useState } from "react";
import type { QueryResult } from "../types";

const MAX_RESULT_AGE_MS = 5 * 60 * 1000;

/**
 * Custom hook to manage the state for query results.
 *
 * @returns An object containing various functions and refs related to query results
 */
export const useQueryResultsState = () => {
    const queryResultsRef = useRef<Map<number, QueryResult>>(new Map());
    const subscribersRef = useRef<
        Map<number, Set<(result: QueryResult) => void>>
    >(new Map());
    const [pendingJobs, setPendingJobs] = useState<Set<number>>(new Set());
    const resultCleanupTimersRef = useRef<Map<number, NodeJS.Timeout>>(
        new Map(),
    );

    /**
     * Notify subscribers of a query result update.
     *
     * @param {number} jobId - The job ID of the query
     * @param {QueryResult} result - The updated query result
     */
    const notifySubscribers = (jobId: number, result: QueryResult) => {
        const callbacks = subscribersRef.current.get(jobId);
        if (callbacks) {
            callbacks.forEach((cb) => cb(result));
        }
    };

    /**
     * Store a query result.
     *
     * @param {number} jobId - The job ID of the query
     * @param {QueryResult} result - The query result to store
     */
    const setQueryResult = (jobId: number, result: QueryResult) => {
        queryResultsRef.current.set(jobId, result);
        notifySubscribers(jobId, result);
    };

    /**
     * Retrieve a query result.
     *
     * @param {number} jobId - The job ID of the query
     * @returns {QueryResult | undefined} - The query result, or undefined if not found
     */
    const getQueryResult = (jobId: number) => {
        return queryResultsRef.current.get(jobId);
    };

    /**
     * Remove a query result.
     *
     * @param {number} jobId - The job ID of the query to remove
     */
    const removeQueryResult = (jobId: number) => {
        queryResultsRef.current.delete(jobId);
        resultCleanupTimersRef.current.delete(jobId);
    };

    /**
     * Schedule a query result for cleanup after a certain period of time.
     *
     * @param {number} jobId - The job ID of the query
     */
    const scheduleCleanup = (jobId: number) => {
        const cleanupTimer = setTimeout(() => {
            removeQueryResult(jobId);
        }, MAX_RESULT_AGE_MS);
        resultCleanupTimersRef.current.set(jobId, cleanupTimer);
    };

    /**
     * Cancel the cleanup timer for a query result.
     *
     * @param {number} jobId - The job ID of the query
     */
    const cancelCleanup = (jobId: number) => {
        const existingTimer = resultCleanupTimersRef.current.get(jobId);
        if (existingTimer) {
            clearTimeout(existingTimer);
        }
    };

    /**
     * Subscribe to updates for a query result.
     *
     * @param {number} jobId - The job ID of the query
     * @param {(result) => void} callback - The callback function to call when the query result updates
     * @returns A function to unsubscribe from updates
     */
    const subscribe = (
        jobId: number,
        callback: (result: QueryResult) => void,
    ) => {
        if (!subscribersRef.current.has(jobId)) {
            subscribersRef.current.set(jobId, new Set());
        }
        subscribersRef.current.get(jobId)!.add(callback);

        const currentResult = queryResultsRef.current.get(jobId);
        if (currentResult) {
            callback(currentResult);
        }

        return () => {
            subscribersRef.current.get(jobId)?.delete(callback);
            if (subscribersRef.current.get(jobId)?.size === 0) {
                subscribersRef.current.delete(jobId);
            }
        };
    };

    return {
        queryResultsRef,
        subscribersRef,
        pendingJobs,
        setPendingJobs,
        resultCleanupTimersRef,
        notifySubscribers,
        setQueryResult,
        getQueryResult,
        removeQueryResult,
        scheduleCleanup,
        cancelCleanup,
        subscribe,
    };
};
