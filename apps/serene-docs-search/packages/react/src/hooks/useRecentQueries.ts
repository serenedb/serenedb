import { useCallback, useEffect, useState } from "react";
import type { SearchStorage } from "../lib/storage";

/** Recent-queries list for the empty state, re-read while the modal is open. */
export function useRecentQueries(storage: SearchStorage, open: boolean, query: string) {
    const [recent, setRecent] = useState<string[]>([]);
    useEffect(() => {
        if (open) setRecent(storage.getRecent());
    }, [open, storage, query]);

    const removeRecent = useCallback(
        (q: string) => {
            storage.removeRecent(q);
            setRecent(storage.getRecent());
        },
        [storage],
    );

    const clearRecent = useCallback(() => {
        storage.clearRecent();
        setRecent([]);
    }, [storage]);

    return { recent, removeRecent, clearRecent };
}
