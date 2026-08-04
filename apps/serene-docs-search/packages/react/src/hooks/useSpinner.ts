import { useEffect, useState } from "react";

/** The design's ASCII spinner frames. */
const SPIN = [".", ":", ";", "!", "i", "l", "v", "c", "o", "e", "n", "u", "z", "x", "%", "&", "8", "$", "#", "@"];

export function useSpinner(active: boolean): string {
    const [i, setI] = useState(0);
    useEffect(() => {
        if (!active) return;
        const t = window.setInterval(() => setI((x) => (x + 1) % SPIN.length), 110);
        return () => window.clearInterval(t);
    }, [active]);
    return SPIN[i];
}
