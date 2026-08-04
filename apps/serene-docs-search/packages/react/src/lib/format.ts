/** Thousands-separated number for the terminal-style UI (thin spaces, not commas). */
export function fmt(n: number): string {
    return n.toLocaleString("en-US").replace(/,/g, " ");
}
