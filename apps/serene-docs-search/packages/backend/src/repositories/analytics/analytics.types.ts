export interface AnalyticsReport {
    topQueries: { q: string; count: number; hits: number }[];
    noHitQueries: { q: string; count: number }[];
    topClicked: { url: string; title: string; clicks: number }[];
}
