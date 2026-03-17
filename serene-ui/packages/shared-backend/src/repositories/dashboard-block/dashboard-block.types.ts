export interface DashboardBlockRow {
    id: number | null;
    dashboard_id: number | null;
    type: string | null;
    position: number | null;
    bounds: string | null;
    text: string | null;
    connection_id: number | null;
    database: string | null;
    query: string | null;
    name: string | null;
    description: string | null;
    custom_refresh_interval_enabled: number | null;
    custom_refresh_interval: number | null;
    custom_row_limit_enabled: number | null;
    custom_row_limit: number | null;
    config: string | null;
}
