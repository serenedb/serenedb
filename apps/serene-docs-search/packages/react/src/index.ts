export { SereneDocsSearch, SereneDocsSearchButton } from "./SereneDocsSearch";
export type { SereneDocsSearchProps } from "./SereneDocsSearch";
export { createMcpSetupInstructions } from "./search/McpSetup";
export type {
    McpClientSetup,
    McpSetupInstructions,
    SereneDocsSearchMcpOptions,
} from "./search/McpSetup";
export { useSereneDocsSearch } from "./hooks/useSereneDocsSearch";
export type {
    AskState,
    AskTurn,
    ConnectionStatus,
    ResultGroup,
    SearchPhase,
    SereneDocsSearch as SereneDocsSearchApi,
    UseSereneDocsSearchOptions,
} from "./hooks/useSereneDocsSearch";
export { applyPendingHighlight, highlightOnPage } from "./lib/highlight";
export { formatHotkey } from "./lib/hotkey";
export { groupResultsBySections } from "./lib/sections";
export type { GroupedSearchResults, SectionResultGroup } from "./lib/sections";
export {
    SereneSearchClient,
    type HealthResponse,
    type SearchResultItem,
    type SearchSectionConfig,
    type SereneSearchConfig,
    type SyncProgress,
} from "@serenedb/docs-search-core";
