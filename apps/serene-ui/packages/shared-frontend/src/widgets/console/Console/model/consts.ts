export const DEFAULT_CONSOLE_QUERY_LIMIT = 1000;
export const DEFAULT_CONSOLE_SPAWN_RESULTS_IN_FIRST_TAB = false;
export const DEFAULT_CONSOLE_COLORFUL_TYPES_IN_RESULTS = true;
export const DEFAULT_CONSOLE_SELECT_RELATED_RESULT_ON_TAB_CHANGE = false;
export const DEFAULT_CONSOLE_SHOW_JSON_BY_DEFAULT = false;
export const DEFAULT_CONSOLE_SHOW_SAVED_QUERIES_IN_AUTOCOMPLETE = true;
export const DEFAULT_CONSOLE_SHOW_EXECUTION_HISTORY_IN_AUTOCOMPLETE = true;
export const DEFAULT_CONSOLE_SHOW_AUTOCOMPLETE = true;
export const DEFAULT_CONSOLE_EXECUTE_SEQUENTIALLY_BY_DEFAULT = false;
export const DEFAULT_CONSOLE_EXECUTE_IN_NEW_TAB_BY_DEFAULT = false;
export const DEFAULT_CONSOLE_ALERT_ON_EXECUTION = "onlyUnseen";

export const CONSOLE_LIMIT_STORAGE_KEY = "console:rows-limit";
export const CONSOLE_SPAWN_RESULTS_IN_FIRST_TAB_STORAGE_KEY =
    "console:spawn-results-in-first-tab";
export const CONSOLE_COLORFUL_TYPES_IN_RESULTS_STORAGE_KEY =
    "console:colorful-types-in-results";
export const CONSOLE_SELECT_RELATED_RESULT_ON_TAB_CHANGE_STORAGE_KEY =
    "console:select-related-result-on-tab-change";
export const CONSOLE_SHOW_JSON_BY_DEFAULT_STORAGE_KEY =
    "console:show-json-by-default";
export const CONSOLE_SHOW_SAVED_QUERIES_IN_AUTOCOMPLETE_STORAGE_KEY =
    "console:show-saved-queries-in-autocomplete";
export const CONSOLE_SHOW_EXECUTION_HISTORY_IN_AUTOCOMPLETE_STORAGE_KEY =
    "console:show-execution-history-in-autocomplete";
export const CONSOLE_SHOW_AUTOCOMPLETE_STORAGE_KEY = "console:show-autocomplete";
export const CONSOLE_EXECUTE_SEQUENTIALLY_BY_DEFAULT_STORAGE_KEY =
    "console:execute-sequentially-by-default";
export const CONSOLE_EXECUTE_IN_NEW_TAB_BY_DEFAULT_STORAGE_KEY =
    "console:execute-in-new-tab-by-default";
export const CONSOLE_ALERT_ON_EXECUTION_STORAGE_KEY =
    "console:alert-on-execution";
export const CONSOLE_SIDEBAR_COLLAPSED_STORAGE_KEY =
    "console:sidebar-collapsed";
export const CONSOLE_SETTINGS_SIDEBAR_COLLAPSED_STORAGE_KEY =
    "console:settings-sidebar-collapsed";
export const CONSOLE_EXECUTION_HISTORY_SIDEBAR_COLLAPSED_STORAGE_KEY =
    "console:execution-history-sidebar-collapsed";

export const CONSOLE_GRID_EDITOR_PANEL_ID = "console-grid-editor";
export const CONSOLE_GRID_SIDEBAR_PANEL_ID = "console-grid-sidebar";
export const CONSOLE_GRID_RIGHT_SIDEBAR_PANEL_ID = "console-grid-right-sidebar";
export const CONSOLE_GRID_SETTINGS_PANEL_ID = "console-grid-settings";
export const CONSOLE_GRID_EXECUTION_HISTORY_PANEL_ID =
    "console-grid-execution-history";

export const CONSOLE_SIDEBAR_SIZE = 300;
export const CONSOLE_SIDEBAR_MIN_SIZE = 300;
export const CONSOLE_RIGHT_SIDEBAR_SIZE = 300;
export const CONSOLE_RIGHT_SIDEBAR_MIN_SIZE = 300;
export const CONSOLE_EDITOR_MIN_WIDTH = 720;
export const CONSOLE_MAIN_AREA_MIN_WIDTH = 1020;

export const CONSOLE_SIDEBAR_ROOT_SELECTOR = "[data-console-sidebar-root='true']";
export const CONSOLE_EDITOR_ROOT_SELECTOR = "[data-console-editor-root='true']";
export const CONSOLE_SIDEBAR_SECTION_IDS = [
    "pinned",
    "entities",
    "savedQueries",
] as const;
