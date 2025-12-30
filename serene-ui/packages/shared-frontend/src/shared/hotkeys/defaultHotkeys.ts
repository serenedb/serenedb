enum DOCKER_DEFAULT_HOTKEYS {
    CONSOLE_NEW_TAB = "ctrl+t, meta+t",
    CONSOLE_CLOSE_TAB = "ctrl+w, meta+w",
    CONSOLE_TOGGLE_LAYOUT = "ctrl+l, meta+l",
    COMMAND_TOGGLE = "ctrl+j, meta+j",
    SETTINGS_TOGGLE = "ctrl+period, meta+period",
    CONSOLE_NEXT_TAB = "ctrl+right, meta+right",
    CONSOLE_PREVIOUS_TAB = "ctrl+left, meta+left",
    CONSOLE_TOGGLE_EXPLORER_EDITOR = "ctrl+e, meta+e",
}

enum ELECTRON_DEFAULT_HOTKEYS {
    CONSOLE_NEW_TAB = "ctrl+t, meta+t",
    CONSOLE_CLOSE_TAB = "ctrl+w, meta+w",
    CONSOLE_TOGGLE_LAYOUT = "ctrl+l, meta+l",
    COMMAND_TOGGLE = "ctrl+j, meta+j",
    SETTINGS_TOGGLE = "ctrl+period, meta+period",
    CONSOLE_NEXT_TAB = "ctrl+right, meta+right",
    CONSOLE_PREVIOUS_TAB = "ctrl+left, meta+left",
    CONSOLE_TOGGLE_EXPLORER_EDITOR = "ctrl+e, meta+e",
}

export const DEFAULT_HOTKEYS =
    DOCKER_DEFAULT_HOTKEYS || ELECTRON_DEFAULT_HOTKEYS;
