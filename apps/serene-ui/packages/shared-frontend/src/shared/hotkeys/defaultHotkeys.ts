enum DOCKER_DEFAULT_HOTKEYS {
    CONSOLE_NEW_TAB = "ctrl+t, meta+t",
    CONSOLE_CLOSE_TAB = "ctrl+w, meta+w",
    CONSOLE_TOGGLE_LAYOUT = "ctrl+l, meta+l",
    COMMAND_TOGGLE = "ctrl+j, meta+j",
    SWITCH_CONNECTION_TOGGLE = "meta+k",
    SETTINGS_TOGGLE = "ctrl+period, meta+period",
    CONSOLE_NEXT_TAB = "ctrl+right, meta+right",
    CONSOLE_PREVIOUS_TAB = "ctrl+left, meta+left",
    CONSOLE_TOGGLE_EXPLORER_EDITOR = "ctrl+e, meta+e",
    CONSOLE_FOCUS_WINDOW_DOWN =
        "ctrl+alt+j, meta+alt+j, ctrl+alt+down, meta+alt+down",
    CONSOLE_FOCUS_WINDOW_UP =
        "ctrl+alt+k, meta+alt+k, ctrl+alt+up, meta+alt+up",
    CONSOLE_FOCUS_WINDOW_LEFT =
        "ctrl+alt+h, meta+alt+h, ctrl+alt+left, meta+alt+left",
    CONSOLE_FOCUS_WINDOW_RIGHT =
        "ctrl+alt+l, meta+alt+l, ctrl+alt+right, meta+alt+right",
}

enum ELECTRON_DEFAULT_HOTKEYS {
    CONSOLE_NEW_TAB = "ctrl+t, meta+t",
    CONSOLE_CLOSE_TAB = "ctrl+w, meta+w",
    CONSOLE_TOGGLE_LAYOUT = "ctrl+l, meta+l",
    COMMAND_TOGGLE = "ctrl+j, meta+j",
    SWITCH_CONNECTION_TOGGLE = "meta+k",
    SETTINGS_TOGGLE = "ctrl+period, meta+period",
    CONSOLE_NEXT_TAB = "ctrl+right, meta+right",
    CONSOLE_PREVIOUS_TAB = "ctrl+left, meta+left",
    CONSOLE_TOGGLE_EXPLORER_EDITOR = "ctrl+e, meta+e",
    CONSOLE_FOCUS_WINDOW_DOWN =
        "ctrl+alt+j, meta+alt+j, ctrl+alt+down, meta+alt+down",
    CONSOLE_FOCUS_WINDOW_UP =
        "ctrl+alt+k, meta+alt+k, ctrl+alt+up, meta+alt+up",
    CONSOLE_FOCUS_WINDOW_LEFT =
        "ctrl+alt+h, meta+alt+h, ctrl+alt+left, meta+alt+left",
    CONSOLE_FOCUS_WINDOW_RIGHT =
        "ctrl+alt+l, meta+alt+l, ctrl+alt+right, meta+alt+right",
}

export const DEFAULT_HOTKEYS =
    DOCKER_DEFAULT_HOTKEYS || ELECTRON_DEFAULT_HOTKEYS;
