import { useHotkeys } from "react-hotkeys-hook";
import { DEFAULT_HOTKEYS } from "./defaultHotkeys";

export const useAppHotkey = (
    hotkey: (typeof DEFAULT_HOTKEYS)[keyof typeof DEFAULT_HOTKEYS],
    callback: (e: KeyboardEvent) => void,
    deps?: unknown[],
) => {
    useHotkeys(hotkey, callback, { preventDefault: true }, deps);
};
