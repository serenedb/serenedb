import { contextBridge, ipcRenderer } from "electron";

type ThemePreference = "dark" | "light" | "system";

const getInitialThemePreference = (): ThemePreference => {
    const themeArg = process.argv.find((arg) =>
        arg.startsWith("--theme-preference="),
    );
    const value = themeArg?.split("=")[1];

    if (value === "light" || value === "system") {
        return value;
    }

    return "dark";
};

contextBridge.exposeInMainWorld("sereneTheme", {
    preference: getInitialThemePreference(),
    setPreference: (theme: ThemePreference) => {
        ipcRenderer.send("theme-preference:set", theme);
    },
});

window.addEventListener("message", (event) => {
    if (event.data === "start-orpc-client") {
        const [serverPort] = event.ports;

        if (!serverPort) {
            console.error("[Preload] No serverPort in event.ports!");
            return;
        }

        ipcRenderer.postMessage("start-orpc-server", null, [serverPort]);
    }
});
