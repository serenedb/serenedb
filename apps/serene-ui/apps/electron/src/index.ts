import path from "path";
import {
    app,
    BrowserWindow,
    ipcMain,
    nativeImage,
    nativeTheme,
} from "electron";
import { initDatabase } from "@serene-ui/shared-backend";
import { initLogger, setWorkerPath } from "@serene-ui/shared-backend";
import fs from "fs";
import { RPCHandler } from "@orpc/server/message-port";
import { apiRouter } from "./routers";
import { onError } from "@orpc/server";
import { updateElectronApp, UpdateSourceType } from "update-electron-app";

declare const MAIN_WINDOW_PRELOAD_WEBPACK_ENTRY: string;
const APP_NAME = "SereneUI";
const APP_UPDATE_HOST = "https://api.serenedb.com/api/updates";
const APP_UPDATE_REPO = "serenedb/serenedb";
const THEME_PREFERENCE_FILE = "theme-preference.json";
const LIGHT_WINDOW_BACKGROUND = "#f5f5f5";
const DARK_WINDOW_BACKGROUND = "#1c1c1c";

type ThemePreference = "dark" | "light" | "system";

let mainWindow: BrowserWindow | null = null;
let currentThemePreference: ThemePreference = "dark";

if (require("electron-squirrel-startup")) {
    app.quit();
}

updateElectronApp({
    updateSource: {
        type: UpdateSourceType.ElectronPublicUpdateService,
        host: APP_UPDATE_HOST,
        repo: APP_UPDATE_REPO,
    },
});

const isThemePreference = (value: unknown): value is ThemePreference =>
    value === "dark" || value === "light" || value === "system";

const ensureAppDataPaths = () => {
    const appDataPath = app.getPath("appData");
    const userDataPath = path.join(appDataPath, APP_NAME);
    app.setPath("userData", userDataPath);

    const dataPath = path.join(userDataPath, "data");
    const logsPath = path.join(userDataPath, "logs");

    fs.mkdirSync(dataPath, { recursive: true });
    fs.mkdirSync(logsPath, { recursive: true });

    return {
        dataPath,
        logsPath,
    };
};

const getThemePreferencePath = () =>
    path.join(app.getPath("userData"), THEME_PREFERENCE_FILE);

const readThemePreference = (): ThemePreference => {
    try {
        const file = fs.readFileSync(getThemePreferencePath(), "utf8");
        const { theme } = JSON.parse(file) as { theme?: unknown };

        if (isThemePreference(theme)) {
            return theme;
        }
    } catch {
        // Ignore missing or malformed theme preference files.
    }

    return "dark";
};

const writeThemePreference = (theme: ThemePreference) => {
    fs.writeFileSync(
        getThemePreferencePath(),
        JSON.stringify({ theme }),
        "utf8",
    );
};

const resolveTheme = (theme: ThemePreference) => {
    if (theme === "system") {
        return nativeTheme.shouldUseDarkColors ? "dark" : "light";
    }

    return theme;
};

const getWindowBackgroundColor = (theme: ThemePreference) =>
    resolveTheme(theme) === "dark"
        ? DARK_WINDOW_BACKGROUND
        : LIGHT_WINDOW_BACKGROUND;

const applyThemePreference = (theme: ThemePreference) => {
    currentThemePreference = theme;
    writeThemePreference(theme);
    mainWindow?.setBackgroundColor(getWindowBackgroundColor(theme));
};

const createWindow = (): void => {
    const iconPath = path.join(
        __dirname,
        "assets",
        "icons",
        "icon_256x256.png",
    );
    mainWindow = new BrowserWindow({
        backgroundColor: getWindowBackgroundColor(currentThemePreference),
        frame: false,
        show: false,
        titleBarStyle: "hidden",
        transparent: true,
        trafficLightPosition: { x: -100, y: -100 },
        width: 1280,
        height: 720,
        minWidth: 700,
        icon: iconPath,
        webPreferences: {
            additionalArguments: [
                `--theme-preference=${currentThemePreference}`,
            ],
            preload: MAIN_WINDOW_PRELOAD_WEBPACK_ENTRY,
            sandbox: false,
        },
    });

    if (process.platform === "darwin") {
        const img = nativeImage.createFromPath(iconPath);
        if (!img.isEmpty()) app.dock?.setIcon(img);
    }

    mainWindow.once("ready-to-show", () => {
        mainWindow?.show();
    });

    mainWindow.on("closed", () => {
        mainWindow = null;
    });

    const indexHtml = path.join(__dirname, "web", "index.html");
    mainWindow.loadFile(indexHtml);
};

const loadBackend = () => {
    const { dataPath, logsPath } = ensureAppDataPaths();
    initLogger(logsPath);

    const dbPath = path.join(dataPath, "db.sqlite");
    const migrationsPath = path.join(__dirname, "migrations");

    initDatabase(dbPath, migrationsPath);

    const workerPath = path.join(__dirname, "query-worker.js");

    setWorkerPath(workerPath);

    const handler = new RPCHandler(apiRouter, {
        interceptors: [
            onError((error) => {
                console.error("[ORPC Error]", error);
            }),
        ],
    });

    ipcMain.on("start-orpc-server", async (event) => {
        const [serverPort] = event.ports;
        if (!serverPort) {
            console.error("[Backend] No serverPort received!");
            return;
        }
        handler.upgrade(serverPort);
        serverPort.start();
    });
};

const registerThemeHandlers = () => {
    ipcMain.on("theme-preference:set", (_event, theme: unknown) => {
        if (!isThemePreference(theme)) {
            return;
        }

        applyThemePreference(theme);
    });

    nativeTheme.on("updated", () => {
        if (currentThemePreference !== "system") {
            return;
        }

        mainWindow?.setBackgroundColor(
            getWindowBackgroundColor(currentThemePreference),
        );
    });
};

app.setName(APP_NAME);

app.on("ready", () => {
    loadBackend();
    currentThemePreference = readThemePreference();
    registerThemeHandlers();
    createWindow();
});

app.on("window-all-closed", () => {
    if (process.platform !== "darwin") {
        app.quit();
    }
});

app.on("activate", () => {
    if (BrowserWindow.getAllWindows().length === 0) {
        createWindow();
    }
});
