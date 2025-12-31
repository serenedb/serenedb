import path from "path";
import { app, BrowserWindow, ipcMain, nativeImage } from "electron";
import { initDatabase } from "@serene-ui/shared-backend";
import { initLogger, setWorkerPath } from "@serene-ui/shared-backend";
import fs from "fs";
import { RPCHandler } from "@orpc/server/message-port";
import { apiRouter } from "./routers";
import { onError } from "@orpc/server";

declare const MAIN_WINDOW_PRELOAD_WEBPACK_ENTRY: string;

if (require("electron-squirrel-startup")) {
    app.quit();
}

const createWindow = (): void => {
    const iconPath = path.join(
        __dirname,
        "assets",
        "icons",
        "icon_256x256.png",
    );
    const mainWindow = new BrowserWindow({
        backgroundColor: "#10121C",
        frame: false,
        titleBarStyle: "hidden",
        transparent: true,
        trafficLightPosition: { x: -100, y: -100 },
        width: 1280,
        height: 720,
        minWidth: 700,
        icon: iconPath,
        webPreferences: {
            preload: MAIN_WINDOW_PRELOAD_WEBPACK_ENTRY,
            sandbox: false,
        },
    });

    if (process.platform === "darwin") {
        const img = nativeImage.createFromPath(iconPath);
        if (!img.isEmpty()) app.dock?.setIcon(img);
    }

    const indexHtml = path.join(__dirname, "web", "index.html");
    mainWindow.loadFile(indexHtml);
};

const loadBackend = () => {
    const userDataPath = app.getPath("userData");
    if (!fs.existsSync(userDataPath)) {
        fs.mkdirSync(userDataPath, { recursive: true });
    }

    const logsPath = path.join(userDataPath, "logs");
    initLogger(logsPath);

    const dbPath = path.join(userDataPath, "db.sqlite");
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

app.on("ready", () => {
    loadBackend();
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
