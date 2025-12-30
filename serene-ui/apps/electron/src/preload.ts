import { ipcRenderer } from "electron/renderer";

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
