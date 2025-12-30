let workerPath: string = "";

export const setWorkerPath = (newPath: string) => {
    workerPath = newPath;
};

export const getWorkerPath = () => {
    if (!workerPath) {
        throw new Error(
            "Worker path not set. Call setWorkerPath() before using workers.",
        );
    }
    return workerPath;
};
