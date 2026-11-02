import winston from "winston";
import path from "path";
import fs from "fs";

const enumerateErrorFormat = winston.format((info) => {
    if (info instanceof Error) {
        Object.assign(info, { error: `${info.stack}` });
    }
    return info;
})();

const levels: winston.config.AbstractConfigSetLevels = {
    error: 0,
    warn: 1,
    info: 2,
    http: 3,
    debug: 4,
};

const level = (): string => {
    const env = process.env.NODE_ENV || "development";
    const isDevelopment = env === "development";
    return isDevelopment ? "debug" : "warn";
};

const colors: winston.config.AbstractConfigSetColors = {
    error: "red",
    warn: "yellow",
    info: "green",
    http: "magenta",
    debug: "white",
};

winston.addColors(colors);

const format = winston.format.combine(
    enumerateErrorFormat,
    winston.format.timestamp({ format: "YYYY-MM-DD HH:mm:ss:ms" }),
    winston.format.uncolorize({
        all: true,
    } as winston.Logform.UncolorizeOptions),
    winston.format.json(),
);

let loggerInstance: winston.Logger | null = null;

export const initLogger = (logsDir?: string): void => {
    let logDirectory: string;

    if (logsDir) {
        logDirectory = logsDir;
    } else {
        logDirectory = path.resolve(process.cwd(), "logs");
    }

    if (!fs.existsSync(logDirectory)) {
        fs.mkdirSync(logDirectory, { recursive: true });
    }

    const transports: winston.transport[] = [
        new winston.transports.Console({
            format: winston.format.combine(
                enumerateErrorFormat,
                winston.format.timestamp({ format: "YYYY-MM-DD HH:mm:ss:ms" }),
                winston.format.colorize({ all: true }),
                winston.format.printf((info) => {
                    if (info.error) {
                        if (info.message) {
                            return `${info.timestamp} ${info.level}: ${info.message}\n${info.error}`;
                        } else {
                            return `${info.timestamp} ${info.level}:\n${info.error}`;
                        }
                    } else {
                        return `${info.timestamp} ${info.level}: ${info.message}`;
                    }
                }),
            ),
        }),
        new winston.transports.File({
            filename: path.join(logDirectory, "error.log"),
            level: "error",
            format: format,
        }),
        new winston.transports.File({
            filename: path.join(logDirectory, "all.log"),
            format: format,
        }),
    ];

    loggerInstance = winston.createLogger({
        level: level(),
        levels,
        format,
        transports,
    });
};

export const logger: winston.Logger = new Proxy({} as winston.Logger, {
    get(_target, prop) {
        if (!loggerInstance) {
            initLogger();
        }
        return (loggerInstance as any)[prop];
    },
});
