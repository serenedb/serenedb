import { z } from "zod";

export const HostModeSchema = z.object({
    mode: z.literal("host"),
    host: z.string().min(1).max(255),
    port: z.preprocess(
        (val) => (val === "" ? 5432 : val),
        z.number().max(65535),
    ),
});
export type HostModeSchema = z.infer<typeof HostModeSchema>;

export const SocketModeSchema = z.object({
    mode: z.literal("socket"),
    socket: z.string().min(1).max(255),
    port: z.preprocess(
        (val) => (val === "" ? 5432 : val),
        z.number().max(65535),
    ),
});
export type SocketModeSchema = z.infer<typeof SocketModeSchema>;

export const ConnectionModesSchema = z.discriminatedUnion("mode", [
    HostModeSchema,
    SocketModeSchema,
]);
export type ConnectionModesSchema = z.infer<typeof ConnectionModesSchema>;
