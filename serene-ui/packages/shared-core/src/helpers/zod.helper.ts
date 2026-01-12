import z from "zod";

export function mapDiscriminatedUnion<
    Options extends readonly z.ZodObject<any>[],
    Discriminator extends string,
>(
    union: z.ZodDiscriminatedUnion<Options, Discriminator>,
    map: (schema: Options[number]) => z.ZodTypeAny,
) {
    return z.union(union.def.options.map(map) as any);
}
