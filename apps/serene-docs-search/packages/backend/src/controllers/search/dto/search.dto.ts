import { IsIn, IsInt, IsOptional, IsString } from "class-validator";

export class SearchDto {
    @IsOptional()
    @IsString()
    q?: string;

    @IsOptional()
    @IsIn(["fulltext", "hybrid"])
    mode?: "fulltext" | "hybrid";

    /** Clamped to 1..50 by the service — validated for type only. */
    @IsOptional()
    @IsInt()
    limit?: number;
}
