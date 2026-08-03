import { IsInt, IsString, Min, MinLength } from "class-validator";

export class ReportQueryDto {
    @IsString()
    @MinLength(1)
    q: string;

    @IsInt()
    @Min(0)
    hits: number;
}
