import { IsOptional, IsString, MinLength } from "class-validator";

export class ReportClickDto {
    @IsString()
    @MinLength(1)
    id: string;

    @IsString()
    @MinLength(1)
    url: string;

    @IsOptional()
    @IsString()
    title?: string;
}
