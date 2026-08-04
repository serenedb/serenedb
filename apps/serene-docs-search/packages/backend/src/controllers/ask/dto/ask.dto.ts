import { Type } from "class-transformer";
import { IsArray, IsIn, IsOptional, IsString, MinLength, ValidateNested } from "class-validator";

export class AskHistoryMessageDto {
    @IsIn(["user", "assistant"])
    role: "user" | "assistant";

    @IsString()
    content: string;
}

export class AskDto {
    @IsString()
    @MinLength(1)
    q: string;

    /** Prior conversation exchanges, oldest first (optional). */
    @IsOptional()
    @IsArray()
    @ValidateNested({ each: true })
    @Type(() => AskHistoryMessageDto)
    history?: AskHistoryMessageDto[];
}
