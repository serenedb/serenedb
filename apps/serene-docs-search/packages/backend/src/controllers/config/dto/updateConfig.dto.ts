import { Equals, IsDefined, IsObject, IsOptional, IsString } from "class-validator";

/**
 * Shallow shape check for PUT /v1/config — the nested source/search unions
 * are normalized and validated by normalizeConfig/applyConfig downstream.
 */
export class UpdateConfigDto {
    @Equals(1)
    version: 1;

    @IsOptional()
    @IsString()
    project?: string;

    @IsDefined()
    @IsObject()
    source: object;

    @IsDefined()
    @IsObject()
    content: object;

    @IsDefined()
    @IsObject()
    search: object;

    @IsOptional()
    @IsObject()
    ai?: object;

    @IsOptional()
    @IsObject()
    sync?: object;

    @IsOptional()
    @IsObject()
    server?: object;

    @IsOptional()
    @IsObject()
    serenedb?: object;
}
