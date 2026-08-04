import { AnalyticsRepository } from "./analytics";
import { EmbeddingRepository } from "./embedding";
import { MetaRepository } from "./meta";
import { SchemaRepository } from "./schema";
import { SearchRepository } from "./search";
import { SectionsRepository } from "./sections";
import { VocabRepository } from "./vocab";

export const Repositories = {
    meta: MetaRepository,
    embedding: EmbeddingRepository,
    schema: SchemaRepository,
    sections: SectionsRepository,
    search: SearchRepository,
    analytics: AnalyticsRepository,
    vocab: VocabRepository,
};
