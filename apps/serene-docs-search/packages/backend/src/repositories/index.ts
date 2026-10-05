import { AnalyticsRepository } from "./analytics";
import { EmbeddingRepository } from "./embedding";
import { MetaRepository } from "./meta";
import { ObjectsRepository } from "./objects";
import { PagesRepository } from "./pages";
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
    objects: ObjectsRepository,
    pages: PagesRepository,
};
