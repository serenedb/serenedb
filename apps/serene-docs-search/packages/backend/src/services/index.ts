import { AskService } from "./ask";
import { ModelsService } from "./models";
import { ParsingService } from "./parsing";
import { RankingService } from "./ranking";
import { SchedulerService } from "./scheduler";
import { SearchService } from "./search";
import { SourcesService } from "./sources";
import { SpellingService } from "./spelling";

export { Indexer } from "./indexing";

export const Services = {
    search: SearchService,
    ask: AskService,
    spelling: SpellingService,
    ranking: RankingService,
    sources: SourcesService,
    parsing: ParsingService,
    models: ModelsService,
    scheduler: SchedulerService,
};
