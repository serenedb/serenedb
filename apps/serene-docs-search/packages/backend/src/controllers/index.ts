import { AnalyticsController } from "./analytics";
import { AskController } from "./ask";
import { ConfigController } from "./config";
import { HealthController } from "./health";
import { SearchController } from "./search";
import { SectionsController } from "./sections";
import { SyncController } from "./sync";

export const Controllers = {
    health: HealthController,
    search: SearchController,
    sections: SectionsController,
    ask: AskController,
    sync: SyncController,
    analytics: AnalyticsController,
    config: ConfigController,
};
