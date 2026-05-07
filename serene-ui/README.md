<picture align=left>
    <source media="(prefers-color-scheme: dark)" srcset="https://github.com/user-attachments/assets/b5fa1de1-93da-41d9-97f1-b4069f3f6533">
    <source media="(prefers-color-scheme: light)" srcset="https://github.com/user-attachments/assets/6a8a990b-e167-47b1-b28c-cc59786fb364">
    <img alt="serenedb+sereneui" src="https://github.com/user-attachments/assets/b5fa1de1-93da-41d9-97f1-b4069f3f6533" />
</picture>

[![Star Us](https://img.shields.io/badge/Star%20Us-9865e8?style=for-the-badge&logo=github&logoColor=white)](https://github.com/serenedb/serenedb)
[![Apache License 2.0](https://img.shields.io/badge/License-Apache%202.0-a2b9f4?style=for-the-badge)](LICENSE)
[![Website](https://img.shields.io/website?up_message=VISIT&down_message=FIXING&color=fbe5f5&url=https%3A%2F%2Fwww.serenedb.com&style=for-the-badge)](https://www.serenedb.com)

</div>

[![Watch the video](https://github.com/user-attachments/assets/7f89b1f9-b723-4df8-8cb0-5b4b2cbc7873)](https://youtu.be/mVaudH7w8yw)

SereneUI is an open-source database client built for [SereneDB](https://github.com/serenedb/serenedb) and compatible with PostgreSQL. It gives transactional and analytical workflows a single workspace: connect to Postgres and SereneDB, write queries, inspect results, manage saved work, build dashboards and move between environments without switching tools.

SereneDB is designed for search-analytics over constantly changing data while keeping PostgreSQL compatibility. SereneUI brings that idea to the interface layer: a console-first client for the OLTP-to-analytics workflow, with database navigation, query execution, visualization and productivity tools living in one window.

## Why SereneUI

Data work rarely happens in one isolated system. Transactional databases keep changing, analytical stores need to stay in sync and people still need a fast way to explore what is going on.

SereneUI exists to make that workflow feel direct. If you are working with PostgreSQL and SereneDB, you should not need separate clients, separate windows or repeated reconnects. SereneUI keeps connections, databases, queries, results and visualizations together so you can move from raw data to useful insight with less context switching.

## Features

**PostgreSQL and SereneDB in one client.** Connect to existing PostgreSQL databases and SereneDB from the same interface. SereneUI is built around SereneDB-specific workflows while still feeling natural for standard Postgres usage.

**OLTP-to-analytics workflow.** Migrate data from PostgreSQL into SereneDB, keep analytical systems close to transactional sources and access SereneDB capabilities that generic SQL clients do not surface.

**Connection-independent workspace.** Connections and databases live in a single explorer, giving you a constant overview of your environments. A dedicated connection selector lets you run queries against the right connection or database without opening a separate query window for each one.

**Editor-like tab system.** Tabs are independent workspaces with their own query and result state. They can be moved between panes, split vertically or horizontally and organized like a modern IDE, making side-by-side query comparison and parallel exploration comfortable.

**Unified sidebar.** Saved queries, entities and frequently used items are organized in one structured sidebar. Collapsible sections preserve context, pinned items keep important work nearby and the active connection is always visible.

**Command system.** Press `Cmd/Ctrl + J` to open the command finder and navigate connections, databases, pages and core actions from the keyboard. The interface is built to minimize clicks without hiding functionality.

**Query history and saved queries.** SereneUI keeps useful work close to the editor. Saved queries and query history are available through the workflow and feed directly into autocompletion.

**Context-aware autocompletion.** Suggestions adapt to the query context, offering tables where tables are expected and columns where columns are expected. Inline suggestions can also surface matching snippets from query history and saved queries as you type.

**Interactive results.** Query results can be explored as tables or JSON. You can sort data, select values, copy results, download CSV and work with returned data directly from the results panel.

**Dashboards.** Turn query output into visual dashboards without exporting data to another tool. Choose the columns you care about, select a chart template, combine data from multiple databases and keep dashboards fresh with manual or automatic refresh.

**Light and dark themes.** The UI has been reworked for long sessions across different lighting conditions, with a comfortable light theme and an improved dark theme.

**Desktop and Docker distribution.** SereneUI can be used as a desktop client or run through Docker, making it easy to start locally, in development environments or alongside SereneDB.

## Installation

### Desktop App

Desktop builds are available from the [SereneDB releases page](https://github.com/serenedb/serenedb/releases).

### Docker

```bash
docker pull serenedb/serene-ui
docker run -p 6543:6543 serenedb/serene-ui
```

Open `http://localhost:6543` after the container starts.

### Docker Compose

```bash
git clone https://github.com/serenedb/serenedb.git
cd serenedb/serene-ui
docker compose up -d
```

## Development

```bash
git clone https://github.com/serenedb/serenedb.git
cd serenedb/serene-ui
npm install
npm run dev
```

The project is organized as an npm workspace:

- `apps/web` contains the React web client.
- `apps/backend` contains the backend used by the Docker distribution.
- `apps/electron` contains the desktop client.
- `packages/shared-core` contains shared types and core logic.
- `packages/shared-frontend` contains reusable frontend code.
- `packages/shared-backend` contains reusable backend code.

Build all packages with:

```bash
npm run build
```

## Architecture

SereneUI is split into shared packages and app-specific entry points. The web UI provides the query editor, explorer, dashboards, command palette and result views. The backend handles database-facing functionality for the Docker version. The Electron app packages the same product experience as a desktop client.

This structure keeps the interface consistent across Docker and desktop builds while allowing platform-specific code to live at the edges.

## Contributing

SereneUI is open source and shaped by the people using it. Bug reports, feature ideas, pull requests and workflow feedback are welcome.

- [Open an issue](https://github.com/serenedb/serenedb/issues)
- [View releases](https://github.com/serenedb/serenedb/releases)
- [Visit SereneDB](https://www.serenedb.com)

## License

Apache 2.0. See [LICENSE](LICENSE).
