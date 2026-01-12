# SereneUI

**One Interface for Your Analytics Data Workflow**
SereneUI is a Postgres-compatible client built for **SereneDB**, letting you work with transactional and analytical databases in one window.

---

## Features
- Connect to SereneDB and PostgreSQL from one interface
- Multi-database query execution with tabbed workflow
- Query history and saved queries with autocompletion
- Interactive results: table/JSON view, sorting, copy/download CSV
- Keyboard-friendly command system for fast navigation

---

## Installation

### Desktop App
Available for **Windows, macOS (soon), and Linux**.

### Docker
```sh
docker pull serenedb/serene-ui
docker run -p 3000:3000 -p 3001:3001 serenedb/serene-ui
```

### Docker Compose
```sh
git clone https://github.com/serenedb/serene-ui.git
cd serene-ui
docker-compose up -d
```

---

### Development
```sh
git clone https://github.com/serenedb/serene-ui.git
cd serene-ui
npm install
npm run dev
```

---

### Contribution
Soon

### License
Apache 2.0
