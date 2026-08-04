# SereneDocsSearch MCP server
# Build context: the serene-docs-search repo root
#   docker build -f packages/mcp/Dockerfile -t serenedb/docs-search-mcp:latest .
FROM node:22-alpine AS build
WORKDIR /app
COPY package.json package-lock.json* ./
COPY packages/core/package.json packages/core/
COPY packages/mcp/package.json packages/mcp/
RUN npm ci --workspaces --include-workspace-root --ignore-scripts || npm install --ignore-scripts
COPY tsconfig.base.json ./
COPY packages/core packages/core
COPY packages/mcp packages/mcp
RUN npm run build -w @serenedb/docs-search-core -w @serenedb/docs-search-mcp

FROM node:22-alpine
WORKDIR /app
ENV NODE_ENV=production
COPY packages/mcp/package.json ./package.json
# drop dev deps (incl. the workspace-only core package, which is bundled into dist)
RUN node -e "const p=require('./package.json');delete p.devDependencies;require('fs').writeFileSync('package.json',JSON.stringify(p,null,2))" \
    && npm install --omit=dev --ignore-scripts && npm cache clean --force
COPY --from=build /app/packages/mcp/dist ./dist
# default: streamable HTTP on 7710; override CMD for stdio
EXPOSE 7710
ENTRYPOINT ["node", "dist/index.js"]
CMD ["--http", "7710"]
