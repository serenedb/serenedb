# SereneDocsSearch sync backend
# Build context: the serene-docs-search repo root
#   docker build -f packages/backend/Dockerfile -t serenedb/docs-search-backend:latest .
FROM node:22-alpine AS build
WORKDIR /app
COPY package.json package-lock.json* ./
COPY packages/core/package.json packages/core/
COPY packages/backend/package.json packages/backend/
RUN npm ci --workspaces --include-workspace-root --ignore-scripts || npm install --ignore-scripts
COPY tsconfig.base.json ./
COPY packages/core packages/core
COPY packages/backend packages/backend
RUN npm run build -w @serenedb/docs-search-core -w @serenedb/docs-search-backend

FROM node:22-alpine
RUN apk add --no-cache git
WORKDIR /app
ENV NODE_ENV=production
COPY packages/backend/package.json ./package.json
# drop dev deps (incl. the workspace-only core package, which is bundled into dist)
RUN node -e "const p=require('./package.json');delete p.devDependencies;require('fs').writeFileSync('package.json',JSON.stringify(p,null,2))" \
    && npm install --omit=dev --ignore-scripts && npm cache clean --force
COPY --from=build /app/packages/backend/dist ./dist
VOLUME /var/lib/serene-search
EXPOSE 7700
CMD ["node", "dist/index.js"]
