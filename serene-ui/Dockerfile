# ============================================================
# Stage 1: Build
# ============================================================
FROM public.ecr.aws/docker/library/node:22-alpine AS builder

WORKDIR /usr/src

ARG NODE_ENV=production
ENV NODE_ENV=${NODE_ENV}

RUN apk add --no-cache python3 make g++

COPY package*.json ./
COPY apps/backend/package*.json ./apps/backend/
COPY apps/web/package*.json ./apps/web/
COPY packages/shared-core/package*.json ./packages/shared-core/
COPY packages/shared-backend/package*.json ./packages/shared-backend/
COPY packages/shared-frontend/package*.json ./packages/shared-frontend/

RUN rm -f package-lock.json && npm install --include=dev

COPY tsconfig.base.json ./
COPY apps/backend ./apps/backend
COPY apps/web ./apps/web
COPY packages/shared-core ./packages/shared-core
COPY packages/shared-backend ./packages/shared-backend
COPY packages/shared-frontend ./packages/shared-frontend

RUN npm run build

# ============================================================
# Stage 2: Runtime
# ============================================================
FROM public.ecr.aws/docker/library/node:22-alpine

WORKDIR /app
ENV NODE_ENV=production

RUN apk add --no-cache nginx supervisor && \
  npm install --no-save better-sqlite3@12.2.0 && \
  npm cache clean --force

COPY apps/web/.nginx/nginx.conf /etc/nginx/nginx.conf
COPY supervisord.conf /etc/supervisord.conf
COPY cookbooks ./cookbooks
COPY --from=builder /usr/src/apps/backend/dist ./backend/dist
COPY --from=builder /usr/src/apps/web/dist/docker /var/www

EXPOSE 3000 6543
CMD ["/usr/bin/supervisord", "-c", "/etc/supervisord.conf"]
