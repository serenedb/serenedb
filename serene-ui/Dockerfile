FROM public.ecr.aws/docker/library/node:22-alpine AS builder
WORKDIR /usr/src

RUN apk add --no-cache python3 make g++

ARG NODE_ENV=production
ENV NODE_ENV=${NODE_ENV}

COPY package*.json ./

COPY tsconfig.base.json ./
COPY apps/backend ./apps/backend
COPY apps/web ./apps/web
COPY packages/shared-core ./packages/shared-core
COPY packages/shared-backend ./packages/shared-backend
COPY packages/shared-frontend ./packages/shared-frontend

RUN npm install -g typescript

RUN rm -f package-lock.json && npm install --include=dev

RUN npm run build

FROM public.ecr.aws/docker/library/node:22-alpine

WORKDIR /app
ENV NODE_ENV=production

RUN apk add --no-cache \
    nginx \
    supervisor

RUN npm install --production --no-save \
    better-sqlite3@12.2.0

COPY apps/web/.nginx/nginx.conf /etc/nginx/nginx.conf
COPY --from=builder /usr/src/apps/backend/dist ./backend/dist
COPY --from=builder /usr/src/apps/web/dist/docker /var/www

WORKDIR /app
COPY ./supervisord.conf /etc/supervisord.conf

EXPOSE 3000 6543
CMD ["/usr/bin/supervisord", "-c", "/etc/supervisord.conf"]
