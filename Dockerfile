FROM node:22-alpine AS deps
WORKDIR /app
COPY package.json package-lock.json ./
RUN npm ci --ignore-scripts
# ffmpeg-static/ffprobe-static download binaries via postinstall; run explicitly for current arch
RUN npm rebuild ffmpeg-static ffprobe-static

FROM node:22-alpine AS build
WORKDIR /app
COPY --from=deps /app/node_modules ./node_modules
COPY package.json tsconfig.json ./
COPY src ./src
RUN npm run build

FROM node:22-alpine AS prod-deps
WORKDIR /app
COPY package.json package-lock.json ./
RUN npm ci --omit=dev --ignore-scripts
RUN npm rebuild ffmpeg-static ffprobe-static

FROM node:22-alpine
RUN apk add --no-cache tini
WORKDIR /app
ENV NODE_ENV=production
COPY --from=prod-deps /app/node_modules ./node_modules
COPY --from=build /app/dist ./dist
COPY package.json ./
RUN mkdir -p /app/output && chown node:node /app/output
USER node
ENTRYPOINT ["/sbin/tini", "--"]
CMD ["node", "dist/index.js"]
