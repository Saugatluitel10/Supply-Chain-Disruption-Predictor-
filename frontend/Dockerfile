# syntax=docker/dockerfile:1

# --- Build stage ---
FROM node:18-alpine AS build
WORKDIR /app

# Install deps
COPY package*.json ./
RUN npm ci

# Build
COPY . .
RUN npm run build

# --- Runtime stage ---
FROM nginx:1.25-alpine

# Copy built assets
COPY --from=build /app/build /usr/share/nginx/html

# Nginx config with caching and SPA fallback
COPY nginx.conf /etc/nginx/conf.d/default.conf

# Non-root best practice
RUN addgroup -S app && adduser -S app -G app \
  && chown -R app:app /var/cache/nginx /var/run /var/log/nginx /usr/share/nginx/html
USER app

EXPOSE 80

HEALTHCHECK --interval=30s --timeout=3s --start-period=30s --retries=3 CMD wget -qO- http://localhost/ || exit 1

CMD ["nginx", "-g", "daemon off;"]
