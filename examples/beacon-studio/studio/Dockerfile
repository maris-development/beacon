# syntax=docker/dockerfile:1
# Builds Beacon Studio from its Git repository and serves it with nginx.
# The Studio lock file needs npm 11, which comes with Node 24.
FROM node:24-alpine AS build
ARG STUDIO_REF=main
ADD https://github.com/maris-development/beacon-studio.git#${STUDIO_REF} /app
WORKDIR /app
RUN npm ci
ARG STUDIO_BRAND=default
RUN npm run build

FROM nginx:1.27-alpine
COPY nginx.conf /etc/nginx/conf.d/default.conf
COPY --from=build /app/build /usr/share/nginx/html
