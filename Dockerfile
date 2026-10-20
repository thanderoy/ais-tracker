# syntax=docker/dockerfile:1

# Multi-stage build producing a small static image: the tracker binary embeds
# the operator terminal (built from web/ into api/internal/webui/dist) via go:embed, so the final
# image carries no frontend files and needs no libc.

# Pre-render the Qwik terminal to static files.
FROM node:22-slim AS ui
WORKDIR /src/web
COPY web/package.json web/package-lock.json web/.npmrc ./
RUN npm ci
COPY web/ ./
RUN mkdir -p ../api/internal/webui/dist && npm run build

FROM golang:1.26 AS build
WORKDIR /src

# Cache modules separately from source.
COPY api/go.mod api/go.sum ./
RUN go mod download

COPY api/ ./
COPY --from=ui /src/api/internal/webui/dist ./internal/webui/dist

# Static, stripped binaries. CGO is off so the result runs on distroless/static.
ARG VERSION=dev
ENV CGO_ENABLED=0 GOOS=linux
RUN go build -trimpath -ldflags "-s -w -X main.version=${VERSION}" -o /out/tracker ./cmd/tracker \
 && go build -trimpath -ldflags "-s -w" -o /out/migrate ./cmd/migrate

# Distroless static: no shell, no package manager, just the binary and CA certs.
FROM gcr.io/distroless/static-debian12:nonroot
COPY --from=build /out/tracker /usr/local/bin/tracker
COPY --from=build /out/migrate /usr/local/bin/migrate
# Migrations are applied by a one-shot job/command; ship the SQL alongside.
COPY --from=build /src/migrations /migrations

EXPOSE 8080
USER nonroot:nonroot
ENTRYPOINT ["/usr/local/bin/tracker"]
