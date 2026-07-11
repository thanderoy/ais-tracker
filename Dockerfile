# syntax=docker/dockerfile:1

# Multi-stage build producing a small static image: the tracker binary embeds
# the dashboard assets (web/) via go:embed, so the final image carries no
# frontend files and needs no libc.

FROM golang:1.26 AS build
WORKDIR /src

# Cache modules separately from source.
COPY go.mod go.sum ./
RUN go mod download

COPY . .

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
