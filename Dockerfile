FROM docker.io/golang:1.26-alpine3.23 AS build
WORKDIR /usr/local/src
COPY go.mod go.sum ./
RUN --mount=type=cache,target=/go/pkg/mod \
    go mod download

COPY . .
RUN --mount=type=cache,target=/go/pkg/mod \
    --mount=type=cache,target=/root/.cache/go-build \
    go build .

FROM docker.io/alpine:3.23
COPY --from=build /usr/local/src/mlb /
ENTRYPOINT [ "/mlb" ]

