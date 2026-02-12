FROM docker.io/golang:1.26.0-alpine3.23 AS build
WORKDIR /usr/local/src
COPY . .
RUN go build .

FROM docker.io/alpine:3.23
COPY --from=build /usr/local/src/mlb /
ENTRYPOINT [ "/mlb" ]

