FROM docker.io/golang:1.21-alpine3.18 AS build
WORKDIR /usr/local/src
COPY . .
RUN go build .

FROM docker.io/alpine:3.18
COPY --from=build /usr/local/src/mlb /
ENTRYPOINT [ "/mlb" ]

