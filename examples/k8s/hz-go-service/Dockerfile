# syntax=docker/dockerfile:1

FROM golang:1.17-alpine AS build

WORKDIR /app

COPY . .

ENV GOARCH=amd64

RUN go mod download

RUN go build -o hz-go-service

FROM alpine

COPY --from=build /app/hz-go-service /app/binary

CMD ["/app/binary"]