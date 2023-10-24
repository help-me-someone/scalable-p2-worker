# Build stage

FROM golang:1.18 AS BuildStage

WORKDIR /app

COPY . .

RUN CGO_ENABLED=0 GOOS=linux go build -o app ./...

# Deploy Stage
FROM alpine:3

RUN apk update
RUN apk upgrade
RUN apk add --no-cache ffmpeg

COPY --from=BuildStage /app .

CMD ["./app"]

