FROM golang:1.22-alpine AS build
WORKDIR /app
COPY go.mod go.sum* ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 go build -o /eventbus ./cmd/server

FROM alpine:3.19
COPY --from=build /eventbus /eventbus
EXPOSE 8080
ENTRYPOINT ["/eventbus"]
