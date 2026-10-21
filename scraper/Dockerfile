FROM --platform=$BUILDPLATFORM golang:1.25-alpine AS build

WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY scraper ./scraper
COPY internal/metricdata ./internal/metricdata
COPY metrics ./metrics
COPY config ./config
RUN go test ./scraper/...
ARG TARGETOS TARGETARCH
RUN CGO_ENABLED=0 GOOS=${TARGETOS} GOARCH=${TARGETARCH} \
    go build -trimpath -o /out/azure-metrics-scraper ./scraper/cmd/metrics-scraper

FROM gcr.io/distroless/static AS final
WORKDIR /app
COPY --from=build /out/azure-metrics-scraper /azure-metrics-scraper
USER 1000:1000
ENTRYPOINT ["/azure-metrics-scraper"]
