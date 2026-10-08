# One image, two binaries: the hub and the load generator.
FROM golang:1.27 AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 go build -trimpath -o /out/hub ./cmd/hub \
 && CGO_ENABLED=0 go build -trimpath -o /out/loadgen ./cmd/loadgen \
 && CGO_ENABLED=0 go build -trimpath -o /out/summarize ./cmd/summarize

FROM gcr.io/distroless/static-debian12:nonroot
COPY --from=build /out/hub /out/loadgen /out/summarize /usr/local/bin/
ENTRYPOINT ["/usr/local/bin/hub"]
