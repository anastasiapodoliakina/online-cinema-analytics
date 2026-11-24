FROM golang:1.24.2 AS build
ADD https://codeload.github.com/minio/minio/tar.gz/refs/tags/RELEASE.2025-04-22T22-12-26Z /tmp/minio.tar.gz
RUN mkdir /src && tar -xzf /tmp/minio.tar.gz --strip-components=1 -C /src
WORKDIR /src
RUN CGO_ENABLED=0 go build -trimpath -ldflags='-s -w' -o /minio .

FROM python:3.12-slim
COPY --from=build /minio /usr/local/bin/minio
EXPOSE 9000 9001
ENTRYPOINT ["minio"]
