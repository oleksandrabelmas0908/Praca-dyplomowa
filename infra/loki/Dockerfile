# The Loki image is distroless, with no shell or HTTP client for the healthcheck: a static busybox
# (1.36.1-musl) provides wget. Loki itself (3.7.9) is unchanged
FROM busybox@sha256:3c6ae8008e2c2eedd141725c30b20d9c36b026eb796688f88205845ef17aa213 AS busybox
FROM grafana/loki@sha256:0db216b630ae1f348a1584c67f3cead8b4c1fc839169a39501db664744158e28
COPY --from=busybox /bin/busybox /busybox
