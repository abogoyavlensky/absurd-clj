# Habitat is shipped as a release binary; this image just downloads it.
FROM debian:bookworm-slim
ARG HABITAT_VERSION=0.5.0
ARG TARGETARCH
RUN apt-get update \
 && apt-get install -y --no-install-recommends ca-certificates curl \
 && rm -rf /var/lib/apt/lists/*
RUN case "$TARGETARCH" in \
      amd64) ARCH=x86_64 ;; \
      arm64) ARCH=arm64 ;; \
      *) echo "unsupported architecture: $TARGETARCH" && exit 1 ;; \
    esac \
 && curl -fsSL -o /usr/local/bin/habitat \
      "https://github.com/earendil-works/absurd/releases/download/${HABITAT_VERSION}/habitat-linux-${ARCH}" \
 && chmod +x /usr/local/bin/habitat
EXPOSE 7890
ENTRYPOINT ["habitat", "run"]
