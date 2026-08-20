FROM docker:27-cli

RUN apk add --no-cache bash curl jq bc coreutils

# docker compose v2 plugin
ARG TARGETARCH
#RUN mkdir -p /usr/local/lib/docker/cli-plugins \
# && curl -x socks5h://127.0.0.1:2080 -fsSL "https://github.com/docker/compose/releases/download/v2.27.0/docker-compose-linux-${TARGETARCH}" \
#    -o /usr/local/lib/docker/cli-plugins/docker-compose \
# && chmod +x /usr/local/lib/docker/cli-plugins/docker-compose
