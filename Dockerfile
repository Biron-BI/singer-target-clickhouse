FROM node:16.13.1-stretch

# should be filled by the result of command:  npm pkg get version | sed 's/"//g'
ARG TAG="0.0.0"

# Install from npm registry to ensure both versions are identical
RUN npm install -g target-clickhouse@${TAG}

LABEL org.opencontainers.image.source=https://github.com/biron-bi/singer-target-clickhouse

ENTRYPOINT ["node", "/usr/local/lib/node_modules/target-clickhouse/dist/index.js"]
