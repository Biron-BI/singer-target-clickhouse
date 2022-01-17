FROM node:16.13.1-stretch

COPY package.json .

COPY yarn.lock .

RUN yarn install

COPY ./src ./src

COPY ./tsconfig.json ./tsconfig.json

RUN yarn run build

# improve so config.json isn't forced
ENTRYPOINT ["node", "dist/index.js"]
