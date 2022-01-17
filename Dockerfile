FROM node:16.13.1-stretch

COPY package.json .

COPY yarn.lock .

COPY ./singer-node ./singer-node

RUN yarn add ./singer-node

RUN yarn install --only=production

COPY ./src ./src

COPY ./tsconfig.json ./tsconfig.json

RUN yarn build

# improve so config.json isn't forced
ENTRYPOINT ["node", "dist/index.js"]
