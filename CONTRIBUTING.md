# Contributing

Feel free to open up issues and pull requests, we'll be happy to review them.

Some guidelines:

## Immutable

This library is built without as little mutable data as possible, using [immutable-js](https://immutable-js.com/). It should remain so.

## Tests

Code is fully tested using mocha and testcontainers-js, so you don't have to boot your own clickhouse instance to run tests.

Make sure you add tests to your PR and before committing run
```sh
yarn test
```
