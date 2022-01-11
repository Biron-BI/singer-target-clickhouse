# Target Clickhouse

A [Singer](https://singer.io/) target for Clickhouse, for use with Singer streams generated by Singer taps, written in node js
using [singer-node](https://www.npmjs.com/package/singer-node).

## Usage

### Install

#### As npm package on host

`npm install -g target-clickhouse`

#### Docker image

`docker pull biron-bi/target-clickhouse:latest`

### Run

1. Create a [config file](#configjson) `config.json` with connection information and ingestion parameters.

   ```json
   {
     "host": "localhost",
     "port": 8123,
     "database": "destination_database",
     "username": "user",
     "password": "averysecurepassword"
   }
   ```

2. Run `target-clickhouse` against a [Singer](https://singer.io) tap.

Npm package:

   ```bash
   <tap-anything> --state $(tail -n 1 state.jsonl) | target-clickhouse --config config.json >> state.jsonl
   ```

Docker:

   ```bash
   <tap-anything> --state $(tail -n 1 state.jsonl) | docker run --rm -i -a STDIN -a STDOUT -a STDERR -v "{1}:/config:ro" biron-bi/target-clickhouse >> state.jsonl
   ```

### Config.json

The fields available to be specified in the config file.

#### Mandatory fields

* `host`
* `port`
* `username`
* `password`
* `database`

#### Optional fields

* `max_batch_rows` The maximum number of rows to buffer in memory before writing to the destination table in Clickhouse. Default to `1000`
* `max_batch_size` The maximum number of bytes to buffer in memory before writing to the destination table in Postgres. Default to `1048576`
* `logging_level` Default to `"INFO"`

## Contributing

Feel free to open up issues and pull requests, we'll be happy to review them.

### Immutable

This library is built without any mutable data and should remain so. The library [immutable-js](https://immutable-js.com/) is used.

### Tests

Code is fully tested using mocha and testcontainers-js, so you don't have to boot your own clickhouse instance to run tests.

Simply run
```sh
yarn test
```

## Sponsorship

Target Clickhouse is written and maintained by **Biron** https://birondata.com/

## Acknowledgements

Special thanks to the people who built
* [singer](https://github.com/singer-io/getting-started)
* [target-postgres](https://github.com/datamill-co/target-postgres)
* [immutable-js](https://immutable-js.com/)

## License

Copyright © 2021 Biron

Distributed under the AGPLv3