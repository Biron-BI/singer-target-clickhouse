import {strict as assert} from "assert"
import RecordProcessor from "../src/RecordProcessor"
import {ColumnMap, ISourceMeta, PkMap, PKType} from "../src/jsonSchemaInspector"
import TargetConnection from "../src/TargetConnection"
import {Writable} from "stream"
import {StringDecoder} from "string_decoder"
import {uselessValueExtractor} from "./helpers"
import {Value} from "../src/SchemaTranslator"

// Represents an id column, for a PK for instance
const id: PkMap = {
  prop: "id",
  sqlIdentifier: "`id`",
  chType: "UInt32",
  valueExtractor: (data) => parseInt(data.id),
  nullable: false,
  pkType: PKType.CURRENT,
  lowCardinality: false,
}

// Represents an id column, for a PK for instance
const rootId: PkMap = {
  prop: "_root_id",
  sqlIdentifier: "`_root_id`",
  chType: "UInt32",
  valueExtractor: (data) => parseInt(data._root_id),
  nullable: false,
  pkType: PKType.ROOT,
  lowCardinality: false,
}

// Represents a name column, as a simple column
const name: ColumnMap = {
  nullable: true,
  prop: "name",
  sqlIdentifier: "`name`",
  chType: "String",
  valueExtractor: (data) => data.name,
  lowCardinality: false,
}

// Represents a name column, as a simple column
const valid: ColumnMap = {
  nullable: false,
  prop: "valid",
  sqlIdentifier: "`valid`",
  chType: "UInt8",
  valueExtractor: (data) => data.valid,
  valueTranslator: (v: Value) => {
    if (v === "true" || v === true || v === 1) {
      return 1
    } else {
      return 0
    }
  },
  lowCardinality: false,
}

const simpleMeta: ISourceMeta = {
  pkMappings: [],
  simpleColumnMappings: [id, name],
  children: [],
  sqlTableName: "`order`",
  prop: "order",
}

const levelColumn = (lvl: number): PkMap => ({
  prop: `_level_${lvl}_index`,
  sqlIdentifier: `\`_level_${lvl}_index\``,
  chType: "UInt32",
  valueExtractor: () => {
    throw "should never be called"
  },
  nullable: false,
  pkType: PKType.LEVEL,
  lowCardinality: false,
})

const metaWithPKAndChildren: ISourceMeta = {
  prop: "order",
  pkMappings: [id],
  simpleColumnMappings: [name],
  children: [{
    simpleColumnMappings: [name],
    pkMappings: [
      rootId, levelColumn(0),
    ],
    sqlTableName: "`order__tags`",
    prop: "tags",
    children: [{
      prop: "values",
      sqlTableName: "`order__tags__values`",
      pkMappings: [rootId, levelColumn(0), levelColumn(1)],
      simpleColumnMappings: [name],
      children: [],
    }],
  }],
  sqlTableName: "`order`",
}

const abort = (err: Error) => {
  throw err
}

const metaWithNestedValueArray: ISourceMeta = {
  prop: "audits",
  sqlTableName: "`audits`",
  pkMappings: [],
  simpleColumnMappings: [],
  children: [
    {
      prop: "events",
      sqlTableName: "`audits__events`",
      pkMappings: [
        {
          prop: "_level_0_index",
          sqlIdentifier: "`_level_0_index`",
          chType: "Int32",
          valueExtractor: uselessValueExtractor,
          nullable: false,
          pkType: PKType.LEVEL,
          lowCardinality: false,
        },
      ],
      simpleColumnMappings: [],
      children: [
        {
          prop: "previous_value",
          sqlTableName: "`audits__events__previous_value`",
          pkMappings: [
            {
              prop: "_level_0_index",
              sqlIdentifier: "`_level_0_index`",
              chType: "Int32",
              valueExtractor: uselessValueExtractor,
              nullable: false,
              pkType: PKType.LEVEL,
              lowCardinality: false,
            },
            {
              prop: "_level_1_index",
              sqlIdentifier: "`_level_1_index`",
              chType: "Int32",
              valueExtractor: uselessValueExtractor,
              nullable: false,
              pkType: PKType.LEVEL,
              lowCardinality: false,
            },
          ],
          simpleColumnMappings: [
            {
              sqlIdentifier: "`value`",
              valueExtractor: (data) => data,
              chType: "String",
              nullable: false,
              lowCardinality: false,
            },
          ],
          children: [],
        },
      ],
    },
  ],
}

class StringWritable extends Writable {
  public data: string
  _decoder: StringDecoder

  constructor() {
    super();
    this._decoder = new StringDecoder();
    this.data = '';
  }

  _write(chunk: any, encoding: any, callback: any) {
    if (encoding === 'buffer') {
      chunk = this._decoder.write(chunk);
    }
    this.data += chunk;
    callback();
  }

  _final(callback: any) {
    this.data += this._decoder.end();
    callback();
  }
}

class TestConnection implements TargetConnection {
  streams: StringWritable[] = []

  constructor() {
  }

  public createWriteStream(query: string): Writable {
    const writable = new StringWritable()
    this.streams.push(writable)
    return writable;
  }

  runQuery(query: string, retries: number): Promise<any> {
    return Promise.resolve(undefined)
  }
}

describe("RecordProcessor", () => {
  describe("pushRecord", async () => {

    it("should handle simple schema and data", async () => {
      const connection = new TestConnection()
      const res = new RecordProcessor(simpleMeta, connection, {
        batchSize: 1,
        translateValues: false,
      })
      res.pushRecord({id: 1, name: "a"}, abort, 0)
      res.pushRecord({id: 2, name: "b"}, abort, 0)

      assert.equal(connection.streams[0].data, '[1,"a"]\n[2,"b"]\n')
      assert.equal(res.buildSQLInsertField()[0], "`id`")
      assert.equal(res.buildSQLInsertField()[1], "`name`")
    }).timeout(30000)

    it("should handle batch size and end ingestion", async () => {
      const connection = new TestConnection()
      const res = new RecordProcessor(simpleMeta, connection, {
        batchSize: 2,
        translateValues: false,
      })
      res.pushRecord({id: 1, name: "a"}, abort, 0)
      res.pushRecord({id: 2, name: "b"}, abort, 0)
      res.pushRecord({id: 3, name: "c"}, abort, 0)

      assert.equal(connection.streams[0].data, '[1,"a"]\n[2,"b"]\n')

      await res.endIngestion()

      assert.equal(connection.streams[0].data, '[1,"a"]\n[2,"b"]\n[3,"c"]\n')
    }).timeout(30000)

    it("should handle value translation", async () => {
      const connection = new TestConnection()
      const res = new RecordProcessor({
        ...simpleMeta,
        simpleColumnMappings: [id, valid],
      }, connection, {
        batchSize: 1,
        translateValues: true,
      })
      res.pushRecord({id: 1, valid: "true"}, abort, 0)

      assert.equal(connection.streams[0].data, '[1,1]\n')
    }).timeout(30000)

    it("should not translate", async () => {
      const connection = new TestConnection()
      const res = new RecordProcessor({
        ...simpleMeta,
        simpleColumnMappings: [id, valid],
      }, connection, {
        batchSize: 1,
        translateValues: false,
      })
      res.pushRecord({id: 1, valid: "true"}, abort, 0)

      assert.equal(connection.streams[0].data, '[1,"true"]\n')
    }).timeout(30000)


    it("should feed deep nested children", async () => {
      const connection = new TestConnection()
      const res = new RecordProcessor(metaWithPKAndChildren, connection, {
        batchSize: 1,
        translateValues: false,
      })
      res.pushRecord(
        {
          id: 1234,
          name: "a", tags: [{
            name: "tag_a", values: [{
              name: "value_a",
            }, {
              name: "value_b",
            }, {
              name: "value_c",
            }],
          }, {
            name: "tag_b", values: [{
              name: "value_d",
            }, {
              name: "value_e",
            }],
          }],
        }, abort, 50,
      )

      assert.deepEqual(connection.streams[0].data, "[1234,\"a\",51]\n")
      assert.deepEqual(connection.streams[1].data, "[1234,0,\"tag_a\",51]\n[1234,1,\"tag_b\",51]\n")
      assert.deepEqual(connection.streams[2].data, "[1234,0,0,\"value_a\",51]\n[1234,0,1,\"value_b\",51]\n[1234,0,2,\"value_c\",51]\n[1234,1,0,\"value_d\",51]\n[1234,1,1,\"value_e\",51]\n")

      // @ts-ignore
      assert.deepEqual(res.children["`order__tags`"]?.children["`order__tags__values`"].buildSQLInsertField(), [
        "`_root_id`",
        "`_level_0_index`",
        "`_level_1_index`",
        "`name`",
        "`_root_ver`",
      ])
    })
      .timeout(30000)

    it("should handle nested value array", async () => {
      const connection = new TestConnection()
      const res = new RecordProcessor(metaWithNestedValueArray, connection, {
        batchSize: 1,
        translateValues: false,
      })
      res.pushRecord(
        {events: [{previous_value: "Test"}]}, abort, 0,
      )
      await res.endIngestion()

      assert.equal(connection.streams[0].data, '[]\n')
      assert.equal(connection.streams[1].data, '[0]\n')
      assert.equal(connection.streams[2].data, '[0,0,"Test"]\n')

    }).timeout(30000)

  }).timeout(30000)
})
