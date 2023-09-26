import {strict as assert} from "assert"
import RecordProcessor from "../src/RecordProcessor"
import {ColumnMap, ISourceMeta, PkMap, PKType} from "../src/jsonSchemaInspector"
import TargetConnection from "../src/TargetConnection"
import {Writable} from "stream"
import {StringDecoder} from "string_decoder"

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

const metaWithNestedValueArray: ISourceMeta = {
  "prop": "audits",
  "sqlTableName": "`audits`",
  "pkMappings": [],
  "simpleColumnMappings": [],
  "children": [
    {
      "prop": "events",
      "sqlTableName": "`audits__events`",
      "pkMappings": [
        {
          "prop": "_level_0_index",
          "sqlIdentifier": "`_level_0_index`",
          "chType": "Int32",
          valueExtractor: () => {
            throw "should never be called"
          },
          "nullable": false,
          "pkType": PKType.LEVEL,
          lowCardinality: false,
        },
      ],
      "simpleColumnMappings": [],
      "children": [
        {
          "prop": "previous_value",
          "sqlTableName": "`audits__events__previous_value`",
          "pkMappings": [
            {
              "prop": "_level_0_index",
              "sqlIdentifier": "`_level_0_index`",
              "chType": "Int32",
              valueExtractor: () => {
                throw "should never be called"
              },
              "nullable": false,
              "pkType": PKType.LEVEL,
              lowCardinality: false,
            },
            {
              "prop": "_level_1_index",
              "sqlIdentifier": "`_level_1_index`",
              "chType": "Int32",
              valueExtractor: () => {
                throw "should never be called"
              },
              "nullable": false,
              "pkType": PKType.LEVEL,
              lowCardinality: false,
            },
          ],
          "simpleColumnMappings": [
            {
              "sqlIdentifier": "`value`",
              valueExtractor: (data) => data.value,
              "chType": "String",
              "nullable": false,
              lowCardinality: false,
            },
          ],
          "children": [],
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
  stream: StringWritable

  constructor() {
  }

  public createWriteStream(query: string): Writable {
    this.stream = new StringWritable();
    return this.stream
  }
}

describe("RecordProcessor", () => {
  describe("pushRecord", async () => {

    it("should handle simple schema and data", async () => {
      const connection = new TestConnection()
      const res = new RecordProcessor(simpleMeta, connection, 1)
      res.pushRecord({id: 1, name: "a"}, 0)
      res.pushRecord({id: 2, name: "b"}, 0)

      assert.equal(connection.stream.data, '[1,"a"]\n[2,"b"]\n')
      assert.equal(res.buildSQLInsertField()[0], "`id`")
      assert.equal(res.buildSQLInsertField()[1], "`name`")
    }).timeout(30000)

    it("should handle batch size and end ingestion", async () => {
      const connection = new TestConnection()
      const res = new RecordProcessor(simpleMeta, connection, 2)
      res.pushRecord({id: 1, name: "a"}, 0)
      res.pushRecord({id: 2, name: "b"}, 0)
      res.pushRecord({id: 3, name: "c"}, 0)

      assert.equal(connection.stream.data, '[1,"a"]\n[2,"b"]\n')

      await res.endIngestion()

      assert.equal(connection.stream.data, '[1,"a"]\n[2,"b"]\n[3,"c"]\n')
    }).timeout(30000)

    it("should feed deep nested children", async () => {
      const connection = new TestConnection()
      const res = new RecordProcessor(metaWithPKAndChildren, connection, 1)
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
        }, 50,
      )

      assert.deepEqual(connection.stream.data, "[1234,0,0,\"value_a\",51]\n[1234,0,1,\"value_b\",51]\n[1234,0,2,\"value_c\",51]\n[1234,1,0,\"value_d\",51]\n[1234,1,1,\"value_e\",51]\n")

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
      const res = new RecordProcessor(metaWithNestedValueArray, connection, 1)
      res.pushRecord(
        {events: [{previous_value: "Test"}]}, 0,
      )

      assert.equal(connection.stream.data, '[0,0,"Test"]\n')
    }).timeout(30000)

  }).timeout(30000)
})
