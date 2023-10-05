import {strict as assert} from 'assert'
import {
  buildMeta,
  getSimpleColumnSqlType,
  IExtendedJSONSchema7,
  ISourceMeta,
  JsonSchemaInspectorContext,
  PKType,
} from "../src/jsonSchemaInspector"
import {uselessValueExtractor} from "./helpers"
import {SchemaKeyProperties} from "singer-node"

const simpleSchema: IExtendedJSONSchema7 = {
  "properties": {
    "author_id": {"type": ["null", "string"]},
    "id": {"type": ["null", "integer"]},
    "created_at": {"format": "date-time", "type": ["string"]},
    "ticket_id": {"type": ["null", "integer"]},
  }, "type": ["null", "object"],
}

const nestedObjectSchema: IExtendedJSONSchema7 = {
  "properties": {
    "id": {"type": ["null", "integer"]},
    "nested": {"type": ["null", "object"], properties: {"color": {type: "string"}}},
  }, "type": ["null", "object"],
}

const arrayScalarSchema: IExtendedJSONSchema7 = {
  "properties": {
    "collaborator_ids": {
      "items": {
        "type": [
          "null",
          "integer",
        ],
      },
      "type": [
        "null",
        "array",
      ],
    },
    "id": {"type": ["null", "integer"]},
  }, "type": ["null", "object"],
}

const arrayObjectSchema: IExtendedJSONSchema7 = {
  "properties": {
    "custom_fields": {
      "items": {
        "properties": {
          "field": {
            "type": [
              "null",
              "integer",
            ],
          },
          "value": {},
        },
        "type": [
          "null",
          "object",
        ],
      },
      "type": [
        "null",
        "array",
      ],
    },
    "id": {"type": ["null", "integer"]},
  }, "type": ["null", "object"],
}

const nestedObjectWithArraysSchema: IExtendedJSONSchema7 = {
  "properties": {
    "id": {"type": ["null", "integer"]},
    "nested": {
      "type": ["null", "object"],
      properties: {
        "color": {type: "string"},
        "tags": {
          type: "array",
          items: {
            properties: {
              "value": {
                type: "integer",
              },
            },
            type: "object",
          },
        },
      },
    },
  }, "type": ["null", "object"],
}

const deepNestedArrayObjectSchema: IExtendedJSONSchema7 = {
  "properties": {
    "bill_fields": {
      "items": {
        "properties": {
          bill_id: {
            type: "number",
          },
          "john_fields": {
            "type": "array",
            items: {
              properties: {
                "jack_fields": {
                  type: "array",
                  items: {
                    properties: {
                      "jack_value": {
                        type: "number",
                      },
                    },
                    type: "object",
                  },
                },
                john_id: {
                  type: "number",
                },
                "name": {
                  type: "string",
                },
              },
              type: "object",
            },
          },
        },
        type: "object",
      },
      "type": "array",
    },
    "id": {
      "type": "integer",
    },
  },
  "type": "object",
}

const nestedValueArraySchema: IExtendedJSONSchema7 = {
  "type": [
    "null",
    "object",
  ],
  "properties": {
    "events": {
      "type": [
        "null",
        "array",
      ],
      "items": {
        "type": [
          "null",
          "object",
        ],
        "properties": {
          "previous_value": {
            "type": [
              "null",
              "array",
              "string",
            ],
            "items": {
              "type": [
                "null",
                "string",
              ],
            },
          },
        },
      },
    },
  },
}

describe("getSimpleColumnSqlType", () => {
  it("should handle simple stream", () => {
    const res = getSimpleColumnSqlType(new JsonSchemaInspectorContext("audits", simpleSchema, []),
      {"type": ["null", "integer"]})
    assert.equal(res, "Int64")
  })
})

describe("JSON Schema Inspector", () => {
  it("should handle simple schema", () => {
    const res = buildMeta(new JsonSchemaInspectorContext("audits", simpleSchema, ["id"]))
    assert.equal(res.sqlTableName, "`audits`")
    assert.equal(res.pkMappings.length, 1)
    assert.equal(res.pkMappings[0]?.chType, "Int64")
    assert.equal(res.simpleColumnMappings.length, 3)
    assert.equal(res.simpleColumnMappings.find((elem) => elem.prop === "created_at")?.nullable, false)
  })

  it("should handle array scalar", () => {
    const res = buildMeta(new JsonSchemaInspectorContext("audits", arrayScalarSchema, ["id"]))
    assert.equal(res.children.length, 1)
    assert.equal(res.children[0]?.sqlTableName, "`audits__collaborator_ids`")
    assert.equal(res.children[0]?.pkMappings.length, 2)
    assert.equal(res.children[0]?.pkMappings[1]?.prop, "_level_0_index")
    assert.equal(res.children[0]?.pkMappings[1]?.chType, "Int32")
    assert.equal(res.children[0]?.pkMappings[1]?.nullable, false)
    assert.equal(res.children[0]?.pkMappings[0]?.prop, "id")
    assert.equal(res.children[0]?.pkMappings[0]?.chType, "Int64")
  })

  it("should handle nested object", () => {
    const res = buildMeta(new JsonSchemaInspectorContext("audits", nestedObjectSchema, ["id"]))
    assert.equal(res.children.length, 0)
    assert.equal(res.simpleColumnMappings.length, 1)
    assert.equal(res.pkMappings.length, 1)
    assert.equal(res.simpleColumnMappings[0]?.sqlIdentifier, "`nested__color`")
    assert.equal(res.simpleColumnMappings[0]?.chType, "String")
  })

  it("should handle array of nested object", () => {
    const res = buildMeta(new JsonSchemaInspectorContext("audits", arrayObjectSchema, ["id"]))
    assert.equal(res.children[0]?.sqlTableName, "`audits__custom_fields`")
    assert.equal(res.children[0]?.simpleColumnMappings.length, 1)
    assert.equal(res.children[0]?.simpleColumnMappings[0]?.sqlIdentifier, "`field`")
    assert.equal(res.children[0]?.pkMappings.length, 2)
    assert.equal(res.children[0]?.pkMappings[1]?.sqlIdentifier, "`_level_0_index`")
    assert.equal(res.children[0]?.pkMappings[0]?.sqlIdentifier, "`_root_id`")
  })

  it("should handle array of nested object with specifying childrenPK", () => {
    const res = buildMeta(new JsonSchemaInspectorContext("audits", arrayObjectSchema, ["id"], undefined, undefined, undefined, undefined, undefined,
      {
        props: ["id"],
        children: {
          "custom_fields": {
            props: [],
            children: {},
          },
        },
      }))
    assert.equal(res.children[0]?.sqlTableName, "`audits__custom_fields`")
    assert.equal(res.children[0]?.simpleColumnMappings.length, 1)
    assert.equal(res.children[0]?.simpleColumnMappings[0]?.sqlIdentifier, "`field`")
    assert.equal(res.children[0]?.pkMappings.length, 3)
    assert.equal(res.children[0]?.pkMappings[0]?.sqlIdentifier, "`_root_id`")
    assert.equal(res.children[0]?.pkMappings[1]?.sqlIdentifier, "`_parent_id`")
    assert.equal(res.children[0]?.pkMappings[2]?.sqlIdentifier, "`_level_0_index`")
  })

  it("should handle deep nested array of nested object with specifying childrenPK", () => {
    const all_key_properties: SchemaKeyProperties = {
      props: ["id"],
      children: {
        bill_fields: {
          props: ["bill_id"],
          children: {
            john_fields: {
              props: ["john_id"],
              children: {},
            },
          },
        },
      },
    }

    const res = buildMeta(new JsonSchemaInspectorContext("audits", deepNestedArrayObjectSchema, ["id"], undefined, undefined, undefined, undefined, undefined, all_key_properties,
    ))
    assert.equal(res.children[0]?.sqlTableName, "`audits__bill_fields`")
    assert.equal(res.children[0]?.pkMappings[0]?.sqlIdentifier, "`_root_id`")
    assert.equal(res.children[0]?.pkMappings[1]?.sqlIdentifier, "`_parent_id`")
    assert.equal(res.children[0]?.pkMappings[2]?.sqlIdentifier, "`bill_id`")
    assert.equal(res.children[0]?.pkMappings[3]?.sqlIdentifier, "`_level_0_index`")

    assert.equal(res.children[0]?.children[0]?.sqlTableName, "`audits__bill_fields__john_fields`")
    assert.equal(res.children[0]?.children[0]?.pkMappings[0]?.sqlIdentifier, "`_root_id`")
    assert.equal(res.children[0]?.children[0]?.pkMappings[1]?.sqlIdentifier, "`_parent_bill_id`")
    assert.equal(res.children[0]?.children[0]?.pkMappings[2]?.sqlIdentifier, "`john_id`")
    assert.equal(res.children[0]?.children[0]?.pkMappings[3]?.sqlIdentifier, "`_level_0_index`")
    assert.equal(res.children[0]?.children[0]?.pkMappings[4]?.sqlIdentifier, "`_level_1_index`")

    // PK should not be in simple columns
    assert.equal(res.children[0]?.children[0]?.simpleColumnMappings.find((col) => col.prop === 'john_id'), undefined)
    assert.notEqual(res.children[0]?.children[0]?.simpleColumnMappings.find((col) => col.prop === 'name'), undefined)

    assert.equal(res.children[0]?.children[0]?.children[0]?.sqlTableName, "`audits__bill_fields__john_fields__jack_fields`")
    assert.equal(res.children[0]?.children[0]?.children[0]?.pkMappings[0]?.sqlIdentifier, "`_root_id`")
    assert.equal(res.children[0]?.children[0]?.children[0]?.pkMappings[1]?.sqlIdentifier, "`_parent_john_id`")
    assert.equal(res.children[0]?.children[0]?.children[0]?.pkMappings[2]?.sqlIdentifier, "`_level_0_index`")
    assert.equal(res.children[0]?.children[0]?.children[0]?.pkMappings[3]?.sqlIdentifier, "`_level_1_index`")
    assert.equal(res.children[0]?.children[0]?.children[0]?.pkMappings[4]?.sqlIdentifier, "`_level_2_index`")
  })

  it("should handle nest object with arrays", () => {
    const res = buildMeta(new JsonSchemaInspectorContext("audits", nestedObjectWithArraysSchema, ["id"]))
    assert.equal(res.children.length, 1)
    assert.equal(res.children[0]?.sqlTableName, "`audits__nested__tags`")
    assert.equal(res.children[0]?.simpleColumnMappings.length, 1)
    assert.equal(res.pkMappings[0].valueExtractor({id: 3}), 3)
    assert.equal(res.simpleColumnMappings[0].valueExtractor({nested: {color: "blue"}}), "blue")
    assert.equal(res.children[0].simpleColumnMappings[0].valueExtractor({value: 10}), 10)
  })

  it("should handle nested value array schema", () => {
    const res = buildMeta(new JsonSchemaInspectorContext("audits", nestedValueArraySchema, []))
    const expectedResult: ISourceMeta = {
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
              "nullable": false,
              "lowCardinality": false,
              "pkType": PKType.LEVEL,
              valueExtractor: uselessValueExtractor,
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
                  "nullable": false,
                  "lowCardinality": false,
                  "pkType": PKType.LEVEL,
                  valueExtractor: uselessValueExtractor,

                },
                {
                  "prop": "_level_1_index",
                  "sqlIdentifier": "`_level_1_index`",
                  "chType": "Int32",
                  "nullable": false,
                  "lowCardinality": false,
                  "pkType": PKType.LEVEL,
                  valueExtractor: uselessValueExtractor,
                },
              ],
              "simpleColumnMappings": [
                {
                  "sqlIdentifier": "`value`",
                  "chType": "String",
                  "nullable": false,
                  "lowCardinality": false,
                  valueExtractor: uselessValueExtractor,
                },
              ],
              "children": [],
            },
          ],
        },
      ],
    }
    assert.equal(JSON.stringify(res), JSON.stringify(expectedResult))
    assert.equal(res.children[0].children[0].simpleColumnMappings[0].valueExtractor("tartempion"), "tartempion")
  })
})
