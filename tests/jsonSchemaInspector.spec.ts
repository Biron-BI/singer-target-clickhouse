import {strict as assert} from 'assert'
import {buildMeta, ChildrenPK, getSimpleColumnSqlType, IExtendedJSONSchema7, JsonSchemaInspectorContext} from "../src/jsonSchemaInspector"
import {List, Map} from "immutable"

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
            type: "number"
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
                        type: "number"
                      }
                    },
                    type: "object"
                  }
                },
                john_id: {
                  type: "number"
                },
              },
              type: "object"
            },
          },
        },
        type: "object"
      },
      "type": "array",
    },
    "id": {
      "type": "integer"
    },
  },
  "type": "object",
}


describe("getSimpleColumnSqlType", () => {
  it("should handle simple stream", () => {
    const res = getSimpleColumnSqlType(new JsonSchemaInspectorContext("audits", simpleSchema, List()),
      {"type": ["null", "integer"]})
    assert.equal(res, "Int64")
  })
})

describe("JSON Schema Inspector", () => {
  it("should handle simple schema", () => {
    const res = buildMeta(new JsonSchemaInspectorContext("audits", simpleSchema, List(["id"])))
    assert.equal(res.sqlTableName, "`audits`")
    assert.equal(res.pkMappings.size, 1)
    assert.equal(res.pkMappings.get(0)?.chType, "Int64")
    assert.equal(res.simpleColumnMappings.size, 3)
    assert.equal(res.simpleColumnMappings.find((elem) => elem.prop === "created_at")?.nullable, false)
  })

  it("should handle array scalar", () => {
    const res = buildMeta(new JsonSchemaInspectorContext("audits", arrayScalarSchema, List(["id"])))
    assert.equal(res.children.size, 1)
    assert.equal(res.children.get(0)?.sqlTableName, "`audits__collaborator_ids`")
    assert.equal(res.children.get(0)?.pkMappings.size, 2)
    assert.equal(res.children.get(0)?.pkMappings.get(1)?.prop, "_level_0_index")
    assert.equal(res.children.get(0)?.pkMappings.get(1)?.chType, "Int32")
    assert.equal(res.children.get(0)?.pkMappings.get(1)?.nullable, false)
    assert.equal(res.children.get(0)?.pkMappings.get(0)?.prop, "id")
    assert.equal(res.children.get(0)?.pkMappings.get(0)?.chType, "Int64")
  })

  it("should handle nested object", () => {
    const res = buildMeta(new JsonSchemaInspectorContext("audits", nestedObjectSchema, List(["id"])))
    assert.equal(res.children.size, 0)
    assert.equal(res.simpleColumnMappings.size, 1)
    assert.equal(res.pkMappings.size, 1)
    assert.equal(res.simpleColumnMappings.get(0)?.sqlIdentifier, "`nested__color`")
    assert.equal(res.simpleColumnMappings.get(0)?.chType, "String")
  })

  it("should handle array of nested object", () => {
    const res = buildMeta(new JsonSchemaInspectorContext("audits", arrayObjectSchema, List(["id"])))
    assert.equal(res.children.get(0)?.sqlTableName, "`audits__custom_fields`")
    assert.equal(res.children.get(0)?.simpleColumnMappings.size, 1)
    assert.equal(res.children.get(0)?.simpleColumnMappings.get(0)?.sqlIdentifier, "`field`")
    assert.equal(res.children.get(0)?.pkMappings.size, 2)
    assert.equal(res.children.get(0)?.pkMappings.get(1)?.sqlIdentifier, "`_level_0_index`")
    assert.equal(res.children.get(0)?.pkMappings.get(0)?.sqlIdentifier, "`_root_id`")
  })

  it("should handle array of nested object with specifying childrenPK", () => {
    const res = buildMeta(new JsonSchemaInspectorContext("audits", arrayObjectSchema, List(["id"]), undefined, undefined, undefined, undefined,
      {
        props: List(["id"]),
        children: Map<string, ChildrenPK>().set("custom_fields", {
          props: List<string>(),
          children: Map()
        })
      }))
    assert.equal(res.children.get(0)?.sqlTableName, "`audits__custom_fields`")
    assert.equal(res.children.get(0)?.simpleColumnMappings.size, 1)
    assert.equal(res.children.get(0)?.simpleColumnMappings.get(0)?.sqlIdentifier, "`field`")
    assert.equal(res.children.get(0)?.pkMappings.size, 3)
    assert.equal(res.children.get(0)?.pkMappings.get(0)?.sqlIdentifier, "`_root_id`")
    assert.equal(res.children.get(0)?.pkMappings.get(1)?.sqlIdentifier, "`_parent_id`")
    assert.equal(res.children.get(0)?.pkMappings.get(2)?.sqlIdentifier, "`_level_0_index`")
  })

  it("should handle deep nested array of nested object with specifying childrenPK", () => {
    const res = buildMeta(new JsonSchemaInspectorContext("audits", deepNestedArrayObjectSchema, List(["id"]), undefined, undefined, undefined, undefined,
      {
        props: List(["id"]),
        children: Map<string, ChildrenPK>().set("bill_fields", {
          props: List(["bill_id"]),
          children: Map<string, ChildrenPK>().set("john_fields", {
            props: List(["john_id"]),
            children: Map()
          })
        })
      }))
    assert.equal(res.children.get(0)?.sqlTableName, "`audits__bill_fields`")
    assert.equal(res.children.get(0)?.pkMappings.get(0)?.sqlIdentifier, "`_root_id`")
    assert.equal(res.children.get(0)?.pkMappings.get(1)?.sqlIdentifier, "`_parent_id`")
    assert.equal(res.children.get(0)?.pkMappings.get(2)?.sqlIdentifier, "`_level_0_index`")

    assert.equal(res.children.get(0)?.children.get(0)?.sqlTableName, "`audits__bill_fields__john_fields`")
    assert.equal(res.children.get(0)?.children.get(0)?.pkMappings.get(0)?.sqlIdentifier, "`_root_id`")
    assert.equal(res.children.get(0)?.children.get(0)?.pkMappings.get(1)?.sqlIdentifier, "`_parent_bill_id`")
    assert.equal(res.children.get(0)?.children.get(0)?.pkMappings.get(2)?.sqlIdentifier, "`_level_0_index`")
    assert.equal(res.children.get(0)?.children.get(0)?.pkMappings.get(3)?.sqlIdentifier, "`_level_1_index`")

    assert.equal(res.children.get(0)?.children.get(0)?.children.get(0)?.sqlTableName, "`audits__bill_fields__john_fields__jack_fields`")
    assert.equal(res.children.get(0)?.children.get(0)?.children.get(0)?.pkMappings.get(0)?.sqlIdentifier, "`_root_id`")
    assert.equal(res.children.get(0)?.children.get(0)?.children.get(0)?.pkMappings.get(1)?.sqlIdentifier, "`_parent_john_id`")
    assert.equal(res.children.get(0)?.children.get(0)?.children.get(0)?.pkMappings.get(2)?.sqlIdentifier, "`_level_0_index`")
    assert.equal(res.children.get(0)?.children.get(0)?.children.get(0)?.pkMappings.get(3)?.sqlIdentifier, "`_level_1_index`")
    assert.equal(res.children.get(0)?.children.get(0)?.children.get(0)?.pkMappings.get(4)?.sqlIdentifier, "`_level_2_index`")
  })

  it("should handle nest object with arrays", () => {
    const res = buildMeta(new JsonSchemaInspectorContext("audits", nestedObjectWithArraysSchema, List(["id"])))
    assert.equal(res.children.size, 1)
    assert.equal(res.children.get(0)?.sqlTableName, "`audits__nested__tags`")
    assert.equal(res.children.get(0)?.simpleColumnMappings.size, 1)
  })
})
