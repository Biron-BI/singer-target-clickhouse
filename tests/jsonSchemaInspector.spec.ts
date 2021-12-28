import {strict as assert} from 'assert'
import {buildMeta, getSimpleColumnSqlType, IExtendedJSONSchema7, JsonSchemaInspectorContext} from "../src/jsonSchemaInspector"
import {List} from "immutable"


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


describe("getSimpleColumnSqlType", () => {
  it("should handle simple stream", () => {
    const res = getSimpleColumnSqlType(new JsonSchemaInspectorContext("audits", simpleSchema, List()),
      {"type": ["null", "integer"]})
    assert.equal(res, "Int32")
  })
})

describe("JSON Schema Inspector", () => {
  it("should handle simple case", () => {
    const res = buildMeta(new JsonSchemaInspectorContext("audits", simpleSchema, List(["id"])))
    assert.equal(res.sqlTableName, "`audits`")
    assert.equal(res.pkMappings.size, 1)
    assert.equal(res.pkMappings.get(0)?.chType, "Int32")
    assert.equal(res.simpleColumnMappings.size, 3)
    assert.equal(res.simpleColumnMappings.find((elem) => elem.prop === "created_at")?.nullable, false)
  })

  it("should handle array scalar case", () => {
    const res = buildMeta(new JsonSchemaInspectorContext("audits", arrayScalarSchema, List(["id"])))
    assert.equal(res.children.size, 1)
    assert.equal(res.children.get(0)?.sqlTableName, "collaborator_ids")
    assert.equal(res.children.get(0)?.pkMappings.size, 2)
    assert.equal(res.children.get(0)?.pkMappings.get(0)?.prop, "_level_0_index")
    assert.equal(res.children.get(0)?.pkMappings.get(0)?.chType, "Int32")
    assert.equal(res.children.get(0)?.pkMappings.get(0)?.nullable, false)
    assert.equal(res.children.get(0)?.pkMappings.get(1)?.prop, "id")
    assert.equal(res.children.get(0)?.pkMappings.get(1)?.chType, "Int32")
  })

  it("should handle nested object case", () => {
    const res = buildMeta(new JsonSchemaInspectorContext("audits", nestedObjectSchema, List(["id"])))
    assert.equal(res.children.size, 0)
    assert.equal(res.simpleColumnMappings.size, 1)
    assert.equal(res.pkMappings.size, 1)
    assert.equal(res.simpleColumnMappings.get(0)?.sqlIdentifier, "`nested_color`")
    assert.equal(res.simpleColumnMappings.get(0)?.chType, "String")
  })

  it("should handle array of nested object case", () => {
    const res = buildMeta(new JsonSchemaInspectorContext("audits", arrayObjectSchema, List(["id"])))
    assert.equal(res.children.get(0)?.sqlTableName, "audits__custom_fields")
    assert.equal(res.children.get(0)?.simpleColumnMappings.size, 1)
    assert.equal(res.children.get(0)?.simpleColumnMappings.get(0)?.sqlIdentifier, "`field`")
    assert.equal(res.children.get(0)?.pkMappings.size, 2)
    assert.equal(res.children.get(0)?.pkMappings.get(0)?.sqlIdentifier, "`_level_0_index`")
    assert.equal(res.children.get(0)?.pkMappings.get(1)?.sqlIdentifier, "`_root_id`")
  })
})
