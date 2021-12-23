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

describe("getSimpleColumnSqlType", () => {
  it("should handle simple stream", () => {
    const res = getSimpleColumnSqlType(new JsonSchemaInspectorContext("audits", simpleSchema, List()),
      {"type": ["null", "integer"]})
    assert.equal(res, "Int32")
  })
})

describe("JSON Schema Inspector", () => {
  it("should handle simple case", async () => {
    const res = buildMeta(new JsonSchemaInspectorContext("audits", simpleSchema, List(["id"])))
    assert.equal(res.sqlTableName, "`audits`")
    assert.equal(res.pkMappings.size, 1)
    assert.equal(res.pkMappings.get(0)?.chType, "Int32")
    assert.equal(res.simpleColumnMappings.length, 3)
    assert.equal(res.simpleColumnMappings.find((elem) => elem.prop === "created_at")?.nullable, false)
  })
})
