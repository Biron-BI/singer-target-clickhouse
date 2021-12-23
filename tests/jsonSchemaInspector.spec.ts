import {strict as assert} from 'assert'
import {buildMeta, getSimpleColumnSqlType, IExtendedJSONSchema7, JsonSchemaInspectorContext} from "../src/jsonSchemaInspector"
import {List} from "immutable"

const schema: IExtendedJSONSchema7 = {
  "properties": {
    "author_id": {"type": ["null", "integer"]},
    "id": {"type": ["null", "integer"]},
    "created_at": {"format": "date-time", "type": ["null", "string"]},
    "ticket_id": {"type": ["null", "integer"]},
  }, "type": ["null", "object"],
}

describe("getSimpleColumnSqlType", () => {
  it("should handle simple stream", () => {
    const res = getSimpleColumnSqlType(new JsonSchemaInspectorContext("audits", schema, List()),
      {"type": ["null", "integer"]})
    assert.equal(res, "Int32")
  })
})

describe("JSON Schema Inspector", () => {
  it("should handle simple case", async () => {
    const res = buildMeta(new JsonSchemaInspectorContext("audits", schema, List(["id"])))
    assert.equal(res.sqlTableName, "`audits`")
    assert.equal(res.pkMappings.size, 1)
    assert.equal(res.pkMappings.get(0)?.chType, "Int32")
  })
})

