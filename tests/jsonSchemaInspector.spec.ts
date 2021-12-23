import {strict as assert} from 'assert'
import {buildMeta, getSimpleColumnSqlType, IExtendedJSONSchema7, JsonSchemaInspectorContext} from "../src/jsonSchemaInspector"
import {List} from "immutable"

const hugeSchema: IExtendedJSONSchema7 = {
  "properties":
    {
      "events": {
        "items": {
          "properties": {
            "attachments": {
              "items": {
                "properties": {
                  "id": {"type": ["null", "integer"]},
                  "size": {"type": ["null", "integer"]},
                  "url": {"type": ["null", "string"]},
                  "inline": {"type": ["null", "boolean"]},
                  "height": {"type": ["null", "integer"]},
                  "width": {"type": ["null", "integer"]},
                  "content_url": {"type": ["null", "string"]},
                  "mapped_content_url": {"type": ["null", "string"]},
                  "content_type": {"type": ["null", "string"]},
                  "file_name": {"type": ["null", "string"]},
                  "thumbnails": {
                    "items": {
                      "properties": {
                        "id": {"type": ["null", "integer"]},
                        "size": {"type": ["null", "integer"]},
                        "url": {"type": ["null", "string"]},
                        "inline": {"type": ["null", "boolean"]},
                        "height": {"type": ["null", "integer"]},
                        "width": {"type": ["null", "integer"]},
                        "content_url": {"type": ["null", "string"]},
                        "mapped_content_url": {"type": ["null", "string"]},
                        "content_type": {"type": ["null", "string"]},
                        "file_name": {"type": ["null", "string"]},
                      }, "type": ["null", "object"],
                    }, "type": ["null", "array"],
                  },
                }, "type": ["null", "object"],
              }, "type": ["null", "array"],
            },
            "created_at": {"format": "date-time", "type": ["null", "string"]},
            "data": {
              "properties": {
                "transcription_status": {"type": ["null", "string"]},
                "transcription_text": {"type": ["null", "string"]},
                "to": {"type": ["null", "string"]},
                "call_duration": {"type": ["null", "string"]},
                "answered_by_name": {"type": ["null", "string"]},
                "recording_url": {"type": ["null", "string"]},
                "started_at": {"format": "date-time", "type": ["null", "string"]},
                "answered_by_id": {"type": ["null", "integer"]},
                "from": {"type": ["null", "string"]},
              }, "type": ["null", "object"],
            },
            "formatted_from": {"type": ["null", "string"]},
            "formatted_to": {"type": ["null", "string"]},
            "transcription_visible": {},
            "trusted": {"type": ["null", "boolean"]},
            "html_body": {"type": ["null", "string"]},
            "subject": {"type": ["null", "string"]},
            "field_name": {"type": ["null", "string"]},
            "audit_id": {"type": ["null", "integer"]},
            "value": {"items": {"type": ["null", "string"]}, "type": ["null", "array", "string"]},
            "author_id": {"type": ["null", "integer"]},
            "via": {
              "properties": {
                "channel": {"type": ["null", "string"]},
                "source": {
                  "properties": {
                    "to": {
                      "properties": {
                        "address": {"type": ["null", "string"]},
                        "name": {"type": ["null", "string"]},
                      }, "type": ["null", "object"],
                    },
                    "from": {
                      "properties": {
                        "title": {"type": ["null", "string"]},
                        "address": {"type": ["null", "string"]},
                        "subject": {"type": ["null", "string"]},
                        "deleted": {"type": ["null", "boolean"]},
                        "name": {"type": ["null", "string"]},
                        "original_recipients": {"items": {"type": ["null", "string"]}, "type": ["null", "array"]},
                        "id": {"type": ["null", "integer"]},
                        "ticket_id": {"type": ["null", "integer"]},
                        "revision_id": {"type": ["null", "integer"]},
                      }, "type": ["null", "object"],
                    },
                    "rel": {"type": ["null", "string"]},
                  }, "type": ["null", "object"],
                },
              }, "type": ["null", "object"],
            },
            "type": {"type": ["null", "string"]},
            "macro_id": {"type": ["null", "string"]},
            "body": {"type": ["null", "string"]},
            "recipients": {"items": {"type": ["null", "integer"]}, "type": ["null", "array"]},
            "macro_deleted": {"type": ["null", "boolean"]},
            "plain_body": {"type": ["null", "string"]},
            "id": {"type": ["null", "integer"]},
            "previous_value": {"items": {"type": ["null", "string"]}, "type": ["null", "array", "string"]},
            "macro_title": {"type": ["null", "string"]},
            "public": {"type": ["null", "boolean"]},
            "resource": {"type": ["null", "string"]},
          }, "type": ["null", "object"],
        }, "type": ["null", "array"],
      },
      "author_id": {"type": ["null", "integer"]},
      "metadata": {
        "properties": {
          "custom": {},
          "trusted": {"type": ["null", "boolean"]},
          "notifications_suppressed_for": {"items": {"type": ["null", "integer"]}, "type": ["null", "array"]},
          "flags_options": {
            "properties": {
              "2": {"properties": {"trusted": {"type": ["null", "boolean"]}}, "type": ["null", "object"]},
              "11": {
                "properties": {
                  "trusted": {"type": ["null", "boolean"]},
                  "message": {"properties": {"user": {"type": ["null", "string"]}}, "type": ["null", "object"]},
                }, "type": ["null", "object"],
              },
            }, "type": ["null", "object"],
          },
          "flags": {"items": {"type": ["null", "integer"]}, "type": ["null", "array"]},
          "system": {
            "properties": {
              "location": {"type": ["null", "string"]},
              "longitude": {"type": ["null", "number"]},
              "message_id": {"type": ["null", "string"]},
              "raw_email_identifier": {"type": ["null", "string"]},
              "ip_address": {"type": ["null", "string"]},
              "json_email_identifier": {"type": ["null", "string"]},
              "client": {"type": ["null", "string"]},
              "latitude": {"type": ["null", "number"]},
            }, "type": ["null", "object"],
          },
        }, "type": ["null", "object"],
      },
      "id": {"type": ["null", "integer"]},
      "created_at": {"format": "date-time", "type": ["null", "string"]},
      "ticket_id": {"type": ["null", "integer"]},
      "via": {
        "properties": {
          "channel": {"type": ["null", "string"]},
          "source": {
            "properties": {
              "from": {
                "properties": {
                  "ticket_ids": {
                    "items": {"type": ["null", "integer"]},
                    "type": ["null", "array"],
                  },
                  "subject": {"type": ["null", "string"]},
                  "name": {"type": ["null", "string"]},
                  "address": {"type": ["null", "string"]},
                  "original_recipients": {"items": {"type": ["null", "string"]}, "type": ["null", "array"]},
                  "id": {"type": ["null", "integer"]},
                  "ticket_id": {"type": ["null", "integer"]},
                  "deleted": {"type": ["null", "boolean"]},
                  "title": {"type": ["null", "string"]},
                }, "type": ["null", "object"],
              },
              "to": {
                "properties": {"name": {"type": ["null", "string"]}, "address": {"type": ["null", "string"]}},
                "type": ["null", "object"],
              },
              "rel": {"type": ["null", "string"]},
            }, "type": ["null", "object"],
          },
        }, "type": ["null", "object"],
      },
    }, "type": ["null", "object"],
}

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
  it("should handle simple case", async () => {
    const res = buildMeta(new JsonSchemaInspectorContext("audits", simpleSchema, List(["id"])))
    assert.equal(res.sqlTableName, "`audits`")
    assert.equal(res.pkMappings.size, 1)
    assert.equal(res.pkMappings.get(0)?.chType, "Int32")
    assert.equal(res.simpleColumnMappings.size, 3)
    assert.equal(res.simpleColumnMappings.find((elem) => elem.prop === "created_at")?.nullable, false)
  })

  it("should handle array scalar case", async () => {
    const res = buildMeta(new JsonSchemaInspectorContext("audits", arrayScalarSchema, List(["id"])))
    assert.equal(res.children.size, 1)
    assert.equal(res.children.get(0)?.prop, "collaborator_ids")
    assert.equal(res.children.get(0)?.pkMappings.size, 2)
    assert.equal(res.children.get(0)?.pkMappings.get(0)?.prop, "_level_0_index")
    assert.equal(res.children.get(0)?.pkMappings.get(0)?.chType, "Int32")
    assert.equal(res.children.get(0)?.pkMappings.get(0)?.nullable, false)
    assert.equal(res.children.get(0)?.pkMappings.get(1)?.prop, "id")
    assert.equal(res.children.get(0)?.pkMappings.get(1)?.chType, "Int32")
  })

  it("should handle nested object case", async () => {
    const res = buildMeta(new JsonSchemaInspectorContext("audits", nestedObjectSchema, List(["id"])))
    assert.equal(res.children.size, 0)
    assert.equal(res.simpleColumnMappings.size, 1)
    assert.equal(res.pkMappings.size, 1)
    assert.equal(res.simpleColumnMappings.get(0)?.sqlIdentifier, "`nested_color`")
    assert.equal(res.simpleColumnMappings.get(0)?.chType, "String")
  })

  it("should handle array of nested object case", async () => {
    const res = buildMeta(new JsonSchemaInspectorContext("audits", arrayObjectSchema, List(["id"])))
    assert.equal(res.children.get(0)?.sqlTableName, "`audits__custom_fields`")
    assert.equal(res.children.get(0)?.simpleColumnMappings.size, 1)
    assert.equal(res.children.get(0)?.simpleColumnMappings.get(0)?.sqlIdentifier, "`field`")
    assert.equal(res.children.get(0)?.pkMappings.size, 2)
    assert.equal(res.children.get(0)?.pkMappings.get(0)?.sqlIdentifier, "`_level_0_index`")
    assert.equal(res.children.get(0)?.pkMappings.get(1)?.sqlIdentifier, "`_root_id`")
  })

  it("should handle all cases at once", async () => {
    const res = buildMeta(new JsonSchemaInspectorContext("audits", hugeSchema, List(["id"])))
    // console.log(JSON.stringify(res, null, 2))
    assert.equal(res.children.size, 5)
    assert.equal(res.pkMappings.size, 1)
  })
})
