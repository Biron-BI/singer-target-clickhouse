import {ColumnMap, PkMap} from "./jsonSchemaInspector"

type Value = any

export default class SchemaTranslator {
  protected mapping: ColumnMap | PkMap

  constructor(mapping: ColumnMap | PkMap) {
    this.mapping = mapping
  }

  extractValue(v: Value): Value {
    if (v === undefined || v === null) {
      return v
    }

    switch (this.mapping.type) {
      case "string":
        return this.extractString(v)
      case "boolean":
        return this.extractBoolean(v)
      case "integer":
        return this.extractInteger(v)
      case "number":
        return this.extractNumber(v)
      default:
        return v
    }
  }

  extractString(v: Value): string {
    return String(v)
  }

  extractBoolean(v: Value): 1 | 0 {
    if (v === "true" || v === true || v === 1) {
      return 1
    } else {
      return 0
    }
  }

  extractNumber(v: Value): number | undefined {
    const ret = parseFloat(v)
    if (isNaN(ret)) {
      return undefined
    }
    return ret
  }

  extractInteger(v: Value): number | undefined {
    const ret = parseInt(v)
    if (isNaN(ret)) {
      return undefined
    }
    return ret
  }
}
