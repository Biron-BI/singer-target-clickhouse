import {JSONSchema7TypeName} from "json-schema"

export type Value = any
export type ValueTranslator = (v: Value) => Value

function getTranslator(type?: JSONSchema7TypeName) {
  switch (type) {
    case "string":
      return extractString
    case "boolean":
      return extractBoolean
    case "integer":
      return extractInteger
    case "number":
      return extractNumber
    default:
      return null
  }
}

function extractString(v: Value): string {
  return String(v)
}

function extractBoolean(v: Value): 1 | 0 {
  if (v === "true" || v === true || v === 1) {
    return 1
  } else {
    return 0
  }
}

function extractNumber(v: Value): number | undefined {
  const ret = parseFloat(v)
  if (isNaN(ret)) {
    return undefined
  }
  return ret
}

function extractInteger(v: Value): number | undefined {
  const ret = parseInt(v)
  if (isNaN(ret)) {
    return undefined
  }
  return ret
}

export default {
  buildTranslator(type?: JSONSchema7TypeName): ValueTranslator {
    const translator = getTranslator(type)
    return (v: Value) => {
      if (v === undefined || v === null || translator === null) {
        return v
      } else {
        return translator(v)
      }
    }
  },
}
