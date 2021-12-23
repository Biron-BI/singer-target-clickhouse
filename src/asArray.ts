import {List} from "immutable"

export function asArray<T>(value: T | T[]): List<T> {
  if (!Array.isArray(value)) {
    return List([value])
  } else {
    return List(value)
  }
}
