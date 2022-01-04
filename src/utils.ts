import {List, Map} from "immutable"

export function asArray<T>(value: T | T[]): List<T> {
  if (!Array.isArray(value)) {
    return List([value])
  } else {
    return List(value)
  }
}

export async function awaitMapValues<T>(map: Map<string, Promise<T>>): Promise<Map<string, T>> {
  const values = await Promise.all(map.values())

  return List(map.keys()).reduce((acc, key, index) => {
    return acc.set(key, values[index])
  }, Map<string, T>())
}

// To escape values already wrapped in delimiter
export const escapeValue = (value: string, delimiter = "'"): string => value.split(delimiter).join(`\\${delimiter}\\`)
