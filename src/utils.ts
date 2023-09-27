export function asArray<T>(value: T | T[]): T[] {
  if (!Array.isArray(value)) {
    return [value]
  } else {
    return value
  }
}
// To escape values already wrapped in delimiter
export const escapeValue = (value: string, delimiter = "'"): string => value.split(delimiter).join(`\\${delimiter}\\`)
