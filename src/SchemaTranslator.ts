import {ColumnMap, PkMap} from "jsonSchemaInspector"

function isoDate(d: Date) {
  // Handle time zone
  d.getTimezoneOffset();
  d = new Date(d.getTime() - (d.getTimezoneOffset() * 60 * 1000))

  return d.toISOString().substr(0, ("YYYY-MM-DD".length));
}

export default class SchemaTranslator {
  protected mapping: ColumnMap | PkMap;

  constructor(mapping: ColumnMap | PkMap) {
    this.mapping = mapping;
  }

  extractValue(v: any): any {
    if (v === undefined || v === null) {
      return v;
    }

    switch (this.mapping.type) {
      case "string":
        return this.extractString(v);
      case "boolean":
        return this.extractBoolean(v);
      case "integer":
        return this.extractInteger(v);
      case "number":
        return this.extractNumber(v);
      default:
        return v;
    }
  }

  extractString(v: any): string {
    // Excel date may be either an actual date or the number of days since 01-01-1900
    if (this.mapping.typeFormat === "x-excel-date") {
      // We start by checking if it is an actual date
      if (String(v).match(/^(\d{4})-(\d{2})-(\d{2})$/) !== null) {
        return String(v);
      }
      const value = parseInt(v);
      if (isNaN(value)) {
        throw new Error("Could not parse excel date: " + v)
      }

      const date = new Date(0);
      const offset = 25567; // Number of days between 01-01-1900 and 01-01-1970
      date.setDate(date.getDate() + parseInt(v) - offset);

      return isoDate(date);
    }
    return String(v);
  }

  extractBoolean(v: any): 1 | 0 {
    if (v === "true" || v === true || v === 1) {
      return 1;
    } else {
      return 0;
    }
  }

  extractNumber(v: any): number | undefined {
    const ret = parseFloat(v);
    if (isNaN(ret)) {
      return undefined;
    }
    return ret;
  }

  extractInteger(v: any): number | undefined {
    const ret = parseInt(v);
    if (isNaN(ret)) {
      return undefined
    }
    return ret;
  }
}
