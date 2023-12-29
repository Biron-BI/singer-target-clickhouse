import {Writable} from "stream"
export interface IQueryResult {
  data: string[][];
  rows: number;
  "rows_before_limit_at_least": number;
  "statistics": {
    "bytes_read": number
    "elapsed": number,
    "rows_read": number,
  };
  "transferred": number;
}
export default interface TargetConnection {
  createWriteStream(query: string, callback: (err: any, res: any) => void): Writable
  runQuery(query: string, retries?: number): Promise<IQueryResult>
}