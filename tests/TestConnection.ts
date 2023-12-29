import TargetConnection from "../src/TargetConnection"
import {Writable} from "stream"
import {StringDecoder} from "string_decoder"
class StringWritable extends Writable {
  public data: string
  _decoder: StringDecoder

  constructor() {
    super();
    this._decoder = new StringDecoder();
    this.data = '';
  }

  _write(chunk: any, encoding: any, callback: any) {
    if (encoding === 'buffer') {
      chunk = this._decoder.write(chunk);
    }
    this.data += chunk;
    callback();
  }

  _final(callback: any) {
    this.data += this._decoder.end();
    callback();
  }
}

export class TestConnection implements TargetConnection {
  streams: StringWritable[] = []
  queries: string[]= []

  constructor() {
  }

  public createWriteStream(query: string, callback: (err: any, res: any) => void): Writable {
    const writable = new StringWritable()
    writable.on("finish", () => {
      callback(null, null)
    })
    this.streams.push(writable)
    return writable;
  }

  runQuery(query: string, retries: number): Promise<any> {
    this.queries.push(query)
    return Promise.resolve(undefined)
  }
}