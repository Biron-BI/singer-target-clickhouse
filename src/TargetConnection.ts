import {Writable} from "stream"

export default interface TargetConnection {
  createWriteStream(query: string, callback: (err: any, res: any) => void): Writable
}