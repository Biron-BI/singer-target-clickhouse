import {Writable} from "stream"

export default interface TargetConnection {
  createWriteStream(query: string): Writable
}