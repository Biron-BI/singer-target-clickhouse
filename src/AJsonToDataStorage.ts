// // abstract class intended to be inherited by each storage destination: mySQL, clickhouse, snowflake, ... based on the connectionAlias
//
// import {buildMeta, IExtendedJSONSchema7, ISourceMeta, JsonSchemaInspectorContext, PkMap} from "./jsonSchemaInspector"
// import SqlProcessorNode from "./SqlProcessorNode"
//
// const conf = require("../config")
//
//
// const {dereferenceExternalSchema} = require("../utils/dereferenceSchema")
// const util = require("util")
// const debuglog = util.debuglog("biron-connect:customerDatabase")
//
// export interface ICompiledSourceMeta {
//   alias: string;
//   cleanFirst: boolean;
//   connAlias: string;
//   directInsert: boolean;
//   meta?: ISourceMeta;
//   schema: JSONSchema7;
//   topic: string;
// }
//
// // all common code between subclasses should be defined here
// // Will probably need to be improved once we add more inherited class
// export default abstract class AJsonToDataStorage {
//   protected meta?: ISourceMeta
//   protected schema?: IExtendedJSONSchema7
//
//   // determine whether data should be inserted directly (or replaced if data wasn't cleared)
//   // or sent to a temporary table before merging (mandatory for clickhouse)
//   protected directInsert: boolean = true
//   protected cleanFirst: boolean
//   protected maxVer: number = 0
//
//   // Will contain all values used to clear data based on 'cleaningColumn'
//   protected cleaningValues: any[] = []
//
//   protected constructor(schema: JSONSchema7, connInfo: ISqlConnection) {
//     this.schema = schema
//     this.setConnInfo(connInfo)
//   }
//
//   protected sqlProcessor?: SqlProcessorNode
//   private currentBatchNb: number = 0
//   private nbRecords: number = 0
//   private recordsThreshold: number = conf.get("batch:threshold")
//
//   public async buildMetadata(source: ISource, codeName: string): Promise<void> {
//     this.directInsert = source.directInsert
//     source.schema = await dereferenceExternalSchema(source.schema)
//     this.schema = source.schema
//     if (source.topic.endsWith(".")) {
//       source.topic += `${codeName}.${source.alias}`
//     }
//     const table = source.table || source.alias
//     const ctx = new JsonSchemaInspectorContext(
//       source.alias,
//       source.schema,
//       undefined, // Primary keys will be inferred from schema or autoPrimaryKeyNames
//       undefined,
//       source.autoPrimaryKeyNames || ["id"],
//       table,
//     )
//
//     // convert the passed schema in a "compiled" version ready to used in an optimized way
//     this.meta = await buildMeta(ctx, true)
//     if (this.directInsert && this.meta.cleaningColumn) { // We only chek root meta
//       throw new Error("direct insert mode is incompatible with cleaning column")
//     }
//   }
//
//   /**
//    * Returns true if main table has any column
//    */
//   public hasColumns(): boolean {
//     return this.meta && this.meta.simpleColumnMappings.length + this.meta.pkMappings.length > 0
//   }
//
//   public abstract checkConnection(): Promise<void>;
//
//   public abstract clearTables(): Promise<void>;
//
//   public async doneProcessing(): Promise<number> {
//     try {
//       await this.saveNewRecords()
//     } catch (err) {
//       throw ono(err, "could not save new records")
//     }
//     await this.finalizeBatchProcessing()
//
//     return this.nbRecords
//   }
//
//   public getMetaStorageData(): ISourceMeta {
//     return this.meta
//   }
//
//   public getUpdatedConfiguration(source: ISource, defaultConnAlias?: string): ICompiledSourceMeta {
//     return {
//       alias: source.alias,
//       cleanFirst: source.cleanFirst,
//       topic: source.topic,
//       directInsert: source.directInsert,
//       meta: this.meta,
//       schema: this.schema, // Storing dereferenced schema,
//       connAlias: source.sqlConnection || defaultConnAlias,
//     }
//   }
//
//   /**
//    * Use built metadata to setup tables in storage. Will erase existing data
//    */
//   public abstract initTables(): Promise<boolean>;
//
//   /**
//    * Prepares tables and local variables for JSON stream processing
//    */
//   public async prepareStreamProcessing(cleanFirst: boolean, directInsert: boolean) {
//     this.cleanFirst = cleanFirst
//     this.directInsert = directInsert
//     this.assertEverythingSet()
//
//     if (directInsert) {
//       this.recordsThreshold = 1 // We want to insert every time a chunk is received
//     } else {
//       await this.initTemporaryTables()
//     }
//     if (cleanFirst) {
//       await this.clearTables()
//     }
//     await this.retrieveMaxRecordVersion()
//     this.nbRecords = 0
//     this.currentBatchNb = 0
//     this.cleaningValues = []
//   }
//
//   protected async retrieveMaxRecordVersion() {
//     this.maxVer = 0
//   }
//
//   public async processChunk(data: { [k: string]: any }[], chunkIndex: number) {
//     this.nbRecords += data.length
//     for (const elem of data) {
//       if (this.meta.cleaningColumn) {
//         const cleaningValue = elem[this.meta.cleaningColumn]
//         if (!this.cleaningValues.includes(cleaningValue)) {
//           this.cleaningValues.push(cleaningValue)
//           await this.deleteCleaningValue(cleaningValue)
//         }
//       }
//       this.pushRecord(elem, chunkIndex, this.maxVer)
//       this.currentBatchNb++
//       if (this.currentBatchNb >= this.recordsThreshold) {
//         try {
//           await this.saveNewRecords()
//         } catch (err) {
//           throw ono(err, "could not save new after threshold limit passed records")
//         }
//         this.currentBatchNb = 0
//       }
//     }
//   }
//
//   // Make sure every needed data is set before attempting to process stream
//   protected abstract assertEverythingSet(): void;
//
//   protected abstract deleteCleaningValue(value: any): Promise<void>
//
//   // Meta is the child
//   protected buildMergeQueries(meta: ISourceMeta, rootMeta: ISourceMeta): string[] {
//     let ret: string[] = []
//     for (const metaChild of meta.children) {
//       ret = ret.concat(this.buildMergeQueries(metaChild, rootMeta))
//     }
//     // query for children
//     if (meta.sqlTableName !== rootMeta.sqlTableName) {
//       ret.push(this.buildMergeQuery(meta.sqlTableName, addPrefix(meta.sqlTableName),
//         addPrefix(rootMeta.sqlTableName), meta.pkMappings.map((pkMap) => pkMap.sqlIdentifier), rootMeta.pkMappings))
//     } else {
//       ret.push(this.buildMergeQuery(meta.sqlTableName, addPrefix(meta.sqlTableName),
//         addPrefix(meta.sqlTableName), meta.pkMappings.map((pkMap) => pkMap.sqlIdentifier), []))
//     }
//     return ret
//   }
//
//   protected abstract buildMergeQuery(existingTable: string, temporaryTable: string, temporaryMainTable: string,
//                                      pks: string[], rootPks: PkMap[]): string;
//
//   protected abstract finalizeBatchProcessing(): Promise<void>;
//
//   /**
//    * Should add more values to query
//    */
//   protected abstract saveNewRecords(): Promise<void>;
//
//   protected abstract setConnInfo(connInfo: ISqlConnection): void;
//
//   protected async initTemporaryTables(): Promise<any> {
//     // We don't need to init temp tables if there are no PK in root and if there isn't a discriminating column
//     if (!canInsertDirectly(this.meta)) {
//       this.meta = metaToTempDb(this.meta) // we temporarily set meta conf to "tmp_<tableName>"
//       await this.initTables()
//       this.meta = restoreTempMeta(this.meta)
//     }
//   }
//
//   private pushRecord(data: { [k: string]: any }, chunkIndex: number, maxVer: number): void {
//     if (!this.sqlProcessor) {
//       this.sqlProcessor = new SqlProcessorNode(this.meta)
//     }
//     this.sqlProcessor.pushRecord(data, chunkIndex, maxVer, this.dbms)
//   }
// }
//
// // expects `tableName`
// export function addPrefix(tableName: string): string {
//   return "`tmp_" + tableName.substr(1) // Start after `
// }
//
// // expects `tmp_tableName`
// export function removePrefix(tableName: string): string {
//   return "`" + tableName.substr(5) // Start after `_tmp
// }
//
// function metaToTempDb(meta: ISourceMeta): ISourceMeta {
//   meta.sqlTableName = addPrefix(meta.sqlTableName)
//   for (let i = 0; i < meta.children.length; i++) {
//     meta.children[i] = metaToTempDb(meta.children[i])
//   }
//   return meta
// }
//
// function restoreTempMeta(meta: ISourceMeta): ISourceMeta {
//   meta.sqlTableName = removePrefix(meta.sqlTableName)
//   for (let i = 0; i < meta.children.length; i++) {
//     meta.children[i] = restoreTempMeta(meta.children[i])
//   }
//   return meta
// }
//
// export function canInsertDirectly(meta: ISourceMeta) {
//   // If there is a primary key, we can't insert directly as we have to ensure there are no duplicates
//   // Same for discriminating column, we want to overwrite them
//   return meta.pkMappings.length === 0 && !meta.cleaningColumn
// }
