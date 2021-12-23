// import AJsonToDataStorage, {canInsertDirectly, ICompiledSourceMeta} from "./AJsonToDataStorage";
// import {ISqlConnection} from "../repos/interfaces";
// import {ISourceMeta, PkMap} from "./jsonSchemaInspector";
// import {JSONSchema7} from "json-schema";
// import {translateCH} from "./jsonSchemaTranslator";
// import {buildTruncateTableQueries} from "./JsonToMySQL";
// import ono = require("ono");
// import {pipeline, Readable, Transform, Writable} from "stream";
// const ClickHouse = require("@apla/clickhouse");
// const util = require("util");
// const debugtrace = util.debuglog("biron-connect:customerDatabase-trace");
// import * as retry from "retry"
//
// interface ICHQueryResult {
//     data: any[];
//     meta: any[];
//     rows: number;
//     "rows_before_limit_at_least": number;
//     "statistics": {
//         "bytes_read": number
//         "elapsed": number,
//         "rows_read": number,
//     };
//     "transferred": number;
// }
//
// /**
//  *  Implement AJSonToDataStorage with connexion and syntax specific to Clickhouse
//  *  One instance of this class per root table
//  */
// export default class JsonToCH extends AJsonToDataStorage {
//
//     protected async deleteCleaningValue(value: any): Promise<void> {
//         debugtrace("Deleting based on %j", value)
//         if (this.meta?.cleaningColumn) {
//             const query = `ALTER TABLE ${this.meta.sqlTableName} DELETE WHERE \`${this.meta.cleaningColumn}\`='${value}'` // Expects strings, if it is something else retrieve type from meta
//             await this.runQuery(query)
//         } else {
//             throw new Error("Trying to delete based on cleaning column but it is undefined")
//         }
//     }
//
//     constructor(schema?: JSONSchema7, connInfo?: ISqlConnection, meta?: ISourceMeta) {
//         super(schema, connInfo);
//         this.connInfo = connInfo;
//         this.meta = meta; // Root meta
//         this.dbms = "ClickHouse";
//     }
//     private connection: any;
//     private connInfo?: ISqlConnection;
//
//     async checkConnection(): Promise<void> {
//         await this.connectionPool();
//     }
//
//     async clearTables(): Promise<void> {
//         const queries: string[] = buildTruncateTableQueries(this.meta);
//         await Promise.all(queries.map(async (query) => this.runQuery(query)));
//     }
//
//     async initTables(): Promise<any> {
//         const sqls = translateCH(this.meta);
//         for (const query of sqls) {
//             await this.runQuery(query);
//         }
//     }
//
//     // FIXME this method is overwritten here because we don't want to create tmp tables when sending to clickhouse anymore
//     protected async initTemporaryTables(): Promise<any> {
//
//     }
//
//     protected assertEverythingSet(): void {
//         if (!this.meta) {
//             throw new Error("Meta data undefined");
//         }
//
//         if (!this.connInfo) {
//             throw new Error("Connection info undefined");
//         }
//
//         if (this.directInsert) {
//             throw new Error("Clickhouse does not support direct insert (can't delete children)");
//         }
//     }
//
//     protected buildMergeQuery(existingTable: string, temporaryTable: string, temporaryMainTable: string,
//                               pks: string[], rootPks: PkMap[]): string {
//         let query = `INSERT INTO ${temporaryTable} SELECT * FROM ${existingTable} old`;
//
//         const whereClauses: string[] = [];
//         if (rootPks.length > 0) {
//             whereClauses.push(`(${this.pksToTuple(rootPks.map((elem) => elem.sqlIdentifierAsRootFk))}) NOT IN (SELECT ${this.pksToTuple(rootPks.map((elem) => elem.sqlIdentifier))} FROM ${temporaryMainTable})`);
//         }
//         // This part is not needed for children, we expect that it never happens, so we only run it on root for efficiency
//         else if (pks.length > 0) {
//             whereClauses.push(`(${this.pksToTuple(pks, "old")}) NOT IN (SELECT ${this.pksToTuple(pks)} FROM ${temporaryTable})`);
//         }
//
//         if (this.meta.cleaningColumn) {
//             whereClauses.push(`old.${this.meta.cleaningColumn} NOT IN (SELECT ${this.meta.cleaningColumn} FROM ${temporaryTable})`)
//         }
//
//         if (whereClauses.length > 0) {
//             query += ` WHERE ${whereClauses.join(" AND ")}`;
//         }
//
//         return query;
//     }
//
//     protected async finalizeBatchProcessing(): Promise<void> {
//         if (this.isReplacingMergeTree()) {
//             await this.runQuery(`OPTIMIZE TABLE ${this.meta.sqlTableName} FINAL`);
//
//             for (const child of this.meta.children) {
//                 await this.deleteChildDuplicates(child)
//             }
//         }
//         await this.assertPKIntegrity(this.meta);
//     }
//
//     private async deleteChildDuplicates(currentNode: ISourceMeta) {
//         const query = `ALTER TABLE ${currentNode.sqlTableName} DELETE WHERE (${
//             this.meta.pkMappings
//                 .map((elem) => elem.sqlIdentifierAsRootFk)
//                 .concat(["_root_ver"])
//                 .join(",")
//         }) NOT IN (SELECT ${
//             this.meta.pkMappings
//                 .map((elem) => elem.sqlIdentifier)
//                 .concat(["_ver"])
//                 .join(",")
//         } FROM ${this.meta.sqlTableName})`
//         await this.runQuery(query);
//
//         for (const child of currentNode.children) {
//             await this.deleteChildDuplicates(child);
//         }
//     }
//
//     /**
//        Returns true if conditions were met to create a replacing merge tree
//      */
//     private isReplacingMergeTree(): boolean {
//         return this.meta.pkMappings.length > 0
//     }
//
//
//     protected async saveNewRecords(): Promise<void> {
//        const asyncPipeline = util.promisify(pipeline);
//
//         if (this.sqlProcessor) {
//             const queries = await this.sqlProcessor.buildClickHouseInsertQuery();
//             await Promise.all(queries.map(async (query) => {
//                 return new Promise(async (resolve, reject) => {
//                     const writeStream = await this.createCHWriteStream(query.baseQuery, (err, result) => {
//                         if (err) {
//                             reject(err);
//                         } else {
//                             resolve(result);
//                         }
//                     });
//
//                     writeStream.on("error", (err) => {
//                         reject(ono(err, "ch write stream error"))
//                     })
//
//                     const transform: Transform = new Transform({objectMode: true,
//                         transform(chunk, encoding, callback) {
//                             debugtrace("inserting: %s", chunk);
//                             this.push(chunk);
//                             callback();
//                         }},
//                     );
//                     try {
//                         await asyncPipeline(query.stream, transform, writeStream);
//                     } catch (err) {
//                         reject(ono(err, "could not process insert data stream"));
//                     }
//                 });
//             }));
//             this.sqlProcessor = null;
//         }
//     }
//
//     protected async retrieveMaxRecordVersion() {
//         if (this.isReplacingMergeTree()) {
//             const res = await this.runQuery(`SELECT max(_ver) FROM ${this.meta.sqlTableName}`);
//             this.maxVer = Number(res.data[0][0]);
//         }
//     }
//
//     protected setConnInfo(connInfo: ISqlConnection): void {
//         this.connInfo = connInfo;
//     }
//
//     // Improvement: concurrency
//     private async assertPKIntegrity(meta: ISourceMeta) {
//         for (const childMeta of meta.children) {
//             await this.assertPKIntegrity(childMeta);
//         }
//         if (meta.pkMappings.length === 0) {
//             return;
//         }
//         const pks: string = meta.pkMappings.map((elem: PkMap) => elem.sqlIdentifier).join(",");
//
//         const query = `SELECT ${pks} FROM (SELECT ${pks} FROM ${meta.sqlTableName} ORDER BY ${pks}) WHERE (${pks})=neighbor(
//             (${pks}), -1, (${meta.pkMappings.map(() => "null").join(",")})) LIMIT 1`;
//         const result = await this.runQuery(query);
//         if (result.rows > 0) {
//             throw ono("Duplicate key on table %s, data: %j, aborting process", meta.sqlTableName, result.data);
//         }
//     }
//
//     private async connectionPool(): Promise<any> {
//         if (!this.connection) {
//             this.connection = new ClickHouse({
//                 host: this.connInfo.host,
//                 user: this.connInfo.user,
//                 port: /*this.connInfo.port ||*/ 8123, // todo
//                 password: this.connInfo.password,
//                 queryOptions: {
//                     mutations_sync: 2, // To run data deletion sync https://clickhouse.tech/docs/en/operations/settings/settings/#mutations_sync
//                     database: this.connInfo.database,
//                     date_time_input_format: "best_effort", // To handle all date types, https://clickhouse.tech/docs/en/operations/settings/settings/#settings-date_time_input_format
//                 }
//             }); // may need to set protocol: https for prod
//
// /* fixme: this would be cleaner but doesn't work
//   const queryBuilder = util.promisify(this.connection.query);
//             try {
//                 await queryBuilder("SELECT 1");
//                 this.connection = connection;
//             } catch (err) {
//                 this.connection = undefined;
//                 throw ono(err, `fail to init data base connection pool [${this.connInfo.alias}]`);
//             }*/
//             return new Promise((resolve, reject) => {
//                 this.connection.query("SELECT 1", (err: Error, data: any) => {
//                     if (err) {
//                         this.connection = undefined;
//                         reject(err);
//                     } else {
//                         resolve(this.connection);
//                     }
//                 });
//             });
//         } else {
//             return this.connection;
//         }
//     }
//
//     // https://github.com/apla/node-clickhouse/blob/HEAD/README.md#inserting-with-stream
//     private async createCHWriteStream(query: string, callback: (err: Error, result: any) => any): Promise<Writable> {
//         const conn = await this.connectionPool();
// //        const asyncQuery = util.promisify(conn.query);
//
//         debugtrace("building stream to query sql %s", query);
//         try {
//             return conn.query(query, {omitFormat: true}, callback);
//         } catch (err) {
//             throw ono("ch stream failed", err);
//         }
//     }
//
//     private pksToTuple(pks: string[], tableDenominator?: string): string {
//         return pks.map((pk => tableDenominator ? `${tableDenominator}.${pk}` : pk)).join(",");
//     }
//
//     // https://github.com/apla/node-clickhouse#promise-interface
//     // This way of querying is not recommended for large SELECT and INSERTS
//     private async runQuery(query: string): Promise<ICHQueryResult> {
//         const conn = await this.connectionPool();
//
//         return new Promise((resolve, reject) => {
//             const op = retry.operation({
//                 retries: 3,
//                 factor: 3
//             });
//             op.attempt(async function () {
//                 try {
//                     debugtrace("query sql %s", query);
//                     const res = await conn.querying(query);
//                     resolve(res)
//                 } catch (err) {
//                     if (op.retry(err)) {
//                         return;
//                     }
//                     reject(ono(err, "even after retries, ch query failed", err));
//                 }
//             });
//         });
//         /*return new Promise((resolve, reject) => {
//             conn.query(query, (err: Error, data: any) => {
//                 if (err) {
//                     reject(err);
//                 } else {
//                     console.log("data:", data);
//                     resolve(data);
//                 }
//             });
//         });*/
//     }
// }
