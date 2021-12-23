// import {ISourceMeta} from "./jsonSchemaInspector";
// import {JsonDataToSql} from "./jsonSchemaTranslator";
// import * as _ from "lodash";
// import {addPrefix} from "./AJsonToDataStorage";
// import {DBMS} from "../repos/interfaces";
// import {Readable} from "stream";
// import {jsonToJSONCompactEachRow} from "../utils/tsvConverter";
//
// type SourceMetaPK = ISourceMeta & {values: string[]};
//
// /**
//  * Tree structure to process stream of data according to precomputed meta data
//  * One node for one table
//  */
// export default class SqlProcessorNode {
//
//     constructor(meta: ISourceMeta) {
//         this.meta = meta;
//     }
//     private children: SqlProcessorNode[] = [];
//
//     // Used for direct insert mode
//     private currentRootMeta?: SourceMetaPK;
//
//     private fields: string[] = [];
//     private readonly meta: ISourceMeta;
//     private values: string[] = [];
//
//
//     async buildClickHouseInsertQuery(): Promise<{baseQuery: string, stream: Readable}[]> {
//         const tableToInsertTo: string = this.meta.sqlTableName;
//         let ret: {baseQuery: string, stream: Readable}[] = [];
//         for (const child of this.children) {
//             // If i have children, this means i have a PK, so it should use temp
//             ret = ret.concat(await child.buildClickHouseInsertQuery());
//         }
//         if (this.fields.length > 0) {
//             /* Improvement: better use of streams: they don't make much sense here as all data is stored in memory in this.values
//                 but they are required whe inserting in TabSeparated with node-clickhouse
//              */
//
//             const query: string = `INSERT INTO ${tableToInsertTo} FORMAT JSONCompactEachRow\n`;
//             const stream: Readable = new Readable({objectMode: true, read(size) {}});
//
//             for (let i = 0; i < this.values.length; i += this.fields.length) {
//                 stream.push(`[${this.values.slice(i, this.fields.length + i).map(jsonToJSONCompactEachRow).join(",")}]`);
//             }
//             stream.push(null);
//             ret.push({baseQuery: query, stream});
//         }
//         return ret;
//     }
//
//     /**
//      * Returns list of queries to be executed to insert new data in tables
//      * The last element of this array is for the root table.
//      * @param toTemporary
//      * @param sqlAction
//      */
//     buildInsertQuery(toTemporary: boolean, sqlAction: "REPLACE" | "INSERT"): {query: string, values: string[]}[] {
//         const tableToInsertTo: string = toTemporary ? addPrefix(this.meta.sqlTableName) : this.meta.sqlTableName;
//         let ret: {query: string, values: string[]}[] = [];
//         for (const child of this.children) {
//             ret = ret.concat(child.buildInsertQuery(toTemporary, "INSERT"));
//         }
//         if (this.fields.length > 0) {
//
//             // if i have 30 values and 5 fields, there are 6 records
//             const recordsNb = this.values.length / this.fields.length;
//             let query = `${sqlAction} INTO ${tableToInsertTo} (${this.fields.join(",")}) VALUES `;
//
//             const placeholder = `(${this.fields.map(() => "?").join(",")})`;
//
//             query += new Array(recordsNb).fill(placeholder).join(",");
//
//             ret.push({query, values: this.values});
//         }
//         return ret;
//     }
//
//     /*
//         Prepare sql query by splitting fields and values
//         Creates children
//         Structure: One child per table
//      */
//     pushRecord(data: { [k: string]: any }, chunkIndex: number, maxVer: number, dbms: DBMS, parentMeta?: SourceMetaPK, rootMeta?: SourceMetaPK, rootVer?: number) {
//         this.currentRootMeta = rootMeta;
//         // We build header fields only once as the meta is the same for all values in this node
//         if (this.fields.length === 0) {
//             this.fields = this.buildSQLInsertField(this.meta, parentMeta, rootMeta);
//             if (dbms === "ClickHouse") {
//                 if (rootMeta === undefined) {
//                     if (this.meta.pkMappings.length > 0) {
//                         this.fields.push("_ver")
//                     }
//                 } else {
//                     this.fields.push("_root_ver")
//                 }
//             }
//         }
//         // Record new version start at max existing version, + position in stream + 1
//         // Only needed if a column for version exists
//         if (!rootVer && (this.fields.includes("_ver") || this.fields.includes("_root_ver"))) {
//             rootVer = maxVer + chunkIndex + 1;
//         }
//         // We parse values from chunk data received and store them in this node
//         this.values = this.values.concat(this.buildSQLInsertValues(data, this.meta, dbms, parentMeta?.values, rootMeta?.values, rootVer));
//
//         // We extract values of primary keys so we can insert them in children later
//         let pkValues: string[] = [];
//         if (this.meta.children.length > 0) {
//             pkValues = this.meta.pkMappings.map(pkMapping => JsonDataToSql.extractValue(data, pkMapping, dbms));
//         }
//         const meAsParent: SourceMetaPK = {...this.meta, values: pkValues};
//
//         if (!this.currentRootMeta) {
//             this.currentRootMeta = meAsParent;
//         }
//
//         for (const child of this.meta.children) {
//             // We check if a children with the same meta has already been added
//             let processor: SqlProcessorNode = this.children.find((elem) => elem.meta.prop === child.prop &&
//                 elem.meta.sqlTableName === child.sqlTableName);
//             // Otherwise we create a new node
//             if (!processor) {
//                 processor = new SqlProcessorNode(child);
//
//                 this.children.push(processor);
//             }
//             const childData: {[k:string]: any}[] = _.get(data, child.prop.split("."));
//
//             if (childData) {
//                 for (const elem of childData) {
//                     processor.pushRecord(elem, chunkIndex, maxVer, dbms, meAsParent, rootMeta || meAsParent, rootVer);
//                 }
//             }
//         }
//     }
//
//     retrieveRootPKValues() {
//         if (this.currentRootMeta) {
//             return this.currentRootMeta.values;
//         } else {
//             throw new Error("No root PK value could be found");
//         }
//     }
//
//     // Fields that'll be inserted in the database
//     private buildSQLInsertField(meta: ISourceMeta, parent?: ISourceMeta, root?: ISourceMeta): string[] {
//         return [
//             ...(root && root.pkMappings || []).map(cm => cm.sqlIdentifierAsRootFk), // If i have a root, i'll want to insert its PK identifier
//             ...(parent && parent.pkMappings || []).map(cm => cm.sqlIdentifierAsFk), // Same goes for papa node
//             ...meta.simpleColumnMappings.map(cm => cm.sqlIdentifier), // And i want to insert my own fields
//         ];
//     }
//
//     // Values that'll be inserted
//     private buildSQLInsertValues(data: { [k: string]: any}, meta: ISourceMeta, dbms: DBMS,  parentPkValues: any[] = [], rootPkValues: any[] = [], version?: number) {
//         const ret = [
//             ...rootPkValues,
//             ...parentPkValues,
//             ...meta.simpleColumnMappings.map(cm => JsonDataToSql.extractValue(data, cm, dbms)), // My fields value
//         ] // Can't find a way to insert version in ret if exist directly, with destructuring
//         if (version) {
//             ret.push(version)
//         }
//         return ret;
//     }
// }
