// import {ColumnMap, ISourceMeta, PkMap} from "./jsonSchemaInspector";
// import * as _ from "lodash";
// import {DBMS} from "../repos/interfaces";
// import ChSchemaTranslator from "./schemaTranslator/ChSchemaTranslator";
// import SchemaTranslator from "./schemaTranslator/SchemaTranslator";
// import MySQLSchemaTranslator from "./schemaTranslator/MySQLSchemaTranslator";
//
// function dbmsToTranslator(dbms: DBMS, mapping: ColumnMap | PkMap): SchemaTranslator {
//     switch (dbms) {
//         case "ClickHouse":
//             return new ChSchemaTranslator(mapping);
//         case "MySQL":
//             return new MySQLSchemaTranslator(mapping)
//         default:
//             throw new Error("Unrecognized DBMS");
//     }
// }
//
// export const JsonDataToSql = {
//     extractValue(data: { [k: string]: any }, mapping: ColumnMap | PkMap, dbms: DBMS): string {
//         let v = mapping.prop ? _.get(data, mapping.prop.split(".")) : data;
//         const translator = dbmsToTranslator(dbms, mapping);
//         return translator.extractValue(v);
//     }
// };
//
// /**
//  * Returns list of sql instructions to setup sql table
//  */
//
// // Je prends le parti de ne pas stocker `ver` dans les PK, car elle n'en est pas vraiment une.
// export function translateCH(meta: ISourceMeta, parentMeta?: ISourceMeta, rootMeta?: ISourceMeta): string[] {
//     const isNodeRoot = rootMeta === undefined;
//     const sqls: string[] = [];
//     let createQuery = `CREATE TABLE ${meta.sqlTableName}( `;
//     const createDefs: string[] = [];
//     sqls.push(`DROP TABLE IF EXISTS ${meta.sqlTableName}`);
//
//     { // CREATE TABLE for current schema
//         { // FK part for column
//             if (parentMeta) {
//                 createDefs.push(...rootMeta.pkMappings.map(fkMapping => `${fkMapping.sqlIdentifierAsRootFk} ${fkMapping.chType}`));
//                 createDefs.push(...parentMeta.pkMappings.map(fkMapping => `${fkMapping.sqlIdentifierAsFk} ${fkMapping.chType}`));
//             }
//         }
//         { // simple createDefs part
//             createDefs.push(...meta.simpleColumnMappings.map(mapping => {
//                 const type = mapping.mandatory ? mapping.chType : `Nullable(${mapping.chType})`;
//                 return `${mapping.sqlIdentifier} ${type}`;
//             }));
//         }
//     }
//
//     let engine = "MergeTree()";
//
//     if (isNodeRoot) {
//         // We create a ReplacingMergeTree and add a versioning column when we want to handle duplicate: for root tables with PK
//         if (meta.pkMappings.length > 0) {
//             createDefs.push("`_ver` UInt64");
//             engine = `ReplacingMergeTree(_ver)`
//         }
//         // In case root has no child, it stays a MergeTree
//     } else {
//         createDefs.push("`_root_ver` UInt64"); // A child shall always have a parent with a versioning column (It may or may not be written in the bible)
//     }
//
//     createQuery += createDefs.join(",");
//
//     // Ternary to handle no PK case
//     createQuery += `) ENGINE = ${engine} ORDER BY (${meta.pkMappings.length > 0 ? meta.pkMappings.map((elem) => elem.sqlIdentifier).join(",") :
//             "tuple()"})`;
//     sqls.push(createQuery);
//     sqls.push(..._.flatMap(meta.children, (child: ISourceMeta) => translateCH(child, meta, rootMeta || meta)));
//
//     return sqls;
// }
