package com.biron.singerTargetClickhouse

import arrow.core.Either
import io.github.oshai.kotlinlogging.KotlinLogging

private val logger = KotlinLogging.logger {}

fun extractValue(data: Any?, mapping: ColumnMap, translateValue: Boolean): Any? {
	val raw = mapping.valueExtractor(data) ?: return null
	return if (translateValue) (mapping.valueTranslator?.invoke(raw) ?: raw) else raw
}

fun extractValue(data: Any?, mapping: PkMap, translateValue: Boolean): Any? {
	val raw = mapping.valueExtractor(data) ?: return null
	return if (translateValue) (mapping.valueTranslator?.invoke(raw) ?: raw) else raw
}

private fun resolveVersionColumn(isRoot: Boolean, hasPkMappings: Boolean, withType: Boolean = true): String {
	val type = if (withType) " UInt64" else ""
	return when {
		isRoot && hasPkMappings -> "`_ver`$type"
		isRoot -> ""
		else -> "`_root_ver`$type"
	}
}

private fun resolveEngine(isRoot: Boolean, hasPkMappings: Boolean): String =
	if (isRoot && hasPkMappings) "ReplacingMergeTree(_ver)" else "MergeTree"

private fun buildOrderByContent(sqlIdentifiers: List<String>): String = when {
	sqlIdentifiers.isEmpty() -> "tuple()"
	sqlIdentifiers.size == 1 -> sqlIdentifiers.single()
	else -> "(${sqlIdentifiers.joinToString(", ")})"
}

private fun resolveOrderBy(meta: SourceMeta, isRoot: Boolean): String {
	val ids = meta.pkMappings
		.filter {
			if (isRoot) it.pkType == PKType.CURRENT
			else it.pkType == PKType.ROOT || it.pkType == PKType.LEVEL
		}
		.map { it.sqlIdentifier }
	return buildOrderByContent(ids)
}

/**
 * From the schema inspection, builds the queries to create the table (recursively for
 * children). Must respect SHOW CREATE TABLE syntax: the generated definition is later
 * compared to the live table definition to detect schema drift.
 */
fun translateCH(database: String, meta: SourceMeta, recursive: Boolean): List<String> {
	if (meta.simpleColumnMappings.isEmpty() && meta.pkMappings.isEmpty()) {
		throw IllegalStateException("Attempting to create table without columns")
	}
	return translateCHInternal(database, meta, recursive, isNodeRoot = true)
}

private fun translateCHInternal(
	database: String,
	meta: SourceMeta,
	recursive: Boolean,
	isNodeRoot: Boolean,
): List<String> {
	val createDefs = buildList {
		meta.pkMappings.forEach { add("${it.sqlIdentifier} ${it.chType}") }
		meta.simpleColumnMappings.forEach { add("${it.sqlIdentifier} ${toQualifiedType(it)}") }
		resolveVersionColumn(isNodeRoot, meta.pkMappings.isNotEmpty())
			.takeIf { it.isNotEmpty() }?.let { add(it) }
	}

	val hasPk = meta.pkMappings.isNotEmpty()
	val query = "CREATE TABLE $database.${meta.sqlTableName} ( ${createDefs.joinToString(", ")} ) " +
		"ENGINE = ${resolveEngine(isNodeRoot, hasPk)} ORDER BY ${resolveOrderBy(meta, isNodeRoot)}"

	return if (recursive) {
		listOf(query) + meta.children.flatMap { translateCHInternal(database, it, recursive, isNodeRoot = false) }
	} else {
		listOf(query)
	}
}

fun listTableNames(meta: SourceMeta): List<String> =
	listOf(meta.sqlTableName) + meta.children.flatMap(::listTableNames)

fun dropStreamTablesQueries(meta: SourceMeta): List<String> =
	listOf("DROP TABLE if exists ${meta.sqlTableName}") + meta.children.flatMap(::dropStreamTablesQueries)

fun toQualifiedType(mapping: ColumnMap): String {
	val modifiers = buildList {
		if (mapping.nullable) add("Nullable")
		if (mapping.lowCardinality) add("LowCardinality")
		if (mapping.nestedArray) add("Array")
	}
	val base = mapping.chType ?: "undefined type"
	return modifiers.fold(base) { acc, modifier -> "$modifier($acc)" }
}

private fun columnMapToColumn(col: ColumnMap): Column = Column(
	name = unescape(col.sqlIdentifier),
	type = toQualifiedType(col),
	isInSortingKey = false,
)

private fun pkMapToColumn(col: PkMap): Column = Column(
	name = unescape(col.sqlIdentifier),
	type = toQualifiedPkType(col),
	isInSortingKey = true,
)

private fun pkMapToColumnNonSorting(col: PkMap): Column = Column(
	name = unescape(col.sqlIdentifier),
	type = toQualifiedPkType(col),
	isInSortingKey = false,
)

private fun toQualifiedPkType(pk: PkMap): String {
	val modifiers = buildList {
		if (pk.nullable) add("Nullable")
		if (pk.lowCardinality) add("LowCardinality")
		if (pk.nestedArray) add("Array")
	}
	val base = pk.chType ?: "undefined type"
	return modifiers.fold(base) { acc, modifier -> "$modifier($acc)" }
}

private fun unescape(v: String): String = v.replace("`", "")

data class ColumnIntersections(
	val missing: List<Column>,
	val modified: List<ModifiedColumn>,
	val obsolete: List<Column>,
)

data class ModifiedColumn(val existing: Column, val newCol: Column)

fun getColumnsIntersections(existingCols: List<Column>, requiredCols: List<Column>): ColumnIntersections {
	val existingByName = existingCols.associateBy { it.name }
	val requiredByName = requiredCols.associateBy { it.name }

	val missing = requiredCols.filter { it.name !in existingByName }
	val modified = existingCols.mapNotNull { existing ->
		requiredByName[existing.name]
			?.takeIf { it.type != existing.type }
			?.let { ModifiedColumn(existing, it) }
	}
	val obsolete = existingCols.filter { it.name !in requiredByName }
	return ColumnIntersections(missing, modified, obsolete)
}

private fun checkPrimaryKeysConsistency(existingColumns: List<Column>, meta: SourceMeta) {
	val tablePks = existingColumns.filter { it.isInSortingKey }.map { it.name }
	val schemaPks = meta.pkMappings.map { it.prop }
	val newPks = schemaPks.filter { it !in tablePks }
		.map { "Could not add new PK property to $it in the table" }
	val removedPks = tablePks.filter { it !in schemaPks }
		.map { "Could not remove the PK property of $it in the table" }
	val errors = newPks + removedPks
	errors.forEach { logger.error { it } }
	if (errors.isNotEmpty()) {
		throw IllegalStateException("Could not update table because of key properties")
	}
}

fun updateSchema(meta: SourceMeta, ch: TargetConnection, existingTables: List<String>) {
	meta.children.forEach { child -> updateSchema(child, ch, existingTables) }

	val isRoot = meta.pkMappings.none { it.pkType == PKType.ROOT }
	if (unescape(meta.sqlTableName) !in existingTables) {
		translateCHInternal(ch.getDatabase(), meta, recursive = false, isNodeRoot = isRoot)
			.forEach { ch.runQuery(it) }
	}

	val existingColumns = ch.listColumns(unescape(meta.sqlTableName))
	val expectedColumns = buildExpectedColumns(meta, isRoot)

	val intersections = getColumnsIntersections(existingColumns, expectedColumns)

	if (isRoot) {
		checkPrimaryKeysConsistency(existingColumns, meta)
	}

	val added: List<Either<String, Unit>> = intersections.missing.map { elem ->
		ch.addColumn(meta.sqlTableName, elem)
			.mapLeft { "Could not create column ${it.newCol.name} ${it.newCol.type}" }
	}

	val updated: List<Either<String, Unit>> = intersections.modified.map { elem ->
		ch.updateColumn(meta.sqlTableName, elem.existing, elem.newCol)
			.mapLeft { "Could not update column ${it.newCol.name} from ${it.existing.type} to ${it.newCol.type}" }
	}

	val removed: List<Either<String, Unit>> = intersections.obsolete.map { elem ->
		ch.removeColumn(meta.sqlTableName, elem)
			.mapLeft { "Could not drop column ${it.existing.name} ${it.existing.type}" }
	}

	val errors = (added + updated + removed).mapNotNull { it.leftOrNull() }
	errors.forEach { logger.error { it } }
	if (errors.isNotEmpty()) {
		throw IllegalStateException("Could not update table")
	}
}

private fun buildExpectedColumns(meta: SourceMeta, isRoot: Boolean): List<Column> {
	val pkColumns = meta.pkMappings
		.filter {
			if (isRoot) it.pkType == PKType.CURRENT
			else it.pkType == PKType.ROOT || it.pkType == PKType.LEVEL
		}
		.map(::pkMapToColumn)

	// to handle properties added by "all_key_properties"
	val nonRootExtraPks = if (!isRoot) {
		meta.pkMappings
			.filter { it.pkType == PKType.CURRENT || it.pkType == PKType.PARENT }
			.map { pkMapToColumnNonSorting(it) }
	} else emptyList()

	val simpleColumns = meta.simpleColumnMappings.map(::columnMapToColumn)

	val versionColumn = if (!isRoot || (isRoot && meta.pkMappings.any { it.pkType == PKType.CURRENT })) {
		listOf(
			Column(
				name = if (isRoot) "_ver" else "_root_ver",
				type = "UInt64",
				isInSortingKey = false,
			),
		)
	} else emptyList()

	return pkColumns + nonRootExtraPks + simpleColumns + versionColumn
}
