package com.biron.singerTargetClickhouse

import io.github.oshai.kotlinlogging.KotlinLogging

private val logger = KotlinLogging.logger {}

/**
 * From the schema inspection, builds the queries to create the table (recursively for
 * children). Must respect SHOW CREATE TABLE syntax: the generated definition is later
 * compared to the live table definition to detect schema drift.
 */
fun translateCH(database: String, meta: SourceMeta, recursive: Boolean): List<String> {
	if (meta.simpleColumnMappings.isEmpty() && meta.pkMappings.isEmpty()) {
		error("Attempting to create table without columns")
	}
	return translateCHInternal(database, meta, recursive, isNodeRoot = true)
}

fun dropStreamTablesQueries(meta: SourceMeta): List<String> =
	listOf("DROP TABLE IF EXISTS ${meta.sqlTableName}") + meta.children.flatMap(::dropStreamTablesQueries)

fun toQualifiedType(mapping: ColumnMap): String =
	wrapModifiers(mapping.chType, mapping.nullable, mapping.lowCardinality, mapping.nestedArray)

fun updateSchema(meta: SourceMeta, ch: TargetConnection, existingTables: List<String>) {
	meta.children.forEach { child -> updateSchema(child, ch, existingTables) }

	val isRoot = meta.pkMappings.none { it.pkType == PKType.ROOT }
	createTableIfMissing(meta, ch, existingTables, isRoot)

	val existingColumns = ch.listColumns(unescape(meta.sqlTableName))
	val partitionKeyColumns = existingColumns.filter { it.isInPartitionKey }.map { it.name }.toSet()
	val expectedColumns = buildExpectedColumns(meta, isRoot, partitionKeyColumns)

	if (isRoot) {
		checkPrimaryKeysConsistency(existingColumns, meta)
	}

	val errors = applyColumnDeltas(meta, ch, getColumnsIntersections(existingColumns, expectedColumns))
	errors.forEach { logger.error { it } }
	if (errors.isNotEmpty()) {
		error("Could not update table")
	}
}

/**
 * Partition key of an existing root table, null when it is not partitioned. ClickHouse cannot change
 * it, so the table wins over the SCHEMA message: `partition_by` is ignored with a warning on an
 * unpartitioned table, and refused when it differs from the table's key. Keys are normalized by
 * ClickHouse, so formatting differences (e.g. backquotes) are not reported.
 */
fun existingPartitionKey(meta: SourceMeta, ch: TargetConnection): String? {
	val table = unescape(meta.sqlTableName)
	val existing = ch.getPartitionKey(table)
	val partitionBy = meta.partitionBy
	if (existing.isEmpty()) {
		partitionBy?.let {
			logger.warn { "[${meta.prop}]: table $table is not partitioned, partition_by is ignored (expected PARTITION BY ${it.expression})" }
		}
		return null
	}
	if (partitionBy != null && !isSpellingOf(existing, partitionBy) &&
		ch.formatExpression(existing) != ch.formatExpression(partitionBy.expression)
	) {
		error(
			"[${meta.prop}]: table $table is partitioned by [$existing] but partition_by requires [${partitionBy.expression}]. " +
					"ClickHouse cannot change the partition key of an existing table (see docs/partitioning.md)",
		)
	}
	return existing
}

/**
 * The generated key always quotes its column, while ClickHouse prints most column names without quotes
 * (even `true` or `false`). The unquoted spelling cannot denote another key: ClickHouse refuses constant
 * partition keys, so a name in a stored key is always a column.
 */
private fun isSpellingOf(existing: String, partitionBy: PartitionBy): Boolean =
	existing == partitionBy.expression || existing == partitionBy.expression.replace("`", "")

private fun translateCHInternal(
	database: String,
	meta: SourceMeta,
	recursive: Boolean,
	isNodeRoot: Boolean,
): List<String> {
	val hasPkMappings = meta.pkMappings.isNotEmpty()

	val createDefs = buildList {
		meta.pkMappings.forEach { add("${it.sqlIdentifier} ${it.chType}") }
		val partitionKeyColumns = setOfNotNull(meta.partitionBy?.column)
		meta.simpleColumnMappings.forEach { add("${it.sqlIdentifier} ${qualifiedType(it, partitionKeyColumns)}") }
		resolveVersionColumn(isNodeRoot, hasPkMappings)
			.takeIf { it.isNotEmpty() }?.let { add(it) }
	}

	val partitionClause = meta.partitionBy?.let { " PARTITION BY ${it.expression}" }.orEmpty()
	val query = "CREATE TABLE $database.${meta.sqlTableName} ( ${createDefs.joinToString(", ")} ) " +
			"ENGINE = ${resolveEngine(isNodeRoot, hasPkMappings)}$partitionClause ORDER BY ${resolveOrderBy(meta, isNodeRoot)}"

	return if (recursive) {
		listOf(query) + meta.children.flatMap { translateCHInternal(database, it, true, false) }
	} else {
		listOf(query)
	}
}

private fun resolveVersionColumn(isRoot: Boolean, hasPkMappings: Boolean): String = when {
	isRoot && hasPkMappings -> "`_ver` UInt64"
	isRoot -> ""
	else -> "`_root_ver` UInt64"
}

private fun resolveEngine(isRoot: Boolean, hasPkMappings: Boolean): String =
	if (isRoot && hasPkMappings) "ReplacingMergeTree(_ver)" else "MergeTree"

private fun resolveOrderBy(meta: SourceMeta, isRoot: Boolean): String {
	val ids = meta.pkMappings
		.filter {
			if (isRoot) it.pkType == PKType.CURRENT
			else it.pkType == PKType.ROOT || it.pkType == PKType.LEVEL
		}
		.map { it.sqlIdentifier }
	return buildOrderByContent(ids)
}

private fun buildOrderByContent(sqlIdentifiers: List<String>): String = when {
	sqlIdentifiers.isEmpty() -> "tuple()"
	sqlIdentifiers.size == 1 -> sqlIdentifiers.single()
	else -> "(${sqlIdentifiers.joinToString(", ")})"
}

private fun toQualifiedType(pk: PkMap): String =
	wrapModifiers(pk.chType, pk.nullable, pk.lowCardinality, pk.nestedArray)

private fun wrapModifiers(chType: String?, nullable: Boolean, lowCardinality: Boolean, nestedArray: Boolean): String {
	val modifiers = buildList {
		if (nullable) add("Nullable")
		if (lowCardinality) add("LowCardinality")
		if (nestedArray) add("Array")
	}
	val base = chType ?: "undefined type"
	return modifiers.fold(base) { acc, modifier -> "$modifier($acc)" }
}

/**
 * Like key columns, the columns of the partition key are never Nullable: ClickHouse refuses a Nullable
 * partition key. They come from `partition_by` when creating a table, from the table itself when updating it.
 */
private fun qualifiedType(col: ColumnMap, partitionKeyColumns: Set<String>): String =
	toQualifiedType(if (unescape(col.sqlIdentifier) in partitionKeyColumns) col.copy(nullable = false) else col)

private fun columnMapToColumn(col: ColumnMap, partitionKeyColumns: Set<String>): Column = Column(
	name = unescape(col.sqlIdentifier),
	type = qualifiedType(col, partitionKeyColumns),
	isInSortingKey = false,
)

private fun pkMapToColumn(col: PkMap, isInSortingKey: Boolean): Column = Column(
	name = unescape(col.sqlIdentifier),
	type = toQualifiedType(col),
	isInSortingKey = isInSortingKey,
)

private fun unescape(v: String): String = v.replace("`", "")

data class ColumnIntersections(
	val missing: List<Column>,
	val modified: List<ModifiedColumn>,
	val obsolete: List<Column>,
)

data class ModifiedColumn(val existing: Column, val newCol: Column)

private fun getColumnsIntersections(existingCols: List<Column>, requiredCols: List<Column>): ColumnIntersections {
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
		error("Could not update table because of key properties")
	}
}

private fun createTableIfMissing(meta: SourceMeta, ch: TargetConnection, existingTables: List<String>, isRoot: Boolean) {
	if (unescape(meta.sqlTableName) in existingTables) return
	translateCHInternal(ch.getDatabase(), meta, recursive = false, isNodeRoot = isRoot)
		.forEach { ch.runQuery(it) }
}

private fun applyColumnDeltas(meta: SourceMeta, ch: TargetConnection, intersections: ColumnIntersections): List<String> {
	val added = intersections.missing.map { col ->
		ch.addColumn(meta.sqlTableName, col)
			.mapLeft { "Could not create column ${it.newCol.name} ${it.newCol.type}" }
	}
	val updated = intersections.modified.map { mod ->
		ch.updateColumn(meta.sqlTableName, mod.existing, mod.newCol)
			.mapLeft { "Could not update column ${it.newCol.name} from ${it.existing.type} to ${it.newCol.type}" }
	}
	val removed = intersections.obsolete.map { col ->
		ch.removeColumn(meta.sqlTableName, col)
			.mapLeft { "Could not drop column ${it.existing.name} ${it.existing.type}" }
	}
	return (added + updated + removed).mapNotNull { it.leftOrNull() }
}

private fun buildExpectedColumns(meta: SourceMeta, isRoot: Boolean, partitionKeyColumns: Set<String>): List<Column> {
	val sortingPks = meta.pkMappings
		.filter { if (isRoot) it.pkType == PKType.CURRENT else it.pkType == PKType.ROOT || it.pkType == PKType.LEVEL }
		.map { pkMapToColumn(it, isInSortingKey = true) }

	// `all_key_properties` may surface CURRENT/PARENT pk columns on child tables that aren't in the sorting key.
	val nonSortingPks = if (isRoot) emptyList() else meta.pkMappings
		.filter { it.pkType == PKType.CURRENT || it.pkType == PKType.PARENT }
		.map { pkMapToColumn(it, isInSortingKey = false) }

	val simpleColumns = meta.simpleColumnMappings.map { columnMapToColumn(it, partitionKeyColumns) }
	val versionColumn = buildVersionColumn(meta, isRoot)

	return sortingPks + nonSortingPks + simpleColumns + versionColumn
}

private fun buildVersionColumn(meta: SourceMeta, isRoot: Boolean): List<Column> {
	val needs = !isRoot || meta.pkMappings.any { it.pkType == PKType.CURRENT }
	if (!needs) return emptyList()
	val name = if (isRoot) "_ver" else "_root_ver"
	return listOf(Column(name = name, type = "UInt64", isInSortingKey = false))
}
