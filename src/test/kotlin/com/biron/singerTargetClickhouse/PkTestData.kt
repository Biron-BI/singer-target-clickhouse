package com.biron.singerTargetClickhouse

internal val id = PkMap(
	prop = "id",
	sqlIdentifier = "`id`",
	chType = "UInt32",
	valueExtractor = { (it as Map<*, *>)["id"]?.toString()?.toIntOrNull() },
	schemaType = null,
	typeFormat = null,
	nullable = false,
	lowCardinality = false,
	nestedArray = false,
	pkType = PKType.CURRENT,
)

private val rootId = PkMap(
	prop = "_root_id",
	sqlIdentifier = "`_root_id`",
	chType = "UInt32",
	valueExtractor = { (it as Map<*, *>)["_root_id"]?.toString()?.toIntOrNull() },
	schemaType = null,
	typeFormat = null,
	nullable = false,
	lowCardinality = false,
	nestedArray = false,
	pkType = PKType.ROOT,
)

private val name = ColumnMap(
	prop = "name",
	sqlIdentifier = "`name`",
	chType = "String",
	valueExtractor = { (it as? Map<*, *>)?.get("name") },
	schemaType = null,
	typeFormat = null,
	nullable = true,
	lowCardinality = false,
	nestedArray = false,
)

internal val validColumn = ColumnMap(
	prop = "valid",
	sqlIdentifier = "`valid`",
	chType = "UInt8",
	valueExtractor = { (it as? Map<*, *>)?.get("valid") },
	schemaType = "boolean",
	typeFormat = null,
	nullable = false,
	lowCardinality = false,
	nestedArray = false,
)

internal val idAsColumn = ColumnMap(
	prop = "id",
	sqlIdentifier = "`id`",
	chType = "Int32",
	valueExtractor = { (it as? Map<*, *>)?.get("id") },
	schemaType = null,
	typeFormat = null,
	nullable = false,
	lowCardinality = false,
	nestedArray = false,
)

private fun levelColumn(level: Int) = PkMap(
	prop = "_level_${level}_index",
	sqlIdentifier = "`_level_${level}_index`",
	chType = "Int32",
	valueExtractor = { throw IllegalStateException("level extractor should never be called") },
	schemaType = null,
	typeFormat = null,
	nullable = false,
	lowCardinality = false,
	nestedArray = false,
	pkType = PKType.LEVEL,
)

internal val simpleMeta = SourceMeta(
	prop = "order",
	sqlTableName = "`order`",
	pkMappings = emptyList(),
	simpleColumnMappings = listOf(idAsColumn, name),
	children = emptyList(),
)

internal val metaWithPKAndChildren = SourceMeta(
	prop = "order",
	sqlTableName = "`order`",
	pkMappings = listOf(id),
	simpleColumnMappings = listOf(name),
	children = listOf(
		SourceMeta(
			prop = "tags",
			sqlTableName = "`order__tags`",
			pkMappings = listOf(rootId, levelColumn(0)),
			simpleColumnMappings = listOf(name),
			children = listOf(
				SourceMeta(
					prop = "values",
					sqlTableName = "`order__tags__values`",
					pkMappings = listOf(rootId, levelColumn(0), levelColumn(1)),
					simpleColumnMappings = listOf(name),
					children = emptyList(),
				),
			),
		),
	),
)

internal val abort: (Throwable) -> Unit = { err -> throw err }

/**
 * Test helper: round-trip an in-memory [data] through the stream's real [StreamReader]
 * (serialize to JSON, re-parse). Uses the same decoder that the production hot path uses,
 * so tests can't silently diverge from it.
 */
internal fun mapToRow(meta: SourceMeta, data: Any?, translateValues: Boolean = false): RecordRow {
	val reader = StreamReader.from(meta, translateValues)
	val mapper = com.fasterxml.jackson.module.kotlin.jsonMapper {
		addModule(com.fasterxml.jackson.module.kotlin.kotlinModule())
	}
	val json = mapper.writeValueAsString(data)
	mapper.factory.createParser(json).apply { codec = mapper }.use { p ->
		p.nextToken()
		return reader.read(p)
	}
}

private val valueAsSelf = ColumnMap(
	prop = null,
	sqlIdentifier = "`value`",
	chType = "String",
	valueExtractor = { it },
	schemaType = null,
	typeFormat = null,
	nullable = false,
	lowCardinality = false,
	nestedArray = false,
)

internal val metaWithNestedValueArray = SourceMeta(
	prop = "audits",
	sqlTableName = "`audits`",
	pkMappings = emptyList(),
	simpleColumnMappings = emptyList(),
	children = listOf(
		SourceMeta(
			prop = "events",
			sqlTableName = "`audits__events`",
			pkMappings = listOf(levelColumn(0)),
			simpleColumnMappings = emptyList(),
			children = listOf(
				SourceMeta(
					prop = "previous_value",
					sqlTableName = "`audits__events__previous_value`",
					pkMappings = listOf(levelColumn(0), levelColumn(1)),
					simpleColumnMappings = listOf(valueAsSelf),
					children = emptyList(),
				),
			),
		),
	),
)
