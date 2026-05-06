package com.biron.singerTargetClickhouse

import com.biron.singer.core.domain.JsonSchema
import com.biron.singer.core.domain.typeNullable
import io.github.oshai.kotlinlogging.KotlinLogging
import java.security.MessageDigest

private val logger = KotlinLogging.logger {}

const val NESTED_SUB_OBJECT_SEPARATOR = "\$%€£"

enum class PKType { ROOT, PARENT, CURRENT, LEVEL }

typealias ValueExtractor = (Any?) -> Any?

data class ColumnMap(
	val prop: String?,
	val valueExtractor: ValueExtractor,
	val sqlIdentifier: String,
	val chType: String?,
	val schemaType: String?,
	val typeFormat: String?,
	val nullable: Boolean,
	val lowCardinality: Boolean,
	val nestedArray: Boolean,
)

data class PkMap(
	val prop: String,
	val valueExtractor: ValueExtractor,
	val sqlIdentifier: String,
	val chType: String?,
	val schemaType: String?,
	val typeFormat: String?,
	val nullable: Boolean,
	val lowCardinality: Boolean,
	val nestedArray: Boolean,
	val pkType: PKType,
)

data class SourceMeta(
	val prop: String,
	val children: List<SourceMeta>,
	val pkMappings: List<PkMap>,
	val simpleColumnMappings: List<ColumnMap>,
	val sqlTableName: String,
	val cleaningColumn: String? = null,
)

data class JsonSchemaInspectorContext(
	val alias: String,
	val schema: JsonSchema,
	val keyProperties: List<String>,
	val subtableSeparator: String = "__",
	val parentCtx: JsonSchemaInspectorContext? = null,
	val level: Int = 0,
	val tableName: String = defaultTableName(alias, subtableSeparator, parentCtx),
	val cleaningColumn: String? = null,
	val allKeyProperties: SchemaKeyProperties = SchemaKeyProperties.empty,
) {
	fun isRoot(): Boolean = parentCtx == null
	val rootCtx: JsonSchemaInspectorContext by lazy { generateSequence(this) { it.parentCtx }.last() }

	companion object {
		fun defaultTableName(alias: String, subtableSeparator: String, parentCtx: JsonSchemaInspectorContext?): String =
			"${parentCtx?.let { "${it.tableName}$subtableSeparator" }.orEmpty()}$alias"
	}
}

private fun formatLevelIndexColumn(level: Int): String = "_level_${level}_index"
fun formatRootPKColumn(prop: String): String = "_root_$prop"
private fun formatParentPKColumn(prop: String): String = "_parent_$prop"

fun escapeIdentifier(id: String, subtableSeparator: String = "__"): String =
	id.replace(NESTED_SUB_OBJECT_SEPARATOR, subtableSeparator)
		.let {
			if (it.length > 64) {
				val uid = sha1Hex(it).substring(0, 10)
				it.substring(0, 64 - uid.length - 27) + uid + it.substring(it.length - 27)
			} else it
		}
		.let { "`$it`" }

private fun sha1Hex(input: String): String =
	MessageDigest.getInstance("SHA-1").digest(input.toByteArray())
		.joinToString("") { "%02x".format(it) }

fun buildMeta(ctx: JsonSchemaInspectorContext): SourceMeta {
	val (simpleColumnMappings, children) = buildMetaProps(ctx)
	return SourceMeta(
		prop = ctx.alias,
		sqlTableName = escapeIdentifier(ctx.tableName, ctx.subtableSeparator),
		pkMappings = buildMetaPkProps(ctx),
		cleaningColumn = ctx.cleaningColumn,
		simpleColumnMappings = simpleColumnMappings,
		children = children,
	)
}

private data class MetaProps(
	val simpleColumnMappings: List<ColumnMap>,
	val children: List<SourceMeta>,
)

private fun buildMetaPkProps(ctx: JsonSchemaInspectorContext): List<PkMap> = buildList {
	// _root_X (only on non-root nodes)
	ctx.rootCtx.takeIf { it != ctx }?.keyProperties?.forEach { prop ->
		add(buildMetaPkProp(prop, ctx.rootCtx, PKType.ROOT, ::formatRootPKColumn))
	}
	// _parent_X (if parent has all_key_properties non-empty)
	ctx.parentCtx?.keyProperties?.takeIf { ctx.parentCtx.allKeyProperties.props.isNotEmpty() }?.forEach { prop ->
		add(buildMetaPkProp(prop, ctx.parentCtx, PKType.PARENT, ::formatParentPKColumn))
	}
	// Current-level key properties
	ctx.keyProperties.forEach { prop ->
		add(buildMetaPkProp(prop, ctx, PKType.CURRENT))
	}
	// level_N_index
	repeat(ctx.level) { level ->
		val prop = formatLevelIndexColumn(level)
		add(
			PkMap(
				prop = prop,
				valueExtractor = buildValueExtractor(prop),
				sqlIdentifier = escapeIdentifier(prop, ctx.subtableSeparator),
				chType = "Int32",
				schemaType = null,
				typeFormat = null,
				nullable = false,
				lowCardinality = false,
				nestedArray = false,
				pkType = PKType.LEVEL,
			)
		)
	}
}

private fun buildMetaPkProp(
	prop: String,
	ctx: JsonSchemaInspectorContext,
	pkType: PKType,
	fieldFormatter: ((String) -> String)? = null,
): PkMap {
	val colType = getSimpleColumnType(ctx, prop)
	return PkMap(
		prop = prop,
		valueExtractor = buildValueExtractor(prop),
		sqlIdentifier = escapeIdentifier(fieldFormatter?.invoke(prop) ?: prop, ctx.subtableSeparator),
		chType = colType?.chType,
		schemaType = colType?.schemaType,
		typeFormat = colType?.typeFormat,
		nullable = false,
		lowCardinality = false,
		nestedArray = false,
		pkType = pkType,
	)
}

private fun buildValueExtractor(prop: String?): ValueExtractor {
	if (prop == null) return { data -> data }
	val parts = prop.split(NESTED_SUB_OBJECT_SEPARATOR)
	if (parts.size == 1) {
		val onlyPart = parts[0]
		return { data -> (data as? Map<*, *>)?.get(onlyPart) }
	}
	return { data ->
		parts.fold(data) { acc, part -> (acc as? Map<*, *>)?.get(part) }
	}
}

private fun buildMetaProps(ctx: JsonSchemaInspectorContext): MetaProps =
	if ("object" in ctx.schema.type) buildObjectMetaProps(ctx)
	else buildScalarMetaProps(ctx)

private fun buildObjectMetaProps(ctx: JsonSchemaInspectorContext): MetaProps =
	(ctx.schema.properties ?: emptyMap())
		.filterKeys { it !in ctx.keyProperties }
		.entries
		.fold(MetaProps(emptyList(), emptyList())) { acc, (key, propDef) ->
			val propDefTypes = propDef.type
			when {
				"object" in propDefTypes -> {
					val flattened = flattenNestedObject(propDef, key, ctx)
					acc.copy(
						simpleColumnMappings = acc.simpleColumnMappings + flattened.simpleColumnMappings,
						children = acc.children + flattened.children,
					)
				}

				"array" in propDefTypes && propDef.format != "nested" -> {
					if (ctx.rootCtx.keyProperties.isEmpty() && ctx.rootCtx.allKeyProperties.props.isEmpty())
						throwError(ctx, "$key refused: array child with no root key properties")
					acc.copy(children = acc.children + createSubTable(propDef, key, ctx))
				}

				else -> {
					val colType = getSimpleColumnType(ctx, key)
					if (colType != null) {
						acc.copy(
							simpleColumnMappings = acc.simpleColumnMappings + ColumnMap(
								prop = key,
								valueExtractor = buildValueExtractor(key),
								sqlIdentifier = escapeIdentifier(key, ctx.subtableSeparator),
								chType = colType.chType,
								schemaType = colType.schemaType,
								typeFormat = colType.typeFormat,
								nullable = colType.nullable,
								lowCardinality = colType.lowCardinality,
								nestedArray = colType.nestedArray,
							),
						)
					} else {
						logger.warn { "'${ctx.alias}': '$key': could not be registered (type '${propDef.type}' unrecognized)" }
						acc
					}
				}
			}
		}

private fun buildScalarMetaProps(ctx: JsonSchemaInspectorContext): MetaProps {
	if (ctx.schema.type.isEmpty()) return MetaProps(emptyList(), emptyList())
	val colType = getSimpleColumnType(ctx, null) ?: return MetaProps(emptyList(), emptyList())
	return MetaProps(
		simpleColumnMappings = listOf(
			ColumnMap(
				prop = null,
				valueExtractor = buildValueExtractor(null),
				sqlIdentifier = escapeIdentifier("value", ctx.subtableSeparator),
				chType = colType.chType,
				schemaType = colType.schemaType,
				typeFormat = colType.typeFormat,
				nullable = ctx.schema.typeNullable,
				lowCardinality = false,
				nestedArray = false,
			),
		),
		children = emptyList(),
	)
}

private fun flattenNestedObject(propDef: JsonSchema, key: String, ctx: JsonSchemaInspectorContext): MetaProps {
	val nullable = propDef.typeNullable
	val mergedProperties: Map<String, JsonSchema> = (propDef.properties ?: emptyMap())
		.map { (nestedKey, nested) ->
			"$key$NESTED_SUB_OBJECT_SEPARATOR$nestedKey" to
					nested.copy(type = if (nullable) makeNullable(nested.type) else nested.type)
		}
		.toMap()

	val nestedSchema = JsonSchema(type = listOf("object"), properties = mergedProperties)
	return buildMetaProps(
		JsonSchemaInspectorContext(
			alias = ctx.alias,
			schema = nestedSchema,
			keyProperties = emptyList(),
			subtableSeparator = ctx.subtableSeparator,
			parentCtx = ctx,
			level = ctx.level,
			tableName = ctx.tableName,
		),
	)
}

private fun createSubTable(propDef: JsonSchema, key: String, ctx: JsonSchemaInspectorContext): SourceMeta {
	val itemSchema = propDef.items ?: JsonSchema(type = listOf("string"))
	val childAllKeyProps = ctx.allKeyProperties.children[key]
	return buildMeta(
		JsonSchemaInspectorContext(
			alias = key,
			schema = itemSchema,
			keyProperties = childAllKeyProps?.props ?: emptyList(),
			subtableSeparator = ctx.subtableSeparator,
			parentCtx = ctx,
			level = ctx.level + 1,
			allKeyProperties = childAllKeyProps ?: SchemaKeyProperties.empty,
		),
	)
}

private data class SimpleColumnType(
	val chType: String?,
	val schemaType: String?,
	val typeFormat: String?,
	val nullable: Boolean,
	val lowCardinality: Boolean,
	val nestedArray: Boolean,
)

private fun getSimpleColumnType(ctx: JsonSchemaInspectorContext, key: String?): SimpleColumnType? {
	var propDef = if (key != null) ctx.schema.properties?.get(key) else ctx.schema
	if (propDef == null) {
		throwError(ctx, "Key '$key' does not match any usable prop in schema props '${ctx.schema.properties}'")
	}
	var nestedArray = false
	if (propDef.format == "nested" && "array" in propDef.type) {
		propDef = propDef.items ?: return null
		nestedArray = true
	}
	val type = propDef.type.firstOrNull { it != "null" }
	val chType = getSimpleColumnSqlType(ctx, propDef, key) ?: return null
	return SimpleColumnType(
		chType = chType,
		schemaType = type,
		typeFormat = propDef.format,
		nullable = propDef.typeNullable,
		lowCardinality = propDef.lowCardinality == true,
		nestedArray = nestedArray,
	)
}

fun getSimpleColumnSqlType(ctx: JsonSchemaInspectorContext, propDef: JsonSchema, key: String? = null): String? {
	val type = propDef.type.firstOrNull { it != "null" }
	val format = propDef.format
	return when (type) {
		"string" -> when (format) {
			"date", "x-excel-date" -> "Date"
			"date-time" -> "DateTime"
			"date-time64" -> "DateTime64"
			"uuid" -> "UUID"
			else -> "String"
		}

		"integer" -> when (format) {
			null -> "Int64"
			"int128" -> "Int128"
			"int64" -> "Int64"
			"int32" -> "Int32"
			"int16" -> "Int16"
			"int8" -> "Int8"
			else -> throwError(ctx, "$key: unsupported integer format [$format]")
		}

		"number" -> when (format) {
			null -> "Decimal(${propDef.precision ?: 16}, ${propDef.decimals ?: 2})"
			"float64" -> "Float64"
			"float32" -> "Float32"
			else -> throwError(ctx, "$key: unsupported number format [$format]")
		}

		"boolean" -> when (format) {
			null -> "UInt8"
			else -> throwError(ctx, "$key: unsupported number format [$format]")
		}

		else -> null
	}
}

private fun makeNullable(types: List<String>): List<String> = when {
	types.isEmpty() -> emptyList()
	"null" in types -> types
	else -> types + "null"
}

private fun throwError(ctx: JsonSchemaInspectorContext, msg: String, childAlias: String? = null): Nothing {
	var current = ctx
	var alias = "${ctx.alias}${childAlias?.let { ".$it" } ?: ""}"
	while (current.parentCtx != null) {
		current = current.parentCtx!!
		alias = "${current.alias}.$alias"
	}
	logger.error { "$alias: $msg" }
	error("$alias: $msg")
}
