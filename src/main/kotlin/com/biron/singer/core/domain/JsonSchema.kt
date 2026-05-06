package com.biron.singer.core.domain

import com.fasterxml.jackson.annotation.JsonFormat
import com.fasterxml.jackson.annotation.JsonIgnoreProperties
import com.fasterxml.jackson.annotation.JsonInclude
import com.fasterxml.jackson.databind.PropertyNamingStrategies
import com.fasterxml.jackson.databind.annotation.JsonNaming

@JsonNaming(PropertyNamingStrategies.LowerCamelCaseStrategy::class)
@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonIgnoreProperties(ignoreUnknown = true)
data class JsonSchema(
	@param:JsonFormat(with = [JsonFormat.Feature.ACCEPT_SINGLE_VALUE_AS_ARRAY])
	@param:JsonInclude(JsonInclude.Include.NON_EMPTY)
	val type: List<String> = emptyList(),
	val format: String? = null,
	val precision: Int? = null,
	val decimals: Int? = null,
	val lowCardinality: Boolean? = null,
	val properties: Map<String, JsonSchema>? = null,
	val items: JsonSchema? = null,
)

val JsonSchema.typeNullable get() = "null" in type
