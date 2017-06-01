package com.softwaremill.blog.tech.classification.elastic

import com.sksamuel.elastic4s.ElasticDsl.{mapping, stringField}
import com.sksamuel.elastic4s.mappings.MappingDefinition

object AbstractsIndex {

  val IndexName = "abstracts"
  val TypeName = "abstract"

  val Mapping: MappingDefinition = mapping(TypeName) fields(
    stringField("label") analyzer "english",
    stringField("text") analyzer "english"
  )
}
