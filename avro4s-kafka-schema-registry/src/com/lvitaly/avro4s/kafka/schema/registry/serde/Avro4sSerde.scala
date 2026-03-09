package com.lvitaly.avro4s.kafka.schema.registry.serde

import com.sksamuel.avro4s.{Decoder, Encoder, SchemaFor}
import org.apache.kafka.common.serialization.{Serde, Serdes}

import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient

object Avro4sSerde {
  def apply[T >: Null: Encoder: Decoder: SchemaFor](
      schemaRegistry: SchemaRegistryClient,
      isKey: Boolean = false,
      autoRegisterSchema: Boolean = true
  ): Serde[T] =
    Serdes.serdeFrom(
      new Avro4sSerializer[T](schemaRegistry, isKey, autoRegisterSchema),
      new Avro4sDeserializer[T](schemaRegistry)
    )
}