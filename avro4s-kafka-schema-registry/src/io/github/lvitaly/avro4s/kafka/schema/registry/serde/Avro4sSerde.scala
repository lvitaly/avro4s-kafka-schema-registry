package io.github.lvitaly.avro4s.kafka.schema.registry.serde

import com.sksamuel.avro4s.{Decoder, Encoder, SchemaFor}
import org.apache.kafka.common.serialization.{Serde, Serdes}

import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient

/** Factory for creating Kafka [[org.apache.kafka.common.serialization.Serde]] instances for Avro messages using avro4s
  * and Confluent Schema Registry.
  */
object Avro4sSerde {

  /** Creates a Kafka [[org.apache.kafka.common.serialization.Serde]] for Avro messages of type `T` using the provided
    * Schema Registry client.
    *
    * @param schemaRegistry      the Schema Registry client to use for schema management
    * @param isKey               whether this serde is for message keys (default: `false`)
    * @param autoRegisterSchema  whether to automatically register the schema if not found (default: `true`)
    * @tparam T the type of the messages to serialize/deserialize, must have implicit [[com.sksamuel.avro4s.Encoder]],
    *           [[com.sksamuel.avro4s.Decoder]], and [[com.sksamuel.avro4s.SchemaFor]]
    * @return a Kafka [[org.apache.kafka.common.serialization.Serde]] for type `T`
    */
  def apply[T >: Null: Encoder: Decoder: SchemaFor](
    schemaRegistry    : SchemaRegistryClient,
    isKey             : Boolean = false,
    autoRegisterSchema: Boolean = true
  ): Serde[T] =
    Serdes.serdeFrom(
      new Avro4sSerializer[T](schemaRegistry, isKey, autoRegisterSchema),
      new Avro4sDeserializer[T](schemaRegistry)
    )
}
