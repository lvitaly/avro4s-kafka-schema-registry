package io.github.lvitaly.avro4s.kafka.schema.registry.serde

import com.sksamuel.avro4s.{AvroInputStream, Decoder, SchemaFor}
import org.apache.kafka.common.errors.SerializationException
import org.apache.kafka.common.serialization.Deserializer

import java.io.{ByteArrayInputStream, IOException}
import java.nio.ByteBuffer
import io.confluent.kafka.schemaregistry.avro.AvroSchema
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException

/** Kafka [[org.apache.kafka.common.serialization.Deserializer]] for Avro messages using avro4s and Confluent
  * Schema Registry.
  *
  * @param schemaRegistry the Schema Registry client to use for schema retrieval
  * @tparam T the type of the messages to deserialize, must have an implicit [[com.sksamuel.avro4s.Decoder]]
  *           and [[com.sksamuel.avro4s.SchemaFor]]
  */
class Avro4sDeserializer[T >: Null: Decoder: SchemaFor](schemaRegistry: SchemaRegistryClient) extends Deserializer[T] {

  private val avroInputStreamBuilder = AvroInputStream.binary[T]

  override def deserialize(topic: String, data: Array[Byte]): T =
    if (data == null) null
    else {
      var id = -1

      try {
        val buffer = ByteBuffer.wrap(data)
        if (buffer.get() != 0) sys.error("Unknown magic byte!")

        id = buffer.getInt
        val schema = schemaRegistry.getSchemaById(id) match {
          case s: AvroSchema => s.rawSchema()
          case _             => sys.error("Failed to get avro schema by id " + id)
        }

        val start  = buffer.position() + buffer.arrayOffset()
        val length = buffer.limit() - 1 - 4
        val in     = new ByteArrayInputStream(buffer.array, start, length)

        val reader = avroInputStreamBuilder.from(in).build(schema)
        val it     = reader.iterator
        if (it.hasNext) it.next() else sys.error("Can not read bytes from input stream")
      } catch {
        case e @ (_: RuntimeException | _: IOException) =>
          throw new SerializationException("Error deserializing Avro message for id " + id, e)
        case e: RestClientException                     =>
          throw new SerializationException("Error retrieving Avro schema for id " + id, e)
      }
    }

}
