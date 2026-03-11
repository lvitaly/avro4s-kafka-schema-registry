package io.github.lvitaly.avro4s.kafka.schema.registry.serde

import com.sksamuel.avro4s
import com.sksamuel.avro4s.{AvroOutputStream, Encoder, SchemaFor}
import org.apache.kafka.common.errors.SerializationException
import org.apache.kafka.common.serialization.Serializer

import java.io.{ByteArrayOutputStream, IOException}
import java.nio.ByteBuffer
import io.confluent.kafka.schemaregistry.avro.AvroSchema
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException

/** Kafka [[org.apache.kafka.common.serialization.Serializer]] for Avro messages using avro4s and Confluent Schema Registry.
  *
  * @param schemaRegistry      the Schema Registry client to use for schema management
  * @param isKey               whether this serializer is for message keys (default: `false`)
  * @param autoRegisterSchema  whether to automatically register the schema if not found (default: `true`)
  * @tparam T the type of the messages to serialize, must have an implicit [[com.sksamuel.avro4s.Encoder]]
  *           and [[com.sksamuel.avro4s.SchemaFor]]
  */
class Avro4sSerializer[T: Encoder: SchemaFor](
  schemaRegistry    : SchemaRegistryClient,
  isKey             : Boolean = false,
  autoRegisterSchema: Boolean = true
) extends Serializer[T] {

  private val subjectSuffix = if (isKey) "-key" else "-value"

  private val writerSchema = avro4s.AvroSchema[T]
  private val parsedSchema = new AvroSchema(writerSchema)

  private val avroOutputStreamBuilder = AvroOutputStream.binary[T]

  override def serialize(topic: String, data: T): Array[Byte] =
    if (data == null) null
    else {
      val id = getSchemaId(subject = topic + subjectSuffix)

      try {
        val out = new ByteArrayOutputStream()
        out.write(0)
        out.write(ByteBuffer.allocate(4).putInt(id).array())

        val writer = avroOutputStreamBuilder.to(out).build()
        writer.write(data)
        writer.close()
        out.toByteArray
      } catch {
        case e @ (_: RuntimeException | _: IOException) =>
          throw new SerializationException("Error serializing Avro message", e)
      }
    }

  private def getSchemaId(subject: String): Int =
    try
      if (autoRegisterSchema)
        this.schemaRegistry.register(subject, parsedSchema)
      else
        this.schemaRegistry.getId(subject, parsedSchema)
    catch {
      case e @ (_: RestClientException | _: IOException) =>
        val action = if (autoRegisterSchema) "registering" else "retrieving"
        throw new SerializationException(s"Error $action Avro schema: " + writerSchema, e)
    }

}
