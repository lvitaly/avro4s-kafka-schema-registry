package com.lvitaly.avro4s.kafka.schema.registry.serde

import com.sksamuel.avro4s
import com.sksamuel.avro4s.{AvroOutputStream, Encoder, SchemaFor}
import org.apache.kafka.common.errors.SerializationException
import org.apache.kafka.common.serialization.Serializer

import java.io.{ByteArrayOutputStream, IOException}
import java.nio.ByteBuffer
import io.confluent.kafka.schemaregistry.ParsedSchema
import io.confluent.kafka.schemaregistry.avro.AvroSchema
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException

class Avro4sSerializer[T: Encoder: SchemaFor](
    schemaRegistry: SchemaRegistryClient,
    isKey: Boolean = false,
    autoRegisterSchema: Boolean = true
) extends Serializer[T] {
  
  private val subjectSuffix = if (isKey) "-key" else "-value"

  override def serialize(topic: String, data: T): Array[Byte] =
    if (data == null) null else {
      var restClientErrorMsg = ""

      val subject      = topic + subjectSuffix
      val writerSchema = avro4s.AvroSchema[T]

      try {
        val parsedSchema = new AvroSchema(writerSchema).asInstanceOf[ParsedSchema]

        val id: Int = if (autoRegisterSchema) {
          restClientErrorMsg = "Error registering Avro schema: "
          this.schemaRegistry.register(subject, parsedSchema)
        } else {
          restClientErrorMsg = "Error retrieving Avro schema: "
          this.schemaRegistry.getId(subject, parsedSchema)
        }

        val out = new ByteArrayOutputStream()
        out.write(0)
        out.write(ByteBuffer.allocate(4).putInt(id).array())

        val writer = AvroOutputStream.binary[T].to(out).build()
        writer.write(data)
        writer.close()
        out.toByteArray
      } catch {
        case e @ (_: RuntimeException | _: IOException) =>
          throw new SerializationException("Error serializing Avro message", e);
        case e: RestClientException                     =>
          throw new SerializationException(restClientErrorMsg + writerSchema, e);
      }
    }

}