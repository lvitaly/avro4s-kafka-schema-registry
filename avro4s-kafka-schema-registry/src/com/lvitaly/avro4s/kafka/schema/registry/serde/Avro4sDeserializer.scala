package com.lvitaly.avro4s.kafka.schema.registry.serde

import com.sksamuel.avro4s.{AvroInputStream, Decoder, SchemaFor}
import org.apache.kafka.common.errors.SerializationException
import org.apache.kafka.common.serialization.Deserializer

import java.io.{ByteArrayInputStream, IOException}
import java.nio.ByteBuffer
import io.confluent.kafka.schemaregistry.avro.AvroSchema
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException

class Avro4sDeserializer[T >: Null: Decoder: SchemaFor](schemaRegistry: SchemaRegistryClient) extends Deserializer[T] {
  
  override def deserialize(topic: String, data: Array[Byte]): T =
    if (data == null) null else {
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
  
        val reader = AvroInputStream.binary[T].from(in).build(schema)
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