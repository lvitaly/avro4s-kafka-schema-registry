package io.github.lvitaly.avro4s.kafka.schema.registry.serde

import io.confluent.kafka.schemaregistry.client.MockSchemaRegistryClient
import org.apache.kafka.common.errors.SerializationException
import utest._

case class MyRecord(id: Int, name: String)

object Avro4sSerdeSpec extends TestSuite {

  val topic = "test-topic"

  def mockRegistry() = new MockSchemaRegistryClient()

  val tests = Tests {

    test("Avro4sSerializer") {
      test("serialize null returns null") {
        val serializer = Avro4sSerializer[MyRecord](mockRegistry())
        assert(serializer.serialize(topic, null) == null)
      }

      test("serialize produces bytes with magic byte and schema id prefix") {
        val serializer = Avro4sSerializer[MyRecord](mockRegistry())
        val bytes      = serializer.serialize(topic, MyRecord(1, "Alice"))
        // first byte is magic byte 0, next 4 bytes are schema id
        assert(bytes.length > 5)
        assert(bytes(0) == 0)
      }

      test("uses -value subject suffix by default") {
        val registry   = mockRegistry()
        val serializer = Avro4sSerializer[MyRecord](registry)
        serializer.serialize(topic, MyRecord(1, "Alice"))
        assert(registry.getAllSubjects.contains(s"$topic-value"))
      }

      test("uses -key subject suffix when isKey=true") {
        val registry   = mockRegistry()
        val serializer = Avro4sSerializer[MyRecord](registry, isKey = true)
        serializer.serialize(topic, MyRecord(1, "Alice"))
        assert(registry.getAllSubjects.contains(s"$topic-key"))
      }

      test("autoRegisterSchema=false throws SerializationException for unknown schema") {
        val serializer = Avro4sSerializer[MyRecord](mockRegistry(), autoRegisterSchema = false)
        val ex         = assertThrows[SerializationException](serializer.serialize(topic, MyRecord(1, "Alice")))
        assert(ex.getMessage.contains("Error retrieving Avro schema"))
      }
    }

    test("Avro4sDeserializer") {
      test("deserialize null returns null") {
        val deserializer = Avro4sDeserializer[MyRecord](mockRegistry())
        assert(deserializer.deserialize(topic, null) == null)
      }

      test("deserialize throws SerializationException for unknown magic byte") {
        val deserializer = Avro4sDeserializer[MyRecord](mockRegistry())
        val ex           = assertThrows[SerializationException](deserializer.deserialize(topic, Array[Byte](1, 0, 0, 0, 1)))
        assert(ex.getMessage.contains("Error deserializing"))
      }
    }

    test("Avro4sSerde") {
      test("round-trip serializes and deserializes correctly") {
        val registry     = mockRegistry()
        val serde        = Avro4sSerde[MyRecord](registry)
        val record       = MyRecord(42, "Bob")
        val bytes        = serde.serializer().serialize(topic, record)
        val deserialized = serde.deserializer().deserialize(topic, bytes)
        assert(deserialized == record)
      }

      test("round-trip with isKey=true") {
        val registry     = mockRegistry()
        val serde        = Avro4sSerde[MyRecord](registry, isKey = true)
        val record       = MyRecord(7, "Carol")
        val bytes        = serde.serializer().serialize(topic, record)
        val deserialized = serde.deserializer().deserialize(topic, bytes)
        assert(deserialized == record)
      }

      test("round-trip null value") {
        val serde = Avro4sSerde[MyRecord](mockRegistry())
        assert(serde.serializer().serialize(topic, null) == null)
        assert(serde.deserializer().deserialize(topic, null) == null)
      }
    }
  }
}
