package io.github.lvitaly.avro4s.kafka.schema.registry.serde

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import org.apache.kafka.common.serialization.{StringDeserializer, StringSerializer}
import utest._

case class OrderEvent(orderId: Int, customerId: String, amount: Double)
case class OrderKey(orderId: Int)

object Avro4sSerdeIntegrationSpec extends TestSuite with KafkaSchemaRegistryFixture {

  private lazy val keySerde   = Avro4sSerde[OrderKey](registryClient, isKey = true)
  private lazy val valueSerde = Avro4sSerde[OrderEvent](registryClient)

  val tests = Tests {

    test("produce single record and consume it back") {
      val topic = uniqueTopic("single")
      createTopic(topic)

      val sent     = OrderEvent(1, "cust-001", 99.99)
      val producer = new KafkaProducer[String, OrderEvent](producerProps(), new StringSerializer, valueSerde.serializer())
      try producer.send(new ProducerRecord(topic, "k1", sent)).get()
      finally producer.close()

      val List((_, received)) = consume(topic, "grp-single", 1, new StringDeserializer, valueSerde.deserializer())
      assert(received == sent)
    }

    test("produce batch of records and consume all in order") {
      val topic = uniqueTopic("batch")
      createTopic(topic)

      val sent     = (1 to 5).map(i => OrderEvent(i, s"cust-$i", i * 10.0)).toList
      val producer = new KafkaProducer[String, OrderEvent](producerProps(), new StringSerializer, valueSerde.serializer())
      try sent.foreach(e => producer.send(new ProducerRecord(topic, e.orderId.toString, e)).get())
      finally producer.close()

      val received = consume(topic, "grp-batch", sent.size, new StringDeserializer, valueSerde.deserializer()).map(_._2)
      assert(received == sent)
    }

    test("Avro key and Avro value both serialized through Schema Registry") {
      val topic = uniqueTopic("avro-kv")
      createTopic(topic)

      val sentKey   = OrderKey(99)
      val sentValue = OrderEvent(99, "cust-kv", 500.0)
      val producer  = new KafkaProducer[OrderKey, OrderEvent](producerProps(), keySerde.serializer(), valueSerde.serializer())
      try producer.send(new ProducerRecord(topic, sentKey, sentValue)).get()
      finally producer.close()

      val List((receivedKey, receivedValue)) =
        consume(topic, "grp-avro-kv", 1, keySerde.deserializer(), valueSerde.deserializer())
      assert(receivedKey == sentKey)
      assert(receivedValue == sentValue)
      assert(registryClient.getAllSubjects.contains(s"$topic-key"))
      assert(registryClient.getAllSubjects.contains(s"$topic-value"))
    }

    test("schema auto-registration is reflected in Schema Registry after produce") {
      val topic = uniqueTopic("schema-check")
      createTopic(topic)

      val producer = new KafkaProducer[String, OrderEvent](producerProps(), new StringSerializer, valueSerde.serializer())
      try producer.send(new ProducerRecord(topic, "k", OrderEvent(1, "c", 1.0))).get()
      finally producer.close()

      val subject  = s"$topic-value"
      val metadata = registryClient.getLatestSchemaMetadata(subject)
      assert(registryClient.getAllSubjects.contains(subject))
      assert(metadata != null)
      assert(metadata.getVersion >= 1)
    }

    test("producer with autoRegisterSchema=false after schema is pre-registered") {
      val topic = uniqueTopic("no-auto-reg")
      createTopic(topic)

      val bootstrap = new KafkaProducer[String, OrderEvent](producerProps(), new StringSerializer, valueSerde.serializer())
      try bootstrap.send(new ProducerRecord(topic, "k", OrderEvent(0, "seed", 0.0))).get()
      finally bootstrap.close()

      val strictSerializer = Avro4sSerializer[OrderEvent](registryClient, autoRegisterSchema = false)
      val producer         = new KafkaProducer[String, OrderEvent](producerProps(), new StringSerializer, strictSerializer)
      try producer.send(new ProducerRecord(topic, "k2", OrderEvent(1, "real", 1.0))).get()
      finally producer.close()

      val received = consume(topic, "grp-no-auto-reg", 2, new StringDeserializer, valueSerde.deserializer()).map(_._2)
      assert(received.size == 2)
      assert(received.last == OrderEvent(1, "real", 1.0))
    }

    test("null value is handled transparently by producer and consumer") {
      val topic = uniqueTopic("nulls")
      createTopic(topic)

      val producer = new KafkaProducer[String, OrderEvent](producerProps(), new StringSerializer, valueSerde.serializer())
      try producer.send(new ProducerRecord[String, OrderEvent](topic, "k-null", null)).get()
      finally producer.close()

      val List((_, received)) = consume(topic, "grp-nulls", 1, new StringDeserializer, valueSerde.deserializer())
      assert(received == null)
    }
  }
}
