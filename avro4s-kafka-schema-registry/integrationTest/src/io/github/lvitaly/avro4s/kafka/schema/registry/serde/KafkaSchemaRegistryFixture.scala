package io.github.lvitaly.avro4s.kafka.schema.registry.serde

import com.dimafeng.testcontainers.{ConfluentKafkaContainer, MultipleContainers, SchemaRegistryContainer}
import io.confluent.kafka.schemaregistry.client.CachedSchemaRegistryClient
import org.apache.kafka.clients.admin.{AdminClient, AdminClientConfig, NewTopic}
import org.apache.kafka.clients.consumer.{ConsumerConfig, KafkaConsumer}
import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.kafka.common.serialization.Deserializer
import org.testcontainers.containers.Network

import java.time.Duration
import java.util.{Collections, Properties, UUID}
import scala.collection.mutable.ListBuffer

trait KafkaSchemaRegistryFixture {

  private object Containers {
    private val kafkaNetworkAlias = "kafka-broker"

    private val network = Network.newNetwork()

    val kafkaContainer: ConfluentKafkaContainer = {
      val container = ConfluentKafkaContainer.Def().createContainer()

      container.container
        .withNetwork(network)
        .withNetworkAliases(kafkaNetworkAlias)

      container
    }

    val schemaRegistryContainer: SchemaRegistryContainer =
      SchemaRegistryContainer.Def(network, kafkaNetworkAlias).createContainer()

    MultipleContainers(kafkaContainer, schemaRegistryContainer).start()
  }

  private val pullTimeout = Duration.ofMillis(500)
  private val deadlineTimeout = Duration.ofSeconds(30)

  lazy val registryClient: CachedSchemaRegistryClient =
    new CachedSchemaRegistryClient(Containers.schemaRegistryContainer.schemaUrl, 100)

  def uniqueTopic(prefix: String): String =
    s"$prefix-${UUID.randomUUID().toString.take(8)}"

  def createTopic(name: String): Unit = {
    val props = new Properties()
    props.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, Containers.kafkaContainer.bootstrapServers)
    val admin = AdminClient.create(props)
    try admin.createTopics(Collections.singleton(new NewTopic(name, 1, 1.toShort))).all().get()
    finally admin.close()
  }

  def producerProps(): Properties = {
    val p = new Properties()
    p.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, Containers.kafkaContainer.bootstrapServers)
    p
  }

  def consumerProps(groupId: String): Properties = {
    val p = new Properties()
    p.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, Containers.kafkaContainer.bootstrapServers)
    p.put(ConsumerConfig.GROUP_ID_CONFIG, groupId)
    p.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")
    p
  }

  /** Polls `topic` until `n` records arrive or 30 s elapses. Returns (key, value) pairs. */
  def consume[K, V](
    topic  : String,
    groupId: String,
    n      : Int,
    keySer : Deserializer[K],
    valSer : Deserializer[V]
  ): List[(K, V)] = {
    val consumer = new KafkaConsumer[K, V](consumerProps(groupId), keySer, valSer)
    val results  = ListBuffer.empty[(K, V)]
    try {
      consumer.subscribe(Collections.singleton(topic))
      val deadline = System.currentTimeMillis() + deadlineTimeout.toMillis
      while (results.size < n && System.currentTimeMillis() < deadline) {
        consumer.poll(pullTimeout).forEach(r => results += (r.key() -> r.value()))
      }
    } finally consumer.close()

    if (results.size < n)
      throw new RuntimeException(
        s"Expected $n records from '$topic' but received ${results.size} within 30 s"
      )
    results.toList
  }
}
