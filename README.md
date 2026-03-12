[![Made in Ukraine](https://img.shields.io/badge/made_in-Ukraine-ffd700.svg?labelColor=0057b7)](https://stand-with-ukraine.pp.ua)

avro4s-kafka-schema-registry
---

Kafka `Serializer`, `Deserializer`, and `Serde` implementations for Avro-encoded messages, backed by [avro4s](https://github.com/sksamuel/avro4s) for schema derivation and the [Confluent Schema Registry](https://docs.confluent.io/platform/current/schema-registry/index.html) for schema management.

## Features

- Derives Avro schemas automatically from Scala case classes via `avro4s`
- Encodes/decodes messages using the Confluent wire format (magic byte + schema ID + Avro binary payload)
- Supports optional auto-registration of schemas in the registry
- Cross-built for Scala 2.12, 2.13, and 3

## Usage

```scala
import io.github.lvitaly.avro4s.kafka.schema.registry.serde
import io.confluent.kafka.schemaregistry.client.CachedSchemaRegistryClient

case class MyEvent(id: String, value: Int)

val schemaRegistry = new CachedSchemaRegistryClient("http://localhost:8081", 1000)

// Create a Serde for use with Kafka Streams or the Kafka consumer/producer API
val serde = Avro4sSerde[MyEvent](
  schemaRegistry,
  isKey = false,            // true for key serdes
  autoRegisterSchema = true // set to false to require pre-registered schemas
)
```

You can also use `Avro4sSerializer` and `Avro4sDeserializer` directly if you only need one direction.

## Building

This project uses [Mill](https://mill-build.com).

```sh
./mill avro4s-kafka-schema-registry[2.13.18].compile
```

## Dependencies

| Dependency | Version |
|---|---|
| avro4s (Scala 2.x) | 4.1.2 |
| avro4s (Scala 3) | 5.0.15 |
| kafka-schema-registry-client | 8.1.1 |

The Confluent Maven repository is required:

```
https://packages.confluent.io/maven/
```

## License

This project is open source. See the repository for license details.
