# Plan dans les grandes lignes

## Kafka-streams kesako

- Kafka 101
  - producers, consumers, brokers
  - topics & partitions
  - consumer groups
  - modèle de parallélisme
- qu'est-ce qu'un stream
  - https://www.youtube.com/watch?v=Z3JKCLG3VP4&t=104
- lib java standard
  - https://www.youtube.com/watch?v=Z3JKCLG3VP4&t=446
- horizaontal scaling
  - https://www.youtube.com/watch?v=Z3JKCLG3VP4&t=535
- stream processing topology, vu de loin
  - https://kafka.apache.org/27/documentation/streams/core-concepts

## exemple #1 - to-uppercase
- créer les topics manuellement avant (sinon erreur au démarrage)
```shell
docker exec -it kafka kafka-topics --create \
    --bootstrap-server kafka:9092 \
    --replication-factor 1 \
    --partitions 1 \
    --topic to-uppercase-input
    
docker exec -it kafka kafka-topics --create \
    --bootstrap-server kafka:9092 \
    --replication-factor 1 \
    --partitions 1 \
    --topic to-uppercase-output
```

- lister les topics:
```shell
docker exec -it kafka kafka-topics --list --bootstrap-server kafka:9092
```

- lister les consumer groups:
```shell
docker exec -it kafka kafka-consumer-groups --bootstrap-server kafka:9092 --list
```

- créer un producteur en ligne de commande
```shell
docker exec -it kafka kafka-console-producer --broker-list kafka:9092 --topic to-uppecase-input
```

- créer un consommateur en ligne de commande
```shell
docker exec -it kafka kafka-console-consumer --bootstrap-server kafka:9092 --topic to-uppercase-output
```

- décrire le consommateur lié à notre appli kafka-streams
```shell
docker exec -it kafka kafka-consumer-groups --bootstrap-server kafka:9092 --describe --group to-uppercase
```

### avec l'API Processor de bas niveau

### avec la DSL fonctionnelle

### tests unitaires

### Récap

- c'est quoi kafka-streams, de loin et dans les grandes lignes
- doc Confluent
  - Concepts
    x Kafka 101
    x Stream
    x Stream Processing Application
    x Processor Topology
    x Stream Processor
    - Stateful Stream Processing
    - Duality of Streams and Tables
    - KStream
    - KTable
    - GlobalKTable
    - Time
    - Aggregations
    - Joins
    - Windowing
    - Interactive Queries
    - Processing Guarantees ?
    - Out-of-Order Handling ?
    - Out-of-Order Terminology ?
  - Architecture
    - Processor Topology
    - Parallelism Model
      - Stream Partitions and Tasks
      - Threading Model
      - Example
    - State

### variation - filtrage via ̀map` avec la DSL

## exemple #2 - linesplit - julien drive



## listing des concepts que j'aimerais aborder et illustrer

- moi
  - Joins
- doc Apache Kafka
  - Concepts
    - Stream Processing Topology
    - Time
    - Duality of Streams and Tables 
    - States (KTable, state stores)
    - Aggregations
    - Windowing
  - Architecture
    - Stream Partitions and Tasks
    - Threading Model
    - Local State Stores ?
    - Fault Tolerance
- doc Confluent
  - Concepts
    - Kafka 101
    - Stream
    - Stream Processing Application
    - Processor Topology
    - Stream Processor
    - Stateful Stream Processing
    - Duality of Streams and Tables
    - KStream
    - KTable
    - GlobalKTable
    - Time
    - Aggregations
    - Joins
    - Windowing
    - Interactive Queries
    - Processing Guarantees ?
    - Out-of-Order Handling ?
    - Out-of-Order Terminology ?
  - Architecture
    - Processor Topology
    - Parallelism Model
      - Stream Partitions and Tasks
      - Threading Model
      - Example
    - State
  

## à caser en quelque part

- kafka-streams fourni 2 apis
  - dsl fonctionnelle, de plus haut niveau (celle qu'on a le plus utilisé)
  - api déclarative, de plus bas niveau

# références

- https://spring.io/blog/2019/12/09/stream-processing-with-spring-cloud-stream-and-apache-kafka-streams-part-6-state-stores-and-interactive-queries
- https://www.youtube.com/watch?v=Z3JKCLG3VP4 