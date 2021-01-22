# Plan dans les grandes lignes

## Kafka-streams kesako

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

### avec la DSL fonctionnelle

### avec l'API 

## à caser en quelque part

- kafka-streams fourni 2 apis
  - dsl fonctionnelle, de plus haut niveau (celle qu'on a le plus utilisé)
  - api déclarative, de plus bas niveau

# références

- https://spring.io/blog/2019/12/09/stream-processing-with-spring-cloud-stream-and-apache-kafka-streams-part-6-state-stores-and-interactive-queries
- https://www.youtube.com/watch?v=Z3JKCLG3VP4