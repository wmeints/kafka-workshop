Kafka-workshop
--------------
This repository contains the sources for the Apache Kafka workshop.
If you want to try out the samples in this repo, be sure to have
kafka installed on your machine.

Each sample has its own readme file with the required settings
that you need for Kafka in order to run the sample.

## Preparation
Before you run any of the samples make sure that you have downloaded Kafka 0.9,
you can get it over here: http://kafka.apache.org/downloads.html

Install Kafka on your computer and start it using the following set of commands.
First start zookeeper.

### For Mac/Linux

```
bin/zookeeper-server-start.sh config/zookeeper.properties
```

After you started the zookeeper server, start a broker on your computer.

```
bin/kafka-server-start.sh config/server.properties
```

Finally create the sample topic using the following command:

```
bin/kafka-topics.sh --zookeeper localhost:2181 --create --replication-factor 1 --partitions 1 --topic events
```

### For Windows
```
bin\windows\zookeeper-server-start.sh config/zookeeper.properties
```

After you started the zookeeper server, start a broker on your computer.

```
bin\windows\kafka-server-start.bat config/server.properties
```

Finally create the sample topic using the following command:

```
bin\windows\kafka-topics.bat --zookeeper localhost:2181 --create --replication-factor 1 --partitions 1 --topic events
```
