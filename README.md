
![github-large](https://softwareengineeringdaily.com/wp-content/uploads/2015/08/kafka-logo-wide.png)
## 1) Introduction
This repository contains a presentation of Apache Kafka tool in pdf format and 2 Code Snippets,one for a Kafka Producer and one for a Kafka Consumer.

The use case scenario for our example is the following:
#### Producer
A producer (which is a single program) is downloading data about Covid-19 from [COVID-19 data API](https://rapidapi.com/Gramzivi/api/covid-19-data). These data have the following schema: 
```
{
    "country": "Italy",
    "provinces": [
        {
            "province": "Italy",
            "confirmed": 110574,
            "recovered": 16847,
            "deaths": 13155,
            "active": 80572
        }
    ],
    "latitude": 41.87194,
    "longitude": 12.56738,
    "date": "2020-04-01"
}
```

As you see, for every particular date and country we take the confirmed, recovered and active cases and deaths from Covid-19. In our example, we assume that our producer is choosing a date window and a particular country to retrieve these data and forwarding (publishing) them in Kafka Cluster sequentially in ascending time (stream of records).
#### Kafka Cluster Architecture
Our cluster has the One-Node-One-Broker architecture, but we can scale it to Many-Nodes-Many-Brokers.

#### Consumer
Our consumer (which is a programm too) subscribes to the topic where the producer publishes these records. In our example this topic has one only partition and no replicas (due to the existance of only one Broker) . So whenever a new record is coming to the partition of Covid-19's Topic, our consumer consumes this and stores it to a database. This Database is a non-relational database MongoDB and all these data are stored in a collection called "Covid".
## 2) Prerequisites
Before running the example,it is necessary to have or install:
- Kafka from [official site](https://kafka.apache.org/downloads). (JavaSE is required)
- Python 3.* or above.
- Jupyter Notebook.
- MongoDB

When Kafka is installed the first thing is to start the Zookeeper Server with the command:
```sh
$ cd $KAFKA_HOME
$ bin/zookeeper-server-start.sh config/zookeeper.properties
```
where `KAFKA_HOME` the directory where kafka is installed.
Next we need to start the broker with the command:
```sh
$ bin/kafka-server-start.sh config/server.properties
```
Then we create our topic:
```sh
$ bin/kafka-topics.sh --create --bootstrap-server localhost:9092 --replication-factor 1 --partitions 1 --topic Covid-19
```
In order for the consumer to connect to the MongoDB, mongo service should be active. In Ubuntu 18.04 you can start this service with the commend:
```sh
$ sudo systemctl start mongod 
```

Now everything is ready!

## Execution
Open in jupyter lab the Code Snippets for Consumer and Producer in two different tabs.
```sh
$ jupyter lab
```

Now run all cells for producer and consumer. We see that everytime a producer is publishing a new record to Kafka broker, consumer consumes this record and stores it the Database. If we open a new terminal and type:
```sh
$ mongo
> use Covid
> db.Covid.find().pretty()
```
we will see the records to be stored in our Database!
```
