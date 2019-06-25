# Firewall Event Consumer

Firewall event consumer is data transformation tool. This tool, it will run as a service, continuously in a docker containers, it will consume a configuration file. This configuration file Kafka topic name, Kafka server name and credentials for that. This service will basically we be a part of consumer groups. What will happen the second configuration Google ProtoBuff in binary format name, multiple messages will be arriving in Kafka. 
Third set configurations are going to be Cassandra table schema, it will  have server, cassandra and etc. What we need those ProtoBuff binary payloads needs to be inserted in Cassnadra.

### The KCL to create table in Cassandra
```
CREATE TABLE fw_events(event_id uuid PRIMARY KEY, SrcIpAddr text, 
DstIpAddr text, SrcPort int, DstPort int, 
LastUpdated timestamp, DeviceId int, Action int, AclRuleId int);
```

### Configuration
- Kafka bootstrap servers

```
  "Brokers": ["localhost:9092"],
```  

- Kafka Version

```
  "Version": "2.1.1",
```

- Kafka Consumer Group

```
  "Group": "cgroup2",
```

- Kafka Topic

```  
  "Topics": "test",
```

- OffsetOldest get oldest offset available on the broker.

```  
  "Oldest": true,
```  

Verbose Loggin

```
  "Verbose": true,
```

Kafka SASL username and password  

```
  "SaslUser": "",
  "SaslPassword": "",  
  "SaslEnable": false,  
  "SaslHandshake": true,
``` 

Cassandra Consistency, set to LocalOne for single node deployment, set to Quorum for the production use

``` 
  "CassandraConsistency": "LocalOne",
```

Cassandra Keyspace

```  
  "CassandraKeyspace": "tutorialspoint",
```  

Casssandra username and password

```
  "CassandraUsername": "cassandra",
  "CassandraPassword": "cassandra"
```
