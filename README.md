
## Table of Contents
- [General](#General)
- [Protocols](#Protocols)
- [Testing](#Testing)

# General

## Java Version
Use `Java JDK-21`. 

## Starting the Server Applications

```bash
# First compile the project with Maven
mvn compile
# Start the client application with the following command where componentId is one of client-0, client-1 or client-2.
mvn exec:java@<componentId>
# You can also combine both commands into one
mvn compile exec:java@<componentId>
```

You need to replace `<componentId>` with the corresponding component ID of the server: `broker-0`, `broker-1`,
`broker-2` and `dns-0` are available.
You may need multiple terminal windows to start multiple servers.

The default configuration for the servers can be started using:

1. `mvn compile`
2. `mvn package -DskipTests`
3. Run selected server
- `mvn exec:java@broker-0`
- `mvn exec:java@broker-1`
- `mvn exec:java@broker-2`
- `mvn exec:java@monitoring-0`
- `mvn exec:java@dns-0`


# Protocols

## Leader Election Protocol (LEP)
Upon connecting to a Broker leader election port, the server sends the greeting `ok LEP`.

### `elect <id>`
Notifies that the broker with the given `id` is up for election.
#### Responses
| State                | Response                          |
|----------------------|-----------------------------------|
| success (ring/bully) | `ok`                              |
| success (raft)       | `vote <sender-id> <candidate-id>` |
| error syntax         | `error usage: elect <id>`         |

### `declare <id>`
Declares the broker with the given `ìd` as the new leader.
#### Responses
| State        | Response                    |
|--------------|-----------------------------|
| success      | `ack <sender-id>`           |
| error syntax | `error usage: declare <id>` |

### `ping`
Sent by the leader to the followers as a heartbeat If the followers do not receive
a heartbeat message within a set timeout, a new leader election is started.
#### Responses
| State        | Response               |
|--------------|------------------------|
| success      | `pong`                 |
| error syntax | `error protocol error` |

### Default Response
If no matching command of the protocol is found, then the broker sends `error protocol error` and closes the connection

### `vote <sender-id> <candidate-id>`
The sender with `sender-id` votes the for the candidate with `candidate-id`

## Naming scheme
The names of the servers start with `0` and are incremented by `1` every new node.

- Message Broker: `broker-#`
- DNS Server: `dns-#`
- Monitoring Server: `monitoring-#`

For example the available servers:
- `dns-0`
- `broker-0`
- `broker-1`
- `broker-2`
- `broker-3`

### Message Broker
The `broker-0` listens for publisher/subscribers on TCP port `20000` and for election updates from other brokers on
TCP port `20001`. These 2 ports are incremented by 10 for every new broker instance. So for example for the first 3
brokers we would have:

- `broker-0`: messaging=`20000`, election=`20001`
- `broker-1`: messaging=`20010`, election=`20011`
- `broker-2`: messaging=`20020`, election=`20021`

### DNS server
The `dns-0` listens per default on TCP port `18000`.

### Monitoring server
The `monitoring-0` listens per default on UDP port `17000`.

