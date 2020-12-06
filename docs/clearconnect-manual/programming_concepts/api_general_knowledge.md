## API general knowledge 

This section discusses some of the general features you will come across when using the ClearConnect API.

**Note:** the Javadoc of the API classes also provides further descriptions.

### Tecnology stack 
The ClearConnect platform is designed to have zero dependencies on any 3rd party products. The technology stack is shown in the diagram below.

![](../assets/Fimtra%20tech%20stack.png)

### Ephemeral ports 

When a service instance is constructed with no TCP port specified (or 0) then an _ephemeral_ port is used. This allows the network layer to allocate available ports for the service without application configuration (a big plus!).

### Wire Protocol

ClearConnect has various built-in wire protocols that encode and decode data for different use-cases. A service is declared to use a wire protocol. Services are free to choose what protocol they want so the ClearConnect platform can have services using different wire protocols.

| Wire Protocol | Description |
| --- | --- |
| `STRING` | A UTF-8 based wire format. |
| `GZIP` | An ISO-8859-1 variation of the STRING wire protocol and uses a gzip algorithm to compress the wire format. This is the default for services. |
| `CIPHER` | An encrypted STRING wire protocol using AES 128bit encryption. |

### Event Listeners

Listening to record changes (with the `IRecordListener`) allows data changes to be handled. However, what about being able to detect when data is available or when a service is ready or what connections a service has? The ClearConnect platform provides an extensive set of event listeners to detect the various platform signals that represent service, data and function availability.

| Listener | Registration Scope | Purpose |
| --- | --- | --- |
| `IRecordAvailableListener` | Proxy/Service | Notifies when records are created in a service (and thus being available) |
| `IRecordSubscriptionListener` | Proxy/Service | Notifies when a record is subscribed for. Knowing whether any process has subscribed for a record means the data does not need to be sourced until the first subscriber appears. |
| `IRpcAvailableListener` | Proxy/Service | Notifies when an RPC is created in a service (and thus available to be called by a proxy) |
| `IFtStatusListener` | Service | Invoked when a service instance becomes active or standby if it is part of a fault tolerant service family. |
| `IProxyConnectionListener` | Service | Provides a service with the ability to be notified when new proxy connections are made to it. |
| `IRecordConnectionStatusListener` | Proxy | Notifies when a record is connected, disconnected or reconnecting. This is a mechanism for identifying if data in a record should be considered _live_ or _stale_. |
| `IServiceConnectionStatusListener` | Proxy | Notifies when the connection to a proxy’s service is connected, disconnected or reconnecting. |
| `IRegistryAvailableListener` | Agent | Notifies when the registry is available or unavailable. |
| `IServiceAvailableListener` | Agent | Notifies when a service family becomes available or unavailable. |
| `IServiceInstanceAvailableListener` | Agent | Notifies when a service instance becomes available or unavailable. |

### Handling fast-producer scenarios

A proxy has no control over the record publishing rate of the service it is connected to. To solve the situation where the data rates are too fast for application code in the `IRecordListener.onChange` to process efficiently, the listener can be replaced with a `CoalescingRecordListener`. This provides a means to consume the updates from the service, batch them up and essentially produce an atomic change representing multiple atomic changes made to the record.

However the `CoalescingRecordListener` cannot be used if the service publishes discrete changes that should not be coalesced into a single, bigger change. This is application specific.


### RPC 2-phase timeout

One of the features of the RPC mechanism in the ClearConnect platform is its 2-phase timeout. The phases for a timeout are:

| Phase | Description |
| --- | --- |
| Execution start | This is the time between invoking in the proxy and the RPC execution starting in the remote service. |
| Execution duration | This is the time allowed for the execution to complete after it has started in the remote service. |

The timeouts are defined when executing the RPC, by default they are both 5 seconds.

### Record `toString`
This is an example of the `toString` of a record:
```
(Immutable)Foo->Agent1-20141023-20:52:22:744@192.168.56.1|HelloWorld|1|{message=SHello World,pi=D3.1415926535898,time=SThu Oct 23 20:52:23 BST 2014}|subMaps{}
```

The format is: `Context` `|` `Name` `|` `Sequence` `|` `Data` `|` `Sub-map`

From the above example:

| `toString` part | Value |
| --- | --- |
| `Context` | `(Immutable)Foo->Agent1-20141023-20:52:22:744@192.168.56.1` |
| `Name` | `HelloWorld` |
| `Sequence` | `1` |
| `Data` | `{message=SHello World,pi=D3.1415926535898,time=SThu Oct 23 20:52:23 BST 2014}` |
| `Sub-map` | `subMaps{}` |

- The context describes the name of the service or proxy that the record was from. For a proxy, the format is:`Service family` `->` `Agent`
  - The `Agent` in the example is `Agent1-20141023-20:52:22:744@192.168.56.1` which itself has a format:
     - `Agent name` `-` `[yyyyMMdd]-[HH:mm:ss:SSS]` `@` `[local host IP]`
This provides an automatic unique naming mechanism for any agent.

- The data is displayed in its key-value `toString` form. The `toString` of an `IValue` has the format:
`Type-code` `String value`

The type codes per record `IValue` class are:

| Type code | IValue class |
| --- | --- |
| S | TextValue |
| D | DoubleValue |
| L | LongValue |
| B | BlobValue |

So the string `message=SHello World` is self-describing; the key is `message`, the data is a `TextValue` (it starts with `S`) with string value of `Hello World`. Similarly, the string `pi=D3.1415926535898` describes the key `pi` has data that is a `DoubleValue` with string value `3.1415926535898`.

**Note:** any `IValue` class can be constructed from its string representation as in the example below:

```java
DoubleValue pi = AbstractValue.constructFromStringValue("D3.1415926535898");
```
