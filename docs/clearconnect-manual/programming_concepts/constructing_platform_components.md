## Constructing Platform Components 

**Note:** the code snippets here are taken from the [Hello World example](../quick_start/hello_world.md)

### EndPointAddress 

ClearConnect is a distributed runtime environment with connections being made between runtimes on local and remote hosts. The `EndPointAddress` is used to abstract the connections from the technology implementation specifics. 

```java
EndPointAddress registryAddress = new EndPointAddress(TcpChannelUtils.LOCALHOST_IP, 22222);
```

### The Platform Registry 

A platform registry must be running for a ClearConnect platform to exist. To run a platform registry, construct a PlatformRegistry as in the example below:

```java
EndPointAddress registryAddress = new EndPointAddress(TcpChannelUtils.LOCALHOST_IP, 22222);

PlatformRegistry registry = new PlatformRegistry("Hello World Platform", registryAddress);
```

In general, the registry should run in its own virtual machine. As long as the registry process is running, the platform is running.

### An Agent 

An agent must be constructed in order to use the platform. To construct an agent you need two pieces of information:

*   The name of the agent
*   The _EndPointAddress_ of the registry

The example below constructs an agent connecting to the registry specified previously:

```java
EndPointAddress registryAddress = new EndPointAddress(TcpChannelUtils.LOCALHOST_IP, 22222);

PlatformRegistryAgent agent = new PlatformRegistryAgent("Agent1", registryAddress);
```

In general, there should only be one agent constructed per virtual machine. The agent can be thought of as a session on the platform. The agent is used to construct (and destroy) services and proxies.

#### Platform Redundancy 

An agent can provide multiple registry end point addresses on construction. This will force the agent to try each address, in order, until a successful connection is made. Should the agent lose the connection to the registry, the agent tries the next address in the list until a connection is made, looping through the list indefinitely.

When an agent re-connects to a registry, it re-registers all the services that were previously created though it (see next section). This mechanism allows a stand-by registry process to run and become aware of all platform services if the primary registry process is lost.

### A Service 

A service is constructed via an agent (technically, it is a platform service _instance_ that is created). To construct a service the minimum following attributes are needed:

*   Service family name
*   Service member name
*   End point address node name (IP address of the host)
*   The wire protocol (the format used for sending messages)
*   The redundancy mode (HA feature)

The example below constructs a service using an agent:

```java
// create the service 
agent.createPlatformServiceInstance("Foo", "Bar1", TcpChannelUtils.LOCALHOST_IP, WireProtocolEnum.GZIP, RedundancyModeEnum.FAULT_TOLERANT); 

// obtain the service 
IPlatformServiceInstance serviceInstance = agent.getPlatformServiceInstance("Foo", "Bar1");
```

Note that the agent maintains a reference to any created platform service instance. The reference is maintained until the service instance is destroyed via `agent.destroyPlatformServiceInstance(…)`.

### A Proxy 

A proxy to a service is also constructed via an agent. To construct the proxy, only the platform service family name is required. The agent performs all the work of contacting the registry to find out which instance to connect to.

The example below constructs a proxy to a service:

```java
// wait to ensure the service is available (this will wait for 60 seconds at most)
agent.waitForPlatformService("Foo");

// obtain a proxy to the service
IPlatformServiceProxy proxy = agent.getPlatformServiceProxy("Foo");
```

As with a service, the proxy reference is maintained by the agent until it is destroyed using `agent.destroyPlatformServiceProxy(…)`.

### Creating and publishing a record 

A record is created and published using a service. In the example below, a record is created in the service, updated and published:

```java
// create the record
IRecord record = serviceInstance.getOrCreateRecord("HelloWorld");

// update the record
record.put("a message", "Hello World");
record.put("the time stamp", LongValue.valueOf(System.currentTimeMillis()));
record.put("pi is", DoubleValue.valueOf(3.1415926535898));

// publish the record change
serviceInstance.publishRecord(record);
```

### Listening for record changes 

Changes to a record are observed by registering a listener against the name of the record in the proxy. The record listener contains the application logic to respond to record changes.

The example below registers a listener against the record created in the service example above:

```java
IRecordListener listener = new IRecordListener()
{
    @Override
    public void onChange(IRecord image, IRecordChange atomicChange)
    {
        //
        // NOTE: a record image is unchanging ONLY in this method - outside this method it
        // is subject to change. So if you want to use a record later on in your code,
        // snapshot it here, like this
        //
        ImmutableRecord mySnapshot = ImmutableSnapshotRecord.create(image);

        // access some of the record fields and print them out
        System.out.println(mySnapshot.get("a message").textValue());
        System.out.println(mySnapshot.get("pi is").doubleValue());
    }
};

proxy.addRecordListener(listener, "HelloWorld");
```

_**Note:**_ The image of a record is immutable only within the context of the listener’s `onChange` method. Outside of this method, the record image is subject to change.

You should not cache the `IRecord` of the `onChange` method if you want to keep a snapshot of the image. The correct way to capture a snapshot of the record’s image during the onChange method is by using the snapshot code below:

```java
ImmutableRecord mySnapshot = ImmutableRecord.snapshot(image);
```