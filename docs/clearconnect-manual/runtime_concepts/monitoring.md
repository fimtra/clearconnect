## Monitoring {#monitoring}

### Deadlock Detection {#deadlock-detection}

Every agent runtime includes a deadlock detection feature. Any deadlocks discovered (even in application code running in the same VM) are logged to the messages log file.

### Connection Monitoring {#connection-monitoring}

All connections between a service and proxy are monitored for aliveness. This is achieved by every VM having a `ConnectionWatchdog` that monitors connections. When a connection is established, the `ConnectionWatchdog` starts to monitor it. The watchdog periodically sends a heartbeat message to all registered connections and checks each connection to verify that a heartbeat (or data) has been received from the other end of the connection (received data can count as a heartbeat). If no heartbeat or data has been received for a specified number of periods (default 3, period default 5000ms) then the watchdog closes the connection. If a heartbeat cannot be sent, the watchdog also closes the connection.

### Service Instance Monitoring {#service-instance-monitoring}

Service instances on the platform are all individually monitored by the `PlatformRegistry`. This is achieved by the registry acting as a client to each service and subscribing for the system records that each service provides. This provides the registry with knowledge about what activity is occurring in each service instance (via the system records) and whether the service instance is available (via the connection being available). If the service instance is stopped, the registry will detect this when the connection closes or the watchdog detects the connection is not alive. Once the registry loses a service instance connection it does not attempt to re-establish the connection. The service must be re-registered with the registry via the agent.