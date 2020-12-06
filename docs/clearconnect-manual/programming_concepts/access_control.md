## Access Control 

The ClearConnect API includes features for controlling access to services and data within services.

### Network Access Control 

When using TCP as the transport technology, a service can be configured with a _whitelist_ of acceptable inbound IP addresses or a *blacklist* of banned inbound IP addresses. This is configured as a system property and specifies a comma-separated list of regular expressions to match against IP addresses. Note that because they are regular expressions a literal "." must be escaped with "\". Any socket connections that do not match the whitelist or match the blacklist will be terminated before any messages are processed. 

Both can be specified but this may become confusing to maintain so its recommended to set one or the other, not both.

- whitelist example: `-DtcpChannel.serverAcl=10\.0\.0\.\d{1,3};10\.1\.\d{1,3}\.3`
- blacklist example: `-DtcpChannel.serverBlacklistAcl=10\.1\.23\.\d{1,3};10\.3\.43\.\d{1,3}3`

### Session Access Control 

All connections between a proxy and service include a session that is "synchronised" as the first order of business. This allows session-level access control. As part of the synchronisation, a proxy sends over a set of string attributes that are inspected by the receiving service. If the attributes are valid, the service responds with a session ID, otherwise the link is terminated.

The following classes and interfaces are used for session access control:

| Class/Interface | Description |
| --- | --- |
| `SessionContexts` | Central component used to register |
| `ISessionManager` | Receives attributes from a proxy and validates them to create a session ID. |
| `ISessionAttributesProvider` | Provides the attributes to send to a service (e.g. username and password). |

**Note: only** when using the `CIPHER` wire protocol will all session attributes and responses be encrypted over the wire. All other wire protocols have no encryption of session information.

### Data Access Control 

When subscribing for a record, a proxy can provide a permission token. The permission token is a string that has meaning in the application domain. The subscription is received by the service along with the permission token and application code can be invoked to inspect the token and assess whether the subscription should be allowed by the proxy. Typically, this mechanism is used to ensure clients subscribe for data they are entitled for.

This code snippet helps to demonstrate this

```java
service.setPermissionFilter(new IPermissionFilter()
{
	@Override
	public boolean accept(String permissionToken, String recordName)
	{
		// examine the token and record...
		// return false if the subscription is not allowed
		// return true if the subscription is OK
		return false;
	}
});
```