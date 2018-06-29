# vos
client and server implementation of VOSpace 2.1 specification

# plugin: TransferGenerator API change in cadc-vos-server-1.1

The method signature for this plugin interface has changed, moving more logic into the implementation.
This means the implementation is now responsible for converting the complete transfer request into a
list of protocols with endpoints, including setting preferred order and filtering out unsupported 
protocol+securityMethod combinations (possibly dependent on transfer.version).

