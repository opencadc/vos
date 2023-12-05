# cadc-vos-client library

This library provides the VOSpaceClient.


## integration tests
The `gradle intTest` target assumes there is a local deployment of `cavern` with a resourceID of
`ivo://opencadc.org/cavern` (in the local registry). Tests currently use client a certificate
named `{username}.pem` and it is assumed by the test setup that this identity can create or use
a container node in the root named `client-int-tests` - normally it is easiest if that identity
is also the cavern root owner.

It is very dangerous and not supported to run integrtation tests against a real VOSpace service
where you care about the content because tests are free to perform cleanup and code can have bugs.

