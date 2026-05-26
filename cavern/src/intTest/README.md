# cavern integration tests

The `intTest` target uses the cadc-etst-vos library to run tests against a locally
deployed cavern instance. This looks up `ivo://opencadc.org/cavern` in a local `reg`
service.

The tests create and run in `/projects/int-tests`; some tests may rely on configuration
with
```
org.opencadc.cavern.allocationParent = /projects
org.opencadc.cavern.selfAllocateGroup = ivo://cadc.nrc.ca/gms?opencadc-vospace-test
```
where the `selfAllocateGroup` is an undocumented prototype feature.

