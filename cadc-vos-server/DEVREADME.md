PackageRunner call sequence
- POST transfer document to the /cavern/pkg sync endpoint
- creates new job in the PENDING state and redirects to /cavern/pkg/<jobID>/run
- GET the rediect which set the state from PENDING to QUEUED
- VospacePackageRunner runs, setting the state from QUEUED to EXECUTING
- VospacePackageRunner builds and writes the package
- VospacePackageRunner sets state from EXECUTING to COMPLETED when finished.
