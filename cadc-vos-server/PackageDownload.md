A transfer to download a package (a compressed collection of resources) uses a different sequence of job phases 
then a typical transfer to push or pull a resource for a VOSpace.

A sequence for a typical synchronous transfer:

- POST a transfer document to the /<service>/synctrans endpoint.
- A new job is created in the PENDING phase, then a redirect is issued to /<service>/synctrans/<jobID>/run.
- GET the redirect, which set the job phase from PENDING to QUEUED, and runs the TransferRunner.
- The TransferRunner  validates the job and transfer, runs the job, updating the job phase from QUEUED to EXECUTING.
- When the job has finished the TransferRunner either:
    - issues a redirect to /<service>/<jobID>/results/transferDetails with the transfer results
    - issues a redirect for a VOSpace resource
- GET the redirect for the results or a resource, and the job phase is updated from EXECUTING to COMPLETED.

A Transfer to download a package uses a different sequence of job phases. When the TransferRunner processes 
a Transfer with a Package View, it puts the job into the suspended phase. In UWS the SUSPENDED phase indicates
that the job is suspending awaiting further processing. After the TransferRunner suspends the job, 
it redirects to the PackageRunner to run the job, return the package, and complete the job.

The sequence for a synchronous package transfer:

- POST a transfer document with a package view to the /<service>/synctrans endpoint.
- A new job is created in the PENDING phase, then a redirect is issued to /<service>/synctrans/<jobID>/run.
- GET the redirect, which set the job phase from PENDING to QUEUED, and runs the TransferRunner.
- The TransferRunner validates the job and transfer, finds a package view in the transfer, 
  updates the job from QUEUED to SUSPENDED (skipping EXECUTING), and issues a redirect 
  to the /<service>/pkg/<jobID>/run endpoint.
- GET the redirect which runs the VOSpacePackageRunner.
- The VOSPacePackageRunner gets the job, updates the job to from SUSPENDED to EXECUTING, 
  generates the package, and streams the package to the client.
- When the job has finished the VOSPacePackageRunner updates the job phase from EXECUTING to COMPLETED.
