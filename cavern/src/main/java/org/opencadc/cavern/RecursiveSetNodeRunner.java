/*
************************************************************************
*******************  CANADIAN ASTRONOMY DATA CENTRE  *******************
**************  CENTRE CANADIEN DE DONNÉES ASTRONOMIQUES  **************
*
*  (c) 2023.                            (c) 2023.
*  Government of Canada                 Gouvernement du Canada
*  National Research Council            Conseil national de recherches
*  Ottawa, Canada, K1A 0R6              Ottawa, Canada, K1A 0R6
*  All rights reserved                  Tous droits réservés
*
*  NRC disclaims any warranties,        Le CNRC dénie toute garantie
*  expressed, implied, or               énoncée, implicite ou légale,
*  statutory, of any kind with          de quelque nature que ce
*  respect to the software,             soit, concernant le logiciel,
*  including without limitation         y compris sans restriction
*  any warranty of merchantability      toute garantie de valeur
*  or fitness for a particular          marchande ou de pertinence
*  purpose. NRC shall not be            pour un usage particulier.
*  liable in any event for any          Le CNRC ne pourra en aucun cas
*  damages, whether direct or           être tenu responsable de tout
*  indirect, special or general,        dommage, direct ou indirect,
*  consequential or incidental,         particulier ou général,
*  arising from the use of the          accessoire ou fortuit, résultant
*  software.  Neither the name          de l'utilisation du logiciel. Ni
*  of the National Research             le nom du Conseil National de
*  Council of Canada nor the            Recherches du Canada ni les noms
*  names of its contributors may        de ses  participants ne peuvent
*  be used to endorse or promote        être utilisés pour approuver ou
*  products derived from this           promouvoir les produits dérivés
*  software without specific prior      de ce logiciel sans autorisation
*  written permission.                  préalable et particulière
*                                       par écrit.
*
*  $Revision: 1 $
*
************************************************************************
*/

package org.opencadc.cavern;

import ca.nrc.cadc.rest.SyncOutput;
import ca.nrc.cadc.util.ThrowableUtil;
import ca.nrc.cadc.uws.ErrorSummary;
import ca.nrc.cadc.uws.ErrorType;
import ca.nrc.cadc.uws.ExecutionPhase;
import ca.nrc.cadc.uws.Job;
import ca.nrc.cadc.uws.JobInfo;
import ca.nrc.cadc.uws.server.JobRunner;
import ca.nrc.cadc.uws.server.JobUpdater;
import ca.nrc.cadc.uws.util.JobLogInfo;
import ca.nrc.cadc.vos.ContainerNode;
import ca.nrc.cadc.vos.Node;
import ca.nrc.cadc.vos.NodeProperty;
import ca.nrc.cadc.vos.NodeReader;
import ca.nrc.cadc.vos.VOSURI;
import ca.nrc.cadc.vos.server.NodePersistence;
import ca.nrc.cadc.vos.server.auth.VOSpaceAuthorizer;

import java.io.FileNotFoundException;
import java.security.AccessControlException;
import java.util.Date;
import java.util.List;
import org.apache.log4j.Logger;

/**
 * Class to set node properties on the target node and all child
 * nodes recursively.
 *
 * @author majorb, yeunga
 *
 */
public class RecursiveSetNodeRunner implements JobRunner {

    private static Logger log = Logger.getLogger(RecursiveSetNodeRunner.class);

    private static final long PHASE_CHECK_INTERVAL = 1000; // 1 second
    private static final int CHILD_BATCH_SIZE = 50;

    private Job job;
    private JobUpdater jobUpdater;
    private long lastPhaseCheck = System.currentTimeMillis();
    private VOSpaceAuthorizer vospaceAuthorizer;
    private NodePersistence nodePersistence;
    private long updateCount = 0;
    private long errorCount = 0;
    private JobLogInfo logInfo;

    @Override
    public void setJobUpdater(JobUpdater jobUpdater) {
        this.jobUpdater = jobUpdater;
    }

    @Override
    public void setJob(Job job) {
        this.job = job;
    }

    @Override
    public void setSyncOutput(SyncOutput syncOutput) {
        // not used
    }

    @Override
    public void run() {
        log.debug("RUN RecursiveSetNodeRunner");
        logInfo = new JobLogInfo(job);

        String startMessage = logInfo.start();
        log.info(startMessage);

        long t1 = System.currentTimeMillis();
        doit();
        long t2 = System.currentTimeMillis();

        logInfo.setElapsedTime(t2 - t1);

        String endMessage = logInfo.end();
        log.info(endMessage);
    }

    private void doit() {
        try {
            // set the phase to executing
            ExecutionPhase ep = jobUpdater.setPhase(
                job.getID(), ExecutionPhase.QUEUED, ExecutionPhase.EXECUTING, new Date());

            if (ep == null) {
                throw new IllegalStateException(
                        "Could not change the job phase from " + ExecutionPhase.QUEUED +
                        " to " + ExecutionPhase.EXECUTING);
            }

            JobInfo jobInfo = job.getJobInfo();
            Node node = null;
            if ( jobInfo != null && jobInfo.getContent() != null && !jobInfo.getContent().isEmpty()
                && jobInfo.getContentType().equalsIgnoreCase("text/xml") ) { 
                log.debug("recursive set node XML: \n\n" + jobInfo.getContent());
                NodeReader reader = new NodeReader();
                node = reader.read(jobInfo.getContent());
            }

            log.debug("node: " + node);
            if (node == null) {
                this.sendError("NotFound");
                return;
            }

            List<NodeProperty> newProperties = node.getProperties();

            // Create the node persistence and authorizer objects
            nodePersistence = new FileSystemNodePersistence();
            vospaceAuthorizer = new VOSpaceAuthorizer();
            vospaceAuthorizer.setNodePersistence(nodePersistence);
            vospaceAuthorizer.setDisregardLocks(true);

            // get the persistent version of the node
            Node persistentNode = (Node) vospaceAuthorizer.getReadPermission(node.getUri().getURI());

            applyNodeProperties(persistentNode, newProperties);

            // set the appropriate end phase and error summary
            ErrorSummary error = null;
            ExecutionPhase endPhase = ExecutionPhase.COMPLETED;
            if (errorCount > 0 || updateCount == 0) {
                String message = String.format("Success count: %s Failure count: %s", updateCount, errorCount);
                // allowed to have a null error type?
                error = new ErrorSummary(message, ErrorType.TRANSIENT);
            }
            if (updateCount == 0) {
                endPhase = ExecutionPhase.ERROR;
            }

            ep = jobUpdater.setPhase(job.getID(), ExecutionPhase.EXECUTING, endPhase, error, new Date());

            if (!endPhase.equals(ep)) {
                log.warn("Could not change the job phase from " + ExecutionPhase.EXECUTING +
                        " to " + endPhase);
            }
        } catch (FileNotFoundException e) {
            sendError("NotFound");
            return;
        } catch (AccessControlException e) {
            sendError("PermissionDenied");
            return;
        } catch (JobAbortedException e) {
            // nothing to do but return
            return;
        } catch (Throwable t) {
            log.error("Unexpected exception", t);
            // if the cause of this throwable is an InterruptedException, and if
            // the job has been aborted, then this can be considered a normal
            // abort procedure
            if (t instanceof InterruptedException || ThrowableUtil.isACause(t, InterruptedException.class)) {
                try {
                    ExecutionPhase ep = jobUpdater.getPhase(job.getID());
                    if (ExecutionPhase.ABORTED.equals(ep)) {
                        return;
                    }
                } catch (Exception e) {
                    log.error("Could not check job phase: ", e);
                }
            }

            sendError("Unexpected Exception: " + t.getMessage());
            return;
        }
    }

    /**
     * Traversing depth first, update the node properties recursively
     * @param node The node to update
     * @param properties The new properties
     * @throws Exception
     */
    private void applyNodeProperties(Node node, List<NodeProperty> properties)
        throws Exception {
        if ( Thread.currentThread().isInterrupted()) {
            log.debug("INTERRUPT");
            throw new InterruptedException();
        }
        // check the write permission
        try {
            vospaceAuthorizer.getWritePermission(node);
        } catch (AccessControlException e) {
            // don't apply the update for this node
            errorCount++;
            return;
        }

        // check the job phase for abort or illegal state
        checkJobPhase();

        // apply the node property changes
        try {
            log.debug("Applying node properties to: " + node.getUri());
            nodePersistence.updateProperties(node, properties);
            updateCount++;
        } catch (Exception e) {
            // unexepected error, abort updates for this node
            log.warn("Failed to update properties on node: " + node, e);
            errorCount++;
            return;
        }

        // apply updates to the children
        if (node instanceof ContainerNode) {
            ContainerNode container = (ContainerNode) node;

            // loop through the batches of children
            boolean firstBatch = true;
            VOSURI lastChildURI = null;
            while (firstBatch || lastChildURI != null) {
                firstBatch = false;
                log.debug("getChildren of " + container.getUri() + " from " + lastChildURI);
                nodePersistence.getChildren(container, lastChildURI, CHILD_BATCH_SIZE);

                for (Node child : container.getNodes()) {
                    // don't update the first child if this is not the
                    // first batch request - it's been done already.
                    if (lastChildURI == null || !child.getUri().equals(lastChildURI)) {
                        this.applyNodeProperties(child, properties);
                    }
                }

                if (container.getNodes().size() == CHILD_BATCH_SIZE) {
                    lastChildURI = container.getNodes().get(CHILD_BATCH_SIZE - 1).getUri();
                } else {
                    lastChildURI = null;
                }

                // remove already completed children
                container.getNodes().clear();
            }
        }
    }

    /**
     * If a minimum of PHASE_CHECK_INTERVAL time has passed, check
     * to ensure the job is still in the EXECUTING PHASE
     */
    private void checkJobPhase()
            throws Exception {
        // only check phase if a minimum of PHASE_CHECK_INTERVAL time has passed
        long now = System.currentTimeMillis();
        long diff = now - lastPhaseCheck;
        log.debug("Last phase check diff: " + diff);
        if (diff > PHASE_CHECK_INTERVAL) {
            // reset the last phase check time
            lastPhaseCheck = now;

            log.debug("Checking job phase");
            ExecutionPhase ep = jobUpdater.getPhase(job.getID());
            log.debug("Job phase is: " + ep);
            if (ExecutionPhase.ABORTED.equals(ep)) {
                throw new JobAbortedException(job);
            }

            if (!ExecutionPhase.EXECUTING.equals(ep)) {
                throw new IllegalStateException("Job should be in phase " +
                    ExecutionPhase.EXECUTING + " but is in phase " + ep);
            }
        }
    }

    /**
     * Set the job execution phase to error and save the error message.
     * @param message
     */
    private void sendError(String message) {
        logInfo.setSuccess(false);
        logInfo.setMessage(message);

        // set the phase to error
        String errorSummary = String.format(message);
        ErrorSummary error = new ErrorSummary(errorSummary, ErrorType.FATAL);

        try {
            ExecutionPhase ep = jobUpdater.setPhase(job.getID(), ExecutionPhase.EXECUTING,
                    ExecutionPhase.ERROR, error, new Date());

            if (!ExecutionPhase.ERROR.equals(ep)) {
                log.warn("Could not change the job phase from " + ExecutionPhase.EXECUTING +
                        " to " + ExecutionPhase.ERROR + " because it is " + jobUpdater.getPhase(job.getID()));
            }
        } catch (Throwable t) {
            log.error("Failed to change the job phase from " + ExecutionPhase.EXECUTING +
                    " to " + ExecutionPhase.ERROR, t);
        }
    }
}
