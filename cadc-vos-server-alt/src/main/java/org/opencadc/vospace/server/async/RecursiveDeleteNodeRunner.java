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
 *  This file is part of the             Ce fichier fait partie du projet
 *  OpenCADC project.                    OpenCADC.
 *
 *  OpenCADC is free software:           OpenCADC est un logiciel libre ;
 *  you can redistribute it and/or       vous pouvez le redistribuer ou le
 *  modify it under the terms of         modifier suivant les termes de
 *  the GNU Affero General Public        la “GNU Affero General Public
 *  License as published by the          License” telle que publiée
 *  Free Software Foundation,            par la Free Software Foundation
 *  either version 3 of the              : soit la version 3 de cette
 *  License, or (at your option)         licence, soit (à votre gré)
 *  any later version.                   toute version ultérieure.
 *
 *  OpenCADC is distributed in the       OpenCADC est distribué
 *  hope that it will be useful,         dans l’espoir qu’il vous
 *  but WITHOUT ANY WARRANTY;            sera utile, mais SANS AUCUNE
 *  without even the implied             GARANTIE : sans même la garantie
 *  warranty of MERCHANTABILITY          implicite de COMMERCIALISABILITÉ
 *  or FITNESS FOR A PARTICULAR          ni d’ADÉQUATION À UN OBJECTIF
 *  PURPOSE.  See the GNU Affero         PARTICULIER. Consultez la Licence
 *  General Public License for           Générale Publique GNU Affero
 *  more details.                        pour plus de détails.
 *
 *  You should have received             Vous devriez avoir reçu une
 *  a copy of the GNU Affero             copie de la Licence Générale
 *  General Public License along         Publique GNU Affero avec
 *  with OpenCADC.  If not, see          OpenCADC ; si ce n’est
 *  <http://www.gnu.org/licenses/>.      pas le cas, consultez :
 *                                       <http://www.gnu.org/licenses/>.
 *
 ************************************************************************
 */

package org.opencadc.vospace.server.async;

import ca.nrc.cadc.auth.AuthenticationUtil;
import ca.nrc.cadc.io.ResourceIterator;
import ca.nrc.cadc.rest.SyncOutput;
import ca.nrc.cadc.util.MultiValuedProperties;
import ca.nrc.cadc.util.ThrowableUtil;
import ca.nrc.cadc.uws.ErrorSummary;
import ca.nrc.cadc.uws.ErrorType;
import ca.nrc.cadc.uws.ExecutionPhase;
import ca.nrc.cadc.uws.Job;
import ca.nrc.cadc.uws.Parameter;
import ca.nrc.cadc.uws.Result;
import ca.nrc.cadc.uws.server.JobRunner;
import ca.nrc.cadc.uws.server.JobUpdater;
import ca.nrc.cadc.uws.util.JobLogInfo;
import java.io.FileNotFoundException;
import java.net.URI;
import java.security.AccessControlException;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import javax.naming.Context;
import javax.naming.InitialContext;
import javax.naming.NamingException;
import javax.security.auth.Subject;
import org.apache.log4j.Logger;
import org.opencadc.vospace.ContainerNode;
import org.opencadc.vospace.Node;
import org.opencadc.vospace.VOSURI;
import org.opencadc.vospace.server.LocalServiceURI;
import org.opencadc.vospace.server.NodeFault;
import org.opencadc.vospace.server.NodePersistence;
import org.opencadc.vospace.server.PathResolver;
import org.opencadc.vospace.server.Utils;
import org.opencadc.vospace.server.auth.VOSpaceAuthorizer;

/**
 * Class to delete the target node and all child nodes recursively.
 *
 *
 */
public class RecursiveDeleteNodeRunner implements JobRunner {

    private static Logger log = Logger.getLogger(RecursiveDeleteNodeRunner.class);

    // containers are not locked while being emptied for the recursive delete and might need a few revisits in order
    // for the operation to succeed.
    private static final long PHASE_CHECK_INTERVAL = 1000; // 1 second
    private static final long MAX_ERROR_BEFORE_ABORT = 100;

    private Job job;
    private JobUpdater jobUpdater;
    private long lastPhaseCheck = System.currentTimeMillis();
    private VOSpaceAuthorizer vospaceAuthorizer;
    private NodePersistence nodePersistence;
    private long deleteCount = 0;
    private long errorCount = 0;
    private JobLogInfo logInfo;
    private String appName;

    public URI resourceID;

    public RecursiveDeleteNodeRunner() {
    }

    @Override
    public void setAppName(String appName) {
        String jndiNodePersistence = appName + "-" + NodePersistence.class.getName();
        try {
            Context ctx = new InitialContext();
            this.nodePersistence = (NodePersistence) ctx.lookup(jndiNodePersistence);
            this.vospaceAuthorizer = new VOSpaceAuthorizer(nodePersistence);
        } catch (NamingException oops) {
            throw new RuntimeException("BUG: NodePersistence implementation not found with JNDI key " + jndiNodePersistence, oops);
        }
    }

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
        log.debug("RUN RecursiveDeleteNodeRunner");
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
                        "Could not change the job phase from " + ExecutionPhase.QUEUED
                                + " to " + ExecutionPhase.EXECUTING);
            }

            VOSURI nodeURI = null;
            for (Parameter param : job.getParameterList()) {
                if (param.getName().equalsIgnoreCase("target")) {
                    nodeURI = new VOSURI(URI.create(param.getValue()));
                    break;
                }
            }

            if (nodeURI == null) {
                throw new IllegalArgumentException("target argument required");
            }

            log.debug("node: " + nodeURI);

            PathResolver pathResolver = new PathResolver(nodePersistence, vospaceAuthorizer, true);
            String nodePath = nodeURI.getPath();
            Node serverNode = pathResolver.getNode(nodePath);
            if (serverNode == null) {
                throw NodeFault.NodeNotFound.getStatus(nodePath);
            }

            ContainerNode parent = serverNode.parent;
            Subject caller = AuthenticationUtil.getCurrentSubject();
            if (!vospaceAuthorizer.hasSingleNodeWritePermission(parent, caller)) {
                throw NodeFault.PermissionDenied.getStatus(nodePath);
            }

            if (serverNode instanceof ContainerNode) {
                try {
                    deleteContainer((ContainerNode) serverNode, caller);
                } catch (JobAbortedException ex) {
                    // nothing to do here
                }
            } else {
                try {
                    nodePersistence.delete(serverNode);
                    log.debug("Deleted data node " + nodePath);
                } catch (Exception ex) {
                    log.debug("Cannot recursively delete node " + nodePath, ex);
                    incErrorCount();
                }
            }

            // set the appropriate end phase and error summary
            ErrorSummary error = null;
            ExecutionPhase endPhase = ExecutionPhase.COMPLETED;
            if (deleteCount == 0) {
                endPhase = ExecutionPhase.ERROR;
                ep = jobUpdater.setPhase(job.getID(), ExecutionPhase.EXECUTING, endPhase, error, new Date());
            } else {
                List<Result> results = new ArrayList<>();
                results.add(new Result("delcount", URI.create("final:" + deleteCount)));
                if (errorCount > 0) {
                    endPhase = ExecutionPhase.ABORTED;
                    results.add(new Result("errorcount", URI.create("final:" + errorCount)));
                    ep = jobUpdater.setPhase(job.getID(), ExecutionPhase.EXECUTING, endPhase, results, new Date());
                } else {
                    ep = jobUpdater.setPhase(job.getID(), ExecutionPhase.EXECUTING, endPhase, results, new Date());
                }
            }
            if (!endPhase.equals(ep)) {
                log.warn("Could not change the job phase from " + ExecutionPhase.EXECUTING + " to " + endPhase);
            }
        } catch (FileNotFoundException e) {
            sendError("NotFound");
        } catch (AccessControlException e) {
            sendError("PermissionDenied");
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
        }
    }

    private boolean deleteContainer(ContainerNode node, Subject caller) throws Exception {

        boolean errors = false;
        List<ContainerNode> childContainers = new ArrayList<>();
        log.debug("Deleting container " + Utils.getPath(node));
        boolean writableParent = vospaceAuthorizer.hasSingleNodeWritePermission(node, caller);
        try (ResourceIterator<Node> iterator = nodePersistence.iterator(node, null, null)) {
            while (iterator.hasNext()) {
                checkJobPhase();
                Node child = iterator.next();
                if (child instanceof ContainerNode) {
                    childContainers.add((ContainerNode) child);
                } else {
                    if (writableParent) {
                        nodePersistence.delete(child);
                        log.debug("Deleted non-container node " + Utils.getPath(child));
                        deleteCount++;
                    } else {
                        log.debug("Unauthorized to delete node " + Utils.getPath(child));
                        errors = true;
                        incErrorCount();
                    }
                }
            }
            if (!childContainers.isEmpty()) {
                // descend down one level
                for (ContainerNode cn : childContainers) {
                    errors |= deleteContainer(cn, caller);
                }
            }
            if (!errors) {
                // delete the empty container
                if (vospaceAuthorizer.hasSingleNodeWritePermission(node.parent, caller)) {
                    nodePersistence.delete(node);
                    log.debug("Deleted container node " + Utils.getPath(node));
                    deleteCount++;
                } else {
                    log.debug("Failed to delete container node " + Utils.getPath(node));
                    incErrorCount();
                    errors = true;
                }
            }
        }
        return errors;
    }

    private void incErrorCount() throws JobAbortedException {
        if (++errorCount > MAX_ERROR_BEFORE_ABORT) {
            throw new JobAbortedException(job);
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
                throw new IllegalStateException("Job should be in phase "
                        + ExecutionPhase.EXECUTING + " but is in phase " + ep);
            }
        }
    }

    /**
     * Set the job execution phase to error and save the error message.
     * @param message - error message
     */
    private void sendError(String message) {
        logInfo.setSuccess(false);
        logInfo.setMessage(message);

        // set the phase to error
        ErrorSummary error = new ErrorSummary(message, ErrorType.FATAL);

        try {
            ExecutionPhase ep = jobUpdater.setPhase(job.getID(), ExecutionPhase.EXECUTING,
                    ExecutionPhase.ERROR, error, new Date());

            if (!ExecutionPhase.ERROR.equals(ep)) {
                log.warn("Could not change the job phase from " + ExecutionPhase.EXECUTING
                        + " to " + ExecutionPhase.ERROR + " because it is " + jobUpdater.getPhase(job.getID()));
            }
        } catch (Throwable t) {
            log.error("Failed to change the job phase from " + ExecutionPhase.EXECUTING
                    + " to " + ExecutionPhase.ERROR, t);
        }
    }
}
