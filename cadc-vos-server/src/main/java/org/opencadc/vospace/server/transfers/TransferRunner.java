/*
 ************************************************************************
 *******************  CANADIAN ASTRONOMY DATA CENTRE  *******************
 **************  CENTRE CANADIEN DE DONNÉES ASTRONOMIQUES  **************
 *
 *  (c) 2009.                            (c) 2009.
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

package org.opencadc.vospace.server.transfers;

import ca.nrc.cadc.auth.AuthMethod;
import ca.nrc.cadc.auth.AuthenticationUtil;
import ca.nrc.cadc.auth.NotAuthenticatedException;
import ca.nrc.cadc.io.ByteLimitExceededException;
import ca.nrc.cadc.net.TransientException;
import ca.nrc.cadc.reg.Standards;
import ca.nrc.cadc.reg.client.RegistryClient;
import ca.nrc.cadc.rest.SyncOutput;
import ca.nrc.cadc.util.StringUtil;
import ca.nrc.cadc.uws.ErrorSummary;
import ca.nrc.cadc.uws.ErrorType;
import ca.nrc.cadc.uws.ExecutionPhase;
import ca.nrc.cadc.uws.Job;
import ca.nrc.cadc.uws.JobInfo;
import ca.nrc.cadc.uws.Parameter;
import ca.nrc.cadc.uws.ParameterUtil;
import ca.nrc.cadc.uws.server.JobNotFoundException;
import ca.nrc.cadc.uws.server.JobPersistenceException;
import ca.nrc.cadc.uws.server.JobRunner;
import ca.nrc.cadc.uws.server.JobUpdater;
import ca.nrc.cadc.uws.util.JobLogInfo;
import java.io.IOException;
import java.io.PrintWriter;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.security.AccessControlException;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import javax.naming.Context;
import javax.naming.InitialContext;
import javax.naming.NamingException;
import org.apache.log4j.Logger;
import org.opencadc.vospace.LinkingException;
import org.opencadc.vospace.NodeBusyException;
import org.opencadc.vospace.NodeLockedException;
import org.opencadc.vospace.NodeNotFoundException;
import org.opencadc.vospace.VOSURI;
import org.opencadc.vospace.server.LocalServiceURI;
import org.opencadc.vospace.server.NodePersistence;
import org.opencadc.vospace.server.auth.VOSpaceAuthorizer;
import org.opencadc.vospace.transfer.Direction;
import org.opencadc.vospace.transfer.Protocol;
import org.opencadc.vospace.transfer.Transfer;
import org.opencadc.vospace.transfer.TransferReader;

public class TransferRunner implements JobRunner {

    private static final Logger log = Logger.getLogger(TransferRunner.class);

    private static final String VOS_PREFIX = "vos://";

    private Job job;
    private JobUpdater jobUpdater;
    private SyncOutput syncOutput;
    private boolean syncOutputCommit = false;
    private JobLogInfo logInfo;

    private final RegistryClient regClient = new RegistryClient();
    private NodePersistence nodePersistence;
    private VOSpaceAuthorizer authorizer;
    private LocalServiceURI localServiceURI;

    public TransferRunner() {
        
    }
    
    @Override
    public void setJob(final Job job) {
        this.job = job;
    }
    
    @Override
    public void setAppName(String appName) {
        String jndiNodePersistence = appName + "-" + NodePersistence.class.getName();
        try {
            Context ctx = new InitialContext();
            this.nodePersistence = (NodePersistence) ctx.lookup(jndiNodePersistence);
            this.authorizer = new VOSpaceAuthorizer(nodePersistence);        
            this.localServiceURI = new LocalServiceURI(nodePersistence.getResourceID());
        } catch (NamingException oops) {
            throw new RuntimeException("BUG: NodePersistence implementation not found with JNDI key " + jndiNodePersistence, oops);
        }
    }

    @Override
    public void setJobUpdater(JobUpdater ju) {
        this.jobUpdater = ju;
    }

    @Override
    public void setSyncOutput(SyncOutput so) {
        this.syncOutput = so;
    }

    /**
     * Run the job. This method is invoked by the JobExecutor to run the job. The run()
     * method is responsible for setting the following Job state: phase, error, resultList.
     */
    @Override
    public void run() {
        log.debug("RUN TransferRunner");
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

    // custom transfer negotiation protocol that uses 3 single-valued parameters
    private Transfer createTransfer(List<Parameter> params) throws URISyntaxException {
        String suri = ParameterUtil.findParameterValue("TARGET", params);
        String sdir = ParameterUtil.findParameterValue("DIRECTION", params);
        String sproto = ParameterUtil.findParameterValue("PROTOCOL", params);
        log.debug("createTransfer: " + suri + " " + sdir + " " + sproto);
        if (!StringUtil.hasText(suri)
                || !StringUtil.hasText(sdir)
                || !StringUtil.hasText(sproto)) {
            throw new IllegalArgumentException("missing parameters: "
                    + "TARGET=" + suri + " DIRECTION=" + sdir + " PROTOCOL=" + sproto);
        }
        VOSURI target;
        try {
            URI uri = new URI(suri);
            target = new VOSURI(uri);
        } catch (URISyntaxException ex) {
            throw new IllegalArgumentException("InvalidArgument : invalid target URI " + suri);
        }

        Direction dir = new Direction(sdir);

        List<Protocol> plist = new ArrayList<>();
        plist.add(new Protocol(new URI(sproto)));

        // also add a protocol with current securityMethod
        AuthMethod am = AuthenticationUtil.getAuthMethod(AuthenticationUtil.getCurrentSubject());
        Protocol proto = new Protocol(new URI(sproto));
        if (am != null && !AuthMethod.ANON.equals(am)) {
            proto.setSecurityMethod(Standards.getSecurityMethod(am));
            plist.add(proto);
        }

        log.debug("createTransfer: " + target + " " + dir + " " + proto);
        Transfer ret = new Transfer(target.getURI(), dir);
        ret.getProtocols().addAll(plist);
        return ret;
    }

    private List<Parameter> getAdditionalParameters(List<Parameter> params) {
        List<Parameter> ret = new ArrayList<Parameter>();
        for (Parameter param : params) {
            if (!"TARGET".equalsIgnoreCase(param.getName())
                    && !"DIRECTION".equalsIgnoreCase(param.getName())
                    && !"PROTOCOL".equalsIgnoreCase(param.getName())) {
                ret.add(param);
            }
        }
        return ret;
    }

    private void doit() {
        Transfer transfer = null;
        boolean customPushPull = false;
        boolean pkgRedirect = false;
        VOSpaceTransfer trans = null;
        List<Parameter> additionalParameters = null;
        try {
            // Get the transfer document from the JobInfo
            JobInfo jobInfo = job.getJobInfo();
            try {
                if (jobInfo == null && !job.getParameterList().isEmpty()) {
                    transfer = createTransfer(job.getParameterList());
                    customPushPull = true;
                } else if (jobInfo != null && jobInfo.getContent() != null && !jobInfo.getContent().isEmpty()
                        && jobInfo.getContentType().equalsIgnoreCase("text/xml")) {
                    log.debug("transfer XML: \n\n" + jobInfo.getContent());
                    TransferReader reader = new TransferReader();
                    transfer = reader.read(jobInfo.getContent(), VOSURI.SCHEME);
                    log.debug("*** transfer version: " + transfer.version);
                }
            } catch (IllegalArgumentException ex) {
                String msg = "Invalid input: " + ex.getMessage();
                log.debug(msg, ex);
                sendError(job.getExecutionPhase(), ErrorType.FATAL, msg, HttpURLConnection.HTTP_BAD_REQUEST, true);
                return;
            }

            additionalParameters = getAdditionalParameters(job.getParameterList());

            log.debug("transfer: " + transfer);
            if (transfer == null) {
                sendError(ErrorType.FATAL, "could not create Transfer from request", HttpURLConnection.HTTP_BAD_REQUEST);
                return;
            }

            Direction direction = transfer.getDirection();
            if (!isValidDirection(direction, customPushPull)) {
                sendError(ErrorType.FATAL, "InternalFault (invalid direction: " + direction + ")", HttpURLConnection.HTTP_BAD_REQUEST);
                return;
            }

            // check if the transfer view has a configured endpoint
            if (transfer.getView() != null && transfer.getView().getURI().equals(Standards.PKG_10)) {
                URL accessURL = regClient.getAccessURL(nodePersistence.getResourceID());
                String accessUrl = accessURL.toExternalForm().replace("capabilities", "pkg");

                // set the job phase to SUSPENDED (the job is suspended pending further processing.)
                // the PackageRunner expects a job to be SUSPENDED before it will execute the job.
                jobUpdater.setPhase(job.getID(), ExecutionPhase.QUEUED, ExecutionPhase.SUSPENDED);

                // redirect to view endpoint
                String location = String.format("%s/%s/run", accessUrl, job.getID());
                log.debug("pkg location: " + location);
                syncOutput.setHeader("Location", location);
                syncOutput.setCode(HttpURLConnection.HTTP_SEE_OTHER);
                pkgRedirect = true;
                return;
            }

            try {
                if (direction.equals(Direction.pushToVoSpace)) {
                    trans = new PushToVOSpaceNegotiation(nodePersistence, jobUpdater, job, transfer);
                } else if (direction.equals(Direction.pullFromVoSpace)) {
                    trans = new PullFromVOSpaceNegotiation(nodePersistence, jobUpdater, job, transfer);
                } else if (direction.equals(Direction.pullToVoSpace)) {
                    trans = new PullToVOSpaceAction(nodePersistence, jobUpdater, job, transfer);
                } else if (direction.equals(Direction.pushFromVoSpace)) {
                    trans = new PushFromVOSpaceAction(nodePersistence, jobUpdater, job, transfer);
                } else if (direction.equals(Direction.BIDIRECTIONAL)) {
                    trans = new BiDirectionalTransferNegotiation(nodePersistence, jobUpdater, job, transfer);
                } else {
                    trans = new InternalTransferAction(nodePersistence, jobUpdater, job, transfer);
                }

                trans.validateView();
                trans.doAction();
            } catch (TransferException ex) {
                sendError(job.getExecutionPhase(), ErrorType.FATAL, ex.getMessage(), HttpURLConnection.HTTP_BAD_REQUEST, true);
            } catch (NodeNotFoundException nfe) {
                log.debug("Node not found: " + nfe.getMessage());
                sendError(job.getExecutionPhase(), ErrorType.FATAL, "NodeNotFound", HttpURLConnection.HTTP_NOT_FOUND, true);
                return;
            } catch (NodeBusyException be) {
                log.debug("Node busy: " + be.getMessage());
                sendError(job.getExecutionPhase(), ErrorType.FATAL, "NodeBusy", HttpURLConnection.HTTP_CONFLICT, true);
                return;
            } catch (NodeLockedException le) {
                log.debug("Node locked: " + le.getMessage());
                sendError(job.getExecutionPhase(), ErrorType.FATAL, "NodeLocked", 423, true);  // 423 = Locked (WebDAV; RFC 4918)
                return;
            } catch (ByteLimitExceededException le) {
                log.debug("Quota exceeded: " + le.getMessage());
                sendError(job.getExecutionPhase(), ErrorType.FATAL, "QuotaExceeded", HttpURLConnection.HTTP_ENTITY_TOO_LARGE, true);
                return;
            } catch (AccessControlException ace) {
                log.debug("permission denied", ace);
                sendError(job.getExecutionPhase(), ErrorType.FATAL, "PermissionDenied", HttpURLConnection.HTTP_FORBIDDEN, true);
                return;
            } catch (NotAuthenticatedException ne) {
                log.debug("not authenticated", ne);
                sendError(job.getExecutionPhase(), ErrorType.FATAL, "NotAuthenticated", HttpURLConnection.HTTP_UNAUTHORIZED, true);
                return;
            } catch (IllegalArgumentException ex) {
                // target node was not a DataNode
                String msg = "Invalid Argument (target node is not a DataNode)";
                log.debug(msg, ex);
                sendError(job.getExecutionPhase(), ErrorType.FATAL, msg, HttpURLConnection.HTTP_BAD_REQUEST, true);
                return;
            } catch (LinkingException link) {
                String msg = "Link Exception: " + link.getMessage();
                log.debug(msg, link);
                // now set the job to error
                sendError(job.getExecutionPhase(), ErrorType.FATAL, msg, HttpURLConnection.HTTP_BAD_REQUEST, true);
                return;
            } catch (UnsupportedOperationException ex) {
                ex.printStackTrace();
                String msg = "Unsupported Operation: " + ex.getMessage();
                log.debug(msg, ex);
                // now set the job to error
                sendError(job.getExecutionPhase(), ErrorType.FATAL, msg, HttpURLConnection.HTTP_NOT_IMPLEMENTED, true);
                return;
            }
        } catch (TransientException e) {
            log.debug(e);
            try {
                String message = e.getClass().getSimpleName() + ":" + e.getMessage();
                sendError(job.getExecutionPhase(), ErrorType.TRANSIENT, message, 503, false);
            } catch (Throwable t) {
                log.error("failed to persist error", t);
                log.error("Original error", e);
            }
        } catch (Throwable t) {

            // TODO: Check if the cause of the throwable was an interrupted exception.
            // If so, and if the job is in the 'aborted' state, it should be considered
            // a normal operation.
            log.error("BUG", t);
            try {
                String message = t.getClass().getSimpleName() + ":" + t.getMessage();
                sendError(job.getExecutionPhase(), ErrorType.FATAL, message, HttpURLConnection.HTTP_INTERNAL_ERROR, false);
            } catch (Throwable t2) {
                log.error("failed to persist error", t2);
                log.error("Original error", t);
            }
        } finally {
            if (!pkgRedirect) {
                try {
                    doTransferRedirect(transfer, additionalParameters);
                } catch (Throwable t) {
                    log.error("failed to do transfer redirect", t);
                    try {
                        sendError(ExecutionPhase.EXECUTING, ErrorType.FATAL, t.getMessage(), HttpURLConnection.HTTP_INTERNAL_ERROR, false);
                    } catch (Exception e) {
                        log.error("Failed to update job.", e);
                    }
                }
            }
            log.debug("DONE");
        }
    }

    private void doTransferRedirect(Transfer transfer, List<Parameter> additionalParameters) {
        if (syncOutput != null && !syncOutputCommit) {
            if (!job.getParameterList().isEmpty() && transfer != null) {
                try {
                    if (transfer.getTargets().isEmpty()) {
                        throw new UnsupportedOperationException("No targets found.");
                    }

                    if (transfer.getTargets().size() > 1) {
                        // Multiple targets are currently only supported for package transfers
                        throw new UnsupportedOperationException("More than one target found. (" + transfer.getTargets().size() + ")");
                    }
                    VOSURI target = new VOSURI(transfer.getTargets().get(0));
                    
                    TransferGenerator gen = nodePersistence.getTransferGenerator();
                    //List<Protocol> plist = TransferUtil.getTransferEndpoints(trans, job, additionalParameters);
                    List<Protocol> plist = gen.getEndpoints(target, transfer, additionalParameters);
                    if (plist.isEmpty()) {
                        sendError(ExecutionPhase.EXECUTING,
                                ErrorType.FATAL, "requested transfer specs not supported",
                                HttpURLConnection.HTTP_BAD_REQUEST,
                                true);
                        return;
                    }
                    Protocol proto = plist.get(0);
                    String loc = proto.getEndpoint();
                    log.debug("Location: " + loc);
                    syncOutput.setHeader("Location", loc);
                    syncOutput.setResponseCode(HttpURLConnection.HTTP_SEE_OTHER);
                    return;
                } catch (Exception e) {
                    throw new RuntimeException("Failed to create protocol list: " + e.getMessage(), e);
                }
            }

            // standard redirect
            StringBuilder sb = new StringBuilder();
            sb.append("/").append(job.getID()).append("/results/transferDetails");

            try {
                AuthMethod authMethod = AuthenticationUtil.getAuthMethod(AuthenticationUtil.getCurrentSubject());
                // HACK: self lookup
                URL serviceURL = regClient.getServiceURL(localServiceURI.getURI(), Standards.VOSPACE_TRANSFERS_20, authMethod);
                URL location = new URL(serviceURL.toExternalForm() + sb.toString());
                String loc = location.toExternalForm();
                log.debug("Location: " + loc);
                syncOutput.setHeader("Location", loc);
                syncOutput.setCode(HttpURLConnection.HTTP_SEE_OTHER);
                return;
            } catch (MalformedURLException bug) {
                throw new RuntimeException("BUG: failed to create valid transferDetails URL", bug);
            }
        }
    }

    private void sendError(ErrorType errorType, String message, int code)
            throws JobNotFoundException, JobPersistenceException, IOException, TransientException {
        sendError(null, errorType, message, code, false);
    }

    private void sendError(ExecutionPhase current, ErrorType errorType, String message, int code, boolean success)
            throws JobNotFoundException, JobPersistenceException, IOException, TransientException {
        logInfo.setSuccess(success);
        logInfo.setMessage(message);
        if (current == null) {
            current = ExecutionPhase.QUEUED;
        }

        log.debug("setting/persisting ExecutionPhase = " + ExecutionPhase.ERROR);
        ErrorSummary es = new ErrorSummary(message, errorType);
        ExecutionPhase ep = jobUpdater.setPhase(job.getID(), current, ExecutionPhase.ERROR, es, new Date());
        if (!ExecutionPhase.ERROR.equals(ep)) {
            log.debug(job.getID() + ": " + current + " -> ERROR [FAILED] -- DONE");
            return;
        }
        log.debug(job.getID() + ": " + current + " -> ERROR [OK]");
        job.setExecutionPhase(ep);

        if (!job.getParameterList().isEmpty()) {
            // custom param-based negotiation
            try {
                log.debug("Setting response code to: " + code);
                syncOutput.setResponseCode(code);
                syncOutput.setHeader("Content-Type", "text/plain");
                PrintWriter pw = new PrintWriter(syncOutput.getOutputStream());
                syncOutputCommit = true;
                pw.println(message);
                pw.close();
            } catch (IOException ex) {
                log.debug("failed to write error to SyncOutput", ex);
            }
        }
    }

    private boolean isValidDirection(Direction direction, boolean syncParamRequest) {
        if (direction == null || direction.getValue() == null) {
            return false;
        }

        if (syncParamRequest) {
            if (direction.equals(Direction.pushToVoSpace)
                    || direction.equals(Direction.pullFromVoSpace)
                    || direction.equals(Direction.BIDIRECTIONAL)) {
                return true;
            }
            return false;
        }

        if (direction.equals(Direction.pushToVoSpace)
                || direction.equals(Direction.pullToVoSpace)
                || direction.equals(Direction.pullFromVoSpace)
                || direction.equals(Direction.pushFromVoSpace)
                || direction.equals(Direction.BIDIRECTIONAL)) {
            return true;
        }

        if (direction.getValue().startsWith(VOS_PREFIX)) {
            return true;
        }

        return false;
    }

}
