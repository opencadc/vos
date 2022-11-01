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

package ca.nrc.cadc.vos.server.transfers;

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
import ca.nrc.cadc.vos.Direction;
import ca.nrc.cadc.vos.LinkingException;
import ca.nrc.cadc.vos.NodeBusyException;
import ca.nrc.cadc.vos.NodeLockedException;
import ca.nrc.cadc.vos.NodeNotFoundException;
import ca.nrc.cadc.vos.Protocol;
import ca.nrc.cadc.vos.Transfer;
import ca.nrc.cadc.vos.TransferReader;
import ca.nrc.cadc.vos.VOSURI;
import ca.nrc.cadc.vos.server.LocalServiceURI;
import ca.nrc.cadc.vos.server.db.NodePersistence;
import ca.nrc.cadc.vos.server.VOSpacePluginFactory;
import ca.nrc.cadc.vos.server.auth.VOSpaceAuthorizer;
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
import org.apache.log4j.Logger;

public class TransferRunner implements JobRunner {
    private static Logger log = Logger.getLogger(TransferRunner.class);

    private static final String VOS_PREFIX = "vos://";

    private Job job;
    private JobUpdater jobUpdater;
    private SyncOutput syncOutput;
    private boolean syncOutputCommit = false;
    private JobLogInfo logInfo;

    private URI serviceURI;
    private RegistryClient regClient;

    public TransferRunner() {
        try {
            LocalServiceURI localServiceURI = new LocalServiceURI();
            this.serviceURI = localServiceURI.getURI();
            this.regClient = new RegistryClient();
        } catch (Throwable bug) {
            throw new RuntimeException("BUG - failed to create VOSpace service URI", bug);
        }
    }

    @Override
    public void setJob(final Job job) {
        this.job = job;
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
    private Transfer createTransfer(List<Parameter> params) {
        String suri = ParameterUtil.findParameterValue("TARGET", params);
        String sdir = ParameterUtil.findParameterValue("DIRECTION", params);
        String sproto = ParameterUtil.findParameterValue("PROTOCOL", params);
        log.debug("createTransfer: " + suri + " " + sdir + " " + sproto);
        if (!StringUtil.hasText(suri) ||
            !StringUtil.hasText(sdir) ||
            !StringUtil.hasText(sproto)) {
            throw new IllegalArgumentException("missing parameters: " +
                "TARGET=" + suri + " DIRECTION=" + sdir + " PROTOCOL=" + sproto);
        }
        VOSURI target;
        try {
            URI uri = new URI(suri);
            target = new VOSURI(uri);
        } catch (URISyntaxException ex) {
            throw new IllegalArgumentException("InvalidArgument : invalid target URI " + suri);
        }

        Direction dir = new Direction(sdir);

        List<Protocol> plist = new ArrayList<Protocol>();
        plist.add(new Protocol(sproto));

        // also add a protocol with current securityMethod
        AuthMethod am = AuthenticationUtil.getAuthMethod(AuthenticationUtil.getCurrentSubject());
        Protocol proto = new Protocol(sproto);
        if (am != null) {
            switch (am) {
                case CERT:
                    proto.setSecurityMethod(Standards.SECURITY_METHOD_CERT);
                    break;
                case COOKIE:
                    proto.setSecurityMethod(Standards.SECURITY_METHOD_COOKIE);
                    break;
                case PASSWORD:
                    proto.setSecurityMethod(Standards.SECURITY_METHOD_HTTP_BASIC);
                    break;
                case TOKEN:
                    proto.setSecurityMethod(Standards.SECURITY_METHOD_TOKEN);
                    break;
                default:
                    break;
            }
        }
        if (proto.getSecurityMethod() != null) {
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
            if (!"TARGET".equalsIgnoreCase(param.getName()) &&
                !"DIRECTION".equalsIgnoreCase(param.getName()) &&
                !"PROTOCOL".equalsIgnoreCase(param.getName())) {
                ret.add(param);
            }
        }
        return ret;
    }

    private void doit() {
        Transfer transfer = null;
        boolean customPushPull = false;
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

            VOSpacePluginFactory vosPluginFactory = new VOSpacePluginFactory();
            NodePersistence nodePer = vosPluginFactory.createNodePersistence();
            //VOSpaceNodePersistence nodePer = new VOSpaceNodePersistence();
            VOSpaceAuthorizer partialPathVOSpaceAuthorizer = new VOSpaceAuthorizer(true);
            partialPathVOSpaceAuthorizer.setNodePersistence(nodePer);

            try {
                if (direction.equals(Direction.pushToVoSpace)) {
                    trans = new PushToVOSpaceNegotiation(nodePer, jobUpdater, job, transfer);
                } else if (direction.equals(Direction.pullFromVoSpace)) {
                    trans = new PullFromVOSpaceNegotiation(nodePer, jobUpdater, job, transfer);
                } else if (direction.equals(Direction.pullToVoSpace)) {
                    trans = new PullToVOSpaceAction(nodePer, jobUpdater, job, transfer);
                } else if (direction.equals(Direction.pushFromVoSpace)) {
                    trans = new PushFromVOSpaceAction(nodePer, jobUpdater, job, transfer);
                } else if (direction.equals(Direction.BIDIRECTIONAL)) {
                    trans = new BiDirectionalTransferNegotiation(nodePer, jobUpdater, job, transfer);
                } else {
                    trans = new InternalTransferAction(nodePer, jobUpdater, job, transfer);
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
            try {
                doTransferRedirect(transfer, additionalParameters);
            } catch (Throwable t) {
                log.error("failed to do tranfer redirect", t);
                try {
                    sendError(ExecutionPhase.EXECUTING, ErrorType.FATAL, t.getMessage(), HttpURLConnection.HTTP_INTERNAL_ERROR, false);
                } catch (Exception e) {
                    log.error("Failed to update job.", e);
                }
            }
            log.debug("DONE");
        }
    }

    private void doTransferRedirect(Transfer trans, List<Parameter> additionalParameters) {
        if (syncOutput != null && !syncOutputCommit) {
            if (!job.getParameterList().isEmpty() && trans != null) {
                try {
                    List<Protocol> plist = TransferUtil.getTransferEndpoints(trans, job, additionalParameters);
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
                URL serviceURL = regClient.getServiceURL(serviceURI,  Standards.VOSPACE_TRANSFERS_20, authMethod);
                URL location = new URL(serviceURL.toExternalForm() + sb.toString());
                String loc = location.toExternalForm();
                log.debug("Location: " + loc);
                syncOutput.setHeader("Location", loc);
                syncOutput.setResponseCode(HttpURLConnection.HTTP_SEE_OTHER);
                return;
            } catch (MalformedURLException bug) {
                throw new RuntimeException("BUG: failed to create valid transferDetails URL", bug);
            }
        }
    }

    protected void sendError(ErrorType errorType, String message, int code)
        throws JobNotFoundException, JobPersistenceException, IOException, TransientException {
        sendError(null, errorType, message, code, false);
    }

    protected void sendError(ExecutionPhase current, ErrorType errorType, String message, int code, boolean success)
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

        if (!job.getParameterList().isEmpty()) // custom param-based negotiation
        {
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

    /**
     * @param direction
     * @return
     */
    protected boolean isValidDirection(Direction direction, boolean syncParamRequest) {
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
