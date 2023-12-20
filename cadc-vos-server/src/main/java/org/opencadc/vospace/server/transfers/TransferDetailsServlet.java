/*
************************************************************************
*******************  CANADIAN ASTRONOMY DATA CENTRE  *******************
**************  CENTRE CANADIEN DE DONNÉES ASTRONOMIQUES  **************
*
*  (c) 2022.                            (c) 2022.
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
*  $Revision: 4 $
*
************************************************************************
 */

package org.opencadc.vospace.server.transfers;

import ca.nrc.cadc.auth.AuthenticationUtil;
import ca.nrc.cadc.log.ServletLogInfo;
import ca.nrc.cadc.net.TransientException;
import ca.nrc.cadc.reg.Standards;
import ca.nrc.cadc.uws.Job;
import ca.nrc.cadc.uws.JobInfo;
import ca.nrc.cadc.uws.Parameter;
import ca.nrc.cadc.uws.Result;
import ca.nrc.cadc.uws.server.JobManager;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.Writer;
import java.security.AccessControlException;
import java.security.PrivilegedExceptionAction;
import java.util.ArrayList;
import java.util.Enumeration;
import java.util.List;
import javax.naming.Context;
import javax.naming.InitialContext;
import javax.naming.NamingException;
import javax.security.auth.Subject;
import javax.servlet.ServletConfig;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import org.apache.log4j.Logger;
import org.opencadc.vospace.VOSURI;
import org.opencadc.vospace.View;
import org.opencadc.vospace.server.NodePersistence;
import org.opencadc.vospace.transfer.Direction;
import org.opencadc.vospace.transfer.Protocol;
import org.opencadc.vospace.transfer.Transfer;
import org.opencadc.vospace.transfer.TransferReader;
import org.opencadc.vospace.transfer.TransferWriter;

/**
 * Servlet to support output of a transfer result from 
 * /transfers/{jobID}/results/transferDetails. Implementation
 * detail/requirement: This servlet MUST be deployed as a 
 * sibling of the /nodes endpoint and MUST be named /xfer 
 * in the servlet mapping.
 * 
 * @author pdowler
 */
public class TransferDetailsServlet extends HttpServlet {

    private static final long serialVersionUID = 2022026164700L;

    private static Logger log = Logger.getLogger(TransferDetailsServlet.class);

    private static final String TEXT_XML = "text/xml";

    private static final String JOB_MANAGER = JobManager.class.getName();

    // name of the resolved path to the data node in the job results
    public static final String DATA_NODE = "dataNode";
    public static final String LINK_NODE = "linkNode";

    private JobManager jobManager;
    private TransferGenerator transferGen;

    /**
     * @param config The servlet config.
     * @throws ServletException If servlet init exception.
     */
    @Override
    public void init(ServletConfig config)
            throws ServletException {
        super.init(config);

        try {
            String cname = config.getInitParameter(JOB_MANAGER);
            Class c = Class.forName(cname);
            this.jobManager = (JobManager) c.newInstance();
            log.info("loaded " + JOB_MANAGER + ": " + cname);
        } catch (Exception ex) {
            log.error("CONFIGURATION ERROR: failed to load " + JOB_MANAGER, ex);
        }

        // ugh: recreate cadc-rest appName
        String appName = config.getServletContext().getServletContextName();
        String jndiNodePersistence = appName + "-" + NodePersistence.class.getName();
        try {
            Context ctx = new InitialContext();
            NodePersistence nodePersistence = (NodePersistence) ctx.lookup(jndiNodePersistence);
            this.transferGen = nodePersistence.getTransferGenerator();
        } catch (NamingException oops) {
            throw new RuntimeException("BUG: NodePersistence implementation not found with JNDI key " + jndiNodePersistence, oops);
        }
    }

    /**
     *
     *
     * @param request The servlet request.
     * @param response The servlet response.
     * @throws ServletException If servlet exception.
     * @throws IOException If IO exception.
     */
    @Override
    protected void doGet(HttpServletRequest request, HttpServletResponse response)
            throws ServletException, IOException {

        long start = System.currentTimeMillis();
        ServletLogInfo logInfo = new ServletLogInfo(request);
        logInfo.setJobID(parseJobID(request.getPathInfo()));

        Enumeration<String> paramNames = request.getParameterNames();
        while (paramNames.hasMoreElements()) {
            String nextName = paramNames.nextElement();
            if (nextName.equalsIgnoreCase("runid")) {
                logInfo.setRunID(request.getParameter(nextName));
                break;
            }
        }

        try {
            log.info(logInfo.start());

            Subject subject = AuthenticationUtil.getSubject(request);
            logInfo.setSubject(subject);

            ClientTransferRunner runner = new ClientTransferRunner(request, response, logInfo);

            if (subject == null) {
                runner.run();
            } else {
                Subject.doAs(subject, runner);
            }

        } catch (Throwable t) {
            String message = "Internal Error: " + t.getMessage();
            response.setStatus(HttpServletResponse.SC_INTERNAL_SERVER_ERROR);
            log.error(message, t);
        } finally {
            if (logInfo != null) {
                logInfo.setElapsedTime(System.currentTimeMillis() - start);
                log.info(logInfo.end());
            }
        }
    }

    private String parseJobID(String path) {
        if (path != null) {
            if (path.startsWith("/")) {
                path = path.substring(1);
            }
            if (path.endsWith("/")) {
                path = path.substring(0, path.length() - 1);
            }
            return path;
        }
        return null;
    }

    class ClientTransferRunner implements PrivilegedExceptionAction<Object> {

        private HttpServletRequest request;
        private HttpServletResponse response;
        private ServletLogInfo logInfo;

        ClientTransferRunner(HttpServletRequest request, HttpServletResponse response, ServletLogInfo logInfo) {
            this.request = request;
            this.response = response;
            this.logInfo = logInfo;
        }

        @Override
        public Object run() throws Exception {
            if (jobManager == null) {
                // config error
                response.setStatus(HttpServletResponse.SC_INTERNAL_SERVER_ERROR);
                response.setContentType("text/plain");
                PrintWriter w = response.getWriter();
                String message = "servlet is not configured to manage transfer jobs";
                w.println(message);
                w.close();

                logInfo.setSuccess(false);
                logInfo.setMessage(message);

                return null;
            }

            // JobID from the request path.
            String jobID = request.getPathInfo();

            // Strip off leading / in pathInfo to get the jobID.
            if (jobID.startsWith("/")) {
                jobID = jobID.substring(1);
            }
            log.debug("jobID: " + jobID);

            try {
                Job job = jobManager.get(null, jobID); // no support for multiple child services

                JobInfo jobInfo = job.getJobInfo();

                if (jobInfo == null) {
                    throw new IllegalArgumentException("No job information.");
                }

                if (Boolean.TRUE.equals(jobInfo.getValid())
                        && TEXT_XML.equals(jobInfo.getContentType())) {
                    TransferReader reader = new TransferReader();
                    Transfer transfer = reader.read(jobInfo.getContent(), VOSURI.SCHEME);

                    // Check early to see if this is a package transfer request
                    boolean isPackageTransfer = false;
                    View transferView = transfer.getView();
                    if (transferView != null && transferView.getURI().equals(Standards.PKG_10)) {
                        isPackageTransfer = true;
                    }

                    // if data node path is not in the results, transfer
                    // cannot happen so remove protocols in the transfer
                    boolean dataNodeFound = false;
                    for (Result result : job.getResultsList()) {
                        if (DATA_NODE.equals(result.getName())) {
                            dataNodeFound = true;
                            break;
                        }
                    }
                    if (!dataNodeFound) {
                        if (!isPackageTransfer) {
                            // clear protocol list
                            transfer.getProtocols().clear();
                        }
                    }

                    // Perform a bit of validation, and determine if this is a package
                    // transfer or not as the handling is a little different
                    if (transfer.getTargets().isEmpty()) {
                        throw new UnsupportedOperationException("No targets found.");
                    }

                    if (transfer.getTargets().size() > 1 && !isPackageTransfer) {
                        // Multiple targets are currently only supported for package transfers
                        throw new UnsupportedOperationException("More than one target found. (" + transfer.getTargets().size() + ")");
                    }

                    Direction dir = transfer.getDirection();
                    if (dir.equals(Direction.pushToVoSpace) || dir.equals(Direction.pullFromVoSpace) || dir.equals(Direction.BIDIRECTIONAL)) {
                        final List<Parameter> additionalParams = new ArrayList<>(0);

                        // Set up the base transfer result. Targets are added in next conditional section
                        Transfer result = new Transfer(dir);

                        VOSURI target = new VOSURI(transfer.getTargets().get(0));
                        result.getTargets().add(target.getURI());
                        result.version = transfer.version;
                        result.setContentLength(transfer.getContentLength());

                        // Add protocol list with endpoints just built
                        List<Protocol> protos = transferGen.getEndpoints(target, transfer, job, additionalParams);
                        result.getProtocols().addAll(protos);
                        result.setView(transfer.getView());

                        response.setContentType("text/xml");
                        Writer out = response.getWriter();

                        // Write out the transfer w/ endpoints
                        TransferWriter writer = new TransferWriter();
                        writer.write(result, out);
                        out.flush();
                        return null;
                    } else if (dir.equals(Direction.pushFromVoSpace) || dir.equals(Direction.pullToVoSpace)) {
                        throw new UnsupportedOperationException("not implemented");
                    } else if (dir.getValue().startsWith("vos")) {
                        // move or copy
                        response.setContentType("text/xml");
                        Writer out = response.getWriter();

                        // Write out the input transfer
                        TransferWriter writer = new TransferWriter();
                        writer.write(transfer, out);
                        out.flush();
                        return null;
                    }
                }

                // internal error
                logInfo.setSuccess(false);
                StringBuilder message = new StringBuilder("cannot respond with valid transfer document");
                if (!Boolean.TRUE.equals(jobInfo.getValid())) {
                    message.append("\n  reason: jobInfo content was not valid");
                }
                if (!TEXT_XML.equals(jobInfo.getContentType())) {
                    message.append("\n  reason: jobInfo content was not " + TEXT_XML);
                }
                logInfo.setMessage(message.toString());

                response.setStatus(HttpServletResponse.SC_INTERNAL_SERVER_ERROR);
                response.setContentType("text/plain");
                PrintWriter out = response.getWriter();
                out.println(message.toString());
                out.flush();
            } catch (Throwable t) {
                logInfo.setSuccess(false);
                String message = "";
                log.debug("", t);

                if (t instanceof AccessControlException) {
                    response.setStatus(HttpServletResponse.SC_UNAUTHORIZED);
                    message = "Unauthorized: " + t.getMessage();
                } else if (t instanceof IllegalArgumentException) {
                    response.setStatus(HttpServletResponse.SC_BAD_REQUEST);
                    message = "Bad request: " + t.getMessage();
                } else if (t instanceof TransientException) {
                    message = "Unavailable: " + t.getMessage();
                    response.setStatus(HttpServletResponse.SC_SERVICE_UNAVAILABLE);
                    response.setHeader("Retry-After", Integer.toString(((TransientException) t).getRetryDelay()));
                } else {
                    message = "Internal Error: " + t.getMessage();
                    response.setStatus(HttpServletResponse.SC_INTERNAL_SERVER_ERROR);
                    log.error("", t);
                }

                logInfo.setMessage(message);
                Writer out = response.getWriter();
                out.write(message);
                out.flush();
            }

            return null;

        }

    }

}
