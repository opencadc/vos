/*
 ************************************************************************
 *******************  CANADIAN ASTRONOMY DATA CENTRE  *******************
 **************  CENTRE CANADIEN DE DONNÉES ASTRONOMIQUES  **************
 *
 *  (c) 2026.                            (c) 2026.
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
import ca.nrc.cadc.log.ServletLogInfo;
import ca.nrc.cadc.uws.Job;
import ca.nrc.cadc.uws.JobInfo;
import ca.nrc.cadc.uws.server.JobManager;
import java.io.IOException;
import java.io.PrintWriter;
import java.security.AccessControlException;
import java.security.PrivilegedExceptionAction;
import java.util.Enumeration;
import javax.security.auth.Subject;
import javax.servlet.ServletConfig;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import org.apache.log4j.Logger;

/**
 * Servlet to serve the directory-size report for a completed UWS job.
 * The UWS job Result named {@link #RESULT_NAME} redirects here.
 */
public class NodeSizeReportServlet extends HttpServlet {

    private static final long serialVersionUID = 20260316120000L;

    private static final Logger log = Logger.getLogger(NodeSizeReportServlet.class);

    public static final String RESULT_NAME = "report";
    public static final String CONTENT_TYPE = "text/plain";

    private static final String JOB_MANAGER = JobManager.class.getName();

    private JobManager jobManager;

    @Override
    public void init(ServletConfig config) throws ServletException {
        super.init(config);
        try {
            String cname = config.getInitParameter(JOB_MANAGER);
            Class<?> c = Class.forName(cname);
            this.jobManager = (JobManager) c.getDeclaredConstructor().newInstance();
            log.info("loaded " + JOB_MANAGER + ": " + cname);
        } catch (Exception ex) {
            log.error("CONFIGURATION ERROR: failed to load " + JOB_MANAGER, ex);
        }
    }

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

            ClientReportRunner runner = new ClientReportRunner(request, response, logInfo);
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
            logInfo.setElapsedTime(System.currentTimeMillis() - start);
            log.info(logInfo.end());
        }
    }

    private static String parseJobID(String path) {
        if (path == null) {
            return null;
        }
        if (path.startsWith("/")) {
            path = path.substring(1);
        }
        if (path.endsWith("/")) {
            path = path.substring(0, path.length() - 1);
        }
        return path;
    }

    private class ClientReportRunner implements PrivilegedExceptionAction<Object> {

        private final HttpServletRequest request;
        private final HttpServletResponse response;
        private final ServletLogInfo logInfo;

        ClientReportRunner(HttpServletRequest request, HttpServletResponse response, ServletLogInfo logInfo) {
            this.request = request;
            this.response = response;
            this.logInfo = logInfo;
        }

        @Override
        public Object run() throws Exception {
            if (jobManager == null) {
                response.setStatus(HttpServletResponse.SC_INTERNAL_SERVER_ERROR);
                response.setContentType(CONTENT_TYPE);
                PrintWriter w = response.getWriter();
                w.println("servlet is not configured to manage node-size jobs");
                w.close();
                logInfo.setSuccess(false);
                return null;
            }

            String jobID = parseJobID(request.getPathInfo());
            log.debug("jobID: " + jobID);

            try {
                Job job = jobManager.get(null, jobID);
                JobInfo jobInfo = job.getJobInfo();
                if (jobInfo == null
                        || !Boolean.TRUE.equals(jobInfo.getValid())
                        || !CONTENT_TYPE.equals(jobInfo.getContentType())
                        || jobInfo.getContent() == null
                        || jobInfo.getContent().isEmpty()) {
                    response.setStatus(HttpServletResponse.SC_NOT_FOUND);
                    response.setContentType(CONTENT_TYPE);
                    PrintWriter out = response.getWriter();
                    out.println("report not found for job " + jobID);
                    out.flush();
                    logInfo.setSuccess(false);
                    return null;
                }

                response.setStatus(HttpServletResponse.SC_OK);
                response.setContentType(CONTENT_TYPE);
                PrintWriter out = response.getWriter();
                out.print(jobInfo.getContent().get(0));
                out.flush();
                logInfo.setSuccess(true);
                return null;
            } catch (Throwable t) {
                logInfo.setSuccess(false);
                if (t instanceof AccessControlException) {
                    response.setStatus(HttpServletResponse.SC_UNAUTHORIZED);
                    response.setContentType(CONTENT_TYPE);
                    PrintWriter out = response.getWriter();
                    out.println("Unauthorized: " + t.getMessage());
                    out.flush();
                    return null;
                }
                throw t;
            }
        }
    }
}
