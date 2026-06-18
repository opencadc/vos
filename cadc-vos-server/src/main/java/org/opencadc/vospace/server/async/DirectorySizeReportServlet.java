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
 *  This file is part of the             Ce fichier fait partie du projet
 *  OpenCADC project.                    OpenCADC.
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
public class DirectorySizeReportServlet extends HttpServlet {

    private static final long serialVersionUID = 20260316120000L;

    private static final Logger log = Logger.getLogger(DirectorySizeReportServlet.class);

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
                w.println("servlet is not configured to manage allocation-size jobs");
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
