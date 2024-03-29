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

package org.opencadc.vospace.client;

import ca.nrc.cadc.auth.NotAuthenticatedException;
import ca.nrc.cadc.net.ExpectationFailedException;
import ca.nrc.cadc.net.HttpDownload;
import ca.nrc.cadc.net.HttpGet;
import ca.nrc.cadc.net.HttpPost;
import ca.nrc.cadc.net.HttpRequestProperty;
import ca.nrc.cadc.net.HttpTransfer;
import ca.nrc.cadc.net.PreconditionFailedException;
import ca.nrc.cadc.net.RangeNotSatisfiableException;
import ca.nrc.cadc.net.ResourceAlreadyExistsException;
import ca.nrc.cadc.net.ResourceNotFoundException;
import ca.nrc.cadc.net.TransientException;
import ca.nrc.cadc.uws.ErrorSummary;
import ca.nrc.cadc.uws.ExecutionPhase;
import ca.nrc.cadc.uws.Job;
import ca.nrc.cadc.uws.JobReader;
import ca.nrc.cadc.xml.XmlUtil;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.LineNumberReader;
import java.io.StringReader;
import java.net.MalformedURLException;
import java.net.URL;
import java.security.AccessControlException;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.log4j.Logger;
import org.jdom2.JDOMException;
import org.opencadc.vospace.Node;
import org.opencadc.vospace.io.XmlProcessor;

/**
 * A client-side wrapper for a recursive set node job to make it runnable.
 */
public class ClientRecursiveSetNode implements Runnable {
    private static Logger log = Logger.getLogger(ClientRecursiveSetNode.class);
    private static final int UWS_WAIT = 6; // sec
    
    private URL jobURL;
    private Node node;
    private boolean monitorAsync;
    private boolean schemaValidation;
    
    private List<HttpRequestProperty> httpRequestProperties;

    private Throwable throwable;
    private ExecutionPhase phase;
    private ErrorSummary error;

    /**
     * @param jobURL UWS job URL for the transfer job
     * @param node The node in question
     * @param schemaValidation boolean
     */
    public ClientRecursiveSetNode(URL jobURL, Node node, boolean schemaValidation) {
        this.httpRequestProperties = new ArrayList<HttpRequestProperty>();
        this.jobURL = jobURL;
        this.node = node;
        this.monitorAsync = false;
        this.schemaValidation = schemaValidation;
    }

    /**
     * Get the URL to the Job.
     * 
     * @return URL to the Job.
     */
    public URL getJobURL() {
        return this.jobURL;
    }

    /**
     * Get the node.
     */
    public Node getNode() {
        return node;
    }

    /**
     * Get any client-side error that was caught during the run method.
     * @return
     */
    public Throwable getThrowable() {
        return throwable;
    }

    /**
     * Get the UWS execution phase of the job.
     *
     * @param waitForSec amount of time to wait for phase
     * @return the current phase
     */
    public ExecutionPhase getPhase(int waitForSec) {
        if (phase != null) {
            return phase;
        }

        String urlStr = jobURL.toExternalForm() + "/phase?WAIT=" + waitForSec;
        try {
            URL phaseURL = new URL(urlStr);
            HttpGet get = new HttpGet(phaseURL, true);
            get.setConnectionTimeout(6000);
            get.setReadTimeout(24000);
            get.prepare();
            InputStream istream = get.getInputStream();
            InputStreamReader isr = new InputStreamReader(istream);
            LineNumberReader r = new LineNumberReader(isr);
            String str = r.readLine();
            ExecutionPhase ep = ExecutionPhase.toValue(str);
            if (ExecutionPhase.ABORTED.equals(ep) || ExecutionPhase.COMPLETED.equals(ep)
                || ExecutionPhase.ERROR.equals(ep)) {
                this.phase = ep; // only set when final phase
            }
            return ep;
        } catch (Exception ex) {
            throw new RuntimeException("failed to get job phase from " + urlStr, ex);
        }
    }
    
    public ExecutionPhase getPhase() {
        return phase;
    }

    public ErrorSummary getServerError() throws IOException {
        if (error != null) {
            return error;
        }

        Job job = getJob();
        this.error = job.getErrorSummary();
        return error;
    }

    private Job getJob() throws IOException {
        try {
            ByteArrayOutputStream out = new ByteArrayOutputStream();
            HttpGet get = new HttpGet(jobURL, out);
            get.run();

            if (get.getThrowable() != null) {
                throw new RuntimeException("Unable to get job because " + get.getThrowable().getLocalizedMessage());
            }
            
            // add the extra xsd information for vospace if we
            // are using schema validation
            JobReader jobReader = null;
            if (schemaValidation) {
                Map<String, String> extraSchemas = new HashMap<String, String>();
                //String xsdFile = XmlUtil.getResourceUrlString(XmlProcessor.VOSPACE_SCHEMA_RESOURCE_20, ClientRecursiveSetNode.class);
                //extraSchemas.put(XmlProcessor.VOSPACE_NS_20, xsdFile);
                String xsdFile = XmlUtil.getResourceUrlString(XmlProcessor.VOSPACE_SCHEMA_RESOURCE_21, ClientRecursiveSetNode.class);
                extraSchemas.put(XmlProcessor.VOSPACE_NS_20, xsdFile);
                jobReader = new JobReader(extraSchemas);
            } else {
                jobReader = new JobReader(false);
            }
            
            return jobReader.read(new StringReader(new String(out.toByteArray(), "UTF-8")));
        } catch (ParseException ex) {
            throw new RuntimeException("failed to parse job from " + jobURL, ex);
        } catch (JDOMException ex) {
            throw new RuntimeException("failed to parse job from " + jobURL, ex);
        } catch (MalformedURLException bug) {
            throw new RuntimeException("BUG: failed to create error url", bug);
        }
    }

    /**
     * Set additional request headers.
     *
     * @param header
     * @param value
     */
    public void setRequestProperty(String header, String value) {
        httpRequestProperties.add(new HttpRequestProperty(header,value));
    }

    /**
     * Enable or disable monitoring an async job until it is finished. If enabled, 
     * the run() method will not return until the job reaches a terminal state. If 
     * disabled, the run() method will simply start the async job and return immediately.
     * 
     * @param enabled
     */
    public void setMonitor(boolean enabled) {
        this.monitorAsync = enabled;
    }

    /**
     * Run the transfer and catch any throwables. The caller must check the
     * getPhase(), getServerError(), and getThrowable() methods to see if the
     * transfer failed.
     */
    public void run() {
        log.debug("start: " + node);
        try {
            runRecursiveSetNode();
        } catch (Throwable t) {
            this.throwable = t;
        }
        log.debug("done: " + node);
    }

    /**
     * Run the recursive set node job. Use this method if you want to have client-side
     * exceptions thrown.
     * @throws IOException
     * @throws InterruptedException if a the job is interrupted
     * @throws RuntimeException for server response errors
     */
    private void runRecursiveSetNode()
        throws IOException, InterruptedException, RuntimeException {
        try {
            URL url = new URL(jobURL.toExternalForm() + "/phase");
            String content = "PHASE=RUN";
            String contentType = "application/x-www-form-urlencoded";
            boolean followRedirects = false;

            HttpPost post = new HttpPost(url, content, contentType, followRedirects);
            post.run();

            if (post.getThrowable() != null) {
                throw new RuntimeException("Unable to run job because " + post.getThrowable().getLocalizedMessage());
            }

            if (monitorAsync) {
                while (phase == null) {
                    ExecutionPhase ep = getPhase(UWS_WAIT);
                }
            }
        } catch (MalformedURLException bug) {
            throw new RuntimeException("BUG: failed to create phase url", bug);
        }
    }
    
}
