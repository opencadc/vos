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

package ca.nrc.cadc.vos.client;

import ca.nrc.cadc.net.HttpDownload;
import ca.nrc.cadc.net.HttpPost;
import ca.nrc.cadc.net.HttpRequestProperty;
import ca.nrc.cadc.net.HttpTransfer;
import ca.nrc.cadc.net.HttpUpload;
import ca.nrc.cadc.net.InputStreamWrapper;
import ca.nrc.cadc.net.OutputStreamWrapper;
import ca.nrc.cadc.net.event.TransferListener;
import ca.nrc.cadc.uws.ErrorSummary;
import ca.nrc.cadc.uws.ExecutionPhase;
import ca.nrc.cadc.uws.Job;
import ca.nrc.cadc.uws.JobReader;
import ca.nrc.cadc.vos.Direction;
import ca.nrc.cadc.vos.Transfer;
import ca.nrc.cadc.vos.XmlProcessor;
import ca.nrc.cadc.xml.XmlUtil;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
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

/**
 * A client-side wrapper for a transfer to make it runnable.
 */
public class ClientTransfer implements Runnable {
    private static Logger log = Logger.getLogger(ClientTransfer.class);
    private static final long POLL_INTERVAL = 100L;

    private URL jobURL;
    private Transfer transfer;
    private boolean monitorAsync;
    private boolean schemaValidation;

    private File localFile;
    private OutputStreamWrapper outWrapper;
    private InputStreamWrapper inWrapper;
    private List<HttpRequestProperty> httpRequestProperties;
    private int maxRetries;
    private TransferListener transListener;

    private Throwable throwable;
    private ExecutionPhase phase;
    private ErrorSummary error;

    private ClientTransfer() {

    }

    /**
     * @param jobURL UWS job URL for the transfer job
     * @param transfer a negotiated transfer
     * @param schemaValidation monitor the job until complete (true) or just start
     *                         it and return (false)
     */
    ClientTransfer(URL jobURL, Transfer transfer, boolean schemaValidation) {
        this.httpRequestProperties = new ArrayList<HttpRequestProperty>();
        this.jobURL = jobURL;
        this.transfer = transfer;
        this.monitorAsync = false;
        this.schemaValidation = schemaValidation;
    }

    /**
     * Get the URL to the Job.
     *
     * @return URL tot the Job.
     */
    public URL getJobURL() {
        return jobURL;
    }

    /**
     * Get the negotiated transfer details.
     *
     * @return the negotiated transfer
     */
    public Transfer getTransfer() {
        return transfer;
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
     * @return the current phase
     */
    public ExecutionPhase getPhase()
        throws IOException {
        if (phase != null) {
            return phase;
        }

        Job job = getJob();
        ExecutionPhase ep = job.getExecutionPhase();
        if (ExecutionPhase.ABORTED.equals(ep)
            || ExecutionPhase.COMPLETED.equals(ep)
            || ExecutionPhase.ERROR.equals(ep)) {
            this.phase = ep; // only set when final phase
        }
        return ep;
    }

    public ErrorSummary getServerError()
        throws IOException {
        if (error != null) {
            return error;
        }

        Job job = getJob();
        this.error = job.getErrorSummary();
        return error;
    }

    private Job getJob()
        throws IOException {
        if (transfer.isQuickTransfer()) {
            throw new IllegalStateException("No job information available for quick transfers");
        }

        try {
            ByteArrayOutputStream out = new ByteArrayOutputStream();
            HttpDownload get = new HttpDownload(jobURL, out);

            runHttpTransfer(get);

            if (get.getThrowable() != null) {
                throw new RuntimeException("Unable to get Job because " + get.getThrowable().getLocalizedMessage());
            }

            // add the extra xsd information for vospace if we are using schema validation
            JobReader jobReader = null;
            if (schemaValidation) {
                Map<String, String> extraSchemas = new HashMap<String, String>();
                //String xsdFile = XmlUtil.getResourceUrlString(XmlProcessor.VOSPACE_SCHEMA_RESOURCE_20,
                //                                              ClientRecursiveSetNode.class);
                //extraSchemas.put(XmlProcessor.VOSPACE_NS_20, xsdFile);
                String xsdFile = XmlUtil.getResourceUrlString(XmlProcessor.VOSPACE_SCHEMA_RESOURCE_21,
                                                              ClientRecursiveSetNode.class);
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

    public void setFile(File file) {
        if (Direction.pullFromVoSpace.equals(transfer.getDirection())
            || Direction.pushToVoSpace.equals(transfer.getDirection())) {
            this.localFile = file;
            return;
        }
        throw new IllegalStateException("cannot specify a local File for transfer direction "
                                            + transfer.getDirection());
    }

    /**
     * After a download, this will be the actual file downloaded.
     *
     * @return
     */
    public File getLocalFile() {
        return localFile;
    }

    public void setOutputStreamWrapper(OutputStreamWrapper wrapper) {
        if (Direction.pushToVoSpace.equals(transfer.getDirection())) {
            this.outWrapper = wrapper;
            return;
        }
        throw new IllegalStateException("cannot specify an OutputStreamWrapper for transfer direction "
                                            + transfer.getDirection());
    }
    
    public void setInputStreamWrapper(InputStreamWrapper wrapper) {
        if (Direction.pullFromVoSpace.equals(transfer.getDirection())) {
            this.inWrapper = wrapper;
            return;
        }
        throw new IllegalStateException("cannot specify an InputStreamWrapper for transfer direction "
                                            + transfer.getDirection());
    }

    /**
     * Set an optional listener to get events from the underying HttpTransfer.
     *
     * @param transListener
     */
    public void setTransferListener(TransferListener transListener) {
        this.transListener = transListener;
    }

    /**
     * Set the maximum number of retries when the server is busy. This value is
     * passed to the underlying HttpTransfer.
     * Set this to Integer.MAX_VALUE to retry indefinitely.
     *
     * @param maxRetries
     */
    public void setMaxRetries(int maxRetries) {
        this.maxRetries = maxRetries;
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
     * Run the transfer and catch any throwables. The cakller must check the
     * getPhase(), getServerError(), and getThrowable() methods to see if the
     * transfer failed.
     */
    public void run() {
        log.debug("start: " + transfer);
        try {
            runTransfer();
        } catch (Throwable t) {
            this.throwable = t;
        }
        log.debug("done: " + transfer);
    }

    /**
     * Run the transfer. Use this method if you want to have client-side
     * exceptiosn thrown.
     * @throws IOException
     * @throws InterruptedException if a server transfer is interrupted
     * @throws RuntimeException for server response errors
     */
    public void runTransfer()
        throws IOException, InterruptedException, RuntimeException {
        try {
            if (Direction.pullFromVoSpace.equals(transfer.getDirection())) {
                checkProtocols();
                doDownload();
            } else if (Direction.pushToVoSpace.equals(transfer.getDirection())) {
                checkProtocols();
                doUpload();
            } else {
                doServerTransfer();
            }
        } catch (JDOMException ex) {
            throw new RuntimeException("failed to parse transfer document", ex);
        } catch (ParseException ex) {
            throw new RuntimeException("failed to parse transfer document", ex);
        }
    }

    private void checkProtocols()
        throws IOException, JDOMException, ParseException {
        // Handle errors by parsing url, getting job and looking at phase/error summary.
        // Zero protocols in resulting transfer indicates that an error was encountered.
        if (transfer.getProtocols().size() == 0) {
            log.debug("Found zero protocols in returned transfer, checking " + "job for error details.");
            Job job = getJob();
            if (job.getExecutionPhase().equals(ExecutionPhase.ERROR) && job.getErrorSummary() != null) {
                throw new RuntimeException("Transfer Failure: " + job.getErrorSummary().getSummaryMessage());
            } else {
                throw new IllegalStateException("Job with no protocol endpoints received for job " + job.getID());
            }
        }
    }

    // pick one of the endpoints
    private List<URL> findGetEndpoint()
        throws MalformedURLException {
        List<String> ret = transfer.getAllEndpoints();
        if (ret.isEmpty()) {
            throw new RuntimeException("failed to find a usable endpoint URL");
        }

        List<URL> urls = new ArrayList<URL>();
        for (String urlStr : ret) {
            urls.add(new URL(urlStr));
        }
        return urls;
    }

    // pick one of the endpoints
    private URL findPutEndpoint()
        throws MalformedURLException {
        String ret = transfer.getEndpoint();
        if (ret == null) {
            throw new RuntimeException("failed to find a usable endpoint URL");
        }
        return new URL(ret);
    }

    private void doUpload()
        throws IOException {
        URL url = findPutEndpoint();
        log.debug(url);

        if (localFile == null && outWrapper == null) {
            throw new IllegalStateException("cannot perform upload without a File or OutputStreamWrapper");
        }

        HttpUpload upload = null;
        if (localFile != null) {
            upload = new HttpUpload(localFile, url);
        } else {
            upload = new HttpUpload(outWrapper, url);
        }

        log.debug("calling HttpUpload.setRequestProperties with " + httpRequestProperties.size() + " props");
        upload.setRequestProperties(httpRequestProperties);
        upload.setMaxRetries(maxRetries);
        if (transListener != null) {
            upload.setTransferListener(transListener);
        }

        runHttpTransfer(upload);

        if (upload.getThrowable() != null) {
            // allow illegal arugment exceptions through
            if (upload.getThrowable() instanceof IllegalArgumentException) {
                throw (IllegalArgumentException) upload.getThrowable();
            } else {
                try {
                    throw new IOException("failed to upload file", upload.getThrowable());
                } catch (NoSuchMethodError e) {
                    // Java5 does not have the above constructor.
                    throw new IOException("failed to upload file: " + upload.getThrowable().getMessage());
                }
            }
        }
    }

    private void doDownload()
        throws IOException, MalformedURLException {
        List<URL> urls = findGetEndpoint();
        if (urls.size() == 0) {
            throw new IllegalArgumentException("No endpoint found");
        }
        
        if (localFile == null && inWrapper == null) {
            throw new IllegalStateException("cannot perform download without a File or InputStreamStreamWrapper");
        }
        
        HttpDownload firstDownload = null;
        for (URL url : urls) {
            log.debug(url);

            HttpDownload download = null;
            if (localFile != null) {
                download = new HttpDownload(url, localFile);
            } else {
                download = new HttpDownload(url, inWrapper);
            }

            if (firstDownload == null) {
                firstDownload = download;
            }
            download.setOverwrite(true);
            download.setRequestProperties(httpRequestProperties);
            download.setMaxRetries(maxRetries);
            if (transListener != null) {
                download.setTransferListener(transListener);
            }

            runHttpTransfer(download);
            if (download.getThrowable() == null) {
                // the actual resulting file
                this.localFile = download.getFile();
                return;
            }
        }

        // got here so none of the urls worked
        throw new IOException("failed to download file", firstDownload.getThrowable());
    }

    // run and monitor an async server side transfer
    private void doServerTransfer()
        throws IOException, InterruptedException {
        try {
            URL phaseURL = new URL(jobURL.toExternalForm() + "/phase");

            final Map<String, Object> parameters = new HashMap<String, Object>();
            parameters.put("PHASE", "RUN");

            HttpPost transfer = new HttpPost(phaseURL, parameters, false);
            if (transListener != null) {
                transfer.setTransferListener(transListener);
            }
            transfer.run();

            Throwable error = transfer.getThrowable();
            if (error != null) {
                log.debug("createGroup throwable", error);
                // transfer returns a -1 code for anonymous uploads.
                if ((transfer.getResponseCode() == -1)
                    || (transfer.getResponseCode() == 401)
                    || (transfer.getResponseCode() == 403)) {
                    throw new AccessControlException(error.getMessage());
                }
                if (transfer.getResponseCode() == 400) {
                    throw new IllegalArgumentException(error.getMessage());
                }
                if (transfer.getResponseCode() == 404) {
                    throw new IllegalArgumentException(error.getMessage());
                }
                throw new RuntimeException("unexpected failure mode: "
                                               + error.getMessage() + "(" + transfer.getResponseCode() + ")");
            }

            if (monitorAsync) {
                while (phase == null) {
                    log.debug("monitorAsync: phase is currently " + phase);
                    Thread.sleep(POLL_INTERVAL);
                    getPhase();
                }
            }
        } catch (MalformedURLException bug) {
            throw new RuntimeException("BUG: failed to create phase url", bug);
        }
    }

    protected void runHttpTransfer(HttpTransfer transfer) {
        transfer.run();
    }

}
