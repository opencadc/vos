/*
 ************************************************************************
 *******************  CANADIAN ASTRONOMY DATA CENTRE  *******************
 **************  CENTRE CANADIEN DE DONNÉES ASTRONOMIQUES  **************
 *
 *  (c) 2024.                            (c) 2024.
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
 *  : 5 $
 *
 ************************************************************************
 */

package org.opencadc.conformance.vos;

import ca.nrc.cadc.auth.RunnableAction;
import ca.nrc.cadc.net.FileContent;
import ca.nrc.cadc.net.HttpGet;
import ca.nrc.cadc.net.HttpPost;
import ca.nrc.cadc.net.HttpUpload;
import ca.nrc.cadc.reg.Standards;
import ca.nrc.cadc.uws.ExecutionPhase;
import ca.nrc.cadc.uws.Job;
import ca.nrc.cadc.uws.JobReader;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.StringReader;
import java.io.StringWriter;
import java.net.MalformedURLException;
import java.net.URI;
import java.net.URL;
import java.nio.charset.Charset;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import javax.security.auth.Subject;
import org.apache.log4j.Logger;
import org.junit.Assert;
import org.junit.Test;
import org.opencadc.vospace.ContainerNode;
import org.opencadc.vospace.DataNode;
import org.opencadc.vospace.LinkNode;
import org.opencadc.vospace.Node;
import org.opencadc.vospace.VOS;
import org.opencadc.vospace.VOSURI;
import org.opencadc.vospace.io.NodeReader;
import org.opencadc.vospace.transfer.Direction;
import org.opencadc.vospace.transfer.Protocol;
import org.opencadc.vospace.transfer.Transfer;
import org.opencadc.vospace.transfer.TransferParsingException;
import org.opencadc.vospace.transfer.TransferReader;
import org.opencadc.vospace.transfer.TransferWriter;

public class TransferTest extends VOSTest {
    private static final Logger log = Logger.getLogger(TransferTest.class);

    private static final List<Integer> PUT_OK = Arrays.asList(new Integer[] { 200, 201});
    
    protected TransferTest(URI resourceID, File testCert) {
        super(resourceID, testCert);
    }

    @Test
    public void syncPushPullTest() {
        try {
            // Create a DataNode.
            String path = "sync-push-pull-node";
            URL nodeURL = getNodeURL(nodesServiceURL, path);
            VOSURI nodeURI = getVOSURI(path);
            log.debug("nodeURL: " + nodeURL);

            // Cleanup leftover node
            delete(nodeURL, false);

            verifyPushPull(nodeURI.getURI());
            
            // Delete the node
            delete(nodeURL, false);

        } catch (Exception e) {
            log.error("Unexpected error", e);
            Assert.fail("Unexpected error: " + e);
        }
    }

    private void verifyPushPull(URI testURI) throws Exception {
        // Create a push-to-vospace Transfer for the node
        Transfer pushTransfer = new Transfer(testURI, Direction.pushToVoSpace);
        pushTransfer.version = VOS.VOSPACE_21;
        pushTransfer.getProtocols().add(new Protocol(VOS.PROTOCOL_HTTPS_PUT)); // anon, preauth
        Protocol putWithCert = new Protocol(VOS.PROTOCOL_HTTPS_PUT);
        putWithCert.setSecurityMethod(Standards.SECURITY_METHOD_CERT);
        pushTransfer.getProtocols().add(putWithCert);

        // negotiate the transfer
        Transfer details = doTransfer(pushTransfer);
        Assert.assertEquals("expected transfer direction = " + Direction.pushToVoSpace,
                Direction.pushToVoSpace, details.getDirection());
        Assert.assertNotNull(details.getProtocols());
        log.info(pushTransfer.getDirection() + " results: " + details.getProtocols().size());
        URL putURL = null;
        for (Protocol p : details.getProtocols()) {
            String endpoint = p.getEndpoint();
            log.info("PUT endpoint: " + endpoint);
            try {

                URL u = new URL(endpoint);
                if (putURL == null) {
                    putURL = u; // first
                }
            } catch (MalformedURLException e) {
                Assert.fail(String.format("invalid protocol endpoint: %s because %s", endpoint, e.getMessage()));
            }
        }
        Assert.assertNotNull(putURL);

        // put the bytes
        Random rnd = new Random();
        byte[] data = new byte[1024];
        rnd.nextBytes(data);
        FileContent content = new FileContent(data, "application/octet-stream");
        HttpUpload put = new HttpUpload(content, putURL);
        put.run();
        log.info("put: " + put.getResponseCode() + " " + put.getThrowable());
        Assert.assertTrue(PUT_OK.contains(put.getResponseCode()));
        Assert.assertNull(put.getThrowable());

        // Create a pull-from-vospace Transfer for the node
        Transfer pullTransfer = new Transfer(testURI, Direction.pullFromVoSpace);
        pullTransfer.version = VOS.VOSPACE_21;
        pullTransfer.getProtocols().add(new Protocol(VOS.PROTOCOL_HTTPS_GET)); // anon, preauth
        Protocol getWithCert = new Protocol(VOS.PROTOCOL_HTTPS_GET);
        getWithCert.setSecurityMethod(Standards.SECURITY_METHOD_CERT);
        pullTransfer.getProtocols().add(getWithCert);


        // Do the transfer
        details = doTransfer(pullTransfer);
        Assert.assertEquals("expected transfer direction = " + Direction.pullFromVoSpace,
                Direction.pullFromVoSpace, details.getDirection());
        Assert.assertNotNull(details.getProtocols());
        log.info(pullTransfer.getDirection() + " results: " + details.getProtocols().size());
        URL getURL = null;
        for (Protocol p : details.getProtocols()) {
            String endpoint = p.getEndpoint();
            log.info("GET endpoint: " + endpoint);
            try {
                URL u = new URL(endpoint);
                if (getURL == null) {
                    getURL = u; // first
                }
            } catch (MalformedURLException e) {
                Assert.fail(String.format("invalid protocol endpoint: %s because %s", endpoint, e.getMessage()));
            }
        }
        Assert.assertNotNull(getURL);

        // get the bytes
        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        HttpGet get = new HttpGet(getURL, bos);
        get.run();
        log.info("get: " + get.getResponseCode() + " " + get.getContentType() + " " + get.getThrowable());
        Assert.assertEquals(200, get.getResponseCode());
        Assert.assertNull(get.getThrowable());
        Assert.assertEquals(content.getBytes().length, get.getContentLength());
        Assert.assertEquals(content.getContentType(), get.getContentType());
        byte[] actual = bos.toByteArray();
        Assert.assertArrayEquals(content.getBytes(), actual);
    }

    @Test
    public void syncPushPullViaDataLinkTest() {
        // C/D
        // L -> C/D
        // put to L
        // get from L
        try {
            // create ContainerNode
            final String cpath = "syncPushPullViaDataLinkTest";
            final URL conURL = getNodeURL(nodesServiceURL, cpath);
            final VOSURI conURI = getVOSURI(cpath);
            
            // create LinkNode -> DataNode
            final String linkPath = "transfer-data-link";
            final URL linkURL = getNodeURL(nodesServiceURL, linkPath);
            final VOSURI linkURI = getVOSURI(linkPath);
            
            // create a ContainerNode/DataNode
            final String path = cpath + "/data-node";
            final URL nodeURL = getNodeURL(nodesServiceURL, path);
            final VOSURI nodeURI = getVOSURI(path);

            // Cleanup leftover node
            log.info("nodeURL: " + nodeURL);
            delete(nodeURL, false);
            log.info("linkURL: " + linkURL);
            delete(linkURL, false);
            log.info("conURL: " + conURL);
            delete(conURL, false);
            
            // create
            final ContainerNode con = new ContainerNode(cpath);
            put(conURL, conURI, con);
            final LinkNode link = new LinkNode(linkPath, nodeURI.getURI());
            put(linkURL, linkURI, link);
            
            final URI testURI = linkURI.getURI();
            log.info("link to data node: " + testURI);

            verifyPushPull(testURI);
            
            // cleanup
            //delete(nodeURL, false);
            //delete(linkURL, false);
            //delete(conURL, false);
            
        } catch (Exception e) {
            log.error("Unexpected error", e);
            Assert.fail("Unexpected error: " + e);
        }
    }
    
    @Test
    public void syncPushPullViaContainerLinkTest() {
        // C/D
        // L -> C
        // put to L/D
        // get from L/D
        try {
            // create ContainerNode
            final String cpath = "transfer-container";
            final URL conURL = getNodeURL(nodesServiceURL, cpath);
            final VOSURI conURI = getVOSURI(cpath);
            
            // create LinkNode -> ContainerNode
            final String linkPath = "transfer-container-link";
            final URL linkURL = getNodeURL(nodesServiceURL, linkPath);
            final VOSURI linkURI = getVOSURI(linkPath);
            
            // create a ContainerNode/DataNode
            final String path = cpath + "/data-node";
            final URL nodeURL = getNodeURL(nodesServiceURL, path);
            final VOSURI nodeURI = getVOSURI(path);

            // Cleanup leftover node
            log.info("nodeURL: " + nodeURL);
            delete(nodeURL, false);
            log.info("linkURL: " + linkURL);
            delete(linkURL, false);
            log.info("conURL: " + conURL);
            delete(conURL, false);
            
            // create
            final ContainerNode con = new ContainerNode(cpath);
            put(conURL, conURI, con);
            final LinkNode link = new LinkNode(linkPath, conURI.getURI());
            put(linkURL, linkURI, link);
            
            final URI testURI = new URI(linkURI.getURI().toASCIIString() + "/" + nodeURI.getName());
            log.info("link to data node: " + testURI);

            verifyPushPull(testURI);
            
            // cleanup
            //delete(nodeURL, false);
            //delete(linkURL, false);
            //delete(conURL, false);

        } catch (Exception e) {
            log.error("Unexpected error", e);
            Assert.fail("Unexpected error: " + e);
        }
    }

    @Test
    public void asyncMoveTest() {
        try {
            // Source ContainerNode
            String sourceName = "move-source-node";
            URL sourceNodeURL = getNodeURL(nodesServiceURL, sourceName);
            final VOSURI sourceNodeURI = getVOSURI(sourceName);
            final ContainerNode sourceNode = new ContainerNode(sourceName);
            log.debug("source URL: " + sourceNodeURL);

            // Destination ContainerNode
            String destinationName = "move-destination-node";
            URL destinationNodeURL = getNodeURL(nodesServiceURL, destinationName);
            final VOSURI destinationNodeURI = getVOSURI(destinationName);
            final ContainerNode destinationNode = new ContainerNode(destinationName);
            log.debug("destination URL: " + destinationNodeURL);
            
            // Source child ContainerNode
            String childContainerName = "move-source-container-child-node";
            String childContainerPath = sourceName + "/" + childContainerName;
            URL childContainerNodeURL = getNodeURL(nodesServiceURL, childContainerPath);
            final VOSURI childContainerNodeURI = getVOSURI(childContainerPath);
            final ContainerNode childContainerNode = new ContainerNode(childContainerName);

            // Source child DataNode
            String childDataName = "move-source-data-child-node";
            String childDataPath = sourceName + "/" + childDataName;
            URL childDataNodeURL = getNodeURL(nodesServiceURL, childDataPath);
            final VOSURI childDataNodeURI = getVOSURI(childDataPath);
            final DataNode childDataNode = new DataNode(childDataName);
            
            // Cleanup old nodes
            delete(childContainerNodeURL, false);
            delete(childDataNodeURL, false);
            delete(sourceNodeURL, false);
            delete(getNodeURL(nodesServiceURL, destinationName + "/" + sourceName + "/" + childContainerName), false);
            delete(getNodeURL(nodesServiceURL, destinationName + "/" + sourceName + "/" + childDataName), false);
            delete(getNodeURL(nodesServiceURL, destinationName + "/" + sourceName), false);
            delete(destinationNodeURL, false);

            // Put new nodes
            put(sourceNodeURL, sourceNodeURI, sourceNode);
            put(destinationNodeURL, destinationNodeURI, destinationNode);
            
            log.debug("source-container-child URL: " + childContainerNodeURL);
            put(childContainerNodeURL, childContainerNodeURI, childContainerNode);

            log.debug("source-data-child URL: " + childDataNodeURL);
            put(childDataNodeURL, childDataNodeURI, childDataNode);

            // Create a Transfer
            Transfer transfer = new Transfer(sourceNodeURI.getURI(), destinationNodeURI.getURI(), false);
            transfer.getProtocols().add(new Protocol(VOS.PROTOCOL_HTTP_GET));
            transfer.getProtocols().add(new Protocol(VOS.PROTOCOL_HTTPS_GET));

            // Write the Transfer document
            TransferWriter transferWriter = new TransferWriter();
            StringWriter sw = new StringWriter();
            transferWriter.write(transfer, sw);
            log.debug("transfer request XML: " + sw);

            // Post the transfer document
            FileContent fileContent = new FileContent(sw.toString().getBytes(), VOSTest.XML_CONTENT_TYPE);
            HttpPost post = new HttpPost(transferServiceURL, fileContent, false);
            Subject.doAs(authSubject, new RunnableAction(post));
            Assert.assertEquals("expected POST response code = 303",303, post.getResponseCode());
            Assert.assertNull("expected POST throwable == null", post.getThrowable());
            URL jobURL = post.getRedirectURL();
            log.debug("jobURL: " + jobURL);
            Assert.assertNotNull(jobURL);
            
            // Get the job
            ByteArrayOutputStream out = new ByteArrayOutputStream();
            HttpGet get = new HttpGet(jobURL, out);
            log.debug("GET: " + post.getRedirectURL());
            Subject.doAs(authSubject, new RunnableAction(get));
            log.debug("GET responseCode: " + get.getResponseCode());
            Assert.assertEquals("expected GET response code = 200", 200, get.getResponseCode());
            Assert.assertNull("expected GET throwable == null", get.getThrowable());
            Assert.assertTrue("expected GET Content-Type starts with " + VOSTest.XML_CONTENT_TYPE,
                              get.getContentType().startsWith(VOSTest.XML_CONTENT_TYPE));

            // Read the job
            log.debug("job XML:\n" + out);
            JobReader reader = new JobReader();
            Job job = reader.read(new StringReader(out.toString()));
            Assert.assertEquals("Job pending", ExecutionPhase.PENDING, job.getExecutionPhase());

            // Run the job.
            Map<String, Object> parameters = new HashMap<>();
            parameters.put("PHASE", "RUN");
            URL jobPhaseURL = new URL(jobURL + "/phase");
            post = new HttpPost(jobPhaseURL, parameters, false);
            Subject.doAs(authSubject, new RunnableAction(post));
            Assert.assertEquals("expected POST response code = 303", 303, post.getResponseCode());

            // polling: WAIT will block for up to 6 sec or until phase change or if job is in
            // a terminal phase
            URL jobPoll = new URL(jobURL + "?WAIT=6"); 
            int count = 0;
            boolean done = false;
            while (!done && count < 10) { // max 10*6 = 60 sec polling
                out = new ByteArrayOutputStream();
                log.debug("poll: " + jobPoll);
                get = new HttpGet(jobPoll, out);
                Subject.doAs(authSubject, new RunnableAction(get));
                Assert.assertNull(get.getThrowable());
                job = reader.read(new StringReader(out.toString()));
                log.debug("current phase: " + job.getExecutionPhase());
                switch (job.getExecutionPhase()) {
                    case QUEUED: 
                    case EXECUTING:
                        count++;
                        break;
                    default:
                        done = true;
                }
                log.debug("done: " + job.getExecutionPhase() + " " + job.getErrorSummary());
                Assert.assertEquals(ExecutionPhase.COMPLETED, job.getExecutionPhase());
            }

            // Check if the job completed
            out = new ByteArrayOutputStream();
            get = new HttpGet(jobURL, out);
            Subject.doAs(authSubject, new RunnableAction(get));
            job = reader.read(new StringReader(out.toString()));
            Assert.assertEquals(ExecutionPhase.COMPLETED, job.getExecutionPhase());

            // Source node should not be found
            get(sourceNodeURL, 404, TEXT_CONTENT_TYPE);

            // Get the destination node
            NodeReader.NodeReaderResult result = get(destinationNodeURL, 200, XML_CONTENT_TYPE);
            List<Node> childNodes = ((ContainerNode) result.node).getNodes();
            Assert.assertEquals("expected single child node in destination node", 1, childNodes.size());
            Assert.assertTrue("expected source node as child node", childNodes.contains(sourceNode));

            // Get the moved source node
            result = get(new URL(destinationNodeURL + "/" + sourceName), 200, XML_CONTENT_TYPE);
            ContainerNode movedSourceNode = (ContainerNode) result.node;
            Assert.assertEquals("expected 2 child nodes in source node", 2, movedSourceNode.getNodes().size());
            Assert.assertTrue("expected child container node", movedSourceNode.getNodes().contains(childContainerNode));
            Assert.assertTrue("expected child data node", movedSourceNode.getNodes().contains(childDataNode));

            // Delete nodes
            delete(getNodeURL(nodesServiceURL, destinationName + "/" + sourceName + "/" + childContainerName));
            delete(getNodeURL(nodesServiceURL, destinationName + "/" + sourceName + "/" + childDataName));
            delete(getNodeURL(nodesServiceURL, destinationName + "/" + sourceName));
            delete(destinationNodeURL);

        } catch (Exception e) {
            log.error("Unexpected error", e);
            Assert.fail("Unexpected error: " + e);
        }
    }

    protected Transfer doTransfer(Transfer transfer) throws IOException, TransferParsingException {
        // Write a transfer document
        TransferWriter transferWriter = new TransferWriter();
        StringWriter sw = new StringWriter();
        transferWriter.write(transfer, sw);
        log.debug("POST Transfer XML: " + sw);

        // POST the transfer document
        FileContent fileContent = new FileContent(sw.toString().getBytes(), VOSTest.XML_CONTENT_TYPE);
        HttpPost post = new HttpPost(synctransServiceURL, fileContent, false);
        Subject.doAs(authSubject, new RunnableAction(post));
        Assert.assertEquals("expected POST response code = 303",303, post.getResponseCode());
        Assert.assertNull("expected POST throwable == null", post.getThrowable());

        // Get the updated transfer
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        HttpGet get = new HttpGet(post.getRedirectURL(), out);
        log.debug("GET: " + post.getRedirectURL());
        Subject.doAs(authSubject, new RunnableAction(get));
        log.debug("GET responseCode: " + get.getResponseCode());
        Assert.assertEquals("expected GET response code = 200", 200, get.getResponseCode());
        Assert.assertNull("expected GET throwable == null", get.getThrowable());
        Assert.assertTrue("expected GET Content-Type starts with " + VOSTest.XML_CONTENT_TYPE,
                get.getContentType().startsWith(VOSTest.XML_CONTENT_TYPE));

        // Read the transfer
        log.debug("GET Transfer XML: " + out);
        TransferReader transferReader = new TransferReader();
        return transferReader.read(out.toString(), "vos");
    }

}
