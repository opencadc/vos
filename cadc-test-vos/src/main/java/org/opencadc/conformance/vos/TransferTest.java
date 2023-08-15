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
 *  : 5 $
 *
 ************************************************************************
 */

package org.opencadc.conformance.vos;

import ca.nrc.cadc.auth.RunnableAction;
import ca.nrc.cadc.net.FileContent;
import ca.nrc.cadc.net.HttpGet;
import ca.nrc.cadc.net.HttpPost;
import ca.nrc.cadc.reg.Standards;
import ca.nrc.cadc.uws.ExecutionPhase;
import ca.nrc.cadc.uws.Job;
import ca.nrc.cadc.uws.JobReader;
import java.io.ByteArrayOutputStream;
import java.io.StringReader;
import java.io.StringWriter;
import java.net.MalformedURLException;
import java.net.URI;
import java.net.URL;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import javax.security.auth.Subject;
import org.apache.log4j.Logger;
import org.junit.Assert;
import org.junit.Test;
import org.opencadc.vospace.ContainerNode;
import org.opencadc.vospace.DataNode;
import org.opencadc.vospace.Node;
import org.opencadc.vospace.VOS;
import org.opencadc.vospace.VOSURI;
import org.opencadc.vospace.io.NodeReader;
import org.opencadc.vospace.transfer.Direction;
import org.opencadc.vospace.transfer.Protocol;
import org.opencadc.vospace.transfer.Transfer;
import org.opencadc.vospace.transfer.TransferReader;
import org.opencadc.vospace.transfer.TransferWriter;

public class TransferTest extends VOSTest {
    private static final Logger log = Logger.getLogger(TransferTest.class);

    protected TransferTest(URI resourceID, String testCertFilename) {
        super(resourceID, testCertFilename);
    }

    @Test
    public void syncPushToVospaceTest() {
        try {
            // Create a DataNode.
            String path = "sync-push-node";
            URL nodeURL = getNodeURL(nodesServiceURL, path);
            VOSURI nodeURI = getVOSURI(path);
            log.debug("nodeURL: " + nodeURL);

            // Create a Transfer
            Transfer transfer = new Transfer(nodeURI.getURI(), Direction.pushToVoSpace);
            transfer.version = VOS.VOSPACE_21;
            Protocol protocol = new Protocol(VOS.PROTOCOL_HTTPS_PUT);
            protocol.setSecurityMethod(Standards.SECURITY_METHOD_CERT);
            transfer.getProtocols().add(protocol);

            // Get the transfer document
            TransferWriter writer = new TransferWriter();
            StringWriter sw = new StringWriter();
            writer.write(transfer, sw);
            log.debug("transfer XML: " + sw);

            // POST the transfer document
            FileContent fileContent = new FileContent(sw.toString().getBytes(), VOSTest.XML_CONTENT_TYPE);
            URL transferURL = getNodeURL(synctransServiceURL, path);
            log.debug("transfer URL: " + transferURL);
            HttpPost post = new HttpPost(synctransServiceURL, fileContent, false);
            Subject.doAs(authSubject, new RunnableAction(post));
            Assert.assertEquals("expected POST response code = 303", 303, post.getResponseCode());
            Assert.assertNull("expected POST throwable == null", post.getThrowable());

            // Get the transfer
            ByteArrayOutputStream out = new ByteArrayOutputStream();
            HttpGet get = new HttpGet(post.getRedirectURL(), out);
            log.debug("GET: " + post.getRedirectURL());
            Subject.doAs(authSubject, new RunnableAction(get));
            log.debug("GET responseCode: " + get.getResponseCode());
            Assert.assertEquals("expected GET response code = 200", 200, get.getResponseCode());
            Assert.assertNull("expected GET throwable == null", get.getThrowable());
            Assert.assertTrue("expected GET Content-Type starts with " + VOSTest.XML_CONTENT_TYPE,
                              get.getContentType().startsWith(VOSTest.XML_CONTENT_TYPE));

            // Get the transfer details
            log.debug("transfer details XML: " + out);
            TransferReader transferReader = new TransferReader();
            Transfer details = transferReader.read(out.toString(), "vos");
            Assert.assertEquals("expected transfer direction = " + Direction.pushToVoSpace,
                                Direction.pushToVoSpace, details.getDirection());
            Assert.assertNotNull("expected > 0 protocols", details.getProtocols());
            String endpoint = null;
            for (Protocol p : details.getProtocols()) {
                try {
                    endpoint = p.getEndpoint();
                    log.debug("endpoint: " + endpoint);
                    new URL(endpoint);
                } catch (MalformedURLException e) {
                    Assert.fail(String.format("invalid protocol endpoint: %s because %s", endpoint, e.getMessage()));
                }
            }

            // Delete the nodes
            delete(nodeURL);

        } catch (Exception e) {
            log.error("Unexpected error", e);
            Assert.fail("Unexpected error: " + e);
        }
    }

    @Test
    public void syncPullFromVospaceTest() {
        try {
            // Create a DataNode
            String path = "sync-pull-node";
            URL nodeURL = getNodeURL(nodesServiceURL, path);
            VOSURI nodeURI = getVOSURI(path);
            DataNode testNode = new DataNode(path);
            log.debug("nodeURL: " + nodeURL);

            put(nodeURL, nodeURI, testNode);

            // Create a Transfer
            Transfer transfer = new Transfer(nodeURI.getURI(), Direction.pullFromVoSpace);
            Protocol protocol = new Protocol(VOS.PROTOCOL_HTTPS_PUT);
            protocol.setSecurityMethod(Standards.SECURITY_METHOD_CERT);
            transfer.getProtocols().add(protocol);

            // Write a transfer document
            TransferWriter transferWriter = new TransferWriter();
            StringWriter sw = new StringWriter();
            transferWriter.write(transfer, sw);
            log.debug("transfer XML: " + sw);

            // POST the transfer document
            FileContent fileContent = new FileContent(sw.toString().getBytes(), VOSTest.XML_CONTENT_TYPE);
            HttpPost post = new HttpPost(synctransServiceURL, fileContent, false);
            Subject.doAs(authSubject, new RunnableAction(post));
            Assert.assertEquals("expected POST response code = 303",303, post.getResponseCode());
            Assert.assertNull("expected POST throwable == null", post.getThrowable());

            // Get the transfer details
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
            log.debug("Transfer details XML: " + out);
            TransferReader transferReader = new TransferReader();
            Transfer details = transferReader.read(out.toString(), "vos");
            Assert.assertEquals("expected transfer direction = " + Direction.pullFromVoSpace,
                                Direction.pullFromVoSpace, details.getDirection());
            Assert.assertNotNull("expected > 0 protocols", details.getProtocols());
            String endpoint = null;
            for (Protocol p : details.getProtocols()) {
                try {
                    endpoint = p.getEndpoint();
                    log.debug("endpoint: " + endpoint);
                    new URL(endpoint);
                } catch (MalformedURLException e) {
                    Assert.fail(String.format("invalid protocol endpoint: %s because %s", endpoint, e.getMessage()));
                }
            }

            // Delete the nodes
            delete(nodeURL);

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
            VOSURI sourceNodeURI = getVOSURI(sourceName);
            ContainerNode sourceNode = new ContainerNode(sourceName);
            log.debug("source URL: " + sourceNodeURL);

            put(sourceNodeURL, sourceNodeURI, sourceNode);

            // Source child ContainerNode
            String childContainerName = "move-source-container-child-node";
            String childContainerPath = sourceName + "/" + childContainerName;
            URL childContainerNodeURL = getNodeURL(nodesServiceURL, childContainerPath);
            VOSURI childContainerNodeURI = getVOSURI(childContainerPath);
            ContainerNode childContainerNode = new ContainerNode(childContainerName);
            log.debug("source-container-child URL: " + childContainerNodeURL);

            put(childContainerNodeURL, childContainerNodeURI, childContainerNode);

            // Source child DataNode
            String childDataName = "move-source-data-child-node";
            String childDataPath = sourceName + "/" + childDataName;
            URL childDataNodeURL = getNodeURL(nodesServiceURL, childDataPath);
            VOSURI childDataNodeURI = getVOSURI(childDataPath);
            DataNode childDataNode = new DataNode(childDataName);
            log.debug("source-data-child URL: " + childDataNodeURL);

            put(childDataNodeURL, childDataNodeURI, childDataNode);

            // Destination ContainerNode
            String destinationName = "move-destination-node";
            URL destinationNodeURL = getNodeURL(nodesServiceURL, destinationName);
            VOSURI destinationNodeURI = getVOSURI(destinationName);
            ContainerNode destinationNode = new ContainerNode(destinationName);
            log.debug("destination URL: " + destinationNodeURL);

            put(destinationNodeURL, destinationNodeURI, destinationNode);

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

            // Wait for job to complete
            Thread.sleep(3000);

            // Get the job
            ByteArrayOutputStream out = new ByteArrayOutputStream();
            HttpGet get = new HttpGet(post.getRedirectURL(), out);
            log.debug("GET: " + post.getRedirectURL());
            Subject.doAs(authSubject, new RunnableAction(get));
            log.debug("GET responseCode: " + get.getResponseCode());
            Assert.assertEquals("expected GET response code = 200", 200, get.getResponseCode());
            Assert.assertNull("expected GET throwable == null", get.getThrowable());
            Assert.assertTrue("expected GET Content-Type starts with " + VOSTest.XML_CONTENT_TYPE,
                              get.getContentType().startsWith(VOSTest.XML_CONTENT_TYPE));

            // Read the job
            log.debug("Job XML: \n" + out);
            JobReader reader = new JobReader();
            Job job = reader.read(new StringReader(out.toString()));
            Assert.assertEquals("Job pending", ExecutionPhase.PENDING, job.getExecutionPhase());

            // Run the job.
            Map<String, Object> parameters = new HashMap<>();
            parameters.put("PHASE", "RUN");
            URL jobPhaseURL = new URL(post.getRedirectURL() + "/phase");
            post = new HttpPost(jobPhaseURL, parameters, false);
            Subject.doAs(authSubject, new RunnableAction(post));
            Assert.assertEquals("expected POST response code = 303", 303, post.getResponseCode());

            // Poll the job phase for 5(?) secs until complete
            int count = 0;
            boolean done = false;
            while (!done) {
                out = new ByteArrayOutputStream();
                get = new HttpGet(jobPhaseURL, out);
                Subject.doAs(authSubject, new RunnableAction(get));
                if ("COMPLETED".equals(out.toString())) {
                    done = true;
                } else {
                    Thread.sleep(1000);
                    if (count++ >= 5) {
                        done = true;
                    }
                }
            }

            // Check if the job completed
            out = new ByteArrayOutputStream();
            get = new HttpGet(post.getRedirectURL(), out);
            Subject.doAs(authSubject, new RunnableAction(get));
            job = reader.read(new StringReader(out.toString()));
            if (!job.getExecutionPhase().equals(ExecutionPhase.COMPLETED)) {
                Assert.fail(String.format("Job error - phase: %s, reason: %s",
                                          job.getExecutionPhase(), job.getErrorSummary()));
            }

            // Source node should not be found
            get(sourceNodeURL, 404, XML_CONTENT_TYPE);

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
            delete(destinationNodeURL);

        } catch (Exception e) {
            log.error("Unexpected error", e);
            Assert.fail("Unexpected error: " + e);
        }
    }

}
