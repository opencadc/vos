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
import ca.nrc.cadc.util.Log4jInit;
import ca.nrc.cadc.uws.ExecutionPhase;
import ca.nrc.cadc.uws.Job;
import ca.nrc.cadc.uws.JobReader;
import java.io.BufferedReader;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.StringReader;
import java.io.StringWriter;
import java.net.MalformedURLException;
import java.net.URI;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import javax.security.auth.Subject;
import org.apache.log4j.Level;
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
import org.opencadc.vospace.io.XmlProcessor;
import org.opencadc.vospace.transfer.Direction;
import org.opencadc.vospace.transfer.Protocol;
import org.opencadc.vospace.transfer.Transfer;
import org.opencadc.vospace.transfer.TransferParsingException;
import org.opencadc.vospace.transfer.TransferReader;
import org.opencadc.vospace.transfer.TransferWriter;

public class TransferTest extends VOSTest {
    private static final Logger log = Logger.getLogger(TransferTest.class);

    static {
        Log4jInit.setLevel("ca.nrc.cadc.conformance.vos", Level.DEBUG);
        Log4jInit.setLevel("ca.nrc.cadc.vos", Level.DEBUG);
    }
    public TransferTest() {
        super();
    }

    @Test
    public void pushToVospaceTest() {
        try {
            // Create a DataNode.
            String name = "sync-push-test";
            URL nodeURL = new URL(String.format("%s/%s", synctransServiceURL, name));
            VOSURI nodeURI = new VOSURI(URI.create("vos://opencadc.org~vospace/" + name));
            DataNode testNode = new DataNode(name);
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
            log.debug("Transfer XML: " + sw);

            // POST the transfer document
            FileContent fileContent = new FileContent(sw.toString().getBytes(), VOSTest.XML_CONTENT_TYPE);
            URL transferURL = new URL(String.format("%s/%s", synctransServiceURL, name));
            log.debug("transferURL: " + transferURL);
            HttpPost post = new HttpPost(transferURL, fileContent, true);
            Subject.doAs(authSubject, new RunnableAction(post));
            Assert.assertEquals("POST response code should be 200",
                                200, post.getResponseCode());
            Assert.assertNull("PUT throwable should be null", post.getThrowable());

            // Get the transfer details
            TransferReader transferReader = new TransferReader();
            Transfer details =  transferReader.read(post.getInputStream(), XmlProcessor.VOSPACE_SCHEMA_RESOURCE_21);
            Assert.assertEquals("transfer direction", Direction.pullToVoSpace, details.getDirection());
            String endpoint = null;
            for (Protocol p : details.getProtocols()) {
                try {
                    endpoint = p.getEndpoint();
                    log.debug("endpoint: " + endpoint);
                    new URL(endpoint);
                } catch (MalformedURLException e) {
                    Assert.fail("invalid protocol endpoint: " + endpoint);
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
    public void pullFromVospaceTest() {
        try {
            // Create a DataNode
            String name = "sync-pull-test";
            URL nodeURL = new URL(String.format("%s/%s", nodesServiceURL, name));
            VOSURI nodeURI = new VOSURI(URI.create("vos://opencadc.org~vospace/" + name));
            DataNode testNode = new DataNode(name);
            log.debug("nodeURL: " + nodeURL);

            put(nodeURL, nodeURI, testNode);

            // Create a Transfer
            Transfer transfer = new Transfer(nodeURI.getURI(), Direction.pullFromVoSpace);
            transfer.getProtocols().add(new Protocol(VOS.PROTOCOL_HTTP_GET));
            transfer.getProtocols().add(new Protocol(VOS.PROTOCOL_HTTPS_GET));

            // Get the transfer document
            TransferWriter transferWriter = new TransferWriter();
            StringWriter sw = new StringWriter();
            transferWriter.write(transfer, sw);
            log.debug("Transfer XML: " + sw);

            // POST the transfer document
            FileContent fileContent = new FileContent(sw.toString().getBytes(), VOSTest.XML_CONTENT_TYPE);
            URL transferURL = new URL(String.format("%s/%s", synctransServiceURL, name));
            log.debug("transferURL: " + transferURL);
            HttpPost post = new HttpPost(transferURL, fileContent, true);
            Subject.doAs(authSubject, new RunnableAction(post));
            Assert.assertEquals("POST response code should be " + 200,
                                200, post.getResponseCode());
            Assert.assertNull("PUT throwable should be null", post.getThrowable());

            // Get the transfer details
            TransferReader transferReader = new TransferReader();
            Transfer details = transferReader.read(post.getInputStream(), XmlProcessor.VOSPACE_SCHEMA_RESOURCE_21);
            Assert.assertEquals("transfer direction", Direction.pullFromVoSpace, details.getDirection());
            Assert.assertNotNull("", details.getProtocols());
            String endpoint = null;
            for (Protocol p : details.getProtocols()) {
                try {
                    endpoint = p.getEndpoint();
                    log.debug("endpoint: " + endpoint);
                    new URL(endpoint);
                } catch (MalformedURLException e) {
                    Assert.fail("invalid protocol endpoint: " + endpoint);
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
    public void testPullLinkNodeFromVOSpace() {
        try {
            // Create a data node
            String dataName = "data-pull-link-node-test";
            URL dataNodeURL = new URL(String.format("%s/%s", nodesServiceURL, dataName));
            log.debug("dataNodeURL: " + dataNodeURL);
            VOSURI dataNodeURI = new VOSURI(URI.create("vos://opencadc.org~vospace/" + dataName));
            DataNode dataNode = new DataNode(dataName);

            put(dataNodeURL, dataNodeURI, dataNode);

            // Add a link node to the data node
            String linkName = "link-pull-link-node-test";
            URL linkNodeURL = new URL(String.format("%s/%s", nodesServiceURL, linkName));
            log.debug("linkNodeURL: " + linkNodeURL);
            VOSURI linkNodeURI = new VOSURI(URI.create("vos://opencadc.org~vospace/" + linkName));
            LinkNode linkNode = new LinkNode(dataName, dataNodeURI.getURI());

            put(linkNodeURL, linkNodeURI, linkNode);

            // POST the transfer and get the resulting transfer
            Transfer transfer = new Transfer(linkNodeURI.getURI(), Direction.pullFromVoSpace);
            transfer.getProtocols().add(new Protocol(VOS.PROTOCOL_HTTP_GET));
            transfer.getProtocols().add(new Protocol(VOS.PROTOCOL_HTTPS_GET));

            // post to sync transfer
            TransferWriter transferWriter = new TransferWriter();
            StringWriter stringWriter = new StringWriter();
            transferWriter.write(transfer, stringWriter);
            FileContent fileContent = new FileContent(stringWriter.toString().getBytes(), VOSTest.XML_CONTENT_TYPE);
            URL transferURL = new URL(String.format("%s/%s", synctransServiceURL, linkName));
            log.debug("transferURL: " + transferURL);
            HttpPost post = new HttpPost(transferURL, fileContent, true);
            Subject.doAs(authSubject, new RunnableAction(post));

            Assert.assertEquals("POST response code should be " + 200,
                                200, post.getResponseCode());
            Assert.assertNull("PUT throwable should be null", post.getThrowable());

            TransferReader transferReader = new TransferReader();
            Transfer result = transferReader.read(post.getInputStream(), XmlProcessor.VOSPACE_SCHEMA_RESOURCE_21);

            Assert.assertEquals("transfer direction", Direction.pullFromVoSpace, result.getDirection());
            Assert.assertNotNull("", result.getProtocols());
            String endpoint = null;
            for (Protocol protocol : result.getProtocols()) {
                try {
                    endpoint = protocol.getEndpoint();
                    log.debug("endpoint: " + endpoint);
                    new URL(endpoint);
                } catch (MalformedURLException e) {
                    Assert.fail("invalid protocol endpoint: " + endpoint);
                }
            }

            // Delete the nodes
            delete(linkNodeURL);
            delete(dataNodeURL);

        } catch (Exception e) {
            log.error("Unexpected error", e);
            Assert.fail("Unexpected error: " + e);
        }
    }

    @Test
    public void moveContainerNodeToContainerNode() {
        try {
            // Create a parent ContainerNode
            String parentName = "move-parent";
            URL parentNodeURL = new URL(String.format("%s/%s", nodesServiceURL, parentName));
            log.debug("parentNodeURL: " + parentNodeURL);
            VOSURI parentNodeURI = new VOSURI(URI.create("vos://opencadc.org~vospace/" + parentName));
            ContainerNode parentNode = new ContainerNode(parentName, false);
            log.debug("parentNodeURL: " + parentNodeURL);

            put(parentNodeURL, parentNodeURI, parentNode);

            // Child ContainerNode
            String childContainerName = "move-child-container";
            URL childContainerNodeURL = new URL(String.format("%s/%s", nodesServiceURL, childContainerName));
            VOSURI childContainerNodeURI = new VOSURI(URI.create(String.format("vos://opencadc.org~vospace/%s/%s",
                                                                               parentName, childContainerName)));
            ContainerNode childContainerNode = new ContainerNode(childContainerName, true);
            log.debug("childContainerNodeURL: " + childContainerNodeURL);

            put(childContainerNodeURL, childContainerNodeURI, childContainerNode);

            // Child DataNode
            String childDataName = "move-child-data";
            URL childDataNodeURL = new URL(String.format("%s/%s", nodesServiceURL, childDataName));
            VOSURI childDataNodeURI = new VOSURI(URI.create(String.format("vos://opencadc.org~vospace/%s/%s",
                                                                          parentName, childDataName)));
            DataNode childDataNode = new DataNode(childDataName);
            log.debug("childDataNodeURL: " + childDataNodeURL);

            put(childDataNodeURL, childDataNodeURI, childDataNode);

            // Destination ContainerNode
            String destinationName = "move-destination";
            URL destinationNodeURL = new URL(String.format("%s/%s", nodesServiceURL, destinationName));
            VOSURI destinationNodeURI = new VOSURI(URI.create("vos://opencadc.org~vospace/" + destinationName));
            ContainerNode destinationNode = new ContainerNode(destinationName, false);
            log.debug("destinationNodeURL: " + destinationNodeURL);

            put(destinationNodeURL, destinationNodeURI, destinationNode);

            // POST to transfer to do the move
            Transfer transfer = new Transfer(parentNodeURI.getURI(), destinationNodeURI.getURI(), false);
            TransferWriter transferWriter = new TransferWriter();
            StringWriter stringWriter = new StringWriter();
            transferWriter.write(transfer, stringWriter);
            FileContent fileContent = new FileContent(stringWriter.toString().getBytes(), VOSTest.XML_CONTENT_TYPE);
            HttpPost post = new HttpPost(transferServiceURL, fileContent, true);
            Subject.doAs(authSubject, new RunnableAction(post));

            Assert.assertEquals("POST response code should be " + 200,
                                200, post.getResponseCode());
            Assert.assertNull("PUT throwable should be null", post.getThrowable());

            // Wait for job to complete
            Thread.sleep(3000);

            // Read the response job doc
            String xml = new BufferedReader(new InputStreamReader(post.getInputStream(), StandardCharsets.UTF_8))
                .lines().collect(Collectors.joining("\n"));
            log.debug("Job XML: \n" + xml);

            // Create a Job from Job XML.
            JobReader reader = new JobReader();
            Job job = reader.read(new StringReader(xml));
            Assert.assertEquals("Job pending", ExecutionPhase.PENDING, job.getExecutionPhase());

            // Run the job.
            Map<String, Object> parameters = new HashMap<>();
            parameters.put("PHASE", "RUN");
            URL jobPhaseURL = new URL(post.getRedirectURL() + "/phase");
            post = new HttpPost(jobPhaseURL, parameters, true);
            Subject.doAs(authSubject, new RunnableAction(post));
            Assert.assertEquals("POST response code should be 303", 303, post.getResponseCode());

            // Poll the job phase for 5(?) secs until complete
            int count = 0;
            boolean done = false;
            while (!done) {
                ByteArrayOutputStream out = new ByteArrayOutputStream();
                HttpGet get = new HttpGet(jobPhaseURL, out);
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
            ByteArrayOutputStream out = new ByteArrayOutputStream();
            HttpGet get = new HttpGet(post.getRedirectURL(), out);
            Subject.doAs(authSubject, new RunnableAction(get));
            job = reader.read(new StringReader(out.toString()));
            if (!job.getExecutionPhase().equals(ExecutionPhase.COMPLETED)) {
                Assert.fail(String.format("Job error - phase: %s, reason: %s", job.getExecutionPhase(), job.getErrorSummary()));
            }

            // Source node should not be found
            NodeReader.NodeReaderResult sourceNode = get(parentNodeURL, 404);
            Assert.assertNull("source node should not be found", sourceNode);

            NodeReader.NodeReaderResult movedNode = get(destinationNodeURL, 200);
            Assert.assertTrue("moved Node should be a ContainerNode", movedNode.node instanceof ContainerNode);
            List<Node> childNodes = ((ContainerNode) movedNode.node).nodes;
            Assert.assertTrue("", childNodes.contains(childContainerNode));
            Assert.assertTrue("", childNodes.contains(childDataNode));

            // Delete nodes
            delete(destinationNodeURL);

        } catch (Exception e) {
            log.error("Unexpected error", e);
            Assert.fail("Unexpected error: " + e);
        }
    }

}
