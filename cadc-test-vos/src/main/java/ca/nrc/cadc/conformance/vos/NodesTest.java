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

package ca.nrc.cadc.conformance.vos;

import ca.nrc.cadc.auth.AuthMethod;
import ca.nrc.cadc.auth.RunnableAction;
import ca.nrc.cadc.auth.SSLUtil;
import ca.nrc.cadc.net.FileContent;
import ca.nrc.cadc.net.HttpDelete;
import ca.nrc.cadc.net.HttpGet;
import ca.nrc.cadc.net.HttpPost;
import ca.nrc.cadc.net.HttpUpload;
import ca.nrc.cadc.net.NetUtil;
import ca.nrc.cadc.reg.Standards;
import ca.nrc.cadc.reg.client.RegistryClient;
import ca.nrc.cadc.util.FileUtil;
import ca.nrc.cadc.util.Log4jInit;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import javax.security.auth.Subject;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.junit.Assert;
import org.junit.Test;
import org.opencadc.vospace.ContainerNode;
import org.opencadc.vospace.DataNode;
import org.opencadc.vospace.LinkNode;
import org.opencadc.vospace.Node;
import org.opencadc.vospace.NodeParsingException;
import org.opencadc.vospace.NodeProperty;
import org.opencadc.vospace.NodeReader;
import org.opencadc.vospace.NodeWriter;
import org.opencadc.vospace.VOS;
import org.opencadc.vospace.VOSURI;

public class NodesTest {

    private static final Logger log = Logger.getLogger(NodesTest.class);
    public static final URI SERVICE_RESOURCE_ID = URI.create("ivo://opencadc.org/vault");
    private static final String TEST_CERT_NAME = "vault-test.pem";
    private static final String XML_CONTENT_TYPE = "application/xml";

    private final URL serviceURL;
    private final Subject authSubject;

    static {
        Log4jInit.setLevel("ca.nrc.cadc.conformance.vos", Level.DEBUG);
    }

    public NodesTest() {
        RegistryClient regClient = new RegistryClient();
        serviceURL = regClient.getServiceURL(SERVICE_RESOURCE_ID, Standards.VOSPACE_NODES_20, AuthMethod.ANON);
        log.info(String.format("%s: %s", Standards.VOSPACE_NODES_20, serviceURL));

        File testCert = FileUtil.getFileFromResource(TEST_CERT_NAME, NodesTest.class);
        authSubject = SSLUtil.createSubject(testCert);
        log.debug("authSubject: " + authSubject);
    }

    @Test
    public void containerNodeTest() {
        try {
            // create a simple container node
            String name = "container-node";
            URL nodeURL = new URL(String.format("%s/%s", serviceURL, name));
            VOSURI vosURI = new VOSURI(URI.create("vos://cadc.nrc.ca!vospace/" + name));

            ContainerNode testNode = new ContainerNode(name, true);

            // PUT the node
            put(nodeURL, vosURI, testNode);

            // GET the new node
            NodeReader.NodeReaderResult result = get(nodeURL, 200);
            Assert.assertTrue(result.node instanceof ContainerNode);
            ContainerNode persistedNode = (ContainerNode) result.node;
            Assert.assertEquals(testNode, persistedNode);
            Assert.assertEquals(vosURI, result.vosURI);

            // POST an update to the node
            String updatedName = "updated-container-node";
            URL updatedNodeURL = new URL(String.format("%s/%s", serviceURL, updatedName));
            VOSURI updatedVOSURI = new VOSURI("" + updatedName);
            testNode.setInheritPermissions(false);
            testNode.setName(updatedName);

            post(updatedNodeURL, updatedVOSURI, testNode);

            // GET the updated node
            result = get(updatedNodeURL, 200);

            Assert.assertTrue(result.node instanceof ContainerNode);
            ContainerNode updatedNode = (ContainerNode) result.node;
            Assert.assertEquals(testNode, updatedNode);
            Assert.assertEquals(vosURI, result.vosURI);
            Assert.assertEquals(testNode.isInheritPermissions(), updatedNode.isInheritPermissions());
            Assert.assertEquals(testNode.getName(), updatedNode.getName());

            // DELETE the node
            delete(updatedNodeURL);

            // GET the deleted node, which should fail
            get(updatedNodeURL, 404);

        } catch (Exception e) {
            log.error("Unexpected error", e);
            Assert.fail("Unexpected error: " + e);
        }
    }

    @Test
    public void dataNodeTest() {
        try {
            // create a simple data node
            String name = "data-node";
            URL nodeURL = new URL(String.format("%s/%s", serviceURL.toString(), name));
            VOSURI vosURI = new VOSURI(URI.create("vos://cadc.nrc.ca!vospace/" + name));
            URI storageID = URI.create("cadc:TEST/file.txt");

            DataNode testNode = new DataNode(name, storageID);

            // PUT the node
            put(nodeURL, vosURI, testNode);

            // GET the new node
            NodeReader.NodeReaderResult result = get(nodeURL, 200);

            Assert.assertTrue(result.node instanceof DataNode);
            DataNode persistedNode = (DataNode) result.node;
            Assert.assertEquals(testNode, persistedNode);
            Assert.assertEquals(vosURI, result.vosURI);
            Assert.assertEquals(testNode.getStorageID(), persistedNode.getStorageID());

            // POST an update to the node
            String updatedName = "updated-data-node";
            URL updatedNodeURL = new URL(String.format("%s/%s", serviceURL, updatedName));
            VOSURI updatedVOSURI = new VOSURI("" + updatedName);
            testNode.setName(updatedName);

            post(updatedNodeURL, updatedVOSURI, testNode);

            // GET the updated node
            result = get(updatedNodeURL, 200);

            DataNode updatedNode = (DataNode) result.node;
            Assert.assertEquals(testNode, updatedNode);
            Assert.assertEquals(vosURI, result.vosURI);
            Assert.assertEquals(testNode.getStorageID(), updatedNode.getStorageID());
            Assert.assertEquals(testNode.getName(), updatedNode.getName());

            // DELETE the node
            delete(updatedNodeURL);

            // GET the deleted node, which should fail
            get(updatedNodeURL, 404);

        } catch (Exception e) {
            log.error("Unexpected error", e);
            Assert.fail("Unexpected error: " + e);
        }
    }

    @Test
    public void linkNodeTest() {
        try {
            // create a simple link node
            String name = "link-node";
            URL nodeURL = new URL(String.format("%s/%s", serviceURL.toString(), name));
            VOSURI vosURI = new VOSURI(URI.create("vos://cadc.nrc.ca!vospace/" + name));
            URI target = URI.create("target");

            LinkNode testNode = new LinkNode(name, target);

            // PUT the node
            put(nodeURL, vosURI, testNode);

            // GET the new node
            NodeReader.NodeReaderResult result = get(nodeURL, 200);

            Assert.assertTrue(result.node instanceof LinkNode);
            LinkNode persistedNode = (LinkNode) result.node;
            Assert.assertEquals(testNode, persistedNode);
            Assert.assertEquals(vosURI, result.vosURI);
            Assert.assertEquals(testNode.getTarget(), persistedNode.getTarget());
            Assert.assertEquals(testNode.getName(), persistedNode.getName());

            // POST an update to the node
            String updatedName = "updated-link-node";
            URL updatedNodeURL = new URL(String.format("%s/%s", serviceURL, updatedName));
            VOSURI updatedVOSURI = new VOSURI("" + updatedName);
            testNode.setName(updatedName);

            post(updatedNodeURL, updatedVOSURI, testNode);

            // GET the updated node
            result = get(updatedNodeURL, 200);

            Assert.assertTrue(result.node instanceof LinkNode);
            LinkNode updatedNode = (LinkNode) result.node;
            Assert.assertEquals(testNode, updatedNode);
            Assert.assertEquals(vosURI, result.vosURI);
            Assert.assertEquals(testNode.getTarget(), updatedNode.getTarget());
            Assert.assertEquals(testNode.getName(), updatedNode.getName());

            // DELETE the node
            delete(updatedNodeURL);

            // GET the deleted node, which should fail
            get(updatedNodeURL, 404);

        } catch (Exception e) {
            log.error("Unexpected error", e);
            Assert.fail("Unexpected error: " + e);
        }
    }

    @Test
    public void complexNodeTest() {
        try {
            // create a fully populated container node
            String name = "complex-node";
            URL nodeURL = new URL(String.format("%s/%s", serviceURL, name));
            VOSURI vosURI = new VOSURI(URI.create("vos://cadc.nrc.ca!vospace/" + name));
            NodeProperty titleProperty = new NodeProperty(VOS.PROPERTY_URI_TITLE, "title");
            NodeProperty descriptionProperty = new NodeProperty(VOS.PROPERTY_URI_DESCRIPTION, "description");
            URI readGroup = URI.create("ivo://cadc.nrc.ca/node?ReadGroup");
            URI writeGroup = URI.create("ivo://cadc.nrc.ca/node?writeGroup");

            ContainerNode testNode = new ContainerNode(name, true);
            testNode.creatorID = authSubject;;
            testNode.isPublic = true;
            testNode.isLocked = true;
            testNode.properties.add(titleProperty);
            testNode.properties.add(descriptionProperty);
            testNode.readOnlyGroup.add(readGroup);
            testNode.readWriteGroup.add(writeGroup);

            // PUT the node
            put(nodeURL, vosURI, testNode);

            // GET the new node
            NodeReader.NodeReaderResult result = get(nodeURL, 200);

            ContainerNode persistedNode = (ContainerNode) result.node;
            Assert.assertEquals(testNode, persistedNode);
            Assert.assertEquals(vosURI, result.vosURI);
            Assert.assertEquals(testNode.creatorID, persistedNode.creatorID);
            Assert.assertEquals(testNode.isPublic, persistedNode.isPublic);
            Assert.assertEquals(testNode.isLocked, persistedNode.isLocked);
            Assert.assertEquals(testNode.properties.size(), persistedNode.properties.size());
            Assert.assertTrue(persistedNode.properties.contains(titleProperty));
            Assert.assertTrue(persistedNode.properties.contains(descriptionProperty));
            Assert.assertEquals(testNode.readOnlyGroup.size(), persistedNode.readOnlyGroup.size());
            Assert.assertTrue(persistedNode.readOnlyGroup.contains(readGroup));
            Assert.assertEquals(testNode.readWriteGroup.size(), persistedNode.readWriteGroup.size());
            Assert.assertTrue(persistedNode.readWriteGroup.contains(writeGroup));

            // POST an update to the node
            NodeProperty formatProperty = new NodeProperty(VOS.PROPERTY_URI_FORMAT, XML_CONTENT_TYPE);
            NodeProperty quotaProperty = new NodeProperty(VOS.PROPERTY_URI_QUOTA, "999");
            URI updatedReadGroup = URI.create("ivo://cadc.nrc.ca/node?UpdatedReadGroup");
            URI updatedWriteGroup = URI.create("ivo://cadc.nrc.ca/node?UpdatedWriteGroup");

            testNode.isPublic = false;
            testNode.isLocked = false;
            testNode.properties.clear();
            testNode.properties.add(formatProperty);
            testNode.properties.add(quotaProperty);
            testNode.readOnlyGroup.clear();
            testNode.readWriteGroup.clear();
            testNode.readOnlyGroup.add(updatedReadGroup);
            testNode.readWriteGroup.add(updatedWriteGroup);

            post(nodeURL, vosURI, testNode);

            // GET the updated node
            result = get(nodeURL, 200);

            Assert.assertTrue(result.node instanceof ContainerNode);
            ContainerNode updatedNode = (ContainerNode) result.node;
            Assert.assertEquals(testNode, updatedNode);
            Assert.assertEquals(vosURI, result.vosURI);
            Assert.assertEquals(testNode.creatorID, updatedNode.creatorID);
            Assert.assertEquals(testNode.isPublic, updatedNode.isPublic);
            Assert.assertEquals(testNode.isLocked, updatedNode.isLocked);
            Assert.assertEquals(testNode.properties.size(), updatedNode.properties.size());
            Assert.assertTrue(updatedNode.properties.contains(titleProperty));
            Assert.assertTrue(updatedNode.properties.contains(descriptionProperty));
            Assert.assertEquals(testNode.readOnlyGroup.size(), updatedNode.readOnlyGroup.size());
            Assert.assertTrue(updatedNode.readOnlyGroup.contains(readGroup));
            Assert.assertEquals(testNode.readWriteGroup.size(), updatedNode.readWriteGroup.size());
            Assert.assertTrue(updatedNode.readWriteGroup.contains(writeGroup));

            // DELETE the node
            delete(nodeURL);

            // GET the deleted node, which should fail
            get(nodeURL, 404);

        } catch (Exception e) {
            log.error("Unexpected error", e);
            Assert.fail("Unexpected error: " + e);
        }
    }

    @Test
    public void listInBatchesTest() {
        try {
            // create root container node
            String name = "batch-parent";
            URL parentURL = new URL(String.format("%s/%s", serviceURL.toString(), name));
            VOSURI parentURI = new VOSURI(URI.create("vos://cadc.nrc.ca!vospace/" + name));
            ContainerNode parent = new ContainerNode(name, true);
            put(parentURL, parentURI, parent);

            // add 6 direct child nodes
            String child1Name = "batch-child-1";
            URL child1URL = new URL(String.format("%s/%s", serviceURL, name));
            VOSURI child1URI = new VOSURI(URI.create("vos://cadc.nrc.ca!vospace/" + name));
            ContainerNode child1 = new ContainerNode(name, true);
            put(child1URL, child1URI, child1);

            String child2Name = "batch-child-2";
            URL child2URL = new URL(String.format("%s/%s", serviceURL, name));
            VOSURI child2URI = new VOSURI(URI.create("vos://cadc.nrc.ca!vospace/" + name));
            DataNode child2 = new DataNode(name, URI.create(""));
            put(child2URL, child2URI, child2);

            String child3Name = "batch-child-3";
            URL child3URL = new URL(String.format("%s/%s", serviceURL, name));
            VOSURI child3URI = new VOSURI(URI.create("vos://cadc.nrc.ca!vospace/" + name));
            LinkNode child3= new LinkNode(name, URI.create(""));
            put(child3URL, child3URI, child3);

            String child4Name = "batch-child-4";
            URL child4URL = new URL(String.format("%s/%s", serviceURL, name));
            VOSURI child4URI = new VOSURI(URI.create("vos://cadc.nrc.ca!vospace/" + name));
            ContainerNode child4 = new ContainerNode(name, true);
            put(child1URL, child1URI, child4);

            String child5Name = "batch-child-5";
            URL child5URL = new URL(String.format("%s/%s", serviceURL, name));
            VOSURI child5URI = new VOSURI(URI.create("vos://cadc.nrc.ca!vospace/" + name));
            DataNode child5 = new DataNode(name, URI.create(""));
            put(child5URL, child5URI, child5);

            String child6Name = "batch-child-6";
            URL child6URL = new URL(String.format("%s/%s", serviceURL, name));
            VOSURI child6URI = new VOSURI(URI.create("vos://cadc.nrc.ca!vospace/" + name));
            LinkNode child6= new LinkNode(name, URI.create(""));
            put(child6URL, child6URI, child6);

            // Get nodes 1 - 3
            URL nodeURL = new URL(String.format("%s?uri=%s&limit=%d", parentURL,
                                                NetUtil.encode(child1URI.getURI().toASCIIString()), 3));
            NodeReader.NodeReaderResult result = get(nodeURL, 200);

            Assert.assertTrue(result.node instanceof ContainerNode);
            ContainerNode parentNode = (ContainerNode) result.node;
            Assert.assertEquals(parentNode.getNodes().size(), 3);
            Assert.assertTrue(parentNode.getNodes().contains(child1));
            Assert.assertTrue(parentNode.getNodes().contains(child2));
            Assert.assertTrue(parentNode.getNodes().contains(child3));

            // Get nodes 3 - 5
            nodeURL = new URL(String.format("%s?uri=%s&limit=%d", parentURL,
                                            NetUtil.encode(child3URI.getURI().toASCIIString()), 3));
            result = get(nodeURL, 200);

            Assert.assertTrue(result.node instanceof ContainerNode);
            parentNode = (ContainerNode) result.node;
            Assert.assertEquals(parentNode.getNodes().size(), 3);
            Assert.assertTrue(parentNode.getNodes().contains(child3));
            Assert.assertTrue(parentNode.getNodes().contains(child4));
            Assert.assertTrue(parentNode.getNodes().contains(child5));

            // Get nodes 5 - 6
            nodeURL = new URL(String.format("%s?uri=%s&limit=%d", parentURL,
                                            NetUtil.encode(child5URI.getURI().toASCIIString()), 3));
            result = get(nodeURL, 200);

            Assert.assertTrue(result.node instanceof ContainerNode);
            parentNode = (ContainerNode) result.node;
            Assert.assertEquals(parentNode.getNodes().size(), 2);
            Assert.assertTrue(parentNode.getNodes().contains(child5));
            Assert.assertTrue(parentNode.getNodes().contains(child6));
        } catch (Exception e) {
            log.error("Unexpected error", e);
            Assert.fail("Unexpected error: " + e);
        }
    }

    @Test
    public void limitNodesTest() {
        try {
            // create root container node
            String name = "limit-parent";
            URL parentURL = new URL(String.format("%s/%s", serviceURL.toString(), name));
            VOSURI parentURI = new VOSURI(URI.create("vos://cadc.nrc.ca!vospace/" + name));
            ContainerNode parent = new ContainerNode(name, true);
            put(parentURL, parentURI, parent);

            // add 3 direct child nodes
            String child1Name = "limit-child-1";
            URL child1URL = new URL(String.format("%s/%s", serviceURL, name));
            VOSURI child1URI = new VOSURI(URI.create("vos://cadc.nrc.ca!vospace/" + name));
            ContainerNode child1 = new ContainerNode(name, true);
            put(child1URL, child1URI, child1);

            String child2Name = "limit-child-2";
            URL child2URL = new URL(String.format("%s/%s", serviceURL, name));
            VOSURI child2URI = new VOSURI(URI.create("vos://cadc.nrc.ca!vospace/" + name));
            DataNode child2 = new DataNode(name, URI.create(""));
            put(child2URL, child2URI, child2);

            String child3Name = "limit-child-3";
            URL child3URL = new URL(String.format("%s/%s", serviceURL, name));
            VOSURI child3URI = new VOSURI(URI.create("vos://cadc.nrc.ca!vospace/" + name));
            LinkNode child3= new LinkNode(name, URI.create(""));
            put(child3URL, child3URI, child3);

            // get the node with a limit of 2 child nodes
            URL nodeURL = new URL(String.format("%s?limit=%d", parentURL, 2));
            NodeReader.NodeReaderResult result = get(nodeURL, 200);

            Assert.assertTrue(result.node instanceof ContainerNode);
            ContainerNode parentNode = (ContainerNode) result.node;
            Assert.assertEquals(parentNode.getNodes().size(), 2);
            Assert.assertTrue(parentNode.getNodes().contains(child1));
            Assert.assertTrue(parentNode.getNodes().contains(child2));
        } catch (Exception e) {
            log.error("Unexpected error", e);
            Assert.fail("Unexpected error: " + e);
        }
    }

    @Test
    public void nodeDetailTest() {
        try {
            // create a fully populated container node
            String name = "detail-node";
            URL nodeURL = new URL(String.format("%s/%s", serviceURL, name));
            VOSURI vosURI = new VOSURI(URI.create("vos://cadc.nrc.ca!vospace/" + name));
            NodeProperty titleProperty = new NodeProperty(VOS.PROPERTY_URI_TITLE, "title");
            NodeProperty descriptionProperty = new NodeProperty(VOS.PROPERTY_URI_DESCRIPTION, "description");
            URI readGroup = URI.create("ivo://cadc.nrc.ca/node?ReadGroup");
            URI writeGroup = URI.create("ivo://cadc.nrc.ca/node?writeGroup");

            ContainerNode testNode = new ContainerNode(name, true);
            testNode.creatorID = authSubject;;
            testNode.isPublic = true;
            testNode.isLocked = true;
            testNode.properties.add(titleProperty);
            testNode.properties.add(descriptionProperty);
            testNode.readOnlyGroup.add(readGroup);
            testNode.readWriteGroup.add(writeGroup);
            testNode.getNodes().add(new DataNode("detail-child-node", URI.create("resource")));

            // get the node with a min detail
            URL detailURL = new URL(nodeURL + "?detail=min");
            NodeReader.NodeReaderResult result = get(detailURL, 200);

            Assert.assertTrue(result.node instanceof ContainerNode);
            ContainerNode parentNode = (ContainerNode) result.node;

            Assert.assertTrue("node properties should be empty", parentNode.properties.isEmpty());
            Assert.assertTrue("child nodes should be empty", parentNode.getNodes().isEmpty());

            // get the node
            result = get(nodeURL, 200);

            Assert.assertTrue(result.node instanceof ContainerNode);
            parentNode = (ContainerNode) result.node;

            Assert.assertFalse("node properties shouldn't be empty", parentNode.properties.isEmpty());
            Assert.assertFalse("child nodes shouldn't be empty", parentNode.getNodes().isEmpty());
        } catch (Exception e) {
            log.error("Unexpected error", e);
            Assert.fail("Unexpected error: " + e);
        }
    }

    private void put(URL nodeURL, VOSURI vosURI, Node node)
        throws IOException {
        StringBuilder sb = new StringBuilder();
        NodeWriter writer = new NodeWriter();
        writer.write(vosURI, node, sb);
        InputStream in = new ByteArrayInputStream(sb.toString().getBytes());

        HttpUpload put = new HttpUpload(in, nodeURL);
        put.setRequestProperty("Content-Type", XML_CONTENT_TYPE);
        Subject.doAs(authSubject, new RunnableAction(put));
        log.debug("responseCode: " + put.getResponseCode());
        Assert.assertEquals("PUT response code should be 201",
                            201, put.getResponseCode());
        Assert.assertNull("PUT throwable should be null", put.getThrowable());
    }

    private NodeReader.NodeReaderResult get(URL url, int responseCode)
        throws NodeParsingException {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        HttpGet get = new HttpGet(url, out);
        Subject.doAs(authSubject, new RunnableAction(get));
        log.debug("responseCode: " + get.getResponseCode());
        Assert.assertEquals("GET response code should be " + responseCode,
                            responseCode, get.getResponseCode());

        if (responseCode == 200) {
            Assert.assertNull("GET throwable should be null", get.getThrowable());
            Assert.assertEquals("GET Content-Type should be " + XML_CONTENT_TYPE, XML_CONTENT_TYPE, get.getContentType());

            NodeReader reader = new NodeReader();
            NodeReader.NodeReaderResult result = reader.read(out.toString());
            Assert.assertNotNull(result);
            Assert.assertNotNull(result.node);
            Assert.assertNotNull(result.vosURI);
            return result;
        } else {
            return null;
        }
    }

    private void post(URL nodeURL, VOSURI vosURI, Node node)
        throws IOException {
        StringBuilder sb = new StringBuilder();
        NodeWriter writer = new NodeWriter();
        writer.write(vosURI, node, sb);

        FileContent content = new FileContent(sb.toString(), XML_CONTENT_TYPE, StandardCharsets.UTF_8);

        HttpPost post = new HttpPost(nodeURL, content, true);
        Subject.doAs(authSubject, new RunnableAction(post));
        log.debug("responseCode: " + post.getResponseCode());
        Assert.assertEquals("POST response code should be 200",
                            200, post.getResponseCode());
        Assert.assertNull("POST throwable should be null", post.getThrowable());
    }

    private void delete(URL nodeURL) {
        HttpDelete delete = new HttpDelete(nodeURL, true);
        Subject.doAs(authSubject, new RunnableAction(delete));
        log.debug("responseCode: " + delete.getResponseCode());
        Assert.assertEquals("DELETE response code should be 204",
                            204, delete.getResponseCode());
        Assert.assertNull("DELETE throwable should be null", delete.getThrowable());
    }

}
