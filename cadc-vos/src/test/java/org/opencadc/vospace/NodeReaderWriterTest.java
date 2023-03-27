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

package org.opencadc.vospace;

import static org.junit.Assert.fail;

import ca.nrc.cadc.auth.HttpPrincipal;
import ca.nrc.cadc.date.DateUtil;
import ca.nrc.cadc.util.Log4jInit;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.StringWriter;
import java.net.URI;
import java.net.URISyntaxException;
import java.security.Principal;
import java.text.DateFormat;
import java.util.Date;
import java.util.List;
import java.util.Objects;
import java.util.Set;

import javax.security.auth.Subject;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 * Test read-write of Nodes using the NodeReader and NodeWriter. Every test here
 * performs a round trip: create node, write as xml, read with xml schema
 * validation enabled, compare to original node.
 *
 * @author jburke
 */
public class NodeReaderWriterTest {
    private static Logger log = Logger.getLogger(NodeReaderWriterTest.class);

    static {
        Log4jInit.setLevel("org.opencadc.vospace", Level.DEBUG);
    }

    DateFormat dateFormat = DateUtil.getDateFormat(DateUtil.IVOA_DATE_FORMAT, DateUtil.UTC);

    // TODO: make lists of nodes for a variety of test scenarios
    VOSURI containerURI;
    VOSURI linkURI;
    VOSURI dataURI;
    VOSURI structuredURI;
    VOSURI unstructuredURI;
    VOSURI detailedURI;

    ContainerNode containerNode;
    LinkNode linkNode;
    DataNode minDataNode;
    DataNode maxDataNode;
    StructuredDataNode structuredDataNode;
    UnstructuredDataNode unstructuredDataNode;
    ContainerNode detailedNode;

    public NodeReaderWriterTest() {
    }

    @BeforeClass
    public static void setUpClass() throws Exception {

    }

    @AfterClass
    public static void tearDownClass() throws Exception {
    }

    @Before
    public void setUp() throws Exception {

        URI storageID = URI.create("cadc:TEST/file.fits");

        // ContainerNode
        containerURI = new VOSURI("vos://cadc.nrc.ca!vospace/dir");
        containerNode = new ContainerNode(containerURI.getName(), false);
        DataNode dn1 = new DataNode("ngc4323", storageID);
        dn1.busy = true;
        containerNode.getNodes().add(dn1);
        DataNode dn2 = new DataNode("ngc5796", storageID);
        dn2.busy = false;
        containerNode.getNodes().add(dn2);
        DataNode dn3 = new DataNode("ngc6801", storageID);
        dn3.busy = true;
        containerNode.getNodes().add(dn3);
        addStandardNodeVariables(containerNode);
        addNodeProperties(containerNode);

        // LinkNode
        linkURI = new VOSURI("vos://cadc.nrc.ca!vospace/dir/link");
        URI target = URI.create("vos://cadc.nrc.ca!vospace/dir/target");
        linkNode = new LinkNode(linkURI.getName(), target);
        addStandardNodeVariables(linkNode);
        addNodeProperties(linkNode);

        // minimum detail DataNode
        dataURI = new VOSURI("vos://cadc.nrc.ca!vospace/dir/dataFile");
        minDataNode = new DataNode(dataURI.getName(), storageID);
        minDataNode.busy = false;

        // maximum detail DataNode
        dataURI = new VOSURI("vos://cadc.nrc.ca!vospace/dir/dataFile");
        URI contentChecksum = URI.create("md5:fd02b367a37f1ec989be20be40672fc5");
        Date contentLastModified = dateFormat.parse("2023-02-03T08:45:12.345");
        Long contentLength = 540000L;
        maxDataNode = new DataNode(dataURI.getName(), contentChecksum, contentLastModified,
                                   contentLength, storageID);
        maxDataNode.busy = true;
        maxDataNode.contentType = "content-type";
        maxDataNode.contentEncoding = "content-encoding";
        maxDataNode.accepts.add(VOS.VIEW_ANY);
        maxDataNode.provides.add(VOS.VIEW_BINARY);
        addStandardNodeVariables(maxDataNode);
        addNodeProperties(maxDataNode);

        // StructuredDataNode
        structuredURI = new VOSURI("vos://cadc.nrc.ca!vospace/dir/structuredFile");
        structuredDataNode = new StructuredDataNode(structuredURI.getName(), storageID);
        structuredDataNode.busy = true;
        addStandardNodeVariables(structuredDataNode);

        // UnstructuredDataNode
        unstructuredURI = new VOSURI("vos://cadc.nrc.ca!vospace/dir/unstructuredFile");
        unstructuredDataNode = new UnstructuredDataNode(unstructuredURI.getName(), storageID);
        unstructuredDataNode.busy = true;
        addStandardNodeVariables(unstructuredDataNode);
    }

    @After
    public void tearDown() {
    }

    private void addStandardNodeVariables(Node node) {
        Subject subject = new Subject();
        subject.getPrincipals().add(new HttpPrincipal("creatorID"));
        node.creatorID = subject;
        node.isPublic = true;
        node.isLocked = false;
        node.readOnlyGroup.add(URI.create("ivo://cadc.nrc.ca/node?ReadGroup-1"));
        node.readOnlyGroup.add(URI.create("ivo://cadc.nrc.ca/node?ReadGroup-2"));
        node.readWriteGroup.add(URI.create("ivo://cadc.nrc.ca/node?writeGroup-1"));
        node.readWriteGroup.add(URI.create("ivo://cadc.nrc.ca/node?writeGroup-2"));
    }

    private void addNodeProperties(Node node) {
        NodeProperty description = new NodeProperty(VOS.PROPERTY_URI_DESCRIPTION, "My award winning images");
        description.readOnly = true;
        node.properties.add(description);

        node.properties.add(new NodeProperty(VOS.PROPERTY_URI_READABLE));
    }

    private void comparePropertyList(Set<NodeProperty> p1, Set<NodeProperty> p2) {
        Assert.assertEquals("properties.size()", p1.size(), p2.size());
        for (NodeProperty np1 : p1) {
            boolean found = false;
            for (NodeProperty np2 : p2) {
                log.debug("looking for " + np1);
                if (np1.getKey().equals(np2.getKey()) && Objects.equals(np1.getValue(), np2.getValue())) {
                    found = true;
                    break; // inner loop
                }
            }
            Assert.assertTrue("found" + np1, found);
        }
    }

    private void compareContainerNodes(ContainerNode n1, ContainerNode n2) {
        List<Node> cn1 = n1.getNodes();
        List<Node> cn2 = n2.getNodes();

        Assert.assertEquals("nodes.size()", cn1.size(), cn2.size());
        for (int i = 0; i < cn1.size(); i++) {
            // order should be preserved since we use list
            compareNodes(cn1.get(i), cn2.get(i));
        }
    }

    private void compareDataNodes(DataNode n1, DataNode n2) {
        Assert.assertEquals("busy", n1.busy, n2.busy);
    }

    private void compareLinkNodes(LinkNode n1, LinkNode n2) {
        Assert.assertEquals("target", n1.getTarget(), n2.getTarget());
    }

    private void compareURIList(List<URI> l1, List<URI> l2) {
        Assert.assertTrue(l1.containsAll(l2));
        Assert.assertTrue(l2.containsAll(l1));
    }

    private void compareNodes(Node n1, Node n2) {
        Assert.assertEquals("same class", n1.getClass(), n2.getClass());
        Assert.assertEquals("name", n1.getName(), n2.getName());
        Assert.assertEquals("owner", n1.getPropertyValue(VOS.PROPERTY_URI_CREATOR),
                            n2.getPropertyValue(VOS.PROPERTY_URI_CREATOR));
        comparePropertyList(n1.properties, n2.properties);
        compareURIList(n1.accepts, n2.accepts);
        compareURIList(n1.provides, n2.provides);
        if (n1 instanceof ContainerNode) {
            compareContainerNodes((ContainerNode) n1, (ContainerNode) n2);
        } else if (n1 instanceof DataNode) {
            compareDataNodes((DataNode) n1, (DataNode) n2);
        } else if (n1 instanceof LinkNode) {
            compareLinkNodes((LinkNode) n1, (LinkNode) n2);
        } else {
            throw new UnsupportedOperationException("no test comparison for node type " + n1.getClass().getName());
        }
    }

    /**
     * Test of write method, of class NodeWriter.
     */
    @Test
    public void writeValidContainerNode() {
        try {
            log.debug("writeValidContainerNode");
            StringBuilder sb = new StringBuilder();
            NodeWriter instance = new NodeWriter();
            instance.write(containerURI, containerNode, sb);
            log.debug(sb.toString());

            // validate the XML
            NodeReader reader = new NodeReader();
            NodeReader.NodeReaderResult result = reader.read(sb.toString());

            Assert.assertTrue(result.node instanceof ContainerNode);
            Assert.assertEquals(containerURI, result.vosURI);
            compareNodes(containerNode, result.node);
        } catch (Exception t) {
            log.error(t);
            fail(t.getMessage());
        }
    }

    @Test
    public void writeValidMinDataNode() {
        try {
            log.debug("writeValidDataNode");
            StringBuilder sb = new StringBuilder();
            NodeWriter instance = new NodeWriter();
            instance.write(dataURI, minDataNode, sb);
            log.debug(sb.toString());

            // validate the XML
            NodeReader reader = new NodeReader();
            NodeReader.NodeReaderResult result = reader.read(sb.toString());
            Node n2 = result.node;

            // make sure default version is still 2.0
            Assert.assertEquals(VOS.VOSPACE_20, n2.version);
            Assert.assertTrue(n2 instanceof DataNode);
            Assert.assertEquals(dataURI, result.vosURI);
            compareNodes(minDataNode, n2);
        } catch (Exception t) {
            log.error(t);
            fail(t.getMessage());
        }
    }

    @Test
    public void writeValidMaxDataNode() {
        try {
            log.debug("writeValidDataNode");
            StringBuilder sb = new StringBuilder();
            NodeWriter instance = new NodeWriter();
            instance.write(dataURI, maxDataNode, sb);
            log.debug(sb.toString());

            // validate the XML
            NodeReader reader = new NodeReader();
            NodeReader.NodeReaderResult result = reader.read(sb.toString());
            Node n2 = result.node;

            // make sure default version is still 2.0
            Assert.assertEquals(VOS.VOSPACE_20, n2.version);
            Assert.assertTrue(n2 instanceof DataNode);
            Assert.assertEquals(dataURI, result.vosURI);
            compareNodes(maxDataNode, n2);
        } catch (Exception t) {
            log.error(t);
            fail(t.getMessage());
        }
    }

    @Test
    public void writeValidUnstructuredDataNode() {
        try {
            log.debug("writeValidUnstructuredDataNode");
            StringBuilder sb = new StringBuilder();
            NodeWriter instance = new NodeWriter();
            instance.write(unstructuredURI, unstructuredDataNode, sb);
            log.debug(sb.toString());

            // validate the XML
            NodeReader reader = new NodeReader();
            NodeReader.NodeReaderResult result = reader.read(sb.toString());

            Assert.assertTrue(result.node instanceof UnstructuredDataNode);
            Assert.assertEquals(unstructuredURI, result.vosURI);
            compareNodes(unstructuredDataNode, result.node);
        } catch (Exception t) {
            log.error(t);
            fail(t.getMessage());
        }
    }

    @Test
    public void writeValidStructuredDataNode() {
        try {
            log.debug("writeValidStructuredDataNode");
            StringBuilder sb = new StringBuilder();
            NodeWriter instance = new NodeWriter();
            instance.write(structuredURI, structuredDataNode, sb);
            log.debug(sb.toString());

            // validate the XML
            NodeReader reader = new NodeReader();
            NodeReader.NodeReaderResult result = reader.read(sb.toString());

            Assert.assertTrue(result.node instanceof StructuredDataNode);
            Assert.assertEquals(structuredURI, result.vosURI);
            compareNodes(structuredDataNode, result.node);
        } catch (Exception t) {
            log.error(t);
            fail(t.getMessage());
        }
    }

    @Test
    public void writeValidLinkNode() {
        try {
            log.debug("writeValidLinkNode");
            StringBuilder sb = new StringBuilder();
            NodeWriter instance = new NodeWriter();
            instance.write(linkURI, linkNode, sb);
            log.debug(sb.toString());

            // validate the XML
            NodeReader reader = new NodeReader();
            NodeReader.NodeReaderResult result = reader.read(sb.toString());

            Assert.assertTrue(result.node instanceof LinkNode);
            Assert.assertEquals(linkURI, result.vosURI);
            compareNodes(linkNode, result.node);
        } catch (Exception t) {
            log.error(t);
            fail(t.getMessage());
        }
    }

    /**
     * Test of write method, of class NodeWriter.
     */
    @Test
    public void writeToOutputStream() {
        try {
            log.debug("writeToOutputStream");
            NodeWriter instance = new NodeWriter();
            ByteArrayOutputStream bos = new ByteArrayOutputStream();
            instance.write(dataURI, minDataNode, bos);
            bos.close();

            // validate the XML
            NodeReader reader = new NodeReader();
            NodeReader.NodeReaderResult result= reader.read(new ByteArrayInputStream(bos.toByteArray()));

            Assert.assertTrue(result.node instanceof DataNode);
            Assert.assertEquals(dataURI, result.vosURI);
            compareNodes(minDataNode, result.node);
        } catch (Exception t) {
            log.error(t);
            fail(t.getMessage());
        }
    }

    /**
     * Test of write method, of class NodeWriter.
     */
    @Test
    public void writeToWriter() {
        try {
            log.debug("writeToWriter");
            NodeWriter instance = new NodeWriter();
            StringWriter sw = new StringWriter();
            instance.write(dataURI, minDataNode, sw);
            sw.close();

            log.debug(sw.toString());

            // validate the XML
            NodeReader reader = new NodeReader();
            NodeReader.NodeReaderResult result = reader.read(sw.toString());

            Assert.assertTrue(result.node instanceof DataNode);
            Assert.assertEquals(dataURI, result.vosURI);
            compareNodes(minDataNode, result.node);
        } catch (Exception t) {
            log.error(t);
            fail(t.getMessage());
        }
    }

    @Test
    public void writeMaxDetailContainerNode() {
        try {
            // ContainerNode
            Node detailedNode = createDetailedNode();

            // write it
            NodeWriter instance = new NodeWriter();
            StringWriter sw = new StringWriter();
            instance.write(detailedURI, detailedNode, sw);
            sw.close();

            log.debug(sw.toString());

            // validate the XML
            NodeReader reader = new NodeReader();
            NodeReader.NodeReaderResult result = reader.read(sw.toString());

            Assert.assertTrue(result.node instanceof ContainerNode);
            Assert.assertEquals(detailedURI, result.vosURI);
            compareNodes(detailedNode, result.node);
        } catch (Exception t) {
            log.error(t);
            fail(t.getMessage());
        }
    }

    @Test
    public void testNoSchemaValidation() {
        try {
            // accepts is not in correct sequence
            String xml = "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n"
                + "<vos:node xmlns:vos=\"http://www.ivoa.net/xml/VOSpace/v2.0\""
                + " xmlns:xsi=\"http://www.w3.org/2001/XMLSchema-instance\""
                + " uri=\"vos://cadc.nrc.ca!vospace/dir/dataFile\" xsi:type=\"vos:DataNode\" busy=\"false\">\n"
                + "  <vos:accepts />\n"
                + "  <vos:properties>\n"
                + "    <vos:property uri=\"ivo://cadc.nrc.ca/vospace/core#storageID\">cadc:TEST/file.fits</vos:property>\n"
                + "  </vos:properties>\n" + "  <vos:provides />\n" + "</vos:node>";
            log.debug(xml);

            // make sure this is not valid
            try {
                NodeReader reader = new NodeReader(true);
                reader.read(xml);
                fail("test XML is actually valid - test is broken");
            } catch (NodeParsingException expected) {
            }

            // read without validation
            NodeReader reader = new NodeReader(false);
            reader.read(xml);
        } catch (Exception t) {
            t.printStackTrace();
            log.error("unexpected exception", t);
            fail(t.getMessage());
        }
    }

    // sample node with lots of detail in it
    private ContainerNode createDetailedNode() throws URISyntaxException {

        // root ContainerNode
        detailedURI = new VOSURI("vos://cadc.nrc.ca!vospace/testContainer");
        detailedNode = new ContainerNode(detailedURI.getName(), true);

        // child DataNode with some props
        VOSURI dataURI1 = new VOSURI("vos://cadc.nrc.ca!vospace/testContainer/ngc4323");
        URI storageID1 = URI.create("cadc:TEST/file1.fits");
        DataNode dataNode1 = new DataNode(dataURI1.getName(), storageID1);
        dataNode1.busy = true;
        dataNode1.properties.add(new NodeProperty(VOS.PROPERTY_URI_AVAILABLESPACE, "123"));
        dataNode1.properties.add(new NodeProperty(VOS.PROPERTY_URI_TITLE, "The Title"));
        dataNode1.accepts.add(URI.create("ivo://cadc.nrc.ca/vospace/view#view1"));
        dataNode1.accepts.add(URI.create("ivo://cadc.nrc.ca/vospace/view#view2"));
        dataNode1.provides.add(URI.create("ivo://cadc.nrc.ca/vospace/view#something"));
        dataNode1.provides.add(URI.create("ivo://cadc.nrc.ca/vospace/view#anotherthing"));
        detailedNode.getNodes().add(dataNode1);

        // add a ContainerNode with some props
        VOSURI containerURI = new VOSURI("vos://cadc.nrc.ca!vospace/testContainer/foo");
        ContainerNode containerNode = new ContainerNode(containerURI.getName(), false);
        containerNode.readOnlyGroup.add(URI.create("ivo://cadc.nrc.ca/gms/groups#bar"));
        detailedNode.getNodes().add(containerNode);

        // add a LinkNode with some props
        VOSURI linkURI = new VOSURI("vos://cadc.nrc.ca!vospace/testContainer/aLink");
        URI target = URI.create("vos://cadc.nrc.ca!vospace/testContainer/baz");
        LinkNode linkNode = new LinkNode(linkURI.getName(), target);
        linkNode.readOnlyGroup.add(URI.create("ivo://cadc.nrc.ca/gms/groups#bar"));
        detailedNode.getNodes().add(linkNode);

        // add another DataNode below
        VOSURI dataURI2 = new VOSURI("vos://cadc.nrc.ca!vospace/testContainer/baz");
        URI storageID2 = URI.create("cadc:TEST/file2.fits");
        DataNode dataNode2 = new DataNode(dataURI2.getName(), storageID2);
        dataNode2.busy = false;
        dataNode2.properties.add(new NodeProperty(VOS.PROPERTY_URI_AVAILABLESPACE, "123"));
        dataNode2.properties.add(new NodeProperty(VOS.PROPERTY_URI_TITLE, "The Title"));
        detailedNode.getNodes().add(dataNode2);

        return detailedNode;
    }
}