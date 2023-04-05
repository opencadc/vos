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
import ca.nrc.cadc.net.HttpGet;
import ca.nrc.cadc.net.NetUtil;
import ca.nrc.cadc.util.FileUtil;
import ca.nrc.cadc.util.Log4jInit;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.net.URI;
import java.net.URL;
import javax.security.auth.Subject;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.junit.Assert;
import org.junit.Test;
import org.opencadc.vospace.ContainerNode;
import org.opencadc.vospace.DataNode;
import org.opencadc.vospace.LinkNode;
import org.opencadc.vospace.NodeProperty;
import org.opencadc.vospace.VOS;
import org.opencadc.vospace.VOSURI;
import org.opencadc.vospace.io.NodeReader;
import org.opencadc.vospace.transfer.Direction;

public class NodesTest extends VOSTest {
    private static final Logger log = Logger.getLogger(NodesTest.class);

    static {
        Log4jInit.setLevel("ca.nrc.cadc.conformance.vos", Level.DEBUG);
        Log4jInit.setLevel("ca.nrc.cadc.vos", Level.DEBUG);
    }

    public NodesTest() {
        super();
    }

    @Test
    public void containerNodeTest() {
        try {
            // create a simple container node
            String name = "container-node";
            URL nodeURL = new URL(String.format("%s/%s", nodesServiceURL, name));
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
            NodeProperty nodeProperty = new NodeProperty(VOS.PROPERTY_URI_TITLE, updatedName);
            testNode.setName(updatedName);
            testNode.setInheritPermissions(false);
            testNode.properties.add(nodeProperty);
            post(nodeURL, vosURI, testNode);

            // GET the updated node
            URL updatedNodeURL = new URL(String.format("%s/%s", nodesServiceURL, updatedName));
            VOSURI updatedVOSURI = new VOSURI(URI.create("vos://cadc.nrc.ca!vospace/" + updatedName));
            result = get(updatedNodeURL, 200);

            Assert.assertTrue(result.node instanceof ContainerNode);
            ContainerNode updatedNode = (ContainerNode) result.node;
            Assert.assertEquals(testNode, updatedNode);
            Assert.assertEquals(updatedVOSURI, result.vosURI);
            Assert.assertEquals(testNode.isInheritPermissions(), updatedNode.isInheritPermissions());
            Assert.assertTrue(updatedNode.properties.contains(nodeProperty));

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
            URL nodeURL = new URL(String.format("%s/%s", nodesServiceURL.toString(), name));
            VOSURI vosURI = new VOSURI(URI.create("vos://cadc.nrc.ca!vospace/" + name));
            DataNode testNode = new DataNode(name, URI.create("storageID"));

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
            NodeProperty nodeProperty = new NodeProperty(VOS.PROPERTY_URI_TITLE, updatedName);
            testNode.setName(updatedName);
            testNode.properties.add(nodeProperty);
            post(nodeURL, vosURI, testNode);

            // GET the updated node
            URL updatedNodeURL = new URL(String.format("%s/%s", nodesServiceURL, updatedName));
            VOSURI updatedVOSURI = new VOSURI("" + updatedName);
            result = get(updatedNodeURL, 200);

            DataNode updatedNode = (DataNode) result.node;
            Assert.assertEquals(testNode, updatedNode);
            Assert.assertEquals(updatedVOSURI, result.vosURI);
            Assert.assertEquals(testNode.getStorageID(), updatedNode.getStorageID());
            Assert.assertEquals(testNode.getName(), updatedNode.getName());
            Assert.assertTrue(updatedNode.properties.contains(nodeProperty));

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
            URL nodeURL = new URL(String.format("%s/%s", nodesServiceURL.toString(), name));
            VOSURI vosURI = new VOSURI(URI.create("vos://cadc.nrc.ca!vospace/" + name));
            LinkNode testNode = new LinkNode(name, URI.create("target"));

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
            NodeProperty nodeProperty = new NodeProperty(VOS.PROPERTY_URI_TITLE, updatedName);
            testNode.setName(updatedName);
            testNode.properties.add(nodeProperty);
            post(nodeURL, vosURI, testNode);

            // GET the updated node
            URL updatedNodeURL = new URL(String.format("%s/%s", nodesServiceURL, updatedName));
            VOSURI updatedVOSURI = new VOSURI("" + updatedName);
            result = get(updatedNodeURL, 200);

            Assert.assertTrue(result.node instanceof LinkNode);
            LinkNode updatedNode = (LinkNode) result.node;
            Assert.assertEquals(testNode, updatedNode);
            Assert.assertEquals(updatedVOSURI, result.vosURI);
            Assert.assertEquals(testNode.getTarget(), updatedNode.getTarget());
            Assert.assertEquals(testNode.getName(), updatedNode.getName());
            Assert.assertTrue(updatedNode.properties.contains(nodeProperty));

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
            URL nodeURL = new URL(String.format("%s/%s", nodesServiceURL, name));
            VOSURI vosURI = new VOSURI(URI.create("vos://cadc.nrc.ca!vospace/" + name));
            NodeProperty titleProperty = new NodeProperty(VOS.PROPERTY_URI_TITLE, "title");
            NodeProperty descriptionProperty = new NodeProperty(VOS.PROPERTY_URI_DESCRIPTION, "description");
            URI readGroup = URI.create("ivo://cadc.nrc.ca/node?ReadGroup");
            URI writeGroup = URI.create("ivo://cadc.nrc.ca/node?writeGroup");

            ContainerNode testNode = new ContainerNode(name, true);
            testNode.creatorID = authSubject;
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
            URL parentURL = new URL(String.format("%s/%s", nodesServiceURL.toString(), name));
            VOSURI parentURI = new VOSURI(URI.create("vos://cadc.nrc.ca!vospace/" + name));
            ContainerNode parent = new ContainerNode(name, true);
            put(parentURL, parentURI, parent);

            // add 6 direct child nodes
            String child1Name = "batch-child-1";
            URL child1URL = new URL(String.format("%s/%s", nodesServiceURL, child1Name));
            VOSURI child1URI = new VOSURI(URI.create("vos://cadc.nrc.ca!vospace/" + child1Name));
            ContainerNode child1 = new ContainerNode(child1Name, true);
            put(child1URL, child1URI, child1);

            String child2Name = "batch-child-2";
            URL child2URL = new URL(String.format("%s/%s", nodesServiceURL, child2Name));
            VOSURI child2URI = new VOSURI(URI.create("vos://cadc.nrc.ca!vospace/" + child2Name));
            DataNode child2 = new DataNode(child2Name, URI.create("storageID"));
            put(child2URL, child2URI, child2);

            String child3Name = "batch-child-3";
            URL child3URL = new URL(String.format("%s/%s", nodesServiceURL, child3Name));
            VOSURI child3URI = new VOSURI(URI.create("vos://cadc.nrc.ca!vospace/" + child3Name));
            LinkNode child3= new LinkNode(child3Name, URI.create("target"));
            put(child3URL, child3URI, child3);

            String child4Name = "batch-child-4";
            URL child4URL = new URL(String.format("%s/%s", nodesServiceURL, child4Name));
            VOSURI child4URI = new VOSURI(URI.create("vos://cadc.nrc.ca!vospace/" + child4Name));
            ContainerNode child4 = new ContainerNode(child4Name, true);
            put(child4URL, child4URI, child4);

            String child5Name = "batch-child-5";
            URL child5URL = new URL(String.format("%s/%s", nodesServiceURL, child5Name));
            VOSURI child5URI = new VOSURI(URI.create("vos://cadc.nrc.ca!vospace/" + child5Name));
            DataNode child5 = new DataNode(child5Name, URI.create("storageID"));
            put(child5URL, child5URI, child5);

            String child6Name = "batch-child-6";
            URL child6URL = new URL(String.format("%s/%s", nodesServiceURL, child6Name));
            VOSURI child6URI = new VOSURI(URI.create("vos://cadc.nrc.ca!vospace/" + child6Name));
            LinkNode child6= new LinkNode(child6Name, URI.create("target"));
            put(child6URL, child6URI, child6);

            // Get nodes 1 - 3
            URL nodeURL = new URL(String.format("%s?uri=%s&limit=%d", parentURL,
                                                NetUtil.encode(child1URI.getURI().toASCIIString()), 3));
            NodeReader.NodeReaderResult result = get(nodeURL, 200);

            Assert.assertTrue(result.node instanceof ContainerNode);
            ContainerNode parentNode = (ContainerNode) result.node;
            Assert.assertEquals(parentNode.nodes.size(), 3);
            Assert.assertTrue(parentNode.nodes.contains(child1));
            Assert.assertTrue(parentNode.nodes.contains(child2));
            Assert.assertTrue(parentNode.nodes.contains(child3));

            // Get nodes 3 - 5
            nodeURL = new URL(String.format("%s?uri=%s&limit=%d", parentURL,
                                            NetUtil.encode(child3URI.getURI().toASCIIString()), 3));
            result = get(nodeURL, 200);

            Assert.assertTrue(result.node instanceof ContainerNode);
            parentNode = (ContainerNode) result.node;
            Assert.assertEquals(parentNode.nodes.size(), 3);
            Assert.assertTrue(parentNode.nodes.contains(child3));
            Assert.assertTrue(parentNode.nodes.contains(child4));
            Assert.assertTrue(parentNode.nodes.contains(child5));

            // Get nodes 5 - 6
            nodeURL = new URL(String.format("%s?uri=%s&limit=%d", parentURL,
                                            NetUtil.encode(child5URI.getURI().toASCIIString()), 3));
            result = get(nodeURL, 200);

            Assert.assertTrue(result.node instanceof ContainerNode);
            parentNode = (ContainerNode) result.node;
            Assert.assertEquals(parentNode.nodes.size(), 2);
            Assert.assertTrue(parentNode.nodes.contains(child5));
            Assert.assertTrue(parentNode.nodes.contains(child6));

            // delete the parent node
            delete(parentURL);

            // GET the deleted node, which should fail
            get(parentURL, 404);

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
            URL parentURL = new URL(String.format("%s/%s", nodesServiceURL.toString(), name));
            VOSURI parentURI = new VOSURI(URI.create("vos://cadc.nrc.ca!vospace/" + name));
            ContainerNode parent = new ContainerNode(name, true);
            put(parentURL, parentURI, parent);

            // add 3 direct child nodes
            String child1Name = "limit-child-1";
            URL child1URL = new URL(String.format("%s/%s", nodesServiceURL, child1Name));
            VOSURI child1URI = new VOSURI(URI.create("vos://cadc.nrc.ca!vospace/" + child1Name));
            ContainerNode child1 = new ContainerNode(child1Name, true);
            put(child1URL, child1URI, child1);

            String child2Name = "limit-child-2";
            URL child2URL = new URL(String.format("%s/%s", nodesServiceURL, child2Name));
            VOSURI child2URI = new VOSURI(URI.create("vos://cadc.nrc.ca!vospace/" + child2Name));
            DataNode child2 = new DataNode(child2Name, URI.create("storageID"));
            put(child2URL, child2URI, child2);

            String child3Name = "limit-child-3";
            URL child3URL = new URL(String.format("%s/%s", nodesServiceURL, child3Name));
            VOSURI child3URI = new VOSURI(URI.create("vos://cadc.nrc.ca!vospace/" + child3Name));
            LinkNode child3= new LinkNode(child3Name, URI.create("target"));
            put(child3URL, child3URI, child3);

            // get the node with a limit of 2 child nodes
            URL nodeURL = new URL(String.format("%s?limit=%d", parentURL, 2));
            NodeReader.NodeReaderResult result = get(nodeURL, 200);

            Assert.assertTrue(result.node instanceof ContainerNode);
            ContainerNode parentNode = (ContainerNode) result.node;
            Assert.assertEquals(parentNode.nodes.size(), 2);
            Assert.assertTrue(parentNode.nodes.contains(child1));
            Assert.assertTrue(parentNode.nodes.contains(child2));

            // delete the parent node
            delete(parentURL);

            // GET the deleted node, which should fail
            get(parentURL, 404);

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
            URL nodeURL = new URL(String.format("%s/%s", nodesServiceURL, name));
            VOSURI nodeURI = new VOSURI(URI.create("vos://cadc.nrc.ca!vospace/" + name));
            NodeProperty titleProperty = new NodeProperty(VOS.PROPERTY_URI_TITLE, "title");
            NodeProperty descriptionProperty = new NodeProperty(VOS.PROPERTY_URI_DESCRIPTION, "description");
            URI readGroup = URI.create("ivo://cadc.nrc.ca/node?ReadGroup");
            URI writeGroup = URI.create("ivo://cadc.nrc.ca/node?writeGroup");

            ContainerNode testNode = new ContainerNode(name, true);
            testNode.creatorID = authSubject;
            testNode.isPublic = true;
            testNode.isLocked = true;
            testNode.properties.add(titleProperty);
            testNode.properties.add(descriptionProperty);
            testNode.readOnlyGroup.add(readGroup);
            testNode.readWriteGroup.add(writeGroup);
            testNode.nodes.add(new DataNode("detail-child-node", URI.create("storageID")));

            put(nodeURL, nodeURI, testNode);

            // get the node with a min detail
            URL detailURL = new URL(nodeURL + "?detail=min");
            NodeReader.NodeReaderResult result = get(detailURL, 200);

            Assert.assertTrue(result.node instanceof ContainerNode);
            ContainerNode parentNode = (ContainerNode) result.node;

            Assert.assertTrue("node properties should be empty", parentNode.properties.isEmpty());
            Assert.assertTrue("child nodes should be empty", parentNode.nodes.isEmpty());

            // get the node
            result = get(nodeURL, 200);

            Assert.assertTrue(result.node instanceof ContainerNode);
            parentNode = (ContainerNode) result.node;

            Assert.assertFalse("node properties shouldn't be empty", parentNode.properties.isEmpty());
            Assert.assertFalse("child nodes shouldn't be empty", parentNode.nodes.isEmpty());

            // delete the node
            delete(nodeURL);

            // GET the deleted node, which should fail
            get(nodeURL, 404);

        } catch (Exception e) {
            log.error("Unexpected error", e);
            Assert.fail("Unexpected error: " + e);
        }
    }

    @Test
    public void dataViewTest() {
        try {
            // upload test file
            String name = "view-data-test.txt";
            URL nodeURL = new URL(String.format("%s/%s", nodesServiceURL, name));
            VOSURI nodeURI = new VOSURI(URI.create("vos://cadc.nrc.ca!vospace/" + name));
            File testFile = FileUtil.getFileFromResource(name, NodesTest.class);

            put(nodeURL, testFile);

            // get the node with view=data
            URL getURL = new URL(nodeURL + "?view=data");
            ByteArrayOutputStream out = new ByteArrayOutputStream();
            HttpGet get = new HttpGet(getURL, out);
            get.setFollowRedirects(false);
            Subject.doAs(authSubject, new RunnableAction(get));

            Assert.assertNotNull(get.getThrowable());
            URL redirectURL = get.getRedirectURL();
            Assert.assertNotNull(redirectURL);
            log.debug("location = " + redirectURL);

            String query = redirectURL.getQuery();
            String[] params = query.split("&");
            Assert.assertEquals(3, params.length);
            for (String p : params) {
                String[] pv = p.split("=");
                String key = pv[0];
                String val = NetUtil.decode(pv[1]);
                Assert.assertEquals(2, pv.length);
                if ("target".equalsIgnoreCase(key)) {
                    Assert.assertEquals(nodeURI.getURI().toASCIIString(), val);
                } else if("protocol".equalsIgnoreCase(key)) {
                    Assert.assertEquals(VOS.PROTOCOL_HTTP_GET.toASCIIString(), val);
                } else if("direction".equalsIgnoreCase(key)) {
                    Assert.assertEquals(Direction.pullFromVoSpace.getValue(), val);
                } else {
                    Assert.fail(String.format("unexpected transfer parameter: %s = %s", key, val));
                }
            }

            // delete the file
            delete(nodeURL);

            // GET the deleted node, which should fail
            get(nodeURL, 404);

        } catch (Exception e) {
            log.error("Unexpected error", e);
            Assert.fail("Unexpected error: " + e);
        }
    }

}
