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
import java.io.ByteArrayOutputStream;
import java.net.URI;
import java.net.URL;
import javax.security.auth.Subject;
import org.apache.log4j.Logger;
import org.junit.Assert;
import org.junit.Test;
import org.opencadc.gms.GroupURI;
import org.opencadc.vospace.ContainerNode;
import org.opencadc.vospace.DataNode;
import org.opencadc.vospace.LinkNode;
import org.opencadc.vospace.Node;
import org.opencadc.vospace.NodeProperty;
import org.opencadc.vospace.VOS;
import org.opencadc.vospace.VOSURI;
import org.opencadc.vospace.io.NodeReader;
import org.opencadc.vospace.transfer.Direction;

public class NodesTest extends VOSTest {
    private static final Logger log = Logger.getLogger(NodesTest.class);

    protected NodesTest(URI resourceID, String testCertFilename) {
        super(resourceID, testCertFilename);
    }

    @Test
    public void testContainerNode() {
        try {
            // create a simple container node
            String name = "testContainerNode";
            URL nodeURL = getNodeURL(nodesServiceURL, name);
            VOSURI nodeURI = getVOSURI(name);
            ContainerNode testNode = new ContainerNode(name);

            // cleanup
            delete(nodeURL, false);
            
            // PUT the node
            log.info("put: " + nodeURI + " -> " + nodeURL);
            put(nodeURL, nodeURI, testNode);

            // GET the new node
            NodeReader.NodeReaderResult result = get(nodeURL, 200, XML_CONTENT_TYPE);
            log.info("found: " + result.vosURI + " owner: " + result.node.ownerDisplay);
            Assert.assertTrue(result.node instanceof ContainerNode);
            ContainerNode persistedNode = (ContainerNode) result.node;
            Assert.assertEquals(testNode, persistedNode);
            Assert.assertEquals(nodeURI, result.vosURI);

            // POST an update to the node
            NodeProperty nodeProperty = new NodeProperty(VOS.PROPERTY_URI_LANGUAGE, "English");
            testNode.getProperties().add(nodeProperty);
            post(nodeURL, nodeURI, testNode);

            // GET the updated node
            result = get(nodeURL, 200, XML_CONTENT_TYPE);
            Assert.assertTrue(result.node instanceof ContainerNode);
            ContainerNode updatedNode = (ContainerNode) result.node;
            Assert.assertEquals(testNode, updatedNode);
            Assert.assertEquals(nodeURI, result.vosURI);
            Assert.assertTrue(updatedNode.getProperties().contains(nodeProperty));

            // DELETE the node
            delete(nodeURL);

            // GET the deleted node, which should fail
            get(nodeURL, 404, TEXT_CONTENT_TYPE);

        } catch (Exception e) {
            log.error("Unexpected error", e);
            Assert.fail("Unexpected error: " + e);
        }
    }

    @Test
    public void testDataNode() {
        try {
            // create a simple data node
            String name = "testDataNode";
            URL nodeURL = getNodeURL(nodesServiceURL, name);
            VOSURI nodeURI = getVOSURI(name);
            DataNode testNode = new DataNode(name);

            // cleanup
            delete(nodeURL, false);
            
            // PUT the node
            log.info("put: " + nodeURI + " -> " + nodeURL);
            put(nodeURL, nodeURI, testNode);

            // GET the new node
            NodeReader.NodeReaderResult result = get(nodeURL, 200, XML_CONTENT_TYPE);
            log.info("found: " + result.vosURI + " owner: " + result.node.ownerDisplay);
            Assert.assertTrue(result.node instanceof DataNode);
            DataNode persistedNode = (DataNode) result.node;
            Assert.assertEquals(testNode, persistedNode);
            Assert.assertEquals(nodeURI, result.vosURI);

            // POST an update to the node
            NodeProperty nodeProperty = new NodeProperty(VOS.PROPERTY_URI_LANGUAGE, "English");
            testNode.getProperties().add(nodeProperty);
            post(nodeURL, nodeURI, testNode);

            // GET the updated node
            result = get(nodeURL, 200, XML_CONTENT_TYPE);
            DataNode updatedNode = (DataNode) result.node;
            Assert.assertEquals(testNode, updatedNode);
            Assert.assertEquals(nodeURI, result.vosURI);
            Assert.assertEquals(testNode.getName(), updatedNode.getName());
            Assert.assertTrue(updatedNode.getProperties().contains(nodeProperty));

            // DELETE the node
            delete(nodeURL);

            // GET the deleted node, which should fail
            get(nodeURL, 404, TEXT_CONTENT_TYPE);

        } catch (Exception e) {
            log.error("Unexpected error", e);
            Assert.fail("Unexpected error: " + e);
        }
    }

    @Test
    public void testLinkNode() {
        try {
            // create a simple link node
            String name = "testLinkNode";
            URL nodeURL = getNodeURL(nodesServiceURL, name);
            VOSURI nodeURI = getVOSURI(name);
            LinkNode testNode = new LinkNode(name, URI.create("target"));

            // cleanup
            delete(nodeURL, false);
            
            // PUT the node
            log.info("put: " + nodeURI + " -> " + nodeURL);
            put(nodeURL, nodeURI, testNode);

            // GET the new node
            NodeReader.NodeReaderResult result = get(nodeURL, 200, XML_CONTENT_TYPE);
            log.info("found: " + result.vosURI + " owner: " + result.node.ownerDisplay);
            Assert.assertTrue(result.node instanceof LinkNode);
            LinkNode persistedNode = (LinkNode) result.node;
            Assert.assertEquals(testNode, persistedNode);
            Assert.assertEquals(nodeURI, result.vosURI);
            Assert.assertEquals(testNode.getTarget(), persistedNode.getTarget());
            Assert.assertEquals(testNode.getName(), persistedNode.getName());

            // POST an update to the node
            NodeProperty nodeProperty = new NodeProperty(VOS.PROPERTY_URI_LANGUAGE, "English");
            testNode.getProperties().add(nodeProperty);
            post(nodeURL, nodeURI, testNode);

            // GET the updated node
            result = get(nodeURL, 200, XML_CONTENT_TYPE);
            Assert.assertTrue(result.node instanceof LinkNode);
            LinkNode updatedNode = (LinkNode) result.node;
            Assert.assertEquals(testNode, updatedNode);
            Assert.assertEquals(nodeURI, result.vosURI);
            Assert.assertEquals(testNode.getTarget(), updatedNode.getTarget());
            Assert.assertEquals(testNode.getName(), updatedNode.getName());
            Assert.assertTrue(updatedNode.getProperties().contains(nodeProperty));

            // DELETE the node
            delete(nodeURL);

            // GET the deleted node, which should fail
            get(nodeURL, 404, TEXT_CONTENT_TYPE);

        } catch (Exception e) {
            log.error("Unexpected error", e);
            Assert.fail("Unexpected error: " + e);
        }
    }

    @Test
    public void testNodePropertyUpdates() {
        try {
            // create a fully populated container node
            String path = "nodes-complex-node";
            
            // TODO: with DataNode we could test updating content-type and content-encoding
            // TODO: with ContainerNode we can test updating inheritPermissions
            ContainerNode testNode = new ContainerNode(path);

            NodeProperty titleProperty = new NodeProperty(VOS.PROPERTY_URI_TITLE, "title");
            NodeProperty descriptionProperty = new NodeProperty(VOS.PROPERTY_URI_DESCRIPTION, "description");
            NodeProperty customProperty = new NodeProperty(URI.create("custom:secret-meaning"), "my secret info");
            testNode.getProperties().add(titleProperty);
            testNode.getProperties().add(descriptionProperty);
            testNode.getProperties().add(customProperty);

            GroupURI readGroup = new GroupURI(URI.create("ivo://org.opencadc/node?ReadGroup"));
            GroupURI writeGroup = new GroupURI(URI.create("ivo://org.opencadc/node?writeGroup"));
            testNode.getReadOnlyGroup().add(readGroup);
            testNode.getReadWriteGroup().add(writeGroup);
            testNode.isPublic = true;

            URL nodeURL = getNodeURL(nodesServiceURL, path);
            VOSURI nodeURI = getVOSURI(path);

            // cleanup
            delete(nodeURL, false);
            
            log.info("put: " + nodeURI + " -> " + nodeURL);
            put(nodeURL, nodeURI, testNode);

            // GET the new node
            NodeReader.NodeReaderResult result = get(nodeURL, 200, XML_CONTENT_TYPE);
            log.info("found: " + result.vosURI + " owner: " + result.node.ownerDisplay);
            ContainerNode persistedNode = (ContainerNode) result.node;
            Assert.assertEquals(testNode, persistedNode);
            Assert.assertEquals(nodeURI, result.vosURI);
            Assert.assertEquals(testNode.isPublic, persistedNode.isPublic);
            Assert.assertTrue(persistedNode.getProperties().contains(titleProperty));
            Assert.assertTrue(persistedNode.getProperties().contains(descriptionProperty));
            Assert.assertTrue(persistedNode.getProperties().contains(customProperty));
            Assert.assertEquals(testNode.getReadOnlyGroup().size(), persistedNode.getReadOnlyGroup().size());
            Assert.assertTrue(persistedNode.getReadOnlyGroup().contains(readGroup));
            Assert.assertEquals(testNode.getReadWriteGroup().size(), persistedNode.getReadWriteGroup().size());
            Assert.assertTrue(persistedNode.getReadWriteGroup().contains(writeGroup));

            // POST an update to the node
            NodeProperty titlePropertyDel = new NodeProperty(VOS.PROPERTY_URI_TITLE);
            NodeProperty descriptionProperty2 = new NodeProperty(VOS.PROPERTY_URI_DESCRIPTION, "modified description");
            testNode.getProperties().clear();
            testNode.getProperties().add(titlePropertyDel);
            testNode.getProperties().add(descriptionProperty2);

            GroupURI updatedReadGroup = new GroupURI(URI.create("ivo://org.opencadc/node?UpdatedReadGroup"));
            testNode.getReadOnlyGroup().clear();
            testNode.getReadOnlyGroup().add(updatedReadGroup);
            GroupURI updatedWriteGroup = new GroupURI(URI.create("ivo://org.opencadc/node?UpdatedWriteGroup"));
            testNode.getReadWriteGroup().clear();
            testNode.getReadWriteGroup().add(updatedWriteGroup);
            testNode.isPublic = false;

            post(nodeURL, nodeURI, testNode);

            // GET the updated node
            result = get(nodeURL, 200, XML_CONTENT_TYPE);
            Assert.assertTrue(result.node instanceof ContainerNode);
            ContainerNode updatedNode = (ContainerNode) result.node;
            Assert.assertEquals(testNode, updatedNode);
            Assert.assertEquals(nodeURI, result.vosURI);
            Assert.assertEquals(testNode.isPublic, updatedNode.isPublic);
            Assert.assertFalse(updatedNode.getProperties().contains(titleProperty));
            Assert.assertTrue(updatedNode.getProperties().contains(descriptionProperty2));
            Assert.assertTrue(persistedNode.getProperties().contains(customProperty));
            Assert.assertEquals(testNode.getReadOnlyGroup().size(), updatedNode.getReadOnlyGroup().size());
            Assert.assertTrue(updatedNode.getReadOnlyGroup().contains(updatedReadGroup));
            Assert.assertEquals(testNode.getReadWriteGroup().size(), updatedNode.getReadWriteGroup().size());
            Assert.assertTrue(updatedNode.getReadWriteGroup().contains(updatedWriteGroup));

            // DELETE the node
            delete(nodeURL);

            // GET the deleted node, which should fail
            get(nodeURL, 404, TEXT_CONTENT_TYPE);

        } catch (Exception e) {
            log.error("Unexpected error", e);
            Assert.fail("Unexpected error: " + e);
        }
    }

    @Test
    public void testListBatches() {
        String[] childNames = new String[] {
            "list-batches-child-1",
            "list-batches-child-2",
            "list-batches-child-3",
            "list-batches-child-4",
            "list-batches-child-5",
            "list-batches-child-6"
        };
        Node[] childNodes = new Node[childNames.length];
        
        try {
            // create root container node
            String parentName = "testListBatches";
            URL parentURL = getNodeURL(nodesServiceURL, parentName);
            VOSURI parentURI = getVOSURI(parentName);
            ContainerNode parent = new ContainerNode(parentName);
            
            // cleanup
            for (String n : childNames) {
                String child1Path = parentName + "/" + n;
                URL curl = getNodeURL(nodesServiceURL, child1Path);
                delete(curl, false);
            }
            delete(parentURL, false);
            
            log.info("put: " + parentURI + " -> " + parentURL);
            put(parentURL, parentURI, parent);

            // add 6 direct child nodes
            int i = 0;
            for (String n : childNames) {
                String childPath = parentName + "/" + n;
                URL childURL = getNodeURL(nodesServiceURL, childPath);
                VOSURI childURI = getVOSURI(childPath);
                ContainerNode child = new ContainerNode(n);
                childNodes[i++] = child;
                put(childURL, childURI, child);
            }
            
            // Get nodes 1 - 3
            VOSURI u1 = getVOSURI(parentName + "/" + childNames[0]);
            URL nodeURL = new URL(String.format("%s?uri=%s&limit=%d", parentURL,
                                                NetUtil.encode(u1.getURI().toASCIIString()), 3));
            NodeReader.NodeReaderResult result = get(nodeURL, 200, XML_CONTENT_TYPE);

            Assert.assertTrue(result.node instanceof ContainerNode);
            ContainerNode parentNode = (ContainerNode) result.node;
            Assert.assertEquals(3, parentNode.getNodes().size());
            Assert.assertTrue(parentNode.getNodes().contains(childNodes[0]));
            Assert.assertTrue(parentNode.getNodes().contains(childNodes[1]));
            Assert.assertTrue(parentNode.getNodes().contains(childNodes[2]));

            // Get nodes 3 - 5
            VOSURI u3 = getVOSURI(parentName + "/" + childNames[2]);
            nodeURL = new URL(String.format("%s?uri=%s&limit=%d", parentURL,
                                            NetUtil.encode(u3.getURI().toASCIIString()), 3));
            result = get(nodeURL, 200, XML_CONTENT_TYPE);

            Assert.assertTrue(result.node instanceof ContainerNode);
            parentNode = (ContainerNode) result.node;
            Assert.assertEquals(3, parentNode.getNodes().size());
            Assert.assertTrue(parentNode.getNodes().contains(childNodes[2]));
            Assert.assertTrue(parentNode.getNodes().contains(childNodes[3]));
            Assert.assertTrue(parentNode.getNodes().contains(childNodes[4]));

            // Get nodes 5 - 6
            VOSURI u5 = getVOSURI(parentName + "/" + childNames[4]);
            nodeURL = new URL(String.format("%s?uri=%s&limit=%d", parentURL,
                                            NetUtil.encode(u5.getURI().toASCIIString()), 3));
            result = get(nodeURL, 200, XML_CONTENT_TYPE);

            Assert.assertTrue(result.node instanceof ContainerNode);
            parentNode = (ContainerNode) result.node;
            Assert.assertEquals(2, parentNode.getNodes().size());
            Assert.assertTrue(parentNode.getNodes().contains(childNodes[4]));
            Assert.assertTrue(parentNode.getNodes().contains(childNodes[5]));

            // delete children
            for (String n : childNames) {
                String child1Path = parentName + "/" + n;
                URL curl = getNodeURL(nodesServiceURL, child1Path);
                delete(curl);
            }
            // delete the parent node
            delete(parentURL);

            // GET the deleted node, which should fail
            get(parentURL, 404, TEXT_CONTENT_TYPE);

        } catch (Exception e) {
            log.error("Unexpected error", e);
            Assert.fail("Unexpected error: " + e);
        }
    }

    @Test
    public void testLimit() {
        String[] childNames = new String[] {
            "limit-nodes-child-1",
            "limit-nodes-child-2",
            "limit-nodes-child-3",
            "limit-nodes-child-4",
            "limit-nodes-child-5",
            "limit-nodes-child-6"
        };
        Node[] childNodes = new Node[childNames.length];
        try {
            // create root container node
            String parentName = "testLimit";
            URL parentURL = getNodeURL(nodesServiceURL, parentName);
            VOSURI parentURI = getVOSURI(parentName);
            ContainerNode parent = new ContainerNode(parentName);
            
            // cleanup
            for (String n : childNames) {
                String child1Path = parentName + "/" + n;
                URL curl = getNodeURL(nodesServiceURL, child1Path);
                delete(curl, false);
            }
            delete(parentURL, false);
            
            log.info("put: " + parentURI + " -> " + parentURL);
            put(parentURL, parentURI, parent);

            // add 3 direct child nodes
            int i = 0;
            for (String n : childNames) {
                String child1Name = n;
                String child1Path = parentName + "/" + child1Name;
                URL child1URL = getNodeURL(nodesServiceURL, child1Path);
                VOSURI child1URI = getVOSURI(child1Path);
                ContainerNode child1 = new ContainerNode(child1Name);
                childNodes[i++] = child1;
                put(child1URL, child1URI, child1);
            }

            // get the node with a limit of 2 child nodes
            URL nodeURL = new URL(String.format("%s?limit=%d", parentURL, 2));
            NodeReader.NodeReaderResult result = get(nodeURL, 200, XML_CONTENT_TYPE);

            Assert.assertTrue(result.node instanceof ContainerNode);
            ContainerNode parentNode = (ContainerNode) result.node;
            Assert.assertEquals(2, parentNode.getNodes().size());
            Assert.assertTrue(parentNode.getNodes().contains(childNodes[0]));
            Assert.assertTrue(parentNode.getNodes().contains(childNodes[1]));

            // delete children
            for (String n : childNames) {
                String child1Path = parentName + "/" + n;
                URL curl = getNodeURL(nodesServiceURL, child1Path);
                delete(curl);
            }
            // delete the parent node
            delete(parentURL);

            // GET the deleted node, which should fail
            get(parentURL, 404, TEXT_CONTENT_TYPE);

        } catch (Exception e) {
            log.error("Unexpected error", e);
            Assert.fail("Unexpected error: " + e);
        }
    }

    @Test
    public void testDetail() {
        try {
            // create a fully populated container node
            String parentName = "testDetail";
            ContainerNode testNode = new ContainerNode(parentName);
            testNode.owner = authSubject;
            testNode.isPublic = true;

            NodeProperty titleProperty = new NodeProperty(VOS.PROPERTY_URI_TITLE, "title");
            NodeProperty descriptionProperty = new NodeProperty(VOS.PROPERTY_URI_DESCRIPTION, "description");
            testNode.getProperties().add(titleProperty);
            testNode.getProperties().add(descriptionProperty);

            GroupURI readGroup = new GroupURI(URI.create("ivo://org.opencadc/node?ReadGroup"));
            GroupURI writeGroup = new GroupURI(URI.create("ivo://org.opencadc/node?writeGroup"));
            testNode.getReadOnlyGroup().add(readGroup);
            testNode.getReadWriteGroup().add(writeGroup);

            URL nodeURL = getNodeURL(nodesServiceURL, parentName);
            VOSURI nodeURI = getVOSURI(parentName);
            
            // add a Child node
            String child1Name = "node-detail-child-node";
            String child1Path = parentName + "/" + child1Name;
            final URL child1URL = getNodeURL(nodesServiceURL, child1Path);
            final VOSURI child1URI = getVOSURI(child1Path);
            
            // cleanup
            delete(child1URL, false);
            delete(nodeURL, false);
            
            log.info("put: " + nodeURI + " -> " + nodeURL);
            put(nodeURL, nodeURI, testNode);
            
            ContainerNode child1 = new ContainerNode(child1Name);
            put(child1URL, child1URI, child1);

            // get the node with a min detail
            URL detailURL = new URL(nodeURL + "?detail=min");
            NodeReader.NodeReaderResult result = get(detailURL, 200, XML_CONTENT_TYPE);
            Assert.assertTrue(result.node instanceof ContainerNode);
            ContainerNode parentNode = (ContainerNode) result.node;
            Assert.assertTrue(parentNode.getProperties().isEmpty());
            Assert.assertFalse(parentNode.getNodes().isEmpty());

            // get the node
            result = get(nodeURL, 200, XML_CONTENT_TYPE);
            Assert.assertTrue(result.node instanceof ContainerNode);
            parentNode = (ContainerNode) result.node;
            Assert.assertFalse(parentNode.getProperties().isEmpty());
            Assert.assertFalse(parentNode.getNodes().isEmpty());

            // delete the node
            delete(child1URL);
            delete(nodeURL);

            // GET the deleted node, which should fail
            get(nodeURL, 404, TEXT_CONTENT_TYPE);

        } catch (Exception e) {
            log.error("Unexpected error", e);
            Assert.fail("Unexpected error: " + e);
        }
    }

    //@Test
    public void dataViewTest() {
        try {
            // upload test file
            String name = "view-data-node";
            URL nodeURL = getNodeURL(nodesServiceURL, name);
            VOSURI nodeURI = getVOSURI(name);
            DataNode node = new DataNode(name);
            put(nodeURL, nodeURI, node);

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
                } else if ("protocol".equalsIgnoreCase(key)) {
                    Assert.assertEquals(VOS.PROTOCOL_HTTPS_GET.toASCIIString(), val);
                } else if ("direction".equalsIgnoreCase(key)) {
                    Assert.assertEquals(Direction.pullFromVoSpace.getValue(), val);
                } else {
                    Assert.fail(String.format("unexpected transfer parameter: %s = %s", key, val));
                }
            }

            // delete the file
            delete(nodeURL);

            // GET the deleted node, which should fail
            get(nodeURL, 404, TEXT_CONTENT_TYPE);

        } catch (Exception e) {
            log.error("Unexpected error", e);
            Assert.fail("Unexpected error: " + e);
        }
    }

}
