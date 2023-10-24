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
import ca.nrc.cadc.auth.SSLUtil;
import ca.nrc.cadc.net.HttpDelete;
import ca.nrc.cadc.net.HttpGet;
import ca.nrc.cadc.net.HttpUpload;
import ca.nrc.cadc.net.NetUtil;
import ca.nrc.cadc.uws.ExecutionPhase;
import ca.nrc.cadc.uws.Job;
import ca.nrc.cadc.uws.Result;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.InputStream;
import java.net.MalformedURLException;
import java.net.URI;
import java.net.URL;
import java.security.PrivilegedExceptionAction;
import javax.security.auth.Subject;
import org.apache.log4j.Logger;
import org.junit.Assert;
import org.junit.Assume;
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

    // permissions tests
    private GroupURI accessGroup;
    private Subject groupMember;
    
    // round trip tests
    private GroupURI group1;
    private GroupURI group2;
    
    protected boolean linkNodeProps = true;
    protected boolean paginationSupported = true;
    protected boolean nodelockSupported = true;
    
    protected boolean cleanupOnSuccess = true;
    
    protected NodesTest(URI resourceID, File testCert) {
        super(resourceID, testCert);
    }

    /**
     * Enable testing that group props can be set and returned for nodes.
     * 
     * @param group1 a valid group URI
     * @param group2 a different valid group URI
     */
    protected void enablePermissionPropsTest(GroupURI group1, GroupURI group2) {
        this.group1 = group1;
        this.group2 = group2;
    }
    
    protected void enablePermissionTests(GroupURI accessGroup, File groupMemberCert) {
        this.accessGroup = accessGroup;
        this.groupMember = SSLUtil.createSubject(groupMemberCert);
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
            
            // not found
            get(nodeURL, 404, TEXT_CONTENT_TYPE);
            
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
            if (nodelockSupported) {
                testNode.isLocked = true; // lock node
            }
            post(nodeURL, nodeURI, testNode);

            // GET the updated node
            result = get(nodeURL, 200, XML_CONTENT_TYPE);
            Assert.assertTrue(result.node instanceof ContainerNode);
            ContainerNode updatedNode = (ContainerNode) result.node;
            Assert.assertEquals(testNode, updatedNode);
            Assert.assertEquals(nodeURI, result.vosURI);
            Assert.assertTrue(updatedNode.getProperties().contains(nodeProperty));
            if (nodelockSupported) {
                Assert.assertTrue(updatedNode.isLocked);
            }

            // failed to add a subdirectory (node locked)
            if (nodelockSupported) {
                String subDirName = name + "/subDir";
                URL subDirURL = getNodeURL(nodesServiceURL, subDirName);
                VOSURI subDirURI = getVOSURI(subDirName);
                ContainerNode subDirNode = new ContainerNode(subDirName);
                log.info("put: " + subDirURI + " -> " + subDirURL);
                try {
                    put(subDirURL, subDirURI, subDirNode);
                    Assert.fail("New node should fail when parent is locked");
                } catch (AssertionError ex) {
                    Assert.assertEquals("expected PUT response code = 200 expected:<200> but was:<403>",
                            ex.getMessage());
                }
            }

            if (cleanupOnSuccess) {
                delete(nodeURL, true);
            }

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
            
            // not found
            get(nodeURL, 404, TEXT_CONTENT_TYPE);
            
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

            if (cleanupOnSuccess) {
                delete(nodeURL);
            }

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
            VOSURI targetURI = getVOSURI("target");
            log.info("link node: " + nodeURI + " -> " + targetURI);
            
            // cleanup
            delete(nodeURL, false);
            
            // not found
            get(nodeURL, 404, TEXT_CONTENT_TYPE);
            
            // PUT the node
            log.info("put: " + nodeURI + " -> " + nodeURL);
            LinkNode testNode = new LinkNode(name, targetURI.getURI());
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

            if (linkNodeProps) {
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
            }
            
            if (cleanupOnSuccess) {
                delete(nodeURL);
            }

        } catch (Exception e) {
            log.error("Unexpected error", e);
            Assert.fail("Unexpected error: " + e);
        }
    }

    @Test
    public void testNodePropertyUpdates() {
        try {
            // create a fully populated container node
            String path = "testNodePropertyUpdates";

            // TODO: with DataNode we could test updating content-type and content-encoding
            ContainerNode testNode = new ContainerNode(path);

            NodeProperty titleProperty = new NodeProperty(VOS.PROPERTY_URI_TITLE, "title");
            NodeProperty descriptionProperty = new NodeProperty(VOS.PROPERTY_URI_DESCRIPTION, "description");
            NodeProperty customProperty = new NodeProperty(URI.create("custom:secret-meaning"), "my secret info");
            testNode.getProperties().add(titleProperty);
            testNode.getProperties().add(descriptionProperty);
            testNode.getProperties().add(customProperty);

            if (group1 != null) {
                testNode.getReadOnlyGroup().add(group1);
            }
            if (group2 != null) {
                testNode.getReadWriteGroup().add(group2);
            }
            testNode.isPublic = true;
            testNode.inheritPermissions = true;

            URL nodeURL = getNodeURL(nodesServiceURL, path);
            VOSURI nodeURI = getVOSURI(path);

            // cleanup
            delete(nodeURL, false);
            
            // not found
            get(nodeURL, 404, TEXT_CONTENT_TYPE);

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
            if (group1 != null) {
                Assert.assertTrue(persistedNode.getReadOnlyGroup().contains(group1));
            }
            Assert.assertEquals(testNode.getReadWriteGroup().size(), persistedNode.getReadWriteGroup().size());
            if (group2 != null) {
                Assert.assertTrue(persistedNode.getReadWriteGroup().contains(group2));
            }
            Assert.assertNotNull(persistedNode.inheritPermissions);
            Assert.assertTrue(persistedNode.inheritPermissions);

            // POST an update to the node
            NodeProperty titlePropertyDel = new NodeProperty(VOS.PROPERTY_URI_TITLE);
            NodeProperty descriptionProperty2 = new NodeProperty(VOS.PROPERTY_URI_DESCRIPTION, "modified description");
            testNode.getProperties().clear();
            testNode.getProperties().add(titlePropertyDel);
            testNode.getProperties().add(descriptionProperty2);

            testNode.getReadOnlyGroup().clear();
            if (group2 != null) {
                testNode.getReadOnlyGroup().add(group2);
            }
            testNode.getReadWriteGroup().clear();
            if (group1 != null) {
                testNode.getReadWriteGroup().add(group1);
            }
            testNode.isPublic = false;
            testNode.inheritPermissions = false;

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
            if (group2 != null) {
                Assert.assertTrue(updatedNode.getReadOnlyGroup().contains(group2));
            }
            Assert.assertEquals(testNode.getReadWriteGroup().size(), updatedNode.getReadWriteGroup().size());
            if (group1 != null) {
                Assert.assertTrue(updatedNode.getReadWriteGroup().contains(group1));
            }
            Assert.assertFalse(updatedNode.inheritPermissions);

            // TODO: POST to clear some props
            
            if (cleanupOnSuccess) {
                delete(nodeURL);
            }

        } catch (Exception e) {
            log.error("Unexpected error", e);
            Assert.fail("Unexpected error: " + e);
        }
    }

    @Test
    public void testInheritPermissions() throws Exception {
        
        Assume.assumeTrue("enablePermissionPropsTest not called", group1 != null);
        
        // create a fully populated container node
        String path = "testInheritPermissions";

        ContainerNode testNode = new ContainerNode(path);

        testNode.getReadOnlyGroup().add(group1);
        testNode.getReadWriteGroup().add(group2);
        testNode.isPublic = false;
        testNode.inheritPermissions = false;

        String subDir = path + "/subdir";
        ContainerNode subDirNode = new ContainerNode(subDir);
        
        String data = path + "/dataNode";
        DataNode dataNode = new DataNode("dataNode");
        
        URL nodeURL = getNodeURL(nodesServiceURL, path);
        URL subDirURL = getNodeURL(nodesServiceURL, subDir);
        URL dataNodeURL = getNodeURL(nodesServiceURL, data);
        
        // cleanup
        delete(dataNodeURL, false);
        delete(subDirURL, false);
        delete(nodeURL, false);

        VOSURI nodeURI = getVOSURI(path);
        VOSURI subDirURI = getVOSURI(subDir);
        VOSURI dataNodeURI = getVOSURI(data);
        log.info("put: " + nodeURI + " -> " + nodeURL);
        put(nodeURL, nodeURI, testNode);
        log.info("put: " + subDirURI + " -> " + subDirURL);
        put(subDirURL, subDirURI, subDirNode);
        log.info("put: " + dataNodeURI + " -> " + dataNodeURL);
        put(dataNodeURL, dataNodeURI, dataNode);

        // GET the new subDir node
        NodeReader.NodeReaderResult result = get(subDirURL, 200, XML_CONTENT_TYPE);
        ContainerNode persistedNode = (ContainerNode) result.node;
        Assert.assertFalse(persistedNode.isPublic);  // null is saved as false
        Assert.assertTrue(persistedNode.getReadOnlyGroup().isEmpty());
        Assert.assertTrue(persistedNode.getReadWriteGroup().isEmpty());
        
        result = get(dataNodeURL, 200, XML_CONTENT_TYPE);
        DataNode dn = (DataNode) result.node;
        Assert.assertFalse(dn.isPublic);  // null is saved as false
        Assert.assertTrue(dn.getReadOnlyGroup().isEmpty());
        Assert.assertTrue(dn.getReadWriteGroup().isEmpty());

        // update the parent node to inherit permissions
        testNode.inheritPermissions = true;
        post(nodeURL, nodeURI, testNode);
        
        // specify permissions on the children to override inherit
        delete(subDirURL);
        subDirNode.isPublic = true;
        // reverse of the parent props should suffice
        subDirNode.getReadOnlyGroup().add(group2);
        subDirNode.getReadWriteGroup().add(group1);
        
        delete(dataNodeURL);
        dataNode.isPublic = true;
        dataNode.getReadOnlyGroup().add(group2);
        dataNode.getReadWriteGroup().add(group1);

        log.info("put: " + subDirURI + " -> " + subDirURL);
        put(subDirURL, subDirURI, subDirNode);
        
        log.info("put: " + dataNodeURI + " -> " + dataNodeURL);
        put(dataNodeURL, dataNodeURI, dataNode);

        // GET the new subDir node
        result = get(subDirURL, 200, XML_CONTENT_TYPE);
        persistedNode = (ContainerNode) result.node;
        Assert.assertTrue(persistedNode.isPublic);
        Assert.assertEquals(1, persistedNode.getReadOnlyGroup().size());
        Assert.assertTrue(persistedNode.getReadOnlyGroup().contains(group2));
        Assert.assertEquals(1, persistedNode.getReadWriteGroup().size());
        Assert.assertTrue(persistedNode.getReadWriteGroup().contains(group1));

        result = get(dataNodeURL, 200, XML_CONTENT_TYPE);
        dn = (DataNode) result.node;
        Assert.assertTrue(dn.isPublic); 
        Assert.assertEquals(1, dn.getReadOnlyGroup().size());
        Assert.assertTrue(dn.getReadOnlyGroup().contains(group2));
        Assert.assertEquals(1, dn.getReadWriteGroup().size());
        Assert.assertTrue(dn.getReadWriteGroup().contains(group1));
        
        // redo test with inherit active
        delete(dataNodeURL);
        delete(subDirURL);
        
        subDirNode.isPublic = null;
        subDirNode.getReadOnlyGroup().clear();
        subDirNode.getReadWriteGroup().clear();
        dataNode.isPublic = null;
        dataNode.getReadOnlyGroup().clear();
        dataNode.getReadWriteGroup().clear();

        log.info("put: " + subDirURI + " -> " + subDirURL);
        put(subDirURL, subDirURI, subDirNode);

        log.info("put: " + dataNodeURI + " -> " + dataNodeURL);
        put(dataNodeURL, dataNodeURI, dataNode);
        
        // GET the new subDir node
        result = get(subDirURL, 200, XML_CONTENT_TYPE);
        persistedNode = (ContainerNode) result.node;
        Assert.assertFalse(persistedNode.isPublic);
        Assert.assertEquals(testNode.getReadOnlyGroup().size(), persistedNode.getReadOnlyGroup().size());
        Assert.assertTrue(persistedNode.getReadOnlyGroup().contains(group1));
        Assert.assertEquals(testNode.getReadWriteGroup().size(), persistedNode.getReadWriteGroup().size());
        Assert.assertTrue(persistedNode.getReadWriteGroup().contains(group2));
        
        result = get(dataNodeURL, 200, XML_CONTENT_TYPE);
        dn = (DataNode) result.node;
        Assert.assertFalse(dn.isPublic);  // null is saved as false
        Assert.assertEquals(testNode.getReadOnlyGroup().size(), dn.getReadOnlyGroup().size());
        Assert.assertTrue(dn.getReadOnlyGroup().contains(group1));
        Assert.assertEquals(testNode.getReadWriteGroup().size(), dn.getReadWriteGroup().size());
        Assert.assertTrue(dn.getReadWriteGroup().contains(group2));

        if (cleanupOnSuccess) {
            delete(dataNodeURL);
            delete(subDirURL);
            delete(nodeURL);
        }
    }

    @Test
    public void testListBatches() {
        if (!paginationSupported) {
            // testLimit tests the minimal required behaviour
            return; // done
        }
        
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

            // cleanup
            for (String n : childNames) {
                String child1Path = parentName + "/" + n;
                URL curl = getNodeURL(nodesServiceURL, child1Path);
                delete(curl, false);
            }
            delete(parentURL, false);

            ContainerNode parent = new ContainerNode(parentName);
            log.info("put: " + parentURI + " -> " + parentURL);
            parent = new ContainerNode(parentName);
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

            if (cleanupOnSuccess) {
                for (String n : childNames) {
                    String child1Path = parentName + "/" + n;
                    URL curl = getNodeURL(nodesServiceURL, child1Path);
                    delete(curl);
                }
                // delete the parent node
                delete(parentURL);
            }

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

            // cleanup
            for (String n : childNames) {
                String child1Path = parentName + "/" + n;
                URL curl = getNodeURL(nodesServiceURL, child1Path);
                delete(curl, false);
            }
            delete(parentURL, false);

            ContainerNode parent = new ContainerNode(parentName);
            log.info("put: " + parentURI + " -> " + parentURL);
            parent = new ContainerNode(parentName);
            put(parentURL, parentURI, parent);
            
            // check that limit=0 is supported
            final URL limitZeroURL = new URL(parentURL + "?limit=0");
            final HttpGet limitZeroGet = new HttpGet(limitZeroURL, true);
            Subject.doAs(authSubject, (PrivilegedExceptionAction<Object>) () -> {
                limitZeroGet.prepare();
                return null;
            });
            Assert.assertEquals(200, limitZeroGet.getResponseCode());
            
            if (!paginationSupported) {
                // check that a pagination request is rejected in the spec-compliant way
                final URL limitURL = new URL(parentURL + "?limit=2");
                final HttpGet limitGet = new HttpGet(limitURL, true);
                try {
                    Subject.doAs(authSubject, (PrivilegedExceptionAction<Object>) () -> {
                        limitGet.prepare();
                        return null;
                    });
                    Assert.fail("expected: " + VOS.IVOA_FAULT_OPTION_NOT_SUPPORTED + " but got: " + limitGet.getResponseCode());
                } catch (IllegalArgumentException ex) {
                    if (ex.getMessage().startsWith(VOS.IVOA_FAULT_OPTION_NOT_SUPPORTED)) {
                        log.info("caught valid reject: " + ex.getMessage());
                    } else {
                        throw ex;
                    }
                }
                
                // delete the parent node
                delete(parentURL);
                return; // done
            }
            
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

            if (cleanupOnSuccess) {
                for (String n : childNames) {
                    String child1Path = parentName + "/" + n;
                    URL curl = getNodeURL(nodesServiceURL, child1Path);
                    delete(curl);
                }
                // delete the parent node
                delete(parentURL);
            }

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

            if (group1 != null) {
                testNode.getReadOnlyGroup().add(group1);
            }
            if (group2 != null) {
                testNode.getReadWriteGroup().add(group2);
            }

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

            if (cleanupOnSuccess) {
                delete(child1URL);
                delete(nodeURL);
            }

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

            if (cleanupOnSuccess) {
                delete(nodeURL);
            }

        } catch (Exception e) {
            log.error("Unexpected error", e);
            Assert.fail("Unexpected error: " + e);
        }
    }

    @Test
    public void testRecursiveDelete() throws Exception {
        // create a tree structure
        String baseDir = "testRecursiveDelete/";
        String subDir = baseDir + "subdir/";
        String[] testTree = {baseDir, baseDir + "file1", subDir, subDir + "file2"};

        // clear possible lock left from a previous test
        URL subdirURL = getNodeURL(nodesServiceURL, subDir);
        NodeReader.NodeReaderResult result = get(subdirURL, 200, XML_CONTENT_TYPE, false);
        if (result != null) {
            if ((result.node.isLocked != null) && result.node.isLocked) {
                result.node.isLocked = false;
                post(subdirURL, getVOSURI(subDir), result.node);
            }
        }

        createNodeTree(testTree);

        Job job = postRecursiveDelete(recursiveDeleteServiceURL, getVOSURI(baseDir));
        Assert.assertEquals("Expected completed job", ExecutionPhase.COMPLETED, job.getExecutionPhase());
        Assert.assertEquals(1, job.getResultsList().size());
        Result res = job.getResultsList().get(0);
        Assert.assertEquals("delcount", res.getName());
        Assert.assertEquals(4, Integer.parseInt(res.getURI().getSchemeSpecificPart()));
        // cleanup
        cleanupNodeTree(testTree);

        if (!nodelockSupported) {
            return;
        }
        
        // repeat test but lock the subdirectory
        createNodeTree(testTree);

        result = get(subdirURL, 200, XML_CONTENT_TYPE, true);
        log.info("found: " + result.vosURI);
        result.node.isLocked = true;
        post(subdirURL, getVOSURI(subDir), result.node);

        // an error should result from try to delete a file in that directory
        job = postRecursiveDelete(recursiveDeleteServiceURL, getVOSURI(subdirURL + "file2"));
        Assert.assertEquals("Expected error job", ExecutionPhase.ERROR, job.getExecutionPhase());

        // now try to delete the root node which results in a partial delete (aborted job)
        job = postRecursiveDelete(recursiveDeleteServiceURL, getVOSURI(baseDir));
        Assert.assertEquals("Expected aborted job", ExecutionPhase.ABORTED, job.getExecutionPhase());
        Assert.assertEquals(2, job.getResultsList().size());
        for (Result jobResult : job.getResultsList()) {
            if ("errorcount".equalsIgnoreCase(jobResult.getName())) {
                Assert.assertEquals(1, Integer.parseInt(jobResult.getURI().getSchemeSpecificPart()));
            } else if ("delcount".equalsIgnoreCase(jobResult.getName())) {
                Assert.assertEquals(1, Integer.parseInt(jobResult.getURI().getSchemeSpecificPart()));
            } else {
                Assert.fail("Unexpected result " + jobResult.getName());
            }
        }

        // unlock to be able to cleanup
        result.node.isLocked = false;
        post(subdirURL, getVOSURI(subDir), result.node);

        // cleanup
        cleanupNodeTree(testTree);

    }

    private void createNodeTree(String[] nodes) throws Exception {
        // cleanup first
        cleanupNodeTree(nodes);

        // build the tree
        for (String nodeName : nodes) {
            URL nodeURL = getNodeURL(nodesServiceURL, nodeName);
            VOSURI nodeURI = getVOSURI(nodeName);
            Node node = null;
            if (nodeName.endsWith("/")) {
                node = new ContainerNode(nodeName);
            } else {
                node = new DataNode(nodeName);
            }
            log.info("put: " + nodeURI + " -> " + nodeURL);
            put(nodeURL, nodeURI, node);
        }
    }

    private void cleanupNodeTree(String[] nodes) throws MalformedURLException {
        for (int i = nodes.length - 1; i >= 0; i--) {
            URL nodeURL = getNodeURL(nodesServiceURL, nodes[i]);
            log.debug("deleting node " + nodeURL);
            delete(nodeURL, false);
        }
    }

    @Test
    public void testPermissions() throws Exception {

        Assume.assumeTrue("enablePermissionTest not called", accessGroup != null);
        
        // create a directory
        String parentName = "testPermissions";
        ContainerNode testNode = new ContainerNode(parentName);
        testNode.owner = authSubject;
        testNode.isPublic = false;

        final URL nodeURL = getNodeURL(nodesServiceURL, parentName);
        final VOSURI nodeURI = getVOSURI(parentName);

        String childName = "testGroupUser";
        ContainerNode childNode = new ContainerNode(childName);
        childNode.parent = testNode;
        String childPath = parentName + "/" + childName;
        final VOSURI childURI = getVOSURI(childPath);
        final URL childURL = getNodeURL(nodesServiceURL, childPath);
        
        // cleanup
        delete(childURL, false);
        delete(nodeURL, false);

        // PUT the node
        log.info("putAction: " + nodeURI + " -> " + nodeURL);
        put(nodeURL, nodeURI, testNode);

        // try to access it as a different user (memberUser) - it should fail
        HttpGet getAction = new HttpGet(nodeURL, true);
        Subject.doAs(groupMember, new RunnableAction(getAction));
        Assert.assertEquals(403, getAction.getResponseCode());

        // give groupMember read access through the group
        getAction = new HttpGet(nodeURL, true);
        testNode.getReadOnlyGroup().add(accessGroup);
        post(nodeURL, nodeURI, testNode);
        Subject.doAs(groupMember, new RunnableAction(getAction));
        Assert.assertEquals("expected GET response code = 200", 200, getAction.getResponseCode());
        Assert.assertNull("expected GET throwable == null", getAction.getThrowable());

        // permission denied to write in the container without write permission
        InputStream is = prepareInput(childURI, childNode);
        HttpUpload putAction = new HttpUpload(is, childURL);
        putAction.setRequestProperty("Content-Type", XML_CONTENT_TYPE);
        log.debug("PUT rejected " + childURL);
        Subject.doAs(groupMember, new RunnableAction(putAction));
        log.debug("PUT responseCode: " + putAction.getResponseCode());
        Assert.assertEquals("expected PUT response code = 403",
                403, putAction.getResponseCode());

        // same test after permission granted

        testNode.getReadWriteGroup().add(accessGroup);
        log.debug("Node update " + testNode.getReadWriteGroup());
        post(nodeURL, nodeURI, testNode);
        log.debug("PUT succeed " + childURL);
        is.reset();
        putAction = new HttpUpload(is, childURL);
        Subject.doAs(groupMember, new RunnableAction(putAction));
        log.debug("PUT responseCode: " + putAction.getResponseCode());
        Assert.assertEquals("expected PUT response code = 200",
                200, putAction.getResponseCode());
        Assert.assertNull("expected PUT throwable == null", putAction.getThrowable());

        log.debug("Delete node " + childURL);
        HttpDelete deleteAction = new HttpDelete(childURL, true);
        Subject.doAs(groupMember, new RunnableAction(deleteAction));
        log.debug("DELETE responseCode: " + deleteAction.getResponseCode());
        Assert.assertEquals("expected PUT response code = 200",
                200, deleteAction.getResponseCode());
        Assert.assertNull("expected PUT throwable == null", deleteAction.getThrowable());

        if (cleanupOnSuccess) {
            delete(nodeURL);
        }
    }

}
