/*
************************************************************************
*******************  CANADIAN ASTRONOMY DATA CENTRE  *******************
**************  CENTRE CANADIEN DE DONNÉES ASTRONOMIQUES  **************
*
*  (c) 2009.                            (c) 2009.
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
*  $Revision: 4 $
*
************************************************************************
*/

package ca.nrc.cadc.vos.server;

import java.net.URI;
import java.net.URL;
import java.security.Principal;
import java.security.PrivilegedExceptionAction;
import java.text.DateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Random;

import javax.security.auth.Subject;
import javax.security.auth.x500.X500Principal;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.jdom2.Document;
import org.jdom2.Element;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import ca.nrc.cadc.date.DateUtil;
import ca.nrc.cadc.db.ConnectionConfig;
import ca.nrc.cadc.db.DBConfig;
import ca.nrc.cadc.db.DBUtil;
import ca.nrc.cadc.net.TransientException;
import ca.nrc.cadc.util.FileMetadata;
import ca.nrc.cadc.util.Log4jInit;
import ca.nrc.cadc.xml.XmlUtil;
import ca.nrc.cadc.vos.ContainerNode;
import ca.nrc.cadc.vos.DataNode;
import ca.nrc.cadc.vos.LinkNode;
import ca.nrc.cadc.vos.Node;
import ca.nrc.cadc.vos.NodeNotFoundException;
import ca.nrc.cadc.vos.NodeNotSupportedException;
import ca.nrc.cadc.vos.NodeProperty;
import ca.nrc.cadc.vos.StructuredDataNode;
import ca.nrc.cadc.vos.VOS;
import ca.nrc.cadc.vos.VOSURI;
import ca.nrc.cadc.vos.server.auth.VOSpaceAuthorizer;

/**
 *
 * @author pdowler
 */
public class RssViewTest
{
    private static final Logger log = Logger.getLogger(RssViewTest.class);
    static
    {
        Log4jInit.setLevel("ca.nrc.cadc.vos.server", Level.INFO);
    }

    private static DateFormat dateFormat = DateUtil.getDateFormat(DateUtil.IVOA_DATE_FORMAT, DateUtil.UTC);

    static final String SERVER = "VOSPACE_WS_TEST";
    static final String DATABASE = "cadctest";
    static final String SCHEMA = "";
    static final String DELETED_NODES = "/DeletedNodes";

    static final String VOS_AUTHORITY = "cadc.nrc.ca!vospace";
    static final String ROOT_CONTAINER = "CADCRsstest1";
    static final String REGTEST_NODE_OWNER = "CN=CADC Regtest1 10577,OU=CADC,O=HIA";
    static final String AUTHTEST_NODE_OWNER = "CN=CADC Authtest1 10627,OU=CADC,O=HIA";

    protected Subject owner;
    protected Principal principal;
    protected TestNodePersistence nodePersistence;
    protected VOSpaceAuthorizer voSpaceAuthorizer;
    
    public RssViewTest()
    {
        nodePersistence = new TestNodePersistence();
        voSpaceAuthorizer = new VOSpaceAuthorizer();
    }

    @BeforeClass
    public static void setUpClass() throws Exception
    {

    }

    @Test
    public void testGetBaseURL()
    {
        log.debug("testGetBaseURL - START");
        try
        {
            RssView v = new RssView();

            String expected = "http://example.com/vospace/nodes";
            URL req = new URL(expected + "/foo/bar");
            VOSURI vos = new VOSURI(new URI("vos://example.com~vospace/foo/bar"));
            ContainerNode n = new ContainerNode(vos);
            String actual = v.getBaseURL(n, req);
            log.debug("request: " + req);
            Assert.assertEquals(expected, actual);

            log.info("testGetBaseURL passed");
        }
        catch(Exception unexpected)
        {
            log.error("unexpected exception", unexpected);
            Assert.fail("unexpected exception: " + unexpected);
        }
        finally
        {
            log.debug("testGetBaseURL - DONE");
        }
    }

    @Test
    public void testSetNodeReturnsDenied()
    {
        log.debug("testSetNodeReturnsDenied - START");
        try
        {
            Subject subject = new Subject();
            subject.getPrincipals().add(new X500Principal(REGTEST_NODE_OWNER));

            // Get the root container node for the test.
            GetRootNodeAction getRootNodeAction = new GetRootNodeAction(nodePersistence);
            ContainerNode root = (ContainerNode) Subject.doAs(subject, getRootNodeAction);
            log.debug("root node: " + root);

            // Get the private child container node for the test.
            GetChildNodeAction getChildNodeAction = new GetChildNodeAction(nodePersistence);
            ContainerNode child = (ContainerNode) Subject.doAs(subject, getChildNodeAction);
            log.debug("child node: " + child);

            // Get the RSS XML from the view.
            Subject deniedSubject = new Subject();
            deniedSubject.getPrincipals().add(new X500Principal(AUTHTEST_NODE_OWNER));

            SetNodeAction setNodeAction = new SetNodeAction(nodePersistence, voSpaceAuthorizer, child, getChildNodeAction.getPath());
            StringBuilder rssXml = (StringBuilder) Subject.doAs(deniedSubject, setNodeAction);

            Document doc = XmlUtil.buildDocument(rssXml.toString());
            Element rss = doc.getRootElement();
            Assert.assertNotNull(rss);

            Element channel = rss.getChild("channel");
            Assert.assertNotNull(channel);

            Element description = channel.getChild("description");
            Assert.assertNotNull(description);
            Assert.assertTrue(description.getText().startsWith("Read permission denied"));

            List<Element> items = channel.getChildren("item");
            Assert.assertEquals(0, items.size());

            log.info("testSetNodeReturnsDenied passed");
        }
        catch(Exception unexpected)
        {
            log.error("unexpected exception", unexpected);
            Assert.fail("unexpected exception: " + unexpected);
        }
        finally
        {
            log.debug("testSetNodeReturnsDenied - DONE");
        }
    }

    @Test
    public void testSetNode()
    {
        try
        {
            Subject subject = new Subject();
            subject.getPrincipals().add(new X500Principal(REGTEST_NODE_OWNER));

            // Get the root container node for the test.
            GetRootNodeAction getRootNodeAction = new GetRootNodeAction(nodePersistence);
            ContainerNode root = (ContainerNode) Subject.doAs(subject, getRootNodeAction);
            log.debug("root node: " + root);

            // Get the RSS XML from the view.
            String path = "http://" + VOS_AUTHORITY + "/" + ROOT_CONTAINER;
            SetNodeAction setNodeAction = new SetNodeAction(nodePersistence, voSpaceAuthorizer, root, path);
            StringBuilder rssXml = (StringBuilder) Subject.doAs(subject, setNodeAction);

            Document doc = XmlUtil.buildDocument(rssXml.toString());
            Element rss = doc.getRootElement();
            Assert.assertNotNull(rss);

            Element channel = rss.getChild("channel");
            Assert.assertNotNull(channel);

            List<Element> items = channel.getChildren("item");
            Assert.assertEquals(10, items.size());

            Iterator<Element> it = items.iterator();
            Element item9 = it.next();
            Element title9 = item9.getChild("title");
            Assert.assertNotNull(title9);
            Assert.assertEquals("child9", title9.getText());
            Element pubDate9 = item9.getChild("pubDate");
            Assert.assertNotNull(pubDate9);
            Date date9 = dateFormat.parse(pubDate9.getText());

            Element item8 = it.next();
            Element title8 = item8.getChild("title");
            Assert.assertNotNull(title8);
            Assert.assertEquals("child8", title8.getText());
            Element pubDate8 = item8.getChild("pubDate");
            Assert.assertNotNull(pubDate8);
            Date date8 = dateFormat.parse(pubDate8.getText());

            Element item7 = it.next();
            Element title7 = item7.getChild("title");
            Assert.assertNotNull(title7);
            Assert.assertEquals("child7", title7.getText());
            Element pubDate7 = item7.getChild("pubDate");
            Assert.assertNotNull(pubDate7);
            Date date7 = dateFormat.parse(pubDate7.getText());

            Element item6 = it.next();
            Element title6 = item6.getChild("title");
            Assert.assertNotNull(title6);
            Assert.assertEquals("child6", title6.getText());
            Element pubDate6 = item6.getChild("pubDate");
            Assert.assertNotNull(pubDate6);
            Date date6 = dateFormat.parse(pubDate6.getText());

            Element item5 = it.next();
            Element title5 = item5.getChild("title");
            Assert.assertNotNull(title5);
            Assert.assertEquals("child5", title5.getText());
            Element pubDate5 = item5.getChild("pubDate");
            Assert.assertNotNull(pubDate5);
            Date date5 = dateFormat.parse(pubDate5.getText());

            Element item4 = it.next();
            Element title4 = item4.getChild("title");
            Assert.assertNotNull(title4);
            Assert.assertEquals("child4", title4.getText());
            Element pubDate4 = item4.getChild("pubDate");
            Assert.assertNotNull(pubDate4);
            Date date4 = dateFormat.parse(pubDate4.getText());

            Element item3 = it.next();
            Element title3 = item3.getChild("title");
            Assert.assertNotNull(title3);
            Assert.assertEquals("child3", title3.getText());
            Element pubDate3 = item3.getChild("pubDate");
            Assert.assertNotNull(pubDate3);
            Date date3 = dateFormat.parse(pubDate3.getText());

            Element item2 = it.next();
            Element title2 = item2.getChild("title");
            Assert.assertNotNull(title2);
            Assert.assertEquals("child2", title2.getText());
            Element pubDate2 = item2.getChild("pubDate");
            Assert.assertNotNull(pubDate2);
            Date date2 = dateFormat.parse(pubDate2.getText());

            Element item1 = it.next();
            Element title1 = item1.getChild("title");
            Assert.assertNotNull(title1);
            Assert.assertEquals("child1", title1.getText());
            Element pubDate1 = item1.getChild("pubDate");
            Assert.assertNotNull(pubDate1);
            Date date1 = dateFormat.parse(pubDate1.getText());

            Element rootItem = it.next();
            Element rootTitle = rootItem.getChild("title");
            Assert.assertNotNull(rootTitle);
            Assert.assertEquals("CADCRsstest1", rootTitle.getText());
            Element rootPubDate = rootItem.getChild("pubDate");
            Assert.assertNotNull(rootPubDate);
            Date rootDate = dateFormat.parse(rootPubDate.getText());

            Assert.assertTrue(date9.after(date8));
            Assert.assertTrue(date8.after(date7));
            Assert.assertTrue(date7.after(date6));
            Assert.assertTrue(date6.after(date5));
            Assert.assertTrue(date5.after(date4));
            Assert.assertTrue(date4.after(date3));
            Assert.assertTrue(date3.after(date2));
            Assert.assertTrue(date2.after(date1));
            Assert.assertTrue(date1.after(rootDate));

            log.info("testSetNode passed");
        }
        catch (IllegalArgumentException e)
        {
            String method = Thread.currentThread().getStackTrace()[1].getMethodName();
            String msg = "Skipping integration test: " + method + ". Could not find datasource in ~/.dbrc";
            log.warn(msg, e);
            System.err.println("WARNING: " + msg);
        }
        catch(Exception unexpected)
        {
            log.error("unexpected exception", unexpected);
            Assert.fail("unexpected exception: " + unexpected);
        }
    }


    /**
     * Test class that mimics a very basic persistence layer. Only get and put methods are implemented
     */
    class TestNodePersistence implements NodePersistence {
        private HashMap<String, Node> nodes = new HashMap<>();
        private Random rand = new Random();


        @Override
        public Node get(VOSURI vos) throws NodeNotFoundException, TransientException {
            if (nodes.containsKey(vos.toString())) {
                return nodes.get(vos.toString());
            } else {
                throw new NodeNotFoundException(vos.toString());
            }
        }

        @Override
        public Node put(Node node) throws NodeNotSupportedException, TransientException {
            // if parent is null, this is just a new root-level node,
            if (node.getParent() != null && node.getParent().appData == null)
                throw new IllegalArgumentException("parent of node is not a persistent node: " + node.getUri().getPath());

            if (node.appData != null)
                throw new UnsupportedOperationException("update of existing node not supported; try updateProperties");

            NodeID nodeID = new NodeID();
            nodeID.id = rand.nextLong();
            if (node.getPropertyValue(VOS.PROPERTY_URI_CREATOR) != null) {
                Subject owner = new Subject();
                owner.getPrincipals().add(new X500Principal(node.getPropertyValue(VOS.PROPERTY_URI_CREATOR)));
                nodeID.owner = owner;
            }
            //nodeID.ownerObject = identManager.toOwner(creator);
            node.appData = nodeID;
            Date now = new Date();
            DateFormat dateFormat = DateUtil.getDateFormat(DateUtil.IVOA_DATE_FORMAT, DateUtil.UTC);
            node.getProperties().add(new NodeProperty(VOS.PROPERTY_URI_DATE, dateFormat.format(now)));
            if (node.getParent() != null) {
                ((ContainerNode) node.getParent()).getNodes().add(node);
            }
            nodes.put(node.getUri().toString(), node);
            try {
                Thread.sleep(100);  // needed since the feed uses a map of nodes with the timestamp as the key
            } catch (InterruptedException ignore) {}
            return node;
        }

        @Override
        public Node get(VOSURI vos, boolean allowPartialPaths) throws NodeNotFoundException, TransientException {
            throw new UnsupportedOperationException("Not required for the test");
        }

        @Override
        public Node get(VOSURI vos, boolean allowPartialPaths, boolean resolveMetadata) throws NodeNotFoundException, TransientException {
            throw new UnsupportedOperationException("Not required for the test");
        }

        @Override
        public void getChildren(ContainerNode node) throws TransientException {
            // called but not required to returned the correct result
        }

        @Override
        public void getChildren(ContainerNode parent, VOSURI start, Integer limit) throws TransientException {
            throw new UnsupportedOperationException("Not required for the test");
        }

        @Override
        public void getChildren(ContainerNode node, boolean resolveMetadata) throws TransientException {
            throw new UnsupportedOperationException("Not required for the test");
        }

        @Override
        public void getChildren(ContainerNode parent, VOSURI start, Integer limit, boolean resolveMetadata) throws TransientException {
            throw new UnsupportedOperationException("Not required for the test");
        }

        @Override
        public void getChild(ContainerNode parent, String name) throws TransientException {
            throw new UnsupportedOperationException("Not required for the test");
        }

        @Override
        public void getChild(ContainerNode parent, String name, boolean resolveMetadata) throws TransientException {
            throw new UnsupportedOperationException("Not required for the test");
        }

        @Override
        public void getProperties(Node node) throws TransientException {
            throw new UnsupportedOperationException("Not required for the test");
        }


        @Override
        public Node updateProperties(Node node, List<NodeProperty> properties) throws TransientException {
            throw new UnsupportedOperationException("Not required for the test");
        }

        @Override
        public void delete(Node node) throws TransientException {
            throw new UnsupportedOperationException("Not required for the test");
        }

        @Override
        public void setFileMetadata(DataNode node, FileMetadata meta, boolean strict) throws TransientException {
            throw new UnsupportedOperationException("Not required for the test");
        }

        @Override
        public void setBusyState(DataNode node, VOS.NodeBusyState curState, VOS.NodeBusyState newState) throws TransientException {
            throw new UnsupportedOperationException("Not required for the test");
        }

        @Override
        public void move(Node src, ContainerNode destination) throws TransientException {
            throw new UnsupportedOperationException("Not required for the test");
        }

        @Override
        public void copy(Node src, ContainerNode destination) throws TransientException {
            throw new UnsupportedOperationException("Not required for the test");
        }
    }

    private class GetRootNodeAction implements PrivilegedExceptionAction<Object>
    {
        private NodePersistence nodePersistence;

        GetRootNodeAction(NodePersistence nodePersistence)
        {
            this.nodePersistence = nodePersistence;
        }

        public Object run() throws Exception
        {
            // Root container.
            VOSURI vos = new VOSURI(new URI("vos", VOS_AUTHORITY, "/" + ROOT_CONTAINER, null, null));
            ContainerNode root = new ContainerNode(vos);
            root.getProperties().add(new NodeProperty(VOS.PROPERTY_URI_CREATOR, REGTEST_NODE_OWNER));
            root.getProperties().add(new NodeProperty(VOS.PROPERTY_URI_ISPUBLIC, Boolean.TRUE.toString()));
            root = (ContainerNode) getNode(vos, root);

            // Child container node of root.
            VOSURI child1Uri = new VOSURI(new URI("vos", VOS_AUTHORITY, "/" + ROOT_CONTAINER + "/child1", null, null));
            ContainerNode child1 = new ContainerNode(child1Uri);
            child1.setParent(root);
            child1.getProperties().add(new NodeProperty(VOS.PROPERTY_URI_CREATOR, REGTEST_NODE_OWNER));
            child1.getProperties().add(new NodeProperty(VOS.PROPERTY_URI_ISPUBLIC, Boolean.TRUE.toString()));
            child1 = (ContainerNode) getNode(child1Uri, child1);

            // Child data node of root.
            VOSURI child2Uri = new VOSURI(new URI("vos", VOS_AUTHORITY, "/" + ROOT_CONTAINER + "/child2", null, null));
            DataNode child2 = new DataNode(child2Uri);
            child2.setParent(root);
            child2.getProperties().add(new NodeProperty(VOS.PROPERTY_URI_CREATOR, REGTEST_NODE_OWNER));
            child2.getProperties().add(new NodeProperty(VOS.PROPERTY_URI_ISPUBLIC, Boolean.TRUE.toString()));
            child2 = (DataNode) getNode(child2Uri, child2);

            // Child data node of child1.
            VOSURI child3Uri = new VOSURI(new URI("vos", VOS_AUTHORITY, "/" + ROOT_CONTAINER + "/child1/child3", null, null));
            DataNode child3 = new DataNode(child3Uri);
            child3.setParent(child1);
            child3.getProperties().add(new NodeProperty(VOS.PROPERTY_URI_CREATOR, REGTEST_NODE_OWNER));
            child3.getProperties().add(new NodeProperty(VOS.PROPERTY_URI_ISPUBLIC, Boolean.TRUE.toString()));
            child3 = (DataNode) getNode(child3Uri, child3);

            // Child data node of child2.
            VOSURI child4Uri = new VOSURI(new URI("vos", VOS_AUTHORITY, "/" + ROOT_CONTAINER + "/child1/child4", null, null));
            DataNode child4 = new DataNode(child4Uri);
            child4.setParent(child1);
            child4.getProperties().add(new NodeProperty(VOS.PROPERTY_URI_CREATOR, REGTEST_NODE_OWNER));
            child4.getProperties().add(new NodeProperty(VOS.PROPERTY_URI_ISPUBLIC, Boolean.TRUE.toString()));
            child4 = (DataNode) getNode(child4Uri, child4);

             // Private child container node of root.
            VOSURI child5Uri = new VOSURI(new URI("vos", VOS_AUTHORITY, "/" + ROOT_CONTAINER + "/child5", null, null));
            ContainerNode child5 = new ContainerNode(child5Uri);
            child5.setParent(root);
            child5.getProperties().add(new NodeProperty(VOS.PROPERTY_URI_CREATOR, REGTEST_NODE_OWNER));
            child5.getProperties().add(new NodeProperty(VOS.PROPERTY_URI_ISPUBLIC, Boolean.FALSE.toString()));
            child5 = (ContainerNode) getNode(child5Uri, child5);

            // Private data node of private child5.
            VOSURI child6Uri = new VOSURI(new URI("vos", VOS_AUTHORITY, "/" + ROOT_CONTAINER + "/child5/child6", null, null));
            DataNode child6 = new DataNode(child6Uri);
            child6.setParent(child5);
            child6.getProperties().add(new NodeProperty(VOS.PROPERTY_URI_CREATOR, REGTEST_NODE_OWNER));
            child6.getProperties().add(new NodeProperty(VOS.PROPERTY_URI_ISPUBLIC, Boolean.FALSE.toString()));
            child6 = (DataNode) getNode(child6Uri, child6);

            // Public data node of private child5.
            VOSURI child7Uri = new VOSURI(new URI("vos", VOS_AUTHORITY, "/" + ROOT_CONTAINER + "/child5/child7", null, null));
            DataNode child7 = new DataNode(child7Uri);
            child7.setParent(child5);
            child7.getProperties().add(new NodeProperty(VOS.PROPERTY_URI_CREATOR, REGTEST_NODE_OWNER));
            child7.getProperties().add(new NodeProperty(VOS.PROPERTY_URI_ISPUBLIC, Boolean.TRUE.toString()));
            child7 = (DataNode) getNode(child7Uri, child7);

            // Private data node of root.
            VOSURI child8Uri = new VOSURI(new URI("vos", VOS_AUTHORITY, "/" + ROOT_CONTAINER + "/child8", null, null));
            DataNode child8 = new DataNode(child8Uri);
            child8.setParent(root);
            child8.getProperties().add(new NodeProperty(VOS.PROPERTY_URI_CREATOR, REGTEST_NODE_OWNER));
            child8.getProperties().add(new NodeProperty(VOS.PROPERTY_URI_ISPUBLIC, Boolean.TRUE.toString()));
            child8 = (DataNode) getNode(child8Uri, child8);

            // Private container node of private child5.
            VOSURI child9Uri = new VOSURI(new URI("vos", VOS_AUTHORITY, "/" + ROOT_CONTAINER + "/child5/child9", null, null));
            ContainerNode child9 = new ContainerNode(child9Uri);
            child9.setParent(child5);
            child9.getProperties().add(new NodeProperty(VOS.PROPERTY_URI_CREATOR, REGTEST_NODE_OWNER));
            child9.getProperties().add(new NodeProperty(VOS.PROPERTY_URI_ISPUBLIC, Boolean.FALSE.toString()));
            child9 = (ContainerNode) getNode(child9Uri, child9);

            return root;
        }

        private Node getNode(VOSURI vos, Node node) throws NodeNotSupportedException, TransientException
        {
            try
            {
                node = nodePersistence.get(vos);
                log.debug("found root node: " + node.getName());
            }
            catch (NodeNotFoundException e)
            {
                node = nodePersistence.put(node);
                log.debug("put root node: " + node.getName());
            }
            return node;
        }

    }

    private class GetRootNodeActionOnStructuredDataNode implements PrivilegedExceptionAction<Object>
    {
        private NodePersistence nodePersistence;

        GetRootNodeActionOnStructuredDataNode(NodePersistence nodePersistence)
        {
            this.nodePersistence = nodePersistence;
        }

        public Object run() throws Exception
        {
            // Root container.
            VOSURI vos = new VOSURI(new URI("vos", VOS_AUTHORITY, "/" + ROOT_CONTAINER, null, null));
            ContainerNode root = new ContainerNode(vos);
            root.getProperties().add(new NodeProperty(VOS.PROPERTY_URI_CREATOR, REGTEST_NODE_OWNER));
            root.getProperties().add(new NodeProperty(VOS.PROPERTY_URI_ISPUBLIC, Boolean.TRUE.toString()));
            root = (ContainerNode) getNode(vos, root);

            // Child container node of root.
            VOSURI child1Uri = new VOSURI(new URI("vos", VOS_AUTHORITY, "/" + ROOT_CONTAINER + "/child1", null, null));
            ContainerNode child1 = new ContainerNode(child1Uri);
            child1.setParent(root);
            child1.getProperties().add(new NodeProperty(VOS.PROPERTY_URI_CREATOR, REGTEST_NODE_OWNER));
            child1.getProperties().add(new NodeProperty(VOS.PROPERTY_URI_ISPUBLIC, Boolean.TRUE.toString()));
            child1 = (ContainerNode) getNode(child1Uri, child1);

            // Child data node of root.
            VOSURI child2Uri = new VOSURI(new URI("vos", VOS_AUTHORITY, "/" + ROOT_CONTAINER + "/child2StructuredDataNode", null, null));
            StructuredDataNode child2 = new StructuredDataNode(child2Uri);
            child2.setParent(root);
            child2.getProperties().add(new NodeProperty(VOS.PROPERTY_URI_CREATOR, REGTEST_NODE_OWNER));
            child2.getProperties().add(new NodeProperty(VOS.PROPERTY_URI_ISPUBLIC, Boolean.TRUE.toString()));
            child2 = (StructuredDataNode) getNode(child2Uri, child2);

            return root;
        }

        private Node getNode(VOSURI vos, Node node) throws NodeNotSupportedException, TransientException
        {
            try
            {
                node = nodePersistence.get(vos);
                log.debug("found root node: " + node.getName());
            }
            catch (NodeNotFoundException e)
            {
                node = nodePersistence.put(node);
                log.debug("put root node: " + node.getName());
            }
            return node;
        }

    }

    private class GetRootNodeActionOnLinkNode implements PrivilegedExceptionAction<Object>
    {
        private NodePersistence nodePersistence;

        GetRootNodeActionOnLinkNode(NodePersistence nodePersistence)
        {
            this.nodePersistence = nodePersistence;
        }

        public Object run() throws Exception
        {
            // Root container.
            VOSURI vos = new VOSURI(new URI("vos", VOS_AUTHORITY, "/" + ROOT_CONTAINER, null, null));
            ContainerNode root = new ContainerNode(vos);
            root.getProperties().add(new NodeProperty(VOS.PROPERTY_URI_CREATOR, REGTEST_NODE_OWNER));
            root.getProperties().add(new NodeProperty(VOS.PROPERTY_URI_ISPUBLIC, Boolean.TRUE.toString()));
            root = (ContainerNode) getNode(vos, root);

            // Child container node of root.
            VOSURI child1Uri = new VOSURI(new URI("vos", VOS_AUTHORITY, "/" + ROOT_CONTAINER + "/child1", null, null));
            ContainerNode child1 = new ContainerNode(child1Uri);
            child1.setParent(root);
            child1.getProperties().add(new NodeProperty(VOS.PROPERTY_URI_CREATOR, REGTEST_NODE_OWNER));
            child1.getProperties().add(new NodeProperty(VOS.PROPERTY_URI_ISPUBLIC, Boolean.TRUE.toString()));
            child1 = (ContainerNode) getNode(child1Uri, child1);

            // Child data node of root.
            VOSURI child2Uri = new VOSURI(new URI("vos", VOS_AUTHORITY, "/" + ROOT_CONTAINER + "/child2LinkNode", null, null));
            URI child2Target = new URI("vos", VOS_AUTHORITY, "/" + ROOT_CONTAINER + "/child2Target", null, null);
            LinkNode child2 = new LinkNode(child2Uri, child2Target);
            child2.setParent(root);
            child2.getProperties().add(new NodeProperty(VOS.PROPERTY_URI_CREATOR, REGTEST_NODE_OWNER));
            child2.getProperties().add(new NodeProperty(VOS.PROPERTY_URI_ISPUBLIC, Boolean.TRUE.toString()));
            child2 = (LinkNode) getNode(child2Uri, child2);

            return root;
        }

        private Node getNode(VOSURI vos, Node node) throws NodeNotSupportedException, TransientException
        {
            try
            {
                node = nodePersistence.get(vos);
                log.debug("found root node: " + node.getName());
            }
            catch (NodeNotFoundException e)
            {
                node = nodePersistence.put(node);
                log.debug("put root node: " + node.getName());
            }
            return node;
        }

    }
    
    private class GetChildNodeAction implements PrivilegedExceptionAction<Object>
    {
        private NodePersistence nodePersistence;

        GetChildNodeAction(NodePersistence nodePersistence)
        {
            this.nodePersistence = nodePersistence;
        }

        public Object run() throws Exception
        {
            // Private container node of private child5.
            VOSURI childUri = new VOSURI(new URI("vos", VOS_AUTHORITY, "/" + ROOT_CONTAINER + "/child5/child9", null, null));
            ContainerNode child = new ContainerNode(childUri);
            return (ContainerNode) getNode(childUri, child);
        }

        public String getPath()
        {
            return "http://" + VOS_AUTHORITY + "/" + ROOT_CONTAINER + "/child5/child9";
        }

        private Node getNode(VOSURI vos, Node node) throws NodeNotSupportedException, TransientException
        {
            try
            {
                node = nodePersistence.get(vos);
                log.debug("found root node: " + node.getName());
            }
            catch (NodeNotFoundException e)
            {
                node = nodePersistence.put(node);
                log.debug("put root node: " + node.getName());
            }
            return node;
        }

    }

    private class SetNodeAction implements PrivilegedExceptionAction<Object>
    {
        private NodePersistence nodePersistence;
        private VOSpaceAuthorizer voSpaceAuthorizer;
        private ContainerNode root;
        private String path;

        SetNodeAction(NodePersistence nodePersistence, VOSpaceAuthorizer voSpaceAuthorizer, ContainerNode root, String path)
        {
            this.nodePersistence = nodePersistence;
            this.voSpaceAuthorizer = voSpaceAuthorizer;
            this.root = root;
            this.path = path;
        }

        public Object run() throws Exception
        {
            RssView view = new RssView();
            view.setNodePersistence(nodePersistence);
            view.setVOSpaceAuthorizer(voSpaceAuthorizer);
            view.setNode(root, "", new URL(path));

            log.debug("xml: " + view.xmlString);

            return view.xmlString;
        }

    }

}
