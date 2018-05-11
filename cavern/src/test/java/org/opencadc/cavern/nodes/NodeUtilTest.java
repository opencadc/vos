/*
************************************************************************
*******************  CANADIAN ASTRONOMY DATA CENTRE  *******************
**************  CENTRE CANADIEN DE DONNÉES ASTRONOMIQUES  **************
*
*  (c) 2017.                            (c) 2017.
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

package org.opencadc.cavern.nodes;

import ca.nrc.cadc.util.Log4jInit;
import ca.nrc.cadc.vos.ContainerNode;
import ca.nrc.cadc.vos.DataNode;
import ca.nrc.cadc.vos.LinkNode;
import ca.nrc.cadc.vos.Node;
import ca.nrc.cadc.vos.NodeProperty;
import ca.nrc.cadc.vos.VOS;
import ca.nrc.cadc.vos.VOSURI;
import java.io.IOException;
import java.net.URI;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.Path;
import java.nio.file.attribute.UserPrincipal;
import java.nio.file.attribute.UserPrincipalLookupService;
import java.util.Iterator;
import java.util.UUID;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.junit.Assert;
import org.junit.Test;

/**
 *
 * @author pdowler
 */
public class NodeUtilTest {
    private static final Logger log = Logger.getLogger(NodeUtilTest.class);

    static {
        Log4jInit.setLevel("org.opencadc.cavern", Level.DEBUG);
        Log4jInit.setLevel("ca.nrc.cadc.reg.client", Level.DEBUG);
        Log4jInit.setLevel("ca.nrc.cadc.util", Level.DEBUG);
    }

    static final String ROOT = System.getProperty("java.io.tmpdir") + "/cavern-tests";

    static final String OWNER = System.getProperty("user.name");
    static final String GROUP = System.getProperty("user.name");

    static {
        try {
            Path root = FileSystems.getDefault().getPath(ROOT);
            if (!Files.exists(root)) {
                Files.createDirectory(root);
            }
        } catch (IOException ex) {
            throw new RuntimeException("TEST SETUP: failed to create test dir: " + ROOT, ex);
        }
    }

    public NodeUtilTest() {
    }

    private Path doCreate(final Path root, final Node n, UserPrincipal up) throws Exception {
        Path actual = NodeUtil.create(root, n);

        Assert.assertNotNull(actual);
        Assert.assertTrue("exists", Files.exists(actual, LinkOption.NOFOLLOW_LINKS));
        Assert.assertEquals("name", n.getName(), actual.toFile().getName());
        UserPrincipal owner = Files.getOwner(actual, LinkOption.NOFOLLOW_LINKS);
        Assert.assertNotNull(owner);
        Assert.assertEquals(up, owner);

        return actual;
    }

    //@Test
    public void testGetRoot() {
        try {
            VOSURI uri = new VOSURI(URI.create("vos://canfar.net~cavern"));
            Path root = FileSystems.getDefault().getPath(ROOT);

            Node rootNode = NodeUtil.get(root, uri);
            Assert.assertNotNull(rootNode);
            final ContainerNode cn1 = (ContainerNode) rootNode;
            Assert.assertEquals(uri, rootNode.getUri());
            Assert.assertTrue(rootNode.getUri().isRoot());
            Assert.assertTrue(rootNode.isPublic());

            uri = new VOSURI(URI.create("vos://canfar.net~cavern/"));
            root = FileSystems.getDefault().getPath(ROOT);

            rootNode = NodeUtil.get(root, uri);
            Assert.assertNotNull(rootNode);
            final ContainerNode cn2 = (ContainerNode) rootNode;
            Assert.assertEquals(uri, rootNode.getUri());
            Assert.assertTrue(rootNode.getUri().isRoot());
            Assert.assertTrue(rootNode.isPublic());

        } catch (Exception unexpected) {
            log.error("unexpected exception", unexpected);
            Assert.fail("unexpected exception: " + unexpected);
        }
    }

    @Test
    public void testCreateDir() {
        try {
            // top-level test dir
            String name = "testCreateDir-" + UUID.randomUUID().toString();
            VOSURI testDir = new VOSURI(URI.create("vos://canfar.net~cavern/" + name));
            Path root = FileSystems.getDefault().getPath(ROOT);
            UserPrincipalLookupService users = root.getFileSystem().getUserPrincipalLookupService();
            UserPrincipal up = users.lookupPrincipalByName(OWNER);
            ContainerNode n = new ContainerNode(testDir);
            NodeUtil.setOwner(n, up);
            Path dir = doCreate(root, n, up);
            Assert.assertTrue("dir", Files.isDirectory(dir));

            // actual child test
            VOSURI uri = new VOSURI(URI.create(testDir.getURI().toASCIIString() + "/dir-" + name));
            ContainerNode tn = new ContainerNode(uri);
            tn.getProperties().add(new NodeProperty(VOS.PROPERTY_URI_ISPUBLIC, "true"));
            String propURI = "ivo://opencadc.org/cavern#clash";
            String propValue = "should I stay or should I go?";
            tn.getProperties().add(new NodeProperty(propURI, propValue));
            
            // use one group-read property to test ACL round trip
            String roGroup = "ivo://cadc.nrc.ca/gms?" + GROUP;
            tn.getProperties().add(new NodeProperty(VOS.PROPERTY_URI_GROUPREAD, roGroup));
            
            NodeUtil.setOwner(tn, up);
            Path tdir = doCreate(root, tn, up);
            Node tn2 = NodeUtil.get(root, uri);
            log.info("found: " + tn2);
            Assert.assertNotNull(tn2);
            Assert.assertTrue(tn2 instanceof ContainerNode);
            Assert.assertEquals(tn.getUri(), tn2.getUri());
            Assert.assertTrue("public", tn2.isPublic());
            Assert.assertNotNull("lastModified", tn2.getPropertyValue(VOS.PROPERTY_URI_DATE));
            Assert.assertEquals("custom " + propURI, propValue, tn2.getPropertyValue(propURI));
            Assert.assertEquals("read-only " + VOS.PROPERTY_URI_GROUPREAD, roGroup, tn2.getPropertyValue(VOS.PROPERTY_URI_GROUPREAD));
            
            //NodeUtil.delete(root, testDir);
            //Assert.assertFalse("deleted", Files.exists(dir));
        } catch (Exception unexpected) {
            log.error("unexpected exception", unexpected);
            Assert.fail("unexpected exception: " + unexpected);
        }
    }

    @Test
    public void testCreateFile() {
        try {
            // top-level test dir
            String name = "testCreateFile-" + UUID.randomUUID().toString();
            VOSURI testDir = new VOSURI(URI.create("vos://canfar.net~cavern/" + name));
            Path root = FileSystems.getDefault().getPath(ROOT);
            UserPrincipalLookupService users = root.getFileSystem().getUserPrincipalLookupService();
            UserPrincipal up = users.lookupPrincipalByName(OWNER);
            ContainerNode n = new ContainerNode(testDir);
            NodeUtil.setOwner(n, up);
            Path dir = doCreate(root, n, up);
            Assert.assertTrue("dir", Files.isDirectory(dir));

            // actual child test
            VOSURI uri = new VOSURI(URI.create(testDir.getURI().toASCIIString() + "/file-" + name));
            DataNode tn = new DataNode(uri);
            tn.getProperties().add(new NodeProperty(VOS.PROPERTY_URI_ISPUBLIC, "true"));
            String propURI = "ivo://opencadc.org/cavern#clash";
            String propValue = "should I stay or should I go?";
            tn.getProperties().add(new NodeProperty(propURI, propValue));
            String origMD5 = "74808746f32f28650559885297f76efa";
            tn.getProperties().add(new NodeProperty(VOS.PROPERTY_URI_CONTENTMD5, origMD5));
            
            // use one group-read property to test ACL round trip
            String roGroup = "ivo://cadc.nrc.ca/gms?" + GROUP;
            tn.getProperties().add(new NodeProperty(VOS.PROPERTY_URI_GROUPREAD, roGroup));
            
            NodeUtil.setOwner(tn, up);
            Path tdir = doCreate(root, tn, up);
            Node tn2 = NodeUtil.get(root, uri);
            log.info("found: " + tn2);
            Assert.assertNotNull(tn2);
            Assert.assertTrue(tn2 instanceof DataNode);
            Assert.assertEquals(tn.getUri(), tn2.getUri());
            Assert.assertTrue("public", tn2.isPublic());
            Assert.assertNotNull("lastModified", tn2.getPropertyValue(VOS.PROPERTY_URI_DATE));
            Assert.assertNotNull("Content-Length", tn2.getPropertyValue(VOS.PROPERTY_URI_CONTENTLENGTH));
            Assert.assertEquals("Content-Length", "0", tn2.getPropertyValue(VOS.PROPERTY_URI_CONTENTLENGTH));
            Assert.assertEquals("custom " + propURI, propValue, tn2.getPropertyValue(propURI));
            Assert.assertEquals("Content-MD5", origMD5, tn2.getPropertyValue(VOS.PROPERTY_URI_CONTENTMD5));
            String roActual = tn2.getPropertyValue(VOS.PROPERTY_URI_GROUPREAD);
            Assert.assertNotNull("read-only", roActual);
            Assert.assertEquals("read-only", roGroup, roActual);
            String rwActual = tn2.getPropertyValue(VOS.PROPERTY_URI_GROUPWRITE);
            Assert.assertNull("read-write", rwActual);

            //NodeUtil.delete(root, testDir);
            //Assert.assertFalse("deleted", Files.exists(dir));
        } catch (Exception unexpected) {
            log.error("unexpected exception", unexpected);
            Assert.fail("unexpected exception: " + unexpected);
        }
    }
    
    @Test
    public void testSetProperties() {
        try {
            // top-level test dir
            String name = "testSetProperties-" + UUID.randomUUID().toString();
            VOSURI testDir = new VOSURI(URI.create("vos://canfar.net~cavern/" + name));
            Path root = FileSystems.getDefault().getPath(ROOT);
            UserPrincipalLookupService users = root.getFileSystem().getUserPrincipalLookupService();
            UserPrincipal up = users.lookupPrincipalByName(OWNER);
            ContainerNode n = new ContainerNode(testDir);
            NodeUtil.setOwner(n, up);
            Path dir = doCreate(root, n, up);
            Assert.assertTrue("dir", Files.isDirectory(dir));

            // actual child test
            VOSURI uri = new VOSURI(URI.create(testDir.getURI().toASCIIString() + "/file-" + name));
            DataNode tn = new DataNode(uri);
            
            final String propURI = "ivo://opencadc.org/cavern#clash";
            final String propValue = "should I stay or should I go?";
            final String origMD5 = "74808746f32f28650559885297f76efa";
            
            NodeUtil.setOwner(tn, up);
            final Path tdir = doCreate(root, tn, up);
            Node tn2 = NodeUtil.get(root, uri);
            log.info("found: " + tn2);
            Assert.assertNotNull(tn2);
            Assert.assertTrue(tn2 instanceof DataNode);
            Assert.assertEquals(tn.getUri(), tn2.getUri());
            
            Assert.assertFalse("public", tn2.isPublic());
            Assert.assertNotNull("lastModified", tn2.getPropertyValue(VOS.PROPERTY_URI_DATE));
            Assert.assertNull("custom " + propURI, tn2.getPropertyValue(propURI));
            Assert.assertNull("Content-MD5", tn2.getPropertyValue(VOS.PROPERTY_URI_CONTENTMD5));
            
            tn.getProperties().add(new NodeProperty(VOS.PROPERTY_URI_ISPUBLIC, "true"));
            tn.getProperties().add(new NodeProperty(VOS.PROPERTY_URI_CONTENTMD5, origMD5));
            tn.getProperties().add(new NodeProperty(propURI, propValue));
            // use one group-read property to test ACL round trip
            String roGroup = "ivo://cadc.nrc.ca/gms?" + GROUP;
            tn.getProperties().add(new NodeProperty(VOS.PROPERTY_URI_GROUPREAD, roGroup));
            
            NodeUtil.setNodeProperties(tdir, tn);
            
            tn2 = NodeUtil.get(root, uri);
            log.info("found: " + tn2);
            Assert.assertNotNull(tn2);
            Assert.assertTrue(tn2 instanceof DataNode);
            Assert.assertEquals(tn.getUri(), tn2.getUri());
            Assert.assertTrue("public", tn2.isPublic());
            Assert.assertNotNull("lastModified", tn2.getPropertyValue(VOS.PROPERTY_URI_DATE));
            Assert.assertEquals("custom " + propURI, propValue, tn2.getPropertyValue(propURI));
            Assert.assertEquals("Content-MD5", origMD5, tn2.getPropertyValue(VOS.PROPERTY_URI_CONTENTMD5));

            //NodeUtil.delete(root, testDir);
            //Assert.assertFalse("deleted", Files.exists(dir));
        } catch (Exception unexpected) {
            log.error("unexpected exception", unexpected);
            Assert.fail("unexpected exception: " + unexpected);
        }
    }
    
    @Test
    public void testNoSuchGroupFail() {
        try {
            // top-level test dir
            String name = "testNoSuchGroupFail-" + UUID.randomUUID().toString();
            VOSURI testDir = new VOSURI(URI.create("vos://canfar.net~cavern/" + name));
            Path root = FileSystems.getDefault().getPath(ROOT);
            UserPrincipalLookupService users = root.getFileSystem().getUserPrincipalLookupService();
            UserPrincipal up = users.lookupPrincipalByName(OWNER);
            ContainerNode n = new ContainerNode(testDir);
            NodeUtil.setOwner(n, up);
            Path dir = doCreate(root, n, up);
            Assert.assertTrue("dir", Files.isDirectory(dir));

            // actual child test
            VOSURI uri = new VOSURI(URI.create(testDir.getURI().toASCIIString() + "/file-" + name));
            DataNode tn = new DataNode(uri);
            // use one group-read property to test ACL round trip
            String roGroup = "ivo://cadc.nrc.ca/gms?NOGROUP";
            tn.getProperties().add(new NodeProperty(VOS.PROPERTY_URI_GROUPREAD, roGroup));
            
            NodeUtil.setOwner(tn, up);
            Path tdir = doCreate(root, tn, up);
            Assert.fail("expected RuntimeException: got " + tdir);
        } catch (RuntimeException expected) {
            log.info("caught expected exception: " + expected);
        } catch (Exception unexpected) {
            log.error("unexpected exception", unexpected);
            Assert.fail("unexpected exception: " + unexpected);
        }
    }
    
    @Test
    public void testExternalGroupFail() {
        try {
            // top-level test dir
            String name = "testExternalGroupFail-" + UUID.randomUUID().toString();
            VOSURI testDir = new VOSURI(URI.create("vos://canfar.net~cavern/" + name));
            Path root = FileSystems.getDefault().getPath(ROOT);
            UserPrincipalLookupService users = root.getFileSystem().getUserPrincipalLookupService();
            UserPrincipal up = users.lookupPrincipalByName(OWNER);
            ContainerNode n = new ContainerNode(testDir);
            NodeUtil.setOwner(n, up);
            Path dir = doCreate(root, n, up);
            Assert.assertTrue("dir", Files.isDirectory(dir));

            // actual child test
            VOSURI uri = new VOSURI(URI.create(testDir.getURI().toASCIIString() + "/file-" + name));
            DataNode tn = new DataNode(uri);
            // use one group-read property to test ACL round trip
            String roGroup = "ivo://extern.net/gms?" + GROUP;
            tn.getProperties().add(new NodeProperty(VOS.PROPERTY_URI_GROUPREAD, roGroup));
            
            NodeUtil.setOwner(tn, up);
            Path tdir = doCreate(root, tn, up);
            Assert.fail("expected RuntimeException: got " + tdir);
        } catch (RuntimeException expected) {
            log.info("caught expected exception: " + expected);
        } catch (Exception unexpected) {
            log.error("unexpected exception", unexpected);
            Assert.fail("unexpected exception: " + unexpected);
        }
    }

    @Test
    public void testCreateLink() {
        try {
            // top-level test dir
            String name = "testCreateLink-" + UUID.randomUUID().toString();
            VOSURI testDir = new VOSURI(URI.create("vos://canfar.net~cavern/" + name));
            Path root = FileSystems.getDefault().getPath(ROOT);
            UserPrincipalLookupService users = root.getFileSystem().getUserPrincipalLookupService();
            UserPrincipal up = users.lookupPrincipalByName(OWNER);
            ContainerNode testN = new ContainerNode(testDir);
            NodeUtil.setOwner(testN, up);
            Path dir = doCreate(root, testN, up);
            Assert.assertTrue("dir", Files.isDirectory(dir));

            VOSURI uri = new VOSURI(URI.create(testDir.getURI().toASCIIString() + "/file-" + name));
            VOSURI luri = new VOSURI(URI.create(testDir.getURI().toASCIIString() + "/link-to-" + name));

            Node dn = new DataNode(uri);
            NodeUtil.setOwner(dn, up);
            final Path file = doCreate(root, dn, up);

            LinkNode ln = new LinkNode(luri, uri.getURI());
            
            // attrs on symlinks don't work, but set property to make sure it doesn't break anything
            String propURI = "ivo://opencadc.org/cavern#clash";
            String propValue = "should I stay or should I go?";
            ln.getProperties().add(new NodeProperty(propURI, propValue));
            NodeUtil.setOwner(ln, up);

            Path link = doCreate(root, ln, up);
            Assert.assertTrue("link", Files.isSymbolicLink(link));

            Node nn = NodeUtil.get(root, luri);
            Assert.assertNotNull(nn);
            log.info("found: " + nn);
            Assert.assertTrue(nn instanceof LinkNode);
            LinkNode ln2 = (LinkNode) nn;
            Assert.assertEquals(ln.getUri(), ln2.getUri());
            Assert.assertEquals(ln.getTarget(), ln2.getTarget());
            
            // see above
            //Assert.assertEquals("custom " + propURI, propValue, ln2.getPropertyValue(propURI));
            
            // TODO: test that the link is relative to fs-mount view will be consistent

            NodeUtil.delete(root, luri);
            Assert.assertFalse("deleted", Files.exists(link));
            Assert.assertTrue("target not deleted", Files.exists(file));

            NodeUtil.delete(root, testDir);
            Assert.assertFalse("deleted", Files.exists(file));
        } catch (Exception unexpected) {
            log.error("unexpected exception", unexpected);
            Assert.fail("unexpected exception: " + unexpected);
        }
    }

    @Test
    public void testCreatePath() {

        try {
            Path root = FileSystems.getDefault().getPath(ROOT);
            UserPrincipalLookupService users = root.getFileSystem().getUserPrincipalLookupService();
            UserPrincipal up = users.lookupPrincipalByName(OWNER);

            String name = "testCreatePath-" + UUID.randomUUID().toString();
            VOSURI testURI = new VOSURI(URI.create("vos://canfar.net~cavern/" + name));
            ContainerNode n = new ContainerNode(testURI);
            NodeUtil.setOwner(n, up);
            Path dir = doCreate(root, n, up);
            Assert.assertTrue("dir", Files.isDirectory(dir));

            StringBuilder sb = new StringBuilder(testURI.getURI().toASCIIString());
            for (int i = 0; i < 10; i++) {
                name = "dir" + i;
                VOSURI uri = new VOSURI(URI.create(sb.toString() + "/" + name));
                sb.append("/").append(name);

                n = new ContainerNode(uri);
                NodeUtil.setOwner(n, up);

                Path d = doCreate(root, n, up);
                Assert.assertTrue("dir", Files.isDirectory(d));

                Node nn = NodeUtil.get(root, uri);
                log.info("found: " + nn);
                Assert.assertNotNull(nn);
                Assert.assertTrue(nn instanceof ContainerNode);
                Assert.assertEquals(n.getUri(), nn.getUri());
                Assert.assertNotNull("lastModified", nn.getPropertyValue(VOS.PROPERTY_URI_DATE));
                // count nodes in path
                int num = 0;
                ContainerNode parent = nn.getParent();
                while (parent != null) {
                    log.info("[testCreatePath] parent: " + parent.getUri());
                    num++;
                    parent = parent.getParent();
                }
                Assert.assertEquals("num parents", i + 1, num); // testURI + dir{i}

            }

            //NodeUtil.delete(root, testURI);
            //Assert.assertFalse("deleted", Files.exists(dir));
        } catch (Exception unexpected) {
            log.error("unexpected exception", unexpected);
            Assert.fail("unexpected exception: " + unexpected);
        }
    }

    @Test
    public void testList() {

        try {
            Path root = FileSystems.getDefault().getPath(ROOT);
            UserPrincipalLookupService users = root.getFileSystem().getUserPrincipalLookupService();
            UserPrincipal up = users.lookupPrincipalByName(OWNER);

            String name = "testList-" + UUID.randomUUID().toString();
            VOSURI testURI = new VOSURI(URI.create("vos://canfar.net~cavern/" + name));
            ContainerNode n = new ContainerNode(testURI);
            NodeUtil.setOwner(n, up);
            Path dir = doCreate(root, n, up);
            Assert.assertTrue("dir", Files.isDirectory(dir));

            for (int i = 0; i < 10; i++) {
                name = "dir" + i;
                VOSURI uri = new VOSURI(URI.create(testURI.getURI().toASCIIString() + "/" + name));

                n = new ContainerNode(uri);
                NodeUtil.setOwner(n, up);

                Path d = doCreate(root, n, up);
                Assert.assertTrue("dir", Files.isDirectory(d));

                Node nn = NodeUtil.get(root, uri);
                log.info("found: " + nn);
                Assert.assertNotNull(nn);
                Assert.assertTrue(nn instanceof ContainerNode);
                Assert.assertEquals(n.getUri(), nn.getUri());
                Assert.assertNotNull("lastModified", nn.getPropertyValue(VOS.PROPERTY_URI_DATE));

                // count children
                ContainerNode parent = nn.getParent();
                Iterator<Node> iter = NodeUtil.list(root, parent, null, null);
                int num = 0;
                while (iter.hasNext()) {
                    Node nf = iter.next();
                    log.info("[testList] found: " + nf.getUri());
                    num++;
                }
                Assert.assertEquals("num siblings", i + 1, num);
            }

            //NodeUtil.delete(root, testURI);
            //Assert.assertFalse("deleted", Files.exists(dir));
        } catch (Exception unexpected) {
            log.error("unexpected exception", unexpected);
            Assert.fail("unexpected exception: " + unexpected);
        }
    }

    @Test
    public void testMove() {
        try {
            // top-level test dir
            String name = "testMove-src-" + UUID.randomUUID().toString();
            VOSURI testDir = new VOSURI(URI.create("vos://canfar.net~cavern/" + name));
            Path root = FileSystems.getDefault().getPath(ROOT);
            UserPrincipalLookupService users = root.getFileSystem().getUserPrincipalLookupService();
            UserPrincipal up = users.lookupPrincipalByName(OWNER);
            ContainerNode n = new ContainerNode(testDir);
            NodeUtil.setOwner(n, up);
            Path dir = doCreate(root, n, up);
            Assert.assertTrue("dir", Files.isDirectory(dir));

            // make move target dir
            String name2 = "testMove-dest-" + UUID.randomUUID().toString();
            VOSURI testDir2 = new VOSURI(URI.create("vos://canfar.net~cavern/" + name2));
            ContainerNode n2 = new ContainerNode(testDir2);
            NodeUtil.setOwner(n2, up);
            Path dir2 = doCreate(root, n2, up);
            Assert.assertTrue("dir2", Files.isDirectory(dir2));

            // TEST MOVE FILE

            // create data node in dir 1
            VOSURI uri = new VOSURI(URI.create(testDir.getURI().toASCIIString() + "/file-" + name));
            DataNode tn = new DataNode(uri);
            NodeUtil.setOwner(tn, up);
            Path tfile = doCreate(root, tn, up);
            Node tn2 = NodeUtil.get(root, uri);
            log.info("found: " + tn2);
            Assert.assertNotNull(tn2);
            Assert.assertTrue(tn2 instanceof DataNode);
            Assert.assertEquals(tn.getUri(), tn2.getUri());
            Assert.assertNotNull("lastModified", tn2.getPropertyValue(VOS.PROPERTY_URI_DATE));

            // move the data node to dir2
            log.debug("Moving: " + tn2.getUri() + " to " + testDir2);
            NodeUtil.move(root, tn.getUri(), testDir2, tn.getName(), up);
            uri = new VOSURI(URI.create(testDir2.getURI().toASCIIString() + "/file-" + name));
            log.debug("Asserting: " + uri);
            Node moved = NodeUtil.get(root, uri);
            Assert.assertNotNull(moved);
            Assert.assertTrue(moved instanceof DataNode);
            Assert.assertEquals(uri, moved.getUri());
            Assert.assertNotNull("lastModified", moved.getPropertyValue(VOS.PROPERTY_URI_DATE));

            // TEST MOVE DIR

            // move dir2 to dir1
            log.debug("Moving: " + testDir2 + " to " + dir);
            NodeUtil.move(root, testDir2, testDir, testDir2.getName(), up);
            uri = new VOSURI(URI.create(testDir.getURI().toASCIIString() + "/" + name2));
            log.debug("Asserting: " + uri);
            moved = NodeUtil.get(root, uri);
            Assert.assertNotNull(moved);
            Assert.assertTrue(moved instanceof ContainerNode);
            Assert.assertEquals(uri, moved.getUri());
            Assert.assertNotNull("lastModified", moved.getPropertyValue(VOS.PROPERTY_URI_DATE));

            // ensure the previously moved file is there
            uri = new VOSURI(URI.create(uri.getURI().toASCIIString() + "/file-" + name));
            log.debug("Asserting: " + uri);
            moved = NodeUtil.get(root, uri);
            Assert.assertNotNull(moved);
            Assert.assertTrue(moved instanceof DataNode);
            Assert.assertEquals(uri, moved.getUri());
            Assert.assertNotNull("lastModified", moved.getPropertyValue(VOS.PROPERTY_URI_DATE));

        } catch (Exception unexpected) {
            log.error("unexpected exception", unexpected);
            Assert.fail("unexpected exception: " + unexpected);
        }
    }

    @Test
    public void testCopyFile() {
        try {
            // top-level test dir
            String name = "testCopyFile-" + UUID.randomUUID().toString();
            VOSURI testDir = new VOSURI(URI.create("vos://canfar.net~cavern/" + name));
            Path root = FileSystems.getDefault().getPath(ROOT);
            UserPrincipalLookupService users = root.getFileSystem().getUserPrincipalLookupService();
            UserPrincipal up = users.lookupPrincipalByName(OWNER);
            ContainerNode n = new ContainerNode(testDir);
            NodeUtil.setOwner(n, up);
            Path dir = doCreate(root, n, up);
            Assert.assertTrue("dir", Files.isDirectory(dir));

            // make copy target dir
            String name2 = UUID.randomUUID().toString();
            VOSURI testDir2 = new VOSURI(URI.create("vos://canfar.net~cavern/" + name2));
            ContainerNode n2 = new ContainerNode(testDir2);
            NodeUtil.setOwner(n2, up);
            Path dir2 = doCreate(root, n2, up);
            Assert.assertTrue("dir2", Files.isDirectory(dir2));

            // TEST COPY FILE

            // create data node in dir 1
            VOSURI uri = new VOSURI(URI.create(testDir.getURI().toASCIIString() + "/file-" + name));
            DataNode tn = new DataNode(uri);
            NodeUtil.setOwner(tn, up);
            Path tfile = doCreate(root, tn, up);
            Node tn2 = NodeUtil.get(root, uri);
            log.info("found: " + tn2);
            Assert.assertNotNull(tn2);
            Assert.assertTrue(tn2 instanceof DataNode);
            Assert.assertEquals(tn.getUri(), tn2.getUri());
            Assert.assertNotNull("lastModified", tn2.getPropertyValue(VOS.PROPERTY_URI_DATE));

            // copy the data node to dir2
            log.debug("Copying: " + tn2.getUri() + " to " + testDir2);
            NodeUtil.copy(root, tn.getUri(), testDir2, up);
            uri = new VOSURI(URI.create(testDir2.getURI().toASCIIString() + "/file-" + name));
            log.debug("Asserting: " + uri);
            Node copied = NodeUtil.get(root, uri);
            Assert.assertNotNull(copied);
            Assert.assertTrue(copied instanceof DataNode);
            Assert.assertEquals(uri, copied.getUri());
            Assert.assertNotNull("lastModified", copied.getPropertyValue(VOS.PROPERTY_URI_DATE));
            // check the original
            log.debug("Asserting: " + tn.getUri());
            Node orig = NodeUtil.get(root, tn.getUri());
            Assert.assertNotNull(orig);
            Assert.assertTrue(orig instanceof DataNode);
            Assert.assertEquals(tn.getUri(), orig.getUri());
            Assert.assertNotNull("lastModified", orig.getPropertyValue(VOS.PROPERTY_URI_DATE));

        } catch (Exception unexpected) {
            log.error("unexpected exception", unexpected);
            Assert.fail("unexpected exception: " + unexpected);
        }
    }

    @Test
    public void testCopyDirectory() {
        try {
            // top-level test dir
            String name = "testCopyDirectory-src-" + UUID.randomUUID().toString();
            VOSURI testDir = new VOSURI(URI.create("vos://canfar.net~cavern/" + name));
            Path root = FileSystems.getDefault().getPath(ROOT);
            UserPrincipalLookupService users = root.getFileSystem().getUserPrincipalLookupService();
            UserPrincipal up = users.lookupPrincipalByName(OWNER);
            ContainerNode cn = new ContainerNode(testDir);
            NodeUtil.setOwner(cn, up);
            Path dir = doCreate(root, cn, up);
            Assert.assertTrue("dir", Files.isDirectory(dir));

            // make copy target dir
            String name2 = "testCopyDirectory-dest-" + UUID.randomUUID().toString();
            VOSURI testDir2 = new VOSURI(URI.create("vos://canfar.net~cavern/" + name2));
            ContainerNode n2 = new ContainerNode(testDir2);
            NodeUtil.setOwner(n2, up);
            Path dir2 = doCreate(root, n2, up);
            Assert.assertTrue("dir2", Files.isDirectory(dir2));

            // TEST COPY POPULATED DIRECTORY

            // create data node in dir 1
            VOSURI top = new VOSURI(URI.create(testDir.getURI().toASCIIString() + "/c"));
            VOSURI[] vosuris = {
                top,
                new VOSURI(URI.create(testDir.getURI().toASCIIString() + "/c/c1")),
                new VOSURI(URI.create(testDir.getURI().toASCIIString() + "/c/c1/c1")),
                new VOSURI(URI.create(testDir.getURI().toASCIIString() + "/c/c1/c1/d1")),
                new VOSURI(URI.create(testDir.getURI().toASCIIString() + "/c/c1/c1/d2")),
                new VOSURI(URI.create(testDir.getURI().toASCIIString() + "/c/c1/c1/d3")),
                new VOSURI(URI.create(testDir.getURI().toASCIIString() + "/c/c1/d1")),
                new VOSURI(URI.create(testDir.getURI().toASCIIString() + "/c/c1/d2")),
                new VOSURI(URI.create(testDir.getURI().toASCIIString() + "/c/c2")),
                new VOSURI(URI.create(testDir.getURI().toASCIIString() + "/c/c2/c1")),
                new VOSURI(URI.create(testDir.getURI().toASCIIString() + "/c/c2/c1/c1")),
                new VOSURI(URI.create(testDir.getURI().toASCIIString() + "/c/c2/c1/c1/d1")),
                new VOSURI(URI.create(testDir.getURI().toASCIIString() + "/c/d1"))
            };

            Node n = null;
            for (VOSURI u : vosuris) {
                if (u.getName().startsWith("d")) {
                    n = new DataNode(u);
                } else {
                    n = new ContainerNode(u);
                }
                NodeUtil.setOwner(n, up);
                doCreate(root, n, up);
            }

            log.debug("Copying: " + top + " to " + testDir2);
            NodeUtil.copy(root, top, testDir2, up);

            // assert both copies are there
            for (VOSURI u : vosuris) {
                // ensure the original is there
                log.debug("Asserting: " + u);
                Node orig = NodeUtil.get(root, u);
                Assert.assertNotNull(orig);
                if (u.getName().startsWith("d")) {
                    Assert.assertTrue(orig instanceof DataNode);
                } else {
                    Assert.assertTrue(orig instanceof ContainerNode);
                }
                Assert.assertEquals(u, orig.getUri());
                Assert.assertNotNull("lastModified", orig.getPropertyValue(VOS.PROPERTY_URI_DATE));
                // assure the copy is there
                VOSURI c = new VOSURI(URI.create(u.toString().replace(
                        testDir.getURI().toASCIIString(), 
                        testDir2.getURI().toASCIIString() )));
                log.debug("Asserting: " + c);
                Node copy = NodeUtil.get(root, c);
                Assert.assertNotNull(copy);
                if (c.getName().startsWith("d")) {
                    Assert.assertTrue(orig instanceof DataNode);
                } else {
                    Assert.assertTrue(orig instanceof ContainerNode);
                }
                Assert.assertEquals(c, copy.getUri());
                Assert.assertNotNull("lastModified", copy.getPropertyValue(VOS.PROPERTY_URI_DATE));
            }

        } catch (Exception unexpected) {
            log.error("unexpected exception", unexpected);
            Assert.fail("unexpected exception: " + unexpected);
        }
    }

    @Test
    public void testCopyDirectoryWithLinks() {
        try {
            // top-level test dir
            String name = "testCopyDirectoryWithLinks-src-" + UUID.randomUUID().toString();
            VOSURI testDir = new VOSURI(URI.create("vos://canfar.net~cavern/" + name));
            Path root = FileSystems.getDefault().getPath(ROOT);
            UserPrincipalLookupService users = root.getFileSystem().getUserPrincipalLookupService();
            UserPrincipal up = users.lookupPrincipalByName(OWNER);
            ContainerNode cn = new ContainerNode(testDir);
            NodeUtil.setOwner(cn, up);
            Path dir = doCreate(root, cn, up);
            Assert.assertTrue("dir", Files.isDirectory(dir));

            // create data node in outside directory
            VOSURI uri = new VOSURI(URI.create(testDir.getURI().toASCIIString() + "/linkdir"));
            cn = new ContainerNode(uri);
            NodeUtil.setOwner(cn, up);
            doCreate(root, cn, up);
            VOSURI linkTarget = new VOSURI(URI.create(testDir.getURI().toASCIIString() + "/linkdir/file"));
            DataNode dn = new DataNode(linkTarget);
            NodeUtil.setOwner(dn, up);
            doCreate(root, dn, up);

            // make copy target dir
            String name2 = "testCopyDirectoryWithLinks-dest-" + UUID.randomUUID().toString();
            VOSURI testDir2 = new VOSURI(URI.create("vos://canfar.net~cavern/" + name2));
            ContainerNode n2 = new ContainerNode(testDir2);
            NodeUtil.setOwner(n2, up);
            Path dir2 = doCreate(root, n2, up);
            Assert.assertTrue("dir2", Files.isDirectory(dir2));

            // TEST COPY POPULATED DIRECTORY

            // create data node in dir 1
            VOSURI top = new VOSURI(URI.create(testDir.getURI().toASCIIString() + "/c"));
            VOSURI[] vosuris = {
                top,
                new VOSURI(URI.create(testDir.getURI().toASCIIString() + "/c/c1")),
                new VOSURI(URI.create(testDir.getURI().toASCIIString() + "/c/c1/c1")),
                new VOSURI(URI.create(testDir.getURI().toASCIIString() + "/c/c1/c1/d1")),
                new VOSURI(URI.create(testDir.getURI().toASCIIString() + "/c/c1/c1/d2")),
                new VOSURI(URI.create(testDir.getURI().toASCIIString() + "/c/c1/c1/d3")),
                new VOSURI(URI.create(testDir.getURI().toASCIIString() + "/c/c1/d1")),
                new VOSURI(URI.create(testDir.getURI().toASCIIString() + "/c/c1/l1")),
                new VOSURI(URI.create(testDir.getURI().toASCIIString() + "/c/c2")),
                new VOSURI(URI.create(testDir.getURI().toASCIIString() + "/c/c2/c1")),
                new VOSURI(URI.create(testDir.getURI().toASCIIString() + "/c/c2/c1/c1")),
                new VOSURI(URI.create(testDir.getURI().toASCIIString() + "/c/c2/c1/c1/d1")),
                new VOSURI(URI.create(testDir.getURI().toASCIIString() + "/c/l1"))
            };

            Node n = null;
            for (VOSURI u : vosuris) {
                if (u.getName().startsWith("d")) {
                    n = new DataNode(u);
                } else if (u.getName().startsWith("l")) {
                    n = new LinkNode(u, linkTarget.getURI());
                } else {
                    n = new ContainerNode(u);
                }
                NodeUtil.setOwner(n, up);
                doCreate(root, n, up);
            }

            log.debug("Copying: " + top + " to " + testDir2);
            NodeUtil.copy(root, top, testDir2, up);

            // assert both copies are there
            for (VOSURI u : vosuris) {
                // ensure the original is there
                log.debug("Asserting: " + u);
                Node orig = NodeUtil.get(root, u);
                Assert.assertNotNull(orig);
                if (u.getName().startsWith("d")) {
                    Assert.assertTrue(orig instanceof DataNode);
                } else if (u.getName().startsWith("l")) {
                    Assert.assertTrue(orig instanceof LinkNode);
                } else {
                    Assert.assertTrue(orig instanceof ContainerNode);
                }
                Assert.assertEquals(u, orig.getUri());
                Assert.assertNotNull("lastModified", orig.getPropertyValue(VOS.PROPERTY_URI_DATE));
                // assure the copy is there
                VOSURI c = new VOSURI(URI.create(u.toString().replace(
                        testDir.getURI().toASCIIString(), 
                        testDir2.getURI().toASCIIString() )));
                log.debug("Asserting: " + c);
                Node copy = NodeUtil.get(root, c);
                Assert.assertNotNull(copy);
                // link nodes turn into data nodes
                if (c.getName().startsWith("d") || c.getName().startsWith("l")) {
                    Assert.assertTrue(copy instanceof DataNode);
                } else {
                    Assert.assertTrue(copy instanceof ContainerNode);
                }
                Assert.assertEquals(c, copy.getUri());
                Assert.assertNotNull("lastModified", copy.getPropertyValue(VOS.PROPERTY_URI_DATE));
            }

        } catch (Exception unexpected) {
            log.error("unexpected exception", unexpected);
            Assert.fail("unexpected exception: " + unexpected);
        }
    }

}
