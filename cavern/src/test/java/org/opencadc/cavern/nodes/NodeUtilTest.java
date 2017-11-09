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
import ca.nrc.cadc.vos.VOS;
import ca.nrc.cadc.vos.VOSURI;
import java.io.IOException;
import java.net.URI;
import java.nio.file.FileSystems;
import java.nio.file.Files;
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
    }
    
    static final String ROOT = System.getProperty("java.io.tmpdir") + "/cavern-tests";
    
    static final String OWNER = System.getProperty("user.name");
    
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
        Assert.assertTrue("exists", Files.exists(actual));
        Assert.assertEquals("name", n.getName(), actual.toFile().getName());
        UserPrincipal owner = Files.getOwner(actual);
        Assert.assertNotNull(owner);
        Assert.assertEquals(up, owner);
        
        return actual;
    }
    
    @Test
    public void testGetRoot() {
        try {
            VOSURI uri = new VOSURI(URI.create("vos://canfar.net~cavern"));
            Path root = FileSystems.getDefault().getPath(ROOT);
            
            Node rootNode = NodeUtil.get(root, uri);
            Assert.assertNotNull(rootNode);
            ContainerNode cn = (ContainerNode) rootNode;
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
            String name = UUID.randomUUID().toString();
            VOSURI uri = new VOSURI(URI.create("vos://canfar.net~cavern/" + name));
                    
            Path root = FileSystems.getDefault().getPath(ROOT);
            UserPrincipalLookupService users = root.getFileSystem().getUserPrincipalLookupService();
            UserPrincipal up = users.lookupPrincipalByName(OWNER);
            
            ContainerNode n = new ContainerNode(uri);
            NodeUtil.setOwner(n, up);
            
            Path dir = doCreate(root, n, up);
            Assert.assertTrue("dir", Files.isDirectory(dir));
            
            Node nn = NodeUtil.get(root, uri);
            log.info("found: " + nn);
            Assert.assertNotNull(nn);
            Assert.assertTrue(nn instanceof ContainerNode);
            Assert.assertEquals(n.getUri(), nn.getUri());
            Assert.assertNotNull("lastModified", nn.getPropertyValue(VOS.PROPERTY_URI_CREATION_DATE));
            
            NodeUtil.delete(root, uri);
            Assert.assertFalse("deleted", Files.exists(dir));
        } catch (Exception unexpected) {
            log.error("unexpected exception", unexpected);
            Assert.fail("unexpected exception: " + unexpected);
        }
    }
    
    @Test
    public void testCreateFile() {
        try {
            String name = UUID.randomUUID().toString();
            VOSURI uri = new VOSURI(URI.create("vos://canfar.net~cavern/" + name));
                    
            Path root = FileSystems.getDefault().getPath(ROOT);
            UserPrincipalLookupService users = root.getFileSystem().getUserPrincipalLookupService();
            UserPrincipal up = users.lookupPrincipalByName(OWNER);
            
            DataNode n = new DataNode(uri);
            NodeUtil.setOwner(n, up);
            
            Path file = doCreate(root, n, up);
            Assert.assertTrue("file", Files.isRegularFile(file));
            
            Node nn = NodeUtil.get(root, uri);
            log.info("found: " + nn);
            Assert.assertNotNull(nn);
            Assert.assertTrue(nn instanceof DataNode);
            Assert.assertEquals(n.getUri(), nn.getUri());
            
            NodeUtil.delete(root, uri);
            Assert.assertFalse("deleted", Files.exists(file));
            
        } catch (Exception unexpected) {
            log.error("unexpected exception", unexpected);
            Assert.fail("unexpected exception: " + unexpected);
        }
    }
    
    @Test
    public void testCreateLink() {
        try {
            String name = UUID.randomUUID().toString();
            VOSURI uri = new VOSURI(URI.create("vos://canfar.net~cavern/" + name));
            VOSURI luri = new VOSURI(URI.create("vos://canfar.net~cavern/link-to-" + name));
                    
            Path root = FileSystems.getDefault().getPath(ROOT);
            UserPrincipalLookupService users = root.getFileSystem().getUserPrincipalLookupService();
            UserPrincipal up = users.lookupPrincipalByName(OWNER);
            
            Node n = new DataNode(uri);
            NodeUtil.setOwner(n, up);
            
            final Path file = doCreate(root, n, up);
            
            LinkNode ln = new LinkNode(luri, uri.getURI());
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
            // TODO: test that the link is relative to fs-mount view will be consistent
            
            NodeUtil.delete(root, luri);
            Assert.assertFalse("deleted", Files.exists(link));
            Assert.assertTrue("target not deleted", Files.exists(file));
            
            NodeUtil.delete(root, uri);
            Assert.assertFalse("deleted", Files.exists(file));
            
        } catch (Exception unexpected) {
            log.error("unexpected exception", unexpected);
            Assert.fail("unexpected exception: " + unexpected);
        }
    }
    
    @Test
    public void testCreateTree() {
        
        try {
            Path root = FileSystems.getDefault().getPath(ROOT);
            UserPrincipalLookupService users = root.getFileSystem().getUserPrincipalLookupService();
            UserPrincipal up = users.lookupPrincipalByName(OWNER);
                 
            String name = UUID.randomUUID().toString();
            VOSURI testURI = new VOSURI(URI.create("vos://canfar.net~cavern/" + name));
            ContainerNode n = new ContainerNode(testURI);
            NodeUtil.setOwner(n, up);
            Path dir = doCreate(root, n, up);
            Assert.assertTrue("dir", Files.isDirectory(dir));
            
            for (int i = 0; i<3; i++) {
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
                Assert.assertNotNull("lastModified", nn.getPropertyValue(VOS.PROPERTY_URI_CREATION_DATE));
            }

            Iterator<Node> iter = NodeUtil.list(root, n, null, null);
            while (iter.hasNext()) {
                Node nn = iter.next();
                log.info("list: " + nn);
            }
            
            NodeUtil.delete(root, testURI);
            Assert.assertFalse("deleted", Files.exists(dir));
        } catch (Exception unexpected) {
            log.error("unexpected exception", unexpected);
            Assert.fail("unexpected exception: " + unexpected);
        } 
        
    }
}
