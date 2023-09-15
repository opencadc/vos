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

package org.opencadc.cavern.files;

import ca.nrc.cadc.auth.HttpPrincipal;
import ca.nrc.cadc.util.Log4jInit;
import ca.nrc.cadc.util.RsaSignatureGenerator;
import ca.nrc.cadc.uws.Job;
import ca.nrc.cadc.vos.Direction;
import ca.nrc.cadc.vos.Protocol;
import ca.nrc.cadc.vos.Transfer;
import ca.nrc.cadc.vos.VOS;
import ca.nrc.cadc.vos.VOSURI;
import ca.nrc.cadc.vos.View;
import java.io.File;
import java.net.MalformedURLException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.nio.file.FileSystem;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.PosixFilePermission;
import java.nio.file.attribute.PosixFilePermissions;
import java.security.Principal;
import java.security.PrivilegedExceptionAction;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import javax.security.auth.Subject;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;

public class CavernURLGeneratorTest
{

    private static final Logger log = Logger.getLogger(CavernURLGeneratorTest.class);

    static {
        Log4jInit.setLevel("org.opencadc.cavern", Level.DEBUG);
    }

    static final String ROOT = System.getProperty("java.io.tmpdir") + "/cavern-tests";
    static final String OWNER = System.getProperty("user.name");
    static String TEST_DIR = "dir-" + UUID.randomUUID().toString();
    static String TEST_FILE = "file-" + UUID.randomUUID().toString();
    
    static String baseURI;

    File pubFile, privFile;

    public CavernURLGeneratorTest() {
    }

    @BeforeClass
    public static void setup() {
        try {
            FileSystem fs = FileSystems.getDefault();
            Path dir = fs.getPath(ROOT, TEST_DIR);
            Path node = fs.getPath(ROOT, TEST_DIR + "/" + TEST_FILE);
            
            Set<PosixFilePermission> filePerms = new HashSet<>();
            filePerms.add(PosixFilePermission.OWNER_READ);
            filePerms.add(PosixFilePermission.OWNER_WRITE);
            Set<PosixFilePermission> dirPerms = new HashSet<>();
            dirPerms.addAll(filePerms);
            dirPerms.add(PosixFilePermission.OWNER_EXECUTE);

            Files.createDirectories(dir, PosixFilePermissions.asFileAttribute(dirPerms));
            Files.createFile(node, PosixFilePermissions.asFileAttribute(filePerms));
            
            baseURI = System.getProperty("ca.nrc.cadc.vos.server.vosUriBase");
            if (baseURI == null) {
                throw new RuntimeException("TEST SETUP: missing system property ca.nrc.cadc.vos.server.vosUriBase");
            }

        } catch (Exception unexpected) {
            log.error("unexpected exception", unexpected);
            Assert.fail("unexpected exception: " + unexpected);
        }
    }

    @AfterClass
    public static void cleanup() {
        if (true) { 
            return;
        }
        try {
            FileSystem fs = FileSystems.getDefault();
            Path dir = fs.getPath(ROOT, TEST_DIR);
            Path node = fs.getPath(ROOT, TEST_DIR + "/" + TEST_FILE);
            Files.delete(node);
            Files.delete(dir);
        } catch (Exception unexpected) {
            log.error("unexpected exception", unexpected);
            Assert.fail("unexpected exception: " + unexpected);
        }
    }

    @Before
    public void initKeys() throws Exception
    {
        String keysDir = "build/resources/test";
        File pub = new File(keysDir + "/CavernPub.key");
        File priv = new File(keysDir + "/CavernPriv.key");
        RsaSignatureGenerator.genKeyPair(pub, priv, 1024);
        privFile = new File(keysDir, RsaSignatureGenerator.PRIV_KEY_FILE_NAME);
        pubFile = new File(keysDir, RsaSignatureGenerator.PUB_KEY_FILE_NAME);
        log.debug("Created pub key: " + pubFile.getAbsolutePath());
    }

    @After
    public void cleanupKeys() throws Exception
    {
        pubFile.delete();
        privFile.delete();
    }
    
    // TODO: acl specific codes will be moved to a library, enable the test after
    @Ignore
    @Test
    public void testNegotiateMount() {
        try {
            Set<Principal> p = new HashSet<Principal>();
            // unit test: this will resolve to a posix user
            p.add(new HttpPrincipal(System.getProperty("user.name")));
            Subject s = new Subject(false, p, new HashSet(), new HashSet()); 
            
            final VOSURI nodeURI = new VOSURI(baseURI + "/" + TEST_DIR);
            List<Protocol> protos = new ArrayList<>();
            protos.add(new Protocol(VOS.PROTOCOL_SSHFS));
            final Transfer trans = new Transfer(nodeURI.getURI(), Direction.BIDIRECTIONAL);
            trans.getProtocols().addAll(protos);
            final View view = null;
            final Job job = null;
            
            Protocol mnt = Subject.doAs(s, new PrivilegedExceptionAction<Protocol>() {
                @Override
                public Protocol run() throws Exception {
                    TestTransferGenerator urlGen = new TestTransferGenerator(ROOT);
                    List<Protocol> result = urlGen.getEndpoints(nodeURI, trans, view, job, null);
                    return result.get(0);
                }
                
            });
            log.info("protocol: " + mnt);
            Assert.assertNotNull(mnt);
            Assert.assertEquals("mount protocol", VOS.PROTOCOL_SSHFS, mnt.getUri());
            Assert.assertNotNull("mount endpoint", mnt.getEndpoint());
            
        } catch (Exception unexpected) {
            log.error("unexpected exception", unexpected);
            Assert.fail("unexpected exception: " + unexpected);
        }
    }

    private VOSURI getTargetVOSURI(String path) throws URISyntaxException {
        int firstSlashIndex = path.indexOf("/");
        String pathStr = path.substring(firstSlashIndex + 1);
        log.debug("path: " + pathStr);
        String targetURIStr = baseURI.toString() + "/" + pathStr;
        log.debug("target URI from path: " + targetURIStr);
        return new VOSURI(new URI(targetURIStr));

    }

    // TODO: acl specific codes will be moved to a library, enable the test after
    @Ignore
    @Test
    public void testRoundTripSuccess() {
        try {

            TestTransferGenerator urlGen = new TestTransferGenerator(ROOT);
            VOSURI nodeURI = new VOSURI(baseURI + "/" + TEST_DIR + "/" + TEST_FILE);
            List<Protocol> protos = new ArrayList<>();
            protos.add(new Protocol(VOS.PROTOCOL_HTTPS_GET));
            final Transfer trans = new Transfer(nodeURI.getURI(), Direction.pullFromVoSpace);
            trans.getProtocols().addAll(protos);
            View view = null;
            Job job = null;
            List<Protocol> result = urlGen.getEndpoints(nodeURI, trans, view, job, null);
            Protocol p = result.get(0);
            Assert.assertNotNull(p);
            String suri = p.getEndpoint();
            log.debug("Transfer URI: " + suri);
            Assert.assertNotNull(suri);
            VOSURI transferURI = new VOSURI(new URI(suri));
            Assert.assertTrue(transferURI.getPath().endsWith("/" + TEST_FILE));

            String path = transferURI.getPath();
            log.debug("Path: " + path);
            String[] parts = path.split("/");
            String token = parts[3];

            VOSURI targetURI = getTargetVOSURI(path);

            // Will throw exception if is invalid
            VOSURI returnURI = urlGen.validateToken(token, targetURI, Direction.pullFromVoSpace);
            Assert.assertEquals("URI was altered? ", returnURI, targetURI);

        } catch (Exception unexpected) {
            log.error("unexpected exception", unexpected);
            Assert.fail("unexpected exception: " + unexpected);
        }
    }

    // TODO: acl specific codes will be moved to a library, enable the test after
    @Ignore
    @Test
    public void testWrongDirectionPull() {
        try {

            TestTransferGenerator urlGen = new TestTransferGenerator(ROOT);
            VOSURI nodeURI = new VOSURI(baseURI + "/" + TEST_DIR + "/" + TEST_FILE);
            List<Protocol> protos = new ArrayList<>();
            protos.add(new Protocol(VOS.PROTOCOL_HTTPS_GET));
            final Transfer trans = new Transfer(nodeURI.getURI(), Direction.pullFromVoSpace);
            trans.getProtocols().addAll(protos);
            View view = null;
            Job job = null;
            List<Protocol> result = urlGen.getEndpoints(nodeURI, trans, view, job, null);
            Protocol p = result.get(0);
            Assert.assertNotNull(p);
            String suri = p.getEndpoint();
            log.debug("Transfer URI: " + suri);
            Assert.assertNotNull(suri);
            URI transferURI = new URI(suri);

            String path = transferURI.getPath();
            String[] parts = path.split("/");
            String token = parts[3];
            try {
                urlGen.validateToken(token, nodeURI, Direction.pushToVoSpace);
                Assert.fail();
            } catch (IllegalArgumentException e) {
                Assert.assertTrue(e.getMessage().contains("Wrong direction"));
            }

        } catch (Exception unexpected) {
            log.error("unexpected exception", unexpected);
            Assert.fail("unexpected exception: " + unexpected);
        }
    }

    // TODO: acl specific codes will be moved to a library, enable the test after
    @Ignore
    @Test
    public void testWrongDirectionPush() {
        try {

            TestTransferGenerator urlGen = new TestTransferGenerator(ROOT);
            VOSURI nodeURI = new VOSURI(baseURI + "/" + TEST_DIR + "/" + TEST_FILE);
            List<Protocol> protos = new ArrayList<>();
            protos.add(new Protocol(VOS.PROTOCOL_HTTPS_PUT));
            final Transfer trans = new Transfer(nodeURI.getURI(), Direction.pushToVoSpace);
            trans.getProtocols().addAll(protos);
            View view = null;
            Job job = null;
            List<Protocol> result = urlGen.getEndpoints(nodeURI, trans, view, job, null);
            Protocol p = result.get(0);
            Assert.assertNotNull(p);
            String suri = p.getEndpoint();
            log.debug("Transfer URI: " + suri);
            Assert.assertNotNull(suri);
            URI transferURI = new URI(suri);

            String path = transferURI.getPath();
            String[] parts = path.split("/");
            String token = parts[3];
            try {
                urlGen.validateToken(token, nodeURI, Direction.pullFromVoSpace);
                Assert.fail();
            } catch (IllegalArgumentException e) {
                Assert.assertTrue(e.getMessage().contains("Wrong direction"));
            }

        } catch (Exception unexpected) {
            log.error("unexpected exception", unexpected);
            Assert.fail("unexpected exception: " + unexpected);
        }
    }


    @Test
    public void testInvalidToken() {
        try {
            // CavernURLGenerator.validateToken will try to Base64 decode the token passed
            // in before passing it to TokenTool.
            TestTransferGenerator urlGen = new TestTransferGenerator(ROOT);
            VOSURI nodeURI = new VOSURI(baseURI + "/" + TEST_DIR + "/" + TEST_FILE);
            String badToken = "something clearly not base 64";

            try {
                urlGen.validateToken(badToken, nodeURI, Direction.pushToVoSpace);
                Assert.fail();
            } catch (IllegalArgumentException e) {
                // expected
            }

        } catch (Exception unexpected) {
            log.error("unexpected exception", unexpected);
            Assert.fail("unexpected exception: " + unexpected);
        }
    }

    class TestTransferGenerator extends CavernURLGenerator {

        public TestTransferGenerator(String root) {
            super(root);
        }

        @Override
        List<URL> getBaseURLs(VOSURI target, URI securityMethod, String scheme) {
            List<URL> list = new ArrayList<URL>(1);
            try {
                list.add(new URL("http://example.com/service/path"));
            } catch (MalformedURLException e) {
                throw new RuntimeException("failure", e);
            }
            return list;
        }
    }
}
