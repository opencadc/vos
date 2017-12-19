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

package org.opencadc.cavern.files;

import ca.nrc.cadc.util.Log4jInit;
import ca.nrc.cadc.util.RsaSignatureGenerator;
import ca.nrc.cadc.uws.Job;
import ca.nrc.cadc.vos.Direction;
import ca.nrc.cadc.vos.Protocol;
import ca.nrc.cadc.vos.VOS;
import ca.nrc.cadc.vos.VOSURI;
import ca.nrc.cadc.vos.View;

import java.io.File;
import java.net.MalformedURLException;
import java.net.URI;
import java.net.URL;
import java.nio.file.FileSystem;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.FileAttribute;
import java.nio.file.attribute.PosixFilePermission;
import java.nio.file.attribute.PosixFilePermissions;
import java.security.AccessControlException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.UUID;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.xerces.impl.dv.util.Base64;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

public class CavernURLGeneratorTest
{

    private static final Logger log = Logger.getLogger(CavernURLGeneratorTest.class);

    static {
        Log4jInit.setLevel("org.opencadc.cavern", Level.DEBUG);
    }

    static final String ROOT = System.getProperty("java.io.tmpdir") + "/cavern-tests";
    static final String OWNER = System.getProperty("user.name");
    static String TEST_DIR = UUID.randomUUID().toString();
    static String TEST_FILE = UUID.randomUUID().toString();

    File pubFile, privFile;

    public CavernURLGeneratorTest() {
    }

    @BeforeClass
    public static void setup() {
        try {
            FileSystem fs = FileSystems.getDefault();
            Path dir = fs.getPath(ROOT, TEST_DIR);
            Path node = fs.getPath(ROOT, TEST_DIR + "/" + TEST_FILE);
            Set<PosixFilePermission> perms = new HashSet<PosixFilePermission>();
            FileAttribute<Set<PosixFilePermission>> fp = PosixFilePermissions.asFileAttribute(perms);
            Files.createDirectories(dir);
            Files.createFile(node, fp);
        } catch (Exception unexpected) {
            log.error("unexpected exception", unexpected);
            Assert.fail("unexpected exception: " + unexpected);
        }
    }

    @AfterClass
    public static void cleanup() {
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
        String keysDir = "src/test/resources";
        RsaSignatureGenerator.genKeyPair(keysDir);
        privFile = new File(keysDir, RsaSignatureGenerator.PRIV_KEY_FILE_NAME);
        pubFile = new File(keysDir, RsaSignatureGenerator.PUB_KEY_FILE_NAME);
        log.debug("Created pub key: " + pubFile.getAbsolutePath());
        log.debug("Current classpath: " + System.getProperty("java.class.path"));
    }

    @After
    public void cleanupKeys() throws Exception
    {
        pubFile.delete();
        privFile.delete();
    }

    @Test
    public void testRoundTripSuccess() {
        try {

            TestURLGen urlGen = new TestURLGen(ROOT);
            VOSURI nodeURI = new VOSURI("vos://canfar.net~cavern/" + TEST_DIR + "/" + TEST_FILE);
            Protocol protocol = new Protocol(VOS.PROTOCOL_HTTP_GET);
            View view = null;
            Job job = null;
            List<URL> urls = urlGen.getURLs(nodeURI, protocol, view, job, null);
            URL transferURL = urls.get(0);
            log.debug("Transfer URL: " + transferURL);
            Assert.assertTrue(transferURL.getPath().endsWith("/" + TEST_FILE));

            String path = transferURL.getPath();
            log.debug("Path: " + path);
            String[] parts = path.split("/");
            String sig = parts[4];
            String meta = parts[3];
            VOSURI retURI = urlGen.getNodeURI(meta, sig, Direction.pullFromVoSpace);
            Assert.assertEquals(nodeURI, retURI);

        } catch (Exception unexpected) {
            log.error("unexpected exception", unexpected);
            Assert.fail("unexpected exception: " + unexpected);
        }
    }

    @Test
    public void testWrongDirection() {
        try {

            TestURLGen urlGen = new TestURLGen(ROOT);
            VOSURI nodeURI = new VOSURI("vos://canfar.net~cavern/" + TEST_DIR + "/" + TEST_FILE);
            Protocol protocol = new Protocol(VOS.PROTOCOL_HTTP_GET);
            View view = null;
            Job job = null;
            List<URL> urls = urlGen.getURLs(nodeURI, protocol, view, job, null);
            URL transferURL = urls.get(0);
            String path = transferURL.getPath();
            String[] parts = path.split("/");
            String sig = parts[4];
            String meta = parts[3];
            try {
                urlGen.getNodeURI(meta, sig, Direction.pushToVoSpace);
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
    public void testInvalidSignature() {
        try {

            TestURLGen urlGen = new TestURLGen(ROOT);
            VOSURI nodeURI = new VOSURI("vos://canfar.net~cavern/" + TEST_DIR + "/" + TEST_FILE);
            Protocol protocol = new Protocol(VOS.PROTOCOL_HTTP_GET);
            View view = null;
            Job job = null;
            List<URL> urls = urlGen.getURLs(nodeURI, protocol, view, job, null);
            URL transferURL = urls.get(0);
            String path = transferURL.getPath();
            String[] parts = path.split("/");
            //String sig = parts[4];
            String meta = parts[3];
            try {
                urlGen.getNodeURI(meta, "12345", Direction.pushToVoSpace);
                Assert.fail();
            } catch (AccessControlException e) {
                // expected
            }

        } catch (Exception unexpected) {
            log.error("unexpected exception", unexpected);
            Assert.fail("unexpected exception: " + unexpected);
        }
    }

    @Test
    public void testMetaTampered() {
        try {

            TestURLGen urlGen = new TestURLGen(ROOT);
            VOSURI nodeURI = new VOSURI("vos://canfar.net~cavern/" + TEST_DIR + "/" + TEST_FILE);
            Protocol protocol = new Protocol(VOS.PROTOCOL_HTTP_GET);
            View view = null;
            Job job = null;
            List<URL> urls = urlGen.getURLs(nodeURI, protocol, view, job, null);
            URL transferURL = urls.get(0);
            log.debug("Transfer URL: " + transferURL);
            Assert.assertTrue(transferURL.getPath().endsWith("/" + TEST_FILE));

            String path = transferURL.getPath();
            log.debug("Path: " + path);
            String[] parts = path.split("/");
            String sig = parts[4];
            //String meta = parts[3];
            VOSURI altURI = new VOSURI("vos://canfar.net~cavern/" + TEST_DIR + "/fakeFile");
            String meta = new String(Base64.encode(("node=" + altURI.toString() + "&dir=pullFromVoSpace").getBytes()));
            try {
                VOSURI retURI = urlGen.getNodeURI(meta, sig, Direction.pullFromVoSpace);
                Assert.fail();
            } catch (AccessControlException e) {
                // expected
            }

        } catch (Exception unexpected) {
            log.error("unexpected exception", unexpected);
            Assert.fail("unexpected exception: " + unexpected);
        }
    }

    @Test
    public void testBase64URL() {
        String[] testStrings = {
            "abcde",
            "ab//de",
            "ab/de",
            "ab++///+de"
        };
        for (String s : testStrings) {
            log.debug("testing: " + s);
            Assert.assertEquals(CavernURLGenerator.base64URLDecode(CavernURLGenerator.base64URLEncode(s)), s);
        }
    }

    class TestURLGen extends CavernURLGenerator {

        public TestURLGen(String root) {
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
