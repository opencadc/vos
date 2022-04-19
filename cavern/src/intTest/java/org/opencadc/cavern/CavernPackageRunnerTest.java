/*
************************************************************************
*******************  CANADIAN ASTRONOMY DATA CENTRE  *******************
**************  CENTRE CANADIEN DE DONNÉES ASTRONOMIQUES  **************
*
*  (c) 2022.                            (c) 2022.
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
*  $Revision: 5 $
*
************************************************************************
*/

package org.opencadc.cavern;

import ca.nrc.cadc.auth.SSLUtil;
import ca.nrc.cadc.net.HttpDownload;
import ca.nrc.cadc.reg.Standards;
import ca.nrc.cadc.util.FileUtil;
import ca.nrc.cadc.util.HexUtil;
import ca.nrc.cadc.util.Log4jInit;
import ca.nrc.cadc.util.StringUtil;
import ca.nrc.cadc.vos.Direction;
import ca.nrc.cadc.vos.Protocol;
import ca.nrc.cadc.vos.Transfer;
import ca.nrc.cadc.vos.VOS;
import ca.nrc.cadc.vos.VOSURI;
import ca.nrc.cadc.vos.View;
import ca.nrc.cadc.vos.client.ClientTransfer;
import ca.nrc.cadc.vos.client.VOSpaceClient;
import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.LineNumberReader;
import java.net.MalformedURLException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.security.PrivilegedExceptionAction;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import javax.security.auth.Subject;
import org.apache.commons.compress.archivers.ArchiveEntry;
import org.apache.commons.compress.archivers.ArchiveInputStream;
import org.apache.commons.compress.archivers.tar.TarArchiveEntry;
import org.apache.commons.compress.archivers.tar.TarArchiveInputStream;
import org.apache.commons.compress.archivers.zip.ZipArchiveEntry;
import org.apache.commons.compress.archivers.zip.ZipArchiveInputStream;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 * Integration tests for package generation CavernPackageRunnerTest
 *  (a dal.git/cadc-pkg-runner PackageRunner implementation.)
 *
 * @author jeevesh
 */
public class CavernPackageRunnerTest extends AbstractClientTransferTest {
    private static final Logger log = Logger.getLogger(CavernPackageRunnerTest.class);

    private static File SSL_CERT;
    private static String ZIP_MIME_TYPE = "application/zip";
    private static String TAR_MIME_TYPE = "application/x-tar";
    private static Subject s;

    private static VOSURI nodeURI;
    private static VOSpaceClient vos;

    public CavernPackageRunnerTest() { }

    static {
        Log4jInit.setLevel("ca.nrc.cadc.vos", Level.INFO);
        Log4jInit.setLevel("ca.nrc.cadc.vos.client", Level.INFO);
        Log4jInit.setLevel("ca.nrc.cadc.vospace", Level.INFO);
        Log4jInit.setLevel("org.opencadc.cavern", Level.INFO);
        Log4jInit.setLevel("org.opencadc.cavern.pkg", Level.INFO);
        Log4jInit.setLevel("org.opencadc.cavern.nodes", Level.INFO);
    }

    @BeforeClass
    public static void staticInit() throws Exception {
        SSL_CERT = FileUtil.getFileFromResource("x509_CADCAuthtest1.pem", CavernPackageRunnerTest.class);
        s = SSLUtil.createSubject(SSL_CERT);

        String uri ="vos://cadc.nrc.ca~arc/home/cadcauthtest1/do-not-delete/vospace-package-tests";
        log.debug("test dir base: " + uri);
        nodeURI = new VOSURI(new URI(uri));
        log.debug("Transfer node base URI: " + nodeURI);

        vos = new VOSpaceClient(nodeURI.getServiceURI());
    }

    // --------- Tar tests ----------

    @Test
    public void testTarPackageMultipleTargets() {
        log.info("testTarPackageMultipleTargets");

        try {
            List<String> expectedFiles = new ArrayList<>();
            // These files should be in both dev and production for cadcauthtest1 user.
            // absolute part of path will be pruned off in the package file
            expectedFiles.add("folderA/air_water.jpg");
            expectedFiles.add("folderA/folderAa/testProperties.properties");
            expectedFiles.add("All-Falcon-Chicks-2016.jpg");

            URI dataURI = new URI(nodeURI + "/folderA");
            URI dataURI1 = new URI(nodeURI + "/All-Falcon-Chicks-2016.jpg");

            List<URI> targetList = new ArrayList<>();
            targetList.add(dataURI);
            targetList.add(dataURI1);
            doTest(TAR_MIME_TYPE, targetList, "generated", expectedFiles);

        } catch(Exception unexpected) {
            log.error("unexpected exception", unexpected);
            Assert.fail("unexpected exception: " + unexpected);
        }
        log.info("testTarPackageMultipleTargets completed");
    }


    @Test
    public void testTarPackageSingleTarget() {
        log.info("testTarPackageSingleTarget");

        try {
            List<String> expectedFiles = new ArrayList<>();
            // These files should be in both dev and production for cadcauthtest1 user.
            // absolute part of path will be pruned off in the package file
            expectedFiles.add("folderA/air_water.jpg");
            expectedFiles.add("folderA/folderAa/testProperties.properties");

            URI dataURI = new URI(nodeURI + "/folderA");
            List<URI> targetList = new ArrayList<>();
            targetList.add(dataURI);

            doTest(TAR_MIME_TYPE, targetList, "folderA.tar", expectedFiles);

        } catch(Exception unexpected) {
            log.error("unexpected exception", unexpected);
            Assert.fail("unexpected exception: " + unexpected);
        }
        log.info("testTarPackageSingleTarget completed");
    }

    @Test
    public void testTarPackageLinkNodeTarget() {
        log.info("testTarPackageLinkNodeTarget");
        try {
            // This will have the LinkNode as the target (root of any potential depth
            // first navigation of the target tree
            URI dataURI = new URI(nodeURI + "/folderB/folderA");
            List<URI> targetList = new ArrayList<>();
            targetList.add(dataURI);

            List<String> expectedFiles = new ArrayList<>();
            // These files should be in both dev and production for cadcauthtest1 user.
            // Should just be the contents of folderA, path should reflect link node
            expectedFiles.add("folderA/air_water.jpg");
            expectedFiles.add("folderA/folderAa/testProperties.properties");

            doTest(TAR_MIME_TYPE, targetList, "folderA.tar", expectedFiles);

        } catch(Exception unexpected) {
            log.error("unexpected exception", unexpected);
            Assert.fail("unexpected exception: " + unexpected);
        }
        log.info("testTarPackageLinkNodeTarget completed");
    }

    @Test
    public void testTarPackageLinkNodeInTargetTree() {
        log.info("testTarPackageLinkNodeInTargetTree");
        try {
            // This will have the LinkNode as a child of the requested folder
            URI dataURI = new URI(nodeURI + "/folderB");
            List<URI> targetList = new ArrayList<>();
            targetList.add(dataURI);

            List<String> expectedFiles = new ArrayList<>();
            // These files should be in both dev and production for cadcauthtest1 user.
            expectedFiles.add("folderB/folderA/air_water.jpg");
            expectedFiles.add("folderB/folderA/folderAa/testProperties.properties");
            expectedFiles.add("folderB/testTextFile.rtf");

            doTest(TAR_MIME_TYPE, targetList, "folderB.tar", expectedFiles);

        } catch(Exception unexpected) {
            log.error("unexpected exception", unexpected);
            Assert.fail("unexpected exception: " + unexpected);
        }
        log.info("testTarPackageLinkNodeInTargetTree completed");
    }

    @Test
    public void testTarPackageNoPermission() {
        log.info("testTarPackageNoPermission");

        try {
            URI nodeURI = new URI("vos://cadc.nrc.ca~vault/CADCRegtest1/vospace-static-test/delegation");
            List<URI> targetList = new ArrayList<>();
            targetList.add(nodeURI);

            doTest(TAR_MIME_TYPE, targetList, "delegation.tar", null);

        } catch(Exception unexpected) {
            log.error("unexpected exception", unexpected);
            Assert.fail("unexpected exception: " + unexpected);
        }
        log.info("testTarPackageNoPermission completed");
    }

    @Test
    public void testTarPackageMultipleInvalidTarget() {
        log.info("testTarPackageMultipleInvalidTarget");
        try {
            URI dataURI = new URI(nodeURI + "/folderA");
            URI dataURI2 = new URI(nodeURI + "/badTarget");
            List<URI> targetList = new ArrayList<>();
            targetList.add(dataURI);
            targetList.add(dataURI2);

            List<String> expectedFiles = new ArrayList<>();
            // These files should be in both dev and production for cadcauthtest1 user.
            // badTarget shouldn't be in it, but the rest of the files should be
            expectedFiles.add("folderA/air_water.jpg");
            expectedFiles.add("folderA/folderAa/testProperties.properties");

            doTest(TAR_MIME_TYPE, targetList, "generated", expectedFiles);

        } catch(Exception unexpected) {
            log.error("unexpected exception", unexpected);
            Assert.fail("unexpected exception: " + unexpected);
        }
        log.info("testTarPackageMultipleInvalidTarget completed");
    }

    @Test
    public void testTarPackageInvalidTarget() {
        log.info("testTarPackageInvalidTarget");

        try {
            URI dataURI = new URI(nodeURI + "/badTarget");
            List<URI> targetList = new ArrayList<>();
            targetList.add(dataURI);

            // File is still created, it is empty
            doTest(TAR_MIME_TYPE, targetList, "badTarget.tar", null);

        } catch(Exception unexpected) {
            log.error("unexpected exception", unexpected);
            Assert.fail("unexpected exception: " + unexpected);
        }
        log.info("testTarPackageInvalidTarget completed");
    }

    @Test
    public void testTarPackageEmptyTarget() {
        log.info("testTarPackageEmptyTarget");

        try {
            URI dataURI = new URI(nodeURI + "/emptyFolder");
            List<URI> targetList = new ArrayList<>();
            targetList.add(dataURI);

            doTest(TAR_MIME_TYPE, targetList, "emptyFolder.tar", null);

        } catch(Exception unexpected) {
            log.error("unexpected exception", unexpected);
            Assert.fail("unexpected exception: " + unexpected);
        }
        log.info("testTarPackageEmptyTarget completed");
    }


    //---------- Zip tests ---------
    @Test
    public void testZipPackageMultipleTargets() {
        log.info("testZipPackageMultipleTargets");

        try {
            List<String> expectedFiles = new ArrayList<>();
            // These files should be in both dev and production for cadcauthtest1 user.
            // absolute part of path will be pruned off in the package file
            expectedFiles.add("/folderA/air_water.jpg");
            expectedFiles.add("/folderA/folderAa/testProperties.properties");
            expectedFiles.add("/All-Falcon-Chicks-2016.jpg");

            URI dataURI1 = new URI(nodeURI + "/folderA");
            URI dataURI2 = new URI(nodeURI + "/All-Falcon-Chicks-2016.jpg");
            List<URI> targetList = new ArrayList<>();
            targetList.add(dataURI1);
            targetList.add(dataURI2);

            doTest(ZIP_MIME_TYPE, targetList, "generated", expectedFiles);

        } catch(Exception unexpected) {
            log.error("unexpected exception", unexpected);
            Assert.fail("unexpected exception: " + unexpected);
        }
        log.info("testZipPackageMultipleTargets completed");
    }

    @Test
    public void testZipPackageSingleTarget() {
        log.info("testZipPackageSingleTarget");
        try {
            URI dataURI = new URI(nodeURI + "/folderA");
            List<URI> targetList = new ArrayList<>();
            targetList.add(dataURI);

            List<String> expectedFiles = new ArrayList<>();
            // These files should be in both dev and production for cadcauthtest1 user.
            expectedFiles.add("/folderA/air_water.jpg");
            expectedFiles.add("/folderA/folderAa/testProperties.properties");

            doTest(ZIP_MIME_TYPE, targetList, "folderA.zip", expectedFiles);

        } catch(Exception unexpected) {
            log.error("unexpected exception", unexpected);
            Assert.fail("unexpected exception: " + unexpected);
        }
        log.info("testZipPackageSingleTarget completed");
    }

    @Test
    public void testZipPackageLinkNodeTarget() {
        log.info("testZipPackageLinkNodeTarget");
        try {
            // This will have the LinkNode as the target (root of any potential depth
            // first navigation of the target tree
            URI dataURI = new URI(nodeURI + "/folderB/folderA");
            List<URI> targetList = new ArrayList<>();
            targetList.add(dataURI);

            List<String> expectedFiles = new ArrayList<>();
            // These files should be in both dev and production for cadcauthtest1 user.
            // Should just be the contents of folderA, path should reflect link node
            expectedFiles.add("/folderA/air_water.jpg");
            expectedFiles.add("/folderA/folderAa/testProperties.properties");

            doTest(ZIP_MIME_TYPE, targetList, "folderA.zip", expectedFiles);

        } catch(Exception unexpected) {
            log.error("unexpected exception", unexpected);
            Assert.fail("unexpected exception: " + unexpected);
        }
        log.info("testZipPackageSingleTarget completed");
    }

    @Test
    public void testZipPackageLinkNodeInTargetTree() {
        log.info("testZipPackageLinkNodeTarget");
        try {
            // This will have the LinkNode as a child of the requested folder
            URI dataURI = new URI(nodeURI + "/folderB");
            List<URI> targetList = new ArrayList<>();
            targetList.add(dataURI);

            List<String> expectedFiles = new ArrayList<>();
            // These files should be in both dev and production for cadcauthtest1 user.
            expectedFiles.add("/folderB/folderA/air_water.jpg");
            expectedFiles.add("/folderB/folderA/folderAa/testProperties.properties");
            expectedFiles.add("/folderB/testTextFile.rtf");

            doTest(ZIP_MIME_TYPE, targetList, "folderB.zip", expectedFiles);

        } catch(Exception unexpected) {
            log.error("unexpected exception", unexpected);
            Assert.fail("unexpected exception: " + unexpected);
        }
        log.info("testZipPackageSingleTarget completed");
    }

    @Test
    public void testZipPackageNoPermission() {
        log.info("testZipPackageNoPermission");

        try {
            URI partPermissions = new URI("vos://cadc.nrc.ca~vault/CADCRegtest1/vospace-static-test/delegation");
            List<URI> targetList = new ArrayList<>();
            targetList.add(partPermissions);

            doTest(ZIP_MIME_TYPE, targetList, "delegation.zip", null);

        } catch(Exception unexpected) {
            log.error("unexpected exception", unexpected);
            Assert.fail("unexpected exception: " + unexpected);
        }
        log.info("testZipPackageNoPermission completed");
    }

    @Test
    public void testZipPackageInvalidTarget() {
        log.info("testZipPackageInvalidTarget");

        try {
            URI dataURI = new URI(nodeURI + "/badTarget");
            List<URI> targetList = new ArrayList<>();
            targetList.add(dataURI);

            // File is still created, it is empty
            doTest(ZIP_MIME_TYPE, targetList, "badTarget.zip", null);

        } catch(Exception unexpected) {
            log.error("unexpected exception", unexpected);
            Assert.fail("unexpected exception: " + unexpected);
        }
        log.info("testZipPackageInvalidTarget completed");
    }

    @Test
    public void testZipPackageMultipleInvalidTarget() {
        log.info("testZipPackageMultipleInvalidTarget");
        try {
            URI dataURI = new URI(nodeURI + "/folderA");
            URI dataURI2 = new URI(nodeURI + "/badTarget");
            List<URI> targetList = new ArrayList<>();
            targetList.add(dataURI);
            targetList.add(dataURI2);

            List<String> expectedFiles = new ArrayList<>();
            // These files should be in both dev and production for cadcauthtest1 user.
            // badTarget shouldn't be in it, but the rest of the files should be
            expectedFiles.add("/folderA/air_water.jpg");
            expectedFiles.add("/folderA/folderAa/testProperties.properties");

            doTest(ZIP_MIME_TYPE, targetList, "generated", expectedFiles);

        } catch(Exception unexpected) {
            log.error("unexpected exception", unexpected);
            Assert.fail("unexpected exception: " + unexpected);
        }
        log.info("testZipPackageMultipleInvalidTarget completed");
    }

    @Test
    public void testZipPackageEmptyTarget() {
        log.info("testZipPackageEmptyTarget");

        try {
            URI dataURI = new URI(nodeURI + "/emptyFolder");
            List<URI> targetList = new ArrayList<>();
            targetList.add(dataURI);

            doTest(ZIP_MIME_TYPE, targetList, "emptyFolder.zip", null);

        } catch(Exception unexpected) {
            log.error("unexpected exception", unexpected);
            Assert.fail("unexpected exception: " + unexpected);
        }
        log.info("testZipPackageEmptyTarget completed");
    }

    // ------------- Apache Commons Compress library-related  functions for reading package contents

    private void  checkZipContent(File zipFile, List<String> expectedFiles)
        throws IOException, NoSuchAlgorithmException {

        FileInputStream fis = new FileInputStream(zipFile);
        ZipArchiveInputStream zipInput = new ZipArchiveInputStream(fis);

        if (expectedFiles != null) {
            int count = 0;
            for (int i = 0; i < expectedFiles.size(); i++) {
                // Read the stream to get the next entry
                // tarInput.getNextTarEntry() is called in here
                Content c = getZipEntry(zipInput);
                log.debug("found file in zip package: " + c.name + " " + c.contentMD5);

                for (String s : expectedFiles) {
                    log.debug("next expected:" + s);
                    if (s.equals(c.name)) {
                        log.debug("found it! " + c.name);
                        count++;
                        break;
                    }
                }
            }

            Assert.assertTrue("not all expectedFiles found. " + count + ": "
                + expectedFiles.size(), expectedFiles.size() == count);
        }

        // ensure zipInput is empty
        ArchiveEntry nullEntry = zipInput.getNextZipEntry();
        Assert.assertNull(nullEntry);
    }

    private void checkTarContent(File tarFile, List<String> expectedFiles)
        throws IOException, NoSuchAlgorithmException {

        FileInputStream fis = new FileInputStream(tarFile);
        TarArchiveInputStream tarInput = new TarArchiveInputStream(fis);

        if (expectedFiles != null) {
            int count = 0;

            for (int i = 0; i < expectedFiles.size(); i++) {
                // Read the stream to get the next entry
                // tarInput.getNextTarEntry() is called in here
                Content c = getEntry(tarInput);
                log.debug("found file in tar package: " + c.name + " " + c.contentMD5);

                for (String s : expectedFiles) {
                    log.debug("next expected:" + s);
                    if (s.equals(c.name)) {
                        count++;
                        break;
                    }
                }
            }

            Assert.assertTrue("not all expectedFiles found." + count + ": "
                + expectedFiles.size(), expectedFiles.size() == count);
        }

        // ensure tarInput is empty
        ArchiveEntry nullEntry = tarInput.getNextTarEntry();
        Assert.assertNull(nullEntry);
    }

    class Content {
        String name;
        String contentMD5;
        Map<String,String> md5map = new HashMap<String,String>();
    }

    private Content getEntry(TarArchiveInputStream ais) throws IOException, NoSuchAlgorithmException {
        TarArchiveEntry entry = ais.getNextTarEntry();
        log.debug("got an entry: " + entry.getName());
        return getArchiveEntry(entry, ais);
    }

    private Content getZipEntry(ZipArchiveInputStream ais) throws IOException, NoSuchAlgorithmException {
        ZipArchiveEntry entry = ais.getNextZipEntry();
        if (entry != null) {
            log.debug("got an entry: " + entry.getName());
            return getArchiveEntry(entry, ais);
        } else {
            return null;
        }
    }

    private Content getArchiveEntry(ArchiveEntry entry, ArchiveInputStream ais)
        throws IOException, NoSuchAlgorithmException {
        Content ret = new Content();
        ret.name = entry.getName();
        log.debug("archive entry name: " + ret.name);

        if (ret.name.endsWith("README")) {
            byte[] buf = new byte[(int) entry.getSize()];
            ais.read(buf);
            ByteArrayInputStream bis = new ByteArrayInputStream(buf);
            LineNumberReader r = new LineNumberReader(new InputStreamReader(bis));
            String line = r.readLine();
            while ( line != null) {
                String[] tokens = line.split(" ");
                // status [md5 filename url]
                String status = tokens[0];

                if ("OK".equals(status)) {
                    String fname = tokens[1];
                    String md5 = tokens[2];
                    ret.md5map.put(fname, md5);
                } else {
                    throw new RuntimeException("archive content failure: " + line);
                }
                line = r.readLine();
            }
        } else {
            MessageDigest md5 = MessageDigest.getInstance("MD5");
            byte[] buf = new byte[8192];
            int n = ais.read(buf);

            while (n > 0) {
                md5.update(buf, 0, n);
                n = ais.read(buf); }
            byte[] md5sum = md5.digest();
            ret.contentMD5 = HexUtil.toHex(md5sum);
        }

        return ret;
    }

    // ------------- common functions for package testing -------------



    private String getExpectedFilename(String expectedName, String jobID, String mimeType) {
        String eName = expectedName;

        if (expectedName.equals("generated")) {
            eName = "cadc-download-" + jobID;

            if (mimeType.equals(ZIP_MIME_TYPE)) {
                eName = eName + ".zip";

            } else if (mimeType.equals(TAR_MIME_TYPE)) {
                eName = eName + ".tar";
            }
            log.debug("generated expected package file name: " + eName);

        }
        return eName;
    }

    private void doTest(String mimeType, List<URI> targetList, String expectedFilename,
        List<String> expectedFiles) throws Exception {

        Subject.doAs(s, new PrivilegedExceptionAction<Object>() {
            public Object run() throws Exception {
                log.debug("doTest target list: " + targetList.toString());

                Transfer t = mkTransfer(targetList, mimeType, null);

                // Running this here, because when it was moved into the doTest function
                // permissions/subject scope didn't appear to work
                ClientTransfer trans = Subject.doAs(s, new CreateTransferAction(vos, t));

                log.debug("jobURL: " + trans.getJobURL().toExternalForm());

                List<Protocol> plist = trans.getTransfer().getProtocols();
                Assert.assertNotNull(plist);
                log.debug("found: " + plist.size() + " protocols");

                int num = 0;
                for (Protocol p : plist) {
                    String endpoint = p.getEndpoint();
                    log.debug("protocol endpoint: " + p + " : " + endpoint);
                    Assert.assertNotNull(endpoint);
                    // check the endpoints here that they're /vault/pkg/*
                    Assert.assertTrue("not a pkg endpoint: " + endpoint, endpoint.contains("/pkg"));

                    String jobID = getJobIDFromEndpoint(endpoint);
                    log.debug("jobID: " + jobID);
                    File packageFile = getPackageFile(endpoint);

                    Assert.assertTrue("package file exists", packageFile.exists());

                    String expectName = getExpectedFilename(expectedFilename, jobID, mimeType);

                    log.debug("expected package file name: " + expectedFilename);
                    log.debug("actual package file name: " + packageFile.getName());

                    Assert.assertEquals(expectName, packageFile.getName());
                    log.debug("package length:" + packageFile.length());

                    if (mimeType.equals(ZIP_MIME_TYPE)) {
                        checkZipContent(packageFile, expectedFiles);
                    } else if (mimeType.equals(TAR_MIME_TYPE)) {
                        checkTarContent(packageFile, expectedFiles);
                    } else {
                        throw new Exception("unrecognized mime type");
                    }

                    cleanup(packageFile);

                    num++;
                    Assert.assertTrue("Expected " + plist.size() + " endpoints. Found " + num, num == plist.size());
                }

                return null;
            }
        });
    }

    private void cleanup(File f) {
        if (f.exists()) {
            f.delete();
        }
    }

    private String getJobIDFromEndpoint(String fullendpoint) throws Exception {
        String jobID = "";

        // Will be a /vault/pkg/<jobid>/run path.
        URL endpoint = new URL(fullendpoint);
        String[] pathParts = endpoint.getPath().split("/");
        String path = endpoint.getPath();
        if (pathParts.length == 5) {
            // format should be https://<host>/arc/pkg/<jobid>/run
            // pathParts[0] is empty because path starts with '/'
            log.debug("pathparts: " + pathParts.length + ": " + pathParts[0]
                + ": " + pathParts[1]
                + ": " + pathParts[2]
                + ": " + pathParts[3]
                + ": " + pathParts[4]);
            jobID = pathParts[3];
            log.debug("jobid from path:" + jobID);
        } else {
            log.debug("incorrect number of path parts found (5):" + pathParts.length + " " + pathParts.toString());
            throw new Exception("incorrect structure for /vault/pkg endpoint");
        }
        return jobID;
    }

    /**
     * Pull the file at the endpoint from
     * @param endpoint
     * @return
     * @throws MalformedURLException
     */
    private File getPackageFile(String endpoint) throws MalformedURLException {
        File tmp = new File(System.getProperty("java.io.tmpdir"));

        log.debug("getPackageFile: " + endpoint + " -> " + tmp.getAbsolutePath());
        URL serviceURL = new URL(endpoint);
        HttpDownload get = new HttpDownload(serviceURL, tmp);
        get.setOverwrite(true); // file exists from create above
        get.run();
        Assert.assertNull("throwable", get.getThrowable());
        Assert.assertEquals(200, get.getResponseCode());
        File ret = get.getFile();
        Assert.assertTrue("file exists", ret.exists());
        log.debug("returned file absolute path: " + ret.getAbsolutePath());
        return ret;
    }

    private Transfer mkTransfer(List<URI> targetList, String mimeType, List<Protocol> pList) throws URISyntaxException {
        List<Protocol> proto = new ArrayList<Protocol>();

        if (pList == null) {
            proto.add(new Protocol(VOS.PROTOCOL_HTTPS_GET));
        } else {
            proto.addAll(pList);
        }

        log.debug("testTarPackageMultipleTargets: " + targetList.toString());

        Transfer t = new Transfer(Direction.pullFromVoSpace);
        t.getTargets().addAll(targetList);
        View packageView = new View(Standards.PKG_10);
        packageView.getParameters().add(new View.Parameter(new URI(VOS.PROPERTY_URI_FORMAT), mimeType));
        t.setView(packageView);
        t.getProtocols().addAll(proto);

        return t;
    }
}
