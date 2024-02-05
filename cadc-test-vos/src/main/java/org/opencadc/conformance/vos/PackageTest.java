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

import ca.nrc.cadc.auth.AuthenticationUtil;
import ca.nrc.cadc.auth.RunnableAction;
import ca.nrc.cadc.net.FileContent;
import ca.nrc.cadc.net.HttpDownload;
import ca.nrc.cadc.net.HttpGet;
import ca.nrc.cadc.net.HttpPost;
import ca.nrc.cadc.reg.Standards;
import ca.nrc.cadc.util.Log4jInit;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.io.StringWriter;
import java.net.URI;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import javax.security.auth.Subject;
import org.apache.commons.compress.archivers.ArchiveEntry;
import org.apache.commons.compress.archivers.ArchiveException;
import org.apache.commons.compress.archivers.ArchiveInputStream;
import org.apache.commons.compress.archivers.ArchiveStreamFactory;
import org.apache.commons.compress.utils.IOUtils;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.junit.Assert;
import org.junit.Test;
import org.opencadc.vospace.ContainerNode;
import org.opencadc.vospace.LinkNode;
import org.opencadc.vospace.VOS;
import org.opencadc.vospace.VOSURI;
import org.opencadc.vospace.View;
import org.opencadc.vospace.transfer.Direction;
import org.opencadc.vospace.transfer.Protocol;
import org.opencadc.vospace.transfer.Transfer;
import org.opencadc.vospace.transfer.TransferParsingException;
import org.opencadc.vospace.transfer.TransferReader;
import org.opencadc.vospace.transfer.TransferWriter;

public class PackageTest extends VOSTest {

    private static final Logger log = Logger.getLogger(PackageTest.class);

    static {
        Log4jInit.setLevel("org.opencadc.conformance.vos", Level.INFO);
    }

    private static final String ZIP_CONTENT_TYPE = "application/zip";
    private static final String TAR_CONTENT_TYPE = "application/x-tar";

    protected PackageTest(URI resourceID, File testCert) {
        super(resourceID, testCert);
    }


    /**
     * returns empty archive file, should return archive with empty directory?
     */
    @Test
    public void permissionDeniedTest() {
        try {
            // Root container node
            String root = "permission-denied-root";
            String file = "permission-denied-file.txt";

            URL rootURL = getNodeURL(nodesServiceURL, root);
            URL fileURL = getNodeURL(nodesServiceURL, file);

            // cleanup
            delete(fileURL, false);
            delete(rootURL, false);

            // upload the folders and files as auth subject
            String content = "permission-denied-file-content";
            VOSURI nodeURI = putContainerNode(root, rootURL);
            VOSURI fileURI = putDataNode(file, content);

            // package targets to download
            List<URI> targets = new ArrayList<>();
            targets.add(nodeURI.getURI());

            List<String> expected = new ArrayList<>();

            Subject anonSubject = AuthenticationUtil.getAnonSubject();
            File tarPkg = downloadPackage(targets, TAR_CONTENT_TYPE, anonSubject);
            log.debug("tar file: " + tarPkg.getAbsolutePath());
            Assert.assertNotNull(tarPkg);
            Assert.assertTrue(tarPkg.canRead());
            Assert.assertEquals(1024L, tarPkg.length());

            // cleanup
            delete(fileURL, false);
            delete(rootURL, false);

        } catch (Exception e) {
            log.error("Unexpected error", e);
            Assert.fail("Unexpected error: " + e);
        }
    }

    @Test
    public void targetNotFoundTest() {
        try {
            String name = "target-not-found-node";
            VOSURI nodeURI = getVOSURI(name);

            // package targets to download
            List<URI> targets = new ArrayList<>();
            targets.add(nodeURI.getURI());

            // should return empty archive if node not found.
            // check tar file exists.
            File tarPkg = downloadPackage(targets, TAR_CONTENT_TYPE, authSubject);
            log.debug("tar file: " + tarPkg.getAbsolutePath());
            Assert.assertNotNull(tarPkg);
            Assert.assertTrue(tarPkg.canRead());
            Assert.assertEquals(1024L, tarPkg.length());

        } catch (Exception e) {
            log.error("Unexpected error", e);
            Assert.fail("Unexpected error: " + e);
        }
    }

    @Test
    public void emptyTargetTest() {
        try {
            String name = "empty-target-node";
            URL nodeURL = getNodeURL(nodesServiceURL, name);

            // cleanup
            delete(nodeURL, false);

            // upload the folders and files
            VOSURI nodeURI = putContainerNode(name, nodeURL);

            // package targets to download
            List<URI> targets = new ArrayList<>();
            targets.add(nodeURI.getURI());

            // tar does not create an empty directory for an empty tar archive
            // check tar file exists.
            File tarPkg = downloadPackage(targets, TAR_CONTENT_TYPE, authSubject);
            log.debug("tar file: " + tarPkg.getAbsolutePath());
            Assert.assertNotNull(tarPkg);
            Assert.assertTrue(tarPkg.canRead());

            // cleanup
            delete(nodeURL, false);

        } catch (Exception e) {
            log.error("Unexpected error", e);
            Assert.fail("Unexpected error: " + e);
        }
    }

    @Test
    public void singleFileTargetTest() {
        try {
            String name = "single-target-node.txt";
            String content = "single-target-node-content";
            URL nodeURL = getNodeURL(nodesServiceURL, name);

            // cleanup
            delete(nodeURL, false);

            // upload the folders and files
            VOSURI nodeURI = putDataNode(name, content);

            // package targets to download
            List<URI> targets = new ArrayList<>();
            targets.add(nodeURI.getURI());

            // expected files in download
            List<String> expected = new ArrayList<>();
            expected.add(name);

            doTest(targets, expected, TAR_CONTENT_TYPE);
            //doTest(targets, expected, ZIP_CONTENT_TYPE);

            // cleanup
            delete(nodeURL, false);

        } catch (Exception e) {
            log.error("Unexpected error", e);
            Assert.fail("Unexpected error: " + e);
        }
    }

    @Test
    public void multipleTargetTest() {
        try {
            //  /root/
            //  /root/target-1/
            //  /root/target-1/file1
            //  /root/target-2/
            //  /root/target-2/file2

            String root = "multi-target-root-node";
            String target1 = root + "/target-1";
            String file1 = target1 + "/target-1-file.txt";
            String target2 = root + "/target-2";
            String file2 = target2 + "/target-2-file.txt";

            URL rootURL = getNodeURL(nodesServiceURL, root);
            URL target1URL = getNodeURL(nodesServiceURL, target1);
            URL file1URL = getNodeURL(nodesServiceURL, file1);
            URL target2URL = getNodeURL(nodesServiceURL, target2);
            URL file2URL = getNodeURL(nodesServiceURL, file2);

            URL[] nodes = new URL[] {file2URL, target2URL, file1URL, target1URL, rootURL};

            // cleanup
            delete(nodes);

            // upload the folders and files
            String content1 = "target-1-file-content";
            String content2 = "target-2-file-content";

            VOSURI rootURI = putContainerNode(root, rootURL);
            VOSURI target1URI = putContainerNode(target1, target1URL);
            VOSURI file1URI = putDataNode(file1, content1, authSubject);
            VOSURI target2URI = putContainerNode(target2, target2URL);
            VOSURI file2URI = putDataNode(file2, content2, authSubject);

            // package targets to download
            List<URI> targets = new ArrayList<>();
            targets.add(target1URI.getURI());
            targets.add(target2URI.getURI());

            // expected files in download
            List<String> expected = new ArrayList<>();
            expected.add(file1);
            expected.add(file2);

            doTest(targets, expected, TAR_CONTENT_TYPE);
            // doTest(targets, expected, ZIP_CONTENT_TYPE);

            // cleanup
            delete(nodes);

        } catch (Exception e) {
            log.error("Unexpected error", e);
            Assert.fail("Unexpected error: " + e);
        }
    }

    @Test
    public void fullTest() {
        try {
            //   /root-folder/
            //   /root-folder/file-1.txt
            //   /root-folder/folder-1/
            //   /root-folder/folder-2/
            //   /root-folder/folder-2/file-2.txt
            //   /root-folder/folder-2/file-3.txt
            //   /root-folder/folder-2/folder-3/
            //   /root-folder/folder-2/folder-3/link-1.txt

            // nodes paths
            String root = "full-root-folder";
            String file1 = root + "/file-1.txt";
            String folder1 = root + "/folder-1";
            String folder2 = root + "/folder-2";
            String file2 = folder2 + "/file-2.txt";
            String file3 = folder2 + "/file-3.txt";
            String folder3 = folder2 + "/folder-3";
            String link1 = folder3 + "/link-1.txt";

            // node URL's
            URL rootURL = getNodeURL(nodesServiceURL, root);
            URL file1URL = getNodeURL(nodesServiceURL, file1);
            URL folder1URL = getNodeURL(nodesServiceURL, folder1);
            URL folder2URL = getNodeURL(nodesServiceURL, folder2);
            URL file2URL = getNodeURL(nodesServiceURL, file2);
            URL file3URL = getNodeURL(nodesServiceURL, file3);
            URL folder3URL = getNodeURL(nodesServiceURL, folder3);
            URL link1URL = getNodeURL(nodesServiceURL, link1);

            URL[] nodes = new URL[] {link1URL, folder3URL, file3URL, file2URL, folder2URL, folder1URL, file1URL, rootURL};

            // cleanup
            delete(nodes);

            // upload the folders and files
            String file1Content = "file-1-content";
            String file2Content = "file-2-content";
            String file3Content = "file-3-content";
            URI linkTarget = URI.create("vos://opencadc.org~cavern/link-node");

            VOSURI rootURI = putContainerNode(root, rootURL);
            VOSURI file1URI = putDataNode(file1, file1Content);
            VOSURI folder1URI = putContainerNode(folder1, folder1URL);
            VOSURI folder2URI = putContainerNode(folder2, folder2URL);
            VOSURI file2URI = putDataNode(file2, file2Content);
            VOSURI file3URI = putDataNode(file3, file3Content);
            VOSURI folder3URI = putContainerNode(folder3, folder3URL);
            VOSURI link1URI = putLinkNode(link1, link1URL, linkTarget);

            // package targets to download
            List<URI> targets = new ArrayList<>();
            targets.add(rootURI.getURI());

            // expected files in download
            List<String> expected = new ArrayList<>();
            expected.add(file1);
            expected.add(file2);
            expected.add(file3);

            doTest(targets, expected, TAR_CONTENT_TYPE);
            // doTest(targets, expected, ZIP_CONTENT_TYPE);

            // cleanup
            delete(nodes);

        } catch (Exception e) {
            log.error("Unexpected error", e);
            Assert.fail("Unexpected error: " + e);
        }
    }

    private VOSURI putContainerNode(String path, URL url) throws IOException {
        ContainerNode node = new ContainerNode(path);
        VOSURI uri = getVOSURI(path);
        put(url, uri, node);
        return uri;
    }

    private VOSURI putDataNode(String path, String content)
            throws IOException, TransferParsingException {
        return uploadFile(path, content, authSubject);
    }

    private VOSURI putDataNode(String path, String content, Subject testSubject)
            throws IOException, TransferParsingException {
        return uploadFile(path, content, testSubject);
    }

    private VOSURI putLinkNode(String path, URL url, URI target)
            throws IOException {
        LinkNode node = new LinkNode(path, target);
        VOSURI uri = getVOSURI(path);
        put(url, uri, node);
        return uri;
    }

    private void doTest(List<URI> targets, List<String> expected, String contentType)
            throws Exception {

        // download the package
        File pkg = downloadPackage(targets, contentType, authSubject);
        Assert.assertNotNull(pkg);
        log.debug("archive file: " + pkg.getAbsolutePath());

        // extract the package
        File extracted = extractPackage(pkg, contentType);
        Assert.assertNotNull(extracted);
        log.debug("extracted file: " + extracted.getAbsolutePath());

        // verify package files
        verifyPackage(expected, extracted);

        // file cleanup
        deleteFile(pkg);
        deleteFile(extracted);
    }

    private VOSURI uploadFile(String filename, String content, Subject testSubject)
            throws IOException, TransferParsingException {

        // Create a Transfer
        VOSURI nodeURI = getVOSURI(filename);
        Transfer transfer = new Transfer(nodeURI.getURI(), Direction.pushToVoSpace);
        transfer.version = VOS.VOSPACE_21;
        Protocol protocol = new Protocol(VOS.PROTOCOL_HTTPS_PUT);
        protocol.setSecurityMethod(Standards.SECURITY_METHOD_CERT);
        transfer.getProtocols().add(protocol);

        // Get the transfer document
        TransferWriter writer = new TransferWriter();
        StringWriter sw = new StringWriter();
        writer.write(transfer, sw);
        log.debug("uploadFile transfer XML: " + sw);

        // POST the transfer document
        FileContent fileContent = new FileContent(sw.toString().getBytes(), VOSTest.XML_CONTENT_TYPE);
        URL transferURL = getNodeURL(synctransServiceURL, filename);
        log.debug("transfer URL: " + transferURL);
        HttpPost post = new HttpPost(synctransServiceURL, fileContent, false);
        Subject.doAs(testSubject, new RunnableAction(post));
        Assert.assertEquals("expected POST response code = 303", 303, post.getResponseCode());
        Assert.assertNull("expected PUT throwable == null", post.getThrowable());

        // Get the transfer details
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        HttpGet get = new HttpGet(post.getRedirectURL(), out);
        log.debug("GET: " + post.getRedirectURL());
        Subject.doAs(testSubject, new RunnableAction(get));
        log.debug("GET responseCode: " + get.getResponseCode());
        Assert.assertEquals("expected GET response code = 200", 200, get.getResponseCode());
        Assert.assertNull("expected GET throwable == null", get.getThrowable());
        Assert.assertTrue("expected GET Content-Type starting with " + VOSTest.XML_CONTENT_TYPE,
                get.getContentType().startsWith(VOSTest.XML_CONTENT_TYPE));

        // Get the endpoint from the transfer details
        log.debug("transfer details XML: " + out);
        TransferReader transferReader = new TransferReader();
        Transfer details = transferReader.read(out.toString(), "vos");
        Assert.assertEquals("expected transfer direction = " + Direction.pushToVoSpace,
                Direction.pushToVoSpace, details.getDirection());
        Assert.assertFalse("expected > 0 endpoints", details.getProtocols().isEmpty());
        URL endpoint = new URL(details.getProtocols().get(0).getEndpoint());

        // PUT a file to the endpoint
        log.debug("PUT: " + endpoint);
        ByteArrayInputStream is = new ByteArrayInputStream(content.getBytes());
        put(endpoint, is, VOSTest.TEXT_CONTENT_TYPE);

        return nodeURI;
    }
    
    private File downloadPackage(List<URI> targets, String contentType, Subject testSubject)
            throws Exception {

        // Create a Transfer
        Transfer transfer = new Transfer(Direction.pullFromVoSpace);
        transfer.getTargets().addAll(targets);
        View packageView = new View(Standards.PKG_10);
        packageView.getParameters().add(new View.Parameter(VOS.PROPERTY_URI_FORMAT, contentType));
        transfer.setView(packageView);
        Protocol protocol = new Protocol(VOS.PROTOCOL_HTTPS_GET);
        protocol.setSecurityMethod(Standards.SECURITY_METHOD_CERT);
        transfer.getProtocols().add(protocol);

        // Write a transfer document
        TransferWriter transferWriter = new TransferWriter();
        StringWriter sw = new StringWriter();
        transferWriter.write(transfer, sw);
        log.debug("transfer XML: " + sw);

        // POST the transfer to synctrans
        FileContent fileContent = new FileContent(sw.toString().getBytes(), VOSTest.XML_CONTENT_TYPE);
        HttpPost post = new HttpPost(synctransServiceURL, fileContent, false);
        Subject.doAs(testSubject, new RunnableAction(post));
        Assert.assertEquals("expected POST response code = 303",303, post.getResponseCode());
        Assert.assertNull("expected POST throwable == null", post.getThrowable());
        URL redirctURL = post.getRedirectURL();

        // Download the package
        File tmp = new File(System.getProperty("java.io.tmpdir"), "package-test-" + UUID.randomUUID());
        if (!tmp.mkdirs()) {
            throw new IOException("unable to create tmp directory: " + tmp.getAbsolutePath());
        }
        HttpDownload download = new HttpDownload(redirctURL, tmp);
        download.setOverwrite(true);
        log.debug("GET: " + redirctURL);
        Subject.doAs(testSubject, new RunnableAction(download));
        log.debug("GET responseCode: " + download.getResponseCode());
        Assert.assertEquals("expected GET response code = 200", 200, download.getResponseCode());
        Assert.assertNull("expected GET throwable == null", download.getThrowable());


        Assert.assertEquals(String.format("expected GET Content-Type %s, found %s ",
                        contentType, download.getContentType()), contentType, download.getContentType());
        File pkg = download.getFile();
        Assert.assertNotNull("download file is null", pkg);
        Assert.assertTrue("package file not found", pkg.exists());
        log.debug("package file: " + pkg.getAbsolutePath());

        String archiveType = contentType.equals(ZIP_CONTENT_TYPE) ? ArchiveStreamFactory.ZIP : ArchiveStreamFactory.TAR;
        Assert.assertTrue(String.format("expected file extension %s, actual %s", archiveType, pkg.getName()),
                pkg.getName().endsWith(archiveType));

        return pkg;
    }

    private File extractPackage(File packageFile, String contentType)
            throws ArchiveException, IOException {

        FileInputStream inputStream = new FileInputStream(packageFile);
        String archiveType = contentType.equals(ZIP_CONTENT_TYPE) ? ArchiveStreamFactory.ZIP : ArchiveStreamFactory.TAR;
        log.debug("archive type: " + archiveType);

        String packageDir = packageFile.getName().replace(".", "-");
        File extractDir = new File(packageFile.getParent(), packageDir);

        ArchiveStreamFactory archiveStreamFactory = new ArchiveStreamFactory();
        ArchiveInputStream archiveInputStream = archiveStreamFactory.createArchiveInputStream(
                archiveType, inputStream);
        ArchiveEntry entry;
        while ((entry = archiveInputStream.getNextEntry()) != null) {
            if (!archiveInputStream.canReadEntryData(entry)) {
                log.debug("unable to read archive entry: " + entry.getName());
                continue;
            }

            File file = new File(extractDir, entry.getName());
            log.debug("archive entry path:" + file.getAbsolutePath());

            if (entry.isDirectory()) {
                if (!file.isDirectory() && !file.mkdirs()) {
                    throw new IOException("failed to create entry directory " + file.getAbsolutePath());
                }
            } else {
                File parent = file.getParentFile();
                if (!parent.isDirectory() && !parent.mkdirs()) {
                    log.debug("archive entry parent is not directory");
                    throw new IOException("failed to create entry parent directory " + parent.getAbsolutePath());
                }
                try (OutputStream outputStream = Files.newOutputStream(file.toPath())) {
                    IOUtils.copy(archiveInputStream, outputStream);
                }
            }
        }
        return extractDir;
    }

    private void verifyPackage(List<String> expectedFiles, File packageDir) throws IOException {

        // List of extracted files with path
        List<Path> extractedFiles = listFiles(packageDir.getAbsolutePath());

        // compare expected to extracted
        int count = 0;
        for (String expected : expectedFiles) {
            log.debug("expected file: " + expected);
            for (Path extracted : extractedFiles) {
                log.debug("extracted file: " + extracted.getFileName());
                if (extracted.toString().endsWith(expected)) {
                    log.debug("matched: " + expected);
                    count++;
                    break;
                }
            }
        }
        Assert.assertEquals("", expectedFiles.size(), count);
    }

    private List<Path> listFiles(String rootDir) throws IOException {
        try (Stream<Path> paths = Files.walk(Paths.get(rootDir))) {
            return paths.filter(Files::isRegularFile).collect(Collectors.toList());
        }
    }

    private void delete(URL[] nodes) {
        for (URL node : nodes) {
            delete(node, false);
        }
    }

    private void deleteFile(File file) {
        boolean deleted = file.delete();
        log.debug(String.format("%s deleted: %s", file.getName(), deleted));
    }

}
