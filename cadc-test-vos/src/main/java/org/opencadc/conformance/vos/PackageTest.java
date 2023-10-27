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
import ca.nrc.cadc.net.HttpGet;
import ca.nrc.cadc.net.HttpPost;
import ca.nrc.cadc.net.OutputStreamWrapper;
import ca.nrc.cadc.reg.Standards;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.StringWriter;
import java.net.MalformedURLException;
import java.net.URI;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.security.AccessControlException;
import java.security.PrivilegedActionException;
import java.security.PrivilegedExceptionAction;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import javax.security.auth.Subject;
import org.apache.commons.compress.archivers.ArchiveEntry;
import org.apache.commons.compress.archivers.ArchiveException;
import org.apache.commons.compress.archivers.ArchiveInputStream;
import org.apache.commons.compress.archivers.ArchiveStreamFactory;
import org.apache.commons.compress.utils.IOUtils;
import org.apache.log4j.Logger;
import org.junit.Assert;
import org.junit.Test;
import org.opencadc.vospace.ContainerNode;
import org.opencadc.vospace.DataNode;
import org.opencadc.vospace.VOS;
import org.opencadc.vospace.VOSURI;
import org.opencadc.vospace.View;
import org.opencadc.vospace.client.ClientTransfer;
import org.opencadc.vospace.client.VOSpaceClient;
import org.opencadc.vospace.transfer.Direction;
import org.opencadc.vospace.transfer.Protocol;
import org.opencadc.vospace.transfer.Transfer;
import org.opencadc.vospace.transfer.TransferParsingException;
import org.opencadc.vospace.transfer.TransferReader;
import org.opencadc.vospace.transfer.TransferWriter;

public class PackageTest extends VOSTest {
    private static final Logger log = Logger.getLogger(PackageTest.class);

    private static final String ZIP_CONTENT_TYPE = "application/zip";
    private static final String TAR_CONTENT_TYPE = "application/x-tar";
//    private static Subject s;

//    private static VOSURI nodeURI;

    protected PackageTest(URI resourceID, File testCert) {
        super(resourceID, testCert);
    }

    @Test
    public void tarPermissionDeniedTest () {
        try {
            permissionDeniedTest(TAR_CONTENT_TYPE);
        } catch (Exception e) {
            log.error("Unexpected error", e);
            Assert.fail("Unexpected error: " + e);
        }
    }

    @Test
    public void zipPermissionDeniedTest () {
        try {
            permissionDeniedTest(ZIP_CONTENT_TYPE);
        } catch (Exception e) {
            log.error("Unexpected error", e);
            Assert.fail("Unexpected error: " + e);
        }
    }

    @Test
    public void tarTargetNotFoundTest () {
        try {
            targetNotFoundTest(TAR_CONTENT_TYPE);
        } catch (Exception e) {
            log.error("Unexpected error", e);
            Assert.fail("Unexpected error: " + e);
        }
    }

    @Test
    public void zipTargetNotFoundTest () {
        try {
            targetNotFoundTest(ZIP_CONTENT_TYPE);
        } catch (Exception e) {
            log.error("Unexpected error", e);
            Assert.fail("Unexpected error: " + e);
        }
    }

    @Test
    public void tarEmptyTargetTest() {
        try {
            emptyTargetTest(TAR_CONTENT_TYPE);
        } catch (Exception e) {
            log.error("Unexpected error", e);
            Assert.fail("Unexpected error: " + e);
        }
    }

    @Test
    public void zipEmptyTargetTest() {
        try {
            emptyTargetTest(ZIP_CONTENT_TYPE);
        } catch (Exception e) {
            log.error("Unexpected error", e);
            Assert.fail("Unexpected error: " + e);
        }
    }

    @Test
    public void singleTargetTest() {
        try {

        } catch (Exception e) {
            log.error("Unexpected error", e);
            Assert.fail("Unexpected error: " + e);
        }
    }

    @Test
    public void multipleTargetTest() {
        try {

        } catch (Exception e) {
            log.error("Unexpected error", e);
            Assert.fail("Unexpected error: " + e);
        }
    }


    public void permissionDeniedTest(String contentType) throws Exception {
        // Root container node created by auth subject
        String name = "permission-denied-node";
        URL nodeURL = getNodeURL(nodesServiceURL, name);

        // cleanup
        delete(nodeURL, false);

        VOSURI nodeURI = getVOSURI(name);
        ContainerNode node = new ContainerNode(name);
        put(nodeURL, nodeURI, node);
        log.debug(String.format("PUT %s URL: %s", name, nodeURL));

        List<URI> targets = new ArrayList<>();
        targets.add(nodeURI.getURI());

        // Package request using anon subject
        Subject anonSubject = AuthenticationUtil.getAnonSubject();

        // download and extract the package
        try {
            downloadPackage(targets, name, null, anonSubject, contentType);
            Assert.fail("should have thrown exception for permission denied");
        } catch (AccessControlException e) {
            log.debug("expected exception: " + e.getMessage());
        }

        // cleanup
        delete(nodeURL, false);
    }

    public void targetNotFoundTest(String contentType) throws Exception {
        String name = "target-not-found-node";
        VOSURI nodeURI = getVOSURI(name);

        List<URI> targets = new ArrayList<>();
        targets.add(nodeURI.getURI());

        // download and extract the package
        try {
            downloadPackage(targets, name, null, authSubject, contentType);
            Assert.fail("should have thrown exception for target not found");
        } catch (AccessControlException e) {
            log.debug("expected exception: " + e.getMessage());
        }
    }

    public void emptyTargetTest(String contentType) throws Exception {

        String name = "empty-target-node";
        URL nodeURL = getNodeURL(nodesServiceURL, name);

        // cleanup
        delete(nodeURL, false);

        VOSURI nodeURI = getVOSURI(name);
        ContainerNode node = new ContainerNode(name);
        put(nodeURL, nodeURI, node);
        log.debug(String.format("PUT %s URL: %s", name, nodeURL));

        List<URI> targets = new ArrayList<>();
        targets.add(nodeURI.getURI());

        // download and extract the package
        Path packageRoot = downloadPackage(targets, name, null, authSubject, contentType);

        File[] files = packageRoot.toFile().listFiles();
        Assert.assertNotNull(files);
        Assert.assertEquals("expected single file", 1, files.length);
        Assert.assertTrue("expected directory", files[0].isDirectory());
        Assert.assertEquals("", name, files[0].getName());

        // cleanup
        delete(nodeURL, false);
    }

    public void singleTargetTest(String contentType) throws Exception {

        String name = "single-target-node";
        URL nodeURL = getNodeURL(nodesServiceURL, name);

        // cleanup
        delete(nodeURL, false);

        // PUT the node
        VOSURI nodeURI = getVOSURI(name);
        ContainerNode node = new ContainerNode(name);
        put(nodeURL, nodeURI, node);
        log.debug(String.format("PUT %s URL: %s", name, nodeURL));

        List<URI> targets = new ArrayList<>();
        targets.add(nodeURI.getURI());

        // Upload

        // download and extract the package
        Path packageRoot = downloadPackage(targets, name, null, authSubject, contentType);

        File[] files = packageRoot.toFile().listFiles();
        Assert.assertNotNull(files);
        Assert.assertEquals("expected single file", 1, files.length);
        Assert.assertTrue("expected directory", files[0].isDirectory());
        Assert.assertEquals("", name, files[0].getName());

        // cleanup
        delete(nodeURL, false);
    }

    private void uploadFile(String filename, String content)
            throws MalformedURLException, IOException, TransferParsingException {
        // Put a file (DataNode)
        URL nodeURL = getNodeURL(nodesServiceURL, filename);
        VOSURI nodeURI = getVOSURI(filename);
        log.debug("upload node URL: " + nodeURL);

        // Create a Transfer
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
        Subject.doAs(authSubject, new RunnableAction(post));
        Assert.assertEquals("expected POST response code = 303", 303, post.getResponseCode());
        Assert.assertNull("expected PUT throwable == null", post.getThrowable());

        // Get the transfer details
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        HttpGet get = new HttpGet(post.getRedirectURL(), out);
        log.debug("GET: " + post.getRedirectURL());
        Subject.doAs(authSubject, new RunnableAction(get));
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
        log.info("PUT: " + endpoint);
        ByteArrayInputStream is = new ByteArrayInputStream(content.getBytes());
        put(endpoint, is, VOSTest.TEXT_CONTENT_TYPE);
    }

    private void uploadFile(VOSURI target, File uploadFile)
            throws PrivilegedActionException {
        List<Protocol> protocols = new ArrayList<Protocol>();
        Protocol basicTLS = new Protocol(VOS.PROTOCOL_HTTPS_PUT);
        basicTLS.setSecurityMethod(Standards.SECURITY_METHOD_CERT);
        protocols.add(basicTLS);
//        DataNode targetNode = new DataNode(target.getName());
        log.debug("uploading: " + target.getURI().toASCIIString());
        Transfer transfer = new Transfer(target.getURI(), Direction.pushToVoSpace);
        transfer.getProtocols().addAll(protocols);
        transfer.version = VOS.VOSPACE_21;

        final VOSpaceClient voSpaceClient = new VOSpaceClient(resourceID);
        final ClientTransfer clientTransfer = Subject.doAs(authSubject, new CreateTransferAction(voSpaceClient, transfer, false));
        clientTransfer.setOutputStreamWrapper(out -> {
            InputStream in = new FileInputStream(uploadFile);
            try {
                in.transferTo(out);
            } finally {
                in.close();
            }
        });
        Subject.doAs(authSubject, (PrivilegedExceptionAction<Object>) () -> {
            clientTransfer.runTransfer();
            return null;
        });

    }

    private Path downloadPackage(List<URI> targets, String expectedFilename, List<String> expectedFilenames,
                        Subject testSubject, String contentType)
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

        // POST the transfer
        FileContent fileContent = new FileContent(sw.toString().getBytes(), VOSTest.XML_CONTENT_TYPE);
        HttpPost post = new HttpPost(pkgServiceURL, fileContent, false);
        Subject.doAs(testSubject, new RunnableAction(post));
        Assert.assertEquals("expected POST response code = 303",303, post.getResponseCode());
        Assert.assertNull("expected POST throwable == null", post.getThrowable());
        URL redirctURL = post.getRedirectURL();

        // Download the package
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        HttpGet get = new HttpGet(redirctURL, out);
        log.debug("GET: " + redirctURL);
        Subject.doAs(authSubject, new RunnableAction(get));
        log.debug("GET responseCode: " + get.getResponseCode());
        Assert.assertEquals("expected GET response code = 200", 200, get.getResponseCode());
        Assert.assertNull("expected GET throwable == null", get.getThrowable());
        Assert.assertTrue(String.format("expected GET Content-Type %s, found %s ", contentType, get.getContentType()),
                get.getContentType().startsWith(contentType));

        // Extract the package into a tmp directory
        InputStream inputStream = new ByteArrayInputStream(out.toByteArray());
        String extractDirectory = String.format("%s%s", System.getProperty("java.io.tmpdir"), UUID.randomUUID());
        String archiveType = contentType.equals(ZIP_CONTENT_TYPE) ? ArchiveStreamFactory.ZIP : ArchiveStreamFactory.TAR;
        extractArchive(inputStream, extractDirectory, archiveType);
        log.debug(String.format("extract package: %s -> %s", redirctURL, extractDirectory));

        return Paths.get(extractDirectory);
    }

    private void extractArchive(InputStream inputStream, String extractDirectory, String archiveType)
            throws ArchiveException, IOException {
        ArchiveStreamFactory archiveStreamFactory = new ArchiveStreamFactory();
        ArchiveInputStream archiveInputStream = archiveStreamFactory.createArchiveInputStream(
                archiveType, inputStream);
        ArchiveEntry archiveEntry;
        while((archiveEntry = archiveInputStream.getNextEntry()) != null) {
            Path path = Paths.get(extractDirectory, archiveEntry.getName());
            File file = path.toFile();
            if(archiveEntry.isDirectory()) {
                if(!file.isDirectory()) {
                    file.mkdirs();
                }
            } else {
                File parent = file.getParentFile();
                if(!parent.isDirectory()) {
                    parent.mkdirs();
                }
                try (OutputStream outputStream = Files.newOutputStream(path)) {
                    IOUtils.copy(archiveInputStream, outputStream);
                }
            }
        }
    }

    static class CreateTransferAction implements PrivilegedExceptionAction<ClientTransfer> {

        VOSpaceClient voSpaceClient;
        Transfer transfer;
        boolean run;

        CreateTransferAction(VOSpaceClient voSpaceClient, Transfer transfer, boolean run) {
            this.voSpaceClient = voSpaceClient;
            this.transfer = transfer;
            this.run = run;
        }

        @Override
        public ClientTransfer run() throws Exception {
            ClientTransfer clientTransfer = voSpaceClient.createTransfer(transfer);
            if (run) {
                clientTransfer.run();
            }
            return clientTransfer;
        }

    }

}
