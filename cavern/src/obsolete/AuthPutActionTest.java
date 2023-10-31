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
************************************************************************
*/

package org.opencadc.cavern;

import ca.nrc.cadc.auth.AuthMethod;
import ca.nrc.cadc.auth.AuthenticationUtil;
import ca.nrc.cadc.auth.SSLUtil;
import ca.nrc.cadc.net.HttpDelete;
import ca.nrc.cadc.net.HttpUpload;
import ca.nrc.cadc.net.ResourceNotFoundException;
import ca.nrc.cadc.reg.Standards;
import ca.nrc.cadc.reg.client.RegistryClient;
import ca.nrc.cadc.util.FileUtil;
import ca.nrc.cadc.util.Log4jInit;
import ca.nrc.cadc.vos.ContainerNode;
import ca.nrc.cadc.vos.DataNode;
import ca.nrc.cadc.vos.Node;
import ca.nrc.cadc.vos.NodeProperty;
import ca.nrc.cadc.vos.VOS;
import ca.nrc.cadc.vos.VOSURI;
import ca.nrc.cadc.vos.client.VOSpaceClient;
import java.io.File;
import java.net.URI;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.security.AccessControlException;
import java.security.PrivilegedAction;
import java.security.PrivilegedActionException;
import java.security.PrivilegedExceptionAction;
import javax.security.auth.Subject;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

public class AuthPutActionTest {

    protected static Logger log = Logger.getLogger(AuthPutActionTest.class);
    protected static Subject cadcauthSubject;
    protected static Subject cadcregSubject;
    protected static String baseURIStr;
    protected static VOSURI baseURI;
    protected static String putTestURIStr;
    private static URL nodesURL;
    private static VOSURI putTestFolderURI;
    private static VOSpaceClient vos;

    // Set this to 'false' if the test folder needs to be retained
    // during manual or dev testing for debugging
    private static boolean cleanupAfterTest = true;

    public AuthPutActionTest() {}
    
    static
    {
        Log4jInit.setLevel("org.opencadc.cavern", Level.INFO);
        Log4jInit.setLevel("org.opencadc.cavern.files", Level.INFO);
    }

    @BeforeClass
    public static void staticInit() throws Exception {
        log.debug("------------ TEST SETUP ------------");
        File SSL_CERT = FileUtil.getFileFromResource("x509_CADCAuthtest1.pem", CavernPackageRunnerTest.class);
        cadcauthSubject = SSLUtil.createSubject(SSL_CERT);

        File SSL_CERT_2 = FileUtil.getFileFromResource("x509_CADCRegtest1.pem",
            AuthGetActionTest.class);
        cadcregSubject = SSLUtil.createSubject(SSL_CERT_2);

        baseURIStr ="vos://cadc.nrc.ca~arc/home/cadcauthtest1/do-not-delete/vospaceFilesTest/putTest";
        putTestURIStr = baseURIStr + "/filesPutTest-" + System.currentTimeMillis();

        log.debug("test dir base URI: " + baseURIStr);
        log.debug("put test folder: " +  putTestFolderURI);

        baseURI = new VOSURI(new URI(baseURIStr));
        vos = new VOSpaceClient(baseURI.getServiceURI());

        putTestFolderURI = new VOSURI(new URI(putTestURIStr));
        final URI serviceURI = putTestFolderURI.getServiceURI();

        log.debug("nodes serviceURI: " + putTestFolderURI);

        RegistryClient rc = new RegistryClient();
        nodesURL = rc.getServiceURL(serviceURI, Standards.VOSPACE_NODES_20, AuthMethod.CERT);

        // Set up the put test container folder
        try {
            Subject.doAs(cadcauthSubject, new PrivilegedExceptionAction() {
                @Override
                public Object run() throws Exception {
                    ContainerNode cn = new ContainerNode(putTestFolderURI);
                    vos.createNode(cn);
                    return null;
                }
            });
        } catch (PrivilegedActionException ioe) {
            log.error("unable to set up test container " + putTestFolderURI.toString());
        }
    }

    @Test
    public void testPutFileAuthOK() throws Throwable {
        final String uri = putTestURIStr + "/smallTextFile.rtf";
        final String sourceFilename = "src/test/resources/smallTextFile.rtf";
        final String expectedMD5 = "e99fb782d60d295a3aa287cc999349d2";
        final VOSURI nodeURI = new VOSURI(uri);
        final File f = new File(sourceFilename);
        log.debug("source file for test: " + sourceFilename);

        try {
            Subject.doAs(cadcauthSubject, new PrivilegedAction<Object>() {
                public Object run() {
                    try {
                        putFile(nodeURI, f);
                        verifyPut(nodeURI, sourceFilename, expectedMD5);
                        return null;
                    } catch (Throwable t) {
                        throw new RuntimeException(t);
                    }
                }
            });
            
        } catch (Throwable t) {
            log.error(t.getMessage(), t);
            Assert.fail("Unexpected exception: " + t.getMessage());
        }
    }

    @Test
    public void testPutOverwriteFileOK() throws Throwable {
        final String uri = putTestURIStr + "/smallTextFile2.rtf";
        final String sourceFilename = "src/test/resources/smallTextFile2.rtf";
        final String expectedMD5 = "351314eb3b99502adb0fc7d7dd33f7b8";
        final VOSURI nodeURI = new VOSURI(uri);
        final File f = new File(sourceFilename);

        log.debug("source file for test: " + sourceFilename);

        // Make a DataNode of the same name as the test file,
        // PUT that node using /cavern/nodes service
        try {
            Subject.doAs(cadcauthSubject, new PrivilegedExceptionAction() {
                @Override
                public Object run() throws Exception {
                    DataNode dn = new DataNode(nodeURI);
                    dn.getProperties().add(new NodeProperty(VOS.PROPERTY_URI_ISPUBLIC, Boolean.FALSE.toString()));
                    vos.createNode(dn);
                    return null;
                }
            });
        } catch (PrivilegedActionException ioe) {
            Assert.fail("unable to set up test node");
        }

        // Now attempt to PUT the file using /cavern/files - should still work
        try {
            Subject.doAs(cadcauthSubject, new PrivilegedAction<Object>() {
                public Object run() {
                    try {
                        putFile(nodeURI, f);
                        verifyPut(nodeURI, sourceFilename, expectedMD5);
                        return null;
                    } catch (Throwable t) {
                        throw new RuntimeException(t);
                    }
                }
            });

        } catch (Throwable t) {
            log.error(t.getMessage(), t);
            Assert.fail("Unexpected exception: " + t.getMessage());
        }
    }

    @Test
    public void testPutFileNotAuth() throws Throwable {
        final String uri = putTestURIStr + "/smallTextFile.rtf";
        final String sourceFilename = "src/test/resources/smallTextFile.rtf";
        final VOSURI nodeURI = new VOSURI(uri);
        final File f = new File(sourceFilename);
        log.debug("source file for test: " + sourceFilename);

        // Now attempt to PUT the file using /cavern/files - should still work

        Subject.doAs(cadcregSubject, new PrivilegedAction<Object>() {
            public Object run() {
                try {
                    HttpUpload p = putFileNOK(nodeURI, f, 403);
                    Assert.assertEquals("wrong exception type: " + p.getThrowable(), p.getThrowable().getClass(),  AccessControlException.class);
                } catch (Throwable t) {
                    log.error(t.getMessage(), t);
                    Assert.fail("Unexpected exception: " + t.getMessage());
                }
                return null;
            }
        });

    }

    @Test
    public void testPutParentFolderNotFound() throws Throwable {
        final String uri = putTestURIStr + "/newFolder/smallTextFile.rtf";
        final String sourceFilename = "src/test/resources/smallTextFile.rtf";
        final VOSURI nodeURI = new VOSURI(uri);
        final File f = new File(sourceFilename);
        log.debug("source file for test: " + sourceFilename);
        Subject.doAs(cadcregSubject, new PrivilegedAction<Object>() {
            public Object run() {
                try {
                    HttpUpload p = putFileNOK(nodeURI, f, 404);
                    Assert.assertEquals("wrong exception type: " + p.getThrowable(), p.getThrowable().getClass(), ResourceNotFoundException.class);
                } catch (Throwable t) {
                    log.error(t.getMessage(), t);
                    Assert.fail("Unexpected exception: " + t.getMessage());
                }
                return null;
            }
        });
    }

    @Test
    public void testPutFileAnonNOK() throws Throwable {
        final String uri = baseURIStr + "/public-do-not-delete/smallTextFile.rtf";
        final String sourceFilename = "src/test/resources/smallTextFile.rtf";
        final VOSURI nodeURI = new VOSURI(uri);
        final File f = new File(sourceFilename);
        log.debug("source file for test: " + sourceFilename);

        try {
            HttpUpload p = putFileNOK(nodeURI, f, 403);
            Assert.assertEquals("wrong exception type: " + p.getThrowable(), p.getThrowable().getClass(),  AccessControlException.class);
        } catch (Throwable t) {
            log.error(t.getMessage(), t);
            Assert.fail("Unexpected exception: " + t.getMessage());
        }
    }



    /**
     * Use Standards.VOSPACE_FILES_20 service (/files) to PUT the given file
     * @param uri
     * @param fileToPut
     * @throws Throwable
     */
    private void putFile(VOSURI uri, File fileToPut) throws Throwable {

        RegistryClient regClient = new RegistryClient();
        AuthMethod authMethod = AuthenticationUtil.getAuthMethod(AuthenticationUtil.getCurrentSubject());
        if (authMethod == null) {
            authMethod = AuthMethod.ANON;
        }
        URL filePutURL = regClient.getServiceURL(uri.getServiceURI(), Standards.VOSPACE_FILES_20, authMethod);
        log.debug("baseURL for getFile: " + filePutURL.toExternalForm());
        URL url = new URL(filePutURL.toString() + uri.getPath());
        log.debug("requested url for putFile: " + url.toExternalForm());

        HttpUpload put = new HttpUpload(fileToPut, url);
        put.setRequestProperty("Content-Type", "text/xml");
        put.run();

        log.debug("PUT response code: " + put.getResponseCode());
        if (put.getThrowable() != null) {
            throw put.getThrowable();
        }

        Assert.assertEquals(200, put.getResponseCode());
    }


    private HttpUpload putFileNOK(VOSURI uri, File fileToPut, int expectedResponseCode) throws Throwable {

        RegistryClient regClient = new RegistryClient();
        AuthMethod authMethod = AuthenticationUtil.getAuthMethod(AuthenticationUtil.getCurrentSubject());
        if (authMethod == null) {
            authMethod = AuthMethod.ANON;
        }
        URL filePutURL = regClient.getServiceURL(uri.getServiceURI(), Standards.VOSPACE_FILES_20, authMethod);
        log.debug("baseURL for getFile: " + filePutURL.toExternalForm());
        URL url = new URL(filePutURL.toString() + uri.getPath());
        log.debug("requested url for putFile: " + url.toExternalForm());

        HttpUpload put = new HttpUpload(fileToPut, url);
        put.setRequestProperty("Content-Type", "text/xml");
        put.run();
        int responseCode = put.getResponseCode();
        log.debug("PUT response code: " + responseCode);
        log.debug("PUT throwable: " + put.getThrowable());

        Assert.assertEquals("wrong expected response code: expected " + expectedResponseCode + " got " + responseCode, expectedResponseCode, responseCode);
        return put;
    }



    private static void verifyPut(VOSURI fileURI, String sourceFilename, String md5Sum) throws Throwable {
        // Pulls file from the build/resources/intTest directory
        final File expectedFile = new File(sourceFilename);
        try {
            Subject.doAs(cadcauthSubject, new PrivilegedExceptionAction() {
                @Override
                public Object run() throws Exception {
                    Node n = vos.getNode(fileURI.getPath());
                    log.debug("MD5: " + n.getPropertyValue(VOS.PROPERTY_URI_CONTENTMD5) + " (expecting " + md5Sum + ")");
                    Assert.assertEquals(("filename not as expected: "), expectedFile.getName(), n.getName());
                    long contentLength = Long.parseLong(n.getPropertyValue(VOS.PROPERTY_URI_CONTENTLENGTH));
                    long expectedFileLength = expectedFile.length();
                    Assert.assertEquals(expectedFileLength, contentLength);
                    Assert.assertEquals(md5Sum, n.getPropertyValue(VOS.PROPERTY_URI_CONTENTMD5));
                    return null;
                }
            });
        } catch (PrivilegedActionException ioe) {
            log.debug("verifyPut exception:" + ioe);
            Assert.fail("unable to set up test node");
        }
    }

    /**
     * Delete the test directory (filesPutTest-{system time millis})>
     */
    @AfterClass
    public static void cleanupTestDir() {
        if (cleanupAfterTest == true) {
            try {
                Subject.doAs(cadcauthSubject, new PrivilegedExceptionAction() {
                    @Override
                    public Object run() throws Exception {
                        log.debug("attempting to remove put test folder: " + putTestFolderURI.getPath());
                        URL nodeResourceURL = new URL(nodesURL.toExternalForm() + putTestFolderURI.getPath());
                        HttpDelete cleanup = new HttpDelete(nodeResourceURL, false);
                        cleanup.run();
                        int code = cleanup.getResponseCode();
                        log.debug("DELETE node response code: " + code);
                        return null;
                    }
                });

            } catch (PrivilegedActionException ioe) {
                log.error("unable to clean up file: " + putTestFolderURI.toString());
            }
        }
    }
}
