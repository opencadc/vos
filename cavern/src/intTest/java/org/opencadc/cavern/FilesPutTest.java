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
import ca.nrc.cadc.net.HttpUpload;
import ca.nrc.cadc.net.OutputStreamWrapper;
import ca.nrc.cadc.reg.Standards;
import ca.nrc.cadc.reg.client.RegistryClient;
import ca.nrc.cadc.util.FileUtil;
import ca.nrc.cadc.util.Log4jInit;
import ca.nrc.cadc.vos.ContainerNode;
import ca.nrc.cadc.vos.DataNode;
import ca.nrc.cadc.vos.Node;
import ca.nrc.cadc.vos.NodeProperty;
import ca.nrc.cadc.vos.NodeWriter;
import ca.nrc.cadc.vos.VOS;
import ca.nrc.cadc.vos.VOSURI;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.net.URI;
import java.net.URL;
import java.security.AccessControlException;
import java.security.PrivilegedAction;
import java.security.PrivilegedExceptionAction;
import javax.net.ssl.HttpsURLConnection;
import javax.security.auth.Subject;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

public class FilesPutTest {

    protected static Logger log = Logger.getLogger(FilesPutTest.class);

    protected static Subject cadcauthSubject;
    protected static String baseURI;
    protected static String putTestURIStr;
    private static URL nodesURL;
    private static VOSURI putTestFolderURI;

    // Set this to 'false' if the test folder needs to be retained
    // during manual or dev testing for debugging
    private static boolean cleanupAfterTest = true;

    public FilesPutTest() {}
    
    static
    {
        Log4jInit.setLevel("org.opencadc.cavern", Level.DEBUG);
        Log4jInit.setLevel("org.opencadc.cavern.files", Level.DEBUG);
    }

    @BeforeClass
    public static void staticInit() throws Exception {
        log.debug("------------ TEST SETUP ------------");
        File SSL_CERT = FileUtil.getFileFromResource("x509_CADCAuthtest1.pem", CavernPackageRunnerTest.class);
        cadcauthSubject = SSLUtil.createSubject(SSL_CERT);

        baseURI ="vos://cadc.nrc.ca~arc/home/cadcauthtest1/do-not-delete/vospaceFilesTest/putTest";
        putTestURIStr = baseURI + "/filesPutTest-" + System.currentTimeMillis();

        log.debug("test dir base URI: " + baseURI);
        log.debug("put test folder: " +  putTestFolderURI);

        putTestFolderURI = new VOSURI(new URI(putTestURIStr));
        final URI serviceURI = putTestFolderURI.getServiceURI();

        log.debug("nodes serviceURI: " + putTestFolderURI);

        RegistryClient rc = new RegistryClient();
        nodesURL = rc.getServiceURL(serviceURI, Standards.VOSPACE_NODES_20, AuthMethod.CERT);

        // Set up the put test container folder
        Subject.doAs(cadcauthSubject, new PrivilegedExceptionAction() {
            @Override
            public Object run() throws Exception {
                ContainerNode cn = new ContainerNode(putTestFolderURI);
                putNode(new URL(nodesURL.toExternalForm() + cn.getUri().getPath()), cn);
                return null;
            }
        });
    }

    @Test
    public void testPutFileOK() throws Throwable {
        final String uri = putTestURIStr + "/smallTextFile.rtf";
        final VOSURI nodeURI = new VOSURI(uri);

        try {
            Subject.doAs(cadcauthSubject, new PrivilegedAction<Object>() {
                public Object run() {
                    try {
                        String filename = "src/test/resources/smallTextFile.rtf";
                        putFile(nodeURI, filename);
                        return null;
                    } catch (Throwable t) {
                        throw new RuntimeException(t);
                    }
                }
            });
            
        } catch (Throwable t) {
            log.error(t.getMessage(), t);
            Assert.fail("Unexcepted exception: " + t.getMessage());
        }
    }


    @Test
    public void testPutOverwriteFileOK() throws Throwable {
        final String uri = putTestURIStr + "/smallTextFile2.rtf";
        final VOSURI nodeURI = new VOSURI(uri);

        // Make a DataNode of the same name as the test file,
        // PUT that node using /cavern/nodes service
        Subject.doAs(cadcauthSubject, new PrivilegedExceptionAction() {
            @Override
            public Object run() throws Exception {
                DataNode dn = new DataNode(nodeURI);
                dn.getProperties().add(new NodeProperty(VOS.PROPERTY_URI_ISPUBLIC, Boolean.FALSE.toString()));
                putNode(new URL(nodesURL.toExternalForm() + dn.getUri().getPath()), dn);
                return null;
            }
        });

        // Now attempt to PUT the file using /cavern/files - should still work
        try {
            Subject.doAs(cadcauthSubject, new PrivilegedAction<Object>() {
                public Object run() {
                    try {
                        String filename = "src/test/resources/smallTextFile2.rtf";
                        putFile(nodeURI, filename);
                        return null;
                    } catch (Throwable t) {
                        throw new RuntimeException(t);
                    }
                }
            });

        } catch (Throwable t) {
            log.error(t.getMessage(), t);
            Assert.fail("Unexcepted exception: " + t.getMessage());
        }
    }


    @Test
    public void testPutFileNotAuth() throws Throwable {
        final String uri = putTestURIStr + "/smallTextFile.rtf";
        final VOSURI nodeURI = new VOSURI(uri);

        try {
            String filename = "src/test/resources/smallTextFile.rtf";
            putFile(nodeURI, filename);
            Assert.fail("should have had an access error.");

        } catch (AccessControlException t) {
            log.debug("expected & got AccessControlException");

        } catch (Throwable t) {
            log.error(t.getMessage(), t);
            Assert.fail("Unexcepted exception: " + t.getMessage());
        }
    }


    private void putFile(VOSURI uri, String fileName) throws Throwable {

        RegistryClient regClient = new RegistryClient();
        AuthMethod authMethod = AuthenticationUtil.getAuthMethod(AuthenticationUtil.getCurrentSubject());
        if (authMethod == null) {
            authMethod = AuthMethod.ANON;
        }
        URL filePutURL = regClient.getServiceURL(uri.getServiceURI(), Standards.VOSPACE_FILES_20, authMethod);
        log.debug("baseURL for getFile: " + filePutURL.toExternalForm());
        URL url = new URL(filePutURL.toString() + uri.getPath());
        log.debug("requested url for putFile: " + url.toExternalForm());

        File fileToUpload = new File(fileName);

        ByteArrayOutputStream out = new ByteArrayOutputStream();
        File tmp = new File(System.getProperty("java.io.tmpdir"));
        HttpUpload put = new HttpUpload(fileToUpload, url);
        put.setRequestProperty("Content-Type", "text/xml");

        put.run();

        if (put.getThrowable() != null) {
            throw put.getThrowable();
        }

        Assert.assertEquals(200, put.getResponseCode());
    }



    private static void putNode(URL url, final Node node) throws Exception {

        OutputStreamWrapper wrapper = new OutputStreamWrapper() {
            @Override
            public void write(OutputStream out) throws IOException {
                OutputStreamWriter writer = new OutputStreamWriter(out);
                NodeWriter nodeWriter = new NodeWriter();
                nodeWriter.write(node, writer);
                out.close();
            }
        };
        HttpUpload upload = new HttpUpload(wrapper, url);
        upload.run();

        int code = upload.getResponseCode();
        log.debug("Put node response code: " + code);

        if (node instanceof ContainerNode && code == 409) {
            // conflict = exists
            return; // ok
        }
        if (code >= 400) {
            throw new RuntimeException("put node failed: " + code, upload.getThrowable());
        }
    }

    /**
     * Delete the test putFile directory
     */
    @AfterClass
    public static void cleanupTestDir() {
        if (cleanupAfterTest == true) {
            try {
                URL nodeResourceURL = new URL(nodesURL.toExternalForm() + putTestFolderURI.getPath());

                // try to delete the node
                HttpsURLConnection connection = (HttpsURLConnection) nodeResourceURL.openConnection();
                connection.setSSLSocketFactory(SSLUtil.getSocketFactory(cadcauthSubject));
                connection.setRequestMethod("DELETE");
                log.debug("Delete node response code: " + connection.getResponseCode());

            } catch (IOException ioe) {
                log.error("unable to clean up file: " + putTestFolderURI.toString());
            }
        }
    }

}
