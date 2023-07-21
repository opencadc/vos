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

import ca.nrc.cadc.net.ResourceNotFoundException;
import ca.nrc.cadc.util.FileUtil;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.net.URL;
import java.security.AccessControlException;
import java.security.PrivilegedAction;

import javax.security.auth.Subject;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import ca.nrc.cadc.auth.AuthMethod;
import ca.nrc.cadc.auth.AuthenticationUtil;
import ca.nrc.cadc.auth.SSLUtil;
import ca.nrc.cadc.net.HttpDownload;
import ca.nrc.cadc.reg.Standards;
import ca.nrc.cadc.reg.client.RegistryClient;
import ca.nrc.cadc.util.Log4jInit;
import ca.nrc.cadc.vos.VOSURI;

public class AuthGetActionTest {

    protected static Logger log = Logger.getLogger(AuthGetActionTest.class);

    protected static Subject cadcauthSubject;
    protected static Subject cadcregSubject;
    protected static String baseURI;
    protected static String publicBaseURI;
    protected static String getFolderURI;
    protected static String getPublicFolderURI;

    public AuthGetActionTest() {}
    
    static
    {
        Log4jInit.setLevel("org.opencadc.cavern", Level.DEBUG);
        Log4jInit.setLevel("org.opencadc.cavern.files", Level.DEBUG);
    }

    @BeforeClass
    public static void staticInit() throws Exception {
        log.debug("------------ TEST SETUP ------------");
        File SSL_CERT = FileUtil.getFileFromResource("x509_CADCAuthtest1.pem",
                            AuthGetActionTest.class);
        cadcauthSubject = SSLUtil.createSubject(SSL_CERT);

        File SSL_CERT_2 = FileUtil.getFileFromResource("x509_CADCRegtest1.pem",
            AuthGetActionTest.class);
        cadcregSubject = SSLUtil.createSubject(SSL_CERT_2);

        baseURI = "vos://cadc.nrc.ca~arc/home/cadcauthtest1/do-not-delete/vospaceFilesTest/";
        publicBaseURI = "vos://cadc.nrc.ca~arc/projects/CADC/do-not-delete/vospaceFilesTest/";
        getFolderURI = baseURI + "getTest";
        getPublicFolderURI = publicBaseURI + "getTest";

        log.debug("get test folder: " + getFolderURI );
        log.debug("public get test folder: " + getPublicFolderURI);
        log.debug("test dir base URI: " + baseURI);
    }

    @Test
    public void testGetPublicFileOK() {
        try {
            final String uri = getPublicFolderURI + "/bowline.jpg";
            File f = getFile(new VOSURI(uri));
            log.debug("filename found: " + f.getName());
            Assert.assertEquals("filename incorrect", f.getName(), "bowline.jpg" );
            cleanup(f);
        } catch (Throwable t) {
            log.error(t.getMessage(), t);
            Assert.fail("Unexpected exception: " + t.getMessage());
        }
    }

    @Test
    public void testGetPublicFileThroughLinkOK() {
        try {
            String uri = getPublicFolderURI + "/ChilkootPass_GoldenStairs2.jpg";
            File f = getFile(new VOSURI(uri));
            log.debug("filename found: " + f.getName());
            Assert.assertEquals("filename incorrect", f.getName(), "ChilkootPass_GoldenStairs2.jpg" );
            cleanup(f);
        } catch (Throwable t) {
            log.error(t.getMessage(), t);
            Assert.fail("Unexpected exception: " + t.getMessage());
        }
    }

    @Test
    public void testGetProprietaryFileOK() {
        try {
            final String uri = getFolderURI + "/All-Falcon-Chicks-2016.jpg";
            final VOSURI vosURI = new VOSURI(uri);

            Subject.doAs(cadcauthSubject, new PrivilegedAction<Object>() {
                public Object run() {
                    try {
                        File f = getFile(vosURI);
                        log.debug("filename found: " + f.getName());
                        Assert.assertEquals("filename incorrect", f.getName(), "All-Falcon-Chicks-2016.jpg" );
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
    public void testGetPublicFileNotFound() {
        try {
            String uri = baseURI + "/doesNotExist.txt";
            HttpDownload d = getFileNOK(new VOSURI(uri), 404);
            Assert.assertEquals("wrong exception type: " + d.getThrowable(), d.getThrowable().getClass(),  ResourceNotFoundException.class);
        } catch (Throwable t) {
            log.error(t.getMessage(), t);
            Assert.fail("Unexpected exception (" + t.getClass().getSimpleName() +
                "): " + t.getMessage());
        }
    }

    @Test
    public void testGetPublicParentFolderNotFound() {
        try {
            // spelled wrong on purpose
            String uri = baseURI + "getTet" + "/bowline.jpg";
            HttpDownload d = getFileNOK(new VOSURI(uri), 404);
            Assert.assertEquals("wrong exception type: " + d.getThrowable(), d.getThrowable().getClass(),  ResourceNotFoundException.class);
        } catch (Throwable t) {
            log.error(t.getMessage(), t);
            Assert.fail("Unexpected exception (" + t.getClass().getSimpleName() +
                "): " + t.getMessage());
        }
    }

    @Test
    public void testGetContainerNodeNOK() {
        try {
            HttpDownload d = getFileNOK(new VOSURI(baseURI), 403);
            Assert.assertEquals("wrong exception type: " + d.getThrowable(), d.getThrowable().getClass(),  AccessControlException.class);
        } catch (Throwable t) {
            log.error(t.getMessage(), t);
            Assert.fail("Unexpected exception (" + t.getClass().getSimpleName() +
                "): " + t.getMessage());
        }
    }

    @Test
    public void testGetProprietaryFileNOK() {
        try {
            final String uri = getFolderURI + "/All-Falcon-Chicks-2016.jpg";
            final VOSURI vosURI = new VOSURI(uri);

            // File is owned by cadcauthSubject
            Subject.doAs(cadcregSubject, new PrivilegedAction<Object>() {
                public Object run() {
                    try {
                        HttpDownload d = getFileNOK(vosURI, 403);
                        Assert.assertEquals("wrong exception type: " + d.getThrowable(), d.getThrowable().getClass(),  AccessControlException.class);
                    } catch (Throwable t) {
                        log.error(t.getMessage(), t);
                        Assert.fail("Unexpected exception: " + t.getMessage());
                    }
                    return null;
                }
            });

        } catch (Throwable t) {
            log.error(t.getMessage(), t);
            Assert.fail("Unexpected exception: " + t.getMessage());
        }
    }


    @Test
    public void testGetProprietaryAnonFileNotAuth() {
        try {
            final String uri = getFolderURI + "/All-Falcon-Chicks-2016.jpg";
            final VOSURI vosURI = new VOSURI(uri);
            try {
                HttpDownload d = getFileNOK(vosURI, 403);
                Assert.assertEquals("wrong exception type: " + d.getThrowable(), d.getThrowable().getClass(),  AccessControlException.class);
            } catch (Throwable t) {
                log.error(t.getMessage(), t);
                Assert.fail("Unexpected exception: " + t.getMessage());
            }

        } catch (Throwable t) {
            log.error(t.getMessage(), t);
            Assert.fail("Unexpected exception: " + t.getMessage());
        }
    }

    @Test
    public void testGetFileNotExist() {
        try {
            final String uri = getFolderURI + "/All-Falcon-Chicks-2020.jpg";
            final VOSURI vosURI = new VOSURI(uri);

            try {
                HttpDownload d = getFileNOK(vosURI, 404);
                Assert.assertEquals("wrong exception type: " + d.getThrowable(), d.getThrowable().getClass(),  ResourceNotFoundException.class);
            } catch (Throwable t) {
                log.error(t.getMessage(), t);
                Assert.fail("Unexpected exception: " + t.getMessage());
            }

        } catch (Throwable t) {
            log.error(t.getMessage(), t);
            Assert.fail("Unexpected exception: " + t.getMessage());
        }
    }



    public File getFile(VOSURI uri) throws Throwable {

        AuthMethod authMethod = AuthenticationUtil.getAuthMethod(AuthenticationUtil.getCurrentSubject());
        if (authMethod == null) {
            authMethod = AuthMethod.ANON;
        }

        RegistryClient regClient = new RegistryClient();
        URL baseURL = regClient.getServiceURL(uri.getServiceURI(), Standards.VOSPACE_FILES_20, authMethod);
        log.debug("baseURL for getFile: " + baseURL.toExternalForm());
        URL url = new URL(baseURL + uri.getPath());
        log.debug("requested url for getFile: " + url.toExternalForm());

        File tmp = new File(System.getProperty("java.io.tmpdir"));
        HttpDownload download = new HttpDownload(url, tmp);

        download.setFollowRedirects(false);
        download.setOverwrite(true); // file potentially exists from previous test runs
        log.debug("about to run download.");
        download.run();

        log.debug("download response code: " + download.getResponseCode());
        if (download.getThrowable() != null) {
            throw download.getThrowable();
        }
        Assert.assertEquals(200, download.getResponseCode());
        File ret = download.getFile();
        Assert.assertTrue("file exists", ret.exists());
        log.debug("returned file absolute path: " + ret.getAbsolutePath());

        return ret;

    }


    public HttpDownload getFileNOK(VOSURI uri, int expectedResponseCode) throws Throwable {

        AuthMethod authMethod = AuthenticationUtil.getAuthMethod(AuthenticationUtil.getCurrentSubject());
        if (authMethod == null) {
            authMethod = AuthMethod.ANON;
        }

        RegistryClient regClient = new RegistryClient();
        URL baseURL = regClient.getServiceURL(uri.getServiceURI(), Standards.VOSPACE_FILES_20, authMethod);
        log.debug("baseURL for getFile: " + baseURL.toExternalForm());
        URL url = new URL(baseURL.toString() + uri.getPath());
        log.debug("requested url for getFile: " + url.toExternalForm());

        ByteArrayOutputStream out = new ByteArrayOutputStream();
        File tmp = new File(System.getProperty("java.io.tmpdir"));
        HttpDownload download = new HttpDownload(url, tmp);

        download.setFollowRedirects(false);
        download.setOverwrite(true); // file potentially exists from previous test runs
        log.debug("about to run download.");
        download.run();

        int responseCode = download.getResponseCode();
        log.debug("GET response code: " + responseCode);
        log.debug("GET throwable: " + download.getThrowable());
        log.debug("GET cause: " + download.getThrowable().getCause());
        log.debug("GET message: " + download.getThrowable().getMessage());

        Assert.assertEquals("wrong expected response code: expected " + expectedResponseCode + " got " + responseCode, expectedResponseCode, responseCode);

        return download;

    }


    private void cleanup(File f) {
        if (f.exists()) {
            f.delete();
        }
    }

}
