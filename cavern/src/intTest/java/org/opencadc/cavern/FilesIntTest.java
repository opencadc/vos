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

import ca.nrc.cadc.net.RemoteServiceException;
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

public class FilesIntTest {

    protected static Logger log = Logger.getLogger(FilesIntTest.class);

    protected static Subject cadcauthSubject;
    protected static String baseURI;
    protected static String getFolderURI;
    protected static String putFolderURI;
    
    public FilesIntTest() {}
    
    static
    {
        Log4jInit.setLevel("ca.nrc.cadc.vos", Level.INFO);
        Log4jInit.setLevel("ca.nrc.cadc.vos.client", Level.INFO);
        Log4jInit.setLevel("ca.nrc.cadc.vospace", Level.INFO);
        Log4jInit.setLevel("org.opencadc.cavern", Level.DEBUG);
        Log4jInit.setLevel("org.opencadc.cavern.files", Level.DEBUG);
    }

    @BeforeClass
    public static void staticInit() throws Exception {
        File SSL_CERT = FileUtil.getFileFromResource("x509_CADCAuthtest1.pem", CavernPackageRunnerTest.class);
        cadcauthSubject = SSLUtil.createSubject(SSL_CERT);

        baseURI ="vos://cadc.nrc.ca~arc/home/cadcauthtest1/do-not-delete/vospaceFilesTest/";
        getFolderURI = baseURI + "getTest";
        putFolderURI = baseURI + "putTest";
        log.debug("------------ TEST SETUP ------------");
        log.debug("get and put test folders: " + getFolderURI + ", " + putFolderURI);
        log.debug("test dir base URI: " + baseURI);
    }


    @Test
    public void testGetPublicFile() throws Throwable {
        try {
            // Q: is there an arc vospace-int-test folder already set up? for whih account?
            final String uri = getFolderURI + "/bowline.jpg";
            File f = getFile(new VOSURI(uri));
            log.debug("filename found: " + f.getName());
            Assert.assertEquals("filename incorrect", f.getName(), "bowline.jpg" );
        } catch (Throwable t) {
            log.error(t.getMessage(), t);
            Assert.fail("Unexcepted exception: " + t.getMessage());
        }
    }
    
    @Test
    public void testGetPublicFileThroughLink() throws Throwable {
        try {
            String uri = getFolderURI + "/ChilkootPass_GoldenStairs2.jpg";
            File f = getFile(new VOSURI(uri));
            log.debug("filename found: " + f.getName());
            Assert.assertEquals("filename incorrect", f.getName(), "ChilkootPass_GoldenStairs2.jpg" );
        } catch (Throwable t) {
            log.error(t.getMessage(), t);
            Assert.fail("Unexcepted exception: " + t.getMessage());
        }
    }
    
    @Test
    public void testGetContainerNode() throws Throwable {
        try {
            try {
                getFile(new VOSURI(baseURI));
                Assert.fail("should have received illegal argument exception.");
            } catch (RemoteServiceException e) {
                log.info("caught expected: " + e);
            }
        } catch (Throwable t) {
            log.error(t.getMessage(), t);
            Assert.fail("Unexcepted exception (" + t.getClass().getSimpleName() +
                    "): " + t.getMessage());
        }
    }
    
    @Test
    public void testGetProprietaryFile() throws Throwable {
        try {
            final String uri = getFolderURI + "/All-Falcon-Chicks-2016.jpg";
            final VOSURI vosURI = new VOSURI(uri);
            try {
                getFile(vosURI);
                Assert.fail("should have received access control exception.");
            } catch (AccessControlException e) {
                // expected
            }

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
            Assert.fail("Unexcepted exception: " + t.getMessage());
        }
    }
    
    public File getFile(VOSURI uri) throws Throwable {
        RegistryClient regClient = new RegistryClient();
        AuthMethod authMethod = AuthenticationUtil.getAuthMethod(AuthenticationUtil.getCurrentSubject());
        if (authMethod == null) {
            authMethod = AuthMethod.ANON;
        }
        URL baseURL = regClient.getServiceURL(uri.getServiceURI(), Standards.VOSPACE_FILES_20, authMethod);
        log.debug("baseURL for getFile: " + baseURL.toExternalForm());
        URL url = new URL(baseURL.toString() + uri.getPath());
        log.debug("requested url for getFile: " + url.toExternalForm());

        ByteArrayOutputStream out = new ByteArrayOutputStream();
        File tmp = new File(System.getProperty("java.io.tmpdir"));
        HttpDownload download = new HttpDownload(url, tmp);

        download.setFollowRedirects(false);
        download.setOverwrite(true); // file exists from create above
        download.run();
        if (download.getThrowable() != null) {
            throw download.getThrowable();
        }

        Assert.assertNull("throwable", download.getThrowable());
        Assert.assertEquals(200, download.getResponseCode());
        File ret = download.getFile();
        Assert.assertTrue("file exists", ret.exists());
        log.debug("returned file absolute path: " + ret.getAbsolutePath());

        return ret;

    }

}
