/*
************************************************************************
*******************  CANADIAN ASTRONOMY DATA CENTRE  *******************
**************  CENTRE CANADIEN DE DONNÉES ASTRONOMIQUES  **************
*
*  (c) 2009.                            (c) 2009.
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
*  $Revision: 4 $
*
************************************************************************
*/

package org.opencadc.cavern;

import ca.nrc.cadc.auth.AuthMethod;
import ca.nrc.cadc.auth.SSLUtil;
import ca.nrc.cadc.net.HttpDownload;
import ca.nrc.cadc.reg.Standards;
import ca.nrc.cadc.reg.client.RegistryClient;
import ca.nrc.cadc.util.FileUtil;
import ca.nrc.cadc.util.Log4jInit;
import ca.nrc.cadc.util.StringUtil;
import ca.nrc.cadc.vos.ContainerNode;
import ca.nrc.cadc.vos.Direction;
import ca.nrc.cadc.vos.LinkNode;
import ca.nrc.cadc.vos.Node;
import ca.nrc.cadc.vos.NodeNotFoundException;
import ca.nrc.cadc.vos.VOS;
import ca.nrc.cadc.vos.VOSURI;
import ca.nrc.cadc.vos.client.VOSpaceClient;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.OutputStream;
import java.net.URI;
import java.net.URL;
import java.net.URLEncoder;
import java.security.PrivilegedExceptionAction;

import javax.security.auth.Subject;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

public class MetadataIntTest {
    private static final Logger log = Logger.getLogger(MetadataIntTest.class);

    private static File SSL_CERT;

    private static VOSURI baseURI;
    private static VOSURI linksBaseURI;

    static {
        Log4jInit.setLevel("org.opencadc.cavern", Level.INFO);
        Log4jInit.setLevel("ca.nrc.cadc.vos", Level.INFO);
    }

    public MetadataIntTest() {
    }

    @BeforeClass
    public static void staticInit() throws Exception
    {
        SSL_CERT = FileUtil.getFileFromResource("x509_CADCRegtest1.pem", MetadataIntTest.class);

        String uriProp = MetadataIntTest.class.getName() + ".baseURI";
        String uri = System.getProperty(uriProp);
        log.debug(uriProp + " = " + uri);
        if ( StringUtil.hasText(uri) ) {
            baseURI = new VOSURI(new URI(uri));
        }
        else {
            throw new IllegalStateException("expected system property " + uriProp + " = <base vos URI>, found: " + uri);
        }
        
        uriProp = MetadataIntTest.class.getName() + ".linksBaseURI";
        uri = System.getProperty(uriProp);
        log.debug(uriProp + " = " + uri);
        if ( StringUtil.hasText(uri) ) {
            linksBaseURI = new VOSURI(new URI(uri));
        }
        else {
            throw new IllegalStateException("expected system property " + uriProp + " = <base vos URI>, found: " + uri);
        }
    }

    @Test
    public void testFileDownloadContentMD5() throws Exception {
        final VOSpaceClient vos = new VOSpaceClient(baseURI.getServiceURI());
        String vosuripath = baseURI.toString() + "/metadataIntTest-" + System.currentTimeMillis();
        final VOSURI uri = new VOSURI(vosuripath);
        Subject s = SSLUtil.createSubject(SSL_CERT);
        try {
            final File testFile1 = new File("src/test/resources/md5file1");
            final String correctSize1 = "25";
            final String correctMD51 = "86bec12f968870284258e4f239e1300c";

            TestActions.UploadNodeAction upload = null;
            Node result = null;

            upload = new TestActions.UploadNodeAction(vos, uri, correctMD51, testFile1);
            result = Subject.doAs(s, upload);
            log.debug("expected md5: " + correctMD51 + "  actual: " + result.getPropertyValue(VOS.PROPERTY_URI_CONTENTMD5));
            Assert.assertEquals("Wrong MD5", correctMD51, result.getPropertyValue(VOS.PROPERTY_URI_CONTENTMD5));
            Assert.assertEquals("Wrong Size", correctSize1, result.getPropertyValue(VOS.PROPERTY_URI_CONTENTLENGTH));
            
            Subject.doAs(s, new PrivilegedExceptionAction<Object>() {

                @Override
                public Object run() throws Exception {
                    
                    // Create the symbolic link needed for the tests below
                    Node linksDir = null;
                    try {
                        linksDir = vos.getNode(linksBaseURI.getPath());
                    } catch (NodeNotFoundException notfound) {
                    }
                    if (linksDir == null) {
                        linksDir = new ContainerNode(linksBaseURI);
                        vos.createNode(linksDir);
                    }
                    VOSURI linkURI = new VOSURI(linksBaseURI.toString() + "/metadataLink-" + System.currentTimeMillis());
                    LinkNode linkNode = new LinkNode(linkURI, uri.getURI());
                    vos.createNode(linkNode);
                    
                    // test the md5 of the target node
                    RegistryClient rc = new RegistryClient();
                    URL syncTrans = rc.getServiceURL(
                        URI.create("ivo://canfar.net/cavern"), Standards.VOSPACE_SYNC_21, AuthMethod.CERT);
                    StringBuilder params = new StringBuilder();
                    params.append("TARGET=").append(URLEncoder.encode(uri.toString(), "UTF-8"));
                    params.append("&");
                    params.append("DIRECTION=").append(Direction.pullFromVoSpaceValue);
                    params.append("&");
                    params.append("PROTOCOL=").append(URLEncoder.encode(VOS.PROTOCOL_HTTPS_GET, "UTF-8"));
                    URL transfer = new URL(syncTrans.toString() + "?" + params.toString());
                    log.debug("Transfer URL: " + transfer);
                    OutputStream out = new ByteArrayOutputStream();
                    HttpDownload download = new HttpDownload(transfer, out);
                    download.setFollowRedirects(true);
                    
                    download.run();
                    Assert.assertEquals("non 200 response", 200, download.getResponseCode());
                    Assert.assertEquals("wrong md5 in header", correctMD51, download.getContentMD5());
                    Assert.assertEquals("wrong size in header", correctSize1, Long.toString(download.getContentLength()));
                    
                    // test the md5 of the target node through the link
                    params = new StringBuilder();
                    params.append("TARGET=").append(URLEncoder.encode(linkURI.toString(), "UTF-8"));
                    params.append("&");
                    params.append("DIRECTION=").append(Direction.pullFromVoSpaceValue);
                    params.append("&");
                    params.append("PROTOCOL=").append(URLEncoder.encode(VOS.PROTOCOL_HTTPS_GET, "UTF-8"));
                    transfer = new URL(syncTrans.toString() + "?" + params.toString());
                    log.debug("Transfer URL: " + transfer);
                    out = new ByteArrayOutputStream();
                    download = new HttpDownload(transfer, out);
                    download.setFollowRedirects(true);
                    
                    download.run();
                    Assert.assertEquals("non 200 response", 200, download.getResponseCode());
                    Assert.assertEquals("wrong md5 in header through link", correctMD51, download.getContentMD5());
                    Assert.assertEquals("wrong size in header through link", correctSize1, Long.toString(download.getContentLength()));
          
                    return null;
                }
                
            });
            
            
            
        } catch (Exception unexpected) {
            log.error("unexpected exception", unexpected);
            Assert.fail("unexpected exception: " + unexpected);
        }
    }
    
    @Test
    public void testNodeContentMD5() throws Exception {
        VOSpaceClient vos = new VOSpaceClient(baseURI.getServiceURI());
        String vosuripath = baseURI.toString() + "/metadataIntTest-" + System.currentTimeMillis();
        VOSURI uri = new VOSURI(vosuripath);
        Subject s = SSLUtil.createSubject(SSL_CERT);

        try {
            final File testFile1 = new File("src/test/resources/md5file1");
            final File testFile2 = new File("src/test/resources/md5file2");
            final String correctSize1 = "25";
            final String correctSize2 = "33";
            final String correctMD51 = "86bec12f968870284258e4f239e1300c";
            final String correctMD52 = "7589efa0a3af85d73b6e9d37ec5f2e7c";
            final String incorrectMD5 = "12343d07086471dbf52398083a993cf7";

            TestActions.UploadNodeAction upload = null;
            Node result = null;

            // 1. put a new file, fail -> check md5 is null
            upload = new TestActions.UploadNodeAction(vos, uri, incorrectMD5, testFile1);
            result = Subject.doAs(s, upload);
            //get = new GetNodeAction(vos, uri.getPath());
            //result = Subject.doAs(s, get);
            log.debug("MD5: " + result.getPropertyValue(VOS.PROPERTY_URI_CONTENTMD5) + " (expecting null)");
            Assert.assertNull("Wrong MD5", result.getPropertyValue(VOS.PROPERTY_URI_CONTENTMD5));
            Assert.assertEquals("Wrong Size", "0", result.getPropertyValue(VOS.PROPERTY_URI_CONTENTLENGTH));

            // 2. put a new file -> check md5 correct
            upload = new TestActions.UploadNodeAction(vos, uri, correctMD51, testFile1);
            result = Subject.doAs(s, upload);
            log.debug("expected md5: " + correctMD51 + "  actual: " + result.getPropertyValue(VOS.PROPERTY_URI_CONTENTMD5));
            Assert.assertEquals("Wrong MD5", correctMD51, result.getPropertyValue(VOS.PROPERTY_URI_CONTENTMD5));
            Assert.assertEquals("Wrong Size", correctSize1, result.getPropertyValue(VOS.PROPERTY_URI_CONTENTLENGTH));

            // 3. replace a file -> check new md5 correct
            upload = new TestActions.UploadNodeAction(vos, uri, correctMD52, testFile2);
            result = Subject.doAs(s, upload);
            Assert.assertEquals("Wrong MD5", correctMD52, result.getPropertyValue(VOS.PROPERTY_URI_CONTENTMD5));
            Assert.assertEquals("Wrong Size", correctSize2, result.getPropertyValue(VOS.PROPERTY_URI_CONTENTLENGTH));

            // this implementation does not have any rollback mechanism to restore the previous copy after a failed write
            // 4. replace a file, fail -> check properties
            upload = new TestActions.UploadNodeAction(vos, uri, incorrectMD5, testFile1);
            result = Subject.doAs(s, upload);
            //get = new GetNodeAction(vos, uri.getPath());
            //result = Subject.doAs(s, get);
            log.debug("MD5: " + result.getPropertyValue(VOS.PROPERTY_URI_CONTENTMD5) + " (expecting null)");
            Assert.assertNull("Wrong MD5", result.getPropertyValue(VOS.PROPERTY_URI_CONTENTMD5));
            Assert.assertEquals("Wrong Size", "0", result.getPropertyValue(VOS.PROPERTY_URI_CONTENTLENGTH));
            
            //upload = new UploadNodeAction(vos, uri, incorrectMD5, testFile1);
            //try
            //{
            //    result = Subject.doAs(s, upload);
            //    Assert.fail("Should have failed due to incorrect md5");
            //}
            //catch (Exception e)
            //{
            //    log.debug(e);
            //    // expected
            //}
            //get = new GetNodeAction(vos, uri.getPath());
            //result = Subject.doAs(s, get);
            //Assert.assertEquals("Wrong MD5", correctMD52, result.getPropertyValue(VOS.PROPERTY_URI_CONTENTMD5));
            //Assert.assertEquals("Wrong Size", correctSize2, result.getPropertyValue(VOS.PROPERTY_URI_CONTENTLENGTH));
        } catch (Exception unexpected) {
            log.error("unexpected exception", unexpected);
            Assert.fail("unexpected exception: " + unexpected);
        }
    }
}
