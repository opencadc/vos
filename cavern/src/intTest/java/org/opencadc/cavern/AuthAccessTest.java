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

import ca.nrc.cadc.auth.AuthMethod;
import ca.nrc.cadc.auth.AuthenticationUtil;
import ca.nrc.cadc.auth.NotAuthenticatedException;
import ca.nrc.cadc.auth.SSLUtil;
import ca.nrc.cadc.net.HttpGet;
import ca.nrc.cadc.net.HttpUpload;
import ca.nrc.cadc.net.NetUtil;
import ca.nrc.cadc.net.OutputStreamWrapper;
import ca.nrc.cadc.reg.Standards;
import ca.nrc.cadc.reg.client.RegistryClient;
import ca.nrc.cadc.util.FileUtil;
import ca.nrc.cadc.util.Log4jInit;
import ca.nrc.cadc.util.StringUtil;
import ca.nrc.cadc.vos.ContainerNode;
import ca.nrc.cadc.vos.DataNode;
import ca.nrc.cadc.vos.Direction;
import ca.nrc.cadc.vos.Node;
import ca.nrc.cadc.vos.NodeProperty;
import ca.nrc.cadc.vos.NodeWriter;
import ca.nrc.cadc.vos.VOS;
import ca.nrc.cadc.vos.VOSURI;

/**
 * Test to verify that deployment and libraries correctly implement authenticated access.
 *
 * @author pdowler
 */
public class AuthAccessTest
{
    private static Logger log = Logger.getLogger(AuthAccessTest.class);

    static
    {
        try
        {
            Log4jInit.setLevel("ca.nrc.cadc.vospace", Level.DEBUG);
            Log4jInit.setLevel("org.opencadc.cavern", Level.DEBUG);

        }
        catch(Throwable t)
        {
            throw new RuntimeException("failed to init SSL", t);
        }
    }

    private static File SSL_CERT;

    private static URL nodeResourceURL;
    private static URL asyncTransferURL;
    private static URL syncTransferURL;

    private static URL anonNodeResourceURL;
    private static URL anonAsyncTransferURL;
    private static URL anonSyncTransferURL;

    private static VOSURI nodeURI;
    private static Direction transferDirection = Direction.pushToVoSpace;
    
    private static Subject sub;

    @BeforeClass
    public static void staticInit() throws Exception
    {
        String nodeName = "authAccessTest-" + System.currentTimeMillis();
        String uriProp = AuthAccessTest.class.getName() + ".baseURI";
        String uri = System.getProperty(uriProp);
        log.debug(uriProp + " = " + uri);

        if ( StringUtil.hasText(uri) )
        {
            nodeURI = new VOSURI(new URI(uri + "/" + nodeName));
            RegistryClient rc = new RegistryClient();
//            URL vos = rc.getServiceURL(nodeURI.getServiceURI(), "https");
//            String baseURL = vos.toExternalForm();
//            nodeResourceURL = new URL(baseURL + "/nodes" + nodeURI.getPath());
//            asyncTransferURL = new URL(baseURL + "/transfers");
//            syncTransferURL = new URL(baseURL + "/synctrans");

            final URI serviceURI = nodeURI.getServiceURI();
            final URL nodesURL = rc.getServiceURL(serviceURI, Standards.VOSPACE_NODES_20, AuthMethod.CERT);
            nodeResourceURL = new URL(nodesURL.toExternalForm() + nodeURI.getPath());
            asyncTransferURL = rc.getServiceURL(serviceURI, Standards.VOSPACE_TRANSFERS_20, AuthMethod.CERT);
            syncTransferURL = rc.getServiceURL(serviceURI, Standards.VOSPACE_SYNC_21, AuthMethod.CERT);

            anonNodeResourceURL = nodeResourceURL;
            anonAsyncTransferURL = asyncTransferURL;
            anonSyncTransferURL = syncTransferURL;
            
            File crt = FileUtil.getFileFromResource("x509_CADCRegtest1.pem", AuthAccessTest.class);
            sub = SSLUtil.createSubject(crt);
            
            Subject.doAs(sub, new PrivilegedExceptionAction() {
                @Override
                public Object run() throws Exception {
                    ContainerNode cn = new ContainerNode(nodeURI.getParentURI());
                    putNode(new URL(nodesURL.toExternalForm() + cn.getUri().getPath()), cn);
                    DataNode dn = new DataNode(nodeURI);
                    dn.getProperties().add(new NodeProperty(VOS.PROPERTY_URI_ISPUBLIC, Boolean.FALSE.toString()));
                    putNode(nodeResourceURL, dn);
                    return null;
                }
            });

        }
        else
            throw new IllegalStateException("expected system property " + uriProp + " = <base vos URI>, found: " + uri);

        log.debug("node resource url: " + nodeResourceURL);
        log.debug("async transfer resource url: " + asyncTransferURL);
        log.debug("sync transfer resource url: " + syncTransferURL);
    }

    @AfterClass
    public static void staticShutdown() throws Exception
    {
        // try to delete the node
        HttpsURLConnection connection = (HttpsURLConnection) nodeResourceURL.openConnection();
        connection.setSSLSocketFactory(SSLUtil.getSocketFactory(sub));
        connection.setRequestMethod("DELETE");
        log.debug("Delete node response code: " + connection.getResponseCode());
    }

    @Test
    public void testAnonAccessNode()
    {
        try
        {
            log.debug("testAnonAccess: " + anonNodeResourceURL);
            ByteArrayOutputStream bos = new ByteArrayOutputStream();
            HttpGet get = new HttpGet(anonNodeResourceURL, bos);
            get.run();
            Assert.assertEquals(401, get.getResponseCode());
            Assert.assertNotNull(get.getThrowable());
            log.debug("throwable: " + get.getThrowable());
            Assert.assertTrue( (get.getThrowable() instanceof NotAuthenticatedException) );
        }
        catch(Exception unexpected)
        {
            log.error("unexpected exception", unexpected);
            Assert.fail("unexpected exception: " + unexpected);
        }
    }

    @Test
    public void testFakeUsernameAccessNode()
    {
        try
        {
            log.debug("testFakeUsernameAccess: " + anonNodeResourceURL);
            ByteArrayOutputStream bos = new ByteArrayOutputStream();
            HttpGet get = new HttpGet(anonNodeResourceURL, bos);
            get.setRequestProperty("authorization", "Basic Y2FkY3JlZ3Rlc3QxOnh5eg=="); // cadcregtest1:xyz
            get.run();
            Assert.assertEquals(401, get.getResponseCode());
            Assert.assertNotNull(get.getThrowable());
            Assert.assertTrue( (get.getThrowable() instanceof NotAuthenticatedException) );
        }
        catch(Exception unexpected)
        {
            log.error("unexpected exception", unexpected);
            Assert.fail("unexpected exception: " + unexpected);
        }
    }

    @Test
    public void testAnonAccessSynctrans()
    {
        try
        {
            StringBuilder sb = new StringBuilder();
            sb.append(anonSyncTransferURL);
            sb.append("?TARGET=").append( NetUtil.encode(nodeURI.getURI().toASCIIString()) );
            sb.append("&DIRECTION=").append( NetUtil.encode(Direction.pullFromVoSpaceValue) );
            sb.append("&PROTOCOL=").append( NetUtil.encode(VOS.PROTOCOL_HTTP_GET) );
            URL url = new URL(sb.toString());
            log.debug("testAnonAccessSynctrans: " + url);

            ByteArrayOutputStream bos = new ByteArrayOutputStream();
            HttpGet get = new HttpGet(url, bos);
            get.run();
            Assert.assertEquals(403, get.getResponseCode());
            Assert.assertNotNull(get.getThrowable());
            log.debug("throwable: " + get.getThrowable());
            Assert.assertTrue( (get.getThrowable() instanceof AccessControlException) );
        }
        catch(Exception unexpected)
        {
            log.error("unexpected exception", unexpected);
            Assert.fail("unexpected exception: " + unexpected);
        }
    }

    @Test
    public void testFakeUsernameAccessSynctrans()
    {
        try
        {

            StringBuilder sb = new StringBuilder();
            sb.append(anonSyncTransferURL);
            sb.append("?TARGET=").append( NetUtil.encode(nodeURI.getURI().toASCIIString()) );
            sb.append("&DIRECTION=").append( NetUtil.encode(Direction.pullFromVoSpaceValue) );
            sb.append("&PROTOCOL=").append( NetUtil.encode(VOS.PROTOCOL_HTTP_GET) );
            URL url = new URL(sb.toString());

            log.debug("testFakeUsernameAccessSynctrans: " + url);
            ByteArrayOutputStream bos = new ByteArrayOutputStream();
            HttpGet get = new HttpGet(url, bos);
            get.setRequestProperty("authorization", "Basic Y2FkY3JlZ3Rlc3QxOnh5eg=="); // cadcregtest1:xyz
            get.run();
            Assert.assertEquals(403, get.getResponseCode());
            Assert.assertNotNull(get.getThrowable());
            Assert.assertTrue( (get.getThrowable() instanceof AccessControlException) );
        }
        catch(Exception unexpected)
        {
            log.error("unexpected exception", unexpected);
            Assert.fail("unexpected exception: " + unexpected);
        }
    }

    private static void putNode(URL url, final Node node) throws Exception
    {
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
        if (node instanceof ContainerNode && code == 409) // conflict = exists
            return; // ok
        if (code >= 400)
            throw new RuntimeException("put node failed: " + code, upload.getThrowable());
    }
}
