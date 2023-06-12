/*
************************************************************************
*******************  CANADIAN ASTRONOMY DATA CENTRE  *******************
**************  CENTRE CANADIEN DE DONNÉES ASTRONOMIQUES  **************
*
*  (c) 2011.                            (c) 2011.
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

import ca.nrc.cadc.auth.RunnableAction;
import ca.nrc.cadc.auth.SSLUtil;
import ca.nrc.cadc.net.InputStreamWrapper;
import ca.nrc.cadc.net.OutputStreamWrapper;
import ca.nrc.cadc.reg.Standards;
import ca.nrc.cadc.util.FileUtil;
import ca.nrc.cadc.util.Log4jInit;
import ca.nrc.cadc.util.StringUtil;
import ca.nrc.cadc.vos.DataNode;
import ca.nrc.cadc.vos.Direction;
import ca.nrc.cadc.vos.LinkNode;
import ca.nrc.cadc.vos.Node;
import ca.nrc.cadc.vos.Protocol;
import ca.nrc.cadc.vos.Transfer;
import ca.nrc.cadc.vos.TransferWriter;
import ca.nrc.cadc.vos.VOS;
import ca.nrc.cadc.vos.VOSURI;
import ca.nrc.cadc.vos.client.ClientTransfer;
import ca.nrc.cadc.vos.client.VOSpaceClient;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URI;
import java.security.PrivilegedExceptionAction;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import javax.security.auth.Subject;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 * integration tests for custom behaviour not covered by the Push/Pull tests in cadcTestVOS.
 *
 * @author pdowler
 */
public class TransferRunnerTest {

    private static final Logger log = Logger.getLogger(TransferRunnerTest.class);

    private static File SSL_CERT;

    private static VOSURI nodeURI;
    
    private static final String FILE_DATA = "abc";

    static {
        Log4jInit.setLevel("org.opencadc.cavern", Level.DEBUG);
        Log4jInit.setLevel("ca.nrc.cadc.vospace", Level.INFO);
        Log4jInit.setLevel("ca.nrc.cadc.vos", Level.DEBUG);
        Log4jInit.setLevel("ca.nrc.cadc.rest", Level.DEBUG);
    }

    public TransferRunnerTest() {
    }

    @BeforeClass
    public static void staticInit() throws Exception {
        SSL_CERT = FileUtil.getFileFromResource("x509_CADCRegtest1.pem", TransferRunnerTest.class);

        String uriProp = TransferRunnerTest.class.getName() + ".baseURI";
        String uri = System.getProperty(uriProp);
        log.debug(uriProp + " = " + uri);
        if (StringUtil.hasText(uri)) {
            nodeURI = new VOSURI(new URI(uri));
        } else {
            throw new IllegalStateException("expected system property " + uriProp + " = <base vos URI>, found: " + uri);
        }
        
        // upload the test file to ensure it's there for download
        try {
            Subject s = SSLUtil.createSubject(SSL_CERT);
            List<Protocol> protocols = new ArrayList<Protocol>();
            Protocol basicTLS = new Protocol(VOS.PROTOCOL_HTTPS_PUT);
            basicTLS.setSecurityMethod(Standards.SECURITY_METHOD_CERT);
            protocols.add(basicTLS);
            VOSpaceClient vos = new VOSpaceClient(nodeURI.getServiceURI());
            DataNode data = new DataNode(new VOSURI(new URI(nodeURI + "/testFile.txt")));
            log.debug("uploading: " + data.getUri().getURI().toASCIIString());
            Transfer t = new Transfer(data.getUri().getURI(), Direction.pushToVoSpace);
            t.getProtocols().addAll(protocols);
            t.version = VOS.VOSPACE_21;
            
            final ClientTransfer trans = Subject.doAs(s, new TestActions.CreateTransferAction(vos, t, false));
            trans.setOutputStreamWrapper(new OutputStreamWrapper() {
                public void write(OutputStream out) throws IOException {
                    out.write(FILE_DATA.getBytes("UTF-8"));
                }});
            Subject.doAs(s, new PrivilegedExceptionAction<Object>() {
                public Object run() throws Exception {
                    trans.runTransfer();
                    return null;
                }});
            if (trans.getThrowable() != null) {
                throw trans.getThrowable();
            }
        } catch (Throwable t) {
            log.error("unexpected", t);
            Assert.fail("Failed to upload test file in test setup: " + t.getMessage());
        }
    }

    @Test
    public void testTransferNegotiation21() {
        try {
            Subject s = SSLUtil.createSubject(SSL_CERT);

            // Only HTTPS connections
            //Protocol anon = new Protocol(VOS.PROTOCOL_HTTP_GET);
            Protocol anonTLS = new Protocol(VOS.PROTOCOL_HTTPS_GET);

            //Protocol basic = new Protocol(VOS.PROTOCOL_HTTP_GET);
            //basic.setSecurityMethod(Standards.SECURITY_METHOD_HTTP_BASIC);

            //Protocol basicTLS = new Protocol(VOS.PROTOCOL_HTTPS_GET);
            //basicTLS.setSecurityMethod(Standards.SECURITY_METHOD_HTTP_BASIC);

            //Protocol cookie = new Protocol(VOS.PROTOCOL_HTTP_GET);
            //cookie.setSecurityMethod(Standards.SECURITY_METHOD_COOKIE);

            Protocol cookieTLS = new Protocol(VOS.PROTOCOL_HTTPS_GET);
            cookieTLS.setSecurityMethod(Standards.SECURITY_METHOD_COOKIE);

            //Protocol token = new Protocol(VOS.PROTOCOL_HTTP_GET);
            //token.setSecurityMethod(Standards.SECURITY_METHOD_TOKEN);

            Protocol tokenTLS = new Protocol(VOS.PROTOCOL_HTTPS_GET);
            tokenTLS.setSecurityMethod(Standards.SECURITY_METHOD_TOKEN);

            Protocol certTLS = new Protocol(VOS.PROTOCOL_HTTPS_GET);
            certTLS.setSecurityMethod(Standards.SECURITY_METHOD_CERT);

            List<Protocol> protocols = new ArrayList<Protocol>();
            //protocols.add(anon);
            protocols.add(anonTLS);
            //protocols.add(basic);
            //protocols.add(basicTLS);
            //protocols.add(cookie);
            protocols.add(cookieTLS);
            //protocols.add(token);
            protocols.add(tokenTLS);
            protocols.add(certTLS);

            VOSpaceClient vos = new VOSpaceClient(nodeURI.getServiceURI());
            DataNode data = new DataNode(new VOSURI(new URI(nodeURI + "/testFile.txt")));
            log.debug("testTransferNegotiation21: " + data.getUri().getURI().toASCIIString());
            Transfer t = new Transfer(data.getUri().getURI(), Direction.pullFromVoSpace);
            t.getProtocols().addAll(protocols);
            t.version = VOS.VOSPACE_21;

            ClientTransfer trans = Subject.doAs(s, new TestActions.CreateTransferAction(vos, t, false));
            Transfer trans2 = trans.getTransfer();
            log.debug("transfer result: " + trans2);
            Assert.assertEquals(VOS.VOSPACE_21, trans2.version);
            List<Protocol> plist = trans2.getProtocols();
            Assert.assertNotNull(plist);
            log.debug("found: " + plist.size() + " protocols");

            //Protocol anon2 = findProto(anon, plist);
            //Assert.assertNotNull(anon2);
            //Assert.assertNotNull(anon2.getEndpoint());
            //log.debug("anon: " + anon2.getEndpoint());

            //Protocol basic2 = findProto(basic, plist);
            //Assert.assertNotNull(basic2);
            //Assert.assertNotNull(basic2.getEndpoint());
            //log.debug("basic: " + basic2.getEndpoint());

            Protocol certTLS2 = findProto(certTLS, plist);
            Assert.assertNotNull(certTLS2);
            Assert.assertNotNull(certTLS2.getEndpoint());
            log.debug("certTLS: " + certTLS2.getEndpoint());

            Assert.assertTrue("anonTLS", plist.contains(anonTLS));
            Assert.assertTrue("cookieTLS", plist.contains(cookieTLS));

            //Assert.assertFalse("basicTLS", plist.contains(basicTLS));

            //Assert.assertFalse("token", plist.contains(token));
            Assert.assertTrue("tokenTLS", plist.contains(tokenTLS));
            
        } catch (Exception unexpected) {
            log.error("unexpected exception", unexpected);
            Assert.fail("unexpected exception: " + unexpected);
        }
    }

    Protocol findProto(Protocol p, List<Protocol> plist) {
        for (Protocol pp : plist) {
            if (pp.equals(p)) {
                return pp;
            }
        }
        return null;
    }

    @Test
    public void testPullFromVOSpace() {
        try {
            Subject s = SSLUtil.createSubject(SSL_CERT);

            VOSpaceClient vos = new VOSpaceClient(nodeURI.getServiceURI());
            DataNode data = new DataNode(new VOSURI(new URI(nodeURI + "/testFile.txt")));
            log.debug("testPullFromVOSpace: " + data.getUri().getURI().toASCIIString());

            List<Protocol> proto = new ArrayList<Protocol>();
            proto.add(new Protocol(VOS.PROTOCOL_HTTPS_GET));

            // https on transfer not supported
            //proto.add(new Protocol(VOS.PROTOCOL_HTTPS_GET));
            Transfer t = new Transfer(data.getUri().getURI(), Direction.pullFromVoSpace);
            t.getProtocols().addAll(proto);
            ClientTransfer trans = Subject.doAs(s, new TestActions.CreateTransferAction(vos, t, false));
            List<Protocol> plist = trans.getTransfer().getProtocols();
            Assert.assertNotNull(plist);
            log.debug("found: " + plist.size() + " protocols");

            for (Protocol reqP : proto) {
                int num = 0;
                for (Protocol p : plist) {
                    log.debug(p + " : " + data.getUri() + " -> " + p.getEndpoint());
                    Assert.assertNotNull(p.getEndpoint());
                    if (reqP.getUri().equals(p.getUri())) {
                        num++;
                    }
                }
                Assert.assertTrue("one or more endpoints for " + reqP.getUri(), (num > 0));
            }
            
            // check the file content
            final byte[] buf = new byte[100];
            TestWrapper tw = new TestWrapper(buf);
            trans.setInputStreamWrapper(tw);
            trans.runTransfer();
            Assert.assertTrue(trans.getThrowable() == null);
            int bytesRead = tw.size;
            Assert.assertEquals(FILE_DATA.length(), bytesRead);
            String result = new String(Arrays.copyOfRange(buf, 0, bytesRead));
            Assert.assertEquals(FILE_DATA, result);
            
        } catch (Exception unexpected) {
            log.error("unexpected exception", unexpected);
            Assert.fail("unexpected exception: " + unexpected);
        }
    }


    @Test
    public void testPullFromVOSpaceNotCommonFormURI() {
        try {
            Subject s = SSLUtil.createSubject(SSL_CERT);

            // Using '!' shouldn't stop the transfer from working
            String notCommonFormURI = "vos://cadc.nrc.ca!arc/home/cadcregtest1/vospace-static-test/transfer";

            VOSURI testURI = new VOSURI(new URI(notCommonFormURI));
            VOSpaceClient vos = new VOSpaceClient(testURI.getServiceURI());
            DataNode data = new DataNode(new VOSURI(new URI(testURI + "/testFile.txt")));
            log.debug("testPullFromVOSpace: " + data.getUri().getURI().toASCIIString());

            List<Protocol> proto = new ArrayList<Protocol>();
            proto.add(new Protocol(VOS.PROTOCOL_HTTPS_GET));

            Transfer t = new Transfer(data.getUri().getURI(), Direction.pullFromVoSpace);
            t.getProtocols().addAll(proto);
            ClientTransfer trans = Subject.doAs(s, new TestActions.CreateTransferAction(vos, t, false));
            List<Protocol> plist = trans.getTransfer().getProtocols();
            Assert.assertNotNull(plist);
            log.debug("found: " + plist.size() + " protocols");

            for (Protocol reqP : proto) {
                int num = 0;
                for (Protocol p : plist) {
                    log.debug(p + " : " + data.getUri() + " -> " + p.getEndpoint());
                    Assert.assertNotNull(p.getEndpoint());
                    if (reqP.getUri().equals(p.getUri())) {
                        num++;
                    }
                }
                Assert.assertTrue("one or more endpoints for " + reqP.getUri(), (num > 0));
            }

            // check the file content
            final byte[] buf = new byte[100];
            TestWrapper tw = new TestWrapper(buf);
            trans.setInputStreamWrapper(tw);
            trans.runTransfer();
            Assert.assertTrue(trans.getThrowable() == null);
            int bytesRead = tw.size;
            Assert.assertEquals(FILE_DATA.length(), bytesRead);
            String result = new String(Arrays.copyOfRange(buf, 0, bytesRead));
            Assert.assertEquals(FILE_DATA, result);

        } catch (Exception unexpected) {
            log.error("unexpected exception", unexpected);
            Assert.fail("unexpected exception: " + unexpected);
        }
    }


    @Test
    public void testNegotiateMount() {
        Protocol sp = new Protocol(VOS.PROTOCOL_SSHFS);
        try {
            Subject s = SSLUtil.createSubject(SSL_CERT);
            VOSpaceClient vos = new VOSpaceClient(nodeURI.getServiceURI());
            List<Protocol> protocols = new ArrayList<>();
            protocols.add(sp);
            Transfer t = new Transfer(nodeURI.getURI(), Direction.BIDIRECTIONAL);
            t.getProtocols().addAll(protocols);
            TransferWriter tw = new TransferWriter();
            ByteArrayOutputStream out = new ByteArrayOutputStream();
            tw.write(t, out);
            log.debug("sending transfer: " + out.toString());
            ClientTransfer trans = Subject.doAs(s, new TestActions.CreateTransferAction(vos, t, false));
            Transfer trans2 = trans.getTransfer();

            Assert.assertNotNull(trans2);
            Assert.assertFalse("result protocols", trans2.getProtocols().isEmpty());
            for (Protocol p : trans2.getProtocols()) {
                log.info("found protocol: " + p);
                Assert.assertEquals("protocol", sp, p);
                Assert.assertNotNull("endpoint", p.getEndpoint());
                log.info(p + " -> " + p.getEndpoint());
                URI uri = new URI(p.getEndpoint());
                Assert.assertEquals("sshfs", uri.getScheme());
            }
        } catch (Exception unexpected) {
            log.error("unexpected exception", unexpected);
            Assert.fail("unexpected exception: " + unexpected);
        }
    }

    @Test
    public void testDownloadLink() {
        try {
            VOSpaceClient vos = new VOSpaceClient(nodeURI.getServiceURI());

            // pre-existing data and link nodes:
            DataNode data = new DataNode(new VOSURI(new URI(nodeURI + "/testFile.txt")));
            log.debug("testDownloadLink: file = " + data.getUri().getURI().toASCIIString());
            LinkNode link = new LinkNode(new VOSURI(new URI(nodeURI + "/testLink.txt")), data.getUri().getURI());
            log.debug("testDownloadLink: link = " + link.getUri().getURI().toASCIIString());

            Subject s = SSLUtil.createSubject(SSL_CERT);

            List<Protocol> proto = new ArrayList<Protocol>();
            proto.add(new Protocol(VOS.PROTOCOL_HTTPS_GET));

            Transfer t = new Transfer(data.getUri().getURI(), Direction.pullFromVoSpace);
            t.getProtocols().addAll(proto);
            ClientTransfer trans = Subject.doAs(s, new TestActions.CreateTransferAction(vos, t, false));
            trans.setFile(new File("/tmp"));
            for (Protocol p : trans.getTransfer().getProtocols()) {
                log.debug(data.getUri() + " -> " + p.getEndpoint());
            }
            Subject.doAs(s, new RunnableAction(trans));

            log.debug("throwable: " + trans.getThrowable());
            Assert.assertNull(trans.getThrowable());
            File result = trans.getLocalFile();
            Assert.assertNotNull(result);
            Assert.assertEquals(data.getUri().getName(), result.getName()); // download DataNode, got right name

            t = new Transfer(link.getUri().getURI(), Direction.pullFromVoSpace);
            t.getProtocols().addAll(proto);
            trans = Subject.doAs(s, new TestActions.CreateTransferAction(vos, t, false));
            trans.setFile(new File("/tmp"));
            for (Protocol p : trans.getTransfer().getProtocols()) {
                log.debug(data.getUri() + " -> " + p.getEndpoint());
            }
            Subject.doAs(s, new RunnableAction(trans));

            Assert.assertNull(trans.getThrowable());
            result = trans.getLocalFile();
            Assert.assertNotNull(result);
            Assert.assertEquals(link.getUri().getName(), result.getName()); // download LinkNode, got right name
        } catch (Exception unexpected) {
            log.error("unexpected exception", unexpected);
            Assert.fail("unexpected exception: " + unexpected);
        }
    }
    
    class TestWrapper implements InputStreamWrapper {
        
        int size;
        byte[] buf;
        
        TestWrapper(byte[] buf) {
            this.buf = buf;
        }
        public void read(InputStream inputStream) throws IOException {
            size = inputStream.read(buf, 0, 100);
        }
    }
    
}
