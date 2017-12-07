package org.opencadc.cavern;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.OutputStream;
import java.net.URI;
import java.net.URL;
import java.security.PrivilegedExceptionAction;
import java.util.List;

import javax.security.auth.Subject;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import ca.nrc.cadc.auth.AuthMethod;
import ca.nrc.cadc.auth.SSLUtil;
import ca.nrc.cadc.net.HttpDownload;
import ca.nrc.cadc.reg.Standards;
import ca.nrc.cadc.reg.client.RegistryClient;
import ca.nrc.cadc.util.FileUtil;
import ca.nrc.cadc.util.Log4jInit;
import ca.nrc.cadc.util.StringUtil;
import ca.nrc.cadc.vos.ContainerNode;
import ca.nrc.cadc.vos.Node;
import ca.nrc.cadc.vos.NodeReader;
import ca.nrc.cadc.vos.VOSURI;

public class LinkNodeIntegrationTest
{
    private static final Logger log = Logger.getLogger(LinkNodeIntegrationTest.class);

    private static File SSL_CERT;

    private static String baseURL;
    private static URI nodeURI;

    static
    {
        Log4jInit.setLevel("ca.nrc.cadc.vospace.integration", Level.INFO);
        Log4jInit.setLevel("ca.nrc.cadc.vospace", Level.INFO);
        Log4jInit.setLevel("ca.nrc.cadc.vos", Level.INFO);
    }

    @BeforeClass
    public static void staticInit() throws Exception
    {
        SSL_CERT = FileUtil.getFileFromResource("x509_CADCRegtest1.pem", LinkNodeIntegrationTest.class);

        String uriProp = LinkNodeIntegrationTest.class.getName() + ".baseURI";
        String uri = System.getProperty(uriProp);
        log.debug(uriProp + " = " + uri);
        if ( StringUtil.hasText(uri) )
        {
            VOSURI vuri = new VOSURI(new URI(uri));
            nodeURI = vuri.getURI();
            RegistryClient rc = new RegistryClient();
            URL vos = rc.getServiceURL(vuri.getServiceURI(), Standards.VOSPACE_NODES_20, AuthMethod.CERT);
            baseURL = vos.toExternalForm();
        }
        else
            throw new IllegalStateException("expected system property " + uriProp + " = <base vos URI>, found: " + uri);
    }

    /**
     * Test that the child nodeIDs retrieved though a link node have the
     * correct URIs.
     */
    @Test
    public void testBatchChildren()
    {
        try
        {
            Subject subject = SSLUtil.createSubject(SSL_CERT);
            Subject.doAs(subject, new PrivilegedExceptionAction<Object>()
            {
                public Object run() throws Exception
                {
                    // get the parent
                    URL url = new URL(baseURL + nodeURI.getPath() + "/batchtargets");
                    log.debug("parent url: " + url);
                    OutputStream out = new ByteArrayOutputStream();
                    HttpDownload getNode = new HttpDownload(url, out);
                    getNode.run();
                    log.debug("throwable: " + getNode.getThrowable());
                    Assert.assertNull(getNode.getThrowable());
                    log.debug("responseCode: " + getNode.getResponseCode());
                    Assert.assertEquals(200, getNode.getResponseCode());
                    NodeReader reader = new NodeReader();
                    log.debug("body: " + out.toString());;
                    Node parent = reader.read(out.toString());

                    List<Node> children = ((ContainerNode) parent).getNodes();
                    Node lastChild = children.get(children.size() - 1);

                    url = new URL(baseURL + nodeURI.getPath() + "/batchtargets?uri=" + lastChild.getUri().toString());
                    log.debug("batch children url: " + url);
                    out = new ByteArrayOutputStream();
                    getNode = new HttpDownload(url, out);
                    getNode.run();
                    log.debug("throwable: " + getNode.getThrowable());
                    Assert.assertNull(getNode.getThrowable());
                    log.debug("responseCode: " + getNode.getResponseCode());
                    Assert.assertEquals(200, getNode.getResponseCode());
                    log.debug("body: " + out.toString());
                    return null;
                }
            });
        }
        catch (Throwable t)
        {
            log.error("unexpected error", t);
            Assert.fail("unexpected error " + t.getMessage());
        }
    }


}
