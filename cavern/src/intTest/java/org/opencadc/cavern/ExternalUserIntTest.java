package org.opencadc.cavern;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.OutputStreamWriter;
import java.net.URI;
import java.net.URL;
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
import ca.nrc.cadc.auth.SSLUtil;
import ca.nrc.cadc.net.HttpDownload;
import ca.nrc.cadc.reg.Standards;
import ca.nrc.cadc.reg.client.RegistryClient;
import ca.nrc.cadc.util.FileUtil;
import ca.nrc.cadc.util.Log4jInit;
import ca.nrc.cadc.util.StringUtil;
import ca.nrc.cadc.vos.ContainerNode;
import ca.nrc.cadc.vos.DataNode;
import ca.nrc.cadc.vos.Node;
import ca.nrc.cadc.vos.NodeProperty;
import ca.nrc.cadc.vos.NodeWriter;
import ca.nrc.cadc.vos.VOS;
import ca.nrc.cadc.vos.VOSURI;

public class ExternalUserIntTest
{

    private static Logger log = Logger.getLogger(ExternalUserIntTest.class);

    static
    {
        try
        {
            Log4jInit.setLevel("ca.nrc.cadc.vospace", Level.INFO);
            Log4jInit.setLevel("ca.nrc.cadc.auth", Level.INFO);


            File regCrt = FileUtil.getFileFromResource("x509_CADCRegtest1.pem", ExternalUserIntTest.class);
            File anonCrt = FileUtil.getFileFromResource("x509_CADCAnontest1.pem", ExternalUserIntTest.class);

            regSubject = SSLUtil.createSubject(regCrt);
            anonSubject = SSLUtil.createSubject(regCrt);
        }
        catch(Throwable t)
        {
            throw new RuntimeException("failed to init SSL", t);
        }
    }

    private static Subject regSubject;
    private static Subject anonSubject;

    private static URL nodeResourceURL;
    private static URL asyncTransferURL;
    private static URL syncTransferURL;

    private static VOSURI nodeURI;

    @BeforeClass
    public static void staticInit() throws Exception
    {
        String nodeName = "externalAccessTest-" + System.currentTimeMillis();
        String uriProp = ExternalUserIntTest.class.getName() + ".baseURI";
        String uri = System.getProperty(uriProp);
        log.debug(uriProp + " = " + uri);

        if ( StringUtil.hasText(uri) )
        {
            nodeURI = new VOSURI(new URI(uri + "/" + nodeName));
            RegistryClient rc = new RegistryClient();
            URL nodeServiceURL = rc.getServiceURL(nodeURI.getServiceURI(), Standards.VOSPACE_NODES_20, AuthMethod.CERT);
            String baseURL = nodeServiceURL.toExternalForm();

            nodeResourceURL = new URL(baseURL + nodeURI.getPath());
            asyncTransferURL = rc.getServiceURL(nodeURI.getServiceURI(), Standards.VOSPACE_TRANSFERS_20, AuthMethod.CERT);
            syncTransferURL = rc.getServiceURL(nodeURI.getServiceURI(), Standards.VOSPACE_SYNC_21, AuthMethod.CERT);

            log.debug("node resource url: " + nodeResourceURL);
            log.debug("async transfer resource url: " + asyncTransferURL);
            log.debug("sync transfer resource url: " + syncTransferURL);

            ContainerNode cn = new ContainerNode(nodeURI.getParentURI());
            URL putNodeURL = new URL(baseURL + cn.getUri().getPath());
            log.debug("PUT Node: " + putNodeURL.toExternalForm());

            putNode(putNodeURL, cn, regSubject);
            DataNode dn = new DataNode(nodeURI);
            dn.getProperties().add(new NodeProperty(VOS.PROPERTY_URI_ISPUBLIC, Boolean.TRUE.toString()));
            putNode(nodeResourceURL, dn, regSubject);
        }
        else
        {
            throw new IllegalStateException("expected system property " + uriProp + " = <base vos URI>, found: " + uri);
        }
    }

    @AfterClass
    public static void staticShutdown() throws Exception
    {
        // try to delete the node
        HttpsURLConnection connection = (HttpsURLConnection) nodeResourceURL.openConnection();
        connection.setSSLSocketFactory(SSLUtil.getSocketFactory(regSubject));
        connection.setRequestMethod("DELETE");
        log.debug("Delete node response code: " + connection.getResponseCode());
    }

    @Test
    public void testExternalAccessToPublicNode()
    {
        try
        {
            Subject.doAs(anonSubject, new PrivilegedExceptionAction<Object>()
            {

                @Override
                public Object run() throws Exception
                {
                    log.debug("testExternalAccessToPublicNode: " + nodeResourceURL);
                    ByteArrayOutputStream bos = new ByteArrayOutputStream();
                    HttpDownload get = new HttpDownload(nodeResourceURL, bos);
                    get.run();
                    Assert.assertEquals(200, get.getResponseCode());
                    Assert.assertNull(get.getThrowable());
                    return null;
                }
            });
        }
        catch(Exception unexpected)
        {
            log.error("unexpected exception", unexpected);
            Assert.fail("unexpected exception: " + unexpected);
        }
    }

    private static void putNode(URL url, Node node, Subject subject) throws Exception
    {
        HttpsURLConnection connection = (HttpsURLConnection) url.openConnection();
        connection.setDoOutput(true);
        connection.setRequestMethod("PUT");
        connection.setRequestProperty("Content-Type", "text/xml");
        connection.setUseCaches(false);
        connection.setDoInput(true);
        connection.setDoOutput(true);

        connection.setSSLSocketFactory(SSLUtil.getSocketFactory(subject));

        OutputStreamWriter out = new OutputStreamWriter(connection.getOutputStream());
        NodeWriter nodeWriter = new NodeWriter();
        nodeWriter.write(node, out);
        out.close();

        int code = connection.getResponseCode();
        if (node instanceof ContainerNode && code == 409) // conflict = exists
            return; // ok
        if (code >= 400)
            throw new RuntimeException("put node failed: " + code
                    + " " + connection.getResponseMessage());
    }

}
