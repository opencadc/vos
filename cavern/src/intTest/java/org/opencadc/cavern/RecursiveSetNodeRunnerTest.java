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

import java.io.File;
import java.io.OutputStream;
import java.io.StringWriter;
import java.io.Writer;
import java.net.HttpURLConnection;
import java.net.URI;
import java.net.URL;
import java.security.AccessControlContext;
import java.security.AccessController;
import java.security.PrivilegedExceptionAction;

import javax.net.ssl.HttpsURLConnection;
import javax.security.auth.Subject;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import ca.nrc.cadc.auth.AuthMethod;
import ca.nrc.cadc.auth.RunnableAction;
import ca.nrc.cadc.auth.SSLUtil;
import ca.nrc.cadc.net.HttpPost;
import ca.nrc.cadc.net.NetUtil;
import ca.nrc.cadc.reg.Standards;
import ca.nrc.cadc.reg.client.RegistryClient;
import ca.nrc.cadc.util.FileUtil;
import ca.nrc.cadc.util.Log4jInit;
import ca.nrc.cadc.uws.ErrorSummary;
import ca.nrc.cadc.uws.ExecutionPhase;
import ca.nrc.cadc.uws.Job;
import ca.nrc.cadc.uws.JobReader;
import ca.nrc.cadc.vos.ContainerNode;
import ca.nrc.cadc.vos.DataNode;
import ca.nrc.cadc.vos.Node;
import ca.nrc.cadc.vos.NodeProperty;
import ca.nrc.cadc.vos.NodeWriter;
import ca.nrc.cadc.vos.VOS;
import ca.nrc.cadc.vos.VOSURI;
import ca.nrc.cadc.vos.client.ClientRecursiveSetNode;
import ca.nrc.cadc.vos.client.VOSpaceClient;

/**
 * Integration tests for the recursive setting of node properties.
 *
 * @author yeunga
 */
public class RecursiveSetNodeRunnerTest
{
    private static final Logger log = Logger.getLogger(RecursiveSetNodeRunnerTest.class);

    private static String BASE_URI_STRING = "vos://cadc.nrc.ca~arc/home/cadcregtest1/vospace-int-test/recursiveprops";
    private static File SSL_CERT;

    private static String baseURL;
    private static VOSURI nodeURI;

    static
    {
        Log4jInit.setLevel("ca.nrc.cadc.vospace", Level.INFO);
        Log4jInit.setLevel("ca.nrc.cadc.vos", Level.DEBUG);
        Log4jInit.setLevel("org.opencadc.cadc.reg", Level.DEBUG);
        Log4jInit.setLevel("org.opencadc.cavern", Level.DEBUG);
    }

    public RecursiveSetNodeRunnerTest() { }

    @BeforeClass
    public static void staticInit() throws Exception
    {
        SSL_CERT = FileUtil.getFileFromResource("x509_CADCRegtest1.pem", RecursiveSetNodeRunnerTest.class);

        nodeURI = new VOSURI(new URI(BASE_URI_STRING));
        RegistryClient rc = new RegistryClient();
        URL vos = rc.getServiceURL(nodeURI.getServiceURI(), Standards.VOSPACE_NODEPROPS_20, AuthMethod.CERT);
        baseURL = vos.toExternalForm();
    }

    private static class CreateAction implements PrivilegedExceptionAction<Node>
    {
        VOSpaceClient vosClient;
        Node node;
        CreateAction(VOSpaceClient vos, Node node)
        {
            this.vosClient = vos;
            this.node = node;
        }
        public Node run() throws Exception
        {
            return vosClient.createNode(node);
        }
    }

    private static class RecursiveNodePropsAction implements PrivilegedExceptionAction<ClientRecursiveSetNode>
    {
        VOSpaceClient vosClient;
        Node node;

        RecursiveNodePropsAction(VOSpaceClient vos, Node node)
        {
            this.vosClient = vos;
            this.node = node;
        }
        public ClientRecursiveSetNode run() throws Exception
        {
            return vosClient.setNodeRecursive(node);
        }
    }

    private static class GetNodeAction implements PrivilegedExceptionAction<Node>
    {
        VOSpaceClient vosClient;
        String path;

        GetNodeAction(VOSpaceClient vos, String path)
        {
            this.vosClient = vos;
            this.path = path;
        }
        public Node run() throws Exception
        {
            return vosClient.getNode(path);
        }
    }

    private static class DeleteNodeAction implements PrivilegedExceptionAction<Node>
    {
        VOSpaceClient vosClient;
        String path;

        DeleteNodeAction(VOSpaceClient vos, String path)
        {
            this.vosClient = vos;
            this.path = path;
        }
        public Node run() throws Exception
        {
            try
            {
                vosClient.deleteNode(path);
            }
            catch (Exception ignore) {}
            return null;
        }
    }
    
    private static class MyJobStatus {
        ExecutionPhase ep;
        ErrorSummary es;
    }
    private static class GetJobAction implements PrivilegedExceptionAction<MyJobStatus>
    {
        ClientRecursiveSetNode rec;

        GetJobAction(ClientRecursiveSetNode rec)
        {
            this.rec = rec;
        }
        public MyJobStatus run() throws Exception
        {
            MyJobStatus ret = new MyJobStatus();
            ret.ep = rec.getPhase();
            ret.es = rec.getServerError();
            return ret;
        }
    }

    @Test
    public void testNotFound() throws Throwable
    {
        VOSpaceClient vos = new VOSpaceClient(nodeURI.getServiceURI());

        Subject s = SSLUtil.createSubject(SSL_CERT);
        DataNode dataNode = new DataNode(new VOSURI(new URI(nodeURI + "/nonexistentNode")));

        RecursiveNodePropsAction recPropsAction = new RecursiveNodePropsAction(vos, dataNode);

        ClientRecursiveSetNode recSetNode = Subject.doAs(s, recPropsAction);
        recSetNode.setMonitor(true);
        Subject.doAs(s, new RunnableAction(recSetNode));
        if (recSetNode.getThrowable() != null)
            throw recSetNode.getThrowable();
        MyJobStatus js = Subject.doAs(s, new GetJobAction(recSetNode)); 
        Assert.assertNotNull("job status", js);
        Assert.assertTrue("expected ERROR phase", ExecutionPhase.ERROR.equals(js.ep));
        Assert.assertTrue("expected NotFound message", js.es.getSummaryMessage().equals("NotFound"));
    }

    @Test
    public void testNoReadPermission() throws Throwable
    {
        VOSpaceClient vos = new VOSpaceClient(nodeURI.getServiceURI());

        Subject s = SSLUtil.createSubject(SSL_CERT);
        DataNode dataNode = new DataNode(new VOSURI(new URI("vos://cadc.nrc.ca~arc/home/cadcauthtest1")));

        RecursiveNodePropsAction recPropsAction = new RecursiveNodePropsAction(vos, dataNode);

        ClientRecursiveSetNode recSetNode = Subject.doAs(s, recPropsAction);
        recSetNode.setMonitor(true);
        Subject.doAs(s, new RunnableAction(recSetNode));
        if (recSetNode.getThrowable() != null)
            throw recSetNode.getThrowable();
        
        MyJobStatus js = Subject.doAs(s, new GetJobAction(recSetNode)); 
        Assert.assertNotNull("job status", js);
        Assert.assertTrue("expected ERROR phase", ExecutionPhase.ERROR.equals(js.ep));
        Assert.assertTrue("expected Success count: 0 Failure count: 1", js.es.getSummaryMessage().equalsIgnoreCase("Success count: 0 Failure count: 1"));
    }

    @Test
    public void testSingleNode() throws Throwable
    {
        VOSpaceClient vos = new VOSpaceClient(nodeURI.getServiceURI());
        Subject s = SSLUtil.createSubject(SSL_CERT);
        DataNode data = new DataNode(new VOSURI(new URI(nodeURI + "/dataNode-" + System.currentTimeMillis())));
//        DataNode data = new DataNode(new VOSURI(new URI(nodeURI + "/dataNode")));
        try
        {
            Node data2 = Subject.doAs(s, new CreateAction(vos, data));
            NodeProperty groupRead = new NodeProperty(VOS.PROPERTY_URI_GROUPREAD, "ivo://cadc.nrc.ca/gms#cadcauthtest1");
            NodeProperty groupWrite = new NodeProperty(VOS.PROPERTY_URI_GROUPWRITE, "ivo://cadc.nrc.ca/gms#cadcauthtest2");
            data2.getProperties().add(groupRead);
            data2.getProperties().add(groupWrite);

            RecursiveNodePropsAction recPropsAction = new RecursiveNodePropsAction(vos, data2);

            ClientRecursiveSetNode recSetNode = Subject.doAs(s, recPropsAction);
            recSetNode.setMonitor(true);
            Subject.doAs(s, new RunnableAction(recSetNode));
            if (recSetNode.getThrowable() != null)
                throw recSetNode.getThrowable();
            
            MyJobStatus js = Subject.doAs(s, new GetJobAction(recSetNode));
            Assert.assertNotNull("job status", js);
            Assert.assertTrue("expected COMPLETED phase", ExecutionPhase.COMPLETED.equals(js.ep));
            Assert.assertNull("expected null error message", js.es);

            GetNodeAction getNode = new GetNodeAction(vos, data2.getUri().getPath());
            Node finalNode = Subject.doAs(s, getNode);
            this.assertContainsProperty(finalNode, groupRead);
            this.assertContainsProperty(finalNode, groupWrite);
        }
        finally
        {
            // clean up
            DeleteNodeAction deleteNode = new DeleteNodeAction(vos, data.getUri().getPath());
            Subject.doAs(s, deleteNode);
        }
    }

    @Test
    public void testNodeTree() throws Throwable
    {
        VOSpaceClient vos = new VOSpaceClient(nodeURI.getServiceURI());
        Subject s = SSLUtil.createSubject(SSL_CERT);

        long time = System.currentTimeMillis();

        Node startContainer = null;

        try
        {

            ContainerNode c1 =   new ContainerNode(new VOSURI(new URI(nodeURI + "/1-" + time)));
            ContainerNode c11 =  new ContainerNode(new VOSURI(new URI(nodeURI + "/1-" + time + "/1" )));
            ContainerNode c12 =  new ContainerNode(new VOSURI(new URI(nodeURI + "/1-" + time + "/2")));
            ContainerNode c121 = new ContainerNode(new VOSURI(new URI(nodeURI + "/1-" + time + "/2/1")));
            DataNode d1 =             new DataNode(new VOSURI(new URI(nodeURI + "/1-" + time + "/d1")));
            DataNode d2 =             new DataNode(new VOSURI(new URI(nodeURI + "/1-" + time + "/2/d2")));
            DataNode d3 =             new DataNode(new VOSURI(new URI(nodeURI + "/1-" + time + "/2/d3")));
            DataNode d4 =             new DataNode(new VOSURI(new URI(nodeURI + "/1-" + time + "/2/1/d4")));
            DataNode d5 =             new DataNode(new VOSURI(new URI(nodeURI + "/1-" + time + "/2/1/d5")));
            DataNode d6 =             new DataNode(new VOSURI(new URI(nodeURI + "/1-" + time + "/2/1/d6")));

            startContainer = Subject.doAs(s, new CreateAction(vos, c1));
            Subject.doAs(s, new CreateAction(vos, c11));
            Subject.doAs(s, new CreateAction(vos, c12));
            Subject.doAs(s, new CreateAction(vos, c121));
            Subject.doAs(s, new CreateAction(vos, d1));
            Subject.doAs(s, new CreateAction(vos, d2));
            Subject.doAs(s, new CreateAction(vos, d3));
            Subject.doAs(s, new CreateAction(vos, d4));
            Subject.doAs(s, new CreateAction(vos, d5));
            Subject.doAs(s, new CreateAction(vos, d6));

            NodeProperty groupRead = new NodeProperty(VOS.PROPERTY_URI_GROUPREAD, "ivo://cadc.nrc.ca/gms#cadcauthtest1");
            NodeProperty groupWrite = new NodeProperty(VOS.PROPERTY_URI_GROUPWRITE, "ivo://cadc.nrc.ca/gms#cadcauthtest2");
            startContainer.getProperties().add(groupRead);
            startContainer.getProperties().add(groupWrite);

            RecursiveNodePropsAction recPropsAction = new RecursiveNodePropsAction(vos, startContainer);

            ClientRecursiveSetNode recSetNode = Subject.doAs(s, recPropsAction);
            recSetNode.setMonitor(true);
            Subject.doAs(s, new RunnableAction(recSetNode));
            if (recSetNode.getThrowable() != null)
                throw recSetNode.getThrowable();
            
            MyJobStatus js = Subject.doAs(s, new GetJobAction(recSetNode));
            Assert.assertNotNull("job status", js);
            Assert.assertTrue("expected COMPLETED phase", ExecutionPhase.COMPLETED.equals(js.ep));
            Assert.assertNull("expected null error message", js.es);

            Node[] nodeList = new Node[] {c1, c11, c12, c121, d1, d2, d3, d4, d5, d6};
            for (Node n : nodeList)
            {
                GetNodeAction getNode = new GetNodeAction(vos, n.getUri().getPath());
                Node result = Subject.doAs(s, getNode);
                this.assertContainsProperty(result, groupRead);
                this.assertContainsProperty(result, groupWrite);
            }
        }
        finally
        {
            // cleanup
            if (startContainer != null)
            {
                DeleteNodeAction deleteNode = new DeleteNodeAction(vos, startContainer.getUri().getPath());
                Subject.doAs(s, deleteNode);
            }
        }
    }

    /**
     * Commented out--job completes before abort can happen most of the time.
     * 
     */
    //@Test
    public void testAbortJob() throws Throwable
    {
        final VOSpaceClient vos = new VOSpaceClient(nodeURI.getServiceURI());
        final Subject s = SSLUtil.createSubject(SSL_CERT);

        long time = System.currentTimeMillis();

        String uri = nodeURI + "/abort-" + time;
        ContainerNode c =   new ContainerNode(new VOSURI(new URI(uri)));
        DataNode d = null;
        final Node startContainer = Subject.doAs(s, new CreateAction(vos, c));

        try
        {

            // make a deep tree of nodes
            for (int i=0; i<4; i++)
            {
                for (int j=0; j<10; j++)
                {
                    d = new DataNode(new VOSURI(new URI(uri + "/d" + j)));
                    Subject.doAs(s, new CreateAction(vos, d));
                }

                uri = uri + "/c" + i;
                c =   new ContainerNode(new VOSURI(new URI(uri)));
                Subject.doAs(s, new CreateAction(vos, c));
            }

            final NodeProperty groupRead = new NodeProperty(VOS.PROPERTY_URI_GROUPREAD, "ivo://cadc.nrc.ca/gms#testAbort");
            startContainer.getProperties().add(groupRead);

            Subject.doAs(s, new PrivilegedExceptionAction<Object>()
            {
                public Object run() throws Exception
                {
                    // post the job
                    String asyncNodePropsUrl = baseURL;
                    NodeWriter nodeWriter = new NodeWriter();
                    Writer stringWriter = new StringWriter();
                    nodeWriter.write(startContainer, stringWriter);
                    URL postUrl = new URL(asyncNodePropsUrl);

                    log.debug("baseURL: " + baseURL);
                    log.debug("posting to: " + postUrl);

                    HttpPost httpPost = new HttpPost(postUrl, stringWriter.toString(), "text/xml", false);
                    httpPost.run();
                    if (httpPost.getThrowable() != null)
                        throw new RuntimeException(httpPost.getThrowable());

                    URL jobURL = httpPost.getRedirectURL();
                    log.debug("Job URL is: " + jobURL.toString());

                    // start the job
                    manageJob(jobURL, "RUN");

                    ExecutionPhase ep = getPhase(jobURL);
                    log.debug("phase is " + ep);
                    int loop = 0;
                    while (!ep.equals(ExecutionPhase.EXECUTING) && loop < 50)
                    {
                        try
                        {
                            log.debug("waiting for 10 ms");
                            Thread.sleep(10);
                        }
                        catch (InterruptedException ignore) {}

                        ep = getPhase(jobURL);
                        log.debug("phase is " + ep);
                        loop++;
                    }

                    if (!ep.equals(ExecutionPhase.EXECUTING))
                    {
                        Assert.fail("Job didn't start");
                    }

                    // abort job
                    manageJob(jobURL, "ABORT");

                    ep = getPhase(jobURL);
                    Assert.assertEquals("Wrong phase", ExecutionPhase.ABORTED, ep);

                    // make sure that not all the nodes received the update
                    Assert.assertFalse("Job not aborted", containsPropertyRecursive(vos, s, startContainer, groupRead));
                    return null;
                }
            });

        }
        finally
        {
            // cleanup
            if (startContainer != null)
            {
                DeleteNodeAction deleteNode = new DeleteNodeAction(vos, startContainer.getUri().getPath());
                Subject.doAs(s, deleteNode);
            }
        }
    }

    private ExecutionPhase getPhase(URL jobURL) throws Exception
    {
        HttpURLConnection conn = (HttpURLConnection) jobURL.openConnection();
        if (conn instanceof HttpsURLConnection)
        {
            HttpsURLConnection sslConn = (HttpsURLConnection) conn;
            initHTTPS(sslConn);
        }
        conn.setRequestMethod("GET");
        conn.setUseCaches(false);
        conn.setDoInput(true);
        conn.setDoOutput(false);
        JobReader jobReader = new JobReader(true);
        Job job = jobReader.read(conn.getInputStream());

        return job.getExecutionPhase();
    }

    private void manageJob(URL jobURL, String phase) throws Exception
    {
        URL phaseURL = new URL(jobURL.toExternalForm() + "/phase");
        String parameters = "PHASE=" + phase;

        HttpURLConnection connection = (HttpURLConnection) phaseURL.openConnection();
        if (connection instanceof HttpsURLConnection)
            initHTTPS((HttpsURLConnection) connection);
        connection.setRequestMethod("POST");
        connection.setRequestProperty("Content-Length", "" + Integer.toString(parameters.getBytes().length));
        connection.setRequestProperty("Content-Type", "application/x-www-form-urlencoded");
        connection.setInstanceFollowRedirects(false);
        connection.setUseCaches(false);
        connection.setDoOutput(true);
        connection.setDoInput(true);

        OutputStream outputStream = connection.getOutputStream();
        outputStream.write(parameters.getBytes("UTF-8"));
        outputStream.close();

        int responseCode = connection.getResponseCode();
        log.debug("phase=" + phase + " response code: " + responseCode);
        String errorBody = NetUtil.getErrorBody(connection);
    }

    private boolean containsPropertyRecursive(VOSpaceClient vos, Subject s, Node node, NodeProperty prop)
    throws Exception
    {
        GetNodeAction get = new GetNodeAction(vos, node.getUri().getPath());
        node = Subject.doAs(s, get);

        boolean containsProp = false;
        for (NodeProperty np : node.getProperties())
        {
            if (np.equals(prop))
                containsProp = true;
        }

        log.debug("Check contains: " + containsProp);

        if (!containsProp)
            return false;

        if (node instanceof ContainerNode)
        {
            ContainerNode container = (ContainerNode) node;
            for (Node child : container.getNodes())
            {
                if (!containsPropertyRecursive(vos, s, child, prop))
                    return false;
            }
        }
        return true;
    }

    private void assertContainsProperty(Node node, NodeProperty prop)
    {
        for (NodeProperty np : node.getProperties())
        {
            if (np.equals(prop))
                return;
        }
        Assert.fail("Property " + prop + " not found.");
    }

    private void initHTTPS(HttpsURLConnection sslConn)
    {
        AccessControlContext ac = AccessController.getContext();
        Subject s = Subject.getSubject(ac);
        sslConn.setSSLSocketFactory(SSLUtil.getSocketFactory(s));
    }

}
