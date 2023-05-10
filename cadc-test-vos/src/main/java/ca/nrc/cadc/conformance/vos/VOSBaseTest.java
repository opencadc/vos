/*
************************************************************************
*******************  CANADIAN ASTRONOMY DATA CENTRE  *******************
**************  CENTRE CANADIEN DE DONNÉES ASTRONOMIQUES  **************
*
*  (c) 2021.                            (c) 2021.
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

package ca.nrc.cadc.conformance.vos;

import static org.junit.Assert.assertNotNull;

import ca.nrc.cadc.auth.SSLUtil;
import ca.nrc.cadc.auth.X509CertificateChain;
import ca.nrc.cadc.util.FileUtil;
import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.net.MalformedURLException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.text.DateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.List;
import java.util.Map;

import javax.net.ssl.HttpsURLConnection;
import javax.net.ssl.SSLSocketFactory;
import org.apache.log4j.Logger;
import org.junit.After;
import org.junit.Before;
import org.xml.sax.SAXException;

import com.meterware.httpunit.GetMethodWebRequest;
import com.meterware.httpunit.PostMethodWebRequest;
import com.meterware.httpunit.PutMethodWebRequest;
import com.meterware.httpunit.WebConversation;
import com.meterware.httpunit.WebRequest;
import com.meterware.httpunit.WebResponse;

import ca.nrc.cadc.auth.AuthMethod;
import ca.nrc.cadc.date.DateUtil;
import ca.nrc.cadc.reg.client.RegistryClient;
import ca.nrc.cadc.vos.ContainerNode;
import ca.nrc.cadc.vos.DataNode;
import ca.nrc.cadc.vos.LinkNode;
import ca.nrc.cadc.vos.Node;
import ca.nrc.cadc.vos.NodeProperty;
import ca.nrc.cadc.vos.NodeWriter;
import ca.nrc.cadc.vos.VOS.NodeBusyState;
import ca.nrc.cadc.vos.VOSException;
import ca.nrc.cadc.vos.VOSURI;

/**
 * Base class for all VOSpace conformance tests. Contains methods to PUT, GET,
 * POST, and DELETE nodes to a VOSpace service.
 *
 * @author jburke
 */
public abstract class VOSBaseTest
{
    private static Logger log = Logger.getLogger(VOSBaseTest.class);

    protected DateFormat dateFormat;

    protected static ContainerNode baseTestNode;
    protected static TestNode testSuiteNode;

    protected VOSURI baseURI;
    protected URI resourceIdentifier;
    protected URL resourceURL;
    protected URI nodeStandardID;

    protected boolean supportLinkNodes;
    protected boolean supportLinkNodeProperties;
    protected boolean resolvePathNodes;
    protected boolean resolveTargetNode;

    /**
     * Constructor takes a path argument, which is the path to the resource
     * being tested, i.e. /nodes or /transfers. A System property service.url
     * is used to define the url to the base VOSpace service,
     * i.e. http://localhost/vospace.
     *
     * @param standardID to the resource to test.
     */
    public VOSBaseTest(final URI standardID)
    {
        try
        {
        	// resourceIdentifier for this test suite
            String propertyName = VOSTestSuite.class.getName() + ".resourceIdentifier";
            String propertyValue = System.getProperty(propertyName);
            log.debug("resourceID: " + propertyName + "=" + propertyValue);
            if (propertyValue != null)
            {
                this.resourceIdentifier = new URI(propertyValue);
            }
            else
            {
            	String msg = "system property " + propertyName + " not set to valid VOSpace resourceIdentifier URI";
                throw new IllegalStateException(msg);
            }

            // Base URI for the test nodes.
            propertyName = VOSTestSuite.class.getName() + ".baseURI";
            propertyValue = System.getProperty(propertyName);
            log.debug(propertyName + "=" + propertyValue);
            if (propertyValue != null)
            {
                this.baseURI = new VOSURI(propertyValue);
                log.debug("baseURI: " + this.baseURI);

                RegistryClient rc = new RegistryClient();
                this.resourceURL = rc.getServiceURL(resourceIdentifier, standardID, AuthMethod.CERT);
                log.debug("resourceURL: " + this.resourceURL);
                if (this.resourceURL == null)
                {
                    throw new RuntimeException("No service URL found for resourceIdentifier=" +
                        resourceIdentifier + ", standardID=" + standardID + ", AuthMethod=" + AuthMethod.CERT);
                }
            }
            else
            {
                throw new IllegalStateException("system property " + propertyName + " not set to valid VOSpace URI");
            }

            // Service supports LinkNodes.
            propertyName = VOSTestSuite.class.getName() + ".supportLinkNodes";
            propertyValue = System.getProperty(propertyName);
            log.debug(propertyName + "=" + propertyValue);
            if (propertyValue != null)
            {
                supportLinkNodes = new Boolean(propertyValue);
            }
            else
            {
                supportLinkNodes = false;
            }
            
            // Service supports LinkNodes.
            propertyName = VOSTestSuite.class.getName() + ".supportLinkNodeProperties";
            propertyValue = System.getProperty(propertyName);
            log.debug(propertyName + "=" + propertyValue);
            if (propertyValue != null)
            {
                supportLinkNodeProperties = new Boolean(propertyValue);
            }
            else
            {
                supportLinkNodeProperties = false;
            }

            // Service resolves LinkNodes in the path.
            propertyName = VOSTestSuite.class.getName() + ".resolvePathNodes";
            propertyValue = System.getProperty(propertyName);
            log.debug(propertyName + "=" + propertyValue);
            if (propertyValue != null)
            {
                resolvePathNodes = new Boolean(propertyValue);
            }
            else
            {
                resolvePathNodes = false;
            }

            // Service resolves target node if it's a LinkNode.
            propertyName = VOSTestSuite.class.getName() + ".resolveTargetNode";
            propertyValue = System.getProperty(propertyName);
            log.debug(propertyName + "=" + propertyValue);
            if (propertyValue != null)
            {
                resolveTargetNode = new Boolean(propertyValue);
            }
            else
            {
                resolveTargetNode = false;
            }
        }
        catch(Throwable t)
        {
            throw new RuntimeException("failed to init VOSpace URI and URL for tests", t);
        }

        dateFormat = DateUtil.getDateFormat("yyyy-MM-dd.HH:mm:ss.SSS", DateUtil.LOCAL);
        log.debug("baseURI: " + baseURI);
        log.debug("resourceIdentifier: " + resourceIdentifier);
        log.debug("resourceURL: " + resourceURL);
        log.debug("supportLinkNodes: " + supportLinkNodes);
        log.debug("resolvePathNodes: " + resolvePathNodes);
        log.debug("resolveTargetNode: " + resolveTargetNode);
    }

    /**
     * @return a ContainerNode.
     */
    private ContainerNode getBaseTestNode()
    {
        try
        {
            if (baseTestNode == null)
            {
                String baseNodeName = baseURI + "/" + VOSTestSuite.baseTestNodeName;

                RegistryClient registryClient = new RegistryClient();
                URL serviceURL = registryClient.getServiceURL(resourceIdentifier, getNodeStandardID(), AuthMethod.CERT);

                baseTestNode = new ContainerNode(new VOSURI(baseNodeName));
                String resourceUrl = serviceURL.toExternalForm() + baseTestNode.getUri().getPath();
                log.debug("**************************************************");
                log.debug("HTTP PUT: " + resourceUrl);

                StringBuilder sb = new StringBuilder();
                NodeWriter writer = new NodeWriter();
                writer.write(baseTestNode, sb);
                InputStream in = new ByteArrayInputStream(sb.toString().getBytes("UTF-8"));
                WebRequest request = new PutMethodWebRequest(resourceUrl, in, "text/xml");
                WebConversation conversation = new WebConversation();
                conversation.setExceptionsThrownOnErrorStatus(false);
                WebResponse response = conversation.sendRequest(request);
                log.debug(getResponseHeaders(response));
                log.debug("Response code: " + response.getResponseCode());
                if (response.getResponseCode() != 200 && response.getResponseCode() != 409)
                {
                    throw new VOSException(response.getResponseMessage());
                }

                log.debug("Created base test Node: " + baseTestNode);
            }
            return baseTestNode;
        }
        catch (Throwable t)
        {
            log.error("unexpected", t);
            log.error("cause", t.getCause());
            throw new RuntimeException("Cannot create base test Node", t);
        }
    }

    /**
     * @return a ContainerNode.
     */
    private TestNode getTestSuiteNode()
    {
        if (testSuiteNode == null)
        {
            ContainerNode sampleNode = null;
            LinkNode sampleLinkNode = null;

            // Create the root test suite container node.
            String testSuiteNodeName = baseURI + "/" + getBaseTestNode().getName() +
                                       "/" + VOSTestSuite.testSuiteNodeName;
            RegistryClient registryClient = new RegistryClient();
            URL serviceURL = registryClient.getServiceURL(resourceIdentifier, getNodeStandardID(), AuthMethod.CERT);

            log.debug("testSuiteNodeName: " + testSuiteNodeName);
            try
            {
                sampleNode = new ContainerNode(new VOSURI(testSuiteNodeName));
                String resourceUrl = serviceURL.toExternalForm() + sampleNode.getUri().getPath();
                log.debug("**************************************************");
                log.debug("HTTP PUT: " + resourceUrl);

                StringBuilder sb = new StringBuilder();
                NodeWriter writer = new NodeWriter();
                writer.write(sampleNode, sb);
                InputStream in = new ByteArrayInputStream(sb.toString().getBytes("UTF-8"));
                WebRequest request = new PutMethodWebRequest(resourceUrl, in, "text/xml");
                WebConversation conversation = new WebConversation();
                conversation.setExceptionsThrownOnErrorStatus(false);
                WebResponse response = conversation.sendRequest(request);
                log.debug(getResponseHeaders(response));
                log.debug("Response code: " + response.getResponseCode());
                if (response.getResponseCode() != 200 && response.getResponseCode() != 409)
                {
                    throw new VOSException(response.getResponseMessage());
                }
                log.debug("Created test suite sample ContainerNode: " + sampleNode);
            }
            catch (Throwable t)
            {
                t.printStackTrace();
                throw new RuntimeException("Cannot create test suite Node " + testSuiteNodeName, t);
            }

            if (supportLinkNodes)
            {
                // Create sibling link node to test suite container node.
                String testSuiteLinkNodeName = baseURI + "/" + getBaseTestNode().getName() +
                                               "/" + VOSTestSuite.testSuiteLinkNodeName;
                try
                {
                    sampleLinkNode = new LinkNode(new VOSURI(testSuiteLinkNodeName), sampleNode.getUri().getURI());
                    String resourceUrl = serviceURL.toExternalForm() + sampleLinkNode.getUri().getPath();
                    log.debug("**************************************************");
                    log.debug("HTTP PUT: " + resourceUrl);

                    StringBuilder sb = new StringBuilder();
                    NodeWriter writer = new NodeWriter();
                    writer.write(sampleLinkNode, sb);
                    InputStream in = new ByteArrayInputStream(sb.toString().getBytes("UTF-8"));
                    WebRequest request = new PutMethodWebRequest(resourceUrl, in, "text/xml");
                    WebConversation conversation = new WebConversation();
                    conversation.setExceptionsThrownOnErrorStatus(false);
                    WebResponse response = conversation.sendRequest(request);
                    log.debug(getResponseHeaders(response));
                    log.debug("Response code: " + response.getResponseCode());
                    if (response.getResponseCode() != 200)
                    {
                        throw new VOSException(response.getResponseMessage());
                    }
                    log.debug("Created test suite sample LinkNode: " + sampleLinkNode);
                }
                catch (Throwable t)
                {
                    t.printStackTrace();
                    throw new RuntimeException("Cannot create test suite LinkNode " + testSuiteLinkNodeName, t);
                }
            }

            testSuiteNode = new TestNode(sampleNode, sampleLinkNode);
        }
        return testSuiteNode;
    }

    protected void setNodeStandardID(final URI nodeStandardID)
    {
        this.nodeStandardID = nodeStandardID;
    }

    protected URI getNodeStandardID()
    {
        return this.nodeStandardID;
    }

    protected TestNode getSampleContainerNode()
        throws URISyntaxException
    {
        return getSampleContainerNode(false);
    }
    
    protected TestNode getSampleContainerNode(String name)
        throws URISyntaxException
    {
        return getSampleContainerNode(name, false);
    }
            
    protected TestNode getSampleContainerNode(boolean withProp)
        throws URISyntaxException
    {
        return getSampleContainerNode("", withProp);
    }

    /**
     * Builds and returns a sample ContainerNode for use in test cases.
     *
     * @return a ContainerNode.
     * @throws URISyntaxException if a Node URI is malformed.
     */
    protected TestNode getSampleContainerNode(String name, boolean withProp)
        throws URISyntaxException
    {
         // List of NodeProperty
        NodeProperty nodeProperty = new NodeProperty("ivo://ivoa.net/vospace/core#description", "My award winning stuff");

        // ContainerNode
        String date = dateFormat.format(Calendar.getInstance().getTime());
        String nodeName = getTestSuiteNode().sampleNode.getUri() + "/" + VOSTestSuite.userName + "_sample_" + date + "_" + name;
        ContainerNode node = new ContainerNode(new VOSURI(nodeName));
        if (withProp) {
            node.getProperties().add(nodeProperty);
        }

        // ContinerNode with LinkNode as parent.
        ContainerNode nodeWithLink = null;
        if (supportLinkNodes)
        {
            nodeName = getTestSuiteNode().sampleNodeWithLink.getUri() + "/" + VOSTestSuite.userName + "_sample_" + date + "_" + name;
            nodeWithLink = new ContainerNode(new VOSURI(nodeName));
            if (withProp) {
                nodeWithLink.getProperties().add(nodeProperty);
            }
        }
        return new TestNode(node, nodeWithLink);
    }

    /**
     * Builds and returns a sample DataNode for use in test cases.
     *
     * @return a DataNode.
     * @throws URISyntaxException if a Node URI is malformed.
     */
    protected TestNode getSampleDataNode()
        throws URISyntaxException
    {
        return getSampleDataNode("", false);
    }
    
    protected TestNode getSampleDataNode(boolean withProp)
        throws URISyntaxException
    {
        return getSampleDataNode("", withProp);
    }
    
    protected TestNode getSampleDataNode(String name)
        throws URISyntaxException
    {
        return getSampleDataNode(name, false);
    }
    /**
     * Builds and returns a sample DataNode for use in test cases.
     *
     * @param name
     * @return a DataNode.
     * @throws URISyntaxException if a Node URI is malformed.
     */
    protected TestNode getSampleDataNode(String name, boolean withProp)
        throws URISyntaxException
    {
        // List of NodeProperty
        NodeProperty nodeProperty = new NodeProperty("ivo://ivoa.net/vospace/core#description", "My award winning thing");
        nodeProperty.setReadOnly(true);

        // DataNode
        String date = dateFormat.format(Calendar.getInstance().getTime());
        String nodeName = getTestSuiteNode().sampleNode.getUri() + "/" + VOSTestSuite.userName + "_sample_" + date + "_" + name;
        log.debug("data node name: " + nodeName);
        DataNode node = new DataNode(new VOSURI(nodeName));
        if (withProp) {
            node.getProperties().add(nodeProperty);
        }
        node.setBusy(NodeBusyState.notBusy);

        // DataNode with LinkNode as parent.
        DataNode nodeWithLink = null;
        if (supportLinkNodes)
        {
            nodeName = getTestSuiteNode().sampleNodeWithLink.getUri() + "/" + VOSTestSuite.userName + "_sample_" + date + "_" + name;
            nodeWithLink = new DataNode(new VOSURI(nodeName));
            nodeWithLink.setBusy(NodeBusyState.notBusy);
        }
        return new TestNode(node, nodeWithLink);
    }

    /**
     * Builds and returns a sample LinkNode for use in test cases.
     *
     * @return a LinkNode.
     * @throws URISyntaxException if a Node URI is malformed.
     */
    protected LinkNode getSampleLinkNode()
        throws URISyntaxException
    {
        return getSampleLinkNode(new URI("http://www.google.com"));
    }

    /**
     * Builds and returns a sample LinkNode for use in test cases.
     *
     * @param target - target Node
     * @return a LinkNode.
     * @throws URISyntaxException if a Node URI is malformed.
     */
    protected LinkNode getSampleLinkNode(Node target)
        throws URISyntaxException
    {
        return getSampleLinkNode("", target.getUri().getURI(), false);
    }

    protected LinkNode getSampleLinkNode(Node target, boolean withProp)
        throws URISyntaxException
    {
        return getSampleLinkNode("", target.getUri().getURI(), withProp);
    }

    /**
     * Builds and returns a sample LinkNode for use in test cases.
     *
     *@param target - target URI
     * @return a LinkNode.
     * @throws URISyntaxException if a Node URI is malformed.
     */
    protected LinkNode getSampleLinkNode(URI target)
        throws URISyntaxException
    {
        return getSampleLinkNode("", target, false);
    }

    protected LinkNode getSampleLinkNode(String name, URI target)
        throws URISyntaxException
    {
        return getSampleLinkNode(name, target, false);
    }
    
    /**
     * Builds and returns a sample LinkNode for use in test cases.
     *
     * @param name
     * @param target node to point to
     * @return a LinkNode.
     * @throws URISyntaxException if a Node URI is malformed.
     */
    protected LinkNode getSampleLinkNode(String name, URI target, boolean withProp)
        throws URISyntaxException
    {
        // List of NodeProperty
        NodeProperty nodeProperty = new NodeProperty("ivo://ivoa.net/vospace/core#description",
                "Link to " + target.getPath());
        nodeProperty.setReadOnly(true);

        // LinkNode
        String date = dateFormat.format(Calendar.getInstance().getTime());
        String nodeName = getTestSuiteNode().sampleNode.getUri() + "/" + VOSTestSuite.userName + "_sample_" + date + "_" + name;
        log.debug("link node name: " + nodeName);
        LinkNode node = new LinkNode(new VOSURI(nodeName), target);
        if (withProp) {
            node.getProperties().add(nodeProperty);
        }
        return node;
    }

    /**
     * Delete a Node from the VOSpace.
     *
     * @param node to be deleted.
     * @return a HttpUnit WebResponse.
     * @throws IOException
     * @throws SAXException if there is an error parsing the retrieved page.
     */
    protected WebResponse delete(Node node)
        throws IOException, SAXException
    {
        return delete(null, node);
    }

    /**
     * Delete a Node from the VOSpace.
     *
     * @param node to be deleted.
     * @return a HttpUnit WebResponse.
     * @throws IOException
     * @throws SAXException if there is an error parsing the retrieved page.
     */
    protected WebResponse delete(URI standard, Node node)
        throws IOException, SAXException
    {
        String resourceUrl;
        if (standard == null)
        {
            resourceUrl = resourceURL + "/" + node.getUri().getPath();
        }
        else
        {
            resourceUrl = getResourceUrl(standard) + node.getUri().getPath();
        }
        log.debug("**************************************************");
        log.debug("HTTP DELETE: " + resourceUrl);
        WebRequest request = new DeleteMethodWebRequest(resourceUrl);

        log.debug(getRequestParameters(request));

        WebConversation conversation = new WebConversation();
        conversation.setExceptionsThrownOnErrorStatus(false);
        WebResponse response = conversation.getResponse(request);
        assertNotNull("Response to request is null", response);

        log.debug(getResponseHeaders(response));
        log.debug("response code: " + response.getResponseCode());
        log.debug("Content-Type: " + response.getContentType());

        return response;
    }

    /**
     * Gets a Node from the VOSpace.
     *
     * @param node to get.
     * @return a HttpUnit WebResponse.
     * @throws IOException
     * @throws SAXException if there is an error parsing the retrieved page.
     */
    protected WebResponse get(Node node)
        throws IOException, SAXException
    {
        return get(node, null);
    }

    /**
    * Gets a Node from the VOSpace.
    *
    * @param standard node standardID
    * @param node to get.
    * @return a HttpUnit WebResponse.
    * @throws IOException
    * @throws SAXException if there is an error parsing the retrieved page.
    */
   protected WebResponse get(URI standard, Node node)
       throws IOException, SAXException
   {
       return get(standard, node, null);
   }

    /**
     * Gets a Node from the VOSpace, appending the parameters to the GET
     * request of the Node URI.
     *
     * @param node to get.
     * @param parameters Map of HTTP request parameters.
     * @return a HttpUnit WebResponse.
     * @throws IOException
     * @throws SAXException if there is an error parsing the retrieved page.
     */
    protected WebResponse get(Node node, Map<String, String> parameters)
        throws IOException, SAXException
    {
        return get(null, node, parameters);
    }

    /**
     * Gets a Node from the VOSpace, appending the parameters to the GET
     * request of the Node URI.
     *
     * @param standard node standardID
     * @param node to get.
     * @param parameters Map of HTTP request parameters.
     * @return a HttpUnit WebResponse.
     * @throws IOException
     * @throws SAXException if there is an error parsing the retrieved page.
     */
    protected WebResponse get(URI standard, Node node, Map<String, String> parameters)
        throws IOException, SAXException
    {
        String resourceUrl;
        if (standard == null)
        {
            resourceUrl = resourceURL + "/" + node.getUri().getPath();
        }
        else
        {
            resourceUrl = getResourceUrl(standard) + node.getUri().getPath();
        }

        log.debug("**************************************************");
        log.debug("HTTP GET: " + resourceUrl);
        WebRequest request = new GetMethodWebRequest(resourceUrl);

        if (parameters != null)
        {
            List<String> keyList = new ArrayList<String>(parameters.keySet());
            for (String key : keyList)
            {
                String value = parameters.get(key);
                request.setParameter(key, value);
            }
        }
        log.debug(getRequestParameters(request));

        WebConversation conversation = new WebConversation();
        conversation.setExceptionsThrownOnErrorStatus(false);
        WebResponse response = conversation.getResponse(request);
        assertNotNull("Response to request is null", response);

        log.debug(getResponseHeaders(response));
        log.debug("response code: " + response.getResponseCode());
        log.debug("Content-Type: " + response.getContentType());

        return response;
    }

    /**
     * Post a Node to the VOSpace.
     *
     * @param node to post.
     * @return a HttpUnit WebResponse.
     * @throws IOException
     * @throws SAXException if there is an error parsing the retrieved page.
     */
    protected WebResponse post(Node node)
        throws IOException, SAXException
    {
        String resourceUrl = resourceURL + node.getUri().getPath();
        log.debug("**************************************************");
        log.debug("HTTP POST: " + resourceUrl);

        StringBuilder sb = new StringBuilder();
        NodeWriter nodeWriter = new NodeWriter();
        nodeWriter.write(node, sb);

        ByteArrayInputStream in = new ByteArrayInputStream(sb.toString().getBytes("UTF-8"));

        WebRequest request = new PostMethodWebRequest(resourceUrl, in, "text/xml");

        log.debug(getRequestParameters(request));

        WebConversation conversation = new WebConversation();
        conversation.setExceptionsThrownOnErrorStatus(false);
        WebResponse response = conversation.sendRequest(request);
        assertNotNull("POST response to " + resourceUrl + " is null", response);

        log.debug(getResponseHeaders(response));
        log.debug("Response code: " + response.getResponseCode());

        return response;
    }

    /**
     * Put a ContainerNode to the VOSpace.
     *
     * @param node to put.
     * @return a HttpUnit WebResponse.
     * @throws IOException
     * @throws SAXException if there is an error parsing the retrieved page.
     */
    protected WebResponse put(ContainerNode node)
        throws IOException, SAXException
    {
        return put(node, new NodeWriter());
    }

    /**
     * Put a ContainerNode to the VOSpace. Also takes a NodeWriter which
     * allows customization of the XML output to testing purposes.
     *
     * @param node to put.
     * @param writer to write Node XML.
     * @return a HttpUnit WebResponse.
     * @throws IOException
     * @throws SAXException if there is an error parsing the retrieved page.
     */
    protected WebResponse put(ContainerNode node, NodeWriter writer)
        throws IOException, SAXException
    {
        String resourceUrl = resourceURL + node.getUri().getPath();
        log.debug("**************************************************");
        log.debug("HTTP PUT: " + resourceUrl);

        StringBuilder sb = new StringBuilder();
        writer.write(node, sb);
        InputStream in = new ByteArrayInputStream(sb.toString().getBytes("UTF-8"));
        WebRequest request = new PutMethodWebRequest(resourceUrl, in, "text/xml");

        log.debug(getRequestParameters(request));

        WebConversation conversation = new WebConversation();
        conversation.setExceptionsThrownOnErrorStatus(false);
        WebResponse response = conversation.sendRequest(request);
        assertNotNull("PUT response to " + resourceUrl + " is null", response);

        log.debug(getResponseHeaders(response));
        log.debug("Response code: " + response.getResponseCode());

        return response;
    }

    /**
     * Put a DataNode or LinkNode to the VOSpace.
     *
     * @param node to put.
     * @return a HttpUnit WebResponse.
     * @throws IOException
     * @throws SAXException if there is an error parsing the retrieved page.
     */
    protected WebResponse put(Node node)
        throws IOException, SAXException
    {
        return put(null, node, new NodeWriter());
    }

    /**
     * Put a DataNode or LinkNode to the VOSpace. Also takes a NodeWriter which
     * allows customization of the XML output to testing purposes.
     *
     * @param node to put.
     * @param writer to write Node XML.
     * @return a HttpUnit WebResponse.
     * @throws IOException
     * @throws SAXException if there is an error parsing the retrieved page.
     */
    protected WebResponse put(Node node, NodeWriter writer)
        throws IOException, SAXException
    {
        return put(null, node, writer);
    }

    /**
     * Put a DataNode or LinkNode to the VOSpace. Also takes a NodeWriter which
     * allows customization of the XML output to testing purposes.
     *
     * @param standard override init standard.
     * @param node to put.
     * @param writer to write Node XML.
     * @return a HttpUnit WebResponse.
     * @throws IOException
     * @throws SAXException if there is an error parsing the retrieved page.
     */
    protected WebResponse put(URI standard, Node node, NodeWriter writer)
        throws IOException, SAXException
    {
        String resourceUrl;
        if (standard == null)
        {
            resourceUrl = resourceURL + node.getUri().getPath();
        }
        else
        {
            resourceUrl = getResourceUrl(standard) + node.getUri().getPath();
        }
        log.debug("**************************************************");
        log.debug("HTTP PUT: " + resourceUrl);

        StringBuilder sb = new StringBuilder();
        writer.write(node, sb);
        InputStream in = new ByteArrayInputStream(sb.toString().getBytes("UTF-8"));
        WebRequest request = new PutMethodWebRequest(resourceUrl, in, "text/xml");

        log.debug(getRequestParameters(request));

        WebConversation conversation = new WebConversation();
        conversation.setExceptionsThrownOnErrorStatus(false);
        WebResponse response = conversation.sendRequest(request);
        assertNotNull("PUT response to " + resourceUrl + " is null", response);

        log.debug(getResponseHeaders(response));
        log.debug("Response code: " + response.getResponseCode());

        return response;
    }

    /**
     * Get an URL.
     *
     * @param resourceUrl url to get.
     * @return a HttpUnit WebResponse.
     * @throws IOException
     * @throws SAXException if there is an error parsing the retrieved page.
     */
    protected WebResponse get(String resourceUrl)
        throws IOException, SAXException
    {
        log.debug("**************************************************");
        log.debug("HTTP GET: " + resourceUrl);

        WebRequest request = new GetMethodWebRequest(resourceUrl);
        WebConversation conversation = new WebConversation();
        WebResponse response = conversation.getResponse(request);
        assertNotNull("GET response to " + resourceUrl + " is null", response);

        log.debug(getResponseHeaders(response));

        return response;
    }

    /**
     * Post parameters to the service url.
     *
     * @param  parameters Map of HTTP request parameters.
     * @return a HttpUnit WebResponse.
     * @throws IOException
     * @throws SAXException if there is an error parsing the retrieved page.
     */
    protected WebResponse post(Map<String, String> parameters)
        throws IOException, SAXException
    {
        return post(null, parameters);
    }

    /**
     * Post parameters to the service url.
     *
     * @param  url Endpoint to POST to.
     * @param  parameters Map of HTTP request parameters.
     * @return a HttpUnit WebResponse.
     * @throws IOException
     * @throws SAXException if there is an error parsing the retrieved page.
     */
    protected WebResponse post(String url, Map<String, String> parameters)
        throws IOException, SAXException
    {
        // POST request to the phase resource.
        String resourceUrl;
        if (url == null)
            resourceUrl = resourceURL.toExternalForm();
        else
            resourceUrl = url;
        log.debug("**************************************************");
        log.debug("HTTP POST: " + resourceUrl);

        WebRequest request = new PostMethodWebRequest(resourceUrl);
        //request.setHeaderField("Content-Type", "multipart/form-data");
        if (parameters != null)
        {
            List<String> keyList = new ArrayList<String>(parameters.keySet());
            for (String key : keyList)
            {
                String value = parameters.get(key);
                request.setParameter(key, value);
            }
        }
        log.debug(getRequestParameters(request));

        WebConversation conversation = new WebConversation();
        WebResponse response = conversation.getResponse(request);
        assertNotNull("POST response to " + resourceUrl + " is null", response);

        log.debug(getResponseHeaders(response));

        return response;
    }

    /**
     * Post a Job to the VOSpace.
     *
     * @param xml Job XML.
     * @return a HttpUnit WebResponse.
     * @throws IOException
     * @throws SAXException if there is an error parsing the retrieved page.
     */
    protected WebResponse post(String xml)
        throws IOException, SAXException
    {
        log.debug("**************************************************");
        log.debug("HTTP POST: " + resourceURL);

        ByteArrayInputStream in = new ByteArrayInputStream(xml.getBytes("UTF-8"));

        WebRequest request = new PostMethodWebRequest(resourceURL.toExternalForm(), in, "text/xml");

        log.debug(getRequestParameters(request));

        WebConversation conversation = new WebConversation();
        conversation.setExceptionsThrownOnErrorStatus(false);
        WebResponse response = conversation.sendRequest(request);
        assertNotNull("POST response to " + resourceURL + " is null", response);

        log.debug(getResponseHeaders(response));
        log.debug("Response code: " + response.getResponseCode());

        return response;
    }

    /**
     * Build a String representation of the Request parameters.
     *
     * @param request the HttpUnit WebRequest.
     * @return String representation of the request parameters.
     */
    public static String getRequestParameters(WebRequest request)
    {
        StringBuilder sb = new StringBuilder();
        sb.append("Request parameters:");
        sb.append("\r\n");
        String[] headers = request.getRequestParameterNames();
        for (int i = 0; i < headers.length; i++)
        {
            sb.append("\t");
            sb.append(headers[i]);
            sb.append("=");
            sb.append(request.getParameter(headers[i]));
            sb.append("\r\n");
        }
        return sb.toString();
    }

    /**
     * Build a String representation of the Response header fields.
     *
     * @param response the HttpUnit WebResponse.
     * @return a String representation of the response header fields.
     */
    public static String getResponseHeaders(WebResponse response)
    {
        StringBuilder sb = new StringBuilder();
        sb.append("Response headers:");
        sb.append("\r\n");
        String[] headers = response.getHeaderFieldNames();
        for (int i = 0; i < headers.length; i++)
        {
            sb.append("\t");
            sb.append(headers[i]);
            sb.append("=");
            sb.append(response.getHeaderField(headers[i]));
            sb.append("\r\n");
        }
        return sb.toString();
    }

    private String getResourceUrl(URI standard) throws MalformedURLException
    {
        RegistryClient rc = new RegistryClient();
        URL serviceURL = rc.getServiceURL(resourceIdentifier, standard, AuthMethod.CERT);
        return serviceURL.toExternalForm();
    }

    private SSLSocketFactory originalSSLState;

    @Before
    public void setUp() throws Exception
    {
        originalSSLState = HttpsURLConnection.getDefaultSSLSocketFactory();

        File crt = FileUtil.getFileFromResource("x509_CADCRegtest1.pem", VOSTestSuite.class);
        SSLUtil.initSSL(crt);
        log.info("VOSBaseTest.setUp(): set up SSL cert. ");
    }

    @After
    public void tearDown()
    {
        // Revert the SSL State after test is done
        HttpsURLConnection.setDefaultSSLSocketFactory(originalSSLState);
        log.info("VOSBaseTest.tearDown() done");
    }

}
