/*
 ************************************************************************
 *******************  CANADIAN ASTRONOMY DATA CENTRE  *******************
 **************  CENTRE CANADIEN DE DONNÉES ASTRONOMIQUES  **************
 *
 *  (c) 2023.                            (c) 2023.
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
 *  : 5 $
 *
 ************************************************************************
 */

package org.opencadc.conformance.vos;

import ca.nrc.cadc.auth.AuthMethod;
import ca.nrc.cadc.auth.RunnableAction;
import ca.nrc.cadc.auth.SSLUtil;
import ca.nrc.cadc.net.FileContent;
import ca.nrc.cadc.net.HttpDelete;
import ca.nrc.cadc.net.HttpGet;
import ca.nrc.cadc.net.HttpPost;
import ca.nrc.cadc.net.HttpUpload;
import ca.nrc.cadc.reg.Standards;
import ca.nrc.cadc.reg.client.RegistryClient;
import ca.nrc.cadc.util.FileUtil;
import ca.nrc.cadc.util.Log4jInit;
import ca.nrc.cadc.uws.ExecutionPhase;
import ca.nrc.cadc.uws.Job;
import ca.nrc.cadc.uws.JobAttribute;
import ca.nrc.cadc.uws.JobReader;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.StringReader;
import java.net.MalformedURLException;
import java.net.URI;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.text.ParseException;
import java.util.HashMap;
import java.util.Map;
import javax.security.auth.Subject;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.jdom2.JDOMException;
import org.junit.Assert;
import org.junit.Before;
import org.opencadc.vospace.ContainerNode;
import org.opencadc.vospace.Node;
import org.opencadc.vospace.NodeNotSupportedException;
import org.opencadc.vospace.VOS;
import org.opencadc.vospace.VOSURI;
import org.opencadc.vospace.io.NodeParsingException;
import org.opencadc.vospace.io.NodeReader;
import org.opencadc.vospace.io.NodeWriter;

public abstract class VOSTest {
    private static final Logger log = Logger.getLogger(VOSTest.class);

    static {
        Log4jInit.setLevel("org.opencadc.conformance.vos", Level.INFO);
        Log4jInit.setLevel("org.opencadc.vospace", Level.INFO);
    }

    
    public static final String XML_CONTENT_TYPE = "text/xml";
    public static final String TEXT_CONTENT_TYPE = "text/plain";
    public static final String ROOT_TEST_FOLDER = "/int-tests";
    
    public final URI resourceID;
    public final String testCertFilename;
    
    public final URL nodesServiceURL;
    public final URL filesServiceURL;
    public final URL synctransServiceURL;
    public final URL transferServiceURL;
    public final URL recursiveDeleteServiceURL;
    public final Subject authSubject;

    protected VOSTest(URI resourceID, String testCertFilename) {
        this.resourceID = resourceID;
        this.testCertFilename = testCertFilename;
        RegistryClient regClient = new RegistryClient();
        this.nodesServiceURL = regClient.getServiceURL(resourceID, Standards.VOSPACE_NODES_20, AuthMethod.ANON);
        log.info(String.format("%s: %s", Standards.VOSPACE_NODES_20, nodesServiceURL));

        this.filesServiceURL = regClient.getServiceURL(resourceID, Standards.VOSPACE_FILES_20, AuthMethod.ANON);
        log.info(String.format("%s: %s", Standards.VOSPACE_FILES_20, filesServiceURL));

        this.synctransServiceURL = regClient.getServiceURL(resourceID, Standards.VOSPACE_SYNC_21, AuthMethod.ANON);
        log.info(String.format("%s: %s", Standards.VOSPACE_SYNC_21, synctransServiceURL));

        this.transferServiceURL = regClient.getServiceURL(resourceID, Standards.VOSPACE_TRANSFERS_20, AuthMethod.ANON);
        log.info(String.format("%s: %s", Standards.VOSPACE_TRANSFERS_20, transferServiceURL));

        this.recursiveDeleteServiceURL = regClient.getServiceURL(resourceID, Standards.VOSPACE_RECURSIVE_DELETE,
                AuthMethod.ANON);
        log.info(String.format("%s: %s", Standards.VOSPACE_RECURSIVE_DELETE, recursiveDeleteServiceURL));

        File testCert = FileUtil.getFileFromResource(this.testCertFilename, VOSTest.class);
        this.authSubject = SSLUtil.createSubject(testCert);
        log.debug("authSubject: " + authSubject);
    }

    @Before
    public void initTestContainer() throws Exception {
        String name = VOSTest.ROOT_TEST_FOLDER;
        URL nodeURL = getNodeURL(nodesServiceURL, null); // method already puts test folder name in
        VOSURI nodeURI = getVOSURI(null);
        ContainerNode testNode = new ContainerNode(name);

        NodeReader.NodeReaderResult result = get(nodeURL, 200, XML_CONTENT_TYPE, false);
        if (result == null) {
            put(nodeURL, nodeURI, testNode);
        }
    }
    
    public URL getNodeURL(URL serviceURL, String path)
        throws MalformedURLException {
        if (path == null) {
            return new URL(String.format("%s%s", serviceURL, ROOT_TEST_FOLDER));
        }
        return new URL(String.format("%s%s/%s", serviceURL, ROOT_TEST_FOLDER, path));
    }

    public VOSURI getVOSURI(String path) {
        if (path == null) {
            return new VOSURI(resourceID, ROOT_TEST_FOLDER);
        }
        return new VOSURI(resourceID, ROOT_TEST_FOLDER + "/" + path);
    }

    public void put(URL nodeURL, VOSURI vosURI, Node node) throws IOException {
        StringBuilder sb = new StringBuilder();
        NodeWriter writer = new NodeWriter();
        writer.write(vosURI, node, sb, VOS.Detail.max);
        String xml = sb.toString();
        log.debug("put node: " + xml);
        InputStream in = new ByteArrayInputStream(xml.getBytes());

        HttpUpload put = new HttpUpload(in, nodeURL);
        put.setRequestProperty("Content-Type", XML_CONTENT_TYPE);
        log.debug("PUT " + nodeURL);
        Subject.doAs(authSubject, new RunnableAction(put));
        log.info("PUT response: " + put.getResponseCode() + " " + put.getThrowable());
        
        // TODO revert response code back to expected 201
        Assert.assertEquals("expected PUT response code = 200",
                            200, put.getResponseCode());
        Assert.assertNull("expected PUT throwable == null", put.getThrowable());
    }

    public void put(URL nodeURL, File file, String contentType) {
        HttpUpload put = new HttpUpload(file, nodeURL);
        put.setRequestProperty("Content-Type", contentType);
        log.debug("PUT " + nodeURL);
        Subject.doAs(authSubject, new RunnableAction(put));
        log.debug("PUT responseCode: " + put.getResponseCode());
        // TODO revert response code back to expected 201
        Assert.assertEquals("expected PUT response code = 200",
                            200, put.getResponseCode());
        Assert.assertNull("expected PUT throwable == null", put.getThrowable());
    }

    public void put(URL nodeURL, InputStream is, String contentType) {
        HttpUpload put = new HttpUpload(is, nodeURL);
        put.setRequestProperty("Content-Type", contentType);
        log.debug("PUT " + nodeURL);
        Subject.doAs(authSubject, new RunnableAction(put));
        log.debug("PUT responseCode: " + put.getResponseCode());
        // TODO revert response code back to expected 201
        Assert.assertEquals("expected PUT response code = 200",
                            200, put.getResponseCode());
        Assert.assertNull("expected PUT throwable == null", put.getThrowable());
    }

    public NodeReader.NodeReaderResult get(URL url, int responseCode, String contentType)
            throws NodeParsingException, NodeNotSupportedException {
        return get(url, responseCode, contentType, true);
    }
    
    public NodeReader.NodeReaderResult get(URL url, int responseCode, String contentType, boolean verify)
            throws NodeParsingException, NodeNotSupportedException {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        HttpGet get = new HttpGet(url, out);
        log.debug("GET: " + url);
        Subject.doAs(authSubject, new RunnableAction(get));
        log.debug("GET responseCode: " + get.getResponseCode() + " " + get.getThrowable());

        if (verify) {
            Assert.assertEquals(responseCode, get.getResponseCode());
            //Assert.assertNull(get.getThrowable());
            Assert.assertEquals("content-type", contentType, get.getContentType());
        }
        if (get.getResponseCode() == 200) {
            NodeReader reader = new NodeReader();
            NodeReader.NodeReaderResult result = reader.read(out.toString());
            Assert.assertNotNull(result);
            Assert.assertNotNull(result.node);
            Assert.assertNotNull(result.vosURI);
            return result;
        } else {
            return null;
        }
    }

    public void post(URL nodeURL, VOSURI vosURI, Node node)
        throws IOException {
        StringBuilder sb = new StringBuilder();
        NodeWriter writer = new NodeWriter();
        writer.write(vosURI, node, sb, VOS.Detail.max);

        FileContent content = new FileContent(sb.toString(), XML_CONTENT_TYPE, StandardCharsets.UTF_8);

        HttpPost post = new HttpPost(nodeURL, content, true);
        log.debug("POST: " + nodeURL);
        Subject.doAs(authSubject, new RunnableAction(post));
        log.debug("POST responseCode: " + post.getResponseCode());
        Assert.assertEquals("expected POST response code = 200",
                            200, post.getResponseCode());
        Assert.assertNull("expected POST throwable == null", post.getThrowable());
    }

    public Job postRecursiveDelete(URL nodeURL, VOSURI vosURI)
            throws Exception {

        Map<String, Object> val = new HashMap<>();
        val.put("nodeURI", vosURI.getURI());
        HttpPost post = new HttpPost(nodeURL, val, false);
        log.debug("POST: " + nodeURL);
        Subject.doAs(authSubject, new RunnableAction(post));
        log.debug("POST responseCode: " + post.getResponseCode());
        Assert.assertEquals("expected POST response code = 303",
                303, post.getResponseCode());
        URL jobURL = post.getRedirectURL();
        URL jobPhaseURL = new URL(jobURL.toString() + "/phase");

        // start the job
        val.clear();
        val.put("phase", "RUN");
        post = new HttpPost(jobPhaseURL , val, false);
        log.debug("POST: " + jobPhaseURL);
        Subject.doAs(authSubject, new RunnableAction(post));
        log.debug("POST responseCode: " + post.getResponseCode());
        Assert.assertEquals("expected POST response code = 303",
                303, post.getResponseCode());
        Assert.assertNull("expected POST throwable == null", post.getThrowable());
        Thread.sleep(3000); // wait for job to finish
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        HttpGet get = new HttpGet(jobURL, out);
        log.debug("GET: " + jobURL);
        Subject.doAs(authSubject, new RunnableAction(get));
        Assert.assertEquals("expected GET response code = 200",
                200, get.getResponseCode());
        Assert.assertNull("expected GET throwable == null", get.getThrowable());

        log.debug("Job XML: \n" + out);
        JobReader jr = new JobReader();
        https://localhost.cadc.dao.nrc.ca/vault/recursiveDelete/or86x0frxikba0rv
        return jr.read(new StringReader(out.toString()));
    }

    public void delete(URL nodeURL) {
        delete(nodeURL, true);
    }
    
    // verify==false ignores 404 only
    public void delete(URL nodeURL, boolean verify) {
        HttpDelete delete = new HttpDelete(nodeURL, true);
        log.info("DELETE: " + nodeURL);
        Subject.doAs(authSubject, new RunnableAction(delete));
        log.info("DELETE response: " + delete.getResponseCode() + " " + delete.getThrowable());
        if (!verify && delete.getResponseCode() == 404) {
            return;
        }
        // TODO revert response code back to expected 204
        Assert.assertEquals("expected DELETE response code = 200",
                            200, delete.getResponseCode());
        Assert.assertNull("expected DELETE throwable == null", delete.getThrowable());
    }

}
