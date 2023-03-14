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

package ca.nrc.cadc.conformance.vos;

import static org.junit.Assert.assertEquals;

import ca.nrc.cadc.auth.AuthMethod;
import ca.nrc.cadc.auth.RunnableAction;
import ca.nrc.cadc.net.FileContent;
import ca.nrc.cadc.net.HttpGet;
import ca.nrc.cadc.net.HttpPost;
import ca.nrc.cadc.reg.Standards;
import ca.nrc.cadc.reg.client.RegistryClient;
import ca.nrc.cadc.util.FileUtil;
import ca.nrc.cadc.util.Log4jInit;
import com.meterware.httpunit.WebResponse;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.StringWriter;
import java.net.MalformedURLException;
import java.net.URI;
import java.net.URL;
import javax.security.auth.Subject;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.junit.Assert;
import org.junit.Test;
import org.opencadc.vospace.DataNode;
import org.opencadc.vospace.Direction;
import org.opencadc.vospace.LinkNode;
import org.opencadc.vospace.Protocol;
import org.opencadc.vospace.Transfer;
import org.opencadc.vospace.TransferParsingException;
import org.opencadc.vospace.TransferReader;
import org.opencadc.vospace.TransferWriter;
import org.opencadc.vospace.VOS;
import org.opencadc.vospace.VOSURI;
import org.opencadc.vospace.XmlProcessor;
import org.xml.sax.SAXException;

public class TransferTest extends VOSTest {
    private static final Logger log = Logger.getLogger(TransferTest.class);

    static {
        Log4jInit.setLevel("ca.nrc.cadc.conformance.vos", Level.DEBUG);
        Log4jInit.setLevel("ca.nrc.cadc.vos", Level.DEBUG);
    }
    public TransferTest() {
        super();
    }

    @Test
    public void pullFromVospaceTest() {
        try {
            // upload test file
            String name = "pull-file-test.txt";
            URL nodeURL = new URL(String.format("%s/%s", nodesServiceURL, name));
            log.debug("nodeURL: " + nodeURL);
            VOSURI nodeURI = new VOSURI(URI.create("vos://cadc.nrc.ca!vospace/" + name));
            File testFile = FileUtil.getFileFromResource(name, NodesTest.class);
            log.debug("testFile: " + testFile.getAbsolutePath());

            put(nodeURL, testFile);

            // create a transfer
            Transfer transfer = new Transfer(nodeURI.getURI(), Direction.pullFromVoSpace);
            transfer.getProtocols().add(new Protocol(VOS.PROTOCOL_HTTP_GET));
            transfer.getProtocols().add(new Protocol(VOS.PROTOCOL_HTTPS_GET));

            // post to sync transfer
            TransferWriter transferWriter = new TransferWriter();
            StringWriter stringWriter = new StringWriter();
            transferWriter.write(transfer, stringWriter);
            FileContent fileContent = new FileContent(stringWriter.toString().getBytes(), VOSTest.XML_CONTENT_TYPE);
            URL transferURL = new URL(String.format("%s/%s", synctransServiceURL, name));
            log.debug("transferURL: " + transferURL);
            HttpPost post = new HttpPost(transferURL, fileContent, true);
            Subject.doAs(authSubject, new RunnableAction(post));

            Assert.assertEquals("POST response code should be " + 200,
                                200, post.getResponseCode());
            Assert.assertNull("PUT throwable should be null", post.getThrowable());

            TransferReader transferReader = new TransferReader();
            Transfer actual = transferReader.read(post.getInputStream(), XmlProcessor.VOSPACE_SCHEMA_RESOURCE_21);

            Assert.assertEquals("transfer direction", Direction.pullFromVoSpace, actual.getDirection());
            Assert.assertNotNull("", actual.getProtocols());
            String endpoint = null;
            for (Protocol protocol : actual.getProtocols()) {
                try {
                    endpoint = protocol.getEndpoint();
                    log.debug("endpoint: " + endpoint);
                    new URL(endpoint);
                } catch (MalformedURLException e) {
                    Assert.fail("invalid protocol endpoint: " + endpoint);
                }
            }

        } catch (Exception e) {
            log.error("Unexpected error", e);
            Assert.fail("Unexpected error: " + e);
        }
    }

    @Test
    public void testPullLinkNodeFromVOSpace() {
        try {
            // put a data node
            String dataName = "data-pull-link-node-test";
            URL dataNodeURL = new URL(String.format("%s/%s", nodesServiceURL, dataName));
            log.debug("dataNodeURL: " + dataNodeURL);
            VOSURI dataNodeURI = new VOSURI(URI.create("vos://cadc.nrc.ca!vospace/" + dataName));
            DataNode dataNode = new DataNode(dataName, URI.create("storageID"));

            put(dataNodeURL, dataNodeURI, dataNode);

            // put a link node to the data node
            String linkName = "link-pull-link-node-test";
            URL linkNodeURL = new URL(String.format("%s/%s", nodesServiceURL, linkName));
            log.debug("linkNodeURL: " + linkNodeURL);
            VOSURI linkNodeURI = new VOSURI(URI.create("vos://cadc.nrc.ca!vospace/" + linkName));
            LinkNode linkNode = new LinkNode(dataName, dataNodeURI.getURI());

            put(linkNodeURL, linkNodeURI, linkNode);

            // post and verify the transfer urls.
            doTransfer(linkNodeURI.getURI(), linkName);

        } catch (Exception e) {
            log.error("Unexpected error", e);
            Assert.fail("Unexpected error: " + e);
        }
    }

    public void doTransfer(URI nodeURI, String nodePath)
        throws IOException, TransferParsingException {
        Transfer transfer = new Transfer(nodeURI, Direction.pullFromVoSpace);
        transfer.getProtocols().add(new Protocol(VOS.PROTOCOL_HTTP_GET));
        transfer.getProtocols().add(new Protocol(VOS.PROTOCOL_HTTPS_GET));

        // post to sync transfer
        TransferWriter transferWriter = new TransferWriter();
        StringWriter stringWriter = new StringWriter();
        transferWriter.write(transfer, stringWriter);
        FileContent fileContent = new FileContent(stringWriter.toString().getBytes(), VOSTest.XML_CONTENT_TYPE);
        URL transferURL = new URL(String.format("%s/%s", synctransServiceURL, nodePath));
        log.debug("transferURL: " + transferURL);
        HttpPost post = new HttpPost(transferURL, fileContent, true);
        Subject.doAs(authSubject, new RunnableAction(post));

        Assert.assertEquals("POST response code should be " + 200,
                            200, post.getResponseCode());
        Assert.assertNull("PUT throwable should be null", post.getThrowable());

        TransferReader transferReader = new TransferReader();
        Transfer result = transferReader.read(post.getInputStream(), XmlProcessor.VOSPACE_SCHEMA_RESOURCE_21);

        Assert.assertEquals("transfer direction", Direction.pullFromVoSpace, result.getDirection());
        Assert.assertNotNull("", result.getProtocols());
        String endpoint = null;
        for (Protocol protocol : result.getProtocols()) {
            try {
                endpoint = protocol.getEndpoint();
                log.debug("endpoint: " + endpoint);
                new URL(endpoint);
            } catch (MalformedURLException e) {
                Assert.fail("invalid protocol endpoint: " + endpoint);
            }
        }
    }

    public URL getTranferURL(VOSURI uri)
        throws Exception {
        RegistryClient regClient = new RegistryClient();
        URL baseURL = regClient.getServiceURL(uri.getServiceURI(), Standards.VOSPACE_SYNC_21, AuthMethod.ANON);
        URL url = new URL(baseURL + uri.getPath());
        ByteArrayOutputStream out = new ByteArrayOutputStream();

        HttpGet get = new HttpGet(url, out);
        get.setFollowRedirects(false);

        Subject.doAs(authSubject, new RunnableAction(get));
        log.debug("responseCode: " + get.getResponseCode());
        Assert.assertEquals("GET response code should be " + 302,
                            302, get.getResponseCode());
        Assert.assertNull("PUT throwable should be null", get.getThrowable());
        Assert.assertNotNull("Unexpected non-redirect response from files", get.getRedirectURL());

        HttpGet transfer = new HttpGet(get.getRedirectURL(), out);
        transfer.setFollowRedirects(false);

        Subject.doAs(authSubject, new RunnableAction(transfer));
        log.debug("responseCode: " + transfer.getResponseCode());
        Assert.assertEquals("GET response code should be " + 302,
                            302, transfer.getResponseCode());
        Assert.assertNull("PUT throwable should be null", transfer.getThrowable());
        Assert.assertNotNull("Unexpected non-redirect response from transfer", transfer.getRedirectURL());

        return transfer.getRedirectURL();
    }

}
