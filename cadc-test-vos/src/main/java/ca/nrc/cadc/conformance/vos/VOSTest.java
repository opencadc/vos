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
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import javax.security.auth.Subject;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.junit.Assert;
import org.opencadc.vospace.Node;
import org.opencadc.vospace.NodeParsingException;
import org.opencadc.vospace.NodeReader;
import org.opencadc.vospace.NodeWriter;
import org.opencadc.vospace.VOSURI;

public abstract class VOSTest {
    private static final Logger log = Logger.getLogger(VOSTest.class);
    public static final URI SERVICE_RESOURCE_ID = URI.create("ivo://opencadc.org/vault");
    public static final String TEST_CERT_NAME = "vault-test.pem";
    public static final String XML_CONTENT_TYPE = "application/xml";
    public final URL nodesServiceURL;
    public final URL filesServiceURL;
    public final URL synctransServiceURL;
    public final Subject authSubject;

    static {
        Log4jInit.setLevel("ca.nrc.cadc.conformance.vos", Level.DEBUG);
        Log4jInit.setLevel("ca.nrc.cadc.vos", Level.DEBUG);
    }

    public VOSTest() {
        RegistryClient regClient = new RegistryClient();
        this.nodesServiceURL = regClient.getServiceURL(SERVICE_RESOURCE_ID, Standards.VOSPACE_NODES_20, AuthMethod.ANON);
        log.info(String.format("%s: %s", Standards.VOSPACE_NODES_20, nodesServiceURL));

        this.filesServiceURL = regClient.getServiceURL(SERVICE_RESOURCE_ID, Standards.VOSPACE_FILES_20, AuthMethod.ANON);
        log.info(String.format("%s: %s", Standards.VOSPACE_FILES_20, filesServiceURL));

        this.synctransServiceURL = regClient.getServiceURL(SERVICE_RESOURCE_ID, Standards.VOSPACE_SYNC_21, AuthMethod.ANON);
        log.info(String.format("%s: %s", Standards.VOSPACE_SYNC_21, synctransServiceURL));

        File testCert = FileUtil.getFileFromResource(TEST_CERT_NAME, NodesTest.class);
        this.authSubject = SSLUtil.createSubject(testCert);
        log.debug("authSubject: " + authSubject);
    }

    public void put(URL nodeURL, VOSURI vosURI, Node node)
        throws IOException {
        StringBuilder sb = new StringBuilder();
        NodeWriter writer = new NodeWriter();
        writer.write(vosURI, node, sb);
        InputStream in = new ByteArrayInputStream(sb.toString().getBytes());

        HttpUpload put = new HttpUpload(in, nodeURL);
        put.setRequestProperty("Content-Type", XML_CONTENT_TYPE);
        Subject.doAs(authSubject, new RunnableAction(put));
        log.debug("responseCode: " + put.getResponseCode());
        Assert.assertEquals("PUT response code should be 201",
                            201, put.getResponseCode());
        Assert.assertNull("PUT throwable should be null", put.getThrowable());
    }

    public void put(URL nodeURL, File file) {
        HttpUpload put = new HttpUpload(file, nodeURL);
        put.setRequestProperty("Content-Type", XML_CONTENT_TYPE);
        Subject.doAs(authSubject, new RunnableAction(put));
        log.debug("responseCode: " + put.getResponseCode());
        Assert.assertEquals("PUT response code should be 201",
                            201, put.getResponseCode());
        Assert.assertNull("PUT throwable should be null", put.getThrowable());
    }

    public NodeReader.NodeReaderResult get(URL url, int responseCode)
        throws NodeParsingException {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        HttpGet get = new HttpGet(url, out);
        Subject.doAs(authSubject, new RunnableAction(get));
        log.debug("responseCode: " + get.getResponseCode());
        Assert.assertEquals("GET response code should be " + responseCode,
                            responseCode, get.getResponseCode());

        if (responseCode == 200) {
            Assert.assertNull("GET throwable should be null", get.getThrowable());
            Assert.assertEquals("GET Content-Type should be " + XML_CONTENT_TYPE, XML_CONTENT_TYPE, get.getContentType());

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
        writer.write(vosURI, node, sb);

        FileContent content = new FileContent(sb.toString(), XML_CONTENT_TYPE, StandardCharsets.UTF_8);

        HttpPost post = new HttpPost(nodeURL, content, true);
        Subject.doAs(authSubject, new RunnableAction(post));
        log.debug("responseCode: " + post.getResponseCode());
        Assert.assertEquals("POST response code should be 200",
                            200, post.getResponseCode());
        Assert.assertNull("POST throwable should be null", post.getThrowable());
    }

    public void delete(URL nodeURL) {
        HttpDelete delete = new HttpDelete(nodeURL, true);
        Subject.doAs(authSubject, new RunnableAction(delete));
        log.debug("responseCode: " + delete.getResponseCode());
        Assert.assertEquals("DELETE response code should be 204",
                            204, delete.getResponseCode());
        Assert.assertNull("DELETE throwable should be null", delete.getThrowable());
    }

}
