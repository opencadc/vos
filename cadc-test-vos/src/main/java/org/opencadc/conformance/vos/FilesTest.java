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

import ca.nrc.cadc.auth.RunnableAction;
import ca.nrc.cadc.net.FileContent;
import ca.nrc.cadc.net.HttpGet;
import ca.nrc.cadc.net.HttpPost;
import ca.nrc.cadc.reg.Standards;
import ca.nrc.cadc.util.Log4jInit;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.StringWriter;
import java.net.URL;
import javax.security.auth.Subject;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.junit.Assert;
import org.junit.Test;
import org.opencadc.vospace.VOS;
import org.opencadc.vospace.VOSURI;
import org.opencadc.vospace.transfer.Direction;
import org.opencadc.vospace.transfer.Protocol;
import org.opencadc.vospace.transfer.Transfer;
import org.opencadc.vospace.transfer.TransferReader;
import org.opencadc.vospace.transfer.TransferWriter;

public class FilesTest extends VOSTest {
    private static final Logger log = Logger.getLogger(FilesTest.class);

    static {
        Log4jInit.setLevel("org.opencadc.conformance.vos", Level.INFO);
        Log4jInit.setLevel("org.opencadc.vos", Level.INFO);
    }

    public FilesTest() {
        super();
    }

    @Test
    public void fileTest() {
        try {
            // Put a DataNode
            String name = "files-data-node";
            URL nodeURL = getNodeURL(nodesServiceURL, name);
            VOSURI nodeURI = getVOSURI(name);
            log.debug("files-data-node URL: " + nodeURL);

            // Create a Transfer
            Transfer transfer = new Transfer(nodeURI.getURI(), Direction.pushToVoSpace);
            transfer.version = VOS.VOSPACE_21;
            Protocol protocol = new Protocol(VOS.PROTOCOL_HTTPS_PUT);
            protocol.setSecurityMethod(Standards.SECURITY_METHOD_CERT);
            transfer.getProtocols().add(protocol);

            // Get the transfer document
            TransferWriter writer = new TransferWriter();
            StringWriter sw = new StringWriter();
            writer.write(transfer, sw);
            log.debug("files-data-node transfer XML: " + sw);

            // POST the transfer document
            FileContent fileContent = new FileContent(sw.toString().getBytes(), VOSTest.XML_CONTENT_TYPE);
            URL transferURL = getNodeURL(synctransServiceURL, name);
            log.debug("transfer URL: " + transferURL);
            HttpPost post = new HttpPost(synctransServiceURL, fileContent, false);
            Subject.doAs(authSubject, new RunnableAction(post));
            Assert.assertEquals("expected POST response code = 303", 303, post.getResponseCode());
            Assert.assertNull("expected PUT throwable == null", post.getThrowable());

            // Get the transfer details
            ByteArrayOutputStream out = new ByteArrayOutputStream();
            HttpGet get = new HttpGet(post.getRedirectURL(), out);
            log.debug("GET: " + post.getRedirectURL());
            Subject.doAs(authSubject, new RunnableAction(get));
            log.debug("GET responseCode: " + get.getResponseCode());
            Assert.assertEquals("expected GET response code = 200", 200, get.getResponseCode());
            Assert.assertNull("expected GET throwable == null", get.getThrowable());
            Assert.assertTrue("expected GET Content-Type starting with " + VOSTest.XML_CONTENT_TYPE,
                              get.getContentType().startsWith(VOSTest.XML_CONTENT_TYPE));

            // Get the endpoint from the transfer details
            log.debug("transfer details XML: " + out);
            TransferReader transferReader = new TransferReader();
            Transfer details = transferReader.read(out.toString(), "vos");
            Assert.assertEquals("expected transfer direction = " + Direction.pushToVoSpace,
                                Direction.pushToVoSpace, details.getDirection());
            Assert.assertTrue("expected >0 endpoints", details.getProtocols().size() > 0);
            URL endpoint = new URL(details.getProtocols().get(0).getEndpoint());
            log.debug("endpoint: " + endpoint);

            // PUT a file to the endpoint
            String expected = "test content for files endpoint";
            ByteArrayInputStream is = new ByteArrayInputStream(expected.getBytes());
            put(endpoint, is, VOSTest.TEXT_CONTENT_TYPE);

            // get the file using files endpoint
            URL fileURL = getNodeURL(filesServiceURL, name);
            out = new ByteArrayOutputStream();
            get = new HttpGet(fileURL, out);
            Subject.doAs(authSubject, new RunnableAction(get));
            log.debug("GET responseCode: " + get.getResponseCode());
            Assert.assertEquals("expected GET response code = 200", 200, get.getResponseCode());
            Assert.assertNull("expected GET throwable == null", get.getThrowable());

            String actual = out.toString();
            log.debug("file content: " + actual);
            Assert.assertEquals("expected file content to match", expected, actual);

            // Delete the node
            delete(nodeURL);

        } catch (Exception e) {
            log.error("Unexpected error", e);
            Assert.fail("Unexpected error: " + e);
        }
    }

}