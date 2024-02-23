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
************************************************************************
*/

package org.opencadc.cavern;

import ca.nrc.cadc.net.FileContent;
import ca.nrc.cadc.net.HttpGet;
import ca.nrc.cadc.net.HttpUpload;
import ca.nrc.cadc.util.FileUtil;
import ca.nrc.cadc.util.Log4jInit;
import java.io.File;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.LineNumberReader;
import java.net.URI;
import java.net.URL;
import java.nio.charset.Charset;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.junit.Assert;
import org.junit.Test;
import org.opencadc.gms.GroupURI;
import org.opencadc.vospace.VOS;
import org.opencadc.vospace.VOSURI;
import org.opencadc.vospace.transfer.Direction;
import org.opencadc.vospace.transfer.Protocol;
import org.opencadc.vospace.transfer.Transfer;

/**
 *
 * @author pdowler
 */
public class TransferTest extends org.opencadc.conformance.vos.TransferTest {
    private static final Logger log = Logger.getLogger(TransferTest.class);

    static {
        Log4jInit.setLevel("org.opencadc.conformance.vos", Level.INFO);
        Log4jInit.setLevel("org.opencadc.vospace", Level.INFO);
        Log4jInit.setLevel("org.opencadc.cavern", Level.INFO);
    }
    
    public TransferTest() {
        super(Constants.RESOURCE_ID, Constants.TEST_CERT);

        // enables the updateNodeNotOwnedTest, the cavern-auth-test.pem user, and the Constants.TEST_CERT user,
        // are both members of the writeGroup group.
        GroupURI writeGroup = new GroupURI(URI.create("ivo://cadc.nrc.ca/gms?Test-Write"));
        File writeGroupCert = FileUtil.getFileFromResource("cavern-write-test.pem", TransferTest.class);
        super.enableWriteAccess(writeGroup, writeGroupCert);
    }

    @Test
    public void testPreauthToken() {
        try {
            // Create a DataNode.
            String path = "sync-push-pull-preauth";
            URL nodeURL = getNodeURL(nodesServiceURL, path);
            VOSURI nodeURI = getVOSURI(path);
            log.debug("nodeURL: " + nodeURL);

            // Cleanup leftover node
            delete(nodeURL, false);

            // Create a push-to-vospace Transfer for the node
            Transfer pushTransfer = new Transfer(nodeURI.getURI(), Direction.pushToVoSpace);
            pushTransfer.version = VOS.VOSPACE_21;
            Protocol protocol = new Protocol(VOS.PROTOCOL_HTTPS_PUT);
            // anon only to get preauth
            pushTransfer.getProtocols().add(protocol);

            // Do the transfer
            Transfer details = doTransfer(pushTransfer);
            Assert.assertEquals("expected transfer direction = " + Direction.pushToVoSpace,
                    Direction.pushToVoSpace, details.getDirection());
            Assert.assertNotNull("expected > 0 protocols", details.getProtocols());
            Assert.assertEquals(1, details.getProtocols().size());
            Protocol p = details.getProtocols().get(0);
            Assert.assertNull(p.getSecurityMethod());
            URL putURL = new URL(p.getEndpoint());
            log.info("put URL: " + putURL);
            
            // try to put the bytes
            String msg = "cavern testPreauthToken";
            FileContent content = new FileContent(msg, "text/plain", Charset.forName("UTF-8"));
            HttpUpload put = new HttpUpload(content, putURL);
            put.prepare(); // throws
            // no response body
            
            // Create a pull-from-vospace Transfer for the node
            Transfer pullTransfer = new Transfer(nodeURI.getURI(), Direction.pullFromVoSpace);
            pullTransfer.version = VOS.VOSPACE_21;
            // anon only to get preauth
            pullTransfer.getProtocols().add(protocol);

            // Do the transfer
            details = doTransfer(pullTransfer);
            Assert.assertEquals("expected transfer direction = " + Direction.pullFromVoSpace,
                    Direction.pullFromVoSpace, details.getDirection());
            Assert.assertNotNull("expected > 0 protocols", details.getProtocols());
            Assert.assertEquals(2, details.getProtocols().size());
            p = details.getProtocols().get(0);
            Assert.assertNull(p.getSecurityMethod());
            URL getURL = new URL(p.getEndpoint());
            log.info("get URL: " + getURL);
            
            HttpGet get = new HttpGet(getURL, true);
            get.prepare(); // throws
            InputStream istream = get.getInputStream();
            LineNumberReader r = new LineNumberReader(new InputStreamReader(istream));
            String actual = r.readLine();
            Assert.assertEquals(msg, actual);
            
            // Delete the node
            delete(nodeURL, false);

        } catch (Exception e) {
            log.error("Unexpected error", e);
            Assert.fail("Unexpected error: " + e);
        }
    }
}
