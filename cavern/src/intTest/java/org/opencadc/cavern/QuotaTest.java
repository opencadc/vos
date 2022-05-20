/*
************************************************************************
*******************  CANADIAN ASTRONOMY DATA CENTRE  *******************
**************  CENTRE CANADIEN DE DONNÉES ASTRONOMIQUES  **************
*
*  (c) 2022.                            (c) 2022.
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

package org.opencadc.cavern;

import static org.junit.Assert.fail;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URI;
import java.security.PrivilegedActionException;
import java.security.PrivilegedExceptionAction;
import java.util.ArrayList;
import java.util.List;

import javax.security.auth.Subject;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.junit.Assert;
import org.junit.Test;

import ca.nrc.cadc.auth.SSLUtil;
import ca.nrc.cadc.net.OutputStreamWrapper;
import ca.nrc.cadc.reg.Standards;
import ca.nrc.cadc.util.FileUtil;
import ca.nrc.cadc.util.Log4jInit;
import ca.nrc.cadc.vos.DataNode;
import ca.nrc.cadc.vos.Direction;
import ca.nrc.cadc.vos.Protocol;
import ca.nrc.cadc.vos.Transfer;
import ca.nrc.cadc.vos.VOS;
import ca.nrc.cadc.vos.VOSURI;
import ca.nrc.cadc.vos.client.ClientTransfer;
import ca.nrc.cadc.vos.client.VOSpaceClient;

/**
 * Test to verify that deployment and libraries correctly implement authenticated access.
 *
 * @author yeunga
 */
public class QuotaTest
{
    private static Logger log = Logger.getLogger(QuotaTest.class);

    static {
        Log4jInit.setLevel("ca.nrc.cadc.vos", Level.DEBUG);
        Log4jInit.setLevel("ca.nrc.cadc.vospace", Level.DEBUG);
        Log4jInit.setLevel("org.opencadc.cavern", Level.DEBUG);
    }

    @Test
    public void testQuotaLimitExceeded()
    {
        // test case 1: write a small file, should not exceed quota
        String testFileName = "src/test/resources/quotaTestFileSmall.txt";
        writeFile(testFileName, false);
        
        // test case 2: write a large file that exceeds the quota
        testFileName = "src/test/resources/quotaTestFile.txt";
        writeFile(testFileName, true);
    }
    
    private void writeFile(String fileName, boolean expectException)
    {
        try
        {
            File ssl_cert = FileUtil.getFileFromResource("x509_CADCAuthtest1.pem", QuotaTest.class);

            String nodeName = "quotaTestFile.txt";
            String targetContainerURIString = "vos://cadc.nrc.ca~arc/home/cadcauthtest1/do-not-delete/quotaTest";
            log.debug("target container URI = " + targetContainerURIString);
            VOSURI targetContainerURI = new VOSURI(new URI(targetContainerURIString));
            
            try {
                Subject sub = SSLUtil.createSubject(ssl_cert);
                List<Protocol> protocols = new ArrayList<Protocol>();
                Protocol basicTLS = new Protocol(VOS.PROTOCOL_HTTPS_PUT);
                basicTLS.setSecurityMethod(Standards.SECURITY_METHOD_CERT);
                protocols.add(basicTLS);
                VOSpaceClient vosClient = new VOSpaceClient(targetContainerURI.getServiceURI());
                DataNode targetNode = new DataNode(new VOSURI(new URI(targetContainerURI + "/" + nodeName)));
                log.debug("uploading: " + targetNode.getUri().getURI().toASCIIString());
                Transfer transfer = new Transfer(targetNode.getUri().getURI(), Direction.pushToVoSpace);
                transfer.getProtocols().addAll(protocols);
                transfer.version = VOS.VOSPACE_21;
                
                final ClientTransfer trans = Subject.doAs(sub, new TestActions.CreateTransferAction(vosClient, transfer, false));
                trans.setOutputStreamWrapper(new OutputStreamWrapper() {
                    public void write(OutputStream out) throws IOException {
                        File fileToUpload = new File(fileName);
                        InputStream in = new FileInputStream(fileToUpload);
                        try {
                            in.transferTo(out);
                        } finally {
                            in.close();
                        }
                    }});
                Subject.doAs(sub, new PrivilegedExceptionAction<Object>() {
                    public Object run() throws Exception {
                        trans.runTransfer();
                        return null;
                    }});
                if (expectException) {
                    fail("failed to detect quota exceeded");
                }
            } catch (PrivilegedActionException paex) {
                Throwable ex = paex.getCause();
                if (expectException && (ex != null) && (ex instanceof IOException)) {
                    String msg = ex.getCause().getMessage();
                    if (msg.contains("quota exceeded")) {
                        // expected, success
                    } else {
                        throw paex;
                    }
                } else {
                    throw paex;
                }
            } catch (Throwable t) {
                log.error("unexpected", t);
                Assert.fail("Failed to upload test file in test setup: " + t.getMessage());
            }
        }
        catch(Exception unexpected)
        {
            log.error("unexpected exception", unexpected);
            Assert.fail("unexpected exception: " + unexpected);
        }
    }
}

