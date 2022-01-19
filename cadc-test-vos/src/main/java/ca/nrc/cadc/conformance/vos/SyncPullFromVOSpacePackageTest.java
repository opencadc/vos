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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import ca.nrc.cadc.reg.Standards;
import ca.nrc.cadc.util.Log4jInit;
import ca.nrc.cadc.uws.ExecutionPhase;
import ca.nrc.cadc.uws.Job;
import ca.nrc.cadc.uws.JobReader;
import ca.nrc.cadc.vos.Direction;
import ca.nrc.cadc.vos.NodeProperty;
import ca.nrc.cadc.vos.NodeWriter;
import ca.nrc.cadc.vos.Protocol;
import ca.nrc.cadc.vos.Transfer;
import ca.nrc.cadc.vos.VOS;
import ca.nrc.cadc.vos.View;
import com.meterware.httpunit.WebResponse;
import java.io.StringReader;
import java.net.URI;
import java.util.ArrayList;
import java.util.List;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.junit.Assert;
import org.junit.Test;

/**
 * Test case for reading data from a service (pullFromVoSpace).
 *
 * @author jeevesh
 */
public class SyncPullFromVOSpacePackageTest extends VOSTransferTest
{
    private static Logger log = Logger.getLogger(SyncPullFromVOSpacePackageTest.class);

    static
    {
        Log4jInit.setLevel("ca.nrc.cadc.conformance.vos", Level.INFO);
    }

    public SyncPullFromVOSpacePackageTest()
    {
        super(Standards.VOSPACE_SYNC_21, Standards.VOSPACE_NODES_20);
    }

    @Test
    public void testPullFromVOSpacePackage()
    {
        try
        {
            log.debug("testPullFromVOSpacePackage");

            // Get a DataNode.
            TestNode dataNode = getSampleDataNode();
            dataNode.sampleNode.getProperties().add(new NodeProperty(VOS.PROPERTY_URI_CONTENTLENGTH, new Long(1024).toString()));
            WebResponse response = put(getNodeStandardID(),dataNode.sampleNode, new NodeWriter());
            assertEquals("PUT response code should be 200", 200, response.getResponseCode());

            // Create the Transfer.
            List<Protocol> protocols = new ArrayList<Protocol>();
            protocols.add(new Protocol(VOS.PROTOCOL_HTTP_GET));
            protocols.add(new Protocol(VOS.PROTOCOL_HTTPS_GET));
            Transfer transfer = new Transfer(dataNode.sampleNode.getUri().getURI(), Direction.pullFromVoSpace);
            transfer.getProtocols().addAll(protocols);
            // Add package view for tar file
            View packageView = new View(new URI(VOS.VIEW_PACKAGE));
            packageView.getParameters().add(new View.Parameter(new URI(VOS.PROPERTY_URI_ACCEPT), "tar"));
            transfer.setView(packageView);

            // Start the transfer.
            TransferResult result = doSyncTransfer(transfer);

            assertEquals("direction", Direction.pullFromVoSpace, result.transfer.getDirection());
            assertNotNull("protocols", result.transfer.getProtocols());

            // Get the Job.
            response = get(result.location);
            assertEquals("GET of Job response code should be 200", 200, response.getResponseCode());

            // Job phase should be EXECUTING.
            JobReader jobReader = new JobReader();
            Job job = jobReader.read(new StringReader(response.getText()));
            assertEquals("Job phase should be EXECUTING", ExecutionPhase.PENDING, job.getExecutionPhase());

            // Invalid Protocol should not be returned.
            assertTrue("has some protocols", !result.transfer.getProtocols().isEmpty());
            String endpoint = null;
            for (Protocol p : result.transfer.getProtocols())
            {
                String expectedEndpoint = "/vault/pkg/" + job.getID();
                endpoint = p.getEndpoint();
                log.debug("expected endpoint: " + expectedEndpoint + ", found: " + endpoint);
                Assert.assertEquals(expectedEndpoint, endpoint);
            }

//            // Delete the node
//            response = delete(getNodeStandardID(), dataNode.sampleNode);
//            assertEquals("DELETE response code should be 200", 200, response.getResponseCode());

            log.info("testPullFromVOSpace passed.");
        }
        catch (Exception unexpected)
        {
            log.error("unexpected exception", unexpected);
            Assert.fail("unexpected exception: " + unexpected);
        }
    }

}
