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

package ca.nrc.cadc.conformance.vos;

import static org.junit.Assert.assertEquals;

import ca.nrc.cadc.reg.Standards;
import ca.nrc.cadc.util.Log4jInit;
import ca.nrc.cadc.uws.ExecutionPhase;
import ca.nrc.cadc.uws.Job;
import ca.nrc.cadc.uws.JobReader;
import ca.nrc.cadc.uws.Parameter;
import ca.nrc.cadc.vos.Direction;
import ca.nrc.cadc.vos.Protocol;
import ca.nrc.cadc.vos.Transfer;
import ca.nrc.cadc.vos.TransferParsingException;
import ca.nrc.cadc.vos.TransferReader;
import ca.nrc.cadc.vos.VOS;
import ca.nrc.cadc.vos.VOSURI;
import ca.nrc.cadc.vos.View;
import com.meterware.httpunit.WebResponse;
import java.io.IOException;
import java.io.StringReader;
import java.io.StringWriter;
import java.net.URI;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.List;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.jdom2.JDOMException;
import org.junit.Assert;
import org.junit.Test;
import org.xml.sax.SAXException;

/**
 * Test case for reading data from a service (pullFromVoSpace).
 *
 * @author jeevesh
 */
public class SyncPullFromVOSpacePackageTest extends VOSTransferTest {
    private static Logger log = Logger.getLogger(SyncPullFromVOSpacePackageTest.class);

    static {
        Log4jInit.setLevel("ca.nrc.cadc.conformance.vos", Level.INFO);
        Log4jInit.setLevel("ca.nrc.cadc.vos.server.transfers", Level.INFO);
    }

    public SyncPullFromVOSpacePackageTest() {
        super(Standards.VOSPACE_SYNC_21, Standards.VOSPACE_NODES_20);
    }

    @Test
    public void testPullFromVOSpaceTarPackage() {
        try {
            log.debug("testPullFromVOSpaceTarPackage");

            URI testURI1 = new URI("vos://cadc.nrc.ca~vospace/pkgTestURI1");
            URI testURI2 = new URI("vos://cadc.nrc.ca~vospace/pkgTestURI2");

            // Create the Transfer.
            List<Protocol> protocols = new ArrayList<Protocol>();
            protocols.add(new Protocol(VOS.PROTOCOL_HTTP_GET));
            protocols.add(new Protocol(VOS.PROTOCOL_HTTPS_GET));

            List<URI> targets = new ArrayList<URI>();
            targets.add(testURI1);
            targets.add(testURI2);

            Transfer transfer = new Transfer(Direction.pullFromVoSpace);
            transfer.getTargets().addAll(targets);
            transfer.getProtocols().addAll(protocols);

            // Add package view for tar file
            View packageView = new View(new URI(Standards.PKG_10.toString()));
            packageView.getParameters().add(new View.Parameter(new URI(VOS.PROPERTY_URI_FORMAT), "application/x-tar"));
            transfer.setView(packageView);

            TransferResult result = doPkgTransfer(transfer);

            // Job phase should be PENDING.
            assertEquals("Job phase should be PENDING", ExecutionPhase.PENDING, result.job.getExecutionPhase());

            // RESPONSEFORMAT should be added as a job parameter
            List<Parameter> pList = result.job.getParameterList();
            Assert.assertFalse("job parameter list empty", pList.isEmpty());
            boolean found = false;
            for (Parameter p: pList) {
                log.debug("parameter: " + p.getName() + ": " + p.getValue());
                if (p.getName().equals("RESPONSEFORMAT")) {
                    found = true;
                    break;
                }
            }
            Assert.assertTrue("RESPONSEFORMAT parameter not found", found == true);


            // Check the protocol objects to make sure endpoints are added
            List<Protocol> protocolList = result.transfer.getProtocols();

            for (Protocol p: protocolList) {
                // Check url quality - don't worry about jobID matching for now
                String endpoint = p.getEndpoint();
                log.debug("endpoint: " + endpoint);
                Assert.assertTrue("invalid endpoint returned: " + endpoint, endpoint.contains("/pkg/"));
            }

            log.info("testPullFromVOSpaceTarPackage passed.");
        } catch (Exception unexpected) {
            log.error("unexpected exception", unexpected);
            Assert.fail("unexpected exception: " + unexpected);
        }
    }


    @Test
    public void testPullFromVOSpacePkgNoProtocols() {
        try {
            log.debug("testPullFromVOSpacePkgNoProtocols");

            // This test doesn't need data nodes created, it should only be manipulating the Job
            URI testURI1 = new URI("vos://cadc.test/pkgTestURI1");
            URI testURI2 = new URI("vos://cadc.test/pkgTestURI2");

            List<URI> targets = new ArrayList<URI>();
            targets.add(testURI1);
            targets.add(testURI2);

            // Create the Transfer.
            Transfer transfer = new Transfer(Direction.pullFromVoSpace);
            transfer.getTargets().addAll(targets);
            // Forget adding protocols

            // Add package view for tar file
            View packageView = new View(new URI(Standards.PKG_10.toString()));
            packageView.getParameters().add(new View.Parameter(new URI(VOS.PROPERTY_URI_FORMAT), "application/x-tar"));
            transfer.setView(packageView);

            TransferResult result = doPkgTransfer(transfer);

            // The job will have finished successfully, so Job phase should be PENDING.
            assertEquals("Job phase should be PENDING", ExecutionPhase.PENDING, result.job.getExecutionPhase());

            // Check the protocol objects to make sure NO endpoints are added
            // If no protocols are provided this should happen
            List<Protocol> protocolList = result.transfer.getProtocols();
            Assert.assertTrue("protocol list should be empty: " + protocolList.size(), protocolList.isEmpty());

            log.info("testPullFromVOSpacePkgNoProtocols passed.");
        } catch (Exception unexpected) {
            log.error("unexpected exception", unexpected);
            Assert.fail("unexpected exception: " + unexpected);
        }
    }

    protected TransferResult doPkgTransfer(Transfer transfer)
        throws IOException, SAXException, JDOMException, ParseException, TransferParsingException {
        // Get the transfer XML.
        StringWriter sw = getTransferXML(transfer);
        log.debug("XML: " + sw.toString());

        // POST the XML to the transfer endpoint.
        WebResponse response = post(sw.toString());
        assertEquals("POST response code should be 303", 303, response.getResponseCode());

        String location = verifyLocation(response);
        String jobPath = getJobPath(location);

        // There are a few redirects to get to the end
        // goes through xfer apparently...
        response = get(location);
        response = followRedirects(location, response);

        // Build up the objects needed for the outgoing TransferResult
        // followRedirects response object should have a transfer document in it with the protocol endpoints added
        log.debug("last GET text: " + response.getText());
        TransferReader tr = new TransferReader();
        Transfer augmented = tr.read(response.getText(),VOSURI.SCHEME);

        // Get the UWS job that was just created/updated (jobPath will be
        // similar to /vault/transfer/<jobid>
        WebResponse jobResponse = get(jobPath);
        log.debug("jobPath response text: " + jobResponse.getText());

        JobReader jobReader = new JobReader();
        Job job = jobReader.read(new StringReader(jobResponse.getText()));

        log.debug("job parameters: " + job.getParameterList());

        return new TransferResult(augmented, job, jobPath);
    }


}
