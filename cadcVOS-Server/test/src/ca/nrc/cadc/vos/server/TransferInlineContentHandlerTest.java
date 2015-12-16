/*
 ************************************************************************
 *******************  CANADIAN ASTRONOMY DATA CENTRE  *******************
 **************  CENTRE CANADIEN DE DONNÉES ASTRONOMIQUES  **************
 *
 *  (c) 2010.                            (c) 2010.
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

package ca.nrc.cadc.vos.server;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.net.URI;
import java.text.DateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import ca.nrc.cadc.date.DateUtil;
import ca.nrc.cadc.util.Log4jInit;
import ca.nrc.cadc.uws.ExecutionPhase;
import ca.nrc.cadc.uws.Job;
import ca.nrc.cadc.uws.JobInfo;
import ca.nrc.cadc.uws.server.JobPersistenceUtil;
import ca.nrc.cadc.uws.web.InlineContentHandler;
import ca.nrc.cadc.vos.Direction;
import ca.nrc.cadc.vos.Protocol;
import ca.nrc.cadc.vos.Transfer;
import ca.nrc.cadc.vos.TransferReader;
import ca.nrc.cadc.vos.TransferWriter;
import ca.nrc.cadc.vos.VOS;
import ca.nrc.cadc.vos.VOSURI;

/**
 *
 * @author jburke
 */
public class TransferInlineContentHandlerTest
{
    private static Logger log = Logger.getLogger(TransferInlineContentHandlerTest.class);
    static
    {
        Log4jInit.setLevel("ca.nrc.cadc.vos", Level.INFO);
    }

    private static final String JOB_ID = "someJobID";
    private static final String RUN_ID = "someRunID";
    private static final String TEST_DATE = "2001-01-01T12:34:56.000";

    private static DateFormat dateFormat;
    private static Date baseDate;

    private static Transfer transfer;

    public TransferInlineContentHandlerTest() { }

    @BeforeClass
    public static void setUpClass()
        throws Exception
    {
        dateFormat = DateUtil.getDateFormat(DateUtil.IVOA_DATE_FORMAT, DateUtil.UTC);
        baseDate = dateFormat.parse(TEST_DATE);

        URI target = new URI("vos://cadc.nrc.ca!vospace/mydata");

        List<Protocol> protocols = new ArrayList<Protocol>();
        protocols.add(new Protocol(VOS.PROTOCOL_HTTP_GET));
        transfer = new Transfer(target, Direction.pullFromVoSpace, protocols);
    }

    @Test
    public void testAcceptTransferDocument()
    {
        try
        {
            InlineContentHandler handler = new TransferInlineContentHandler();

            TransferWriter writer = new TransferWriter();
            ByteArrayOutputStream out = new ByteArrayOutputStream();
            writer.write(transfer, out);
            ByteArrayInputStream in = new ByteArrayInputStream(out.toByteArray());

            Job job = new Job();
            job.setExecutionPhase(ExecutionPhase.PENDING);
            JobPersistenceUtil.assignID(job, JOB_ID);
            job.setRunID(RUN_ID);
            job.setQuote(new Date(baseDate.getTime() + 10000L));
            job.setExecutionDuration(123L);
            job.setDestructionTime(new Date(baseDate.getTime() + 300000L));

            try
            {
                handler.accept("filename", "text/plain", null);
                Assert.fail("Content-Type not set to text/xml should have thrown IllegalArgumentException");
            }
            catch (IllegalArgumentException ignore) {}

            try
            {
                handler.accept("filename", "text/xml", null);
                Assert.fail("Null InputStream should have thrown IOException");
            }
            catch (IOException ignore) {}

            handler.accept("filename", "text/xml", in);

            JobInfo jobInfo = handler.getJobInfo();
            Assert.assertNotNull(jobInfo.getContent());
            Assert.assertEquals("text/xml", jobInfo.getContentType());

            TransferReader reader = new TransferReader();
            Transfer newTransfer = reader.read(jobInfo.getContent(), VOSURI.SCHEME);

            Assert.assertEquals("vos uri", transfer.getTarget(), newTransfer.getTarget());
            Assert.assertEquals("dirdction", transfer.getDirection(), newTransfer.getDirection());
            Assert.assertEquals("view", transfer.getView(), newTransfer.getView());
            Assert.assertEquals("protocol uri", transfer.getProtocols().get(0).getUri(), newTransfer.getProtocols().get(0).getUri());
        }
        catch(Exception unexpected)
        {
            log.error("unexpected exception", unexpected);
            Assert.fail("unexpected exception: " + unexpected);
        }
    }

}
