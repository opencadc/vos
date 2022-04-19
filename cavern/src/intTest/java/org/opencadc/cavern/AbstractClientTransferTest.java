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
*  $Revision: 5 $
*
************************************************************************
*/

package org.opencadc.cavern;

import ca.nrc.cadc.uws.Job;
import ca.nrc.cadc.vos.Node;
import ca.nrc.cadc.vos.Protocol;
import ca.nrc.cadc.vos.Transfer;
import ca.nrc.cadc.vos.TransferWriter;
import ca.nrc.cadc.vos.client.ClientTransfer;
import ca.nrc.cadc.vos.client.VOSpaceClient;
import java.io.IOException;
import java.io.StringWriter;
import java.security.PrivilegedExceptionAction;
import java.util.List;
import org.apache.log4j.Logger;

/**
 * Common functions for tests that need ClientTransfer functionality.
 *
 * @author jeevesh
 */
public class AbstractClientTransferTest {
    private static final Logger log = Logger.getLogger(AbstractClientTransferTest.class);


    public AbstractClientTransferTest() { }

    protected static class GetAction implements PrivilegedExceptionAction<Node> {
        VOSpaceClient vos;
        String path;
        GetAction(VOSpaceClient vos, String path) {
            this.vos = vos;
            this.path = path;
        }
        @Override
        public Node run() throws Exception {
            return vos.getNode(path);
        }
    }

    protected static class CreateTransferAction implements PrivilegedExceptionAction<ClientTransfer> {
        VOSpaceClient vos;
        Transfer trans;

        CreateTransferAction(VOSpaceClient vos, Transfer trans) {
            this.vos = vos;
            this.trans = trans;
        }
        @Override
        public ClientTransfer run() throws Exception {
            return vos.createTransfer(trans);
        }

    }

    protected static class DeleteAction implements PrivilegedExceptionAction<Object> {
        VOSpaceClient vos;
        String path;

        DeleteAction(VOSpaceClient vos, String path) {
            this.vos = vos;
            this.path = path;
        }
        @Override
        public Object run() throws Exception {
            vos.deleteNode(path);
            return null;
        }

    }

    protected Protocol findProto(Protocol p, List<Protocol> plist) {
        for (Protocol pp : plist) {
            if (pp.equals(p))
                return pp;
        }
        return null;
    }

    protected StringWriter getTransferXML(Transfer transfer) throws IOException {
        // Get the transfer XML.
        TransferWriter writer = new TransferWriter();
        StringWriter sw = new StringWriter();
        writer.write(transfer, sw);
        return sw;
    }

    protected class TransferResult {
        public Transfer transfer;
        public Job job;
        public String location;

        TransferResult(Transfer transfer, Job job, String location) {
            this.location = location;
            this.transfer = transfer;
            this.job = job;
        }
    }
}
