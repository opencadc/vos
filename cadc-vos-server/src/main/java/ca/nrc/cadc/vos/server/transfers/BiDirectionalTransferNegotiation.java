/*
************************************************************************
*******************  CANADIAN ASTRONOMY DATA CENTRE  *******************
**************  CENTRE CANADIEN DE DONNÉES ASTRONOMIQUES  **************
*
*  (c) 2018.                            (c) 2018.
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

package ca.nrc.cadc.vos.server.transfers;

import ca.nrc.cadc.net.TransientException;
import ca.nrc.cadc.uws.ExecutionPhase;
import ca.nrc.cadc.uws.Job;
import ca.nrc.cadc.uws.server.JobNotFoundException;
import ca.nrc.cadc.uws.server.JobPersistenceException;
import ca.nrc.cadc.uws.server.JobUpdater;
import ca.nrc.cadc.vos.ContainerNode;
import ca.nrc.cadc.vos.LinkingException;
import ca.nrc.cadc.vos.Node;
import ca.nrc.cadc.vos.NodeBusyException;
import ca.nrc.cadc.vos.NodeFault;
import ca.nrc.cadc.vos.NodeNotFoundException;
import ca.nrc.cadc.vos.Transfer;
import ca.nrc.cadc.vos.TransferParsingException;
import ca.nrc.cadc.vos.VOSURI;
import ca.nrc.cadc.vos.server.NodePersistence;
import ca.nrc.cadc.vos.server.PathResolver;
import ca.nrc.cadc.vos.server.auth.VOSpaceAuthorizer;
import java.io.IOException;
import java.net.URISyntaxException;
import org.apache.log4j.Logger;

/**
 *
 * @author pdowler
 */
public class BiDirectionalTransferNegotiation extends VOSpaceTransfer {
    private static final Logger log = Logger.getLogger(BiDirectionalTransferNegotiation.class);

    private VOSpaceAuthorizer authorizer;

    public BiDirectionalTransferNegotiation(NodePersistence per, JobUpdater ju, Job job, Transfer transfer)
    {
        super(per, ju, job, transfer);
        this.authorizer = new VOSpaceAuthorizer(true);
        authorizer.setNodePersistence(nodePersistence);
    }

    @Override
    public void doAction() 
            throws TransferException, JobPersistenceException, JobNotFoundException, LinkingException, NodeNotFoundException, TransferParsingException, 
            IOException, TransientException, URISyntaxException, NodeBusyException {
        boolean updated = false;
        try {
            // Even though Transfer.java supports multiple targets, this type
            // of transfer does not.
            // This call confirms a single target exists or throws TransferException.
            TransferUtil.confirmSingleTarget(transfer);
            VOSURI target = new VOSURI(transfer.getTargets().get(0));

            PathResolver resolver = new PathResolver(nodePersistence);

            Node node = resolver.resolveWithReadPermissionCheck(target, authorizer, true);
            if (!(node instanceof ContainerNode)) {
                NodeFault f = NodeFault.InvalidArgument;
                f.setMessage("node is not a container node");
                throw new TransferException(f);
            }
            
            updateTransferJob(node, node.getUri().getURI(), ExecutionPhase.EXECUTING);
            updated = true;
        }
        finally {
            if (!updated) {
                updateTransferJob(null, null, ExecutionPhase.QUEUED); // no phase change
            }
        }
    }
}
