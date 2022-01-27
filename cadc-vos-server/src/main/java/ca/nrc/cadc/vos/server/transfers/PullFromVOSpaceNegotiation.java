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
 *  $Revision: 5 $
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
import ca.nrc.cadc.vos.DataNode;
import ca.nrc.cadc.vos.LinkNode;
import ca.nrc.cadc.vos.LinkingException;
import ca.nrc.cadc.vos.Node;
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
 * @author pdowler
 */
public class PullFromVOSpaceNegotiation extends VOSpaceTransfer {
    private static final Logger log = Logger.getLogger(PullFromVOSpaceNegotiation.class);

    private VOSpaceAuthorizer authorizer;
    private boolean isPackageViewTransfer = false;

    public PullFromVOSpaceNegotiation(NodePersistence per, JobUpdater ju, Job job, Transfer transfer) {
        super(per, ju, job, transfer);
        this.authorizer = new VOSpaceAuthorizer(true);
        authorizer.setNodePersistence(nodePersistence);
    }

    @Override
    public void validateView() throws IllegalArgumentException,
            TransferException, Exception {
        // There is no representation for the package view in View.properties,
        // so this function needs to do some extra work for this instance.
        // For all other View values, the standard validation is performed.

        if (TransferUtil.isPackageTransfer(transfer)) {
            isPackageViewTransfer = true;
        } else {
            super.validateView();
        }
    }

    @Override
    public void doAction()
        throws TransferException, JobPersistenceException, JobNotFoundException,
        LinkingException, NodeNotFoundException, TransferParsingException,
        IOException, TransientException, URISyntaxException {
        boolean updated = false;
        try {

            if (isPackageViewTransfer) {
                // Set job back to PENDING so next service in the chain can pick it up
                // (in this case /vault/pkg/<jobid>
                updateTransferJob(null, null, ExecutionPhase.PENDING);

                // Set this value so in the finally clause updateTransferJob() is not
                // run again.
                updated = true;
                return;
            }

            // Even though Transfer.java supports multiple targets, this type
            // of transfer does not yet. When view=zip | tar is implemented,
            // this is likely to change.
            // This call confirms a single target exists or throws TransferException.
            TransferUtil.confirmSingleTarget(transfer);

            VOSURI target = new VOSURI(transfer.getTargets().get(0));

            PathResolver resolver = new PathResolver(nodePersistence);

            // TODO: change this to check all targets provided in the transfer
            // careful to capture a link to data node so we can get the right filename in the transfer
            Node apparentNode = resolver.resolveWithReadPermissionCheck(target, authorizer, false);
            Node actualNode = apparentNode;
            if (apparentNode instanceof LinkNode) {
                // TODO: override resolve to start at node
                actualNode = resolver.resolveLeafNodeWithReadPermissionCheck(target, apparentNode, authorizer);
            }
            log.debug("Resolved path: " + target + " -> " + actualNode.getUri());


            if (!(actualNode instanceof DataNode)) {
                NodeFault f = NodeFault.InvalidArgument;
                f.setMessage("target is not a data node");
                throw new TransferException(f);
            }

            updateTransferJob(apparentNode, actualNode.getUri().getURI(), ExecutionPhase.EXECUTING);
            updated = true;
        } finally {
            if (!updated) {
                updateTransferJob(null, null, ExecutionPhase.QUEUED); // no phase change
            }
        }
    }
}
