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

package org.opencadc.vospace.server.transfers;

import ca.nrc.cadc.net.TransientException;
import ca.nrc.cadc.uws.ExecutionPhase;
import ca.nrc.cadc.uws.Job;
import ca.nrc.cadc.uws.Parameter;
import ca.nrc.cadc.uws.server.JobNotFoundException;
import ca.nrc.cadc.uws.server.JobPersistenceException;
import ca.nrc.cadc.uws.server.JobUpdater;
import java.io.IOException;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.List;
import org.apache.log4j.Logger;
import org.opencadc.vospace.DataNode;
import org.opencadc.vospace.LinkNode;
import org.opencadc.vospace.LinkingException;
import org.opencadc.vospace.Node;
import org.opencadc.vospace.NodeNotFoundException;
import org.opencadc.vospace.VOS;
import org.opencadc.vospace.VOSURI;
import org.opencadc.vospace.server.LocalServiceURI;
import org.opencadc.vospace.server.NodeFault;
import org.opencadc.vospace.server.NodePersistence;
import org.opencadc.vospace.server.PathResolver;
import org.opencadc.vospace.server.auth.VOSpaceAuthorizer;
import org.opencadc.vospace.transfer.Transfer;
import org.opencadc.vospace.transfer.TransferParsingException;

/**
 * @author pdowler
 */
public class PullFromVOSpaceNegotiation extends VOSpaceTransfer {

    private static final Logger log = Logger.getLogger(PullFromVOSpaceNegotiation.class);

    private VOSpaceAuthorizer authorizer;
    private boolean isPackageViewTransfer = false;

    public PullFromVOSpaceNegotiation(NodePersistence per, JobUpdater ju, Job job, Transfer transfer) {
        super(per, ju, job, transfer);
        this.authorizer = new VOSpaceAuthorizer(nodePersistence);
    }

    /*
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
     */
    @Override
    public void doAction() throws Exception {
        boolean updated = false;
        try {

            /*
            if (isPackageViewTransfer) {
                // Set job back to PENDING so next service in the chain can pick it up
                // (in this case /vault/pkg/<jobid>
                updateTransferJob(null, null, ExecutionPhase.PENDING);

                // Add RESPONSEFORMAT parameter to the job, using the parameter
                // type from the view
                Parameter p = TransferUtil.viewParam2JobParam(VOS.PROPERTY_URI_FORMAT,
                    "RESPONSEFORMAT", transfer);

                if (p != null) {
                    // View parameter with uri=VOS.PROPERTY_URI_FORMAT found
                    List<Parameter> params = new ArrayList<>();
                    params.add(p);

                    // need job id here, and uws parameter made from transfer view paramter
                    jobUpdater.addParameters(job.getID(), params);
                }

                // Set this value so in the finally clause updateTransferJob() is not
                // run again.
                updated = true;
                return;
            }
             */
            // Even though Transfer.java supports multiple targets, this type
            // of transfer does not yet.
            // This call confirms a single target exists or throws TransferException.
            confirmSingleTarget(transfer);

            VOSURI target = new VOSURI(transfer.getTargets().get(0));

            // careful to capture a link to data node so we can get the right filename in the transfer
            PathResolver resolver1 = new PathResolver(nodePersistence, authorizer, false);
            Node apparentNode = resolver1.getNode(target.getPath());
            Node actualNode = apparentNode;
            if (apparentNode instanceof LinkNode) {
                PathResolver resolver2 = new PathResolver(nodePersistence, authorizer, true);
                actualNode = resolver2.getNode(target.getPath());
            }
            log.debug("Resolved path: " + target + " -> " + actualNode);

            if (!(actualNode instanceof DataNode)) {
                throw new TransferException("target is not a data node");
            }

            LocalServiceURI loc = new LocalServiceURI(nodePersistence.getResourceID());
            updateTransferJob(apparentNode, loc.getURI(actualNode).getURI(), ExecutionPhase.EXECUTING);
            updated = true;
        } finally {
            if (!updated) {
                updateTransferJob(null, null, ExecutionPhase.QUEUED); // no phase change
            }
        }
    }
}
