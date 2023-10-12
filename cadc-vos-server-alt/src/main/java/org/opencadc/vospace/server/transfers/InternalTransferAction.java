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

import ca.nrc.cadc.auth.AuthenticationUtil;
import ca.nrc.cadc.io.ResourceIterator;
import ca.nrc.cadc.net.TransientException;
import ca.nrc.cadc.uws.ExecutionPhase;
import ca.nrc.cadc.uws.Job;
import ca.nrc.cadc.uws.Result;
import ca.nrc.cadc.uws.server.JobNotFoundException;
import ca.nrc.cadc.uws.server.JobPersistenceException;
import ca.nrc.cadc.uws.server.JobUpdater;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import javax.security.auth.Subject;
import org.apache.log4j.Logger;
import org.opencadc.vospace.ContainerNode;
import org.opencadc.vospace.LinkingException;
import org.opencadc.vospace.Node;
import org.opencadc.vospace.NodeNotFoundException;
import org.opencadc.vospace.VOSURI;
import org.opencadc.vospace.server.LocalServiceURI;
import org.opencadc.vospace.server.NodeFault;
import org.opencadc.vospace.server.NodePersistence;
import org.opencadc.vospace.server.PathResolver;
import org.opencadc.vospace.server.auth.VOSpaceAuthorizer;
import org.opencadc.vospace.transfer.Transfer;
import org.opencadc.vospace.transfer.TransferParsingException;

/**
 * Internal transfer is a move or copy of a node within the vospace.
 *
 * @author pdowler
 */
public class InternalTransferAction extends VOSpaceTransfer {

    private static final Logger log = Logger.getLogger(InternalTransferAction.class);

    private VOSpaceAuthorizer authorizer;

    public InternalTransferAction(NodePersistence per, JobUpdater ju, Job job, Transfer transfer) {
        super(per, ju, job, transfer);
        this.authorizer = new VOSpaceAuthorizer(nodePersistence);
    }

    public void doAction() throws Exception {
        ExecutionPhase ep;
        try {
            // async job: QUEUED -> EXECUTING
            ep = jobUpdater.setPhase(job.getID(), ExecutionPhase.QUEUED, ExecutionPhase.EXECUTING, new Date());
            if (!ExecutionPhase.EXECUTING.equals(ep)) {
                log.debug(job.getID() + ": QUEUED -> EXECUTING [FAILED] -- DONE");
                return;
            }
            log.debug(job.getID() + ": QUEUED -> EXECUTING [OK]");
            job.setExecutionPhase(ep);

            // Even though Transfer.java supports multiple targets, this type of negotiation does not.
            int targetListSize = transfer.getTargets().size();
            if (targetListSize > 1) {
                throw new UnsupportedOperationException("Move/Copy supports a single target (found " + targetListSize + ")");
            }
            if (targetListSize == 0) {
                throw new UnsupportedOperationException("Move/Copy requires a target (found " + targetListSize + ")");
            }

            // move or copy?
            VOSURI srcURI = new VOSURI(transfer.getTargets().get(0));
            VOSURI destURI = new VOSURI(transfer.getDirection().getValue());
            if (transfer.isKeepBytes()) {
                throw new UnsupportedOperationException("copyNode is not implemented");
            }

            log.debug("checking move permissions: " + srcURI + " -> " + destURI);

            // resolve the links to containers in the path so we get the actual node
            PathResolver res = new PathResolver(nodePersistence, authorizer, true);
            Node srcNode = res.getNode(srcURI.getPath());

            log.debug("Resolved src path: " + srcURI + " -> " + srcNode);
            LocalServiceURI loc = new LocalServiceURI(nodePersistence.getResourceID());
            srcURI = loc.getURI(srcNode);

            // resolve destination, parent first
            Node destParent = res.getNode(destURI.getParentURI().getPath());
            if (!(destParent instanceof ContainerNode)) {
                throw new IllegalArgumentException("parent of destination is not a ContainerNode");
            }

            ContainerNode destContainer = (ContainerNode) destParent;
            Node n = nodePersistence.get(destContainer, destURI.getName());
            String destName = null;
            if (n instanceof ContainerNode) {
                // found: destURI is an existing container: move into
                destContainer = (ContainerNode) n;
                destName = srcURI.getName();
            } else {
                throw new TransferException("destination is not a container");
            }
            if (destName == null) {
                // destURI is not an existing container: move and/or rename
                // keep destContainer === destParent == destURI.getParent
                destName = destURI.getName();
            }

            log.debug("Resolved move dest: " + destContainer + " move name: " + destName);

            Subject caller = AuthenticationUtil.getCurrentSubject();
            // check src side of move permission
            //authorizer.getDeletePermission(srcNode);
            if (srcNode instanceof ContainerNode) {
                checkRecursiveWritePermission((ContainerNode) srcNode, caller, loc);
            } else {
                if (!authorizer.hasSingleNodeWritePermission(srcNode, caller)) {
                    // TODO: put the path here
                    throw NodeFault.PermissionDenied.getStatus(loc.getURI(srcNode).getPath());
                }
            }

            log.debug("src permissions OK: " + srcURI + " -> " + destURI);

            // check authorization
            //authorizer.getWritePermission(destContainer);
            if (!authorizer.hasSingleNodeWritePermission(destContainer, caller)) {
                // TODO: put the path here
                throw NodeFault.PermissionDenied.getStatus(loc.getURI(destContainer).getPath());
            }
            log.debug("dest permissions OK: " + srcURI + " -> " + destURI);

            // perform the move
            log.debug("performing move: " + srcNode + " -> " + destContainer + "/" + destName);
            srcNode.setName(destName);
            srcNode.parent = destContainer;
            // TODO: srcNode.owner = caller; ??
            //nodePersistence.move(srcNode, destContainer);
            nodePersistence.put(srcNode);

            // final job state
            List<Result> resultsList = new ArrayList<Result>();
            String newPath = loc.getURI(srcNode).getPath() + "/" + destName;
            resultsList.add(new Result("destination", new URI(newPath)));
            job.setResultsList(resultsList);
            log.debug("setting final job state: " + ExecutionPhase.COMPLETED + " " + newPath);
            ep = jobUpdater.setPhase(job.getID(), ExecutionPhase.EXECUTING,
                    ExecutionPhase.COMPLETED, resultsList, new Date());
            if (!ExecutionPhase.COMPLETED.equals(ep)) {
                log.debug(job.getID() + ": EXECUTING -> COMPLETED [FAILED] -- DONE");
                return;
            }
            log.debug(job.getID() + ": EXECUTING -> COMPLETED [OK]");
        } finally {
            // TODO? 
        }
    }

    private void checkRecursiveWritePermission(ContainerNode containderNode, Subject subject, LocalServiceURI loc) throws Exception {
        // collect child containers for later
        List<ContainerNode> childContainers = new ArrayList<>();
        try (ResourceIterator<Node> iter = nodePersistence.iterator(containderNode, null, null)) {
            while (iter.hasNext()) {
                Node n = iter.next();
                if (!authorizer.hasSingleNodeWritePermission(n, subject)) {
                    // TODO: put the path here
                    throw NodeFault.PermissionDenied.getStatus(loc.getURI(n).getPath());
                }
                if (n instanceof ContainerNode) {
                    ContainerNode cn = (ContainerNode) n;
                    childContainers.add(cn);
                }
            }
        }
        // now recurse so we only have one open iterator at a time
        for (ContainerNode cn : childContainers) {
            checkRecursiveWritePermission(cn, subject, loc);
        }
    }
}
