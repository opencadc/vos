/*
************************************************************************
*******************  CANADIAN ASTRONOMY DATA CENTRE  *******************
**************  CENTRE CANADIEN DE DONNÉES ASTRONOMIQUES  **************
*
*  (c) 2024.                            (c) 2024.
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
import ca.nrc.cadc.uws.ExecutionPhase;
import ca.nrc.cadc.uws.Job;
import ca.nrc.cadc.uws.Result;
import ca.nrc.cadc.uws.server.JobUpdater;
import java.net.URI;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import javax.security.auth.Subject;
import org.apache.log4j.Logger;
import org.opencadc.vospace.ContainerNode;
import org.opencadc.vospace.DataNode;
import org.opencadc.vospace.Node;
import org.opencadc.vospace.VOSURI;
import org.opencadc.vospace.server.LocalServiceURI;
import org.opencadc.vospace.server.NodeFault;
import org.opencadc.vospace.server.NodePersistence;
import org.opencadc.vospace.server.PathResolver;
import org.opencadc.vospace.server.Utils;
import org.opencadc.vospace.server.auth.VOSpaceAuthorizer;
import org.opencadc.vospace.transfer.Transfer;

/**
 * Internal transfer is a move or copy of a node within the vospace.
 *
 * @author pdowler
 */
public class InternalTransferAction extends VOSpaceTransfer {

    private static final Logger log = Logger.getLogger(InternalTransferAction.class);

    private final VOSpaceAuthorizer authorizer;

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
            PathResolver res = new PathResolver(nodePersistence, authorizer);
            Node srcNode = res.getNode(srcURI.getPath(), false);
            if (srcNode == null) {
                throw NodeFault.NodeNotFound.getStatus(srcURI.getPath());
            }

            log.debug("Resolved src path: " + srcURI + " -> " + srcNode);
            LocalServiceURI loc = new LocalServiceURI(nodePersistence.getResourceID());
            srcURI = loc.getURI(srcNode);

            PathResolver.ResolvedNode rn = res.getTargetNode(destURI.getPath());
            if (rn == null) {
                throw NodeFault.NodeNotFound.getStatus("Parent directory for destination " + destURI.getPath());
            }

            if (rn.node != null) {
                if (rn.node instanceof ContainerNode) {
                    log.debug("destination is container node - ok");
                } else if (rn.node instanceof DataNode) {
                    // this only detects the case where destination data node name is in the request
                    throw NodeFault.DuplicateNode.getStatus("destination is an existing DataNode: " + destURI.getPath());
                } else {
                    throw NodeFault.DuplicateNode.getStatus("destination (parent) is not ContainerNode: " + destURI.getPath());
                }
            }
            ContainerNode destContainer;
            String destName;
            if (rn.node == null) {
                if (rn.brokenLeafLink) {
                    throw NodeFault.UnreadableLinkTarget.getStatus(destURI.getPath());
                } else {
                    destContainer = rn.parent;
                    destName = rn.name;
                }
            } else {
                destContainer = (ContainerNode) rn.node;
                destName = srcURI.getName();
            }

            // prevent moves from directory to its sub-directories
            if (srcNode instanceof ContainerNode) {
                String srcPath = Utils.getPath(srcNode);
                String destPath = Utils.getPath(destContainer);
                if (destPath.contains(srcPath)) {
                    throw NodeFault.InvalidArgument.getStatus(
                            "Cannot move container node into its descendants: source "
                                    + "path " + srcPath + " in resolved path " + destPath);
                }
            }
            log.debug("Resolved move dest: " + destContainer + " move name: " + destName);
            
            Subject caller = AuthenticationUtil.getCurrentSubject();
            // check permission to remove src from its current parent
            if (!authorizer.hasSingleNodeWritePermission(srcNode.parent, caller)) {
                throw NodeFault.PermissionDenied.getStatus(loc.getURI(srcNode).getPath());
            }

            log.debug("src permissions OK: " + srcURI + " -> " + destURI);

            // check write permission in destination container
            if (!authorizer.hasSingleNodeWritePermission(destContainer, caller)) {
                // TODO: put the path here
                throw NodeFault.PermissionDenied.getStatus(loc.getURI(destContainer).getPath());
            }
            log.debug("dest permissions OK: " + srcURI + " -> " + destURI);

            // prevent overwrite destination when requested destination is path to parent
            Node dn = nodePersistence.get(destContainer, destName);
            if (dn != null) {
                throw NodeFault.DuplicateNode.getStatus("destination is an existing DataNode: " + loc.getURI(dn).getPath());
            }
            
            // perform the move
            log.debug("performing move: " + srcNode + " -> " + destContainer + "/" + destName);
            nodePersistence.move(srcNode, destContainer, destName);

            // final job state
            List<Result> resultsList = new ArrayList<>();
            String newPath = loc.getURI(destContainer).getURI().toASCIIString() + "/" + destName;
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

    // no longer used above but keeping for now
    private void checkRecursiveWritePermission(ContainerNode containderNode, Subject subject, LocalServiceURI loc) throws Exception {
        // collect child containers for later
        List<ContainerNode> childContainers = new ArrayList<>();
        try (ResourceIterator<Node> iter = nodePersistence.iterator(containderNode, null, null)) {
            while (iter.hasNext()) {
                Node n = iter.next();
                if (!authorizer.hasSingleNodeWritePermission(n, subject)) {
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
