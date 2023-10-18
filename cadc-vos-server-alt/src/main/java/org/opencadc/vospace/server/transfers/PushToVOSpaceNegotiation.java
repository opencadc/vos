/*
************************************************************************
*******************  CANADIAN ASTRONOMY DATA CENTRE  *******************
**************  CENTRE CANADIEN DE DONNÉES ASTRONOMIQUES  **************
*
*  (c) 2011.                            (c) 2011.
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
import ca.nrc.cadc.uws.ExecutionPhase;
import ca.nrc.cadc.uws.Job;
import ca.nrc.cadc.uws.server.JobUpdater;
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
import org.opencadc.vospace.server.auth.VOSpaceAuthorizer;
import org.opencadc.vospace.transfer.Transfer;

/**
 *
 * @author pdowler
 */
public class PushToVOSpaceNegotiation extends VOSpaceTransfer {

    private static final Logger log = Logger.getLogger(PushToVOSpaceNegotiation.class);

    private VOSpaceAuthorizer authorizer;

    public PushToVOSpaceNegotiation(NodePersistence per, JobUpdater ju, Job job, Transfer transfer) {
        super(per, ju, job, transfer);
        this.authorizer = new VOSpaceAuthorizer(nodePersistence);
    }

    public void doAction()
            throws Exception {
        boolean updated = false;
        try {
            // Even though Transfer.java supports multiple targets, this type
            // of transfer does not.
            // This call confirms a single target exists or throws TransferException.
            confirmSingleTarget(transfer);

            VOSURI target = new VOSURI(transfer.getTargets().get(0));

            PathResolver resolver = new PathResolver(nodePersistence, authorizer, true);

            Node n = resolver.getNode(target.getParentURI().getPath());
            if (!(n instanceof ContainerNode)) {
                throw new TransferException("parent is not a container node");
            }
            ContainerNode parent = (ContainerNode) n;
            log.debug("found parent: " + parent);
            Subject caller = AuthenticationUtil.getCurrentSubject();
            if (!authorizer.hasSingleNodeWritePermission(parent, caller)) {
                throw NodeFault.PermissionDenied.getStatus(target.getParentURI().getURI().toASCIIString());
            }
            
            //Node node = resolveNodeForWrite(authorizer, nodePersistence, target, DataNode.class, true, true, true);
            Node node = nodePersistence.get(parent, target.getName());
            log.debug("target node: " + target + " -> " + node);
            
            DataNode dn = null;
            if (node == null) {
                if (!authorizer.hasSingleNodeWritePermission(parent, caller)) {
                    throw NodeFault.PermissionDenied.getStatus(target.getParentURI().getURI().toASCIIString());
                }
                // create: this should do the same things that CreateNodeAction does
                dn = new DataNode(target.getName());
                dn.parent = parent;
                dn.owner = caller;
                if (parent.inheritPermissions != null && parent.inheritPermissions) {
                    dn.isPublic = parent.isPublic;
                    dn.getReadOnlyGroup().addAll(parent.getReadOnlyGroup());
                    dn.getReadWriteGroup().addAll(parent.getReadWriteGroup());
                }
                nodePersistence.put(dn);
            } else if (node instanceof DataNode) {
                dn = (DataNode) node;
                if (!authorizer.hasSingleNodeWritePermission(dn, caller)) {
                    throw NodeFault.PermissionDenied.getStatus(target.getParentURI().getURI().toASCIIString());
                }
            } else {
                throw NodeFault.InvalidArgument.getStatus("transfer destination is not a data node");
            }

            // Check the user's quota
            // Note: content length is always null until it has can
            // be collected from the transfer information
            checkQuota(dn, null);

            // If trying to write a node and the node is busy, then fail
            //VOS.NodeBusyState busy = ((DataNode) node).getBusy();
            //if (busy == VOS.NodeBusyState.busyWithWrite) {
            //    throw new NodeBusyException("node is busy with write");
            //}
            LocalServiceURI loc = new LocalServiceURI(nodePersistence.getResourceID());
            updateTransferJob(node, loc.getURI(dn).getURI(), ExecutionPhase.EXECUTING);
            updated = true;
        } finally {
            if (!updated) {
                updateTransferJob(null, null, ExecutionPhase.QUEUED); // no phase change
            }
        }
    }

    /*
    private Node resolveNodeForWrite(VOSpaceAuthorizer voSpaceAuthorizer,
            NodePersistence nodePer, VOSURI uri,
            Class<? extends Node> newNodeType, boolean persist, boolean resolveLeafNode, boolean allowWriteToLink)
            throws NodeNotFoundException, LinkingException, FileNotFoundException, URISyntaxException, TransientException {
        try {

            Node result = resolveNodeForRead(voSpaceAuthorizer, nodePer, uri, resolveLeafNode);
            try {
                result = (Node) voSpaceAuthorizer.getWritePermission(result);

                // check to ensure the parent node isn't locked
                if (result.getParent() != null && result.getParent().isLocked()) {
                    throw new NodeLockedException(result.getParent().getUri().toString());
                }

                return result;
            } catch (NodeLockedException e) {
                // allow the DuplicateNode exception to happen later if this is a link node
                if (!allowWriteToLink && result != null && result instanceof LinkNode) {
                    return result;
                }
                throw e;
            }

        } catch (NodeNotFoundException ex) {
            // this could be a new file.
            Node pn = resolveNodeForRead(voSpaceAuthorizer, nodePer,
                    uri.getParentURI(), true);
            // parent node exists
            if (!(pn instanceof ContainerNode)) {
                throw new IllegalArgumentException(
                        "parent is not a ContainerNode: "
                        + pn.getUri().getURI()
                                .toASCIIString());
            }
            voSpaceAuthorizer.getWritePermission(pn);
            // create the new DataNode

            try {
                Node newNode = null;
                if (newNodeType.equals(DataNode.class)) {
                    newNode = new DataNode(new VOSURI(pn.getUri()
                            + "/" + uri.getName()));
                } else if (newNodeType.equals(ContainerNode.class)) {
                    newNode = new ContainerNode(new VOSURI(pn.getUri()
                            + "/" + uri.getName()));
                } else {
                    throw new IllegalArgumentException(
                            "BUG: Only DataNode and ContainerNode supported");
                }
                newNode.setParent((ContainerNode) pn);
                if (persist) {
                    nodePer.put(newNode);
                }
                return newNode;
            } catch (NodeNotSupportedException e2) {
                throw new IllegalArgumentException(
                        "node type not supported.", e2);
            }

        }
    }
     */
}
