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

package ca.nrc.cadc.vos.server.transfers;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.URISyntaxException;

import org.apache.log4j.Logger;

import ca.nrc.cadc.io.ByteLimitExceededException;
import ca.nrc.cadc.net.TransientException;
import ca.nrc.cadc.uws.ExecutionPhase;
import ca.nrc.cadc.uws.Job;
import ca.nrc.cadc.uws.server.JobNotFoundException;
import ca.nrc.cadc.uws.server.JobPersistenceException;
import ca.nrc.cadc.uws.server.JobUpdater;
import ca.nrc.cadc.vos.ContainerNode;
import ca.nrc.cadc.vos.DataNode;
import ca.nrc.cadc.vos.LinkNode;
import ca.nrc.cadc.vos.LinkingException;
import ca.nrc.cadc.vos.Node;
import ca.nrc.cadc.vos.NodeBusyException;
import ca.nrc.cadc.vos.NodeFault;
import ca.nrc.cadc.vos.NodeLockedException;
import ca.nrc.cadc.vos.NodeNotFoundException;
import ca.nrc.cadc.vos.NodeNotSupportedException;
import ca.nrc.cadc.vos.Transfer;
import ca.nrc.cadc.vos.TransferParsingException;
import ca.nrc.cadc.vos.VOS;
import ca.nrc.cadc.vos.VOSURI;
import ca.nrc.cadc.vos.server.db.NodePersistence;
import ca.nrc.cadc.vos.server.PathResolver;
import ca.nrc.cadc.vos.server.auth.VOSpaceAuthorizer;

/**
 *
 * @author pdowler
 */
public class PushToVOSpaceNegotiation  extends VOSpaceTransfer
{
    private static final Logger log = Logger.getLogger(PushToVOSpaceNegotiation.class);

    private VOSpaceAuthorizer authorizer;

    public PushToVOSpaceNegotiation(NodePersistence per, JobUpdater ju, Job job, Transfer transfer)
    {
        super(per, ju, job, transfer);
        this.authorizer = new VOSpaceAuthorizer(true);
        authorizer.setNodePersistence(nodePersistence);
    }

    public void doAction()
        throws TransferException, JobPersistenceException, JobNotFoundException,
        LinkingException, NodeNotFoundException, TransferParsingException,
        IOException, TransientException, URISyntaxException, NodeBusyException,
        ByteLimitExceededException
    {
        boolean updated = false;
        try
        {
            // Even though Transfer.java supports multiple targets, this type
            // of transfer does not.
            // This call confirms a single target exists or throws TransferException.
            TransferUtil.confirmSingleTarget(transfer);

            VOSURI target = new VOSURI(transfer.getTargets().get(0));

            PathResolver resolver = new PathResolver(nodePersistence);

            Node parent = resolver.resolveWithReadPermissionCheck(target.getParentURI(), authorizer, true);
            if ( !(parent instanceof ContainerNode) )
            {
                NodeFault f = NodeFault.InvalidArgument;
                f.setMessage("parent is not a container node");
                throw new TransferException(f);
            }
            // this might create it, TODO: move create out here so method only does what it says
            Node node = resolveNodeForWrite(authorizer, nodePersistence, target, DataNode.class, true, true, true);
            log.debug("Resolved path: " + target + " -> " + node.getUri());

            if (!(node instanceof DataNode))
            {
                NodeFault f = NodeFault.InvalidArgument;
                f.setMessage("destination is not a data node");
                throw new TransferException(f);
            }

            // Check the user's quota
            // Note: content length is always null until it has can
            // be collected from the transfer information
            checkQuota(node, null);

            // If trying to write a node and the node is busy, then fail
            VOS.NodeBusyState busy = ((DataNode) node).getBusy();
            if (busy == VOS.NodeBusyState.busyWithWrite)
            {
                NodeFault f = NodeFault.InvalidArgument;
                f.setMessage("node is busy with write");
                throw new NodeBusyException(f.getMessage());
            }

            updateTransferJob(node, node.getUri().getURI(), ExecutionPhase.EXECUTING);
            updated = true;
        }
        finally
        {
            if (!updated)
                updateTransferJob(null, null, ExecutionPhase.QUEUED); // no phase change
        }
    }

    // resolves all the links in the path and returns a node with
    // a resolved path
    private Node resolveNodeForRead(VOSpaceAuthorizer partialPathVOSpaceAuthorizer,
            NodePersistence nodePer, VOSURI uri, boolean resolveLeafNode)
        throws NodeNotFoundException, LinkingException, TransientException
    {
        PathResolver resolver = new PathResolver(nodePer);
        return resolver.resolveWithReadPermissionCheck(
                uri, partialPathVOSpaceAuthorizer, resolveLeafNode);
    }

    // resolves all the links in the path, checks for write authorization
    // and if the target does not exists, it might create it (container
    // node must exist)
    private Node resolveNodeForWrite(VOSpaceAuthorizer voSpaceAuthorizer,
            NodePersistence nodePer, VOSURI uri,
            Class<? extends Node> newNodeType, boolean persist, boolean resolveLeafNode, boolean allowWriteToLink)
        throws NodeNotFoundException, LinkingException, FileNotFoundException, URISyntaxException, TransientException
    {
        try
        {

            Node result = resolveNodeForRead(voSpaceAuthorizer, nodePer, uri, resolveLeafNode);
            try
            {
                result = (Node) voSpaceAuthorizer.getWritePermission(result);

                // check to ensure the parent node isn't locked
                if (result.getParent() != null && result.getParent().isLocked())
                    throw new NodeLockedException(result.getParent().getUri().toString());

                return result;
            }
            catch (NodeLockedException e)
            {
                // allow the DuplicateNode exception to happen later if this is a link node
                if (!allowWriteToLink && result != null && result instanceof LinkNode)
                {
                    return result;
                }
                throw e;
            }

        }
        catch (NodeNotFoundException ex)
        {
            // this could be a new file.
            Node pn = resolveNodeForRead(voSpaceAuthorizer, nodePer,
                    uri.getParentURI(), true);
            // parent node exists
            if (!(pn instanceof ContainerNode))
            {
                throw new IllegalArgumentException(
                        "parent is not a ContainerNode: "
                                + pn.getUri().getURI()
                                        .toASCIIString());
            }
            voSpaceAuthorizer.getWritePermission(pn);
            // create the new DataNode

            try
            {
                Node newNode = null;
                if (newNodeType.equals(DataNode.class))
                {
                    newNode = new DataNode(new VOSURI(pn.getUri()
                        + "/" + uri.getName()));
                }
                else if(newNodeType.equals(ContainerNode.class))
                {
                    newNode = new ContainerNode(new VOSURI(pn.getUri()
                            + "/" + uri.getName()));
                }
                else
                {
                    throw new IllegalArgumentException(
                            "BUG: Only DataNode and ContainerNode supported");
                }
                newNode.setParent((ContainerNode) pn);
                if( persist )
                {
                    nodePer.put(newNode);
                }
                return newNode;
            }
            catch (NodeNotSupportedException e2)
            {
                throw new IllegalArgumentException(
                        "node type not supported.", e2);
            }

        }
    }
}
