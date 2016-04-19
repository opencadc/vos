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

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import org.apache.log4j.Logger;

import ca.nrc.cadc.net.TransientException;
import ca.nrc.cadc.uws.ExecutionPhase;
import ca.nrc.cadc.uws.Job;
import ca.nrc.cadc.uws.Result;
import ca.nrc.cadc.uws.server.JobNotFoundException;
import ca.nrc.cadc.uws.server.JobPersistenceException;
import ca.nrc.cadc.uws.server.JobUpdater;
import ca.nrc.cadc.vos.ContainerNode;
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

/**
 * Internal transfer is a move or copy of a node within the vospace.
 *
 * @author pdowler
 */
public class InternalTransferAction  extends VOSpaceTransfer
{
    private static final Logger log = Logger.getLogger(InternalTransferAction.class);

    private VOSpaceAuthorizer authorizer;

    public InternalTransferAction(NodePersistence per, JobUpdater ju, Job job, Transfer transfer)
    {
        super(per, ju, job, transfer);
        this.authorizer = new VOSpaceAuthorizer(true);
        authorizer.setNodePersistence(nodePersistence);
    }

    public void doAction()
        throws TransferException, JobPersistenceException, JobNotFoundException,
        LinkingException, NodeNotFoundException, TransferParsingException,
        IOException, TransientException, URISyntaxException
    {
        ExecutionPhase ep;
        try
        {
            // async job: QUEUED -> EXECUTING
            ep = jobUpdater.setPhase(job.getID(), ExecutionPhase.QUEUED, ExecutionPhase.EXECUTING, new Date());
            if ( !ExecutionPhase.EXECUTING.equals(ep) )
            {
                log.debug(job.getID() + ": QUEUED -> EXECUTING [FAILED] -- DONE");
                return;
            }
            log.debug(job.getID() + ": QUEUED -> EXECUTING [OK]");
            job.setExecutionPhase(ep);

            // move or copy?
            VOSURI srcURI = new VOSURI(transfer.getTarget());
            VOSURI destURI = new VOSURI(transfer.getDirection().getValue());
            if (transfer.isKeepBytes()) // copy
            {
                throw new UnsupportedOperationException("copyNode is not implemented");
            }

            log.debug("checking move permissions: " + srcURI + " -> " + destURI);

            // resolve the links to containers in the path so we get the actual node
            PathResolver res = new PathResolver(nodePersistence);
            Node srcNode = res.resolveWithReadPermissionCheck(srcURI, authorizer, false);

            log.debug("Resolved src path: " + srcURI + " -> " + srcNode.getUri());
            srcURI = srcNode.getUri();



            // resolve destination, parent first
            Node destParent = res.resolveWithReadPermissionCheck(destURI.getParentURI(), authorizer, true);
            if ( !(destParent instanceof ContainerNode) )
                throw new IllegalArgumentException("parent of destination is not a ContainerNode");

            ContainerNode destContainer = (ContainerNode) destParent;
            nodePersistence.getChild(destContainer, destURI.getName());
            String destName = null;
            for (Node n : destContainer.getNodes()) // safer to loop over children in case some op loads additional children
            {
                if (n.getName().equals(destURI.getName()))
                {
                    if (n instanceof ContainerNode)
                    {
                        // found: destURI is an existing container: move into
                        destContainer = (ContainerNode) n;
                        destName = srcURI.getName();
                        break;
                    }
                    else
                    {
                        NodeFault f = NodeFault.DuplicateNode;
                        f.setMessage("destination is not a container");
                        throw new TransferException(f);
                    }
                }
            }
            if (destName == null) // destURI is not an existing container: move and/or rename
            {
                // keep destContainer === destParent == destURI.getParent
                destName = destURI.getName();
            }

            log.debug("Resolved move dest: " + destContainer.getUri() + " move name: " + destName);

            // check src side of move permission
            authorizer.getDeletePermission(srcNode);
            log.debug("src permissions OK: " + srcURI + " -> " + destURI);

            // check authorization
            authorizer.getWritePermission(destContainer);
            log.debug("dest permissions OK: " + srcURI + " -> " + destURI);

            // perform the move
            log.debug("performing move: " + srcNode.getUri() + " -> " + destContainer.getUri() + "/" + destName);
            srcNode.setName(destName);
            nodePersistence.move(srcNode, destContainer);

            // final job state

            List<Result> resultsList = new ArrayList<Result>();
            String newPath = destContainer.getUri().toString() + "/" + destName;
            resultsList.add(new Result("destination", new URI(newPath)));
            job.setResultsList(resultsList);
            log.debug("setting final job state: " + ExecutionPhase.COMPLETED + " " + newPath);
            ep = jobUpdater.setPhase(job.getID(), ExecutionPhase.EXECUTING,
                    ExecutionPhase.COMPLETED, resultsList, new Date());
            if ( !ExecutionPhase.COMPLETED.equals(ep) )
            {
                log.debug(job.getID() + ": EXECUTING -> COMPLETED [FAILED] -- DONE");
                return;
            }
            log.debug(job.getID() + ": EXECUTING -> COMPLETED [OK]");
        }
        finally { }
    }
}
