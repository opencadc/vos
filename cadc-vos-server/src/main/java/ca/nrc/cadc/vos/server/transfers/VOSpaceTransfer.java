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
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import org.apache.log4j.Logger;

import ca.nrc.cadc.auth.AuthMethod;
import ca.nrc.cadc.io.ByteLimitExceededException;
import ca.nrc.cadc.net.TransientException;
import ca.nrc.cadc.reg.Standards;
import ca.nrc.cadc.reg.client.RegistryClient;
import ca.nrc.cadc.uws.ExecutionPhase;
import ca.nrc.cadc.uws.Job;
import ca.nrc.cadc.uws.Result;
import ca.nrc.cadc.uws.server.JobNotFoundException;
import ca.nrc.cadc.uws.server.JobPersistenceException;
import ca.nrc.cadc.uws.server.JobUpdater;
import ca.nrc.cadc.vos.LinkNode;
import ca.nrc.cadc.vos.LinkingException;
import ca.nrc.cadc.vos.Node;
import ca.nrc.cadc.vos.NodeBusyException;
import ca.nrc.cadc.vos.NodeNotFoundException;
import ca.nrc.cadc.vos.NodeProperty;
import ca.nrc.cadc.vos.Transfer;
import ca.nrc.cadc.vos.TransferParsingException;
import ca.nrc.cadc.vos.VOS;
import ca.nrc.cadc.vos.View;
import ca.nrc.cadc.vos.server.LocalServiceURI;
import ca.nrc.cadc.vos.server.NodePersistence;
import ca.nrc.cadc.vos.server.Views;

/**
 * Base class for all types of transfers.
 *
 * @author pdowler
 */
public abstract class VOSpaceTransfer
{
    private static final Logger log = Logger.getLogger(VOSpaceTransfer.class);

    private URI defaultViewURI;
    private URI serviceURI;
    private RegistryClient regClient;

    protected NodePersistence nodePersistence;
    protected Job job;
    protected Transfer transfer;
    protected JobUpdater jobUpdater;


    private VOSpaceTransfer() { }

    protected VOSpaceTransfer(NodePersistence per, JobUpdater jobUpdater, Job job, Transfer transfer)
    {
        this.nodePersistence = per;
        this.jobUpdater = jobUpdater;
        this.job = job;
        this.transfer = transfer;
        this.regClient = new RegistryClient();
        try
        {
            this.defaultViewURI = new URI(VOS.VIEW_DEFAULT);
            LocalServiceURI localServiceURI = new LocalServiceURI();
            this.serviceURI = localServiceURI.getURI();
            log.debug("vospace serviceURI: " + serviceURI);
        }
        catch(Throwable bug)
        {
            throw new RuntimeException("BUG - failed to create VOSpace service URI", bug);
        }
    }

    public void validateView()
        throws TransferException, Exception
    {
        if (transfer.getView() != null && !transfer.getView().getURI().equals(defaultViewURI))
        {
            Views views = new Views();
            View view = views.getView(transfer.getView().getURI());
            if (view == null)
            {
                updateTransferJob(null, null, job.getExecutionPhase());
                throw new TransferException("ViewNotSupported (" + transfer.getView().getURI() + ")");
            }
        }
    }

    protected void updateTransferJob(Node target, URI resolvedPath, ExecutionPhase end)
        throws JobNotFoundException, JobPersistenceException, IOException, TransientException
    {
        URI uri;
        try
        {
            AuthMethod authMethod = AuthMethod.ANON;
            if (job.getProtocol().equals("https"))
            {
                authMethod = AuthMethod.CERT;
            }
            URL serviceURL = regClient.getServiceURL(serviceURI, Standards.VOSPACE_XFER_20, authMethod);
            String path = "/" + job.getID();
            URL url = new URL(serviceURL.toExternalForm() + path);
            log.debug("transfer URL: " + url);

            uri = url.toURI();
        }
        catch (URISyntaxException e)
        {
            throw new RuntimeException("BUG: failed to create xfer URL", e);
        }

        // Job Results
        List<Result> resultsList = new ArrayList<Result>();
        resultsList.add(new Result("transferDetails", uri));
        if (resolvedPath != null)
        {
            resultsList.add(new Result(TransferDetailsServlet.DATA_NODE, resolvedPath));
        }
        if (target instanceof LinkNode)
        {
            resultsList.add(new Result(TransferDetailsServlet.LINK_NODE, target.getUri().getURI()));
        }
        job.setResultsList(resultsList);
        ExecutionPhase ep = jobUpdater.setPhase(job.getID(), ExecutionPhase.QUEUED,
                end, resultsList, new Date());
        if ( !end.equals(ep) )
        {
            log.debug(job.getID() + ": QUEUED -> "+end+" [FAILED] -- DONE");
            return;
        }
        log.debug(job.getID() + ": QUEUED -> "+end+" [OK]");
    }

    protected void checkQuota(Node node, Long contentLength) throws ByteLimitExceededException, TransientException
    {
        // get the remaining space for this user
        long quota;
        try
        {
            // get the root node and call getRemainingQuota()
            Node root = node;
            while (root.getParent() != null)
            {
                root = root.getParent();
            }
            quota = getRemainingQuota(root);
        }
        catch (NodeNotFoundException e)
        {
            throw new IllegalArgumentException("No such node.", e);
        }
        catch (FileNotFoundException e)
        {
            throw new IllegalArgumentException("No such node.", e);
        }
        catch (URISyntaxException e)
        {
            throw new IllegalArgumentException(
                    "Unable to create URI for given AD File.", e);
        }

        log.debug("Bytes remaining in user's quota: " + quota);
        if (quota <= 0)
        {
            throw new ByteLimitExceededException(quota + " bytes remain in VOSpace quota.", quota);
        }

        // Note: the current vospace spec doesn't allow for the collection content-length
        // information, so this will always be null until that is added.
        if ((contentLength != null) && (quota < contentLength))
        {
            throw new ByteLimitExceededException(quota + " bytes remain in VOSpace quota.", quota);
        }
    }

    private long getRemainingQuota(Node root)
            throws URISyntaxException, NodeNotFoundException, FileNotFoundException, TransientException
    {

        // get the extra properties for the root node
        nodePersistence.getProperties(root);

        long quota = -1;
        long used = -1;
        for (NodeProperty property : root.getProperties())
        {
            if (property.getPropertyURI().equals(VOS.PROPERTY_URI_QUOTA))
            {
                quota = Long.parseLong(property.getPropertyValue());
            }
            if (property.getPropertyURI().equals(VOS.PROPERTY_URI_CONTENTLENGTH))
            {
                used = Long.parseLong(property.getPropertyValue());
            }
        }

        log.debug("Quota property: " + quota);
        log.debug("Used space: " + used);

        // If the quota property is not set, return an unlimited quota
        if (quota == -1)
            return Long.MAX_VALUE;

        // vospace never used before?
        if (used == -1)
        {
            return quota;
        }

        if (used >= quota)
        {
            return 0;
        }

        return (quota - used);
    }

    public abstract void doAction()
        throws TransferException, JobPersistenceException, JobNotFoundException,
        LinkingException, NodeNotFoundException, TransferParsingException,
        IOException, TransientException, URISyntaxException, NodeBusyException;

}
