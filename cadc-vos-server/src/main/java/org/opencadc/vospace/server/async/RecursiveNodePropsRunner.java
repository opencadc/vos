/*
************************************************************************
*******************  CANADIAN ASTRONOMY DATA CENTRE  *******************
**************  CENTRE CANADIEN DE DONNÉES ASTRONOMIQUES  **************
*
*  (c) 2023.                            (c) 2023.
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
*  $Revision: 1 $
*
************************************************************************
*/

package org.opencadc.vospace.server.async;

import ca.nrc.cadc.auth.AuthenticationUtil;
import ca.nrc.cadc.io.ResourceIterator;
import ca.nrc.cadc.rest.SyncOutput;
import ca.nrc.cadc.util.ThrowableUtil;
import ca.nrc.cadc.uws.ErrorSummary;
import ca.nrc.cadc.uws.ErrorType;
import ca.nrc.cadc.uws.ExecutionPhase;
import ca.nrc.cadc.uws.Job;
import ca.nrc.cadc.uws.JobInfo;
import ca.nrc.cadc.uws.Result;
import ca.nrc.cadc.uws.server.JobRunner;
import ca.nrc.cadc.uws.server.JobUpdater;
import ca.nrc.cadc.uws.util.JobLogInfo;
import java.io.FileNotFoundException;
import java.net.URI;
import java.security.AccessControlException;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import javax.naming.Context;
import javax.naming.InitialContext;
import javax.security.auth.Subject;
import org.apache.log4j.Logger;
import org.opencadc.vospace.ContainerNode;
import org.opencadc.vospace.Node;
import org.opencadc.vospace.NodeNotSupportedException;
import org.opencadc.vospace.VOSURI;
import org.opencadc.vospace.io.NodeParsingException;
import org.opencadc.vospace.io.NodeReader;
import org.opencadc.vospace.server.NodePersistence;
import org.opencadc.vospace.server.PathResolver;
import org.opencadc.vospace.server.Utils;
import org.opencadc.vospace.server.actions.UpdateNodeAction;
import org.opencadc.vospace.server.auth.VOSpaceAuthorizer;

/**
 * Class to set node properties on the target node and all child
 * nodes recursively.
 *
 * @author adriand
 *
 */
public class RecursiveNodePropsRunner extends AbstractRecursiveRunner {
    private static Logger log = Logger.getLogger(RecursiveNodePropsRunner.class);

    public RecursiveNodePropsRunner() {
        
    }
    
    @Override
    public void setAppName(String appName) {
        super.setAppName(appName);
        authorizer.setDisregardLocks(true); //TODO locks don't apply to property updates
    }

    @Override
    public void setJob(Job job) {
        super.setJob(job);
        this.job = job;

        JobInfo jobInfo = job.getJobInfo();

        if (jobInfo != null && jobInfo.getContent() != null && !jobInfo.getContent().isEmpty()
                && jobInfo.getContentType().equalsIgnoreCase("text/xml")) {
            log.debug("recursive set node XML: \n\n" + jobInfo.getContent());
            NodeReader reader = new NodeReader();
            NodeReader.NodeReaderResult res = null;
            try {
                res = reader.read(jobInfo.getContent());
            } catch (Exception e) {
                this.sendError("Error parsing input properties");
            }
            this.clientNode = res.node;
            this.target = res.vosURI;
        }
        log.debug("node: " + clientNode + " target: " + target);
        if (clientNode == null) {
            this.sendError("NotFound");
        }
    }

    /**
     * Traversing depth first, update the node properties recursively
     * @throws Exception
     */
    @Override
    protected boolean performAction(Node node) throws Exception {
        Subject subject = AuthenticationUtil.getCurrentSubject(); // TODO create this once?

        if (Utils.getAdminProps(clientNode, nodePersistence.getAdminProps(), subject, nodePersistence).size() > 0) {
            this.sendError("Cannot recursively change admin props");
            return false;
        }

        if (Thread.currentThread().isInterrupted()) {
            throw new InterruptedException();
        }

        // check the write permission
        if (!authorizer.hasSingleNodeWritePermission(node, subject)) {
            // don't apply the update for this node
            log.debug("User " + subject.toString() + "not allowed to update node " + Utils.getPath(node));
            incErrorCount();
            return false;
        }

        // check the job phase for abort or illegal state
        checkJobPhase();
        boolean success = true;
        // apply updates to the children recursively
        if (node instanceof ContainerNode) {
            ContainerNode container = (ContainerNode) node;

            // collect child containers for later
            List<ContainerNode> childContainers = new ArrayList<>();
            
            try (ResourceIterator<Node> iter = nodePersistence.iterator(container, null, null)) {
                while (iter.hasNext()) {
                    Node n = iter.next();
                    if (n instanceof ContainerNode) {
                        ContainerNode cn = (ContainerNode) n;
                        childContainers.add(cn);
                    } else {
                        success &= performAction(n);
                    }
                }
            }
            // now recurse so we only have one open iterator at a time
            for (ContainerNode cn : childContainers) {
                success &= performAction(cn);
            }
        }

        // apply the node property changes
        try {
            log.debug("Applying node properties to: " + node);
            UpdateNodeAction.updateProperties(node, clientNode, nodePersistence, subject);
            incSuccessCount();
            log.debug("Properties updated for node " + Utils.getPath(node));
        } catch (Exception e) {
            // unexepected error, abort updates for this node
            log.warn("Failed to update properties on node: " + node, e);
            incErrorCount();
            success = false;
        }
        return success;
    }
}
