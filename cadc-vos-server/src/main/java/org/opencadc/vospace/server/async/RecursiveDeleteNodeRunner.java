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

package org.opencadc.vospace.server.async;

import ca.nrc.cadc.auth.AuthenticationUtil;
import ca.nrc.cadc.io.ResourceIterator;
import ca.nrc.cadc.net.ResourceNotFoundException;
import ca.nrc.cadc.rest.SyncOutput;
import ca.nrc.cadc.util.ThrowableUtil;
import ca.nrc.cadc.uws.ErrorSummary;
import ca.nrc.cadc.uws.ErrorType;
import ca.nrc.cadc.uws.ExecutionPhase;
import ca.nrc.cadc.uws.Job;
import ca.nrc.cadc.uws.Parameter;
import ca.nrc.cadc.uws.Result;
import ca.nrc.cadc.uws.server.JobRunner;
import ca.nrc.cadc.uws.server.JobUpdater;
import ca.nrc.cadc.uws.util.JobLogInfo;
import java.net.URI;
import java.security.AccessControlException;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import javax.naming.Context;
import javax.naming.InitialContext;
import javax.naming.NamingException;
import javax.security.auth.Subject;
import org.apache.log4j.Logger;
import org.opencadc.vospace.ContainerNode;
import org.opencadc.vospace.Node;
import org.opencadc.vospace.VOSURI;
import org.opencadc.vospace.server.NodeFault;
import org.opencadc.vospace.server.NodePersistence;
import org.opencadc.vospace.server.PathResolver;
import org.opencadc.vospace.server.Utils;
import org.opencadc.vospace.server.actions.DeleteNodeAction;
import org.opencadc.vospace.server.auth.VOSpaceAuthorizer;

/**
 * Class to delete the target node and all child nodes recursively.
 *
 * @author adriand
 */
public class RecursiveDeleteNodeRunner extends AbstractRecursiveRunner {

    private static Logger log = Logger.getLogger(RecursiveDeleteNodeRunner.class);

    @Override
    public void setJob(Job job) {
        this.job = job;
        for (Parameter param : job.getParameterList()) {
            if (param.getName().equalsIgnoreCase("target")) {
                this.target = new VOSURI(URI.create(param.getValue()));
                break;
            }
        }

        if (target == null) {
            throw new IllegalArgumentException("target argument required");
        }

        log.debug("target: " + target);
    }

    @Override
    protected boolean performAction(Node node) throws Exception {
        if (Thread.currentThread().isInterrupted()) {
            throw new InterruptedException();
        }

        List<ContainerNode> childContainers = new ArrayList<>();
        Subject subject = AuthenticationUtil.getCurrentSubject(); // TODO create this once?
        log.debug("Deleting node " + Utils.getPath(node));
        // check the job phase for abort or illegal state
        checkJobPhase();
        boolean success = true;
        if (node instanceof ContainerNode) {
            try (ResourceIterator<Node> iterator =
                         nodePersistence.iterator((ContainerNode) node, null, null)) {
                while (iterator.hasNext()) {
                    Node child = iterator.next();
                    if (child instanceof ContainerNode) {
                        childContainers.add((ContainerNode) child);
                    } else {
                        try {
                            DeleteNodeAction.delete(child, authorizer, nodePersistence);
                            log.debug("Deleted non-container node " + Utils.getPath(child));
                            incSuccessCount();
                        } catch (Exception ex) {
                            log.debug("Cannot delete node " + Utils.getPath(child), ex);
                            success = false;
                            incErrorCount();
                        }
                    }
                }
            }
            if (!childContainers.isEmpty()) {
                // descend down one level
                for (ContainerNode cn : childContainers) {
                    success &= performAction(cn);
                }
            }
        }
        if (success) {
            // delete the empty container
            try {
                DeleteNodeAction.delete(node, authorizer, nodePersistence);
                log.debug("Deleted container node " + Utils.getPath(node));
                incSuccessCount();
            } catch (Exception ex) {
                log.debug("Cannot delete node " + Utils.getPath(node), ex);
                success = false;
                incErrorCount();
            }
        }
        return success;
    }
}
