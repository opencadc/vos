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

package org.opencadc.vospace.server.actions;

import ca.nrc.cadc.auth.AuthenticationUtil;
import ca.nrc.cadc.auth.IdentityManager;
import ca.nrc.cadc.net.HttpTransfer;
import java.util.List;
import javax.security.auth.Subject;
import org.apache.log4j.Logger;
import org.opencadc.vospace.ContainerNode;
import org.opencadc.vospace.Node;
import org.opencadc.vospace.NodeNotSupportedException;
import org.opencadc.vospace.NodeProperty;
import org.opencadc.vospace.VOS;
import org.opencadc.vospace.VOSURI;
import org.opencadc.vospace.io.NodeWriter;
import org.opencadc.vospace.server.NodeFault;
import org.opencadc.vospace.server.NodePersistence;
import org.opencadc.vospace.server.PathResolver;
import org.opencadc.vospace.server.Utils;

/**
 * Class to perform the update of a Node.
 *
 * @author adriand
 */
public class UpdateNodeAction extends NodeAction {

    protected static Logger log = Logger.getLogger(UpdateNodeAction.class);

    @Override
    public void doAction() throws Exception {
        final Node clientNode = getInputNode();
        if (clientNode == null) {
            throw NodeFault.InvalidArgument.getStatus("no node document");
        }
        final VOSURI clientNodeTarget = getInputURI();
        final VOSURI target = getTargetURI();
        
        // validate doc vs path because some redundancy in API
        if (!clientNodeTarget.equals(target)) {
            throw NodeFault.InvalidArgument.getStatus(
                    "invalid input: vos URI mismatch: doc=" + clientNodeTarget + " and path=" + target);
        }

        voSpaceAuthorizer.setDisregardLocks(true); // locks don't apply to properties updates
        PathResolver pathResolver = new PathResolver(nodePersistence, voSpaceAuthorizer, true);
        Node serverNode = pathResolver.getNode(target.getPath());
        if (serverNode == null) {
            throw NodeFault.NodeNotFound.getStatus("Target " + clientNodeTarget.toString());
        }

        if (!clientNode.getClass().equals(serverNode.getClass())) {
            // TODO: suitable NodeFault?
            throw NodeFault.InvalidArgument.getStatus("invalid input: cannot change type of node " + target.getPath()
                    + " from " + serverNode.getClass().getSimpleName() + " to " + clientNode.getClass().getSimpleName());
        }

        Subject caller = AuthenticationUtil.getCurrentSubject();
        if (!voSpaceAuthorizer.hasSingleNodeWritePermission(serverNode, caller)) {
            throw NodeFault.PermissionDenied.getStatus(Utils.getPath(serverNode));
        }

        // TODO: attempt to set owner if admin
        Node storedNode = updateProperties(serverNode, clientNode, nodePersistence, caller);
        IdentityManager im = AuthenticationUtil.getIdentityManager();
        storedNode.ownerDisplay = im.toDisplayString(storedNode.owner);
        
        // output modified node
        NodeWriter nodeWriter = getNodeWriter();
        syncOutput.setCode(200); // ??
        syncOutput.setHeader(HttpTransfer.CONTENT_TYPE, getMediaType());
        // TODO: should the VOSURI in the output target or actual? eg resolveLinks=true
        nodeWriter.write(localServiceURI.getURI(storedNode), storedNode, syncOutput.getOutputStream(), VOS.Detail.max);
    }

    public static Node updateProperties(Node serverNode, Node clientNode, NodePersistence nodePersistence, Subject caller)
            throws NodeNotSupportedException {
        // merge change request
        if (clientNode.clearReadOnlyGroups || !clientNode.getReadOnlyGroup().isEmpty()) {
            serverNode.getReadOnlyGroup().clear();
            serverNode.getReadOnlyGroup().addAll(clientNode.getReadOnlyGroup());
        } //else: no update

        if (clientNode.clearReadWriteGroups || !clientNode.getReadWriteGroup().isEmpty()) {
            serverNode.getReadWriteGroup().clear();
            serverNode.getReadWriteGroup().addAll(clientNode.getReadWriteGroup());
        } //else: no update

        if (clientNode.clearIsPublic) {
            serverNode.isPublic = null;
        } else if (clientNode.isPublic != null) {
            serverNode.isPublic = clientNode.isPublic;
        } //else: no update

        if (clientNode.clearIsLocked) {
            serverNode.isLocked = null;
        } else if (clientNode.isLocked != null) {
            serverNode.isLocked = clientNode.isLocked;
        } //else: no update

        if (serverNode instanceof ContainerNode) {
            ContainerNode scn = (ContainerNode) serverNode;
            ContainerNode ccn = (ContainerNode) clientNode;
            if (ccn.inheritPermissions != null) {
                scn.inheritPermissions = ccn.inheritPermissions;
            }
        }
        
        // pick out eligible admin-only props (they are immutable to normal users)
        List<NodeProperty> allowedAdminProps = Utils.getAdminProps(clientNode, nodePersistence.getAdminProps(),
                caller, nodePersistence);
        // filter and merge request into serverNode
        Utils.updateNodeProperties(serverNode.getProperties(), clientNode.getProperties(),
                nodePersistence.getImmutableProps());
        serverNode.getProperties().addAll(allowedAdminProps);
        return nodePersistence.put(serverNode);
    }
}