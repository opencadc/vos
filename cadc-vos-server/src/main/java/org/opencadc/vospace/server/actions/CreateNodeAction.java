/*
 ************************************************************************
 *******************  CANADIAN ASTRONOMY DATA CENTRE  *******************
 **************  CENTRE CANADIEN DE DONNÉES ASTRONOMIQUES  **************
 *
 *  (c) 2026.                            (c) 2026.
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
import ca.nrc.cadc.auth.HttpPrincipal;
import ca.nrc.cadc.auth.IdentityManager;
import ca.nrc.cadc.net.HttpConstants;
import ca.nrc.cadc.net.HttpTransfer;
import java.io.IOException;
import java.security.AccessControlException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import javax.security.auth.Subject;
import org.apache.log4j.Logger;
import org.opencadc.vospace.ContainerNode;
import org.opencadc.vospace.Node;
import org.opencadc.vospace.NodeProperty;
import org.opencadc.vospace.VOS;
import org.opencadc.vospace.VOSURI;
import org.opencadc.vospace.io.NodeWriter;
import org.opencadc.vospace.server.NodeFault;
import org.opencadc.vospace.server.PathResolver;
import org.opencadc.vospace.server.Utils;

/**
 * Class to perform the creation of a Node action.
 *
 * @author majorb
 * @author adriand
 */
public class CreateNodeAction extends NodeAction {

    protected static Logger log = Logger.getLogger(CreateNodeAction.class);

    @Override
    public void doAction() throws Exception {
        final Node clientNode = getInputNode();
        if (clientNode == null) {
            throw NodeFault.InvalidArgument.getStatus("no node document");
        }
        final VOSURI clientNodeURI = getInputURI();
        final VOSURI target = getTargetURI();
        
        // validate doc vos path because some redundancy in API
        if (!target.equals(clientNodeURI)) {
            throw NodeFault.InvalidURI.getStatus("invalid input: vos URI mismatch: doc=" + clientNodeURI + " and path=" + target);
        }
        
        // get parent container node
        // TBD: resolveLinks=true?
        PathResolver pathResolver = new PathResolver(nodePersistence, voSpaceAuthorizer);
        Node serverNode = pathResolver.getNode(target.getParentURI().getPath(), true);
        if (!(serverNode instanceof ContainerNode)) {
            throw NodeFault.ContainerNotFound.getStatus(clientNodeURI.toString());
        }

        ContainerNode parent = (ContainerNode) serverNode;
        Node cur = nodePersistence.get(parent, target.getName());
        if (cur != null) {
            throw NodeFault.DuplicateNode.getStatus(clientNodeURI.toString());
        }
        // does not exist, parent resolved
        clientNode.parent = parent;
        
        // there are 3 kinds of create node:
        // 1. admin-allocate: admin creates a node owned by someone else, optionally assigns admin props
        // 2. self-allocate: a user creates there own allocation
        // 3. normal/common: a user creates a node
        final IdentityManager im = AuthenticationUtil.getIdentityManager();
        Subject caller = AuthenticationUtil.getCurrentSubject();
        final List<NodeProperty> allowedAdminProps;
        Subject writePerm = caller;
        if (Utils.isAdmin(caller, nodePersistence)) {
            if (clientNode.ownerDisplay != null) {
                // admin assigns owner
                clientNode.owner = toOwnerSubject(clientNode.ownerDisplay, im);
                if (clientNode.owner == null) {
                    throw new RuntimeException("admin-allocate: failed to convert '" + clientNode.ownerDisplay + "' "
                        + " to a Subject");
                }
            } else {
                // admin is owner
                clientNode.owner = caller;
            }
            allowedAdminProps = Utils.getAdminProps(clientNode, nodePersistence.getAdminProps(), caller, nodePersistence);
            logInfo.setMessage("admin-allocate");
        } else if (isSelfAllocate(caller, clientNode)) {
            checkSelfAllocatePermission(caller, clientNodeURI);
            clientNode.owner = caller;
            allowedAdminProps = new ArrayList<>();
            allowedAdminProps.add(new NodeProperty(VOS.PROPERTY_URI_CREATOR));
            writePerm = nodePersistence.getRootNode().owner; // elevate write permission check below
            logInfo.setMessage("self-allocate");
        } else {
            // normal create: caller is owner
            clientNode.owner = caller;
            allowedAdminProps = new ArrayList<>();
        }

        if (!voSpaceAuthorizer.hasSingleNodeWritePermission(parent, writePerm)) {
            throw NodeFault.PermissionDenied.getStatus(clientNodeURI.toString());
        }
        // protected against accidentally using this again
        writePerm = null;
        
        // backwards compat: ignore if clients still send this, just in case
        NodeProperty creatorJWT = clientNode.getProperty(VOS.PROPERTY_URI_CREATOR_JWT);
        if (creatorJWT != null) {
            clientNode.getProperties().remove(creatorJWT);
        }

        // inherit
        if (parent.inheritPermissions != null && parent.inheritPermissions) {
            // explicit clientNode settings override inherit
            if (clientNode.isPublic == null) {
                clientNode.isPublic = parent.isPublic;
            }
            if (clientNode.getReadOnlyGroup().isEmpty()) {
                clientNode.getReadOnlyGroup().addAll(parent.getReadOnlyGroup());
            }
            if (clientNode.getReadWriteGroup().isEmpty()) {
                clientNode.getReadWriteGroup().addAll(parent.getReadWriteGroup());
            }
            if (clientNode instanceof ContainerNode) {
                ContainerNode cn = (ContainerNode) clientNode;
                if (cn.inheritPermissions == null) {
                    cn.inheritPermissions = parent.inheritPermissions;
                }
            }
        } else {
            // null is equivalent to false, but always set/persist this??
            if (clientNode.isPublic == null) {
                clientNode.isPublic = false;
            }
            if (clientNode instanceof ContainerNode) {
                ContainerNode cn = (ContainerNode) clientNode;
                if (cn.inheritPermissions == null) {
                    cn.inheritPermissions = false;
                }
            }
        }
        
        // sanitize input properties into clean set
        Set<NodeProperty> np = new HashSet<>();
        Utils.updateNodeProperties(np, clientNode.getProperties(), nodePersistence.getImmutableProps());
        clientNode.getProperties().clear();
        clientNode.getProperties().addAll(np);
        clientNode.getProperties().addAll(allowedAdminProps);

        log.debug("Putting node " + target.getName() + " in path " + target.getPath());
        Node storedNode = nodePersistence.put(clientNode);
        storedNode.ownerDisplay = im.toDisplayString(storedNode.owner);
        
        // output modified node
        NodeWriter nodeWriter = getNodeWriter();
        syncOutput.setCode(201);
        syncOutput.setHeader(HttpConstants.HDR_CONTENT_TYPE, getMediaType());
        // TODO: should the VOSURI in the output target or actual? eg resolveLinks=true
        nodeWriter.write(localServiceURI.getURI(storedNode), storedNode, syncOutput.getOutputStream(), VOS.Detail.max);
    }

    /**
     * Check if the caller is allowed to create a their own allocation.The uri is
 supplied in case the permission checks need the VOSpace resourceID or the path. The default behaviour of this method is to throw an AccessControlException.
     * 
     * @param caller the user trying to create an allocation
     * @param uri the vos URI of the allocation
     * @throws AccessControlException if the caller is not allowed
     * @throws Exception if some error prevents checking for authorisation
     */
    protected void checkSelfAllocatePermission(Subject caller, VOSURI uri) 
            throws AccessControlException, Exception {
        throw new AccessControlException("permission denied: self-allocate is not enabled");
    }

    // self-allocate: a user tries to create a ContainerNode allocation for themselves
    private boolean isSelfAllocate(Subject caller, Node node) {
        return node instanceof ContainerNode 
                && nodePersistence.isAllocation((ContainerNode) node);
    }
    
    private Subject toOwnerSubject(String ownerStringRep, IdentityManager im) {
        // try IdentityManager.soSubject(Object)
        // this could work for different principal type X IdentityManager combinations
        try {
            Subject ret = im.toSubject(ownerStringRep);
            log.debug("create owner subject by im.toSubject: " + ret);
            return ret;
        } catch (Exception ex) {
            log.debug("IdentityManager.toSubject() failed for " + ownerStringRep + ": " +  ex);
        }

        // HACK: known IdentityManager implementations prefer the HttpPrincipal aka
        // network username for toDisplayString(Subject) so that's the value that
        // normally gets put into node.ownerDisplay and rendered in node documents
        // so choosing HttpPrincipal here makes output and input (set owner) use the
        // same values... but seems kind of sketchy.
        try {
            Subject tmp = new Subject();
            tmp.getPrincipals().add(new HttpPrincipal(ownerStringRep));
            Subject ret = im.augment(tmp);
            log.debug("create owner subject by im.augment(HttpPrincipal): " + ret);
            return ret;
        } catch (Exception ex) {
            log.debug("IdentityManager.augment() failed for " + ownerStringRep + ": " + ex);
        }
        
        // neither of those worked...
        return null;
    }
}
