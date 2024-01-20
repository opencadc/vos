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
 *  $Revision: 4 $
 *
 ************************************************************************
 */

package org.opencadc.vospace.server;

import ca.nrc.cadc.auth.AuthenticationUtil;
import ca.nrc.cadc.util.StringUtil;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import javax.security.auth.Subject;
import org.apache.log4j.Logger;
import org.opencadc.vospace.ContainerNode;
import org.opencadc.vospace.LinkNode;
import org.opencadc.vospace.Node;
import org.opencadc.vospace.VOSURI;
import org.opencadc.vospace.server.auth.VOSpaceAuthorizer;

/**
 * Utility class to follow and resolve the target of link nodes in a local vospace including checking
 * read permission along the path.
 *
 * <p>If the last node on the path is a link node, it will not be resolved.
 *
 * @author adriand
 * @author majorb
 */
public class PathResolver {

    protected static final Logger log = Logger.getLogger(PathResolver.class);
    // enforce a maximum visit limit to prevent stack overflows on
    // the recursive method
    private static final int VISIT_LIMIT_MAX = 40;
    private final NodePersistence nodePersistence;
    private final VOSpaceAuthorizer voSpaceAuthorizer;
    private final boolean resolveLeafLink;

    List<String> visitedPaths = new ArrayList<>();
    private int visitLimit = 20;
    int visitCount = 0;

    /**
     * Ctor
     * @param nodePersistence - node persistence to use
     * @param voSpaceAuthorizer - vo authorizer to check permissions along the path
     * @param resolveLeafLink - If true and the leaf node is a LinkNode it resolves it, otherwise it returns the
     *                        LinkNode after potentially resolving other links in the path.
     */
    public PathResolver(NodePersistence nodePersistence, VOSpaceAuthorizer voSpaceAuthorizer, boolean resolveLeafLink) {
        this.nodePersistence = nodePersistence;
        this.voSpaceAuthorizer = voSpaceAuthorizer;
        this.resolveLeafLink = resolveLeafLink;
    }

    /**
     * Resolves a node URI and returns a Resolved object that contains either the resolved node if the node exists
     * or the resolved parent and the name of the node if the node is to be created by the caller.
     * @param nodeURI URI of the node to be resolved
     * @return Resolved object containing either the child node if it exists already or the parent node and the name
     *         of the child node to be created otherwise
     * @throws Exception
     */
    public Resolved resolveNode(VOSURI nodeURI) throws Exception {
        log.debug("resolve node: [" + nodeURI + "]");

        ContainerNode parent;
        try {
            parent = (ContainerNode) getNode(nodeURI.getParent());
        } catch (ClassCastException ex) {
            throw new IllegalArgumentException(nodeURI.getParent() + " not a valid path");
        }
        Node node = nodePersistence.get(parent, nodeURI.getName());
        Resolved result = new Resolved();
        if ((parent == null) && (node != null)) {
            // node in root directory
            parent = nodePersistence.getRootNode();
        } else {
            result.childName = nodeURI.getName(); // default name
        }
        while (node instanceof LinkNode) {
            // resolve it
            validateTargetURI((LinkNode) node);
            VOSURI targetURI = new VOSURI(((LinkNode)node).getTarget());
            log.debug("Target uri " + targetURI);
            try {
                parent = (ContainerNode) getNode(targetURI.getParent());
                log.debug("Parent " + targetURI.getParent() + " is resolved to " + parent);
            } catch (ClassCastException ex) {
                throw new IllegalArgumentException(nodeURI.getParent() + " in link node not a valid path");
            }
            node = nodePersistence.get(parent, targetURI.getName());
            result.childName = targetURI.getName();
        }
        if (node == null) {
            // new DataNode that needs to be created later.
            result.parent = parent;
        } else {
            // existing DataNode/ContainerNode
            result.childName = null;
            result.child = node;
        }
        return result;
    }

    /**
     * Resolves a node URI, follow links, and returns the end node.
     *
     * @param nodePath
     * @return the last node in the path or null if not found
     * @throws org.opencadc.vospace.LinkingException
     */
    public Node getNode(String nodePath) throws Exception {
        final Subject subject = AuthenticationUtil.getCurrentSubject();
        log.debug("get: [" + nodePath + "]");
        ContainerNode node = nodePersistence.getRootNode();
        voSpaceAuthorizer.hasSingleNodeReadPermission(node, subject);
        
        Node ret = node;
        if (StringUtil.hasLength(nodePath)) {
            if (nodePath.charAt(0) == '/') {
                nodePath = nodePath.substring(1);
            }
            Iterator<String> pathIter = Arrays.stream(nodePath.split("/")).iterator();
            while (pathIter.hasNext()) {
                String name = pathIter.next();
                log.debug("get node: '" + name + "' in path '" + nodePath + "'");
                Node child = nodePersistence.get(node, name);
                if (child == null) {
                    log.debug("---------------!!!!!!!!!!!!!!!");
                    return null;
                }
                if (!voSpaceAuthorizer.hasSingleNodeReadPermission(child, subject)) {
                    LocalServiceURI lsURI = new LocalServiceURI(nodePersistence.getResourceID());
                    throw NodeFault.PermissionDenied.getStatus(lsURI.getURI(child).toString());
                }

                while (child instanceof LinkNode) {
                    if (!resolveLeafLink && !pathIter.hasNext()) {
                        log.debug("Returning link node " + Utils.getPath(child));
                        break;
                    }
                    log.debug("Resolving link node " + Utils.getPath(child));
                    if (visitCount > visitLimit) {
                        throw NodeFault.UnreadableLinkTarget.getStatus("Exceeded link limit.");
                    }
                    visitCount++;
                    log.debug("visit number " + visitCount);

                    LinkNode linkNode = (LinkNode) child;
                    VOSURI targetURI = validateTargetURI(linkNode);

                    String linkPath = targetURI.getPath();
                    if (visitedPaths.contains(linkPath)) {
                        throw NodeFault.UnreadableLinkTarget.getStatus(
                                "detected link node cycle: already followed link -> " + linkPath);
                    }
                    visitedPaths.add(linkPath);

                    // recursive follow
                    log.debug("Resolve: " + linkPath);
                    child = getNode(linkPath);
                }
                if (pathIter.hasNext()) {
                    if (child instanceof ContainerNode) {
                        node = (ContainerNode) child;
                    } else {
                        return null;
                    }
                }
                ret = child;
            }
        }

        log.debug("return node: " + ((ret != null) ? Utils.getPath(ret) : null));
        return ret;
    }


    /**
     * Return a new VOSURI representing the target URI of the link node.
     *
     * @param linkNode node to validate
     * @return A VOSURI of the target of the link node.
     */
    public VOSURI validateTargetURI(LinkNode linkNode) throws Exception {
        LocalServiceURI localServiceURI = new LocalServiceURI(nodePersistence.getResourceID());
        VOSURI targetURI = new VOSURI(linkNode.getTarget());

        log.debug("Validating target: " + targetURI.getServiceURI() + " vs " + localServiceURI.getVOSBase().getServiceURI());
        if (!localServiceURI.getVOSBase().getServiceURI().equals(targetURI.getServiceURI())) {
            throw NodeFault.InvalidArgument.getStatus("External link " + targetURI.getServiceURI().toASCIIString());
        }
        return targetURI;
    }

    /**
     * Set the limit for number of link reference resolutions.
     * Default The value is 20.
     *
     * @param visitLimit link reference resolutions
     */
    public void setVisitLimit(int visitLimit) {
        if (visitLimit > VISIT_LIMIT_MAX) {
            throw new IllegalArgumentException(
                    "Too high a visit limit.  Must be below " + VISIT_LIMIT_MAX);
        }
        this.visitLimit = visitLimit;
    }

    public static class Resolved {
        public ContainerNode parent;
        public String childName;
        public Node child; // possibly null

        public String toString() {
            return "parent " + parent + ", child " + child + ", childName " + childName;
        }
    }



}
