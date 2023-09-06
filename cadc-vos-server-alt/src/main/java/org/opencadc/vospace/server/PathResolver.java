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
import ca.nrc.cadc.net.ResourceNotFoundException;
import ca.nrc.cadc.util.StringUtil;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import javax.security.auth.Subject;
import org.apache.log4j.Logger;
import org.opencadc.vospace.ContainerNode;
import org.opencadc.vospace.DataNode;
import org.opencadc.vospace.LinkNode;
import org.opencadc.vospace.LinkingException;
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
    private final boolean resolveLinks;
    
    private List<String> visitedPaths = new ArrayList<>();
    private int visitLimit = 20;
    private int visitCount = 0;


    public PathResolver(NodePersistence nodePersistence, VOSpaceAuthorizer voSpaceAuthorizer, boolean resolveLinks) {
        this.nodePersistence = nodePersistence;
        this.voSpaceAuthorizer = voSpaceAuthorizer;
        this.resolveLinks = resolveLinks;
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
                Node child = nodePersistence.get((ContainerNode) node, name);
                if (child == null) {
                    return null;
                }
                if (!voSpaceAuthorizer.hasSingleNodeReadPermission(child, subject)) {
                    throw NodeFault.PermissionDenied.getStatus();
                }

                if (resolveLinks && pathIter.hasNext()) {
                    while (child instanceof LinkNode) {
                        log.debug("Resolving link node " + Utils.getPath(node));
                        if (visitCount > visitLimit) {
                            throw new LinkingException("Exceeded link limit.");
                        }
                        visitCount++;
                        log.debug("visit number " + visitCount);

                        LinkNode linkNode = (LinkNode) child;
                        VOSURI targetURI = validateTargetURI(linkNode);

                        String linkPath = targetURI.getPath();
                        if (visitedPaths.contains(linkPath)) {
                            throw new LinkingException("detected link node cycle: already followed link -> " + linkPath);
                        }
                        visitedPaths.add(linkPath);
                        
                        // recursive follow
                        child = getNode(targetURI.getPath());
                    }
                }
                if (pathIter.hasNext()) {
                    if (child instanceof ContainerNode) {
                        node = (ContainerNode) child;
                    } else {
                        return null;
                        //throw new IllegalArgumentException("invalid path: found " + child.getClass().getSimpleName()
                        //        + " named '" + name + "' before end of path " + nodePath);
                    }
                }
                ret = child;
            }
        }
        
        log.debug("return node: " + Utils.getPath(ret));
        return ret;
    }


    /**
     * Return a new VOSURI representing the target URI of the link node.
     *
     * @param linkNode node to validate
     * @return A VOSURI of the target of the link node.
     */
    public VOSURI validateTargetURI(LinkNode linkNode) {
        LocalServiceURI localServiceURI = new LocalServiceURI(nodePersistence.getResourceID());
        VOSURI nodeURI = localServiceURI.getURI(linkNode);
        VOSURI targetURI = new VOSURI(linkNode.getTarget());

        log.debug("Validating target: " + targetURI.toString());

        if (targetURI.getServiceURI() != localServiceURI.getVOSBase().getServiceURI()) {
            throw new IllegalArgumentException("External link " + targetURI.getServiceURI().toASCIIString());
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

}
