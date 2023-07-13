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
import ca.nrc.cadc.auth.IdentityManager;
import ca.nrc.cadc.net.ResourceNotFoundException;
import java.security.AccessControlContext;
import java.security.AccessController;
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
 * permissions
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
    private NodePersistence nodePersistence;
    private VOSpaceAuthorizer voSpaceAuthorizer;
    private List<String> visitedPaths;
    private boolean resolveMetadata;
    private int visitLimit = 20;
    private int visitCount = 0;


    public PathResolver(NodePersistence nodePersistence, VOSpaceAuthorizer voSpaceAuthorizer) {
        this.nodePersistence = nodePersistence;
        this.voSpaceAuthorizer = voSpaceAuthorizer;
    }

    /**
     * Constructor.
     *
     * @param nodePersistence node persistence to use
     */
    public PathResolver(NodePersistence nodePersistence) {
        this(nodePersistence, true);
    }

    public PathResolver(NodePersistence nodePersistence, boolean resolveMetadata) {
        if (nodePersistence == null) {
            throw new IllegalArgumentException("null node persistence.");
        }
        this.nodePersistence = nodePersistence;
        this.resolveMetadata = resolveMetadata;
    }


    /**
     * Resolves a node URI following the links and returns the end node
     *
     * @param nodePath
     * @param writable
     * @return
     * @throws ResourceNotFoundException
     */
    public Node getNode(String nodePath, boolean writable) throws ResourceNotFoundException, LinkingException {
        Node node = nodePersistence.getRootNode();
        AccessControlContext acContext = AccessController.getContext();
        Subject subject = Subject.getSubject(acContext);

        voSpaceAuthorizer.checkServiceStatus(writable);

        if (nodePath == null) {
            if (writable) {
                voSpaceAuthorizer.hasSingleNodeWritePermission(node, subject);
            } else {
                voSpaceAuthorizer.hasSingleNodeReadPermission(node, subject);
            }
        } else {
            Iterator<String> pathIter = Arrays.stream(nodePath.split("/")).iterator();
            while (pathIter.hasNext()) {
                String path = pathIter.next();
                log.debug("Loading element " + path + "of path " + nodePath);
                node = nodePersistence.get((ContainerNode) node, path);
                if (node == null) {
                    log.debug("not found " + nodePath + " - '" + path + "' element missing");
                    throw new ResourceNotFoundException("not found " + nodePath);
                }
                IdentityManager im = AuthenticationUtil.getIdentityManager();
                node.owner = im.toSubject(node.ownerID);
                node.ownerDisplay = im.toDisplayString(node.owner);
                if (writable) {
                    voSpaceAuthorizer.hasSingleNodeWritePermission(node, subject);
                } else {
                    voSpaceAuthorizer.hasSingleNodeReadPermission(node, subject);
                }
                LocalServiceURI localServiceURI = new LocalServiceURI(nodePersistence.getResourceID());

                while (node instanceof LinkNode) {
                    log.debug("Resolving link node " + Utils.getPath(node));
                    if (visitCount > visitLimit) {
                        throw new LinkingException("Exceeded link limit.");
                    }
                    visitCount++;
                    log.debug("visit number " + visitCount);

                    LinkNode linkNode = (LinkNode) node;
                    VOSURI targetURI = validateTargetURI(linkNode);

                    // follow the link
                    // TODO need to check this is the same vault
                    node = getNode(targetURI.getPath(), writable);
                }
                if (node instanceof ContainerNode) {
                    continue;
                }
                if (node instanceof DataNode) {
                    if (pathIter.hasNext()) {
                        throw new IllegalArgumentException("Illegal path"); //TODO - different exception
                    }
                }
            }
        }

        //TODO - is this still required?
        // HACK: this causes a query string embedded in the VOSURI (eg within LinkNode)
        // to be tacked onto the DataNode and added to the resulting data URL... TBD.
        //        if (nodeURI.getQuery() != null && node instanceof DataNode)
        //        {
        //            String fragment = null;
        //            if (nodeURI.getFragment() != null) {
        //                //TODO not sure how to pass the query and the fragment around with the node
        //                throw new UnsupportedOperationException("TODO");
        //                fragment = uri.getFragment();

        //            try
        //            {
        //                VOSURI nodeURI = LocalServiceURI.getURI(node);
        //                URI queryUri = new URI(nodeURI.getScheme(),
        //                                       nodeURI.getAuthority(),
        //                                       nodeURI.getPath(),
        //                                       uri.getQuery(),
        //                                       fragment);
        //                Node dataNode = new DataNode(new VOSURI(queryUri), node.properties);
        //                dataNode.accepts.addAll(node.accepts);
        //                dataNode.provides.addAll(node.provides);
        //                dataNode.parent = node.parent;
        //                dataNode.setName(node.getName());
        //                return dataNode;
        //            }
        //            catch (URISyntaxException e)
        //            {
        //                throw new LinkingException("Unable to append query part to " + node.getUri());
        //            }
        //            }
        //        }
        log.debug("returning node: " + Utils.getPath(node));
        return node;
    }


    /**
     * Return a new VOSURI representing the target URI of the link node.
     *
     * @param linkNode node to validate
     * @return A VOSURI of the target of the link node.
     * @throws LinkingException If the target is non vospace, not local, or
     *                          an invalid URI.
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
