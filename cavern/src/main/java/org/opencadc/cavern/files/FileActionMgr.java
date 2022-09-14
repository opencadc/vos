/*
************************************************************************
*******************  CANADIAN ASTRONOMY DATA CENTRE  *******************
**************  CENTRE CANADIEN DE DONNÉES ASTRONOMIQUES  **************
*
*  (c) 2022.                            (c) 2022.
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

package org.opencadc.cavern.files;


import ca.nrc.cadc.util.StringUtil;
import ca.nrc.cadc.vos.ContainerNode;
import ca.nrc.cadc.vos.DataNode;
import ca.nrc.cadc.vos.Direction;
import ca.nrc.cadc.vos.LinkingException;
import ca.nrc.cadc.vos.Node;
import ca.nrc.cadc.vos.NodeLockedException;
import ca.nrc.cadc.vos.NodeNotFoundException;
import ca.nrc.cadc.vos.NodeNotSupportedException;
import ca.nrc.cadc.vos.VOSURI;
import ca.nrc.cadc.vos.server.PathResolver;
import ca.nrc.cadc.vos.server.auth.VOSpaceAuthorizer;
import java.io.IOException;
import java.net.URISyntaxException;
import java.security.AccessControlException;
import java.util.Map;
import org.apache.log4j.Logger;
import org.opencadc.cavern.FileSystemNodePersistence;


/**
 * @author jeevesh
 */
public class FileActionMgr {
    private final Logger log = Logger.getLogger(FileActionMgr.class);

    private VOSpaceAuthorizer authorizer;
    private PathResolver pathResolver;
    private FileSystemNodePersistence fsPersistence;

    public FileActionMgr() {}

    public void initTools() {
        // Set up authorizer and path resolver
        this.fsPersistence = new FileSystemNodePersistence();
        this.pathResolver = new PathResolver(fsPersistence, true);
        this.authorizer = new VOSpaceAuthorizer(true);
        this.authorizer.setNodePersistence(fsPersistence);
    }

    public VOSURI initPreauthTarget(String path, Map<String, String> initParams, Direction direction)
        throws AccessControlException, IOException, URISyntaxException {
        VOSURI nodeURI = null;

        // Long marker as cavern debug is rather verbose
        log.debug("---------------- initPreauthTarget debug log ----------------------");
        log.debug("path passed in: " + path);

        if (!StringUtil.hasLength(path)) {
            throw new IllegalArgumentException("Invalid preauthorized request");
        }
        String[] parts = path.split("/");
        log.debug(" number of parts in path: " + parts.length);
        if (parts.length < 2) {
            throw new IllegalArgumentException("Invalid preauthorized request");
        }

        String token = parts[0];
        log.debug("token: " + token);

        CavernURLGenerator urlGen = new CavernURLGenerator();
        VOSURI targetVOSURI = urlGen.getURIFromPath(path);

        // preauth token is validated in this step. Exceptions are thrown
        // if it's not valid
        nodeURI = urlGen.getNodeURI(token, targetVOSURI, direction);
        log.debug("Init node uri: " + nodeURI);

        return nodeURI;
    }

    public Node resolveWithReadPermission(VOSURI targetVOSURI, Map<String, String> initParams, Direction direction)
        throws AccessControlException, NodeNotFoundException, LinkingException, URISyntaxException {

        log.debug("[resolveWithReadPermission]: checking read permission for targetVOSURI: " + targetVOSURI.toString());

        // Authorization is done in this step
        Node node = pathResolver.resolveWithReadPermissionCheck(targetVOSURI, authorizer, true);
        log.debug("node resolved with read permission: " + targetVOSURI.toString());

        return node;
    }

    public Node resolveWithWritePermission(VOSURI targetVOSURI, Map<String, String> initParams, Direction direction)
        throws AccessControlException, NodeNotFoundException, LinkingException, URISyntaxException {

        Node resolvedNode = null;

        try {
            // Test to see if the node exists already or not
            log.debug("[resolveWithWritePermission]: checking read permission for targetVOSURI: " + targetVOSURI.toString());
            resolvedNode = resolveWithReadPermission(targetVOSURI, initParams, direction);

            try {
                // Node exists, check write permissions & whether parent is locked
                resolvedNode = (Node) authorizer.getWritePermission(resolvedNode);

                // check to ensure the parent node isn't locked
                if (resolvedNode.getParent() != null && resolvedNode.getParent().isLocked()) {
                    throw new NodeLockedException(resolvedNode.getParent().getUri().toString());
                }

                return resolvedNode;

            } catch (NodeLockedException e) {
                throw e;
            }

        } catch (NodeNotFoundException ex) {
            // this could be a new file. Check parent exists and is writable
            Node pn = resolveWithReadPermission(targetVOSURI.getParentURI(), initParams, direction);

            // parent node exists
            if (!(pn instanceof ContainerNode))
            {
                throw new IllegalArgumentException(
                    "parent is not a ContainerNode: " + pn.getUri().getURI().toASCIIString());
            }
            authorizer.getWritePermission(pn);

            // everything checks out so far, create the new DataNode
            try {
                Node newNode =  new DataNode(new VOSURI(pn.getUri()
                        + "/" + targetVOSURI.getName()));

                newNode.setParent((ContainerNode) pn);
                fsPersistence.put(newNode);
                return newNode;
            } catch (NodeNotSupportedException e2) {
                throw new IllegalArgumentException(
                    "node type not supported.", e2);
            }

        }
    }


    public VOSURI getVOSURIForPath(String path) throws URISyntaxException {
        log.debug("path passed in: " + path);

        // Without a leading /, CavernURLGenerator.getURIFromPath will strip off
        // the first element in the path. If the path has been pulled from SyncInput,
        // it comes as relative, not absolute, so this step is needed.
        if (!path.startsWith("/")) {
            path = "/" + path;
            log.debug("added leading '/' to path: " + path);
        }

        CavernURLGenerator urlGen = new CavernURLGenerator();
        VOSURI targetVOSURI = urlGen.getURIFromPath(path);

        return targetVOSURI;

    }

    public Node putNode(DataNode node) throws NodeNotSupportedException {
        log.debug("attempting to put node: " + node.toString());
        return fsPersistence.put(node);
    }
}
