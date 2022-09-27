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

import ca.nrc.cadc.rest.InlineContentHandler;
import ca.nrc.cadc.rest.RestAction;
import ca.nrc.cadc.util.PropertiesReader;

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

import ca.nrc.cadc.vos.server.LocalServiceURI;
import ca.nrc.cadc.vos.server.PathResolver;
import ca.nrc.cadc.vos.server.auth.VOSpaceAuthorizer;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.attribute.UserPrincipalLookupService;
import java.security.AccessControlException;

import org.apache.log4j.Logger;
import org.opencadc.cavern.FileSystemNodePersistence;

/**
 *
 * @author majorb
 * @author jeevesh
 */
public abstract class FileAction extends RestAction {
    private static final Logger log = Logger.getLogger(FileAction.class);

    // Key values needed for FileAction
    private VOSURI nodeURI;
    private boolean isPreauth;

    // Supporting tools and values
    private String root;
    private UserPrincipalLookupService upLookupSvc;
    private VOSpaceAuthorizer authorizer;
    protected PathResolver pathResolver;
    private FileSystemNodePersistence fsPersistence;

    protected FileAction(boolean isPreauth) {
        this.isPreauth = isPreauth;

        // Set up tools needed for generating nodeURI and
        // validating permissions
        PropertiesReader pr = new PropertiesReader("Cavern.properties");
        this.root = pr.getFirstPropertyValue("VOS_FILESYSTEM_ROOT");
        if (root == null) {
            throw new IllegalStateException("VOS_FILESYSTEM_ROOT not configured.");
        }

        Path rootPath = Paths.get(root);
        this.upLookupSvc = rootPath.getFileSystem().getUserPrincipalLookupService();

        this.fsPersistence = new FileSystemNodePersistence();
        this.pathResolver = new PathResolver(fsPersistence, true);
        this.authorizer = new VOSpaceAuthorizer(true);
        this.authorizer.setNodePersistence(fsPersistence);

    }

    protected abstract Direction getDirection();

    @Override
    protected InlineContentHandler getInlineContentHandler() {
        return null;
    }

    protected VOSURI getNodeURI() {
        return nodeURI;
    }

    protected String getRoot() {
        return root;
    }

    protected UserPrincipalLookupService getUpLookupSvc() {
        return upLookupSvc;
    }

    @Override
    public void initAction() throws Exception {
        initNodeURI(syncInput.getPath());
        log.debug("sync input available: ");
    }

    /**
     * Initialize the nodeURI value. Check authorization, either token validation or
     * user authentication against node attributes.
     * @param path - path to node URI will be made for
     */
    protected void initNodeURI(String path) {
        if (isPreauth == true) {
            initPreauthTarget(path);
        } else {
            initAuthTarget(path);
        }
    }

    private void initPreauthTarget(String path) throws IllegalArgumentException {

        // Long debug marker as cavern debug is rather verbose
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

        try {
            nodeURI = getURIFromPath(path, true);
            log.debug("checking preauth token for node uri: " + nodeURI);

            // preauth token is validated in this step.
            // Exceptions are thrown if it's not valid
            CavernURLGenerator urlGen = new CavernURLGenerator();
            urlGen.validateToken(token, nodeURI, getDirection());

            log.debug("preauth token good node uri: " + nodeURI);

        } catch ( URISyntaxException | IOException e ) {
            log.debug("unable to init preauth target: " + nodeURI + ": " + e);
            throw new IllegalArgumentException(e.getCause());
        }
    }

    private void initAuthTarget(String path) throws IllegalArgumentException {
        try {
            // false indicates there's no token
            nodeURI = getURIFromPath(path, false);
            log.debug("nodeURI from path: " + nodeURI);

            Direction direction = getDirection();
            // Check read permission on node
            if (Direction.pullFromVoSpace == direction) {
                resolveWithReadPermission(nodeURI);
            } else if (Direction.pushToVoSpace == direction) {
                resolveWithWritePermission(nodeURI);
            } else {
                throw new IllegalArgumentException("direction not supported: " + direction.toString());
            }

        } catch (URISyntaxException | NodeNotFoundException | LinkingException e) {
            log.debug("initAuthTarget:uri syntax exception " + nodeURI + ": " + e.getMessage());
            throw new IllegalArgumentException(e.getCause());

        }
    }

    private Node resolveWithReadPermission(VOSURI targetVOSURI)
        throws AccessControlException, NodeNotFoundException, LinkingException, URISyntaxException {

        log.debug("[resolveWithReadPermission]: checking read permission for targetVOSURI: " + targetVOSURI.toString());
        // Authorization is done in this step.
        Node node = pathResolver.resolveWithReadPermissionCheck(targetVOSURI, authorizer, true);
        log.debug("node resolved with read permission: " + targetVOSURI.toString());

        return node;
    }

    private Node resolveWithWritePermission(VOSURI targetVOSURI)
        throws AccessControlException, NodeNotFoundException, LinkingException, URISyntaxException {

        Node resolvedNode = null;
        try {
            // Test to see if the node exists already or not
            log.debug("[resolveWithWritePermission]: checking read permission for targetVOSURI: " + targetVOSURI.toString());
            resolvedNode = resolveWithReadPermission(targetVOSURI);

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
            Node pn = resolveWithReadPermission(targetVOSURI.getParentURI());

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

    private static VOSURI getURIFromPath(String path, boolean hasToken) throws URISyntaxException {

        log.debug("getURIFromPath for " + path);
        LocalServiceURI localServiceURI = new LocalServiceURI();
        VOSURI baseURI = localServiceURI.getVOSBase();
        log.debug("baseURI for cavern deployment: " + baseURI.toString());

        String pathStr = null;
        if (hasToken == true) {
            int firstSlashIndex = path.indexOf("/");
            pathStr = path.substring(firstSlashIndex + 1);
        } else {
            pathStr = path;
        }

        log.debug("path: " + pathStr);
        String targetURIStr = baseURI.toString() + "/" + pathStr;
        log.debug("target URI for validating token: " + targetURIStr);

        URI targetURI = new URI(targetURIStr);
        log.debug("targetURI for system: " + targetURI.toString());
        VOSURI targetVOSURI = new VOSURI(targetURI);
        log.debug("targetVOSURI: " + targetVOSURI.getURI().toString());

        return targetVOSURI;
    }
}
