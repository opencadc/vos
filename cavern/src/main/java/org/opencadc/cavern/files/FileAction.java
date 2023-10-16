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

import ca.nrc.cadc.net.ResourceNotFoundException;
import ca.nrc.cadc.rest.InlineContentHandler;
import ca.nrc.cadc.rest.RestAction;
import ca.nrc.cadc.util.StringUtil;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.security.AccessControlException;
import javax.naming.Context;
import javax.naming.InitialContext;
import javax.naming.NamingException;
import org.apache.log4j.Logger;
import org.opencadc.cavern.FileSystemNodePersistence;
import org.opencadc.vospace.ContainerNode;
import org.opencadc.vospace.DataNode;
import org.opencadc.vospace.LinkingException;
import org.opencadc.vospace.Node;
import org.opencadc.vospace.NodeLockedException;
import org.opencadc.vospace.NodeNotFoundException;
import org.opencadc.vospace.NodeNotSupportedException;
import org.opencadc.vospace.VOSURI;
import org.opencadc.vospace.server.LocalServiceURI;
import org.opencadc.vospace.server.NodePersistence;
import org.opencadc.vospace.server.PathResolver;
import org.opencadc.vospace.server.auth.VOSpaceAuthorizer;
import org.opencadc.vospace.transfer.Direction;

/**
 *
 * @author majorb
 * @author jeevesh
 */
public abstract class FileAction extends RestAction {
    private static final Logger log = Logger.getLogger(FileAction.class);

    // Key values needed for FileAction
    private VOSURI nodeURI;
    private final boolean isPreauth;

    protected FileSystemNodePersistence nodePersistence;
    protected VOSpaceAuthorizer authorizer;
    protected PathResolver pathResolver;
    

    protected FileAction(boolean isPreauth) {
        this.isPreauth = isPreauth;
    }

    protected abstract Direction getDirection();

    @Override
    protected InlineContentHandler getInlineContentHandler() {
        return null;
    }

    protected VOSURI getNodeURI() {
        if (nodeURI == null) {
            this.nodeURI = getURIFromPath(syncInput.getPath(), false);
        }
        return nodeURI;
    }

    @Override
    public void initAction() throws ResourceNotFoundException, IllegalArgumentException {
        String jndiNodePersistence = appName + "-" + NodePersistence.class.getName();
        try {
            Context ctx = new InitialContext();
            this.nodePersistence = (FileSystemNodePersistence) ctx.lookup(jndiNodePersistence);
            this.authorizer = new VOSpaceAuthorizer(nodePersistence);
            this.pathResolver = new PathResolver(nodePersistence, authorizer, true);
        } catch (NamingException oops) {
            throw new RuntimeException("BUG: NodePersistence implementation not found with JNDI key " + jndiNodePersistence, oops);
        }
    }

    protected void initPreauthTarget(String path) throws IllegalArgumentException {

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
            VOSURI tmpURI = getURIFromPath(path, true);
            log.debug("checking preauth token for node uri: " + tmpURI);

            // preauth token is validated in this step.
            // Exceptions are thrown if it's not valid
            CavernURLGenerator urlGen = (CavernURLGenerator) nodePersistence.getTransferGenerator();
            nodeURI = urlGen.validateToken(token, tmpURI, getDirection());

            log.debug("preauth token good node uri: " + nodeURI);

        } catch (IOException ex) {
            log.debug("unable to init preauth target: " + path + ": " + ex);
            throw new IllegalArgumentException(ex.getCause());
        }
    }

    private VOSURI getURIFromPath(String path, boolean hasToken) {

        log.debug("getURIFromPath for " + path);
        LocalServiceURI localServiceURI = new LocalServiceURI(nodePersistence.getResourceID());
        VOSURI baseURI = localServiceURI.getVOSBase();
        log.debug("baseURI for cavern deployment: " + baseURI.toString());

        String pathStr = null;
        if (hasToken) {
            int firstSlashIndex = path.indexOf("/");
            pathStr = path.substring(firstSlashIndex + 1);
        } else {
            pathStr = path;
        }

        log.debug("path: " + pathStr);
        String targetURIStr = baseURI.toString() + "/" + pathStr;
        log.debug("target URI for validating token: " + targetURIStr);

        try {
            URI targetURI = new URI(targetURIStr);
            log.debug("targetURI for system: " + targetURI.toString());
            VOSURI targetVOSURI = new VOSURI(targetURI);
            log.debug("targetVOSURI: " + targetVOSURI.getURI().toString());
            return targetVOSURI;
        } catch (URISyntaxException ex) {
            throw new RuntimeException("BUG (probably): failed to generate VOSURI from path: " + path, ex);
        }

        
    }
}
