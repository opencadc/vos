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

import ca.nrc.cadc.auth.AuthenticationUtil;
import ca.nrc.cadc.auth.PosixPrincipal;
import ca.nrc.cadc.io.ByteLimitExceededException;
import ca.nrc.cadc.net.ResourceNotFoundException;
import ca.nrc.cadc.reg.Standards;
import ca.nrc.cadc.reg.client.LocalAuthority;
import ca.nrc.cadc.rest.InlineContentException;
import ca.nrc.cadc.rest.InlineContentHandler;
import ca.nrc.cadc.util.HexUtil;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.nio.file.AccessDeniedException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.security.AccessControlException;
import java.security.DigestInputStream;
import java.security.MessageDigest;
import javax.security.auth.Subject;
import org.apache.log4j.Logger;
import org.opencadc.auth.PosixMapperClient;
import org.opencadc.cavern.PosixIdentityManager;
import org.opencadc.vospace.ContainerNode;
import org.opencadc.vospace.DataNode;
import org.opencadc.vospace.Node;
import org.opencadc.vospace.NodeProperty;
import org.opencadc.vospace.VOS;
import org.opencadc.vospace.VOSURI;
import org.opencadc.vospace.transfer.Direction;

/**
 *
 * @author majorb
 * @author jeevesh
 */
public abstract class PutAction extends FileAction {
    private static final Logger log = Logger.getLogger(PutAction.class);

    private static final String INPUT_STREAM = "in";

    private final PosixIdentityManager identityManager = new PosixIdentityManager();
    private final PosixMapperClient posixMapper;
    
    public PutAction(boolean isPreauth) {
        super(isPreauth);
        LocalAuthority loc = new LocalAuthority();
        URI posixMapperID = loc.getServiceURI(Standards.POSIX_GROUPMAP.toASCIIString());
        this.posixMapper = new PosixMapperClient(posixMapperID);
    }

    protected Direction getDirection() {
        return Direction.pushToVoSpace;
    }

    @Override
    protected InlineContentHandler getInlineContentHandler() {
        return new InlineContentHandler() {
            public Content accept(String name, String contentType,
                    InputStream inputStream)
                    throws InlineContentException, IOException {
                InlineContentHandler.Content c = new InlineContentHandler.Content();
                c.name = INPUT_STREAM;
                c.value = inputStream;
                return c;
            }
        };
    }

    @Override
    public void doAction() throws Exception {
        VOSURI nodeURI = getNodeURI();
        DataNode node = null;
        Path target = null;
        boolean putStarted = false;
        boolean successful = false;
        
        try {
            log.debug("put: start " + nodeURI.getURI().toASCIIString());
            
            // PathResolver checks read permission
            String parentPath = nodeURI.getParentURI().getPath();
            Node n = pathResolver.getNode(parentPath);
            if (n == null) {
                throw new ResourceNotFoundException("not found: parent container " + parentPath);
            }
            if (!(n instanceof ContainerNode)) {
                throw new IllegalArgumentException("parent is not a container node");
            }
            ContainerNode cn = (ContainerNode) n;
            n = nodePersistence.get(cn, nodeURI.getName());
            
            // only support data nodes for now
            if (n != null && !(DataNode.class.isAssignableFrom(n.getClass()))) {
                throw new IllegalArgumentException("not a data node");
            }
            node = (DataNode) n;
            
            // check write permission
            if (n != null && authorizer.hasSingleNodeWritePermission(n, AuthenticationUtil.getCurrentSubject())) {
                log.debug("authorized to write to existing data node");
            } else if (authorizer.hasSingleNodeWritePermission(cn, AuthenticationUtil.getCurrentSubject())) {
                log.debug("authorized to write to parent container");
            } else {
                throw new AccessControlException("permission denied: write to " + nodeURI.getPath());
            }
            
            target = nodePersistence.nodeToPath(nodeURI);
            
            InputStream in = (InputStream) syncInput.getContent(INPUT_STREAM);
            MessageDigest md = MessageDigest.getInstance("MD5");
            DigestInputStream vis = new DigestInputStream(in, md);
            log.debug("copy: start " + target);
            putStarted = true;
            Files.copy(vis, target, StandardCopyOption.REPLACE_EXISTING);
            log.debug("copy: done " + target);

            URI expectedMD5 = syncInput.getDigest();
            byte[] md5 = md.digest();
            String propValue = HexUtil.toHex(md5);
            URI actualMD5 = URI.create("md5:" + propValue);
            if (expectedMD5 != null && !expectedMD5.equals(actualMD5)) {
                // upload failed: do not keep corrupt data
                log.debug("upload corrupt: " + expectedMD5 + " != " + propValue);
                //cleanupOnFailure(target); already in finally
                return;
            }
            
            // re-read node from filesystem
            node = (DataNode) nodePersistence.get(cn, nodeURI.getName());
            
            // update Node
            Subject caller = AuthenticationUtil.getCurrentSubject();
            node.owner = caller;
            node.ownerID = identityManager.toPosixPrincipal(caller);
        
            log.debug(nodeURI + " MD5: " + propValue);
            NodeProperty csp = node.getProperty(VOS.PROPERTY_URI_CONTENTMD5);
            if (csp == null) {
                csp = new NodeProperty(VOS.PROPERTY_URI_CONTENTMD5, actualMD5.toASCIIString());
                node.getProperties().add(csp);
            } else {
                csp.setValue(actualMD5.toASCIIString());
            }

            nodePersistence.put(node);
            successful = true;
        } catch (AccessDeniedException e) {
            // TODO: this is a deployment error because cavern doesn't have permission to filesystem
            log.debug("403 error with PUT: ",  e);
            throw new AccessControlException(e.getLocalizedMessage());

        } catch (IOException ex) {
            String msg = ex.getMessage();
            if (msg != null && msg.contains("quota")) {
                // TODO: traverse up the path to find quota
                //rootPath = Paths.get(getRoot());
                //restoreOwnNGroup(rootPath, node);
                long limit = -1;
                ContainerNode curNode = node.parent;
                while (curNode != null && limit == -1) {
                    String limitValue = curNode.getPropertyValue(VOS.PROPERTY_URI_QUOTA);
                    if (limitValue != null) {
                        limit = Long.parseLong(limitValue);
                    }
                }

                // TODO: Replace the quota limit below with the number of bytes 
                //       remaining in the quota when we can determine it. This 
                //       means the above code to obtain the limitValue will need
                //       to be changed
                
                if (limit == -1) {
                    // VOS.PROPERTY_URI_QUOTA attribute is not set on any parent node
                    msg = "VOS.PROPERTY_URI_QUOTA attribute not set, " + ex.getMessage();
                    log.warn(msg);
                }

                throw new ByteLimitExceededException(ex.getMessage(), limit);
            }
            
            throw ex;
        } finally {
            if (successful) {
                log.debug("put: done " + nodeURI.getURI().toASCIIString());
            } else if (putStarted) {
                cleanupOnFailure(target);
            }
        }
    }
    
    private void cleanupOnFailure(Path target) {
        throw new UnsupportedOperationException();
    }
    
    /*
    private void cleanUpOnFailure(Path target, DataNode node, Path rootPath) throws IOException {
        log.debug("clean up on put failure " + target);
        Files.delete(target);
        // restore empty DataNode: remove props that are no longer applicable
        NodeProperty npToBeRemoved = node.findProperty(VOS.PROPERTY_URI_CONTENTMD5);
        node.getProperties().remove(npToBeRemoved);
        try {
            nodePersistence.put(node);
        } catch (NodeNotSupportedException bug) {
            throw new RuntimeException("BUG: unexpected " + bug, bug);
        }
        return;
    }
    
    private void restoreOwnNGroup(Path rootPath, Node node) throws IOException {
        PosixPrincipal pp = NodeUtil.getOwner(node);
        Integer gid = NodeUtil.getDefaultGroup(pp);
        Path target = NodeUtil.nodeToPath(rootPath, node);
        NodeUtil.setPosixOwnerGroup(target, pp.getUidNumber(), gid);
    }
    */
}
