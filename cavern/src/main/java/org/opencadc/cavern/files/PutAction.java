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

import ca.nrc.cadc.io.ByteLimitExceededException;
import ca.nrc.cadc.rest.InlineContentException;
import ca.nrc.cadc.rest.InlineContentHandler;
import ca.nrc.cadc.util.HexUtil;
import ca.nrc.cadc.vos.DataNode;
import ca.nrc.cadc.vos.Direction;
import ca.nrc.cadc.vos.Node;
import ca.nrc.cadc.vos.NodeProperty;
import ca.nrc.cadc.vos.VOS;
import ca.nrc.cadc.vos.VOSURI;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.AccessDeniedException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.nio.file.attribute.GroupPrincipal;
import java.nio.file.attribute.UserPrincipal;
import java.security.AccessControlException;
import java.security.DigestInputStream;
import java.security.MessageDigest;
import org.apache.log4j.Logger;
import org.opencadc.cavern.nodes.NodeUtil;

/**
 *
 * @author majorb
 * @author jeevesh
 */
public abstract class PutAction extends FileAction {
    private static final Logger log = Logger.getLogger(PutAction.class);

    private static final String INPUT_STREAM = "in";

    public PutAction(boolean isPreauth) {
        super(isPreauth);
    }

    protected Direction getDirection() {
        return Direction.pushToVoSpace;
    };

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
        Node node = null;
        Path target = null;
        Path rootPath = null;
        boolean putStarted = false;
        boolean successful = false;

        try {
            log.debug("put: start " + nodeURI.getURI().toASCIIString());

            rootPath = Paths.get(getRoot());
            node = NodeUtil.get(rootPath, nodeURI);
            if (node == null) {
                // Node needs to be created ahead of time for PUT
                sendError(404, "node must be created before PUT");
                return;
            }

            // only support data nodes for now
            if (!(DataNode.class.isAssignableFrom(node.getClass()))) {
                log.debug("400 error with PUT: not a writable node");
                // not using sendError here because of .getBytes()
                syncOutput.getOutputStream().write("Not a writable node".getBytes());
                syncOutput.setCode(400);
            }

            target = NodeUtil.nodeToPath(rootPath, node);
            
            InputStream in = (InputStream) syncInput.getContent(INPUT_STREAM);
            MessageDigest md = MessageDigest.getInstance("MD5");
            DigestInputStream vis = new DigestInputStream(in, md);
            log.debug("copy: start " + target);
            putStarted = true;
            Files.copy(vis, target, StandardCopyOption.REPLACE_EXISTING);
            log.debug("copy: done " + target);

            String expectedMD5 = syncInput.getHeader("Content-MD5");
            log.debug("set properties");
            byte[] md5 = md.digest();
            String propValue = HexUtil.toHex(md5);
            if (expectedMD5 != null && !expectedMD5.equals(propValue)) {
                // upload failed: do not keep corrupt data
                log.debug("upload corrupt: " + expectedMD5 + " != " + propValue);
                cleanUpOnFailure(target, node, rootPath);
                return;
            }
            
            log.debug(nodeURI + " MD5: " + propValue);
            node.getProperties().add(new NodeProperty(VOS.PROPERTY_URI_CONTENTMD5, propValue));

            NodeUtil.setNodeProperties(target, node);

            // doing this last because it requires chown which is most likely to fail during experimentation
            log.debug("restore owner & group");
            restoreOwnNGroup(rootPath, node);
            successful = true;
        } catch (AccessControlException | AccessDeniedException e) {
            log.debug("403 error with PUT: ",  e);
            sendError(403, e.getLocalizedMessage());

        } catch (IOException e) {
            String msg = e.getMessage();
            if (msg != null && msg.contains("quota")) {
                rootPath = Paths.get(getRoot());
                restoreOwnNGroup(rootPath, node);
                Node curNode = node.getParent();
                String limitValue = curNode.getPropertyValue(VOS.PROPERTY_URI_QUOTA);
                while (limitValue == null) {
                    curNode = curNode.getParent();
                    if (curNode == null) {
                        // quota limit not defined in a parent container, exit while loop
                        break;
                    } else {
                        limitValue = curNode.getPropertyValue(VOS.PROPERTY_URI_QUOTA);
                    }
                }

                // TODO: Replace the quota limit below with the number of bytes 
                //       remaining in the quota when we can determine it. This 
                //       means the above code to obtain the limitValue will need
                //       to be changed.
                // get quota limit from a parent container
                // -1 to indicate quota limit not defined
                long limit = -1;
                if (limitValue == null) {
                    // VOS.PROPERTY_URI_QUOTA attribute is not set on the node
                    msg = "VOS.PROPERTY_URI_QUOTA attribute not set, " + e.getMessage();
                    log.warn(msg);
                } else {
                    limit = Long.parseLong(limitValue);
                }

                throw new ByteLimitExceededException(e.getMessage(), limit);
            } else {
                // unexpected IOException
                throw e;
            }
        } finally {
            if (successful) {
                log.debug("put: done " + nodeURI.getURI().toASCIIString());
            } else if (putStarted) {
                cleanUpOnFailure(target, node, rootPath);
            }
        }
    }
    
    private void cleanUpOnFailure(Path target, Node node, Path rootPath) throws IOException {
        log.debug("clean up on put failure " + target);
        Files.delete(target);
        // restore empty DataNode: remove props that are no longer applicable
        NodeProperty npToBeRemoved = node.findProperty(VOS.PROPERTY_URI_CONTENTMD5);
        node.getProperties().remove(npToBeRemoved);
        NodeUtil.create(rootPath, node);
        return;
    }
    
    private void restoreOwnNGroup(Path rootPath, Node node) throws IOException {
        UserPrincipal owner = NodeUtil.getOwner(getUpLookupSvc(), node);
        GroupPrincipal group = NodeUtil.getDefaultGroup(getUpLookupSvc(), owner);
        Path target = NodeUtil.nodeToPath(rootPath, node);
        NodeUtil.setPosixOwnerGroup(rootPath, target, owner, group);
    }

}
