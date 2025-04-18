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

package org.opencadc.cavern.files;

import ca.nrc.cadc.auth.AuthenticationUtil;
import ca.nrc.cadc.net.ResourceNotFoundException;
import java.io.FileNotFoundException;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.file.AccessDeniedException;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.security.AccessControlException;
import java.util.Date;
import javax.security.auth.Subject;
import org.apache.log4j.Logger;
import org.opencadc.permissions.ReadGrant;
import org.opencadc.vospace.ContainerNode;
import org.opencadc.vospace.DataNode;
import org.opencadc.vospace.LinkingException;
import org.opencadc.vospace.Node;
import org.opencadc.vospace.NodeNotFoundException;
import org.opencadc.vospace.VOS;
import org.opencadc.vospace.VOSURI;
import org.opencadc.vospace.io.NodeWriter;
import org.opencadc.vospace.server.NodeFault;

/**
 * Get file metadata from filesystem and output http headers.
 * 
 * @author pdowler
 */
public class HeadAction extends FileAction {
    private static final Logger log = Logger.getLogger(HeadAction.class);

    public HeadAction() { 
    }

    @Override
    public void doAction() throws Exception {
        resolveAndSetMetadata();
    }
    
    // GetAction re-uses all this logic
    DataNode resolveAndSetMetadata() throws Exception {
        try {
            VOSURI nodeURI = getNodeURI();
            log.debug("target: " + nodeURI);
            
            Subject caller = AuthenticationUtil.getCurrentSubject();
            boolean preauthGranted = false;
            if (preauthToken != null) {
                CavernURLGenerator cav = new CavernURLGenerator(nodePersistence);
                Object tokenUser = cav.validateToken(preauthToken, nodeURI, ReadGrant.class);
                preauthGranted = true;
                caller.getPrincipals().clear();
                if (tokenUser != null) {
                    Subject s = identityManager.toSubject(tokenUser);
                    caller.getPrincipals().addAll(s.getPrincipals());
                }
                // reset loggables
                logInfo.setSubject(caller);
                logInfo.setResource(nodeURI.getURI());
                logInfo.setPath(syncInput.getContextPath() + syncInput.getComponentPath());
                logInfo.setGrant("read: preauth-token");
            }
            log.debug("preauthGranted:" + preauthGranted);
            
            // PathResolver checks read permission
            // TODO: disable permission checks in resolver if preauthGranted
            Node node = pathResolver.getNode(nodeURI.getPath(), true);
            if (node == null) {
                throw NodeFault.NodeNotFound.getStatus(nodeURI.getURI().toASCIIString());
            }

            // GetAction code is common for both /files and /preauth endpoints. Neither will support
            // GET for container nodes
            if (node instanceof ContainerNode) {
                log.debug("container nodes not supported for GET");
                throw new IllegalArgumentException("GET for directories not supported");
            }
            DataNode dn = (DataNode) node;
            log.debug("node path resolved: " + node.getName());
            log.debug("node type: " + node.getClass().getCanonicalName());
            syncOutput.setHeader("Content-Length", dn.bytesUsed);
            syncOutput.setHeader("Content-Disposition", "inline; filename=\"" + nodeURI.getName() + "\"");
            syncOutput.setHeader("Content-Type", node.getPropertyValue(VOS.PROPERTY_URI_TYPE));
            syncOutput.setHeader("Content-Encoding", node.getPropertyValue(VOS.PROPERTY_URI_CONTENTENCODING));
            
            if (node.getPropertyValue(VOS.PROPERTY_URI_DATE) != null) {
                Date lastMod = NodeWriter.getDateFormat().parse(node.getPropertyValue(VOS.PROPERTY_URI_DATE));
                syncOutput.setLastModified(lastMod);
            }
            
            String contentMD5 = node.getPropertyValue(VOS.PROPERTY_URI_CONTENTMD5);
            if (contentMD5 != null) {
                try {
                    // backwards compatible hack in case the stored attribute does not have the full URI
                    if (!contentMD5.startsWith("md5")) {
                        contentMD5 = "md5:" + contentMD5;
                    }
                    URI md5 = new URI(contentMD5);
                    syncOutput.setDigest(md5);
                } catch (URISyntaxException ex) {
                    log.error("found invalid checksum attribute " + contentMD5 + " on node " + nodeURI);
                    // yes, just skip: users can set attributes so hard to tell if this is a bug or
                    // user mistake
                }
            }

            syncOutput.setCode(200);
            return dn;
        } catch (AccessDeniedException ex) {
            throw new RuntimeException("CONFIG: unexpected read fail", ex);
        }
    }
}
