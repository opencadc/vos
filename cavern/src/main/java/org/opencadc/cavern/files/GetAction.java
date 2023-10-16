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
import java.io.FileNotFoundException;
import java.io.OutputStream;
import java.nio.file.AccessDeniedException;
import java.nio.file.FileSystem;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.security.AccessControlException;
import org.apache.log4j.Logger;
import org.opencadc.vospace.ContainerNode;
import org.opencadc.vospace.LinkingException;
import org.opencadc.vospace.Node;
import org.opencadc.vospace.NodeNotFoundException;
import org.opencadc.vospace.VOS;
import org.opencadc.vospace.VOSURI;
import org.opencadc.vospace.transfer.Direction;

/**
 *
 * @author majorb
 * @author jeevesh
 */

public abstract class GetAction extends FileAction {
    private static final Logger log = Logger.getLogger(GetAction.class);

    public GetAction(boolean isPreauth) {
        super(isPreauth);
    }

    protected Direction getDirection() {
        return Direction.pullFromVoSpace;
    };

    @Override
    public void doAction()  throws Exception {
        try {
            VOSURI nodeURI = getNodeURI();
            log.warn("target: " + nodeURI);
            
            // PathResolver checks read permission: nothing else to do
            Node node = pathResolver.getNode(nodeURI.getPath());

            // GetAction code is common for both /files and /preauth endpoints. Neither will support
            // GET for container nodes
            if (node instanceof ContainerNode) {
                log.debug("container nodes not supported for GET");
                throw new IllegalArgumentException("GET for directories not supported");
            }
            
            final Path source = nodePersistence.nodeToPath(nodeURI);

            log.debug("node path resolved: " + node.getName());
            log.debug("node type: " + node.getClass().getCanonicalName());
            String contentEncoding = node.getPropertyValue(VOS.PROPERTY_URI_CONTENTENCODING);
            String contentLength = node.getPropertyValue(VOS.PROPERTY_URI_CONTENTLENGTH);
            String contentMD5 = node.getPropertyValue(VOS.PROPERTY_URI_CONTENTMD5);
            syncOutput.setHeader("Content-Disposition", "inline; filename=" + nodeURI.getName());
            syncOutput.setHeader("Content-Type", node.getPropertyValue(VOS.PROPERTY_URI_TYPE));
            syncOutput.setHeader("Content-Encoding", contentEncoding);
            syncOutput.setHeader("Content-Length", contentLength);
            syncOutput.setHeader("Content-MD5", contentMD5);

            // IOExceptions thrown from getOutputStream, Files.copy and out.flush
            // will be handled by RestServlet
            OutputStream out = syncOutput.getOutputStream();
            log.debug("Starting copy of file " + source);
            Files.copy(source, out);
            log.debug("Completed copy of file " + source);
            out.flush();

        } catch (NodeNotFoundException | FileNotFoundException | NoSuchFileException e) {
            log.debug("404 error with GET: ",  e);
            throw new ResourceNotFoundException(e.getMessage());
        } catch (LinkingException e) {
            log.debug("400 error with GET: ",  e);
            throw new IllegalArgumentException(e.getMessage());
        } catch (AccessControlException | AccessDeniedException e) {
            log.debug(e);
            throw new AccessControlException(e.getMessage());
        }
    }
}
