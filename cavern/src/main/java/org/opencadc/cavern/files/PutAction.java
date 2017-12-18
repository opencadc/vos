/*
************************************************************************
*******************  CANADIAN ASTRONOMY DATA CENTRE  *******************
**************  CENTRE CANADIEN DE DONNÉES ASTRONOMIQUES  **************
*
*  (c) 2017.                            (c) 2017.
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


import ca.nrc.cadc.rest.InlineContentException;
import ca.nrc.cadc.rest.InlineContentHandler;
import ca.nrc.cadc.vos.DataNode;
import ca.nrc.cadc.vos.Direction;
import ca.nrc.cadc.vos.Node;
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

import org.apache.log4j.Logger;
import org.opencadc.cavern.nodes.NodeUtil;

/**
 *
 * @author majorb
 */
public class PutAction extends FileAction {
    private static final Logger log = Logger.getLogger(PutAction.class);

    private static final String INPUT_STREAM = "in";

    public PutAction() {
        super();
    }

    @Override
    public Direction getDirection() {
        return Direction.pushToVoSpace;
    }

    @Override
    protected InlineContentHandler getInlineContentHandler()
    {
        return new InlineContentHandler() {
            public Content accept(String name, String contentType,
                    InputStream inputStream)
                    throws InlineContentException, IOException
            {
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

        try {
            Path rootPath = Paths.get(getRoot());
            Node node = NodeUtil.get(rootPath, nodeURI);
            if (node == null) {
                // When the /files endpoint supports the putting of data
                // before the node is created this will have to change.
                // For now, return NotFound.
                syncOutput.setCode(404);
                return;
            }

            UserPrincipal owner = NodeUtil.getOwner(getUpLookupSvc(), node);
            GroupPrincipal group = NodeUtil.getDefaultGroup(getUpLookupSvc(), owner);

            // only support data nodes for now
            if (!(DataNode.class.isAssignableFrom(node.getClass()))) {
                syncOutput.getOutputStream().write("Not a writable node".getBytes());
                syncOutput.setCode(400);
            }

            Path target = NodeUtil.nodeToPath(rootPath, node);

            InputStream in = (InputStream) syncInput.getContent(INPUT_STREAM);
            log.debug("Starting copy to file: " + target);
            Files.copy(in, target, StandardCopyOption.REPLACE_EXISTING);
            log.debug("Completed copy to file: " + target);

            log.debug("Restoring original permissions");
            NodeUtil.applyPermissions(rootPath, target, owner, group);
        } catch (AccessControlException | AccessDeniedException e) {
            log.debug(e);
            syncOutput.setCode(403);
        }
    }
}
