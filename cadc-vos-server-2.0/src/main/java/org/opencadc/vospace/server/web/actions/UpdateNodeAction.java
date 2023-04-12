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

package org.opencadc.vospace.server.web.actions;

import ca.nrc.cadc.io.ByteCountInputStream;
import ca.nrc.cadc.net.ResourceAlreadyExistsException;
import ca.nrc.cadc.net.TransientException;
import ca.nrc.cadc.rest.InlineContentHandler;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.security.AccessControlException;
import java.util.ArrayList;
import java.util.List;
import org.apache.log4j.Logger;
import org.opencadc.vospace.ContainerNode;
import org.opencadc.vospace.Node;
import org.opencadc.vospace.NodeNotFoundException;
import org.opencadc.vospace.NodeProperty;
import org.opencadc.vospace.VOSURI;
import org.opencadc.vospace.io.NodeParsingException;
import org.opencadc.vospace.io.NodeReader;
import org.opencadc.vospace.io.NodeWriter;
import org.opencadc.vospace.server.LocalServiceURI;
import org.opencadc.vospace.server.NodeFault;

/**
 * Class to perform the creation of a Node.
 *
 * @author adriand
 */
public class UpdateNodeAction extends NodeAction
{

    protected static Logger log = Logger.getLogger(UpdateNodeAction.class);

    private static final String INLINE_CONTENT_TAG = "inputstream";

    // 12Kb XML Doc size limit
    private static final long DOCUMENT_SIZE_MAX = 12288L;

    @Override
    public Node getClientNode()
        throws NodeParsingException, IOException
    {
        InputStream in = (InputStream) syncInput.getContent(INLINE_CONTENT_TAG);
        return getNode(in);
    }

    @Override
    public Node doAuthorizationCheck()
        throws AccessControlException, FileNotFoundException, TransientException
    {
        try
        {
            Node node = nodePersistence.get(nodeURI);
            voSpaceAuthorizer.getWritePermission(node);

            return node;
        }
        catch (NodeNotFoundException ex)
        {
            // node does not exist: FAIL
            throw new FileNotFoundException("not found: " + nodeURI.getURI().toASCIIString());
        }
    }

    @Override
    public void performNodeAction(Node clientNode, Node serverNode)
            throws Exception {

        //TODO This should not be required. In fact the update should take the clientNode
        // and check if they are different
        List<NodeProperty> props = new ArrayList<>();
        props.addAll(clientNode.properties);
        Node storedNode = nodePersistence.updateProperties(serverNode, props);

        // return the node in xml format
        final NodeWriter nodeWriter = getNodeWriter();
        syncOutput.setHeader("Content-Type", getMediaType());
        nodeWriter.write(LocalServiceURI.getURI(storedNode), storedNode, syncOutput.getOutputStream());
    }

    private Node getNode(InputStream content) throws IOException, NodeParsingException {
        ByteCountInputStream sizeLimitInputStream =
                new ByteCountInputStream(content, DOCUMENT_SIZE_MAX);

        NodeReader.NodeReaderResult nrr = new NodeReader().read(sizeLimitInputStream);

        log.debug("Node input representation read " + sizeLimitInputStream.getByteCount() + " bytes.");

        // ensure the path in the XML URI matches the path in the URL
        if (!nrr.vosURI.getPath().equals(nodeURI.getPath()))
        {
            throw new NodeParsingException("Node path in URI XML ("
                    + nrr.vosURI.getPath()
                    + ") not equal to node path in URL ("
                    + nodeURI.getPath()
                    + ")");
        }

        return nrr.node;
    }

    @Override
    protected InlineContentHandler getInlineContentHandler() {
        return (name, contentType, inputStream) -> {
            InlineContentHandler.Content content = new InlineContentHandler.Content();
            content.name = INLINE_CONTENT_TAG;
            content.value = inputStream;
            return content;
        };
    }
}
