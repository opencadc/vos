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
 *                                       Node
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

import ca.nrc.cadc.io.ResourceIterator;
import ca.nrc.cadc.net.TransientException;
import ca.nrc.cadc.util.FileMetadata;
import java.net.URI;
import java.util.List;
import java.util.Set;
import org.opencadc.vospace.ContainerNode;
import org.opencadc.vospace.DataNode;
import org.opencadc.vospace.Node;
import org.opencadc.vospace.NodeNotSupportedException;
import org.opencadc.vospace.NodeProperty;

/**
 * An interface defining the methods available for working with VOSpace
 * nodes in the persistent layer.
 *
 * @author majorb
 * @author pdowler
 */
public interface NodePersistence {
    /**
     * Returns the resource ID for the service in the form of a URI (ivo://{authority}/{name})
     *
     * @return service ID
     */
    URI getResourceID();

    /**
     * Get the container node that represents the root of all other nodes.
     * This container node is used to navigate a path (from the root) using
     * <code>get(ContainerNode parent, String name)</code>. The owner of the
     * root node is the admin when that role is required for permission
     * checking.
     *
     * @return the root container node
     */
    ContainerNode getRootNode();

    /**
     * Get the set of property keys that are not writable in this node
     * persistence implementation.
     *
     * @return set of immutable property keys
     */
    public Set<URI> getImmutableProps();

    /**
     * Get a node by name. Concept: The caller uses this to navigate the path from the root
     * node to the target, checking permissions and deciding what to do about
     * LinkNode(s) along the way.
     *
     * @param parent parent node, may be special root node but not null
     * @param name   relative name of the child node
     * @return the child node or null if it does not exist
     * @throws TransientException
     */
    Node get(ContainerNode parent, String name) throws TransientException;

    /**
     * Load additional node properties for the specified node. Note: this was here
     * as an optimization and may not be necessary for all implementations, but
     * those could just implement as a no-op.
     *
     * @param node
     * @throws TransientException
     */
    void getProperties(Node node) throws TransientException;

    /**
     * Put the specified node. This can be an insert or update; to update, the argument
     * node must have been retrieved from persistence so it has the right Entity.id
     * value. This method may modify the Entity.metaChecksum and the Entity.lastModified
     * value.
     *
     * @param node the node to insert or update
     * @return the possibly modified node
     * @throws NodeNotSupportedException
     * @throws TransientException
     */
    Node put(Node node) throws NodeNotSupportedException, TransientException;

    /**
     * Delete the specified node.
     *
     * @param node the node to delete
     * @throws TransientException
     */
    void delete(Node node) throws TransientException;

    /**
     * Get an iterator over the children of a node. The output can optionally be
     * limited to a specific number of children and can optionally start at a
     * specific child (usually the last one from a previous "batch") to resume
     * listing at a known position. The caller is responsible for closing the
     * iterator when it is finished, whether or not it reached the end; using
     * try-with-resources is the recommended pattern.
     *
     * @param parent the container to iterate
     * @param limit  max number of nodes to return, may be null
     * @param start  first node in order to consider, may be null
     * @return iterator of matching child nodes, may be empty
     */
    ResourceIterator<Node> iterator(ContainerNode parent, Integer limit, String start);
}
