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
import org.opencadc.vospace.NodeNotFoundException;
import org.opencadc.vospace.NodeNotSupportedException;
import org.opencadc.vospace.NodeProperty;
import org.opencadc.vospace.VOSURI;

/**
 * An interface defining the methods available for working with VOSpace
 * nodes in the persistent layer.
 * 
 * @author majorb
 */
public interface NodePersistence
{

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
     * @param name relative name of the child node
     * @return the child node or null if it does not exist
     * @throws TransientException
     */
    Node get(ContainerNode parent, String name) throws TransientException, NodeNotFoundException;

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
     * @param limit max number of nodes to return, may be null
     * @param start first node in order to consider, may be null
     * @return iterator of matching child nodes, may be empty
     */
    ResourceIterator<Node> iterator(ContainerNode parent, Integer limit, String start);

    /**
     * Update the properties of the specified node.  The node must have
     * been retrieved from the persistent layer. Properties in the list are
     * merged with existing properties following the semantics specified in
     * the VOSpace 2.0 specification.
     * 
     * @param node node to update
     * @param properties new properties
     * @return the modified node
     * @throws TransientException If a transient error occurs
     */
    Node updateProperties(Node node, List<NodeProperty> properties)
        throws TransientException;

    /**
     * Update the node metadata after a transfer (put) is complete.
     *
     * @param node node to update
     * @param meta metadata from the successful put
     * @param strict If the update should only occur if the lastModified date is the same.
     * @throws TransientException If a transient error occurs
     */
    void setFileMetadata(DataNode node, FileMetadata meta, boolean strict)
        throws TransientException;

    /**
     * Set the busy state of the node from curState to newState.
     * 
     * @param node The node on which to alter the busy state.
     * @param curState The current state of the node.
     * @param newState The new state for the node.
     * @throws TransientException If a transient error occurs
     */
//TODO    void setBusyState(DataNode node, NodeBusyState curState, NodeBusyState newState)
//        throws TransientException;
    
    /**
     * Move the specified node to the new path.  The node must have been retrieved
     * from the persistent layer.
     * 
     * @param src The node to move.
     * @param destination The destination container.
     * @throws TransientException If a transient error occurs
     */
    void move(Node src, ContainerNode destination)
        throws TransientException;
    
    /**
     * Copy the specified node to the specified path.  The node must been retrieved
     * from the persistent layer.
     * 
     * @param src The node to move.
     * @param destination The destination container.
     * @throws TransientException If a transient error occurs
     */
    void copy(Node src, ContainerNode destination)
        throws TransientException;
    
}


/**
 * Find the node with the specified path. The returned node(s) will include
 * some properties (typically inherently single-valued properties like owner,
 * content-length, content-type, content-encoding, content-MD5) plus all
 * properties needed to make authorization checks (isPublic, group-read, and
 * group-write). Remaining properties and child nodes can be filled in as
 * needed with getProperties(Node) and getChildren(ContainerNode).
 *
 * @param vos a node identifier
 * @return the specified node
 * @throws NodeNotFoundException If node not found
 * @throws TransientException If transient network problems
 */
//    Node get(VOSURI vos)
//        throws NodeNotFoundException, TransientException;

/**
 * Find the node with the specified path. The returned node(s) will include
 * some properties (typically inherently single-valued properties like owner,
 * content-length, content-type, content-encoding, content-MD5) plus all
 * properties needed to make authorization checks (isPublic, group-read, and
 * group-write). Remaining properties and child nodes can be filled in as
 * needed with getProperties(Node) and getChildren(ContainerNode). When partial
 * path is allowed (only allowed for a LinkNode) and the path of the identified
 * node cannot be resolved to a leaf node, and the last resolved node in the
 * partial path must be a LinkNode, and is returned.
 *
 * @param vos a node identifier
 * @param allowPartialPaths true if partial path is allowed, false otherwise
 * @return the specified node
 * @throws NodeNotFoundException If node not found
 * @throws TransientException If transient network problems
 */
//Node get(VOSURI vos, boolean allowPartialPaths)
//    throws NodeNotFoundException, TransientException;

/**
 * Find the node with the specified path. The returned node(s) will include
 * some properties (typically inherently single-valued properties like owner,
 * content-length, content-type, content-encoding, content-MD5) plus all
 * properties needed to make authorization checks (isPublic, group-read, and
 * group-write). Remaining properties and child nodes can be filled in as
 * needed with getProperties(Node) and getChildren(ContainerNode). When partial
 * path is allowed (only allowed for a LinkNode) and the path of the identified
 * node cannot be resolved to a leaf node, and the last resolved node in the
 * partial path must be a LinkNode, and is returned.
 *
 * @param vos a node identifier
 * @param allowPartialPaths true if partial path is allowed, false otherwise
 * @param resolveMetadata If false, return raw system values for resolvable
 *                        metadata.
 * @return the specified node
 * @throws NodeNotFoundException If node not found
 * @throws TransientException If transient network problems
 */
//Node get(VOSURI vos, boolean allowPartialPaths, boolean resolveMetadata)
//    throws NodeNotFoundException, TransientException;

//
//    /**
//     * Load all the children of a container.
//     *
//     * @param node the container node
//     * @throws TransientException If transient network errors
//     */
//    void getChildren(ContainerNode node)
//            throws TransientException;
//
//    /**
//     * Load some of the children of a container. If <code>uri</code> is null, a
//     * server-selected first node is used. If <code>limit</code> is null or
//     * exceeds an arbitrary internal value, the internal value is used.
//     *
//     * @param parent parent container
//     * @param start URI of the first child
//     * @param limit children limit
//     * @throws TransientException If transient network error
//     */
//    void getChildren(ContainerNode parent, VOSURI start, Integer limit)
//            throws TransientException;
//
//    /**
//     * Load all the children of a container based on the detail level.
//     *
//     * @param node container node
//     * @param resolveMetadata false, do not look up the owner subject so as to reduce
//     *     the time spent on the current service.
//     * @throws TransientException If transient network error
//     */
//    void getChildren(ContainerNode node, boolean resolveMetadata)
//            throws TransientException;
//
//    /**
//     * Load some of the children of a container.
//     *
//     * @param parent parent container
//     * @param start URI of first child. If null, server-selected first node is used.
//     * @param limit children limit. If null or exceeds an arbitrary internal value,
//     *       the internal value is used.
//     * @param resolveMetadata is false, do not look up the owner subject so as to reduce
//     *      the time spent on the current service.
//     * @throws TransientException If transient network error
//     */
//    void getChildren(ContainerNode parent, VOSURI start, Integer limit, boolean resolveMetadata)
//            throws TransientException;
//
//
//    /**
//     * Load a single child of a container.
//     *
//     * @param parent parent container node
//     * @param name name of the child node to load
//     * @throws TransientException If transient network error
//     */
//    void getChild(ContainerNode parent, String name)
//            throws TransientException;
//
//    /**
//     * Load a single child of a container.
//     *
//     * @param parent parent container node
//     * @param name name of the child node to load
//     * @param resolveMetadata If false, return raw system values for resolvable
//     *                        metadata.
//     * @throws TransientException If a transient error occurs
//     */
//    void getChild(ContainerNode parent, String name, boolean resolveMetadata)
//            throws TransientException;
