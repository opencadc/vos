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

package ca.nrc.cadc.vos;

import java.net.URI;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;

import org.apache.log4j.Logger;

/**
 * Abstract class defining an object within VOSpace.
 *  
 * @see ca.nrc.cadc.vos.DataNode
 * @see ca.nrc.cadc.vos.ContainerNode
 * 
 * @author majorb
 *
 */
public abstract class Node extends Entity implements Comparable<Node> {
    private static final Logger log = Logger.getLogger(Node.class);

    /**
     * Keep track of the VOSpace version since some things differ. This field
     * is here to facilitate reading and writing the same version of documents
     * on the server side in order to maintain support for older clients.
     */
    public int version = VOS.VOSPACE_20;

    // The name of the node
    private String name;

    // The ID of the creator
    public String creatorID;

    // Flag indicating if the node is public
    public Boolean isPublic;

    // Flag indicating if the node is locked
    public Boolean isLocked;

    // Set of groups with read access to the Node
    public final Set<URI> readOnlyGroup = new TreeSet<>();

    // Set of groups with read/write access to the Node
    public final Set<URI> readWriteGroup = new TreeSet<>();

    // Set of node properties
    public final Set<NodeProperty> properties = new TreeSet<>();

    // To be used by controlling applications as they wish
    public transient Object appData;

    // List of views which this node accepts
    public final transient List<URI> accepts = new ArrayList<>();

    // List of views which this node provides
    public final transient List<URI> provides = new ArrayList<>();

    /**
     * Node constructor.
     *
     * @param name The name of the node.
     */
    protected Node(String name) {
        this(name, new TreeSet<>());
    }

    /**
     * Node constructor.
     *
     * @param name The name of the node.
     * @param properties The node's properties.
     */
    protected Node(String name, Set<NodeProperty> properties) {
        super();
        NodeUtil.assertNotNull(Node.class, "name", "name");
        this.name = name;
        this.properties.addAll(properties);
    }

    @Override
    public String toString() {
        return this.getClass().getSimpleName()
            + ", [name=" + name
            + ", appData=" + appData
            + ", properties=" + properties + "]";
    }

    /**
     * Nodes are considered equal if the names are equal.
     */
    @Override
    public boolean equals(Object o) {
        if (o == null) {
            return false;
        }
        Node node = (Node) o;
        return this.name.equals(node.getName());
    }

    /**
     * @param node the node for comparison.
     * @return an integer denoting the display order for two nodes.
     */
    @Override
    public int compareTo(Node node) {
        if (node == null) {
            return -1;
        }
        return this.name.compareTo(node.getName());
    }

    /**
     * @return the hashcode of toString().
     */
    @Override
    public int hashCode() {
        return this.name.hashCode();
    }

    /**
     * Get the name of the node.
     *
     * @return node name.
     */
    public String getName() {
        return this.name;
    }

    /**
     * Set the name of the node.
     */
    public void setName(String name) {
        this.name = name;
    }

    /**
     * Get a node property by its key.
     * 
     * @param uri the node property identifier.
     * @return the node property object or null if not found.
     */
    public NodeProperty getProperty(URI uri) {
        for (NodeProperty nodeProperty : this.properties) {
            if (nodeProperty.getKey() == uri) {
                return nodeProperty;
            }
        }
        return null;
    }

    /**
     * Return the value of the specified property.
     *
     * @param uri the node property identifier.
     * @return the value or null if not found.
     */
    public String getPropertyValue(URI uri) {
        NodeProperty nodeProperty = getProperty(uri);
        if (nodeProperty != null) {
            return nodeProperty.getValue();
        }
        return null;
    }

}
