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

package org.opencadc.vospace.server;

import ca.nrc.cadc.auth.AuthenticationUtil;
import java.net.URI;
import java.net.URISyntaxException;
import java.security.Principal;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;
import javax.security.auth.Subject;
import org.apache.log4j.Logger;
import org.opencadc.vospace.ContainerNode;
import org.opencadc.vospace.Node;
import org.opencadc.vospace.NodeProperty;
import org.opencadc.vospace.VOS;

/**
 * Utility methods
 *
 * @author adriand
 */
public class Utils {
    static final Logger log = Logger.getLogger(Utils.class);

    /**
     * Returns the path of the parent with no leading or trailing backslashes
     *
     * @param nodePath the path of the node
     * @return the path of the parent
     */
    public static String getParentPath(final String nodePath) {
        if (isRoot(nodePath)) {
            return null; // there is no parent of the root
        }
        String np = nodePath;
        if (np.endsWith("/")) {
            np = np.substring(0, np.length() - 1);
        }
        if (nodePath.startsWith("/")) {
            np = np.substring(1, np.length());
        }
        int index = np.lastIndexOf("/");
        if (index > 0) {
            return np.substring(0, index);
        } else {
            return null;
        }
    }

    public static boolean isRoot(String nodePath) {
        if (nodePath == null || nodePath.length() == 0 || nodePath.equals("/")) {
            return true;
        }
        return false;
    }

    /**
     * Get a linked list of nodes from leaf to root.
     *
     * @param leaf leaf node
     * @return list of nodes, with leaf first and root last
     */
    public static LinkedList<Node> getNodeList(Node leaf) {
        LinkedList<Node> nodes = new LinkedList<Node>();
        Node cur = leaf;
        while (cur != null) {
            nodes.add(cur);
            cur = cur.parent;
        }
        return nodes;
    }


    public static String getPath(Node node) {
        StringBuilder sb = new StringBuilder();
        sb.append(node.getName());
        Node tmp = node.parent;
        while (tmp != null) {
            sb.insert(0, tmp.getName() + "/");
            tmp = tmp.parent;
        }
        return sb.toString();
    }

    /**
     * Takes a set of old properties and updates it with a new set of properties. Essentially
     * this means updating values or removing and adding elements. It is not a straight 
     * replacement.
     *
     * @param oldProps set of old Node Properties that are being updated
     * @param newProps set of new Node Properties to be used for the update
     * @param immutable set of immutable property keys to skip
     */
    public static void updateNodeProperties(Set<NodeProperty> oldProps, Set<NodeProperty> newProps, Set<URI> immutable) 
            throws Exception {
        for (Iterator<NodeProperty> newIter = newProps.iterator(); newIter.hasNext(); ) {
            NodeProperty newProperty = newIter.next();
            if (newProperty.getKey().toASCIIString().startsWith(VOS.VOSPACE_URI_NAMESPACE)) {
                try {
                    validatePropertyKey(newProperty.getKey());
                } catch (URISyntaxException ex) {
                    throw NodeFault.InvalidArgument.getStatus(ex.getMessage());
                }
            }
            if (oldProps.contains(newProperty)) {
                oldProps.remove(newProperty);
            }
            if (!newProperty.isMarkedForDeletion() && !immutable.contains(newProperty.getKey())) {
                oldProps.add(newProperty);
            }
        }
    }
    
    private static void validatePropertyKey(URI key) throws URISyntaxException {
        if (key.getScheme() == null) {
            throw new URISyntaxException("invalid structure: no scheme", key.toASCIIString());
        }

        // vocabulary concepts allowed by IVOA Identifiers (ivo),
        // IVOA Vocabularies-2.x (http, https) or a vocabulary stored in a specific vospace service
        if (key.getScheme().equals("ivo") 
                || key.getScheme().equals("http") || key.getScheme().equals("https")
                || key.getScheme().equals("vos")) {
            // must have authority + path + fragment
            if (key.getAuthority() == null) {
                throw new URISyntaxException("invalid structure: no authority", key.toASCIIString());
            }
            log.warn("key path: '" + key.getPath() + "'");
            if (key.getPath() == null || key.getPath().equals("/")) {
                throw new URISyntaxException("invalid structure: no path", key.toASCIIString());
            }
            if (key.getFragment() == null) {
                throw new URISyntaxException("invalid structure: no fragment", key.toASCIIString());
            }
            // do not allow query string
            if (key.getQuery() != null) {
                throw new URISyntaxException("invalid structure: query string not allowed", key.toASCIIString());
            }
            if (key.toASCIIString().startsWith(VOS.VOSPACE_URI_NAMESPACE)) {
                if (!VOS.VOSPACE_CORE_PROPERTIES.contains(key)) {
                    throw new URISyntaxException("unrecognized property URI in vospace namespace", key.toASCIIString());
                }
            }
        } else {
            // allow scheme:scheme-specific-part
            if (key.getAuthority() != null) {
                throw new URISyntaxException("invalid structure: authority in unrecognized scheme", key.toASCIIString());
            }
            String ssp = key.getSchemeSpecificPart();
            if (ssp == null || ssp.isEmpty()) {
                throw new URISyntaxException("invalid structure: custom scheme with no scheme-specific part", key.toASCIIString());
            }
            // don't care about query string or fragment
        }
    }

    // needed by create and update
    public static List<NodeProperty> getAdminProps(Node clientNode, Set<URI> adminProps, Subject caller,
                                                   NodePersistence nodePersistence) {
        List<NodeProperty> aps = new ArrayList<>();
        // extract admin props
        for (URI pk : adminProps) {
            NodeProperty ap = clientNode.getProperty(pk);
            if (ap != null) {
                aps.add(ap);
            }
        }
        // clear if not admin
        if (!aps.isEmpty() && !isAdmin(caller, nodePersistence)) {
            log.debug("Not admin - cleared admin props");
            aps.clear();
        }
        log.debug("Admin props " + aps.size());
        return aps;
    }

    // needed by create
    public static boolean isAdmin(Subject caller, NodePersistence nodePersistence) {
        if (caller == null || caller.getPrincipals().isEmpty()) {
            return false;
        }

        ContainerNode root = nodePersistence.getRootNode();
        for (Principal owner : root.owner.getPrincipals()) {
            for (Principal p : caller.getPrincipals()) {
                if (AuthenticationUtil.equals(owner, p)) {
                    return true;
                }
            }
        }

        // TODO: also check admin group(s) aka root.getReadWriteGroup() membership
        return false;
    }
}
