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

package org.opencadc.cavern.nodes;

import ca.nrc.cadc.date.DateUtil;
import ca.nrc.cadc.vos.*;
import java.io.IOException;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.Path;
import java.nio.file.attribute.GroupPrincipal;
import java.nio.file.attribute.PosixFileAttributeView;
import java.nio.file.attribute.PosixFileAttributes;
import java.nio.file.attribute.UserPrincipal;
import java.nio.file.attribute.UserPrincipalLookupService;
import java.text.DateFormat;
import java.util.Date;
import java.util.List;
import org.apache.log4j.Logger;

/**
 * Utility methods for interacting with nodes.
 * @author pdowler
 */
public abstract class NodeUtil {
    private static final Logger log = Logger.getLogger(NodeUtil.class);

    private NodeUtil() { 
    }
    
    public static Path nodeToPath(Path root, Node node) {
        assertNotNull("node", node);
        return nodeToPath(root, node.getUri());
    }
    
    public static Path nodeToPath(Path root, VOSURI uri) {
        assertNotNull("root", root);
        assertNotNull("uri", uri);
        log.debug("[nodeToPath] root: " + root + " uri: " + uri);
        
        String nodePath = uri.getPath().substring(1);
        Path np = root.resolve(nodePath);
        log.debug("[nodeToPath] path: " + uri + " -> " + np);
        return np;
    }
    
    public static Path create(Path root, Node node) throws IOException {
        Path np = nodeToPath(root, node);
        log.debug("[create] path: " + node.getUri() + " -> " + np);
        
        UserPrincipal owner = getOwner(root.getFileSystem().getUserPrincipalLookupService(), node);
        assertNotNull("owner", owner);
        // TODO: don't assume convention of user == default group
        GroupPrincipal group = root.getFileSystem().getUserPrincipalLookupService().lookupPrincipalByGroupName(owner.getName());
        
        Path ret = null;
        if (node instanceof ContainerNode) {
            ret = Files.createDirectory(np);
        } else if (node instanceof DataNode) {
            ret = Files.createFile(np);
        } else if (node instanceof LinkNode) {
            LinkNode ln = (LinkNode) node;
            String targPath = ln.getTarget().getPath().substring(1);
            Path absPath = root.resolve(targPath);
            Path tp = root.relativize(absPath);
            ret = Files.createSymbolicLink(np, tp);
        } else {
            throw new UnsupportedOperationException("unexpected node type: " + node.getClass().getName());
        }
        
        PosixFileAttributeView pv = Files.getFileAttributeView(ret, PosixFileAttributeView.class, LinkOption.NOFOLLOW_LINKS);
        pv.setOwner(owner);
        if (group != null) {
            pv.setGroup(group);
        }
        
        return ret;
    }
    
    public static Node get(Path root, VOSURI uri) throws IOException {
        Path np = nodeToPath(root, uri);
        log.debug("[get] path: " + uri.getURI() + " -> " + np);
        
        PosixFileAttributes attrs = Files.readAttributes(np, PosixFileAttributes.class, LinkOption.NOFOLLOW_LINKS);
        Node ret = null;
        if (attrs.isDirectory()) {
            ret = new ContainerNode(uri);
        } else if (attrs.isRegularFile()) {
            ret = new DataNode(uri);
            // TODO: restore file-specific properties
        } else if (attrs.isSymbolicLink()) {
            Path tp = Files.readSymbolicLink(np);
            URI turi = URI.create(uri.getScheme() + "://" + uri.getAuthority() + "/" + tp.toFile().getPath());
            ret = new LinkNode(uri, turi);
        } else {
            throw new IllegalStateException("found unexpected file system object: " + np);
        }
        log.debug("[get] attrs: " + attrs);
        setOwner(ret, attrs.owner());
        DateFormat df = DateUtil.getDateFormat(DateUtil.IVOA_DATE_FORMAT, DateUtil.UTC);
        //Date created = new Date(attrs.creationTime().toMillis());
        //Date accessed = new Date(attrs.lastAccessTime().toMillis());
        Date modified = new Date(attrs.lastModifiedTime().toMillis());
        //ret.getProperties().add(new NodeProperty(VOS.PROPERTY_URI_CREATION_DATE, df.format(created)));
        //ret.getProperties().add(new NodeProperty(VOS.PROPERTY_URI_ACCESS_DATE, df.format(accessed)));
        //ret.getProperties().add(new NodeProperty(VOS.PROPERTY_URI_MODIFIED_DATE, df.format(modified)));
        ret.getProperties().add(new NodeProperty(VOS.PROPERTY_URI_CREATION_DATE, df.format(modified)));
        
        // TODO: restore generic properties
        return ret;
    }
    
    public static void delete(Path root, VOSURI uri) throws IOException {
        Path np = nodeToPath(root, uri);
        log.debug("[create] path: " + uri + " -> " + np);
        if (Files.isDirectory(np, LinkOption.NOFOLLOW_LINKS)) {
            // TODO: walk tree and empty it from bottom up
        }
        Files.delete(np);
    }
    
    public static List<Node> list(Path root, ContainerNode parent, VOSURI start, Long limit) {
        throw new UnsupportedOperationException();
    }
    
    public static void assertNotNull(String name, Object o) {
        if (o == null) {
            throw new IllegalArgumentException(name + " cannot be null");
        }
    }
            
    // temporary: store bare UserPrincipal in node property list as string
    public static void setOwner(Node node, UserPrincipal owner) {
        node.getProperties().add(new NodeProperty(VOS.PROPERTY_URI_CREATOR, owner.getName()));
    }
    
    public static UserPrincipal getOwner(UserPrincipalLookupService users, Node node) throws IOException {
        NodeProperty prop = node.findProperty(VOS.PROPERTY_URI_CREATOR);
        if (prop != null) {
            return users.lookupPrincipalByName(prop.getPropertyValue());
        }
        return null;
    }
}
