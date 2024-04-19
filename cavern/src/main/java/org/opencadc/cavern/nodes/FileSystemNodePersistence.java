/*
************************************************************************
*******************  CANADIAN ASTRONOMY DATA CENTRE  *******************
**************  CENTRE CANADIEN DE DONNÉES ASTRONOMIQUES  **************
*
*  (c) 2024.                            (c) 2024.
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

import ca.nrc.cadc.auth.AuthenticationUtil;
import ca.nrc.cadc.auth.PosixPrincipal;
import ca.nrc.cadc.io.ResourceIterator;
import ca.nrc.cadc.net.TransientException;
import ca.nrc.cadc.reg.Standards;
import ca.nrc.cadc.reg.client.LocalAuthority;
import ca.nrc.cadc.util.InvalidConfigException;
import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URI;
import java.net.URL;
import java.nio.file.DirectoryNotEmptyException;
import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Comparator;
import java.util.NoSuchElementException;
import java.util.Set;
import java.util.TreeSet;
import java.util.UUID;
import javax.security.auth.Subject;
import org.apache.log4j.Logger;
import org.opencadc.auth.PosixMapperClient;
import org.opencadc.cavern.CavernConfig;
import org.opencadc.cavern.files.CavernURLGenerator;
import org.opencadc.gms.GroupURI;
import org.opencadc.vospace.ContainerNode;
import org.opencadc.vospace.LinkNode;
import org.opencadc.vospace.Node;
import org.opencadc.vospace.NodeNotSupportedException;
import org.opencadc.vospace.VOS;
import org.opencadc.vospace.VOSURI;
import org.opencadc.vospace.server.LocalServiceURI;
import org.opencadc.vospace.server.NodePersistence;
import org.opencadc.vospace.server.PathResolver;
import org.opencadc.vospace.server.Views;
import org.opencadc.vospace.server.auth.VOSpaceAuthorizer;
import org.opencadc.vospace.server.transfers.TransferGenerator;

/**
 * NodePersistence implementation that uses a POSIX file system for node metadata
 * and file data.
 * 
 * @author pdowler
 */
public class FileSystemNodePersistence implements NodePersistence {

    private static final Logger log = Logger.getLogger(FileSystemNodePersistence.class);
    
    static final Set<URI> ADMIN_PROPS = new TreeSet<>(
            Arrays.asList(
                    VOS.PROPERTY_URI_CREATOR, 
                    VOS.PROPERTY_URI_QUOTA
            ));

    static final Set<URI> IMMUTABLE_PROPS = new TreeSet<>(
            Arrays.asList(
                    VOS.PROPERTY_URI_AVAILABLESPACE,
                    VOS.PROPERTY_URI_CONTENTLENGTH,
                    VOS.PROPERTY_URI_CONTENTMD5,
                    VOS.PROPERTY_URI_CONTENTDATE,
                    VOS.PROPERTY_URI_DATE,
                    VOS.PROPERTY_URI_CREATOR,
                    VOS.PROPERTY_URI_QUOTA
            ));
    
    private final PosixIdentityManager identityManager;
    private final PosixMapperClient posixMapper;
    private final GroupCache groupCache;
    
    private final ContainerNode root;
    private final Set<ContainerNode> allocationParents = new TreeSet<>();
    private final Path rootPath;
    private final VOSURI rootURI;
    private final CavernConfig config;
    private final boolean localGroupsOnly;

    public FileSystemNodePersistence() {
        this.config = new CavernConfig();
        this.rootPath = config.getRoot();
        
        LocalServiceURI loc = new LocalServiceURI(config.getResourceID());
        this.rootURI = loc.getVOSBase();

        // must be hard coded to this and not set via java system properties
        this.identityManager = new PosixIdentityManager();
        
        // root node
        UUID rootID = new UUID(0L, 0L); // cosmetic: not used in cavern
        this.root = new ContainerNode(rootID, "");
        root.owner = config.getRootOwner();
        root.ownerDisplay = identityManager.toDisplayString(root.owner);
        root.ownerID = identityManager.toPosixPrincipal(root.owner);
        root.isPublic = true;
        root.inheritPermissions = false;
        
        // create root directories for node/files
        try {
            if (!Files.exists(rootPath, LinkOption.NOFOLLOW_LINKS)) {
                Files.createDirectories(rootPath);
            }
            PosixPrincipal admin = identityManager.toPosixPrincipal(root.owner);
            log.info("root node: " + rootPath + " owner: " + admin.getUidNumber() + "(" + admin.username + ")");
            //NodeUtil.setPosixOwnerGroup(rootPath, admin.getUidNumber(), admin.defaultGroup);
            NodeUtil.setPosixOwnerGroup(rootPath, 0, 0);
        } catch (IOException e) {
            throw new IllegalStateException("Error creating filesystem root directory " + root, e);
        }
        
        LocalAuthority la = new LocalAuthority();
        // only require a group mapper because IVOA GMS does not include numeric gid
        // assume user mapper is the same service
        URI posixMapperID = la.getServiceURI(Standards.POSIX_GROUPMAP.toASCIIString());
        if ("https".equals(posixMapperID.getScheme())) {
            try {
                URL baseURL = posixMapperID.toURL();
                this.posixMapper = new MyPosixMapperClient(baseURL);
            } catch (MalformedURLException ex) {
                throw new InvalidConfigException("invalid " + Standards.POSIX_GROUPMAP.toASCIIString() + " base URL: " + posixMapperID, ex);
            }
        } else {
            this.posixMapper = new MyPosixMapperClient(posixMapperID);
        }
        this.groupCache = new GroupCache(posixMapper);
        this.localGroupsOnly = true;
        
        for (String ap : config.getAllocationParents()) {
            if (ap.isEmpty()) {
                // allocations are in root
                allocationParents.add(root);
                log.info("allocationParent: /");
            } else {
                try {

                    // simple top-level names only
                    ContainerNode cn = (ContainerNode) get(root, ap);
                    String str = "";
                    if (cn == null) {
                        cn = new ContainerNode(ap);
                        cn.parent = root;
                        str = "created/";
                    }
                    cn.isPublic = true;
                    cn.owner = root.owner;
                    cn.inheritPermissions = false;
                    put(cn);
                    allocationParents.add(cn);
                    log.info(str + "loaded allocationParent: /" + cn.getName());
                } catch (NodeNotSupportedException bug) {
                    throw new RuntimeException("BUG: failed to update isPublic=true on allocationParent " + ap, bug);
                }
            }
        }
    }
    
    // support FileAction
    public CavernConfig getConfig() {
        return config;
    }
    
    public PosixIdentityManager getIdentityManager() {
        return identityManager;
    }
    
    // support FileAction
    public Path nodeToPath(VOSURI uri) {
        return NodeUtil.nodeToPath(rootPath, uri);
    }
    
    @Override
    public URI getResourceID() {
        return config.getResourceID();
    }

    @Override
    public ContainerNode getRootNode() {
        return root;
    }

    @Override
    public boolean isAllocation(ContainerNode cn) {
        if (cn.parent == null) {
            return false; // root is never an allocation
        }
        ContainerNode p = cn.parent;
        for (ContainerNode ap : allocationParents) {
            if (NodeUtil.absoluteEquals(p, ap)) {
                return true;
            }
        }
        return false;
    }
    
    @Override
    public Set<URI> getAdminProps() {
        return ADMIN_PROPS;
    }

    @Override
    public Set<URI> getImmutableProps() {
        return IMMUTABLE_PROPS;
    }

    @Override
    public Views getViews() {
        throw new UnsupportedOperationException();
    }

    @Override
    public TransferGenerator getTransferGenerator() {
        return new CavernURLGenerator(this);
    }

    @Override
    public Node get(ContainerNode parent, String name) throws TransientException {
        identityManager.addToCache(AuthenticationUtil.getCurrentSubject());
        try {
            NodeUtil nut = new NodeUtil(rootPath, rootURI, groupCache);
            Node ret = nut.get(parent, name);
            if (ret == null) {
                return null;
            }
            
            PosixPrincipal owner = NodeUtil.getOwner(ret);
            Subject so = identityManager.toSubject(owner);
            ret.owner = so;
            ret.ownerID = identityManager.toPosixPrincipal(so);
            ret.ownerDisplay = identityManager.toDisplayString(so);
            ret.parent = parent;
            return ret;
        } catch (IOException | InterruptedException ex) {
            throw new RuntimeException("oops", ex);
        }
    }

    @Override
    public ResourceIterator<Node> iterator(ContainerNode parent, Integer limit, String start) {
        if (start == null && limit != null && limit == 0) {
            return new EmptyNodeIterator();
        }
        if (start != null) {
            throw new UnsupportedOperationException("batch container node listing: limit ignored, resume not implemented");
        }
        
        try {
            NodeUtil nut = new NodeUtil(rootPath, rootURI, groupCache);
            // this is a complicated way to get the Path
            LocalServiceURI loc = new LocalServiceURI(getResourceID());
            VOSURI vu = loc.getURI(parent);
            ResourceIterator<Node> ni = nut.list(vu);
            return new IdentWrapper(parent, ni, nut);
        } catch (IOException ex) {
            throw new RuntimeException("oops", ex);
        }
    }
    
    private class EmptyNodeIterator implements ResourceIterator<Node> {

        @Override
        public boolean hasNext() {
            return false;
        }

        @Override
        public Node next() {
            throw new NoSuchElementException();
        }

        @Override
        public void close() throws IOException {
            // no-op
        }
    }
    
    private class IdentWrapper implements ResourceIterator<Node> {

        private final ContainerNode parent;
        private final ResourceIterator<Node> childIter;
        private final NodeUtil nut;
        
        IdentWrapper(ContainerNode parent, ResourceIterator<Node> childIter, NodeUtil nut) {
            this.parent = parent;
            this.childIter = childIter;
            this.nut = nut;
        }
        
        @Override
        public boolean hasNext() {
            return childIter.hasNext();
        }

        @Override
        public Node next() {
            Node ret = childIter.next();
            PosixPrincipal owner = NodeUtil.getOwner(ret);
            Subject so = identityManager.toSubject(owner);
            ret.owner = so;
            ret.ownerID = identityManager.toPosixPrincipal(so);
            ret.ownerDisplay = identityManager.toDisplayString(so);
            ret.parent = parent;
            return ret;
        }

        @Override
        public void close() throws IOException {
            childIter.close();
        }
    }

    @Override
    public void getProperties(Node node) throws TransientException {
        //no-op
    }

    @Override
    public Node put(Node node) throws NodeNotSupportedException, TransientException {
        if (node == null) {
            throw new IllegalArgumentException("arg cannot be null: node");
        }
        if (node.parent == null) {
            throw new RuntimeException("BUG: cannot persist node without parent: " + node);
        }
        if (node.ownerID == null) {
            if (node.owner == null) {
                throw new RuntimeException("BUG: cannot persist node without owner: " + node);
            }
            node.ownerID = identityManager.toPosixPrincipal(node.owner);
        }
        
        //if (node.isStructured()) {
        //    throw new NodeNotSupportedException("StructuredDataNode is not supported.");
        //}
        if (localGroupsOnly) {
            if (!node.getReadOnlyGroup().isEmpty() || !node.getReadWriteGroup().isEmpty()) {
                LocalAuthority loc = new LocalAuthority();
                try {
                    URI localGMS = loc.getServiceURI(Standards.GMS_SEARCH_10.toASCIIString());
                    StringBuilder serr = new StringBuilder("non-local groups:");
                    int len = serr.length();
                    for (GroupURI g : node.getReadOnlyGroup()) {
                        if (!localGMS.equals(g.getServiceID())) {
                            serr.append(" ").append(g.getURI().toASCIIString());
                        }
                    }
                    for (GroupURI g : node.getReadWriteGroup()) {
                        if (!localGMS.equals(g.getServiceID())) {
                            serr.append(" ").append(g.getURI().toASCIIString());
                        }
                    }
                    String err = serr.toString();
                    if (err.length() > len) {
                        throw new IllegalArgumentException(err);
                    }
                } catch (NoSuchElementException ex) {
                    throw new RuntimeException("CONFIG: localGroupOnly policy && local GMS service not configured");
                }
            }
        }

        NodeUtil nut = new NodeUtil(rootPath, rootURI, groupCache);

        if (node instanceof LinkNode) {
            LinkNode ln = (LinkNode) node;
            try {
                PathResolver ps = new PathResolver(this, new VOSpaceAuthorizer(this));
                ps.validateTargetURI(ln);
            } catch (Exception ex) {
                throw new UnsupportedOperationException("link to external resource", ex);
            }
        }

        // this is a complicated way to get the Path
        LocalServiceURI loc = new LocalServiceURI(getResourceID());
        VOSURI vu = loc.getURI(node);
        try {
            nut.put(node, vu);
            return node;
        } catch (IOException | InterruptedException ex) {
            throw new RuntimeException("failed to create/update node at " + vu.getPath(), ex);
        }
    }

    @Override
    public void move(Node node, ContainerNode dest, String newName) {
        if (node == null || dest == null) {
            throw new IllegalArgumentException("args cannot be null");
        }
        if (node.parent == null || dest.parent == null) {
            throw new IllegalArgumentException("args must both be peristent nodes before move");
        }
        
        NodeUtil nut = new NodeUtil(rootPath, rootURI, groupCache);
        Subject caller = AuthenticationUtil.getCurrentSubject();
        PosixPrincipal owner = identityManager.addToCache(caller);
        
        LocalServiceURI loc = new LocalServiceURI(getResourceID());
        VOSURI srcURI = loc.getURI(node);
        VOSURI destURI = loc.getURI(dest);

        try {
            nut.move(srcURI, destURI, owner, newName);
        } catch (IOException ex) {
            throw new RuntimeException("failed to move " + srcURI.getPath() + " -> " + destURI.getPath(), ex);
        }
    }
    
    @Override
    public void delete(Node node) throws TransientException {
        NodeUtil nut = new NodeUtil(rootPath, rootURI, groupCache);
        Subject caller = AuthenticationUtil.getCurrentSubject();
        identityManager.addToCache(caller);
        try {
            // this is a complicated way to get the Path to delete
            LocalServiceURI loc = new LocalServiceURI(getResourceID());
            VOSURI vu = loc.getURI(node);
            nut.delete(vu);
        } catch (DirectoryNotEmptyException ex) {
            throw new IllegalArgumentException("container node '" + node.getName() + "' is not empty");
        } catch (IOException ex) {
            throw new RuntimeException("oops", ex);
        }
    }
}
