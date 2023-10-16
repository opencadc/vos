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

package org.opencadc.cavern;

import ca.nrc.cadc.auth.AuthenticationUtil;
import ca.nrc.cadc.auth.PosixPrincipal;
import ca.nrc.cadc.io.ResourceIterator;
import ca.nrc.cadc.net.TransientException;
import java.io.IOException;
import java.net.URI;
import java.nio.file.Path;
import java.util.Iterator;
import java.util.Set;
import java.util.UUID;
import javax.security.auth.Subject;
import org.apache.log4j.Logger;
import org.opencadc.cavern.files.CavernURLGenerator;
import org.opencadc.cavern.nodes.NodeUtil;
import org.opencadc.vospace.ContainerNode;
import org.opencadc.vospace.LinkNode;
import org.opencadc.vospace.LinkingException;
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
    
    private final PosixIdentityManager identityManager;
    
    private final ContainerNode root;
    private final Path rootPath;
    private final VOSURI rootURI;
    private final CavernConfig config;

    public FileSystemNodePersistence() {
        this.config = new CavernConfig();
        this.rootPath = config.getRoot();
        
        LocalServiceURI loc = new LocalServiceURI(config.getResourceID());
        this.rootURI = loc.getVOSBase();

        // must be hard coded to this and not set via java system properties
        this.identityManager = new PosixIdentityManager();
        
        // root node
        Subject rawOwner = config.getRootOwner();
        UUID rootID = new UUID(0L, 0L);
        this.root = new ContainerNode(rootID, "");
        root.owner = identityManager.augment(rawOwner);
        root.ownerDisplay = identityManager.toDisplayString(root.owner);
        log.warn("ROOT owner: " + root.owner);
        root.ownerID = identityManager.toPosixPrincipal(root.owner);
        root.isPublic = true;
        root.inheritPermissions = false;
        // TODO: create and chown the root directory (idempotent)
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
    public Set<URI> getImmutableProps() {
        throw new UnsupportedOperationException();
    }

    @Override
    public Views getViews() {
        throw new UnsupportedOperationException();
    }

    @Override
    public TransferGenerator getTransferGenerator() {
        return new CavernURLGenerator(this, config.getProperties());
    }

    @Override
    public Node get(ContainerNode parent, String name) throws TransientException {
        NodeUtil nut = new NodeUtil(rootPath, rootURI);
        nut.addToCache(AuthenticationUtil.getCurrentSubject());
        try {
            Node ret = nut.get(parent, name);
            if (ret == null) {
                return null;
            }
            
            PosixPrincipal owner = NodeUtil.getOwner(ret);
            Subject so = nut.getFromCache(owner);
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
        
        NodeUtil nut = new NodeUtil(rootPath, rootURI);
        try {
            // this is a complicated way to get the Path
            LocalServiceURI loc = new LocalServiceURI(getResourceID());
            VOSURI vu = loc.getURI(parent);
            Iterator<Node> ni = nut.list(vu, limit, start);
            return new IdentWrapper(parent, ni, nut);
        } catch (IOException | InterruptedException ex) {
            throw new RuntimeException("oops", ex);
        }
    }
    
    private class IdentWrapper implements ResourceIterator<Node> {

        private final ContainerNode parent;
        //private final ResourceIterator<Node> childIter;
        private final Iterator<Node> childIter;
        private final NodeUtil nut;
        
        //IdentWrapper(ContainerNode parent, ResourceIterator<Node> childIter, NodeUtil nut) {
        IdentWrapper(ContainerNode parent, Iterator<Node> childIter, NodeUtil nut) {
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
            Subject so = nut.getFromCache(owner);
            ret.owner = so;
            ret.ownerID = identityManager.toPosixPrincipal(so);
            ret.ownerDisplay = identityManager.toDisplayString(so);
            ret.parent = parent;
            return ret;
        }

        @Override
        public void close() throws IOException {
            //childIter.close();
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
        if (node.parentID == null) {
            if (node.parent == null) {
                throw new RuntimeException("BUG: cannot persist node without parent: " + node);
            }
            node.parentID = node.parent.getID();
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

        NodeUtil nut = new NodeUtil(rootPath, rootURI);

        if (node instanceof LinkNode) {
            LinkNode ln = (LinkNode) node;
            try {
                PathResolver ps = new PathResolver(this, new VOSpaceAuthorizer(this), true);
                ps.validateTargetURI(ln);
            } catch (Exception ex) {
                throw new UnsupportedOperationException("link to external resource", ex);
            }
        }

        try {
            // this is a complicated way to get the Path
            LocalServiceURI loc = new LocalServiceURI(getResourceID());
            VOSURI vu = loc.getURI(node);
            nut.put(node, vu);
            return node;
        } catch (IOException | InterruptedException ex) {
            throw new RuntimeException("oops", ex);
        }
    }

    @Override
    public void delete(Node node) throws TransientException {
        NodeUtil nut = new NodeUtil(rootPath, rootURI);
        Subject caller = AuthenticationUtil.getCurrentSubject();
        PosixPrincipal owner = nut.addToCache(caller);
        try {
            // this is a complicated way to get the Path to delete
            LocalServiceURI loc = new LocalServiceURI(getResourceID());
            VOSURI vu = loc.getURI(node);
            nut.delete(vu);
        } catch (IOException ex) {
            throw new RuntimeException("oops", ex);
        }
    }
}
