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

package org.opencadc.cavern;

import ca.nrc.cadc.auth.AuthenticationUtil;
import ca.nrc.cadc.net.TransientException;
import ca.nrc.cadc.util.FileMetadata;
import ca.nrc.cadc.util.PropertiesReader;
import ca.nrc.cadc.vos.ContainerNode;
import ca.nrc.cadc.vos.DataNode;
import ca.nrc.cadc.vos.Node;
import ca.nrc.cadc.vos.NodeNotFoundException;
import ca.nrc.cadc.vos.NodeNotSupportedException;
import ca.nrc.cadc.vos.NodeProperty;
import ca.nrc.cadc.vos.VOS;
import ca.nrc.cadc.vos.VOSURI;
import ca.nrc.cadc.vos.server.NodeID;
import ca.nrc.cadc.vos.server.NodePersistence;
import java.io.IOException;
import java.net.URI;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.attribute.UserPrincipal;
import java.util.Iterator;
import java.util.List;
import javax.security.auth.Subject;
import org.apache.log4j.Logger;
import org.opencadc.cavern.nodes.NodeUtil;

/**
 *
 * @author pdowler
 */
public class FileSystemNodePersistence implements NodePersistence {

    private static final Logger log = Logger.getLogger(FileSystemNodePersistence.class);

    private PosixIdentityManager identityManager;
    private Path root;

    public FileSystemNodePersistence() {
        PropertiesReader pr = new PropertiesReader("Cavern.properties");
        String rootConfig = pr.getFirstPropertyValue("VOS_FILESYSTEM_ROOT");
        if (rootConfig == null) {
            throw new RuntimeException("CONFIG: Failed to find VOS_FILESYSTEM_ROOT");
        }
        this.root = Paths.get(rootConfig);
        this.identityManager = new PosixIdentityManager(root.getFileSystem().getUserPrincipalLookupService());
    }

    @Override
    public Node get(VOSURI uri) throws NodeNotFoundException, TransientException {
        try {
            Node ret = NodeUtil.get(root, uri);
            if (ret == null) {
                throw new NodeNotFoundException(uri.getPath());
            }
            Node cur = ret;
            while (cur != null) {
                UserPrincipal owner = NodeUtil.getOwner(root.getFileSystem().getUserPrincipalLookupService(), cur);
                Subject so = identityManager.toSubject(owner);
                cur.appData = new NodeID(-1L, so, null);
                cur = cur.getParent();
            }
            log.debug("[get] " + ret);
            return ret;
        } catch (IOException ex) {
            throw new RuntimeException("oops", ex);
        }
    }

    @Override
    public Node get(VOSURI vosuri, boolean bln) throws NodeNotFoundException, TransientException {
        return get(vosuri);
        //throw new UnsupportedOperationException("get-partial");
    }

    @Override
    public Node get(VOSURI vosuri, boolean bln, boolean bln1) throws NodeNotFoundException, TransientException {
        return get(vosuri);
        //throw new UnsupportedOperationException("get-partial-resolve");
    }

    @Override
    public void getChildren(ContainerNode cn) throws TransientException {
        getChildren(cn, null, 100, true);
    }

    @Override
    public void getChildren(ContainerNode cn, VOSURI vosuri, Integer intgr) throws TransientException {
        getChildren(cn, vosuri, 100, true);
    }

    @Override
    public void getChildren(ContainerNode cn, boolean bln) throws TransientException {
        getChildren(cn, null, 100, bln);
    }

    @Override
    public void getChildren(ContainerNode cn, VOSURI start, Integer limit, boolean bln) throws TransientException {
        try {
            Iterator<Node> ni = NodeUtil.list(root, cn, start, limit);
            while (ni.hasNext()) {
                Node cur = ni.next();
                UserPrincipal owner = NodeUtil.getOwner(root.getFileSystem().getUserPrincipalLookupService(), cur);
                Subject so = identityManager.toSubject(owner);
                log.debug(cur.getUri() + " owner: " + so);
                cur.appData = new NodeID(-1L, so, null);
                cur.setParent(cn);
                cn.getNodes().add(cur);
                log.debug("[getChildren] " + cur);
            }
        } catch (IOException ex) {
            throw new RuntimeException("oops", ex);
        }
    }

    @Override
    public void getChild(ContainerNode cn, String name) throws TransientException {
        getChild(cn, name, true);
    }

    @Override
    public void getChild(ContainerNode cn, String name, boolean resolveMetadata) throws TransientException {
        VOSURI vuri = new VOSURI(URI.create(cn.getUri().getURI().toASCIIString() + "/" + name));
        try {

            Node child = get(vuri);
            if (child != null) {
                cn.getNodes().add(child);
            }
        } catch (NodeNotFoundException ignore) {
            log.debug("not found: " + vuri);
        }
    }

    @Override
    public void getProperties(Node node) throws TransientException {
        //throw new UnsupportedOperationException("getProperties");
    }

    @Override
    public Node put(Node node) throws NodeNotSupportedException, TransientException {
        if (node.isStructured()) {
            throw new NodeNotSupportedException("StructuredDataNode is not supported.");
        }
        if (node.getParent() != null && node.getParent().appData == null) {
            throw new IllegalArgumentException("parent of node is not a persistent node: " + node.getUri().getPath());
        }

        if (node.appData != null) { // persistent node == update == not supported
            throw new UnsupportedOperationException("update of existing node not supported");
        }

        try {
            Subject s = AuthenticationUtil.getCurrentSubject();
            UserPrincipal owner = identityManager.toUserPrincipal(s);
            NodeUtil.setOwner(node, owner);
            NodeUtil.create(root, node);
            return NodeUtil.get(root, node.getUri());
        } catch (IOException ex) {
            throw new RuntimeException("oops", ex);
        }
    }

    @Override
    public Node updateProperties(Node node, List<NodeProperty> properties) throws TransientException {
        // node arg is the current node, properties are the new props
        try {
            Path np = NodeUtil.nodeToPath(root, node);
            for (NodeProperty prop : properties)
            {
                NodeProperty cur = node.findProperty(prop.getPropertyURI());
                if (cur != null) {
                    if (prop.isMarkedForDeletion()) { // delete
                        node.getProperties().remove(cur);
                    } else { // update
                        cur.setValue(prop.getPropertyValue());
                    }
                } else { // add
                    if (!prop.isMarkedForDeletion()) {
                        node.getProperties().add(prop);
                    } // else: ignore delete non-existent prop
                }
            }
            NodeUtil.setNodeProperties(np, node);
        }  catch (IOException ex) {
            throw new RuntimeException("oops", ex);
        }
        return node;
    }

    @Override
    public void delete(Node node) throws TransientException {
        try {
            NodeUtil.delete(root, node.getUri());
        } catch (IOException ex) {
            throw new RuntimeException("oops", ex);
        }
    }

    @Override
    public void setFileMetadata(DataNode dn, FileMetadata fm, boolean bln) throws TransientException {
        // callback from storage system
        throw new UnsupportedOperationException();
    }

    @Override
    public void setBusyState(DataNode dn, VOS.NodeBusyState nbs, VOS.NodeBusyState nbs1) throws TransientException {
        // callback from storage system
        throw new UnsupportedOperationException();
    }

    @Override
    public void move(Node node, ContainerNode cn) throws TransientException {
        log.debug("move: " + node.getUri() + " to " + cn.getUri() + " as " + node.getName());
        // move rule check: check authorities
        checkSameAuthority(node, cn);

        if (node instanceof ContainerNode)
        {
            // move rule check: check that we are not moving root or a root container
            if (node.getParent() == null || node.getParent().getUri().isRoot())
                throw new IllegalArgumentException("Cannot move a root container.");

            // move rule check: check that 'src' is not in the path of 'dest' so that
            // circular paths are not created
            // This check is probably done by the file system, consider removing it.
            Node target = cn;
            while (target != null && !target.getUri().isRoot())
            {
                if (target.getUri().getPath().equals(node.getUri().getPath()))
                    throw new IllegalArgumentException("Cannot move to a contained sub-node.");
                target = target.getParent();
            }
        }

        try {
            Subject s = AuthenticationUtil.getCurrentSubject();
            UserPrincipal owner = identityManager.toUserPrincipal(s);
            NodeUtil.move(root, node.getUri(), cn.getUri(), owner);
        } catch (IOException ex) {
            throw new RuntimeException("oops", ex);
        }
    }

    @Override
    public void copy(Node node, ContainerNode cn) throws TransientException {
        log.debug("copy: " + node.getUri() + " to " + cn.getUri() + " as " + node.getName());
        // copy rule check: check authorities
        checkSameAuthority(node, cn);

        try {
            Subject s = AuthenticationUtil.getCurrentSubject();
            UserPrincipal owner = identityManager.toUserPrincipal(s);
            NodeUtil.copy(root, node.getUri(), cn.getUri(), owner);
        } catch (IOException ex) {
            throw new RuntimeException("oops", ex);
        }
    }

    private void checkSameAuthority(Node n1, Node n2) {
        URI srcAuthority = n1.getUri().getServiceURI(); // this removes the ! vs ~ issue
        URI destAuthority = n2.getUri().getServiceURI();
        if (!srcAuthority.equals(destAuthority))
        {
            throw new RuntimeException("Nodes have different authorities.");
        }
    }
}
