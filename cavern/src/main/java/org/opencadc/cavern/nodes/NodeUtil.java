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

package org.opencadc.cavern.nodes;

import ca.nrc.cadc.auth.PosixPrincipal;
import ca.nrc.cadc.date.DateUtil;
import ca.nrc.cadc.net.ResourceAlreadyExistsException;
import ca.nrc.cadc.net.ResourceNotFoundException;
import ca.nrc.cadc.reg.Standards;
import ca.nrc.cadc.reg.client.LocalAuthority;
import ca.nrc.cadc.vos.ContainerNode;
import ca.nrc.cadc.vos.DataNode;
import ca.nrc.cadc.vos.LinkNode;
import ca.nrc.cadc.vos.Node;
import ca.nrc.cadc.vos.NodeProperty;
import ca.nrc.cadc.vos.VOS;
import ca.nrc.cadc.vos.VOSURI;
import ca.nrc.cadc.vos.server.NodeID;
import java.io.IOException;
import java.net.URI;
import java.nio.file.DirectoryStream;
import java.nio.file.FileVisitResult;
import java.nio.file.FileVisitor;
import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.nio.file.attribute.BasicFileAttributes;
import java.nio.file.attribute.FileTime;
import java.nio.file.attribute.PosixFileAttributeView;
import java.nio.file.attribute.PosixFileAttributes;
import java.nio.file.attribute.PosixFilePermission;
import java.nio.file.attribute.PosixFilePermissions;
import java.text.DateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import javax.security.auth.Subject;
import org.apache.log4j.Logger;
import org.opencadc.auth.PosixGroup;
import org.opencadc.auth.PosixMapperClient;
import org.opencadc.cavern.PosixIdentityManager;
import org.opencadc.gms.GroupURI;
import org.opencadc.util.fs.AclCommandExecutor;
import org.opencadc.util.fs.ExtendedFileAttributes;

/**
 * Utility methods for interacting with nodes. This is now like a DAO class
 * that is instantiated for short term use so it provides thread safety for the
 * FileSystemNodePersistence methods.
 *
 * @author pdowler
 */
public class NodeUtil {

    private static final Logger log = Logger.getLogger(NodeUtil.class);
    
    // set of node properties that are stored in some special way 
    // and *not* as extended attributes
    private static final Set<String> FILESYSTEM_PROPS = new HashSet<>(
            Arrays.asList(
                    VOS.PROPERTY_URI_AVAILABLESPACE,
                    VOS.PROPERTY_URI_CONTENTLENGTH,
                    VOS.PROPERTY_URI_CREATION_DATE,
                    VOS.PROPERTY_URI_CREATOR,
                    VOS.PROPERTY_URI_DATE,
                    VOS.PROPERTY_URI_GROUPREAD,
                    VOS.PROPERTY_URI_GROUPWRITE,
                    VOS.PROPERTY_URI_ISLOCKED,
                    VOS.PROPERTY_URI_ISPUBLIC,
                    VOS.PROPERTY_URI_QUOTA)
    );

    private final Path root;
    private final PosixMapperClient posixMapper;
    
    private final Map<PosixPrincipal,Subject> identityCache = new TreeMap<>();
    private final PosixIdentityManager identityManager = new PosixIdentityManager();
    
    public NodeUtil(Path root) {
        this.root = root;
        
        LocalAuthority loc = new LocalAuthority();
        // only require a group mapper because IVOA GMS does not include numeric gid
        // assume user mapper is the same service
        URI posixMapperID = loc.getServiceURI(Standards.POSIX_GROUPMAP.toASCIIString());
        this.posixMapper = new PosixMapperClient(posixMapperID);
    }
    
    public PosixPrincipal addToCache(Subject s) {
        if (s == null || s.getPrincipals().isEmpty()) {
            // anon request
            return null;
        }
        
        PosixPrincipal pp = identityManager.toPosixPrincipal(s);
        if (pp == null) {
            throw new RuntimeException("BUG or CONFIG: no PosixPrincipal in subject: " + s);
        }
        identityCache.put(pp, s); // possibly replace old entry
        return pp;
    }
    
    public Subject getFromCache(PosixPrincipal pp) {
        Subject so = identityCache.get(pp);
        if (so == null) {
            so = identityManager.toSubject(pp);
            addToCache(so);
        }
        return so;
    }

    public static Path nodeToPath(Path root, Node node) {
        assertNotNull("node", node);
        return nodeToPath(root, node.getUri());
    }

    public static Path nodeToPath(Path root, VOSURI uri) {
        assertNotNull("root", root);
        assertNotNull("uri", uri);
        log.debug("[nodeToPath] root: " + root + " uri: " + uri);

        String nodePath = uri.getPath();
        if (nodePath.startsWith("/")) {
            nodePath = nodePath.substring(1);
        }
        Path np = root.resolve(nodePath);
        log.debug("[nodeToPath] path: " + uri + " -> " + np);
        return np;
    }

    public static VOSURI pathToURI(Path root, Path p, VOSURI rootURI) {
        Path tp = root.relativize(p);
        return new VOSURI(URI.create(rootURI.getScheme() + "://"
                + rootURI.getAuthority() + "/" + tp.toFile().getPath()));
    }

    public Path create(Node node)
            throws IOException, InterruptedException {
        Path np = nodeToPath(root, node);
        log.debug("[create] path: " + node.getUri() + " -> " + np);

        NodeID nid = (NodeID) node.appData;
        PosixPrincipal owner = (PosixPrincipal) nid.ownerObject;
        log.warn("posix owner: " + owner.getUidNumber() + ":" + owner.defaultGroup);
        assertNotNull("owner", owner);
        Integer group = getDefaultGroup(owner);
        assertNotNull("group", group);

        Path ret = null;
        Set<PosixFilePermission> perms = new HashSet<PosixFilePermission>();
        perms.add(PosixFilePermission.OWNER_READ);
        perms.add(PosixFilePermission.OWNER_WRITE);
        perms.add(PosixFilePermission.GROUP_READ);
        perms.add(PosixFilePermission.GROUP_WRITE);
        // sticky public aka OTHER_READ??
        if (node instanceof ContainerNode) {
            log.debug("[create] dir: " + np);
            perms.add(PosixFilePermission.OWNER_EXECUTE);
            perms.add(PosixFilePermission.GROUP_EXECUTE);
            // sticky public aka OTHER_EXECUTE??
            ret = Files.createDirectory(np, PosixFilePermissions.asFileAttribute(perms));
        } else if (node instanceof DataNode) {
            log.debug("[create] file: " + np);
            ret = Files.createFile(np, PosixFilePermissions.asFileAttribute(perms));
        } else if (node instanceof LinkNode) {
            log.debug("[create] link: " + np);
            LinkNode ln = (LinkNode) node;
            String targPath = ln.getTarget().getPath().substring(1);
            Path absPath = root.resolve(targPath);
            Path rel = np.getParent().relativize(absPath);
            log.debug("[create] link: " + np + "\ntarget: " + targPath
                    + "\nabs: " + absPath + "\nrel: " + rel);
            ret = Files.createSymbolicLink(np, rel);
        } else {
            throw new UnsupportedOperationException(
                    "unexpected node type: " + node.getClass().getName());
        }

        try {
            setPosixOwnerGroup(ret, owner.getUidNumber(), group);
            setNodeProperties(ret, node);
        } catch (IOException ex) {
            log.debug("CREATE FAIL", ex);
            Files.delete(ret);
            throw new UnsupportedOperationException("failed to create " + node.getClass().getSimpleName()
                    + " " + node.getUri(), ex);
        }

        return ret;
    }
    
    public static void setPosixOwnerGroup(Path p, Integer owner, Integer group) throws IOException {
        if (owner == null || group == null) {
            throw new RuntimeException("BUG: owner or default group cannot be null: " + owner + " + " + group);
        }
        PosixFileAttributeView pv = Files.getFileAttributeView(p,
                PosixFileAttributeView.class, LinkOption.NOFOLLOW_LINKS);
        try {
            Files.setAttribute(p, "unix:uid", owner, LinkOption.NOFOLLOW_LINKS);
            Files.setAttribute(p, "unix:gid", group, LinkOption.NOFOLLOW_LINKS);
        } catch (IOException ex) {
            throw new RuntimeException("failed to set owner/group: " + p, ex);
        }
    }

    /**
     * Set the node properties.  This method will use Sets to uniquely identify the ACLs (Read and Read/Write) in
     * order to merge the provided properties and preserve the existing ones that aren't specified.
     * @param path              The path of the storage item.
     * @param node              The VOSpace Node input.
     * @throws IOException          If any I/O errors occur.
     */
    public void setNodeProperties(Path path, Node node) throws IOException, InterruptedException {
        log.debug("setNodeProperties: " + node);
        if (!node.getProperties().isEmpty() && !(node instanceof LinkNode)) {
            
            for (NodeProperty prop : node.getProperties()) {
                if (!FILESYSTEM_PROPS.contains(prop.getPropertyURI())) {
                    if (prop.isMarkedForDeletion()) {
                        ExtendedFileAttributes.setFileAttribute(path, prop.getPropertyURI(), null);
                    } else {
                        ExtendedFileAttributes.setFileAttribute(path, prop.getPropertyURI(), prop.getPropertyValue());
                    }
                }
            }
            
            // group permissions
            LocalAuthority loc = new LocalAuthority();
            URI localGMS = loc.getServiceURI(Standards.GMS_SEARCH_10.toASCIIString());
            boolean isDir = (node instanceof ContainerNode);
            AclCommandExecutor acl = new AclCommandExecutor(path);

            final Set<Integer> readGroupPrincipals = new HashSet<>(acl.getReadOnlyACL(isDir));
            NodeProperty rop = node.findProperty(VOS.PROPERTY_URI_GROUPREAD);
            if (rop != null) {
                if (rop.isMarkedForDeletion()) {
                    readGroupPrincipals.clear();
                } else {
                    // ugh: raw multi-valued node prop is space-separated
                    String val = rop.getPropertyValue();
                    log.warn("raw read-only prop: " + val);
                    if (val != null) {
                        String[] vals = val.split(" ");
                        if (vals.length > 0) {
                            readGroupPrincipals.clear();
                            List<GroupURI> guris = new ArrayList<>();
                            for (String sro : vals) {
                                GroupURI guri = new GroupURI(URI.create(sro));
                                guris.add(guri);
                            }
                            if (!guris.isEmpty()) {
                                try {
                                    List<PosixGroup> pgs = posixMapper.getGID(guris);
                                    // TODO: check if any input groups were not resolved/acceptable and do what??
                                    for (PosixGroup pg : pgs) {
                                        readGroupPrincipals.add(pg.getGID());
                                    }
                                } catch (ResourceNotFoundException | ResourceAlreadyExistsException ex) {
                                    throw new RuntimeException("failed to map GroupURI(s) to numeric GID(s): "
                                            + ex.toString(), ex);
                                }
                            }
                        } else {
                            log.warn("oops: no groups in " + VOS.PROPERTY_URI_GROUPREAD + " value");
                        }
                    } else {
                        log.warn("oops: no property value in " + VOS.PROPERTY_URI_GROUPREAD + " but !markForDeletion");
                    }
                }
            }

            final Set<Integer> writeGroupPrincipals = new HashSet<>(acl.getReadWriteACL(isDir));
            NodeProperty rwp = node.findProperty(VOS.PROPERTY_URI_GROUPWRITE);
            if (rwp != null) {
                if (rwp.isMarkedForDeletion()) {
                    writeGroupPrincipals.clear();
                } else {
                    // ugh: raw multi-valued node prop is space-separated
                    String val = rwp.getPropertyValue();
                    log.warn("raw read-write prop: " + val);
                    if (val != null) {
                        String[] vals = val.split(" ");
                        if (vals.length > 0) {
                            writeGroupPrincipals.clear();
                            List<GroupURI> guris = new ArrayList<>();
                            for (String sro : vals) {
                                GroupURI guri = new GroupURI(URI.create(sro));
                                guris.add(guri);
                            }
                            if (!guris.isEmpty()) {
                                try {
                                    List<PosixGroup> pgs = posixMapper.getGID(guris);
                                    // TODO: check if any input groups were not resolved/acceptable and do what??
                                    for (PosixGroup pg : pgs) {
                                        writeGroupPrincipals.add(pg.getGID());
                                    }
                                } catch (ResourceNotFoundException | ResourceAlreadyExistsException ex) {
                                    throw new RuntimeException("failed to map GroupURI(s) to numeric GID(s): "
                                            + ex.toString(), ex);
                                }
                            }
                        } else {
                            log.warn("oops: no groups in " + VOS.PROPERTY_URI_GROUPWRITE + " value");
                        }
                    } else {
                        log.warn("oops: no property value in " + VOS.PROPERTY_URI_GROUPWRITE + " but !markForDeletion");
                    }
                }
            }
            
            // this if probably makes it impossible to remove grants
            //maybe: if (rop != null || rwp != null) {
            if (!readGroupPrincipals.isEmpty() || !writeGroupPrincipals.isEmpty()) {
                log.debug("set read groups: " + readGroupPrincipals);
                log.debug("set write groups: " + writeGroupPrincipals);
                acl.setACL(readGroupPrincipals, writeGroupPrincipals, isDir);
                log.debug("Setting ACLs: OK");
            }
            
            // public aka world-readable flag
            // HACK: this is located after set group permisisons because the ACL code wipes out
            // the current world-readable aka public
            // INCOMPLETE: this does not correctly handle the case where the update did not
            // mention "public" at all and ends up setting it to false (Node.isPublic doesn't
            // handle null correctly for updates)
            PosixFileAttributeView pv = Files.getFileAttributeView(path,
                PosixFileAttributeView.class, LinkOption.NOFOLLOW_LINKS);
            Set<PosixFilePermission> perms = pv.readAttributes().permissions(); // current perms
            if (node.isPublic()) { 
                // set
                if (!perms.contains(PosixFilePermission.OTHERS_READ)) {
                    perms.add(PosixFilePermission.OTHERS_READ);
                    if (node instanceof ContainerNode) {
                        perms.add(PosixFilePermission.OTHERS_EXECUTE);
                    }
                    pv.setPermissions(perms);
                }
            } else {
                // unset
                if (perms.contains(PosixFilePermission.OTHERS_READ)) {
                    perms.remove(PosixFilePermission.OTHERS_READ);
                    if (node instanceof ContainerNode) {
                        perms.remove(PosixFilePermission.OTHERS_EXECUTE);
                    }
                    pv.setPermissions(perms);
                }
            }
        }
    }

    public Path update(Path root, Node node) throws IOException {
        Path np = nodeToPath(root, node);
        log.debug("[update] path: " + node.getUri() + " -> " + np);
        throw new UnsupportedOperationException();
    }

    public Node get(VOSURI uri)  throws IOException, InterruptedException {
        return get(uri, false);
    }
    
    public Node get(VOSURI uri, boolean allowPartialPath) 
            throws IOException, InterruptedException {
        LinkedList<String> nodeNames = new LinkedList<String>();
        nodeNames.add(uri.getName());
        VOSURI parent = uri.getParentURI();
        VOSURI rootURI = uri;
        while (parent != null) {
            if (parent.isRoot()) {
                rootURI = parent;
            } else {
                nodeNames.add(parent.getName());
            }
            parent = parent.getParentURI();
        }
        log.debug("[get] path components: " + nodeNames.size());

        ContainerNode cn = null;
        Iterator<String> iter = nodeNames.descendingIterator();
        Path cur = root;
        StringBuilder sb = new StringBuilder(rootURI.getURI().toASCIIString());
        Node ret = cn;
        while (iter.hasNext()) {
            String pathComp = iter.next();
            Path p = cur.resolve(pathComp);
            cur = p;
            sb.append("/").append(pathComp); // for next loop
            log.debug("[get-walk] " + sb.toString() + " " + allowPartialPath);
            try {
                Node tmp = pathToNode(p, rootURI);
                if (cn == null) {
                    if (tmp instanceof ContainerNode) {
                        cn = (ContainerNode) tmp; // top-level dir
                    }
                } else {
                    cn.getNodes().add(tmp);
                    tmp.setParent(cn);
                    if (tmp instanceof ContainerNode) {
                        cn = (ContainerNode) tmp;
                    }
                }
                if (tmp instanceof LinkNode) {
                    if (allowPartialPath) {
                        return tmp;
                    } else if (uri.equals(tmp.getUri())) {
                        return tmp;
                    } else {
                        ret = null;
                        break;
                    }
                }
                ret = tmp;
            } catch (NoSuchFileException ex) {
                return null;
            }
        }
        log.debug("[get] returning " + ret);
        return ret;
    }

    public void move(VOSURI source, VOSURI destDir, String destName, PosixPrincipal owner) throws IOException {
        Path sourcePath = nodeToPath(root, source);
        VOSURI destWithName = new VOSURI(URI.create(destDir.toString() + "/" + destName));
        Path destPath = nodeToPath(root, destWithName);
        Files.move(sourcePath, destPath, StandardCopyOption.ATOMIC_MOVE);

        Integer group = getDefaultGroup(owner);
        assertNotNull("group", group);
        setPosixOwnerGroup(destPath, owner.getUidNumber(), group);
    }

    public void copy(VOSURI source, VOSURI destDir, PosixPrincipal owner) throws IOException {
        Integer group = getDefaultGroup(owner);
        assertNotNull("group", group);

        Path sourcePath = nodeToPath(root, source);
        VOSURI destWithName = new VOSURI(URI.create(destDir.toString() + "/" + source.getName()));
        Path destPath = nodeToPath(root, destWithName);
        // follow links
        if (Files.isDirectory(sourcePath)) {
            Files.walkFileTree(sourcePath, new CopyVisitor(root, sourcePath, destPath, owner, group));
        } else { // links and files
            Files.copy(sourcePath, destPath, StandardCopyOption.COPY_ATTRIBUTES);
            setPosixOwnerGroup(destPath, owner.getUidNumber(), group);
            Files.setLastModifiedTime(destPath, FileTime.fromMillis(System.currentTimeMillis()));
        }
    }

    Node pathToNode(Path p, VOSURI rootURI)
            throws IOException, InterruptedException, NoSuchFileException {
        boolean getAttrs = System.getProperty(NodeUtil.class.getName() + ".disable-get-attrs") == null;
        return pathToNode(p, rootURI, getAttrs);
    }
    
    // getAttrs == false needed in MountedContainerTest
    Node pathToNode(Path p, VOSURI rootURI, boolean getAttrs)
            throws IOException, InterruptedException, NoSuchFileException {
        Node ret = null;
        VOSURI nuri = pathToURI(root, p, rootURI);
        PosixFileAttributes attrs = Files.readAttributes(p,
                PosixFileAttributes.class, LinkOption.NOFOLLOW_LINKS);
        if (attrs.isDirectory()) {
            ret = new ContainerNode(nuri);
        } else if (attrs.isRegularFile()) {
            ret = new DataNode(nuri);
            // restore file-specific properties -- this is old and no longer know what it means
        } else if (attrs.isSymbolicLink()) {
            Path tp = Files.readSymbolicLink(p);
            Path abs = p.getParent().resolve(tp);
            Path rel = root.relativize(abs);
            URI turi = URI.create(rootURI.getScheme() + "://"
                    + rootURI.getAuthority() + "/" + rel.toString());
            log.debug("[pathToNode] link: " + p + "\ntarget: " + tp + "\nabs: "
                    + abs + "\nrel: " + rel + "\nuri: " + turi);
            ret = new LinkNode(nuri, turi);
        } else {
            throw new IllegalStateException(
                    "found unexpected file system object: " + p);
        }

        // note: the above PosixFileAttributes attrs.owner() returns the numeric uid *iff* the system
        // cannot resolve it to a name - correct use would depend on how the system was configured
        // this does not depend on external system config:
        Integer owner = (Integer) Files.getAttribute(p, "unix:uid", LinkOption.NOFOLLOW_LINKS);
        PosixPrincipal op = new PosixPrincipal(owner);
        Subject osub = new Subject(false, new TreeSet<>(), new TreeSet<>(), new TreeSet<>());
        osub.getPrincipals().add(op);
        // NodePersistence will reconstruct full subject - appData has to work with getOwner(Node) below
        ret.appData = new NodeID(null, osub, op);
        
        DateFormat df = DateUtil.getDateFormat(DateUtil.IVOA_DATE_FORMAT, DateUtil.UTC);
        //Date created = new Date(attrs.creationTime().toMillis());
        //Date accessed = new Date(attrs.lastAccessTime().toMillis());
        Date modified = new Date(attrs.lastModifiedTime().toMillis());
        //ret.getProperties().add(new
        //    NodeProperty(VOS.PROPERTY_URI_CREATION_DATE, df.format(created)));
        //ret.getProperties().add(new
        //    NodeProperty(VOS.PROPERTY_URI_ACCESS_DATE, df.format(accessed)));
        //ret.getProperties().add(new
        //    NodeProperty(VOS.PROPERTY_URI_MODIFIED_DATE, df.format(modified)));
        ret.getProperties().add(new NodeProperty(VOS.PROPERTY_URI_DATE,
                df.format(modified)));
        
        if (attrs.isRegularFile()) {
            ret.getProperties().add(new NodeProperty(VOS.PROPERTY_URI_CONTENTLENGTH, Long.toString(attrs.size())));
        }

        if (getAttrs && !attrs.isSymbolicLink()) {
            
            Map<String,String> uda = ExtendedFileAttributes.getAttributes(p);
            for (Map.Entry<String,String> me : uda.entrySet()) {
                ret.getProperties().add(new NodeProperty(me.getKey(), me.getValue()));
            }
            
            AclCommandExecutor acl = new AclCommandExecutor(p);
            StringBuilder sb = new StringBuilder();
            List<Integer> rogids = acl.getReadOnlyACL(attrs.isDirectory());
            if (!rogids.isEmpty()) {
                try {
                    List<PosixGroup> pgs = posixMapper.getURI(rogids);
                    for (PosixGroup pg : pgs) {
                        sb.append(pg.getGroupURI().getURI().toASCIIString()).append(" ");
                    }
                } catch (ResourceNotFoundException | ResourceAlreadyExistsException ex) {
                    throw new RuntimeException("failed to map numeric GID(s) to GroupURI(s): " + ex.toString(), ex);
                }
            }
            sb.trimToSize();
            String sval = sb.toString().trim();
            if (sval.length() > 0) {
                ret.getProperties().add(new NodeProperty(VOS.PROPERTY_URI_GROUPREAD, sval));
            }
            sb.setLength(0);
            
            List<Integer> rwgids = acl.getReadWriteACL(attrs.isDirectory());
            if (!rwgids.isEmpty()) {
                try {
                    List<PosixGroup> pgs = posixMapper.getURI(rwgids);
                    for (PosixGroup pg : pgs) {
                        sb.append(pg.getGroupURI().getURI().toASCIIString()).append(" ");
                    }
                } catch (ResourceNotFoundException | ResourceAlreadyExistsException ex) {
                    throw new RuntimeException("failed to map numeric GID(s) to GroupURI(s): " + ex.toString(), ex);
                }
            }
            sb.trimToSize();
            sval = sb.toString().trim();
            if (sval.length() > 0) {
                ret.getProperties().add(new NodeProperty(VOS.PROPERTY_URI_GROUPWRITE, sval));
            }
            String mask = acl.getMask();
            if (mask != null) {
                ret.getProperties().add(new NodeProperty(VOS.PROPERTY_URI_GROUPMASK, mask));
            }
        }

        NodeProperty publicProp = new NodeProperty(VOS.PROPERTY_URI_ISPUBLIC, Boolean.toString(false));
        ret.getProperties().add(publicProp);
        for (PosixFilePermission pfp : attrs.permissions()) {
            log.debug("posix perm: " + pfp);
            if (PosixFilePermission.OTHERS_READ.equals(pfp)) {
                publicProp.setValue(Boolean.toString(true));
            }
        }
        return ret;
    }

    public static void delete(Path root, VOSURI uri) throws IOException {
        Path np = nodeToPath(root, uri);
        log.debug("[create] path: " + uri + " -> " + np);
        delete(np);
    }

    private static void delete(Path path) throws IOException {
        if (Files.isDirectory(path, LinkOption.NOFOLLOW_LINKS)) {
            Files.walkFileTree(path, new DeleteVisitor());
        } else {
            Files.delete(path);
        }
    }

    private static class DeleteVisitor implements FileVisitor<Path> {

        @Override
        public FileVisitResult preVisitDirectory(Path t,
                BasicFileAttributes bfa) throws IOException {
            log.debug("enter: " + t);
            return FileVisitResult.CONTINUE;
        }

        @Override
        public FileVisitResult visitFile(Path t, BasicFileAttributes bfa)
                throws IOException {
            log.debug("delete: " + t);
            Files.delete(t);
            return FileVisitResult.CONTINUE;
        }

        @Override
        public FileVisitResult visitFileFailed(Path t, IOException ioe)
                throws IOException {
            return FileVisitResult.CONTINUE;
        }

        @Override
        public FileVisitResult postVisitDirectory(Path t, IOException ioe)
                throws IOException {
            Files.delete(t);
            log.debug("delete: " + t);
            return FileVisitResult.CONTINUE;
        }
    }

    private static class CopyVisitor implements FileVisitor<Path> {

        PosixPrincipal owner;
        Integer group;
        Path root;
        Path destDir;
        Path sourceDir;

        // TODO: alt ctor without group to preserve group during copy?
        CopyVisitor(Path root, Path source, Path dest, PosixPrincipal owner, Integer group) {
            this.root = root;
            this.destDir = dest;
            this.owner = owner;
            this.group = group;
        }

        @Override
        public FileVisitResult preVisitDirectory(Path t,
                BasicFileAttributes bfa) throws IOException {
            log.debug("copy: pre-visit directory: " + t);
            if (sourceDir == null) {
                sourceDir = t;
            }
            Path dir = destDir.resolve(sourceDir.relativize(t));
            log.debug("Creating: " + dir);
            Files.createDirectories(dir);
            NodeUtil.setPosixOwnerGroup(dir, owner.getUidNumber(), group);
            Files.setLastModifiedTime(dir, FileTime.fromMillis(System.currentTimeMillis()));
            return FileVisitResult.CONTINUE;
        }

        @Override
        public FileVisitResult visitFile(Path t, BasicFileAttributes bfa)
                throws IOException {
            log.debug("copy: visit file: " + t);
            Path file = destDir.resolve(sourceDir.relativize(t));
            log.debug("creating: " + file);
            Files.copy(t, file);
            NodeUtil.setPosixOwnerGroup(file, owner.getUidNumber(), group);
            Files.setLastModifiedTime(file, FileTime.fromMillis(System.currentTimeMillis()));
            return FileVisitResult.CONTINUE;
        }

        @Override
        public FileVisitResult visitFileFailed(Path t, IOException ioe)
                throws IOException {
            return FileVisitResult.CONTINUE;
        }

        @Override
        public FileVisitResult postVisitDirectory(Path t, IOException ioe)
                throws IOException {
            return FileVisitResult.CONTINUE;
        }
    }

    public Iterator<Node> list(ContainerNode node, VOSURI start, Integer limit) 
            throws IOException, InterruptedException {
        Path np = nodeToPath(root, node);
        log.debug("[list] " + node.getUri() + " -> " + np);
        VOSURI rootURI = node.getUri();
        while (!rootURI.isRoot()) {
            rootURI = rootURI.getParentURI();
        }
        log.debug("[list] root: " + rootURI + " -> " + root);
        // TODO: rewrite this to not instantiate a list of children and just stream
        List<Node> nodes = new ArrayList<Node>();
        if (limit == null || limit > 0) {
            try (DirectoryStream<Path> stream = Files.newDirectoryStream(np)) {
                for (Path file : stream) {
                    log.debug("[list] visit: " + file);
                    Node n = pathToNode(file, rootURI);
                    if (!nodes.isEmpty() || start == null
                            || start.getName().equals(n.getName())) {
                        nodes.add(n);
                    }
                    
                    if (limit != null && limit == nodes.size()) {
                        break;
                    }
                }
            }
        }
        log.debug("[list] found: " + nodes.size());
        return nodes.iterator();
    }

    // currently unused visitor with the minimal setup to call pathToNode
    private static class DirectoryVisitor implements FileVisitor<Path> {

        private final Path root;
        private final VOSURI rootURI;

        List<Node> nodes = new ArrayList<Node>();

        public DirectoryVisitor(Path root, VOSURI rootURI) {
            this.root = root;
            this.rootURI = rootURI;
        }

        @Override
        public FileVisitResult preVisitDirectory(Path t,
                BasicFileAttributes bfa) throws IOException {
            log.debug("[preVisitDirectory] " + t);
            return FileVisitResult.CONTINUE;
        }

        @Override
        public FileVisitResult visitFile(Path t, BasicFileAttributes bfa)
                throws IOException {
            log.debug("[visitFile] " + t);
            // Node n = pathToNode(root, t, rootURI);
            return FileVisitResult.CONTINUE;
        }

        @Override
        public FileVisitResult visitFileFailed(Path t, IOException ioe)
                throws IOException {
            log.debug("[list] visitFileFailed: " + t + " reason: " + ioe);
            return FileVisitResult.CONTINUE;
        }

        @Override
        public FileVisitResult postVisitDirectory(Path t, IOException ioe)
                throws IOException {
            log.debug("[postVisitDirectory] " + t);
            return FileVisitResult.CONTINUE;
        }

    }

    public static void assertNotNull(String name, Object o) {
        if (o == null) {
            throw new IllegalArgumentException(name + " cannot be null");
        }
    }
    
    public static void setOwner(Node node, NodeID ownerData) {
        node.appData = ownerData;
    }
    
    public static PosixPrincipal getOwner(Node node) throws IOException {
        if (node.appData != null) {
            NodeID nid = (NodeID) node.appData;
            Set<PosixPrincipal> ps = nid.owner.getPrincipals(PosixPrincipal.class);
            if (!ps.isEmpty()) {
                return ps.iterator().next();
            }
        }
        return null;
    }

    // temporarily assume default GID == UID
    public static Integer getDefaultGroup(PosixPrincipal user) throws IOException {
        // TODO: use PosixMapperClient directly to get default group??
        if (user.defaultGroup != null) {
            return user.defaultGroup;
        }
        throw new RuntimeException("CONFIG or BUG: posix principal default group is null");
    }
}
