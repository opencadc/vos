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
import ca.nrc.cadc.io.ResourceIterator;
import ca.nrc.cadc.net.ResourceAlreadyExistsException;
import ca.nrc.cadc.net.ResourceNotFoundException;
import ca.nrc.cadc.reg.Standards;
import ca.nrc.cadc.reg.client.LocalAuthority;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
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
import java.util.Arrays;
import java.util.Date;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Set;
import java.util.TreeSet;
import javax.security.auth.Subject;
import org.apache.log4j.Logger;
import org.opencadc.auth.PosixGroup;
import org.opencadc.util.fs.AclCommandExecutor;
import org.opencadc.util.fs.ExtendedFileAttributes;
import org.opencadc.vospace.ContainerNode;
import org.opencadc.vospace.DataNode;
import org.opencadc.vospace.LinkNode;
import org.opencadc.vospace.Node;
import org.opencadc.vospace.NodeProperty;
import org.opencadc.vospace.VOS;
import org.opencadc.vospace.VOSURI;

/**
 * Utility methods for interacting with nodes. This is now like a DAO class
 * that is instantiated for short term use so it provides thread safety for the
 * FileSystemNodePersistence methods.
 *
 * @author pdowler
 */
class NodeUtil {

    private static final Logger log = Logger.getLogger(NodeUtil.class);
    
    // set of node properties that are stored in some special way 
    // and *not* as extended attributes
    private static final Set<URI> FILESYSTEM_PROPS = new HashSet<>(
        Arrays.asList(
            VOS.PROPERTY_URI_AVAILABLESPACE,
            VOS.PROPERTY_URI_CONTENTLENGTH,
            VOS.PROPERTY_URI_CONTENTDATE,
            VOS.PROPERTY_URI_CREATOR,
            VOS.PROPERTY_URI_DATE,
            VOS.PROPERTY_URI_GROUPREAD,
            VOS.PROPERTY_URI_GROUPWRITE,
            VOS.PROPERTY_URI_ISLOCKED, // but not supported
            VOS.PROPERTY_URI_ISPUBLIC
        )
    );

    private final Path root;
    private final VOSURI rootURI;
    private final GroupCache groupCache;
    
    public NodeUtil(Path root, VOSURI rootURI, GroupCache groupCache) {
        this.root = root;
        this.rootURI = rootURI;
        this.groupCache = groupCache;
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

    public VOSURI pathToURI(Path root, Path p) {
        Path tp = root.relativize(p);
        return new VOSURI(URI.create(rootURI.getScheme() + "://"
                + rootURI.getAuthority() + "/" + tp.toFile().getPath()));
    }

    // put node, create if necessary
    public void put(Node node, VOSURI uri)
            throws IOException, InterruptedException {
        Path np = nodeToPath(root, uri);
        log.debug("[put] path: " + node + " -> " + np);

        PosixPrincipal owner = (PosixPrincipal) node.ownerID;
        log.debug("posix owner: " + owner.getUidNumber() + ":" + owner.defaultGroup);
        assertNotNull("owner", owner);
        Integer group = getDefaultGroup(owner);
        assertNotNull("group", group);

        Path ret = null;
        Set<PosixFilePermission> perms = new HashSet<PosixFilePermission>();
        perms.add(PosixFilePermission.OWNER_READ);
        perms.add(PosixFilePermission.OWNER_WRITE);
        perms.add(PosixFilePermission.GROUP_READ);
        perms.add(PosixFilePermission.GROUP_WRITE);
        if (!Files.exists(np, LinkOption.NOFOLLOW_LINKS)) {
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
        } else {
            ret = np;
        }

        try {
            log.debug("[put] set owner: " + owner.getUidNumber() + ":" + group);
            setPosixOwnerGroup(ret, owner.getUidNumber(), group);
            setNodeProperties(ret, node);
        } catch (IOException ex) {
            log.debug("CREATE FAIL", ex);
            Files.delete(ret);
            throw new UnsupportedOperationException("failed to put " + node.getClass().getSimpleName()
                    + " " + node.getName(), ex);
        }
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

    private void setNodeProperties(Path path, Node node) throws IOException, InterruptedException {
        log.debug("setNodeProperties: " + node);
        // update action in library handles the merge
        // clear existing
        Map<String,String> cur = ExtendedFileAttributes.getAttributes(path);
        for (Map.Entry<String,String> me : cur.entrySet()) {
            ExtendedFileAttributes.setFileAttribute(path, me.getKey(), null);
        }
        for (NodeProperty prop : node.getProperties()) {
            if (!FILESYSTEM_PROPS.contains(prop.getKey())) {
                if (node instanceof LinkNode) {
                    throw new IllegalArgumentException("cannot assign properties to LinkNode");
                }
                ExtendedFileAttributes.setFileAttribute(path, prop.getKey().toASCIIString(), prop.getValue());
            }
        }

        final boolean isDir = (node instanceof ContainerNode);
        boolean inherit = false;
        if (isDir) {
            ContainerNode cn = (ContainerNode) node;
            if (cn.inheritPermissions != null) {
                // set
                String val = cn.inheritPermissions.toString();
                ExtendedFileAttributes.setFileAttribute(path, VOS.PROPERTY_URI_INHERIT_PERMISSIONS.toASCIIString(), val);
                inherit = cn.inheritPermissions;
            } else if (cn.clearInheritPermissions) {
                ExtendedFileAttributes.setFileAttribute(path, VOS.PROPERTY_URI_INHERIT_PERMISSIONS.toASCIIString(), null);
            }
        }

        final AclCommandExecutor acl = new AclCommandExecutor(path, isDir);

        // important: the calling library is responsible for merging changes into the current
        // state of the node, so this is now just setting what is supplied with minor optimization
        // to avoid unecessary exec

        final Set<Integer> roGIDs = new TreeSet<>();
        try {
            List<PosixGroup> pgs = groupCache.getFromGroupCache(node.getReadOnlyGroup());
            // TODO: check if any input groups were not resolved/acceptable and do what??
            for (PosixGroup pg : pgs) {
                roGIDs.add(pg.getGID());
            }
        } catch (ResourceNotFoundException | ResourceAlreadyExistsException ex) {
            throw new RuntimeException("failed to map GroupURI(s) to numeric GID(s): " + ex.toString(), ex);
        }
        Set<Integer> curGID = acl.getReadOnlyACL();
        log.debug("cur ro: " + curGID);
        final boolean changeRO = !roGIDs.equals(curGID);

        final Set<Integer> rwGIDs = new TreeSet<>();
        try {
            List<PosixGroup> pgs = groupCache.getFromGroupCache(node.getReadWriteGroup());
            // TODO: check if any input groups were not resolved/acceptable and do what??
            for (PosixGroup pg : pgs) {
                rwGIDs.add(pg.getGID());
            }
        } catch (ResourceNotFoundException | ResourceAlreadyExistsException ex) {
            throw new RuntimeException("failed to map GroupURI(s) to numeric GID(s): "
                    + ex.toString(), ex);
        }

        curGID = acl.getReadWriteACL();
        log.debug("cur rw: " + curGID);
        boolean changeRW = !rwGIDs.equals(curGID);

        boolean changePublic = false;
        boolean worldReadable = false;
        PosixFileAttributeView pv = Files.getFileAttributeView(path,
            PosixFileAttributeView.class, LinkOption.NOFOLLOW_LINKS);
        Set<PosixFilePermission> perms = pv.readAttributes().permissions(); // current perms
        if (node.isPublic != null && node.isPublic) {
            worldReadable = true;
            if (!perms.contains(PosixFilePermission.OTHERS_READ)) {
                changePublic = true;
            }
        } else if (node.clearIsPublic || (node.isPublic != null && !node.isPublic)) {
            worldReadable = false;
            if (perms.contains(PosixFilePermission.OTHERS_READ)) {
                changePublic = true;
            }
        } else {
            // null -> unchanged
            worldReadable = perms.contains(PosixFilePermission.OTHERS_READ);
            changePublic = false;
        }

        if (changeRO || changeRW || changePublic) {
            log.debug("set ACL: public=" + worldReadable + " ro=" + roGIDs + " rw=" + rwGIDs);
            acl.setACL(worldReadable, roGIDs, rwGIDs);
        }

        if (isDir) {
            if (inherit) {
                log.debug("set default ACL: public=" + worldReadable + " ro=" + roGIDs + " rw=" + rwGIDs);
                acl.setACL(worldReadable, roGIDs, rwGIDs, true);
            } else if (!inherit) {
                log.debug("clear default ACL: public=" + worldReadable);
                acl.clearDefaultACL();
            } // else: leave it alone
        }

        log.debug("final ro: " + acl.getReadOnlyACL());
        log.debug("final rw: " + acl.getReadWriteACL());
    }

    public Node get(ContainerNode parent, String name) 
            throws IOException, InterruptedException {
        // resolve parent path from root
        LinkedList<String> nodeNames = new LinkedList<>();
        ContainerNode cur = parent;
        while (cur != null) {
            if (cur.parent != null) {
                // not root
                nodeNames.add(cur.getName());
            }
            cur = cur.parent;
        }
        Path parentPath = root;
        Iterator<String> comp = nodeNames.descendingIterator();
        while (comp.hasNext()) {
            String n = comp.next();
            parentPath = parentPath.resolve(n);
        }
        
        // TODO: above would have been easier if we could store Path in a previously returned Node
        
        Path np = parentPath.resolve(name);
        if (np == null) {
            return null;
        }
        
        return pathToNode(np);
    }
    
    // move without rename
    public void move(VOSURI source, VOSURI destDir, PosixPrincipal owner, String destName) throws IOException {
        Path sourcePath = nodeToPath(root, source);
        if (destName == null) {
            destName = source.getName(); // no rename
        }
        VOSURI destWithName = new VOSURI(URI.create(destDir.toString() + "/" + destName));
        Path destPath = nodeToPath(root, destWithName);
        log.warn("atomic move: " + sourcePath + " -> " + destPath);
        Files.move(sourcePath, destPath, StandardCopyOption.ATOMIC_MOVE);

        Integer group = getDefaultGroup(owner);
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

    Node pathToNode(Path p)
            throws IOException, InterruptedException, NoSuchFileException {
        boolean getAttrs = System.getProperty(NodeUtil.class.getName() + ".disable-get-attrs") == null;
        return pathToNode(p, getAttrs);
    }
    
    // getAttrs == false needed in MountedContainerTest
    Node pathToNode(Path p, boolean getAttrs)
            throws IOException, InterruptedException, NoSuchFileException {
        Node ret = null;
        if (!Files.exists(p, LinkOption.NOFOLLOW_LINKS)) {
            return null;
        }
        
        //VOSURI nuri = pathToURI(root, p, rootURI);
        PosixFileAttributes attrs = Files.readAttributes(p,
                PosixFileAttributes.class, LinkOption.NOFOLLOW_LINKS);
        if (attrs.isDirectory()) {
            ret = new ContainerNode(p.getFileName().toString());
        } else if (attrs.isRegularFile()) {
            ret = new DataNode(p.getFileName().toString());
            // restore file-specific properties -- this is old and no longer know what it means
        } else if (attrs.isSymbolicLink()) {
            Path tp = Files.readSymbolicLink(p);
            Path abs = p.getParent().resolve(tp);
            Path rel = root.relativize(abs);
            URI turi = URI.create(rootURI.getScheme() + "://"
                    + rootURI.getAuthority() + "/" + rel.toString());
            log.debug("[pathToNode] link: " + p + "\ntarget: " + tp + "\nabs: "
                    + abs + "\nrel: " + rel + "\nuri: " + turi);
            ret = new LinkNode(p.getFileName().toString(), turi);
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
        // NodePersistence will reconstruct full subject
        ret.ownerID = op;
        
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
        ret.getProperties().add(new NodeProperty(VOS.PROPERTY_URI_DATE, df.format(modified)));
        if (attrs.isRegularFile()) {
            ret.getProperties().add(new NodeProperty(VOS.PROPERTY_URI_CONTENTLENGTH, Long.toString(attrs.size())));
        }

        if (getAttrs && !attrs.isSymbolicLink()) {
            Map<String,String> uda = ExtendedFileAttributes.getAttributes(p);
            for (Map.Entry<String,String> me : uda.entrySet()) {
                try {
                    URI pk = new URI(me.getKey());
                    log.debug("found prop: " + pk + " = " + me.getValue());
                    if (VOS.PROPERTY_URI_INHERIT_PERMISSIONS.equals(pk)) {
                        if (ret instanceof ContainerNode) {
                            ContainerNode cn = (ContainerNode) ret;
                            cn.inheritPermissions = Boolean.parseBoolean(me.getValue());
                        } else {
                            log.error("found " + VOS.PROPERTY_URI_INHERIT_PERMISSIONS + " on a " + ret.getClass().getSimpleName());
                        }
                    } else {
                        ret.getProperties().add(new NodeProperty(pk, me.getValue()));
                    }
                } catch (URISyntaxException ex) {
                    // users can set attrs in a mounted filesystem so this obviously is not
                    // a great solution to this fail
                    throw new RuntimeException("BUG: invalid attribute key: " + me.getKey(), ex);
                }
            }
            
            boolean isDir = (ret instanceof ContainerNode);
            AclCommandExecutor acl = new AclCommandExecutor(p, isDir);
            
            // TODO: could collect all gids from read-only and read-write and prime the gid cache in 1 call instead of 2
            Set<Integer> rogids = acl.getReadOnlyACL();
            if (!rogids.isEmpty()) {
                try {
                    List<PosixGroup> pgs = groupCache.getFromGidCache(rogids);
                    log.debug("\tmapped ro: " + rogids.size() + " gid -> " + pgs.size() + " PosixGroup");
                    for (PosixGroup pg : pgs) {
                        log.debug("\tro: " + pg.getGID() + " aka " + pg.getGroupURI());
                        ret.getReadOnlyGroup().add(pg.getGroupURI());
                    }
                } catch (ResourceNotFoundException | ResourceAlreadyExistsException ex) {
                    throw new RuntimeException("failed to map numeric GID(s) to GroupURI(s): " + ex.toString(), ex);
                }
            }
            
            Set<Integer> rwgids = acl.getReadWriteACL();
            if (!rwgids.isEmpty()) {
                try {
                    List<PosixGroup> pgs = groupCache.getFromGidCache(rwgids);
                    log.debug("\tmapped rw: " + rwgids.size() + " gid -> " + pgs.size() + " PosixGroup");
                    for (PosixGroup pg : pgs) {
                        log.debug("\trw: " + pg.getGID() + " aka " + pg.getGroupURI());
                        ret.getReadWriteGroup().add(pg.getGroupURI());
                    }
                } catch (ResourceNotFoundException | ResourceAlreadyExistsException ex) {
                    throw new RuntimeException("failed to map numeric GID(s) to GroupURI(s): " + ex.toString(), ex);
                }
            }
        }

        ret.isPublic = false;
        for (PosixFilePermission pfp : attrs.permissions()) {
            log.debug("posix perm: " + pfp);
            if (PosixFilePermission.OTHERS_READ.equals(pfp)) {
                ret.isPublic = true;
            }
        }
        
        return ret;
    }

    public void delete(VOSURI uri) throws IOException {
        Path p = nodeToPath(root, uri);
        Files.delete(p);
    }
    
    /* 
    // magic recursive delete is obsolete
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
    */
    
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

    public ResourceIterator<Node> list(VOSURI vu) throws IOException {
        Path np = nodeToPath(root, vu);
        log.debug("[list] " + vu.getPath() + " -> " + np);
        return new NodeIterator(np);
    }

    private class NodeIterator implements ResourceIterator<Node> {
        DirectoryStream<Path> stream;
        private final Iterator<Path> content;
        
        NodeIterator(Path path) throws IOException {
            this.stream = Files.newDirectoryStream(path);
            this.content = stream.iterator();
        }
        
        @Override
        public boolean hasNext() {
            return content.hasNext();
        }

        @Override
        public Node next() {
            Path cur = content.next();
            try {
                Node ret = pathToNode(cur);
                return ret;
            } catch (IOException | InterruptedException ex) {
                throw new RuntimeException("container node listing failed", ex);
            }
        }

        @Override
        public void close() throws IOException {
            stream.close();
        }
        
    }

    // currently unused visitor with the minimal setup to call pathToNode
    private class DirectoryVisitor implements FileVisitor<Path> {

        public DirectoryVisitor() {
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
            //Node n = pathToNode(t);
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
    
    //public static void setOwner(Node node, Subject s) {
    //    node.owner = s;
    //}
    
    public static PosixPrincipal getOwner(Node node) {
        if (node.ownerID != null) {
            return (PosixPrincipal) node.ownerID;
        }
        // TODO: throw??
        return null;
    }

    // temporarily assume default GID == UID
    public static Integer getDefaultGroup(PosixPrincipal user) throws IOException {
        if (user.defaultGroup != null) {
            return user.defaultGroup;
        }
        throw new RuntimeException("CONFIG or BUG: posix principal default group is null");
    }
}
