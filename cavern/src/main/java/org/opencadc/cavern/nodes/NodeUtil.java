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

import org.opencadc.gms.GroupURI;
import ca.nrc.cadc.date.DateUtil;
import ca.nrc.cadc.reg.Standards;
import ca.nrc.cadc.reg.client.LocalAuthority;
import ca.nrc.cadc.vos.ContainerNode;
import ca.nrc.cadc.vos.DataNode;
import ca.nrc.cadc.vos.LinkNode;
import ca.nrc.cadc.vos.Node;
import ca.nrc.cadc.vos.NodeProperty;
import ca.nrc.cadc.vos.VOS;
import ca.nrc.cadc.vos.VOSURI;
import java.io.IOException;
import java.net.URI;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.nio.file.DirectoryStream;
import java.nio.file.FileSystemException;
import java.nio.file.FileVisitResult;
import java.nio.file.FileVisitor;
import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.nio.file.attribute.AclEntry;
import java.nio.file.attribute.AclEntry.Builder;
import java.nio.file.attribute.AclEntryPermission;
import java.nio.file.attribute.AclEntryType;
import java.nio.file.attribute.AclFileAttributeView;
import java.nio.file.attribute.BasicFileAttributes;
import java.nio.file.attribute.FileTime;
import java.nio.file.attribute.GroupPrincipal;
import java.nio.file.attribute.PosixFileAttributeView;
import java.nio.file.attribute.PosixFileAttributes;
import java.nio.file.attribute.PosixFilePermission;
import java.nio.file.attribute.PosixFilePermissions;
import java.nio.file.attribute.UserDefinedFileAttributeView;
import java.nio.file.attribute.UserPrincipal;
import java.nio.file.attribute.UserPrincipalLookupService;
import java.nio.file.attribute.UserPrincipalNotFoundException;
import java.text.DateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;
import org.apache.log4j.Logger;

/**
 * Utility methods for interacting with nodes.
 *
 * @author pdowler
 */
public abstract class NodeUtil {

    private static final Logger log = Logger.getLogger(NodeUtil.class);

    // set of node properties that are stored in some special way 
    // and *not* as extended attributes
    private static Set<String> FILESYSTEM_PROPS = new HashSet<>(
            Arrays.asList(
                    new String[]{
                        VOS.PROPERTY_URI_AVAILABLESPACE,
                        VOS.PROPERTY_URI_CONTENTLENGTH,
                        VOS.PROPERTY_URI_CREATION_DATE,
                        VOS.PROPERTY_URI_CREATOR,
                        VOS.PROPERTY_URI_DATE,
                        VOS.PROPERTY_URI_GROUPREAD,
                        VOS.PROPERTY_URI_GROUPWRITE,
                        VOS.PROPERTY_URI_ISLOCKED,
                        VOS.PROPERTY_URI_ISPUBLIC
                    }
            )
    );

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

    public static Path create(Path root, Node node)
            throws IOException {
        Path np = nodeToPath(root, node);
        log.debug("[create] path: " + node.getUri() + " -> " + np);

        UserPrincipalLookupService users = root.getFileSystem().getUserPrincipalLookupService();
        UserPrincipal owner = getOwner(users, node);
        assertNotNull("owner", owner);
        GroupPrincipal group = getDefaultGroup(users, owner);
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
            setPosixOwnerGroup(root, ret, owner, group);
            setNodeProperties(ret, node);
        } catch (IOException ex) {
            log.debug("CREATE FAIL", ex);
            Files.delete(ret);
            throw new UnsupportedOperationException("failed to create " + node.getClass().getSimpleName()
                    + " " + node.getUri(), ex);
        }

        return ret;
    }
    
    public static void setPosixOwnerGroup(Path root, Path p, UserPrincipal owner, GroupPrincipal group) throws IOException {
        PosixFileAttributeView pv = Files.getFileAttributeView(p,
                PosixFileAttributeView.class, LinkOption.NOFOLLOW_LINKS);
        if (owner != null) {
            try {
                pv.setOwner(owner);
            } catch (IOException ex) {
                throw new RuntimeException("failed to set owner: " + owner.getName() + " on " + p, ex);
            }
        }
        if (group != null) {
            try {
                pv.setGroup(group);
            } catch (IOException ex) {
                throw new RuntimeException("failed to set group: " + group.getName() + " on " + p, ex);
            }
        }
    }
    
    public static void setNodeProperties(Path path, Node node) throws IOException { 
        log.debug("setNodeProperties: " + node);
        if (!node.getProperties().isEmpty() && !(node instanceof LinkNode)) {
            UserDefinedFileAttributeView udv = Files.getFileAttributeView(path,
                    UserDefinedFileAttributeView.class, LinkOption.NOFOLLOW_LINKS);
            for (NodeProperty prop : node.getProperties()) {
                if (!FILESYSTEM_PROPS.contains(prop.getPropertyURI())) {
                    if (prop.isMarkedForDeletion()) {
                        try {
                            udv.delete(prop.getPropertyURI());
                        } catch (FileSystemException ex) {
                            if (ex.getMessage().contains("No data available")) {
                                log.debug("ignore: attempt to delete attribute that does not exist");
                            } else {
                                throw ex;
                            }
                        }
                    } else {
                        setAttribute(udv, prop.getPropertyURI(), prop.getPropertyValue());
                    }
                }
            }
            
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
            
            LocalAuthority loc = new LocalAuthority();
            URI localGMS = loc.getServiceURI(Standards.GMS_GROUPS_01.toASCIIString());
            boolean isDir = (node instanceof ContainerNode);
            UserPrincipalLookupService users = path.getFileSystem().getUserPrincipalLookupService();
            AclCommandExecutor acl = new AclCommandExecutor(path, users);
            String sro = node.getPropertyValue(VOS.PROPERTY_URI_GROUPREAD);
            if (sro != null) {
                GroupURI guri = new GroupURI(sro);
                URI groupGMS = guri.getServiceID();
                if (!groupGMS.equals(localGMS)) {
                    // TODO: throw? warn? store as normal extended attr? (pathToNode would re-instantiate)
                    throw new IllegalArgumentException("external group not supported: " + guri);
                }
                try {
                    GroupPrincipal gp = users.lookupPrincipalByGroupName(guri.getName());
                    acl.setReadOnlyACL(gp, isDir);
                } catch (UserPrincipalNotFoundException ex) {
                    throw new RuntimeException("failed to find existing group: " + guri, ex);
                }
            }
            
            String srw = node.getPropertyValue(VOS.PROPERTY_URI_GROUPWRITE);
            if (srw != null) {
                GroupURI guri = new GroupURI(srw);
                URI groupGMS = guri.getServiceID();
                if (!groupGMS.equals(localGMS)) {
                    // TODO: throw? warn? store as normal extended attr? (pathToNode would re-instantiate)
                    throw new IllegalArgumentException("external group not supported: " + guri);
                }
                try {
                    GroupPrincipal gp = users.lookupPrincipalByGroupName(guri.getName());
                    acl.setReadWriteACL(gp, isDir);
                } catch (UserPrincipalNotFoundException ex) {
                    throw new RuntimeException("failed to find existing group: " + guri, ex);
                }
            }
            
            
        }
    }

    // NOTE: AclFileAttributeView not supported with linux OpenJDK so this code is maybe correct but useless
    @Deprecated
    private static void createACL(Path root, Path p, GroupPrincipal readOnly, GroupPrincipal readWrite) throws IOException {
        final AclFileAttributeView av = Files.getFileAttributeView(p, AclFileAttributeView.class, LinkOption.NOFOLLOW_LINKS);
        if (av == null) {
            throw new UnsupportedOperationException("AclFileAttributeView for " + p);
        }
        final PosixFileAttributes attrs = Files.readAttributes(p, PosixFileAttributes.class, LinkOption.NOFOLLOW_LINKS);

        List<AclEntry> acls = av.getAcl();
        acls.clear(); // setfacl -b

        if (readOnly != null) {
            Builder b = AclEntry.newBuilder().setType(AclEntryType.ALLOW);
            b.setPrincipal(readOnly);
            b.setPermissions(AclEntryPermission.READ_ATTRIBUTES);
            //b.setPermissions(AclEntryPermission.READ_NAMED_ATTRS);
            b.setPermissions(AclEntryPermission.READ_ACL);
            if (attrs.isDirectory()) {
                b.setPermissions(AclEntryPermission.LIST_DIRECTORY);
            } else if (attrs.isRegularFile()) {
                b.setPermissions(AclEntryPermission.READ_DATA);
            } else if (attrs.isSymbolicLink()) {
                // nothing more
            }
            AclEntry ro = b.build();
            acls.add(ro);
        }

        if (readWrite != null) {
            Builder b = AclEntry.newBuilder().setType(AclEntryType.ALLOW);
            b.setPrincipal(readWrite);
            b.setPermissions(AclEntryPermission.READ_ATTRIBUTES);
            //b.setPermissions(AclEntryPermission.READ_NAMED_ATTRS);
            b.setPermissions(AclEntryPermission.READ_ACL);
            b.setPermissions(AclEntryPermission.WRITE_ATTRIBUTES);
            //b.setPermissions(AclEntryPermission.WRITE_NAMED_ATTRS);
            b.setPermissions(AclEntryPermission.WRITE_ACL);
            if (attrs.isDirectory()) {
                b.setPermissions(AclEntryPermission.LIST_DIRECTORY);
                b.setPermissions(AclEntryPermission.ADD_FILE);
                b.setPermissions(AclEntryPermission.ADD_SUBDIRECTORY);
                b.setPermissions(AclEntryPermission.DELETE_CHILD);
            } else if (attrs.isRegularFile()) {
                b.setPermissions(AclEntryPermission.READ_DATA);
                b.setPermissions(AclEntryPermission.APPEND_DATA);
                b.setPermissions(AclEntryPermission.WRITE_DATA);
            } else if (attrs.isSymbolicLink()) {
                // nothing more
            }
            AclEntry rw = b.build();
            acls.add(rw);
        }

        av.setAcl(acls);
    }

    private static void setAttribute(UserDefinedFileAttributeView v, String name, String value) throws IOException {
        log.debug("setAttribute: " + name + " = " + value);
        if (value != null) {
            value = value.trim();
            ByteBuffer buf = ByteBuffer.wrap(value.getBytes(Charset.forName("UTF-8")));
            v.write(name, buf);
        } // else: do nothing
    }

    private static String getAttribute(UserDefinedFileAttributeView v, String name) throws IOException {
        int sz = v.size(name);
        ByteBuffer buf = ByteBuffer.allocate(2 * sz);
        v.read(name, buf);
        return new String(buf.array(), Charset.forName("UTF-8")).trim();
    }

    public static Path update(Path root, Node node) throws IOException {
        Path np = nodeToPath(root, node);
        log.debug("[update] path: " + node.getUri() + " -> " + np);
        throw new UnsupportedOperationException();
    }

    public static Node get(Path root, VOSURI uri)  throws IOException {
        return get(root, uri, false);
    }
    
    public static Node get(Path root, VOSURI uri, boolean allowPartialPath) throws IOException {
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
                Node tmp = pathToNode(root, p, rootURI);
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

    public static void move(Path root, VOSURI source, VOSURI destDir, String destName, UserPrincipal owner) throws IOException {
        Path sourcePath = nodeToPath(root, source);
        VOSURI destWithName = new VOSURI(URI.create(destDir.toString() + "/" + destName));
        Path destPath = nodeToPath(root, destWithName);
        Files.move(sourcePath, destPath, StandardCopyOption.ATOMIC_MOVE);

        // TODO: preserve old group during move?
        GroupPrincipal group = getDefaultGroup(root.getFileSystem().getUserPrincipalLookupService(), owner);
        assertNotNull("group", group);
        setPosixOwnerGroup(root, destPath, owner, group);
    }

    public static void copy(Path root, VOSURI source, VOSURI destDir, UserPrincipal owner) throws IOException {
        GroupPrincipal group = getDefaultGroup(root.getFileSystem().getUserPrincipalLookupService(), owner);
        assertNotNull("group", group);

        Path sourcePath = nodeToPath(root, source);
        VOSURI destWithName = new VOSURI(URI.create(destDir.toString() + "/" + source.getName()));
        Path destPath = nodeToPath(root, destWithName);
        // follow links
        if (Files.isDirectory(sourcePath)) {
            Files.walkFileTree(sourcePath, new CopyVisitor(root, sourcePath, destPath, owner, group));
        } else { // links and files
            Files.copy(sourcePath, destPath, StandardCopyOption.COPY_ATTRIBUTES);
            setPosixOwnerGroup(root, destPath, owner, group);
            Files.setLastModifiedTime(destPath, FileTime.fromMillis(System.currentTimeMillis()));
        }
    }

    static Node pathToNode(Path root, Path p, VOSURI rootURI)
            throws IOException, NoSuchFileException {
        boolean getAttrs = System.getProperty(NodeUtil.class.getName() + ".disable-get-attrs") == null;
        return pathToNode(root, p, rootURI, getAttrs);
    }
    
    // getAttrs == false needed in MountedContainerTest
    static Node pathToNode(Path root, Path p, VOSURI rootURI, boolean getAttrs)
            throws IOException, NoSuchFileException {
        Node ret = null;
        VOSURI nuri = pathToURI(root, p, rootURI);
        PosixFileAttributes attrs = Files.readAttributes(p,
                PosixFileAttributes.class, LinkOption.NOFOLLOW_LINKS);
        if (attrs.isDirectory()) {
            ret = new ContainerNode(nuri);
        } else if (attrs.isRegularFile()) {
            ret = new DataNode(nuri);
            // TODO: restore file-specific properties
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

        setOwner(ret, attrs.owner());
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
            UserDefinedFileAttributeView udv = Files.getFileAttributeView(p,
                    UserDefinedFileAttributeView.class, LinkOption.NOFOLLOW_LINKS);
            if (udv == null) {
                throw new UnsupportedOperationException("file system does not support "
                    + "user defined file attributes.");
            }
            for (String propName : udv.list()) {
                String propValue = getAttribute(udv, propName);
                if (propValue != null) {
                    ret.getProperties().add(new NodeProperty(propName, propValue));
                }
            }
            LocalAuthority loc = new LocalAuthority();
            URI resourceID = loc.getServiceURI(Standards.GMS_GROUPS_01.toASCIIString());
            AclCommandExecutor acl = new AclCommandExecutor(p, p.getFileSystem().getUserPrincipalLookupService());
            GroupPrincipal rog = acl.getReadOnlyACL(attrs.isDirectory());
            if (rog != null) {
                GroupURI guri = new GroupURI(URI.create(resourceID.toASCIIString() + "?" + rog.getName()));
                ret.getProperties().add(new NodeProperty(VOS.PROPERTY_URI_GROUPREAD, guri.getURI().toASCIIString()));
            }
            GroupPrincipal rwg = acl.getReadWriteACL(attrs.isDirectory());
            if (rwg != null) {
                GroupURI guri = new GroupURI(URI.create(resourceID.toASCIIString() + "?" + rwg.getName()));
                ret.getProperties().add(new NodeProperty(VOS.PROPERTY_URI_GROUPWRITE, guri.getURI().toASCIIString()));
            }
        }

        NodeProperty publicProp = new NodeProperty(VOS.PROPERTY_URI_ISPUBLIC, Boolean.toString(false));
        ret.getProperties().add(publicProp);
        for (PosixFilePermission pfp : attrs.permissions()) {
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

        UserPrincipal owner;
        GroupPrincipal group;
        Path root;
        Path destDir;
        Path sourceDir;

        // TODO: alt ctor without group to preserve group during copy?
        CopyVisitor(Path root, Path source, Path dest, UserPrincipal owner, GroupPrincipal group) {
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
            NodeUtil.setPosixOwnerGroup(root, dir, owner, group);
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
            NodeUtil.setPosixOwnerGroup(root, file, owner, group);
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

    public static Iterator<Node> list(Path root, ContainerNode node,
            VOSURI start, Integer limit) throws IOException {
        Path np = nodeToPath(root, node);
        log.debug("[list] " + node.getUri() + " -> " + np);
        VOSURI rootURI = node.getUri();
        while (!rootURI.isRoot()) {
            rootURI = rootURI.getParentURI();
        }
        log.debug("[list] root: " + rootURI + " -> " + root);
        List<Node> nodes = new ArrayList<Node>();
        if (limit == null || limit > 0) {
            try (DirectoryStream<Path> stream = Files.newDirectoryStream(np)) {
                for (Path file : stream) {
                    log.debug("[list] visit: " + file);
                    Node n = pathToNode(root, file, rootURI);
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

    // temporary: store bare UserPrincipal in node property list as string
    public static void setOwner(Node node, UserPrincipal owner) {
        node.getProperties().add(
                new NodeProperty(VOS.PROPERTY_URI_CREATOR, owner.getName()));
    }

    public static UserPrincipal getOwner(UserPrincipalLookupService users,
            Node node) throws IOException {
        NodeProperty prop = node.findProperty(VOS.PROPERTY_URI_CREATOR);
        if (prop != null) {
            return users.lookupPrincipalByName(prop.getPropertyValue());
        }
        return null;
    }

    public static GroupPrincipal getDefaultGroup(UserPrincipalLookupService users,
            UserPrincipal user) throws IOException {
        // TODO: this assumes default group name == owner name and should be fixed
        return users.lookupPrincipalByGroupName(user.getName());
    }
}
