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
import ca.nrc.cadc.vos.ContainerNode;
import ca.nrc.cadc.vos.DataNode;
import ca.nrc.cadc.vos.LinkNode;
import ca.nrc.cadc.vos.Node;
import ca.nrc.cadc.vos.NodeProperty;
import ca.nrc.cadc.vos.VOS;
import ca.nrc.cadc.vos.VOSURI;

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
import java.nio.file.attribute.GroupPrincipal;
import java.nio.file.attribute.PosixFileAttributeView;
import java.nio.file.attribute.PosixFileAttributes;
import java.nio.file.attribute.PosixFilePermission;
import java.nio.file.attribute.UserPrincipal;
import java.nio.file.attribute.UserPrincipalLookupService;
import java.text.DateFormat;
import java.util.ArrayList;
import java.util.Date;
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
public abstract class NodeUtil
{
    private static final Logger log = Logger.getLogger(NodeUtil.class);

    private NodeUtil()
    {
    }

    public static Path nodeToPath(Path root, Node node)
    {
        assertNotNull("node", node);
        return nodeToPath(root, node.getUri());
    }

    public static Path nodeToPath(Path root, VOSURI uri)
    {
        assertNotNull("root", root);
        assertNotNull("uri", uri);
        log.debug("[nodeToPath] root: " + root + " uri: " + uri);

        String nodePath = uri.getPath();
        if (nodePath.startsWith("/"))
        {
            nodePath = nodePath.substring(1);
        }
        Path np = root.resolve(nodePath);
        log.debug("[nodeToPath] path: " + uri + " -> " + np);
        return np;
    }

    public static VOSURI pathToURI(Path root, Path p, VOSURI rootURI)
    {
        Path tp = root.relativize(p);
        return new VOSURI(URI.create(rootURI.getScheme() + "://"
                + rootURI.getAuthority() + "/" + tp.toFile().getPath()));
    }

    public static Path create(Path root, Node node, GroupPrincipal posixGroup)
            throws IOException
    {
        Path np = nodeToPath(root, node);
        log.debug("[create] path: " + node.getUri() + " -> " + np);

        UserPrincipal owner = getOwner(
                root.getFileSystem().getUserPrincipalLookupService(), node);
        assertNotNull("owner", owner);

        // TODO: don't assume convention of user == default group
        // GroupPrincipal group =
        // root.getFileSystem().getUserPrincipalLookupService().lookupPrincipalByGroupName(owner.getName());

        Path ret = null;
        if (node instanceof ContainerNode)
        {
            log.debug("[create] dir: " + np);
            ret = Files.createDirectory(np);
        } else if (node instanceof DataNode)
        {
            log.debug("[create] file: " + np);
            ret = Files.createFile(np);
        } else if (node instanceof LinkNode)
        {
            LinkNode ln = (LinkNode) node;
            String targPath = ln.getTarget().getPath().substring(1);
            Path absPath = root.resolve(targPath);
            Path rel = np.getParent().relativize(absPath);
            log.debug("[create] link: " + np + "\ntarget: " + targPath
                    + "\nabs: " + absPath + "\nrel: " + rel);
            ret = Files.createSymbolicLink(np, rel);
        } else
        {
            throw new UnsupportedOperationException(
                    "unexpected node type: " + node.getClass().getName());
        }

        applyPermissions(root, ret, posixGroup, owner);

        return ret;
    }

    private static void applyPermissions(Path root, Path p,
            GroupPrincipal posixGroup, UserPrincipal owner) throws IOException
    {
        PosixFileAttributeView pv = Files.getFileAttributeView(p,
                PosixFileAttributeView.class, LinkOption.NOFOLLOW_LINKS);
        if (posixGroup != null)
        {
            pv.setGroup(posixGroup);
            if (!(Files.isSymbolicLink(p)))
            {
                Set<PosixFilePermission> perms = pv.readAttributes()
                        .permissions();
                perms.add(PosixFilePermission.GROUP_WRITE);
                perms.add(PosixFilePermission.GROUP_WRITE);
                if (Files.isDirectory(p))
                {
                    perms.add(PosixFilePermission.GROUP_EXECUTE);
                }
                pv.setPermissions(perms);
            }
        }
        pv.setOwner(owner);
    }

    public static Path update(Path root, Node node) throws IOException
    {
        Path np = nodeToPath(root, node);
        log.debug("[update] path: " + node.getUri() + " -> " + np);
        throw new UnsupportedOperationException();
    }

    public static Node get(Path root, VOSURI uri) throws IOException
    {
        LinkedList<String> nodeNames = new LinkedList<String>();
        nodeNames.add(uri.getName());
        VOSURI parent = uri.getParentURI();
        VOSURI rootURI = uri;
        while (parent != null)
        {
            if (parent.isRoot())
            {
                rootURI = parent;
            } else
            {
                nodeNames.add(parent.getName());
            }
            parent = parent.getParentURI();
        }
        log.debug("[get] path components: " + nodeNames.size());

        ContainerNode cn = null; // new ContainerNode(rootURI);
        // cn.getProperties().add(new NodeProperty(VOS.PROPERTY_URI_ISPUBLIC,
        // Boolean.toString(true)));
        Iterator<String> iter = nodeNames.descendingIterator();
        Path cur = root;
        StringBuilder sb = new StringBuilder(rootURI.getURI().toASCIIString());
        Node ret = cn;
        while (iter.hasNext())
        {
            String pathComp = iter.next();
            Path p = cur.resolve(pathComp);
            cur = p;
            sb.append("/").append(pathComp); // for next loop
            log.debug("[get-walk] " + sb.toString());
            try
            {
                Node tmp = pathToNode(root, p, rootURI);
                if (cn == null)
                {
                    cn = (ContainerNode) tmp; // top-level dir
                } else
                {
                    cn.getNodes().add(tmp);
                    tmp.setParent(cn);
                    if (tmp instanceof ContainerNode)
                    {
                        cn = (ContainerNode) tmp;
                    }
                }
                ret = tmp;
            } catch (NoSuchFileException ex)
            {
                return null;
            }
        }
        // TODO: restore generic properties
        log.debug("[get] returning " + ret);
        return ret;
    }

    public static void move(Path root, VOSURI source, VOSURI destDir,
            UserPrincipal owner, GroupPrincipal posixGroup) throws IOException
    {
        Path sourcePath = nodeToPath(root, source);
        VOSURI destWithName = new VOSURI(URI.create(destDir.toString() + "/" + source.getName()));
        Path destPath = nodeToPath(root, destWithName);
        Files.move(sourcePath, destPath, StandardCopyOption.ATOMIC_MOVE);
        applyPermissions(root, destPath, posixGroup, owner);
    }

    public static void copy(Path root, VOSURI source, VOSURI dest, UserPrincipal owner, GroupPrincipal posixGroup) throws IOException {
        Path sourcePath = nodeToPath(root, source);
        Path destPath = nodeToPath(root, dest);
        // follow links
        if (Files.isDirectory(sourcePath)) {
            Files.walkFileTree(sourcePath, new CopyVisitor(root, sourcePath, destPath, owner, posixGroup));
        } else {
            Files.copy(sourcePath, destPath, StandardCopyOption.COPY_ATTRIBUTES);
            applyPermissions(root, destPath, posixGroup, owner);
        }
    }

    private static Node pathToNode(Path root, Path p, VOSURI rootURI)
            throws IOException, NoSuchFileException
    {
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
        DateFormat df = DateUtil.getDateFormat(DateUtil.IVOA_DATE_FORMAT,
                DateUtil.UTC);
        // Date created = new Date(attrs.creationTime().toMillis());
        // Date accessed = new Date(attrs.lastAccessTime().toMillis());
        Date modified = new Date(attrs.lastModifiedTime().toMillis());
        // ret.getProperties().add(new
        // NodeProperty(VOS.PROPERTY_URI_CREATION_DATE, df.format(created)));
        // ret.getProperties().add(new
        // NodeProperty(VOS.PROPERTY_URI_ACCESS_DATE, df.format(accessed)));
        // ret.getProperties().add(new
        // NodeProperty(VOS.PROPERTY_URI_MODIFIED_DATE, df.format(modified)));
        ret.getProperties().add(new NodeProperty(VOS.PROPERTY_URI_CREATION_DATE,
                df.format(modified)));

        for (PosixFilePermission pfp : attrs.permissions())
        {
            if (PosixFilePermission.OTHERS_READ.equals(pfp))
            {
                ret.getProperties().add(new NodeProperty(
                        VOS.PROPERTY_URI_ISPUBLIC, Boolean.toString(true)));
            }
        }
        return ret;
    }

    public static void delete(Path root, VOSURI uri) throws IOException
    {
        Path np = nodeToPath(root, uri);
        log.debug("[create] path: " + uri + " -> " + np);
        delete(np);
    }

    private static void delete(Path path) throws IOException
    {
        if (Files.isDirectory(path, LinkOption.NOFOLLOW_LINKS))
        {
            Files.walkFileTree(path, new DeleteVisitor());
        } else
        {
            Files.delete(path);
        }
    }

    private static class DeleteVisitor implements FileVisitor<Path>
    {
        @Override
        public FileVisitResult preVisitDirectory(Path t,
                BasicFileAttributes bfa) throws IOException
        {
            log.debug("enter: " + t);
            return FileVisitResult.CONTINUE;
        }

        @Override
        public FileVisitResult visitFile(Path t, BasicFileAttributes bfa)
                throws IOException
        {
            log.debug("delete: " + t);
            Files.delete(t);
            return FileVisitResult.CONTINUE;
        }

        @Override
        public FileVisitResult visitFileFailed(Path t, IOException ioe)
                throws IOException
        {
            return FileVisitResult.CONTINUE;
        }

        @Override
        public FileVisitResult postVisitDirectory(Path t, IOException ioe)
                throws IOException
        {
            Files.delete(t);
            log.debug("delete: " + t);
            return FileVisitResult.CONTINUE;
        }
    }

    private static class CopyVisitor implements FileVisitor<Path>
    {
        UserPrincipal owner;
        GroupPrincipal posixGroup;
        Path root;
        Path destDir;
        Path sourceDir;

        CopyVisitor(Path root, Path source, Path dest, UserPrincipal owner, GroupPrincipal posixGroup) {
            this.root = root;
            this.destDir = dest;
            this.owner = owner;
            this.posixGroup = posixGroup;
        }

        @Override
        public FileVisitResult preVisitDirectory(Path t,
                BasicFileAttributes bfa) throws IOException
        {
            log.debug("copy: pre-visit dir: " + t);
            if (sourceDir == null) {
                sourceDir = t;
            } else {
                Path dir = destDir.resolve(sourceDir.relativize(t));
                Files.createDirectories(dir);
                NodeUtil.applyPermissions(root, dir, posixGroup, owner);
            }
            return FileVisitResult.CONTINUE;
        }

        @Override
        public FileVisitResult visitFile(Path t, BasicFileAttributes bfa)
                throws IOException
        {
            log.debug("copy: visit file: " + t);
            Path file = destDir.resolve(sourceDir.relativize(t));
            Files.copy(t, file);
            NodeUtil.applyPermissions(root, file, posixGroup, owner);
            return FileVisitResult.CONTINUE;
        }

        @Override
        public FileVisitResult visitFileFailed(Path t, IOException ioe)
                throws IOException
        {
            return FileVisitResult.CONTINUE;
        }

        @Override
        public FileVisitResult postVisitDirectory(Path t, IOException ioe)
                throws IOException
        {
            return FileVisitResult.CONTINUE;
        }
    }

    public static Iterator<Node> list(Path root, ContainerNode node,
            VOSURI start, Integer limit) throws IOException
    {
        Path np = nodeToPath(root, node);
        log.debug("[list] " + node.getUri() + " -> " + np);
        VOSURI rootURI = node.getUri();
        while (!rootURI.isRoot())
        {
            rootURI = rootURI.getParentURI();
        }
        log.debug("[list] root: " + rootURI + " -> " + root);
        List<Node> nodes = new ArrayList<Node>();
        try (DirectoryStream<Path> stream = Files.newDirectoryStream(np))
        {
            for (Path file : stream)
            {
                log.warn("[list] visit: " + file);
                Node n = pathToNode(root, file, rootURI);
                if (!nodes.isEmpty() || start == null
                        || start.getName().equals(n.getName()))
                {
                    nodes.add(n);
                }
                if (limit != null && limit == nodes.size())
                {
                    break;
                }
            }
        }
        log.debug("[list] found: " + nodes.size());
        return nodes.iterator();
    }

    // currently unused visitor with the minimal setup to call pathToNode
    private static class DirectoryVisitor implements FileVisitor<Path>
    {
        private final Path root;
        private final VOSURI rootURI;

        List<Node> nodes = new ArrayList<Node>();

        public DirectoryVisitor(Path root, VOSURI rootURI)
        {
            this.root = root;
            this.rootURI = rootURI;
        }

        @Override
        public FileVisitResult preVisitDirectory(Path t,
                BasicFileAttributes bfa) throws IOException
        {
            log.debug("[preVisitDirectory] " + t);
            return FileVisitResult.CONTINUE;
        }

        @Override
        public FileVisitResult visitFile(Path t, BasicFileAttributes bfa)
                throws IOException
        {
            log.debug("[visitFile] " + t);
            // Node n = pathToNode(root, t, rootURI);
            return FileVisitResult.CONTINUE;
        }

        @Override
        public FileVisitResult visitFileFailed(Path t, IOException ioe)
                throws IOException
        {
            log.debug("[list] visitFileFailed: " + t + " reason: " + ioe);
            return FileVisitResult.CONTINUE;
        }

        @Override
        public FileVisitResult postVisitDirectory(Path t, IOException ioe)
                throws IOException
        {
            log.debug("[postVisitDirectory] " + t);
            return FileVisitResult.CONTINUE;
        }

    }

    public static void assertNotNull(String name, Object o)
    {
        if (o == null)
        {
            throw new IllegalArgumentException(name + " cannot be null");
        }
    }

    // temporary: store bare UserPrincipal in node property list as string
    public static void setOwner(Node node, UserPrincipal owner)
    {
        node.getProperties().add(
                new NodeProperty(VOS.PROPERTY_URI_CREATOR, owner.getName()));
    }

    public static UserPrincipal getOwner(UserPrincipalLookupService users,
            Node node) throws IOException
    {
        NodeProperty prop = node.findProperty(VOS.PROPERTY_URI_CREATOR);
        if (prop != null)
        {
            return users.lookupPrincipalByName(prop.getPropertyValue());
        }
        return null;
    }
}
