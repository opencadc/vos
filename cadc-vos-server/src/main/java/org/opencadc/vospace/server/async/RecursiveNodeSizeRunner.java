/*
 ************************************************************************
 *******************  CANADIAN ASTRONOMY DATA CENTRE  *******************
 **************  CENTRE CANADIEN DE DONNÉES ASTRONOMIQUES  **************
 *
 *  (c) 2026.                            (c) 2026.
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

package org.opencadc.vospace.server.async;

import ca.nrc.cadc.auth.AuthenticationUtil;
import ca.nrc.cadc.io.ResourceIterator;
import ca.nrc.cadc.net.HttpUpload;
import ca.nrc.cadc.net.OutputStreamWrapper;
import ca.nrc.cadc.reg.Standards;
import ca.nrc.cadc.uws.Parameter;
import java.io.IOException;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.security.AccessControlException;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.List;
import javax.security.auth.Subject;
import org.apache.log4j.Logger;
import org.opencadc.vospace.ContainerNode;
import org.opencadc.vospace.DataNode;
import org.opencadc.vospace.Node;
import org.opencadc.vospace.VOS;
import org.opencadc.vospace.VOSURI;
import org.opencadc.vospace.server.PathResolver;
import org.opencadc.vospace.server.Utils;
import org.opencadc.vospace.transfer.Direction;
import org.opencadc.vospace.transfer.Protocol;
import org.opencadc.vospace.transfer.Transfer;

/**
 * Async job runner that computes recursive byte usage for an allocation.
 */
public class RecursiveNodeSizeRunner extends AbstractRecursiveRunner {

    private static final Logger log = Logger.getLogger(RecursiveNodeSizeRunner.class);

    public static final String CONTENT_TYPE = "text/plain";
    public static final String PERMISSION_DENIED_RESULT_TEXT = "Permission Denied";

    private int maxDepth = 0;
    private VOSURI dest;

    @Override
    protected void initTarget() throws Exception {
        for (Parameter p : job.getParameterList()) {
            if ("target".equalsIgnoreCase(p.getName())) {
                this.target = new VOSURI(URI.create(p.getValue()));
            } else if ("maxdepth".equalsIgnoreCase(p.getName())) {
                try {
                    int v = Integer.parseInt(p.getValue());
                    if (v < 0) {
                        v = 0;
                    }
                    this.maxDepth = v;
                } catch (NumberFormatException ignore) {
                    // keep default 0
                }
            } else if ("dest".equalsIgnoreCase(p.getName())) {
                try {
                    this.dest = new VOSURI(new URI(p.getValue()));
                } catch (URISyntaxException e) {
                    throw new IllegalArgumentException("dest must be a valid URI: " + p.getValue(), e);
                }
            }
        }
        if (target == null) {
            throw new IllegalArgumentException("target argument required");
        }
        if (dest == null) {
            throw new IllegalArgumentException("dest argument required");
        }
        validateDest();
        log.debug("target: " + target + " dest: " + dest + " maxDepth=" + maxDepth);
    }

    @Override
    protected boolean performAction(Node node) throws Exception {
        if (!(node instanceof ContainerNode)) {
            sendError("target must be a ContainerNode");
            return false;
        }
        ContainerNode root = (ContainerNode) node;
        /*if (!nodePersistence.isAllocation(root)) {
            sendError("target is not an allocation root");
            return false;
        }*/
        Subject subject = AuthenticationUtil.getCurrentSubject();
        String nodePath = Utils.getPath(root);
        if (!authorizer.hasSingleNodeReadPermission(root, subject)) {
            sendError("no read permission on the container node: " + nodePath);
            return false;
        }

        URL putURL = getPutURL();
        log.debug("putURL: " + putURL);

        DynamicReportOutput out = new DynamicReportOutput(root, subject);
        if (out.scanException != null) {
            log.error("Error while traversing nodes : ", out.scanException);
            throw out.scanException;
        }

        HttpUpload upload = new HttpUpload(out, putURL);
        upload.setRequestProperty("Content-Type", CONTENT_TYPE);
        upload.run();

        if (out.scanException != null) {
            log.error("Error while traversing nodes : ", out.scanException);
            throw out.scanException;
        }

        if (upload.getThrowable() != null) {
            log.error("Error uploading node-size-report to: " + putURL, upload.getThrowable());
            throw new IOException("upload failed : " + upload.getThrowable().getMessage(), upload.getThrowable());
        }

        log.debug("Finished uploading node-size-report to: " + putURL);
        return true;
    }

    private void validateDest() {
        if (!dest.getServiceURI().equals(target.getServiceURI())) {
            throw new UnsupportedOperationException("dest must be the same vospace service as target");
        }
        if (dest.getPath().endsWith("/")) {
            throw new IllegalArgumentException("dest must be a DataNode path, not a container");
        }
    }

    private URL getPutURL() throws Exception {
        Subject caller = AuthenticationUtil.getCurrentSubject();
        ensureDestDataNode(caller);

        Protocol sc = new Protocol(VOS.PROTOCOL_HTTPS_PUT);
        sc.setSecurityMethod(Standards.SECURITY_METHOD_CERT);

        Transfer request = new Transfer(dest.getURI(), Direction.pushToVoSpace);
        request.version = VOS.VOSPACE_21;
        request.getProtocols().add(sc);
        List<Protocol> endpoints = nodePersistence.getTransferGenerator().getEndpoints(dest, request, null);
        if (endpoints == null || endpoints.isEmpty()) {
            throw new IllegalStateException("endpoint not found for: " + dest);
        }
        return new URL(endpoints.get(0).getEndpoint());
    }

    private void ensureDestDataNode(Subject caller) throws Exception {
        PathResolver pr = new PathResolver(nodePersistence, authorizer);
        PathResolver.ResolvedNode rn = pr.getTargetNode(dest.getPath());

        if (rn == null) {
            throw new IllegalArgumentException("parent path not found for dest: " + dest.getPath());
        }

        if (rn.node == null) {
            log.debug("dest node not found, creating new DataNode");
            // create new DataNode
            if (!authorizer.hasSingleNodeWritePermission(rn.parent, caller)) {
                throw new AccessControlException("no write permission on dest parent");
            }
            DataNode dn = new DataNode(rn.name);
            dn.parent = rn.parent;
            dn.owner = caller;
            if (rn.parent.inheritPermissions != null && rn.parent.inheritPermissions) {
                dn.isPublic = rn.parent.isPublic;
                dn.getReadOnlyGroup().addAll(rn.parent.getReadOnlyGroup());
                dn.getReadWriteGroup().addAll(rn.parent.getReadWriteGroup());
            }
            nodePersistence.put(dn);
        } else if (rn.node instanceof DataNode) {
            log.debug("dest node found");
            if (!authorizer.hasSingleNodeWritePermission(rn.parent, caller)) {
                throw new AccessControlException("no write permission on dest");
            }
        } else {
            throw new IllegalArgumentException("dest must be a DataNode path, not a container");
        }
    }

    private class DynamicReportOutput implements OutputStreamWrapper {

        // Each frame on the stack represents one ContainerNode mid-traversal.
        private final class Frame {
            final ContainerNode node;
            final int depth;
            long runningSize; // accumulated byte count of children processed so far.
            final List<ContainerNode> pendingChildren; // ordered list of child ContainerNodes not yet visited.

            Frame(ContainerNode node, int depth, long runningSize, List<ContainerNode> pendingChildren) {
                this.node = node;
                this.depth = depth;
                this.runningSize = runningSize;
                this.pendingChildren = pendingChildren;
            }
        }

        private final Subject subject;
        private final Deque<Frame> stack = new ArrayDeque<>();

        Exception scanException = null;
        private String pendingPath = null;
        private Long pendingSize = null; // sentinel: MIN_VALUE means "none"
        private int permissionDeniedCount = 0;

        /*
         * Push the root frame.
         * advance() will run until the first report line is ready,
         * leaving that line in pendingPath/pendingSize and leaving the rest of the work on the stack.
         * */
        DynamicReportOutput(ContainerNode root, Subject subject) {
            this.subject = subject;
            try {
                pushFrame(root, 0);
                advance();
                if (pendingPath == null || pendingSize == null) {
                    throw new RuntimeException("BUG: Failed to set Pending State for root node: " + root);
                }
                log.debug("Initial recursive node size calculation complete, pendingPath=" + pendingPath + " pendingSize=" + pendingSize);
            } catch (Exception e) {
                scanException = e;
            }
        }

        @Override
        public void write(OutputStream os) throws IOException {
            Writer out = new OutputStreamWriter(os, StandardCharsets.UTF_8);
            try {
                log.debug("Resuming recursive node size calculation");
                // Drain whatever the ctor already produced and then drive the rest.
                while (pendingSize != null) {
                    writeLine(out, pendingPath, pendingSize);
                    setPendingState(null, null); // clear pending state
                    advance();
                }
                if (permissionDeniedCount > 0) {
                    out.write("\nWARNING: " + permissionDeniedCount + " container(s) were not checked due to permissions; sizes are an under-estimate");
                }
                out.flush();
                log.debug("Finished recursive node size calculation");
            } catch (Exception e) {
                scanException = e;
                throw new RuntimeException(e);
            }
        }

        /**
         * Iterates the stack until the next report line is ready, storing it in pendingPath/pendingSize to write later,
         * or until the stack is empty (done).
         */
        private void advance() throws Exception {
            while (!stack.isEmpty()) {
                Frame top = stack.peek();

                if (!top.pendingChildren.isEmpty()) {
                    // Dive into the next child container.
                    ContainerNode child = top.pendingChildren.remove(0);

                    if (!authorizer.hasSingleNodeReadPermission(child, subject)) {
                        setPendingState(Utils.getPath(child), -1L);
                        permissionDeniedCount++;
                        return;
                    }

                    long childSize = pushFrame(child, top.depth + 1);
                    if (childSize >= 0) {
                        top.runningSize += childSize;
                        if (pendingSize != null) {
                            return; // short-circuit set pending state, yield to writer
                        }
                    }
                } else {
                    // All children of 'top' are done; this node is now fully sized.
                    stack.pop();
                    long nodeTotal = top.runningSize;

                    // Propagate to the parent frame if one exists.
                    if (!stack.isEmpty()) {
                        stack.peek().runningSize += nodeTotal;
                    }
                    incSuccessCount();
                    if (top.depth <= maxDepth) {
                        setPendingState(Utils.getPath(top.node), nodeTotal);
                        return; // caller will write the line then call advance() again
                    }
                }
            }
            // Stack exhausted -- no more lines.
        }

        /**
         * Pushes a new frame for {@code containerNode} after collecting its direct children.
         * Returns the resolved size immediately if the node can be short-circuited (cached bytesUsed at/beyond maxDepth), or -1
         * if a new frame was pushed and the caller must continue iterating the stack.
         */
        private long pushFrame(ContainerNode node, int depth) throws Exception {
            if (Thread.currentThread().isInterrupted()) {
                throw new InterruptedException();
            }
            checkJobPhase();

            // short circuit : Need to check if the bytesUsed is in sync. If not, keep the recursion going.
            if (depth >= maxDepth && node.bytesUsed != null) {
                if (depth == maxDepth) {
                    setPendingState(Utils.getPath(node), node.bytesUsed);
                }
                incSuccessCount();
                return node.bytesUsed;
            }

            // Iterate children to collect DataNode bytes and child ContainerNodes.
            List<ContainerNode> childContainers = new ArrayList<>();
            long dataNodeBytes = 0L;
            try (ResourceIterator<Node> iter = nodePersistence.iterator(node, null, null)) {
                while (iter.hasNext()) {
                    Node child = iter.next();
                    if (child instanceof ContainerNode) {
                        childContainers.add((ContainerNode) child);
                    } else if (child instanceof DataNode) {
                        Long bytes = ((DataNode) child).bytesUsed;
                        if (bytes == null) {
                            bytes = 0L;
                        }
                        dataNodeBytes += bytes;
                        incSuccessCount();
                    } else {
                        log.debug("Skipping LinkNode: " + Utils.getPath(child));
                    }
                }
            }

            stack.push(new Frame(node, depth, dataNodeBytes, childContainers));
            return -1;
        }

        private void writeLine(Writer out, String path, long size) throws IOException {
            String sizeStr = (size == -1L) ? PERMISSION_DENIED_RESULT_TEXT : Long.toString(size);
            out.write(sizeStr + '\t' + path + '\n');
        }

        private void setPendingState(String pendingPath, Long childSize) {
            this.pendingPath = pendingPath;
            this.pendingSize = childSize;
        }
    }

}
