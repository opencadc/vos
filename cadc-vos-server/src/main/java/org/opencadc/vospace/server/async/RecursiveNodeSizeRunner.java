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

import ca.nrc.cadc.auth.AuthMethod;
import ca.nrc.cadc.auth.AuthenticationUtil;
import ca.nrc.cadc.cred.client.CredUtil;
import ca.nrc.cadc.io.ResourceIterator;
import ca.nrc.cadc.net.HttpConstants;
import ca.nrc.cadc.net.HttpUpload;
import ca.nrc.cadc.reg.Standards;
import ca.nrc.cadc.uws.Parameter;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.security.AccessControlException;
import java.util.ArrayList;
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
    private File tmpFile;
    private BufferedWriter reportWriter;

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

        // Write the report to a local temp file first, then upload it.
        tmpFile = File.createTempFile("node-size-report-", ".txt");

        try {
            reportWriter = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(tmpFile), StandardCharsets.UTF_8));

            log.debug("Initializing nodesize calculation for: " + nodePath);
            accumulateNodeSize(root, subject, 0);
            log.debug("Finished nodesize calculation for: " + nodePath);
        } finally {
            if (reportWriter != null) {
                try {
                    reportWriter.close();
                } catch (IOException e) {
                    log.warn("failed to close report writer", e);
                }
            }
        }

        URL putURL = getPutURL();
        log.debug("Uploading node-size-report to: " + putURL);
        uploadReportToDest(putURL);
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

    // Note: Report permission denied for all the depths
    private void writeToReport(String path, long size, int depth) throws IOException {
        if (depth <= maxDepth || size < 0L) {
            if (size < 0L) {
                reportWriter.write(PERMISSION_DENIED_RESULT_TEXT);
            } else {
                reportWriter.write(Long.toString(size));
            }
            reportWriter.write('\t');
            reportWriter.write(path);
            reportWriter.newLine();
        }
    }

    private long accumulateNodeSize(ContainerNode node, Subject subject, int depth) throws Exception {
        if (Thread.currentThread().isInterrupted()) {
            throw new InterruptedException();
        }

        checkJobPhase();

        if (!authorizer.hasSingleNodeReadPermission(node, subject)) {
            log.debug("no read permission on node " + Utils.getPath(node));
            // incErrorCount(); // Not considering this as an error. Just reporting permission denied for this node and continuing with the rest of the tree.
            writeToReport(Utils.getPath(node), -1L, depth);
            return 0;
        }

        // if the bytesUsed is in place and depth is reached to or beyond max depth, skip further recursion, else keep the recursion going.
        if (depth >= maxDepth) {
            if (node.bytesUsed != null) {
                incSuccessCount();
                writeToReport(Utils.getPath(node),  node.bytesUsed, depth);
                return node.bytesUsed;
            } else {
                Long dataNodeSize = nodePersistence.getDataNodeSize(node);
                if (dataNodeSize != null) {
                    incSuccessCount();
                    writeToReport(Utils.getPath(node), dataNodeSize, depth);
                    return dataNodeSize;
                }
            }
        }

        // collect child containers for later
        List<ContainerNode> childContainers = new ArrayList<>();
        long currentNodeSize = 0L;
        try (ResourceIterator<Node> iter = nodePersistence.iterator(node, null, null)) {
            while (iter.hasNext()) {
                Node child = iter.next();
                if (child instanceof ContainerNode) {
                    childContainers.add((ContainerNode) child);
                } else if (child instanceof DataNode) {
                    DataNode dn = (DataNode) child;
                    Long bytes = dn.bytesUsed;
                    if (bytes == null) {
                        bytes = 0L;
                    }
                    currentNodeSize += bytes;
                    incSuccessCount();
                } else {
                    // ignore LinkNode
                    log.debug("Skipping LinkNode : " + Utils.getPath(node));
                }
            }
        }

        // now recurse so we only have one open iterator at a time
        for (ContainerNode cn : childContainers) {
            currentNodeSize += accumulateNodeSize(cn, subject, depth + 1);
        }

        incSuccessCount();
        writeToReport(Utils.getPath(node), currentNodeSize, depth);
        return currentNodeSize;
    }

    private URL getPutURL() throws Exception {
        Subject caller = AuthenticationUtil.getCurrentSubject();
        ensureDestDataNode(caller);

        CredUtil.checkCredentials(caller);
        AuthMethod authType = AuthenticationUtil.getAuthMethodFromCredentials(caller);
        URI securityMethod = Standards.getSecurityMethod(authType);

        Protocol sc = new Protocol(VOS.PROTOCOL_HTTPS_PUT);
        sc.setSecurityMethod(securityMethod);

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

    private void uploadReportToDest(URL putURL) throws Exception {
        HttpUpload upload = new HttpUpload(tmpFile, putURL);
        upload.setRequestProperty(HttpConstants.HDR_CONTENT_TYPE, CONTENT_TYPE);
        upload.run();
        if (upload.getThrowable() != null) {
            throw new IOException("failed to write nodesize report to " + dest, upload.getThrowable());
        }
    }

}
