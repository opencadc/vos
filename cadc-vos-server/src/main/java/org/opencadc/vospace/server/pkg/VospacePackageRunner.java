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
 *  : 5 $
 *
 ************************************************************************
 */

package org.opencadc.vospace.server.pkg;

import ca.nrc.cadc.auth.AuthenticationUtil;
import ca.nrc.cadc.io.ResourceIterator;
import ca.nrc.cadc.net.TransientException;
import ca.nrc.cadc.reg.Standards;
import ca.nrc.cadc.uws.ExecutionPhase;
import ca.nrc.cadc.uws.Job;
import ca.nrc.cadc.uws.JobInfo;
import java.io.IOException;
import java.net.URI;
import java.net.URL;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.security.AccessControlException;
import java.security.PrivilegedActionException;
import java.security.PrivilegedExceptionAction;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.ListIterator;
import java.util.NoSuchElementException;
import javax.naming.Context;
import javax.naming.InitialContext;
import javax.naming.NamingException;
import javax.security.auth.Subject;
import org.apache.log4j.Logger;
import org.opencadc.pkg.server.PackageItem;
import org.opencadc.pkg.server.PackageRunner;
import org.opencadc.pkg.server.TarWriter;
import org.opencadc.vospace.ContainerNode;
import org.opencadc.vospace.DataNode;
import org.opencadc.vospace.LinkNode;
import org.opencadc.vospace.Node;
import org.opencadc.vospace.NodeNotFoundException;
import org.opencadc.vospace.VOS;
import org.opencadc.vospace.VOSURI;
import org.opencadc.vospace.View;
import org.opencadc.vospace.server.NodePersistence;
import org.opencadc.vospace.server.PathResolver;
import org.opencadc.vospace.server.Utils;
import org.opencadc.vospace.server.auth.VOSpaceAuthorizer;
import org.opencadc.vospace.server.transfers.TransferGenerator;
import org.opencadc.vospace.transfer.Protocol;
import org.opencadc.vospace.transfer.Transfer;
import org.opencadc.vospace.transfer.TransferParsingException;
import org.opencadc.vospace.transfer.TransferReader;

public class VospacePackageRunner extends PackageRunner {

    private static final Logger log = Logger.getLogger(VospacePackageRunner.class);
    private static final String CONTINUING_PROCESSING = ", continuing package processing...";
    private URI resourceID;
    private Transfer packageTransfer;
    private NodePersistence nodePersistence;
    private VOSpaceAuthorizer vospaceAuthorizer;
    private PathResolver pathResolver;
    private List<URI> targetList;
    private String appName;

    public VospacePackageRunner() {
    }

    /**
     * Sets the name of this instance which is used to generate the JNDI key to the NodePersistence,
     * and initialize other resources used by this class.
     *
     * @param appName the name of the instance.
     */
    @Override
    public void setAppName(String appName) {
        this.appName = appName;
        String jndiKey = appName + "-" + NodePersistence.class.getName();
        try {
            Context ctx = new InitialContext();
            this.nodePersistence = (NodePersistence) ctx.lookup(jndiKey);
            this.vospaceAuthorizer = new VOSpaceAuthorizer(nodePersistence);
            this.pathResolver = new PathResolver(nodePersistence, vospaceAuthorizer);
            this.resourceID = nodePersistence.getResourceID();
        } catch (NamingException e) {
            throw new RuntimeException("BUG: NodePersistence implementation not found with JNDI key " + jndiKey, e);
        }
    }

    /**
     * Get the list of targets for the package and the package name.
     */
    @Override
    protected void initPackage() {
        log.info("VospacePackageRunner init");
        log.debug("initPackage start");
        try {
            // check job is valid
            log.debug("job id passed in: " + job.getID());
            JobInfo jobInfo = job.getJobInfo();

            // Get the target list from the job
            TransferReader tr = new TransferReader();
            this.packageTransfer = tr.read(jobInfo.getContent(), VOSURI.SCHEME);
            this.targetList = packageTransfer.getTargets();

            StringBuilder sb = new StringBuilder();
            sb.append(appName).append("-download-");
            if (targetList.size() > 1) {
                sb.append(job.getID());
            } else {
                sb.append(getFilenameFromURI(targetList.get(0)));
            }
            this.packageName = sb.toString();
            log.debug("package name: " + this.packageName);
        } catch (IOException | TransferParsingException e) {
            throw new RuntimeException("ERROR parsing transfer document: ", e);
        }
        log.debug("initPackage end");
    }

    /**
     * Get the list of PackageItem's to be included in this package.
     *
     * @return List of PackageItem's.
     */
    @Override
    protected Iterator<PackageItem> getItems() {
        return new PackageItemIterator(targetList);
    }

    /**
     * Return the expected phase of the PackageRunner job.
     *
     * @return SUSPENDED ExecutionPhase
     */
    @Override
    protected ExecutionPhase getInitialPhase() {
        return ExecutionPhase.SUSPENDED;
    }

    /**
     * Get the response format MIME type from the package view in the transfer.
     *
     * @return MIME type
     */
    @Override
    protected String getResponseFormat() {
        String responseFormat = null;
        View packageView = packageTransfer.getView();
        if (packageView != null && packageView.getURI().equals(Standards.PKG_10)) {
            for (View.Parameter parameter : packageView.getParameters()) {
                if (parameter.getUri().equals(VOS.PROPERTY_URI_FORMAT)) {
                    responseFormat = parameter.getValue();
                    log.debug("found package response format: " + responseFormat);
                    break;
                }
            }
            log.debug("package response format not found in View parameters");
        } else {
            throw new IllegalStateException("VospacePackageRunner does not have expected a Transfer View: "
                    + Standards.PKG_10);
        }
        if (responseFormat == null) {
            responseFormat = TarWriter.MIME_TYPE;
        }
        return responseFormat;
    }

    /**
     * Get the URL to the given node VOSURI.
     *
     * @param nodeURI VOSURI to the node.
     * @return URL to the node in VOSpace.
     * @throws PrivilegedActionException for an error creating a transfer to get a URL to pull the node from VOSpace.
     * @throws NodeNotFoundException if the node is not found.
     */
    protected URL getURL(VOSURI nodeURI)
            throws PrivilegedActionException, NodeNotFoundException {
        log.debug("get node URL for: " + nodeURI.getURI().toASCIIString());
        String remoteIP = this.job.getRemoteIP();
        return Subject.doAs(AuthenticationUtil.getCurrentSubject(), (PrivilegedExceptionAction<URL>) () -> {
            // request anonymous https urls
            Protocol protocol = new Protocol(VOS.PROTOCOL_HTTPS_GET);
            protocol.setSecurityMethod(Standards.SECURITY_METHOD_ANON);
            packageTransfer.getProtocols().clear();
            packageTransfer.getProtocols().add(protocol);

            // Use a temporary Job with just the remote IP to avoid the original Job in
            // a transfer URL which may close the Job.
            Job pkgJob = new Job();
            pkgJob.setRemoteIP(remoteIP);
            TransferGenerator transferGenerator = nodePersistence.getTransferGenerator();
            List<Protocol> protocols = transferGenerator.getEndpoints(nodeURI, packageTransfer, pkgJob, null);
            log.debug("num transfer protocols: " + protocols.size());

            // Get the node endpoint from the first protocol
            if (protocols.isEmpty()) {
                throw new NodeNotFoundException("endpoint not found for: " + nodeURI);
            }
            URL nodeURL = new URL(protocols.get(0).getEndpoint());
            log.debug("found node URL: " + nodeURL.getFile());
            return nodeURL;
        });
    }

    // Get a PackageItem for a directory from the given ContainerNode.
    // The parentPath is the relative path to the ContainerNode parent in the package.
    protected PackageItem getDirectoryPackageItem(String parentPath, ContainerNode node) {
        String relativePath = parentPath + "/" + node.getName();
        return new PackageItem(relativePath);
    }

    // Get a PackageItem for a file from the given DataNode.
    // The parentPath is the relative path to the DataNode parent in the package.
    protected PackageItem getFilePackageItem(String parentPath, DataNode node)
            throws NodeNotFoundException, PrivilegedActionException {

        String nodeRelativePath = parentPath + "/" + node.getName();
        String nodePath = Utils.getPath(node);
        VOSURI nodeURI = new VOSURI(resourceID, nodePath);
        URL endpointURL = getURL(nodeURI);
        return new PackageItem(nodeRelativePath, endpointURL);
    }

    // Get a PackageItem for a symbolic link from the given LinkNode.
    // The parentPath is the relative path to the LinkNode parent in the package.
    protected PackageItem getSymbolicLinkPackageItem(String parentPath, LinkNode node) {

        // check that the link node target is in the path of a package targets
        URI linkTarget = node.getTarget();
        boolean targetInPackage = false;
        for (URI pkgTarget : targetList) {
            if (linkTarget.toASCIIString().startsWith(pkgTarget.toASCIIString())) {
                targetInPackage = true;
                break;
            }
        }
        if (!targetInPackage) {
            return null;
        }

        String relativePath = parentPath + "/" + node.getName();

        // get the relative path for the link in the package.
        // relativize throws a runtime if the two paths don't share a common root
        // which is possible if the link and target come from different package targets.
        // since all package targets are in the archive root, and it's a relative path,
        // prepend a temp path segment to both paths to avoid the exception.
        String nodePath = Utils.getPath(node);
        Path linkPath = Paths.get("tmp" + nodePath);
        Path targetPath = Paths.get("tmp" + linkTarget.getPath());
        Path linkRelativePath = null;
        try {
            linkRelativePath = linkPath.relativize(targetPath);
        } catch (IllegalArgumentException e) {
            log.debug(String.format("unable to create relative link %s -> %s", linkPath, targetPath));
        }
        if (linkRelativePath == null) {
            return null;
        }
        return new PackageItem(relativePath, linkRelativePath.toString());
    }

    // Build a filename from the provided URI.
    // The filename is the last element in the path for the URI.
    private static String getFilenameFromURI(URI uri) {
        String path = uri.getPath();
        int i = path.lastIndexOf("/");
        if (i >= 0) {
            path = path.substring(i + 1);
        }
        return path;
    }

    class PackageItemIterator implements Iterator<PackageItem> {

        private final Subject caller;
        private final List<RelativeContainerNode> deferredNodes;
        private final List<RelativeContainerNode> currentNodes;
        private final Iterator<Node> targetIterator;
        private ListIterator<RelativeContainerNode> currentIterator;
        private ResourceIterator<Node> childIterator;
        private PackageItem next = null;
        private String currentParentPath = "";

        public PackageItemIterator(List<URI> targets) {
            if (targets == null) {
                throw new IllegalArgumentException("list of targets is null");
            }
            this.targetIterator = getNodeIterator(targets);
            this.deferredNodes = new ArrayList<>();
            this.currentNodes = new ArrayList<>();
            this.caller = AuthenticationUtil.getCurrentSubject();
            advance();
        }

        /**
         * Returns {@code true} if the iteration has more elements.
         * (In other words, returns {@code true} if {@link #next} would
         * return an element rather than throwing an exception.)
         *
         * @return {@code true} if the iteration has more elements
         */
        @Override
        public boolean hasNext() {
            log.debug("hasNext(): " + (next != null));
            return next != null;
        }

        /**
         * Returns the next element in the iteration.
         *
         * @return the next element in the iteration
         * @throws NoSuchElementException if the iteration has no more elements
         */
        @Override
        public PackageItem next() {
            log.debug("next().start");
            if (next == null) {
                throw new NoSuchElementException("no more PackageItem's");
            }
            PackageItem current = next;
            advance();
            log.debug(String.format("current: %s next: %s", current, next == null ? null : next));
            log.debug("next().end");
            return current;
        }

        /**
         * Find the PackageItem to return as next().
         */
        private void advance() {
            try {
                log.debug("advance().start");
                next = null;

                // process list of given targets, returning a PackageItem for a DataNode
                // or a LinkNode, and adding ContainerNode's to a list of deferred nodes to processed later.
                while (targetIterator.hasNext()) {
                    Node node = targetIterator.next();
                    targetIterator.remove();
                    if (node != null) {
                        log.debug("target: " + node.getName());
                        next = doChildNode("", node);
                        if (next != null) {
                            log.debug("return next: " + next);
                            return;
                        }
                    }
                }

                // process deferred container nodes;
                boolean hasNext = true;
                while (hasNext) {
                    // if the currentNodeIterator is null or empty, move the deferred nodes into currentNodes
                    // a refresh to iterator to the currentNodes.
                    log.debug(String.format("empty: currentIterator - %s, deferredNodes - %s",
                            isCurrentIteratorEmpty(), deferredNodes.isEmpty()));
                    if (isCurrentIteratorEmpty() && !deferredNodes.isEmpty()) {
                        // copy deferredNodes into currentNodes for processing
                        currentNodes.addAll(deferredNodes);
                        deferredNodes.clear();
                        log.debug(String.format("copied %s deferred nodes to current", currentNodes.size()));

                        // update currentNodesIterator
                        currentIterator = currentNodes.listIterator();
                        log.debug("updated currentIterator");
                    }

                    // if the children iterator is empty and the parent iterator is not,
                    // get the next parent and use it's children to update the children iterator.
                    log.debug(String.format("empty: currentIterator - %s, childIterator - %s",
                            isCurrentIteratorEmpty(), isChildIteratorEmpty()));
                    if (isChildIteratorEmpty() && !isCurrentIteratorEmpty()) {
                        // get the next parent (container) node and it's package-item
                        RelativeContainerNode currentNode = currentIterator.next();
                        log.debug("currentIterator next: " + currentNode.node.getName());
                        currentIterator.remove();
                        next = getDirectoryPackageItem(currentNode.parentPath, currentNode.node);
                        currentParentPath = next.getRelativePath();

                        // check read access to the parent and if granted refresh the childIterator
                        boolean canRead = vospaceAuthorizer.hasSingleNodeReadPermission(currentNode.node, caller);
                        log.debug(String.format("%s read permission: %s", currentNode.node.getName(), canRead));
                        if (canRead) {
                            childIterator = nodePersistence.iterator(currentNode.node, null, null);
                            log.debug("refreshed childIterator for: " + currentNode.node.getName());
                        }
                        // return the container PackageItem
                        log.debug("return next: " + next);
                        return;
                    }

                    // loop through the child nodes for the next PackageItem.
                    if (!isChildIteratorEmpty()) {
                        while (childIterator.hasNext()) {
                            Node child = childIterator.next();
                            log.debug("childIterator next: " + child.getName());
                            next = doChildNode(currentParentPath, child);
                            if (next != null) {
                                log.debug("return next: " + next);
                                return;
                            }
                        }
                    }

                    if (isChildIteratorEmpty() && isCurrentIteratorEmpty() && deferredNodes.isEmpty()) {
                        hasNext = false;
                        log.debug("iterators and deferred nodes empty, exit");
                    }
                }
            } finally {
                log.debug("advance().end");
            }
        }

        /**
         * Check if the childIterator is null or empty.
         */
        private boolean isChildIteratorEmpty() {
            return childIterator == null || !childIterator.hasNext();
        }

        /**
         * Check if the currentIterator is null or empty.
         */
        private boolean isCurrentIteratorEmpty() {
            return currentIterator == null || !currentIterator.hasNext();
        }

        /**
         * Get the list of nodes for the given list of target URI.
         *
         * @param targets list of target URI.
         * @return Iterator for the target Node's.
         */
        private Iterator<Node> getNodeIterator(List<URI> targets) {
            log.debug("getNodeIterator().start");
            List<Node> targetNodes = new ArrayList<>();
            for (URI target : targets) {
                VOSURI vosURI = new VOSURI(target);
                String nodePath = vosURI.getPath();
                try {
                    Node node = pathResolver.getNode(nodePath, true);
                    targetNodes.add(node);
                    log.debug(String.format("target %s -> node %s", target.toASCIIString(), node.getName()));
                } catch (Exception e) {
                    log.debug("skipping target, read permission denied: " + nodePath);
                }
            }
            log.debug("# nodes: " +  targetNodes.size());
            log.debug("getNodeIterator().end");
            return targetNodes.iterator();
        }

        /**
         * Process the child nodes of a container.
         * If the node is a ContainerNode, add the node to the list of container nodes for deferred processing.
         * if the node is a DataNode or LinkNode, create a PackageItem for the node,
         * returning null if unable to create the PackageItem for the node.
         *
         * @param child child Node.
         * @return a PackageItem, or null if the node is a ContainerNode, or error creating the PackageItem.
         */
        private PackageItem doChildNode(String parentPath, Node child) {
            log.debug("doChildNode().start");
            PackageItem packageItem = null;
            try {
                if (child instanceof ContainerNode) {
                    deferredNodes.add(new RelativeContainerNode(parentPath, (ContainerNode) child));
                    log.debug(child.getName() + " added to deferred nodes");
                } else {
                    boolean canRead = vospaceAuthorizer.hasSingleNodeReadPermission(child, caller);
                    if (!canRead) {
                        log.debug(child.getName() + " read permission denied");
                    } else if (child instanceof DataNode) {
                        packageItem = getFilePackageItem(parentPath, (DataNode) child);
                    } else if (child instanceof LinkNode) {
                        packageItem = getSymbolicLinkPackageItem(parentPath, (LinkNode) child);
                    } else {
                        log.info("unknown node type: " + Utils.getPath(child) + CONTINUING_PROCESSING);
                    }
                    log.debug("return: " + packageItem);
                }
            } catch (AccessControlException e) {
                log.info(String.format("permission denied: %s %s", child.getName(), CONTINUING_PROCESSING));
            } catch (NodeNotFoundException e) {
                log.info(String.format("node not found: %s %s", child.getName(), CONTINUING_PROCESSING));
            } catch (TransientException e) {
                log.info(String.format("transientException: %s %s", child.getName(), CONTINUING_PROCESSING));
            } catch (Exception e) {
                log.info(String.format("%s: %s%s", e.getClass().getName(), child.getName(), CONTINUING_PROCESSING));
            }
            log.debug("doChildNode().end");
            return packageItem;
        }

    }

    /**
     * Class that holds a container node, and the path
     * from the target to the container node parent.
     */
    static class RelativeContainerNode {
        public String parentPath;
        public ContainerNode node;

        /**
         *
         * @param parentPath the path from the target to node parent.
         * @param node a ContainerNode
         */
        public RelativeContainerNode(String parentPath, ContainerNode node) {
            this.parentPath = parentPath;
            this.node = node;
        }
    }

}
