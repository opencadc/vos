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
import ca.nrc.cadc.uws.Job;
import ca.nrc.cadc.uws.JobInfo;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URI;
import java.net.URL;
import java.security.AccessControlException;
import java.security.PrivilegedActionException;
import java.security.PrivilegedExceptionAction;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import javax.naming.Context;
import javax.naming.InitialContext;
import javax.naming.NamingException;
import javax.security.auth.Subject;
import org.apache.log4j.Logger;
import org.opencadc.pkg.server.PackageItem;
import org.opencadc.pkg.server.PackageRunner;
import org.opencadc.vospace.ContainerNode;
import org.opencadc.vospace.DataNode;
import org.opencadc.vospace.LinkNode;
import org.opencadc.vospace.LinkingException;
import org.opencadc.vospace.Node;
import org.opencadc.vospace.NodeNotFoundException;
import org.opencadc.vospace.VOS;
import org.opencadc.vospace.VOSURI;
import org.opencadc.vospace.server.NodeFault;
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

    public VospacePackageRunner() {}

    /**
     * Sets the name of this instance which is used to generate the JNDI key to the NodePersistence,
     * and initialize other resources used by this class.
     *
     * @param appName the name of the instance.
     */
    @Override
    public void setAppName(String appName) {
        String jndiKey = appName + "-" + NodePersistence.class.getName();
        try {
            Context ctx = new InitialContext();
            this.nodePersistence = (NodePersistence) ctx.lookup(jndiKey);
            this.vospaceAuthorizer = new VOSpaceAuthorizer(nodePersistence);
            this.pathResolver = new PathResolver(nodePersistence, vospaceAuthorizer, true);
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
        log.debug("initPackage start");
        try {
            // check job is valid
            log.debug("job id passed in: " + job.getID());
            JobInfo jobInfo = job.getJobInfo();

            // Get the target list from the job
            TransferReader tr = new TransferReader();
            this.packageTransfer = tr.read(jobInfo.getContent(), VOSURI.SCHEME);
            List<URI> targetList = packageTransfer.getTargets();
            if (targetList.size() > 1) {
                this.packageName = "cadc-download-" + job.getID();
            } else {
                this.packageName = getFilenameFromURI(targetList.get(0));
            }
            log.debug("package name: " + this.packageName);
        } catch (IOException | TransferParsingException e) {
            throw new RuntimeException("ERROR reading jobInfo: ", e);
        }
        log.debug("initPackage done");
    }

    /**
     * Get the list of PackageItem's to be included in this package.
     *
     * @return List of PackageItem's.
     */
    @Override
    protected Iterator<PackageItem> getItems() {
        // packageTransfer is populated in initPackage
        List<URI> targetList = packageTransfer.getTargets();
        log.debug("target list: " + targetList.toString());

        List<PackageItem> packageItems = new ArrayList<>();
        for (URI targetURI : targetList) {
            VOSURI nodeURI = new VOSURI(targetURI);
            log.debug("target nodeURI: " + nodeURI);

            addPackageItem(nodeURI, packageItems);
        }
        return packageItems.iterator();
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
            if (protocols.size() == 0) {
                throw new NodeNotFoundException("endpoint not found for: " + nodeURI);
            }
            return new URL(protocols.get(0).getEndpoint());
        });
    }

    // Add a PackageItem for the given VOSURI to the list of PackageItems's.
    private void addPackageItem(VOSURI nodeURI, List<PackageItem> packageItems) {
        String nodePath = nodeURI.getPath();
        log.debug("add node path: " + nodePath);

        try {
            // PathResolver checks read permission for the root node path
            Node node;
            try {
                node = pathResolver.getNode(nodePath);
            } catch (AccessControlException e) {
                log.debug("read permission denied to node path: " + nodePath);
                return;
            }

            if (node == null) {
                throw NodeFault.NodeNotFound.getStatus(nodePath);
            }

            if (node instanceof ContainerNode) {
                addContainerNode((ContainerNode) node, packageItems);
            } else if (node instanceof DataNode) {
                packageItems.add(getFilePackageItem((DataNode) node));
            } else if (node instanceof LinkNode) {
                packageItems.add(getSymbolicLinkPackageItem((LinkNode) node));
            } else {
                log.info("unrecognized or unsupported node type " + nodeURI + CONTINUING_PROCESSING);
            }
        } catch (AccessControlException e) {
            log.info("permission denied: " + nodeURI + CONTINUING_PROCESSING);
        } catch (NodeNotFoundException e) {
            log.info("node not found: " + nodeURI + CONTINUING_PROCESSING);
        } catch (FileNotFoundException e) {
            log.info("couldn't generate endpoints: " + nodeURI + CONTINUING_PROCESSING);
        } catch (LinkingException e) {
            log.info("linking exception: " + nodeURI + CONTINUING_PROCESSING);
        } catch (MalformedURLException e) {
            log.info("malformed URL: " + nodeURI + CONTINUING_PROCESSING);
        } catch (TransientException e) {
            log.info("transientException: " + nodeURI + CONTINUING_PROCESSING);
        } catch (Exception e) {
            log.info(String.format("%s: %s%s", e.getClass().getName(), nodeURI, CONTINUING_PROCESSING));
        }
    }

    // Add a ContainerNode and any child nodes to the list of PackageItem's.
    private void addContainerNode(ContainerNode node, List<PackageItem> packageItems)
            throws IOException, NodeNotFoundException, PrivilegedActionException {

        log.debug("add ContainerNode: " + node);
        List<ContainerNode> childContainers = new ArrayList<>();
        Subject caller = AuthenticationUtil.getCurrentSubject();

        try (ResourceIterator<Node> iterator = nodePersistence.iterator(node, null, null)) {
            while (iterator.hasNext()) {
                Node child = iterator.next();
                boolean canRead = vospaceAuthorizer.hasSingleNodeReadPermission(child, caller);

                if (child instanceof ContainerNode) {
                    log.debug(String.format("child ContainerNode: %s canRead: %s", child, canRead));
                    if (canRead) {
                        // add container to queue to be processed after data and link nodes
                        // to use a single iterator
                        childContainers.add((ContainerNode) child);
                        log.debug("add ContainerNode to queue");
                    } else {
                        // for containers that can't be read, add empty container to the package
                        PackageItem packageItem = getDirectoryPackageItem((ContainerNode) child);
                        packageItems.add(packageItem);
                        log.debug("add empty ContainerNode");
                    }
                } else if (child instanceof DataNode) {
                    log.debug(String.format("child DataNode: %s canRead: %s", child, canRead));
                    if (canRead) {
                        PackageItem packageItem = getFilePackageItem((DataNode) child);
                        packageItems.add(packageItem);
                        log.debug("add DataNode to packages");
                    } else {
                        log.debug("skip DataNode");
                    }
                } else if (child instanceof LinkNode) {
                    log.debug(String.format("child LinkNode: %s canRead: %s", child, canRead));
                    if (canRead) {
                        PackageItem packageItem = getSymbolicLinkPackageItem((LinkNode) child);
                        packageItems.add(packageItem);
                        log.debug("add LinkNode to packages");
                    } else {
                        log.debug("skip LinkNode");
                    }
                } else {
                    log.info("unrecognized or unsupported node type " + Utils.getPath(child) + CONTINUING_PROCESSING);
                }
            }
        }

        if (!childContainers.isEmpty()) {
            log.debug("process container nodes");
            for (ContainerNode child : childContainers) {
                log.debug("process: " + child);
                addContainerNode(child, packageItems);
            }
        }
    }

    // Get a PackageItem for a directory from the given ContainerNode.
    protected PackageItem getDirectoryPackageItem(ContainerNode node) {
        String nodePath = Utils.getPath(node);
        log.debug("ContainerNode path: " + nodePath);

        return new PackageItem(nodePath);
    }

    // Get a PackageItem for a file from the given DataNode.
    protected PackageItem getFilePackageItem(DataNode node)
            throws PrivilegedActionException, NodeNotFoundException {
        String nodePath = Utils.getPath(node);
        log.debug("DataNode path: " + nodePath);

        VOSURI nodeURI = new VOSURI(this.resourceID, nodePath);
        URL endpointURL = getURL(nodeURI);
        log.debug("DataNode URL:" + endpointURL);

        return new PackageItem(nodePath, endpointURL);
    }

    // Get a PackageItem for a symbolic link from the given LinkNode.
    protected PackageItem getSymbolicLinkPackageItem(LinkNode node) {
        String nodePath = Utils.getPath(node);
        log.debug("LinkNode path: " + nodePath);

        URI targetURI = node.getTarget();
        VOSURI nodeURI = new VOSURI(this.resourceID, nodePath);
        String linkPath = nodeURI.getPath();
        log.debug("LinkNode target path: " + linkPath);

        return new PackageItem(nodePath, linkPath);
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

}
