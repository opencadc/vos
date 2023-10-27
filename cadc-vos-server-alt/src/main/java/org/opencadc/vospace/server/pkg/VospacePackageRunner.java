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
import ca.nrc.cadc.uws.Job;
import ca.nrc.cadc.uws.JobInfo;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URI;
import java.net.URL;
import java.security.AccessControlException;
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
import org.opencadc.vospace.LinkingException;
import org.opencadc.vospace.Node;
import org.opencadc.vospace.NodeNotFoundException;
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
     *
     * @param appName
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
     *
     * @throws IllegalArgumentException
     */
    @Override
    protected void initPackage() throws IllegalArgumentException {
        log.debug("initPackage start");
        try {
            // check job is valid
            log.debug("job id passed in: " + job.getID());
            JobInfo jobInfo = job.getJobInfo();
            if (jobInfo != null) {
                throw new RuntimeException("null jobInfo: " + job.toString());
            }

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
     *
     * @return
     * @throws IOException
     */
    @Override
    protected Iterator<PackageItem> getItems() throws IOException {
        // packageTransfer is populated in initPackage
        List<URI> targetList = packageTransfer.getTargets();
        log.debug("target list: " + targetList.toString());

        Subject caller = AuthenticationUtil.getCurrentSubject();
        log.debug("subject: " + caller);

        List<PackageItem> packageItems = new ArrayList<>();
        for (URI targetURI : targetList) {
            VOSURI nodeURI = new VOSURI(targetURI);
            log.debug("node: " + nodeURI);

            addPackageItem(nodeURI, packageItems, caller);
        }
        return packageItems.iterator();
    }

    /**
     *
     * @param nodeURI
     * @param packageItems
     * @param caller
     */
    private void addPackageItem(VOSURI nodeURI, List<PackageItem> packageItems, Subject caller) {
        String nodePath = nodeURI.getPath();
        log.debug("node path: " + nodePath);

        try {
            // PathResolver checks read permission for the node path
            Node node = pathResolver.getNode(nodePath);
            if (node == null) {
                throw NodeFault.NodeNotFound.getStatus(nodePath);
            }

            if (node instanceof DataNode) {
                addDataNode((DataNode) node, packageItems);
            } else if (node instanceof ContainerNode) {
                addContainerNode((ContainerNode) node, packageItems, caller);
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

    private void addDataNode(DataNode node, List<PackageItem> packageItems)
            throws Exception {
        String nodePath = Utils.getPath(node);
        log.debug("DataNode path: " + nodePath);

        VOSURI nodeURI = new VOSURI(this.resourceID, nodePath);
        log.debug("DataNode uri: " + nodeURI);

        // Use a temporary Job with just the remote IP to avoid the original Job in
        // a transfer URL which may close the Job.
        Job pkgJob = new Job();
        pkgJob.setRemoteIP(this.job.getRemoteIP());
        TransferGenerator transferGenerator = nodePersistence.getTransferGenerator();
        List<Protocol> protocols = transferGenerator.getEndpoints(nodeURI, packageTransfer, pkgJob, null);

        // Get the node endpoint from the first protocol
        Protocol protocol = protocols.get(0);
        String endpoint = protocol.getEndpoint();
        log.debug("DataNode endpoint:" + endpoint);

        packageItems.add(new PackageItem(new URL(endpoint), nodePath));
    }

    private void addContainerNode(ContainerNode node, List<PackageItem> packageItems, Subject caller)
            throws Exception {

        List<ContainerNode> childContainers = new ArrayList<>();

        try (ResourceIterator<Node> iterator = nodePersistence.iterator(node, null, null)) {
            while (iterator.hasNext()) {
                Node child = iterator.next();
                boolean canRead = vospaceAuthorizer.hasSingleNodeReadPermission(child, caller);
                if (child instanceof ContainerNode) {
                    if (canRead) {
                        childContainers.add((ContainerNode) child);
                    } else {
                        // add empty container node to the package
                        packageItems.add(getPackageItem(node));
                    }
                } else if (child instanceof DataNode) {
                    if (canRead) {
                        addDataNode((DataNode) child, packageItems);
                    }
                } else {
                    log.info("unrecognized or unsupported node type " + Utils.getPath(node) + CONTINUING_PROCESSING);
                }
            }
        }

        if (!childContainers.isEmpty()) {
            for (ContainerNode child : childContainers) {
                addContainerNode(child, packageItems, caller);
            }
        }
    }

    /**
     * Build a filename from the URI provided
     * @return - name of last element in URI path.
     */
    private static String getFilenameFromURI(URI uri) {
        String path = uri.getPath();
        int i = path.lastIndexOf("/");
        if (i >= 0) {
            path = path.substring(i + 1);
        }
        return path;
    }

    private PackageItem getPackageItem(Node node) throws Exception {
        String nodePath = Utils.getPath(node);
        VOSURI nodeURI = new VOSURI(this.resourceID, nodePath);
        log.debug("Node uri: " + nodeURI);

        // Use a temporary Job with just the remote IP to avoid the original Job in
        // a transfer URL which may close the Job.
        // Use a temporary Job with just the remote IP to avoid the original Job in
        // a transfer URL which may close the Job.
        Job pkgJob = new Job();
        pkgJob.setRemoteIP(this.job.getRemoteIP());
        TransferGenerator transferGenerator = nodePersistence.getTransferGenerator();
        List<Protocol> protocols = transferGenerator.getEndpoints(nodeURI, packageTransfer, pkgJob, null);

        // Get the node endpoint from the first protocol
        Protocol protocol = protocols.get(0);
        URL endpoint = new URL(protocol.getEndpoint());
        log.debug("Node endpoint:" + endpoint);
        return new PackageItem(endpoint, nodePath);
    }

}
