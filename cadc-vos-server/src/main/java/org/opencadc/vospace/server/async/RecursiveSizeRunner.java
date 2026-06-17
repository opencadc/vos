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
import ca.nrc.cadc.auth.AuthMethod;
import ca.nrc.cadc.io.ResourceIterator;
import ca.nrc.cadc.net.TransientException;
import ca.nrc.cadc.reg.Standards;
import ca.nrc.cadc.reg.client.RegistryClient;
import ca.nrc.cadc.uws.Job;
import ca.nrc.cadc.uws.JobInfo;
import ca.nrc.cadc.uws.Parameter;
import ca.nrc.cadc.uws.Result;
import ca.nrc.cadc.uws.server.JobNotFoundException;
import ca.nrc.cadc.uws.server.JobPersistence;
import ca.nrc.cadc.uws.server.JobPersistenceException;

import java.net.URI;
import java.net.URL;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import javax.security.auth.Subject;

import org.apache.log4j.Logger;
import org.opencadc.vospace.ContainerNode;
import org.opencadc.vospace.DataNode;
import org.opencadc.vospace.Node;
import org.opencadc.vospace.VOSURI;
import org.opencadc.vospace.server.Utils;

/**
 * Async job runner that computes recursive byte usage for an allocation.
 */
public class RecursiveSizeRunner extends AbstractRecursiveRunner {

    private static final Logger log = Logger.getLogger(RecursiveSizeRunner.class);

    private static final String SIZE_REPORT_SERVLET_PATH = "/size-report";

    private final RegistryClient regClient = new RegistryClient();

    private long totalBytes = 0L;
    private int maxDepth = 0;
    private final Map<String, Long> reportLines = new LinkedHashMap<>();
    private Boolean sortDesc = null;

    @Override
    public void setJob(Job job) {
        this.job = job;

        for (Parameter p : job.getParameterList()) {
            if ("target".equalsIgnoreCase(p.getName())) {
                this.target = new VOSURI(URI.create(p.getValue()));
            } else if ("maxdepth".equalsIgnoreCase(p.getName())) {
                try {
                    int v = Integer.parseInt(p.getValue());
                    if (v < 0) {
                        v = 0;
                    }
                    this.maxDepth = v; // TODO: max should be 5?
                } catch (NumberFormatException ignore) {
                    // keep default 0
                }
            } else if ("sort".equalsIgnoreCase(p.getName())) {
                if ("desc".equalsIgnoreCase(p.getValue())) {
                    this.sortDesc = true;
                } else if ("asc".equalsIgnoreCase(p.getValue())) {
                    this.sortDesc = false;
                } else {
                    throw new IllegalArgumentException("Unknown sort value: " + p.getValue() + ". must be asc or desc");
                }
            }
        }
        if (target == null) {
            throw new IllegalArgumentException("target argument required");
        }
        log.debug("target: " + target + " maxDepth=" + maxDepth + " sortDesc=" + sortDesc);
    }

    @Override
    protected boolean performAction(Node node) throws Exception {
        if (!(node instanceof ContainerNode)) {
            sendError("target must be a ContainerNode");
            return false;
        }
        ContainerNode root = (ContainerNode) node;
        if (!nodePersistence.isAllocation(root)) {
            sendError("target is not an allocation root");
            return false;
        }
        Subject subject = AuthenticationUtil.getCurrentSubject();
        if (!authorizer.hasSingleNodeReadPermission(root, subject)) {
            sendError("no read permission on allocation root");
            return false;
        }

        Long nodeSize = accumulateNodeSize(root, subject, 0);
        totalBytes = nodeSize >= 0L ? nodeSize : 0L;
        addToReport(Utils.getPath(root), nodeSize, 0);
        return nodeSize >= 0L; // success if permissions are OK
    }

    private void addToReport(String path, long size, int depth) {
        if (depth <= maxDepth) {
            reportLines.put(path, size);
        }
    }

    private Long accumulateNodeSize(Node node, Subject subject, int depth) throws Exception {
        if (Thread.currentThread().isInterrupted()) {
            throw new InterruptedException();
        }

        checkJobPhase();

        if (!authorizer.hasSingleNodeReadPermission(node, subject)) {
            log.debug("no read permission on node " + Utils.getPath(node));
            // incErrorCount(); // Not considering this as an error. Just reporting permission denied for this node and continuing with the rest of the tree.
            addToReport(Utils.getPath(node), -1L, depth);
            return -1L;
        }

        if (node instanceof DataNode) {
            throw new RuntimeException("BUG: DataNode not expected");
        }

        ContainerNode cn = (ContainerNode) node;

        // TODO: Need to check if the bytesUsed is in sync. If not, need to remove this block of code to keep the recursion going.
        if (depth >= maxDepth && cn.bytesUsed != null) {
            incSuccessCount();
            addToReport(Utils.getPath(cn), cn.bytesUsed, depth);
            return cn.bytesUsed;
        }

        long currentNodeSize = 0L;
        try (ResourceIterator<Node> iter = nodePersistence.iterator(cn, null, null)) {
            while (iter.hasNext()) {
                Node child = iter.next();
                if (child instanceof ContainerNode) {
                    Long childNodeSize = accumulateNodeSize(child, subject, depth + 1);
                    if (childNodeSize > 0L) {
                        currentNodeSize += childNodeSize;
                    }
                } else {
                    if (!authorizer.hasSingleNodeReadPermission(child, subject)) {
                        log.debug("Skipping this Data node for no read permission: " + Utils.getPath(child));
                        // incErrorCount(); // Not considering this as an error. Skipping this data node.
                        continue;
                    }

                    DataNode dn = (DataNode) child;
                    Long bytes = dn.bytesUsed;
                    if (bytes == null) {
                        bytes = 0L;
                    }
                    currentNodeSize += bytes;
                    incSuccessCount();
                }
            }
        }

        incSuccessCount();
        addToReport(Utils.getPath(cn), currentNodeSize, depth);
        return currentNodeSize;
    }

    @Override
    protected List<Result> getAdditionalResults() {
        try {
            String reportText = formatReport();
            persistReport(reportText);

            List<Result> out = new ArrayList<>();
            out.add(new Result("bytesUsed", URI.create("final:" + totalBytes)));
            out.add(new Result(SizeReportServlet.RESULT_NAME, buildReportURI()));
            return out;
        } catch (Exception ex) {
            throw new RuntimeException("failed to persist allocation-size report", ex);
        }
    }

    private String formatReport() {
        List<Map.Entry<String, Long>> lines = new ArrayList<>(reportLines.entrySet());

        if (sortDesc != null) {
            Comparator<Map.Entry<String, Long>> bySize = Map.Entry.comparingByValue();
            if (sortDesc) {
                bySize = bySize.reversed();
            }
            lines = lines.stream().sorted(bySize).collect(Collectors.toList());
        }

        StringBuilder sb = new StringBuilder();
        for (Map.Entry<String, Long> entry : lines) {
            sb.append(entry.getValue() == -1L ? "Permission Denied" : entry.getValue())
                    .append('\t').append(entry.getKey()).append('\n');
        }
        return sb.toString();
    }

    private void persistReport(String reportText) throws TransientException, JobPersistenceException, JobNotFoundException {
        if (!(getJobUpdater() instanceof JobPersistence)) {
            throw new IllegalStateException("BUG: JobUpdater is not JobPersistence");
        }
        JobPersistence jobPersistence = (JobPersistence) getJobUpdater();
        Job persisted = jobPersistence.get(job.getID());
        JobInfo jobInfo = new JobInfo(SizeReportServlet.CONTENT_TYPE, Boolean.TRUE);
        jobInfo.getContent().add(reportText);
        persisted.setJobInfo(jobInfo);
        jobPersistence.put(persisted);
    }

    private URI buildReportURI() {
        Subject subject = AuthenticationUtil.getCurrentSubject();
        AuthMethod authMethod = AuthenticationUtil.getAuthMethod(subject);
        URL nodesURL = regClient.getServiceURL(nodePersistence.getResourceID(), Standards.VOSPACE_NODES_20, authMethod);
        String base = nodesURL.toExternalForm().replace("/nodes", SIZE_REPORT_SERVLET_PATH);
        return URI.create(base + "/" + job.getID());
    }
}
