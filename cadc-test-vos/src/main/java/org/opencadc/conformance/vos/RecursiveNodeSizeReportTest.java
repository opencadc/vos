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

package org.opencadc.conformance.vos;

import ca.nrc.cadc.auth.RunnableAction;
import ca.nrc.cadc.auth.SSLUtil;
import ca.nrc.cadc.net.FileContent;
import ca.nrc.cadc.net.HttpGet;
import ca.nrc.cadc.net.HttpPost;
import ca.nrc.cadc.net.HttpUpload;
import ca.nrc.cadc.reg.Standards;
import ca.nrc.cadc.uws.ExecutionPhase;
import ca.nrc.cadc.uws.Job;
import ca.nrc.cadc.uws.JobReader;
import ca.nrc.cadc.uws.Result;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.StringReader;
import java.io.StringWriter;
import java.net.MalformedURLException;
import java.net.URI;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import javax.security.auth.Subject;
import org.apache.log4j.Logger;
import org.junit.Assert;
import org.junit.Test;
import org.opencadc.gms.GroupURI;
import org.opencadc.vospace.ContainerNode;
import org.opencadc.vospace.DataNode;
import org.opencadc.vospace.Node;
import org.opencadc.vospace.NodeNotSupportedException;
import org.opencadc.vospace.VOS;
import org.opencadc.vospace.VOSURI;
import org.opencadc.vospace.io.NodeParsingException;
import org.opencadc.vospace.io.NodeReader;
import org.opencadc.vospace.transfer.Direction;
import org.opencadc.vospace.transfer.Protocol;
import org.opencadc.vospace.transfer.Transfer;
import org.opencadc.vospace.transfer.TransferParsingException;
import org.opencadc.vospace.transfer.TransferReader;
import org.opencadc.vospace.transfer.TransferWriter;

/**
 * Tests the recursive async-nodesize endpoint.
 * */
public class RecursiveNodeSizeReportTest extends VOSTest {

    private static final Logger log = Logger.getLogger(RecursiveNodeSizeReportTest.class);

    // Root-level allocation for async node-size-report tests
    private static final String ALLOCATION_ROOT = "vault-async-size-test";

    private static final long FILE_A_BYTES = 100L;
    private static final long FILE_B_BYTES = 250L;

    private static final long D1_F1_BYTES = 100L;
    private static final long D1_D2_F2_BYTES = 200L;
    private static final long D3_F3_BYTES = 400L;
    private static final long DENIED_F4_BYTES = 300L;
    private static final long DEPTH_TOTAL_BYTES = D1_F1_BYTES + D1_D2_F2_BYTES + D3_F3_BYTES + DENIED_F4_BYTES;
    private static final long DEPTH_VISIBLE_TO_GROUP_BYTES = D1_F1_BYTES + D1_D2_F2_BYTES + D3_F3_BYTES;

    private Subject groupMember;
    private GroupURI accessGroup;

    public RecursiveNodeSizeReportTest(URI resourceID, File testCert) {
        super(resourceID, testCert);
    }

    protected void enablePermissionTests(GroupURI accessGroup, File groupMemberCert) {
        this.accessGroup = accessGroup;
        this.groupMember = SSLUtil.createSubject(groupMemberCert);
    }

    // Test the /async-nodesize endpoint without any filter/sorting parameters
    @Test
    public void testAllocationAsyncSize() throws Exception {
        String alloc = ALLOCATION_ROOT + "/";
        String fileA = alloc + "fileA";
        String subDir = alloc + "sub/";
        String fileB = subDir + "fileB";
        String[] tree = {alloc, fileA, subDir, fileB};

        try {
            createRootNodeTree(tree);
            uploadData(fileA, FILE_A_BYTES);
            uploadData(fileB, FILE_B_BYTES);
            waitForBytesUsed(fileA, FILE_A_BYTES);
            waitForBytesUsed(fileB, FILE_B_BYTES);

            Job job = postAllocationSize(nodeSizeReportServiceURL, getRootVOSURI(ALLOCATION_ROOT), authSubject, null);

            Assert.assertEquals(ExecutionPhase.COMPLETED, job.getExecutionPhase());

            long bytesUsed = getResultLong(job, "bytesUsed");
            Assert.assertEquals(FILE_A_BYTES + FILE_B_BYTES, bytesUsed);
            Assert.assertTrue(getResultLong(job, "successcount") >= 2L);

            Map<String, Long> report = fetchNodeSizeReport(authSubject);
            Assert.assertEquals(Long.valueOf(FILE_A_BYTES + FILE_B_BYTES), report.get("/" + ALLOCATION_ROOT));
        } finally {
            String[] cleanupTree = new String[]{alloc, fileA, subDir, fileB, alloc + "report"};
            cleanupRootNodeTree(cleanupTree);
        }
    }

    @Test
    public void testAllocationAsyncSizeWithParams() throws Exception {
        log.debug("Testing /async-nodesize for different params.");
        buildNodesTree();
        try {
            testAllocationAsyncSizeMaxDepth0();
            testAllocationAsyncSizeMaxDepth1();
            testAllocationAsyncSizeMaxDepth2();

            testAllocationAsyncSizeSortAsc();
            testAllocationAsyncSizeSortDesc();
        } finally {
            cleanupNodesTree();
        }
    }

    private void testAllocationAsyncSizeMaxDepth0() throws Exception {
        log.debug("Testing /async-nodesize for maxdepth=0:");
        Job job = postAllocationSize(nodeSizeReportServiceURL, getRootVOSURI(ALLOCATION_ROOT), authSubject, Map.of("maxdepth", "0"));
        Assert.assertEquals(ExecutionPhase.COMPLETED, job.getExecutionPhase());
        Assert.assertEquals(DEPTH_TOTAL_BYTES, getResultLong(job, "bytesUsed"));

        Map<String, Long> report = fetchNodeSizeReport(authSubject);
        Assert.assertEquals(1, report.size());
        Assert.assertEquals(Long.valueOf(DEPTH_TOTAL_BYTES), report.get("/" + ALLOCATION_ROOT));
    }

    private void testAllocationAsyncSizeMaxDepth1() throws Exception {
        log.debug("Testing /async-nodesize for maxdepth=1:");
        Job job = postAllocationSize(nodeSizeReportServiceURL, getRootVOSURI(ALLOCATION_ROOT), authSubject, Map.of("maxdepth", "1"));
        Assert.assertEquals(ExecutionPhase.COMPLETED, job.getExecutionPhase());

        Map<String, Long> parsedReport = fetchNodeSizeReport(authSubject);
        Assert.assertEquals(4, parsedReport.size());
        Assert.assertEquals(Long.valueOf(DEPTH_TOTAL_BYTES), parsedReport.get("/" + ALLOCATION_ROOT));
        Assert.assertEquals(Long.valueOf(D1_F1_BYTES + D1_D2_F2_BYTES), parsedReport.get("/" + ALLOCATION_ROOT + "/d1"));
        Assert.assertEquals(Long.valueOf(D3_F3_BYTES), parsedReport.get("/" + ALLOCATION_ROOT + "/d3"));
        Assert.assertEquals(Long.valueOf(DENIED_F4_BYTES), parsedReport.get("/" + ALLOCATION_ROOT + "/denied"));
        Assert.assertNull(parsedReport.get("/" + ALLOCATION_ROOT + "/d1/d2"));
    }

    private void testAllocationAsyncSizeMaxDepth2() throws Exception {
        log.debug("Testing /async-nodesize for maxdepth=2:");
        Job job = postAllocationSize(nodeSizeReportServiceURL, getRootVOSURI(ALLOCATION_ROOT),
                authSubject, Map.of("maxdepth", "2"));
        Assert.assertEquals(ExecutionPhase.COMPLETED, job.getExecutionPhase());

        Map<String, Long> report = fetchNodeSizeReport(authSubject);
        Assert.assertEquals(5, report.size());
        Assert.assertEquals(Long.valueOf(DEPTH_TOTAL_BYTES), report.get("/" + ALLOCATION_ROOT));
        Assert.assertEquals(Long.valueOf(D1_D2_F2_BYTES), report.get("/" + ALLOCATION_ROOT + "/d1/d2"));
        Assert.assertEquals(Long.valueOf(D1_F1_BYTES + D1_D2_F2_BYTES), report.get("/" + ALLOCATION_ROOT + "/d1"));
        Assert.assertEquals(Long.valueOf(D3_F3_BYTES), report.get("/" + ALLOCATION_ROOT + "/d3"));
        Assert.assertEquals(Long.valueOf(DENIED_F4_BYTES), report.get("/" + ALLOCATION_ROOT + "/denied"));
    }

    private void testAllocationAsyncSizeSortAsc() throws Exception {
        log.debug("Testing /async-nodesize for sort=asc:");

        // max depth 1
        log.debug("Testing /async-nodesize for sort=asc, maxdepth=1:");
        Job job = postAllocationSize(nodeSizeReportServiceURL, getRootVOSURI(ALLOCATION_ROOT), authSubject, Map.of("maxdepth", "1", "sort", "asc"));
        Assert.assertEquals(ExecutionPhase.COMPLETED, job.getExecutionPhase());

        Map<String, Long> report = fetchNodeSizeReport(authSubject);
        Assert.assertEquals(4, report.size());
        Assert.assertEquals(Long.valueOf(D1_F1_BYTES + D1_D2_F2_BYTES), report.get("/" + ALLOCATION_ROOT + "/d1"));
        Assert.assertEquals(Long.valueOf(DENIED_F4_BYTES), report.get("/" + ALLOCATION_ROOT + "/denied"));
        Assert.assertEquals(Long.valueOf(D3_F3_BYTES), report.get("/" + ALLOCATION_ROOT + "/d3"));
        Assert.assertEquals(Long.valueOf(DEPTH_TOTAL_BYTES), report.get("/" + ALLOCATION_ROOT));

        //max depth 2
        log.debug("Testing /async-nodesize for sort=asc, maxdepth=2:");
        job = postAllocationSize(nodeSizeReportServiceURL, getRootVOSURI(ALLOCATION_ROOT), authSubject, Map.of("maxdepth", "2", "sort", "asc"));
        Assert.assertEquals(ExecutionPhase.COMPLETED, job.getExecutionPhase());

        report = fetchNodeSizeReport(authSubject);
        Assert.assertEquals(5, report.size());
        Assert.assertEquals(Long.valueOf(D1_D2_F2_BYTES), report.get("/" + ALLOCATION_ROOT + "/d1/d2"));
        Assert.assertEquals(Long.valueOf(D1_F1_BYTES + D1_D2_F2_BYTES), report.get("/" + ALLOCATION_ROOT + "/d1"));
        Assert.assertEquals(Long.valueOf(DENIED_F4_BYTES), report.get("/" + ALLOCATION_ROOT + "/denied"));
        Assert.assertEquals(Long.valueOf(D3_F3_BYTES), report.get("/" + ALLOCATION_ROOT + "/d3"));
        Assert.assertEquals(Long.valueOf(DEPTH_TOTAL_BYTES), report.get("/" + ALLOCATION_ROOT));

    }

    private void testAllocationAsyncSizeSortDesc() throws Exception {
        log.debug("Testing /async-nodesize for sort=desc:");
        Job job = postAllocationSize(nodeSizeReportServiceURL, getRootVOSURI(ALLOCATION_ROOT), authSubject, Map.of("maxdepth", "1", "sort", "desc"));
        Assert.assertEquals(ExecutionPhase.COMPLETED, job.getExecutionPhase());

        Map<String, Long> report = fetchNodeSizeReport(authSubject);
        Assert.assertEquals(4, report.size());
        Assert.assertEquals(Long.valueOf(DEPTH_TOTAL_BYTES), report.get("/" + ALLOCATION_ROOT));
        Assert.assertEquals(Long.valueOf(D3_F3_BYTES), report.get("/" + ALLOCATION_ROOT + "/d3"));
        Assert.assertEquals(Long.valueOf(D1_F1_BYTES + D1_D2_F2_BYTES), report.get("/" + ALLOCATION_ROOT + "/d1"));
        Assert.assertEquals(Long.valueOf(DENIED_F4_BYTES), report.get("/" + ALLOCATION_ROOT + "/denied"));
    }

    @Test
    public void testAllocationAsyncSizePermissionDenied() throws Exception {
        try {
            buildNodesTree();
            Job job = postAllocationSize(nodeSizeReportServiceURL, getRootVOSURI(ALLOCATION_ROOT), groupMember, Map.of("maxdepth", "2"));
            Assert.assertEquals(ExecutionPhase.COMPLETED, job.getExecutionPhase());
            Assert.assertEquals(DEPTH_VISIBLE_TO_GROUP_BYTES, getResultLong(job, "bytesUsed"));

            Map<String, Long> report = fetchNodeSizeReport(groupMember);
            Assert.assertTrue(report.containsKey("/" + ALLOCATION_ROOT + "/denied"));
            Assert.assertEquals(Long.valueOf(-1), report.get("/" + ALLOCATION_ROOT + "/denied"));
            Assert.assertEquals(Long.valueOf(DEPTH_VISIBLE_TO_GROUP_BYTES), report.get("/" + ALLOCATION_ROOT));
            Assert.assertEquals(Long.valueOf(D1_D2_F2_BYTES), report.get("/" + ALLOCATION_ROOT + "/d1/d2"));
        } finally {
            cleanupNodesTree();
        }
    }

    @Test
    public void testAllocationAsyncSizeRequiresDest() throws Exception {
        // POST job with target but no dest -> execution fails with 400 IllegalArgumentException
        Map<String, Object> val = new HashMap<>(Map.of("target", getRootVOSURI(ALLOCATION_ROOT).getURI()));
        HttpPost post = new HttpPost(nodeSizeReportServiceURL, val, false);
        Subject.doAs(authSubject, new RunnableAction(post));
        Assert.assertEquals(303, post.getResponseCode());

        URL jobPhaseURL = new URL(post.getRedirectURL().toString() + "/phase");
        val.clear();
        val.put("phase", "RUN");
        post = new HttpPost(jobPhaseURL, val, false);
        Subject.doAs(authSubject, new RunnableAction(post));
        Assert.assertEquals(400, post.getResponseCode());
        Assert.assertTrue(post.getThrowable() instanceof IllegalArgumentException);
        Assert.assertTrue(post.getThrowable().getMessage().contains("dest"));
    }

    private VOSURI getDefaultReportDest(VOSURI target) {
        String path = target.getPath();
        if (!path.endsWith("/")) {
            path = path + "/";
        }
        return new VOSURI(target.getServiceURI(), path + "report");
    }

    private void buildNodesTree() throws Exception {
        String alloc = ALLOCATION_ROOT + "/";
        String d1 = alloc + "d1/";
        String d1d2 = d1 + "d2/";
        String d3 = alloc + "d3/";
        String denied = alloc + "denied/";
        String[] tree = {
            alloc,
            d1,
            d1 + "f1",
            d1d2,
            d1d2 + "f2",
            d3,
            d3 + "f3",
            denied,
            denied + "f4"
        };
        createRootNodeTree(tree);

        String[] readableTestDirs = {alloc, d1, d1 + "f1", d1d2, d1d2 + "f2", d3, d3 + "f3"};
        makeReadable(readableTestDirs, accessGroup);

        ContainerNode deniedNode = new ContainerNode(denied);
        deniedNode.isPublic = false;
        deniedNode.inheritPermissions = false;
        post(getRootNodeURL(denied), getRootVOSURI(denied), deniedNode);

        uploadData(d1 + "f1", D1_F1_BYTES);
        uploadData(d1d2 + "f2", D1_D2_F2_BYTES);
        uploadData(d3 + "f3", D3_F3_BYTES);
        uploadData(denied + "f4", DENIED_F4_BYTES);

        waitForBytesUsed(d1 + "f1", D1_F1_BYTES);
        waitForBytesUsed(d1d2 + "f2", D1_D2_F2_BYTES);
        waitForBytesUsed(d3 + "f3", D3_F3_BYTES);
        waitForBytesUsed(denied + "f4", DENIED_F4_BYTES);
    }

    protected void makeReadable(String[] subdirNames, GroupURI accessGroup)
            throws Exception {
        for (String nodeName : subdirNames) {
            URL nodeURL = getRootNodeURL(nodeName);
            NodeReader.NodeReaderResult result = get(nodeURL, 200, XML_CONTENT_TYPE);
            result.node.getReadOnlyGroup().add(accessGroup);
            result.node.getReadWriteGroup().add(accessGroup);
            log.debug("Node update " + result.node.getReadOnlyGroup());

            VOSURI nodeURI = getRootVOSURI(nodeName);
            post(nodeURL, nodeURI, result.node);
            log.debug("Added group permissions to " + nodeName);
        }
    }

    private void cleanupNodesTree() throws Exception {
        String alloc = ALLOCATION_ROOT + "/";
        String[] tree = {
            alloc,
            alloc + "d1/",
            alloc + "d1/f1",
            alloc + "d1/d2/",
            alloc + "d1/d2/f2",
            alloc + "d3/",
            alloc + "d3/f3",
            alloc + "denied/",
            alloc + "denied/f4",
            alloc + "report"
        };
        cleanupRootNodeTree(tree);
    }

    private URL getRootNodeURL(String path) throws MalformedURLException {
        if (path == null || path.isEmpty()) {
            throw new IllegalArgumentException("path required");
        }
        return new URL(String.format("%s/%s", nodesServiceURL, path));
    }

    private VOSURI getRootVOSURI(String path) {
        return new VOSURI(resourceID, path);
    }

    private void createRootNodeTree(String[] nodes) throws Exception {
        cleanupRootNodeTree(nodes);
        for (String nodeName : nodes) {
            URL nodeURL = getRootNodeURL(nodeName);
            VOSURI nodeURI = getRootVOSURI(nodeName);
            Node node;
            if (nodeName.endsWith("/")) {
                node = new ContainerNode(nodeName);
            } else {
                node = new DataNode(nodeName);
            }
            log.debug("put: " + nodeURI + " -> " + nodeURL);
            put(nodeURL, nodeURI, node);
        }
    }

    private void cleanupRootNodeTree(String[] nodes) throws MalformedURLException {
        for (int i = nodes.length - 1; i >= 0; i--) {
            delete(getRootNodeURL(nodes[i]), false);
        }
    }

    private void uploadData(String path, long numBytes) throws IOException, TransferParsingException {
        byte[] data = new byte[(int) numBytes];
        for (int i = 0; i < data.length; i++) {
            data[i] = (byte) (i & 0xff);
        }

        VOSURI nodeURI = getRootVOSURI(path);
        Transfer transfer = new Transfer(nodeURI.getURI(), Direction.pushToVoSpace);
        transfer.version = VOS.VOSPACE_21;
        Protocol putWithCert = new Protocol(VOS.PROTOCOL_HTTPS_PUT);
        putWithCert.setSecurityMethod(Standards.SECURITY_METHOD_CERT);
        transfer.getProtocols().add(putWithCert);

        TransferWriter writer = new TransferWriter();
        StringWriter sw = new StringWriter();
        writer.write(transfer, sw);

        FileContent fileContent = new FileContent(sw.toString().getBytes(), XML_CONTENT_TYPE);
        HttpPost post = new HttpPost(synctransServiceURL, fileContent, false);
        Subject.doAs(authSubject, new RunnableAction(post));
        Assert.assertEquals(303, post.getResponseCode());

        ByteArrayOutputStream out = new ByteArrayOutputStream();
        HttpGet get = new HttpGet(post.getRedirectURL(), out);
        Subject.doAs(authSubject, new RunnableAction(get));
        Assert.assertEquals(200, get.getResponseCode());

        TransferReader transferReader = new TransferReader();
        Transfer details = transferReader.read(out.toString(), "vos");
        Assert.assertFalse(details.getProtocols().isEmpty());
        URL endpoint = new URL(details.getProtocols().get(0).getEndpoint());

        HttpUpload upload = new HttpUpload(new ByteArrayInputStream(data), endpoint);
        upload.setRequestProperty("Content-Type", "application/octet-stream");
        Subject.doAs(authSubject, new RunnableAction(upload));
        Assert.assertEquals(201, upload.getResponseCode());
        Assert.assertNull(upload.getThrowable());
    }

    private void waitForBytesUsed(String path, long expectedBytes)
            throws MalformedURLException, NodeParsingException, NodeNotSupportedException, InterruptedException {
        for (int i = 0; i < 30; i++) {
            if (verifyBytesUsed(path, expectedBytes)) {
                return;
            }
            Thread.sleep(1000L);
        }
        Assert.fail("timed out waiting for bytesUsed on " + path);
    }

    private boolean verifyBytesUsed(String path, long expectedBytes)
            throws MalformedURLException, NodeParsingException, NodeNotSupportedException {
        URL nodeURL = getRootNodeURL(path);
        NodeReader.NodeReaderResult result = get(nodeURL, 200, XML_CONTENT_TYPE, true);
        if (result.node instanceof DataNode) {
            Long bytes = ((DataNode) result.node).bytesUsed;
            return bytes != null && bytes == expectedBytes;
        }
        Long bytes = ((ContainerNode) result.node).bytesUsed;
        return bytes != null && bytes == expectedBytes;
    }

    private Job postAllocationSize(URL asyncURL, VOSURI vosURI, Subject subject, Map<String, String> params)
            throws Exception {
        delete(getRootNodeURL(ALLOCATION_ROOT + "/report"), false); // cleanup the report data node if it exists

        log.debug("postAllocationSize: " + asyncURL + " " + vosURI + " params=" + params);
        Map<String, Object> val = new HashMap<>();
        val.put("target", vosURI.getURI());
        URI uri = getDefaultReportDest(vosURI).getURI();
        val.put("dest", uri);
        if (params != null && !params.isEmpty()) {
            val.putAll(params);
        }
        HttpPost post = new HttpPost(asyncURL, val, false);
        Subject.doAs(subject, new RunnableAction(post));
        Assert.assertEquals(303, post.getResponseCode());

        URL jobURL = post.getRedirectURL();
        URL jobPhaseURL = new URL(jobURL.toString() + "/phase");

        val.clear();
        val.put("phase", "RUN");
        post = new HttpPost(jobPhaseURL, val, false);
        Subject.doAs(subject, new RunnableAction(post));
        Assert.assertEquals(303, post.getResponseCode());

        URL jobPoll = new URL(jobURL + "?WAIT=6");
        int count = 0;
        boolean done = false;
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        JobReader reader = new JobReader();
        while (!done && count < 10) {
            out = new ByteArrayOutputStream();
            HttpGet get = new HttpGet(jobPoll, out);
            Subject.doAs(subject, new RunnableAction(get));
            Assert.assertNull(get.getThrowable());
            Job job = reader.read(new StringReader(out.toString()));
            switch (job.getExecutionPhase()) {
                case QUEUED:
                case EXECUTING:
                    count++;
                    break;
                default:
                    done = true;
            }
        }
        return reader.read(new StringReader(out.toString()));
    }

    private Map<String, Long> fetchNodeSizeReport(Subject subject) throws Exception {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        HttpGet get = new HttpGet(new URL(String.format("%s/%s/%s", filesURL, ALLOCATION_ROOT, "report")), out);
        Subject.doAs(subject, new RunnableAction(get));
        Assert.assertNull(get.getThrowable());
        Assert.assertEquals(200, get.getResponseCode());

        return parseNodeSizeReport(out.toString(StandardCharsets.UTF_8));
    }

    private Map<String, Long> parseNodeSizeReport(String reportText) {
        Map<String, Long> map = new LinkedHashMap<>();
        for (String line : reportText.split("\n")) {
            if (line.isEmpty()) {
                continue;
            }
            int tab = line.indexOf('\t');
            Assert.assertTrue("invalid report line: " + line, tab > 0);
            long bytes = "Permission Denied".equals(line.substring(0, tab))
                    ? -1L
                    : Long.parseLong(line.substring(0, tab));
            map.put(line.substring(tab + 1), bytes);
        }
        return map;
    }

    private long getResultLong(Job job, String name) {
        Result result = findResult(job, name);
        Assert.assertNotNull("missing result: " + name, result);
        return Long.parseLong(result.getURI().getSchemeSpecificPart());
    }

    private Result findResult(Job job, String name) {
        for (Result result : job.getResultsList()) {
            if (name.equalsIgnoreCase(result.getName())) {
                return result;
            }
        }
        return null;
    }
}
