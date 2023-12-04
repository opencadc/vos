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

package org.opencadc.conformance.vos;

import ca.nrc.cadc.auth.RunnableAction;
import ca.nrc.cadc.auth.SSLUtil;
import ca.nrc.cadc.net.FileContent;
import ca.nrc.cadc.net.HttpGet;
import ca.nrc.cadc.net.HttpPost;
import ca.nrc.cadc.uws.ExecutionPhase;
import ca.nrc.cadc.uws.Job;
import ca.nrc.cadc.uws.JobReader;
import ca.nrc.cadc.uws.Result;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.StringReader;
import java.net.MalformedURLException;
import java.net.URI;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import javax.security.auth.Subject;
import org.apache.log4j.Logger;
import org.junit.Assert;
import org.junit.Test;
import static org.opencadc.conformance.vos.VOSTest.XML_CONTENT_TYPE;
import org.opencadc.gms.GroupURI;
import org.opencadc.vospace.ContainerNode;
import org.opencadc.vospace.DataNode;
import org.opencadc.vospace.Node;
import org.opencadc.vospace.NodeNotSupportedException;
import org.opencadc.vospace.NodeProperty;
import org.opencadc.vospace.VOS;
import org.opencadc.vospace.VOSURI;
import org.opencadc.vospace.io.NodeParsingException;
import org.opencadc.vospace.io.NodeReader;
import org.opencadc.vospace.io.NodeWriter;

public class RecursiveNodePropsTest extends VOSTest {
    private static final Logger log = Logger.getLogger(RecursiveNodePropsTest.class);

    // permissions tests
    private GroupURI accessGroup;
    private Subject groupMember;

    // round trip tests
    private GroupURI group1;
    private GroupURI group2;

    protected boolean nodelockSupported = true;
    protected boolean linkNodeProps = true;
    protected boolean paginationSupported = true;

    protected boolean cleanupOnSuccess = true;

    protected RecursiveNodePropsTest(URI resourceID, File testCert) {
        super(resourceID, testCert);
    }

    /**
     * Enable testing that group props can be set and returned for nodes.
     * 
     * @param group1 a valid group URI
     * @param group2 a different valid group URI
     */
    protected void enablePermissionPropsTest(GroupURI group1, GroupURI group2) {
        this.group1 = group1;
        this.group2 = group2;
    }
    
    protected void enablePermissionTests(GroupURI accessGroup, File groupMemberCert) {
        this.accessGroup = accessGroup;
        this.groupMember = SSLUtil.createSubject(groupMemberCert);
    }


    @Test
    public void testRecursiveNodeProps() throws Exception {
        // create a tree structure
        String baseDir = "testRecursiveNodeProps/";
        String subDir = baseDir + "subdir/";
        String[] testTree = {baseDir, baseDir + "file1", subDir, subDir + "file2"};

        // clear possible lock left from a previous test
        URL subdirURL = getNodeURL(nodesServiceURL, subDir);
        NodeReader.NodeReaderResult result = get(subdirURL, 200, XML_CONTENT_TYPE, false);
        if (result != null) {
            if ((result.node.isLocked != null) && result.node.isLocked) {
                result.node.isLocked = false;
                post(subdirURL, getVOSURI(subDir), result.node);
            }
        }

        createNodeTree(testTree);
        ContainerNode testNode = new ContainerNode(baseDir);
        NodeProperty titleProperty = new NodeProperty(VOS.PROPERTY_URI_TITLE, "title");
        NodeProperty descriptionProperty = new NodeProperty(VOS.PROPERTY_URI_DESCRIPTION, "description");
        NodeProperty customProperty = new NodeProperty(URI.create("custom:secret-meaning"), "my secret info");
        testNode.getProperties().add(titleProperty);
        testNode.getProperties().add(descriptionProperty);
        testNode.getProperties().add(customProperty);

        Job job = postRecursiveNodeProps(recursiveNodePropsServiceURL, testNode, authSubject);
        Assert.assertEquals("Expected completed job", ExecutionPhase.COMPLETED, job.getExecutionPhase());
        Assert.assertEquals(1, job.getResultsList().size());
        Result res = job.getResultsList().get(0);
        Assert.assertEquals("successcount", res.getName());
        Assert.assertEquals(4, Integer.parseInt(res.getURI().getSchemeSpecificPart()));

        for (String file : testTree) {
            URL fileURL = getNodeURL(nodesServiceURL, subDir);
            result = get(fileURL, 200, XML_CONTENT_TYPE, false);
            Assert.assertNotNull(result.node);
            int found = 0;
            for (NodeProperty prop : result.node.getProperties()) {
                if (VOS.PROPERTY_URI_TITLE.equals(prop.getKey())) {
                    Assert.assertTrue("title".equals(prop.getValue()));
                    found += 1;
                } else if (VOS.PROPERTY_URI_DESCRIPTION.equals(prop.getKey())) {
                    Assert.assertTrue("description".equals(prop.getValue()));
                    found += 1;
                } else if (URI.create("custom:secret-meaning").equals(prop.getKey())) {
                    Assert.assertTrue("my secret info".equals(prop.getValue()));
                    found += 1;
                }
            }
            Assert.assertEquals(3, found);
        }

        // repeat with other updates
        if (nodelockSupported) {
            // a locked directory should have not prevent the updates
            result = get(subdirURL, 200, XML_CONTENT_TYPE, true);
            log.info("found: " + result.vosURI);
            result.node.isLocked = true;
            post(subdirURL, getVOSURI(subDir), result.node);
        }
        // update title
        titleProperty = new NodeProperty(VOS.PROPERTY_URI_TITLE, "better title");
        // remove secret
        customProperty = new NodeProperty(URI.create("custom:secret-meaning"));
        testNode = new ContainerNode(baseDir);

        testNode.getProperties().add(titleProperty);
        testNode.getProperties().add(customProperty);

        job = postRecursiveNodeProps(recursiveNodePropsServiceURL, testNode, authSubject);
        Assert.assertEquals("Expected completed job", ExecutionPhase.COMPLETED, job.getExecutionPhase());
        Assert.assertEquals(1, job.getResultsList().size());
        res = job.getResultsList().get(0);
        Assert.assertEquals("successcount", res.getName());
        Assert.assertEquals(4, Integer.parseInt(res.getURI().getSchemeSpecificPart()));

        for (String file : testTree) {
            URL fileURL = getNodeURL(nodesServiceURL, subDir);
            result = get(fileURL, 200, XML_CONTENT_TYPE, false);
            Assert.assertNotNull(result.node);
            int found = 0;
            for (NodeProperty prop : result.node.getProperties()) {
                Assert.assertNotEquals(URI.create("custom:secret-meaning"), prop.getKey());
                if (VOS.PROPERTY_URI_TITLE.equals(prop.getKey())) {
                    Assert.assertTrue("better title".equals(prop.getValue()));
                    found += 1;
                } else if (VOS.PROPERTY_URI_DESCRIPTION.equals(prop.getKey())) {
                    Assert.assertTrue("description".equals(prop.getValue()));
                    found += 1;
                }
            }
            Assert.assertEquals(2, found);
        }

        if (nodelockSupported) {
            // unlock node to be able to delete
            result = get(subdirURL, 200, XML_CONTENT_TYPE, true);
            log.info("found: " + result.vosURI);
            result.node.isLocked = false;
            post(subdirURL, getVOSURI(subDir), result.node);
        }

        // try to set and admin prop -> fail
        testNode.getProperties().clear();
        NodeProperty quotaProperty = new NodeProperty(VOS.PROPERTY_URI_QUOTA, "100000");
        testNode.getProperties().add(quotaProperty);

        job = postRecursiveNodeProps(recursiveNodePropsServiceURL, testNode, authSubject);
        Assert.assertEquals("Expected error job", ExecutionPhase.ERROR, job.getExecutionPhase());
        Assert.assertEquals(0, job.getResultsList().size());
        for (String nodeName : testTree) {
            Assert.assertFalse(checkProp(nodeName, quotaProperty));
        }

        // cleanup
        cleanupNodeTree(testTree);

    }

    @Test
    public void testRecursiveNodePropsPermissions() throws Exception {
        // create a tree structure
        String baseDir = "testRecursivePropsPerm/";
        String testDir = baseDir + "testDir/";
        String subDir = testDir + "subdir/";
        String[] testTree = {baseDir, testDir, testDir + "file1", subDir, subDir + "file2"};

        createNodeTree(testTree);

        // user without write permission on testDir or below => it cannot be updated
        ContainerNode testNode = new ContainerNode(baseDir);

        NodeProperty titleProperty = new NodeProperty(VOS.PROPERTY_URI_TITLE, "title");
        testNode.getProperties().add(titleProperty);

        Job job = postRecursiveNodeProps(recursiveNodePropsServiceURL, testNode, groupMember);
        Assert.assertEquals("Expected error job", ExecutionPhase.ERROR, job.getExecutionPhase());

        // grant write permission to basedir and testdir => partial update:
        // file1 and subDir will not be updated and file2 will not be visited
        String[] testTopTree = Arrays.copyOfRange(testTree, 0, 2);
        makeWritable(testTopTree, accessGroup);
        // can only update the one file in the testDir and the base dir
        job = postRecursiveNodeProps(recursiveNodePropsServiceURL, testNode, groupMember);
        Assert.assertEquals("Expected aborted job", ExecutionPhase.ABORTED, job.getExecutionPhase());
        Assert.assertEquals(2, job.getResultsList().size());
        for (Result jobResult : job.getResultsList()) {
            if ("errorcount".equalsIgnoreCase(jobResult.getName())) {
                Assert.assertEquals(2, Integer.parseInt(jobResult.getURI().getSchemeSpecificPart()));
            } else if ("successcount".equalsIgnoreCase(jobResult.getName())) {
                Assert.assertEquals(2, Integer.parseInt(jobResult.getURI().getSchemeSpecificPart()));
            } else {
                Assert.fail("Unexpected result " + jobResult.getName());
            }
        }
        for (String nodeName : testTopTree) {
            Assert.assertTrue(nodeName, checkProp(nodeName, titleProperty));
        }
        String[] testBottomTree = Arrays.copyOfRange(testTree, 2, 5);
        for (String nodeName : testBottomTree) {
            Assert.assertFalse(nodeName, checkProp(nodeName, titleProperty));
        }

        // grant write permission to the remaining nodes => full update
        makeWritable(testBottomTree, accessGroup);
        testNode.getProperties().clear();
        titleProperty.setValue("better title");
        testNode.getProperties().add(titleProperty);
        job = postRecursiveNodeProps(recursiveNodePropsServiceURL, testNode, groupMember);
        Assert.assertEquals("Expected completed job", ExecutionPhase.COMPLETED, job.getExecutionPhase());
        Assert.assertEquals(1, job.getResultsList().size());
        Result res = job.getResultsList().get(0);
        Assert.assertEquals("successcount", res.getName());
        Assert.assertEquals(testTree.length, Integer.parseInt(res.getURI().getSchemeSpecificPart()));
        for (String nodeName : testTree) {
            Assert.assertTrue(nodeName, checkProp(nodeName, titleProperty));
        }

        // cleanup
        cleanupNodeTree(testTree);
    }

    private boolean checkProp(String nodeName, NodeProperty prop) throws NodeParsingException,
            NodeNotSupportedException, MalformedURLException {
        URL fileURL = getNodeURL(nodesServiceURL, nodeName);
        NodeReader.NodeReaderResult result = get(fileURL, 200, XML_CONTENT_TYPE, false);
        Assert.assertNotNull(result.node);
        for (NodeProperty pp : result.node.getProperties()) {
            if (pp.getKey().equals(prop.getKey())) {
                return pp.getKey().equals(prop.getKey());
            }
        }
        return false;
    }
    
    public Job postRecursiveNodeProps(URL asyncURL, Node propsNode, Subject actor)
            throws Exception {
        log.info("postRecursiveNodeProps: " + asyncURL + " " + getVOSURI(propsNode.getName()));
        StringBuilder sb = new StringBuilder();
        NodeWriter writer = new NodeWriter();
        writer.write(getVOSURI(propsNode.getName()), propsNode, sb, VOS.Detail.max);
        log.debug("post content: " + sb.toString());
        FileContent content = new FileContent(sb.toString(), XML_CONTENT_TYPE, StandardCharsets.UTF_8);

        HttpPost post = new HttpPost(asyncURL, content, false);
        log.debug("POST: " + asyncURL);
        Subject.doAs(actor, new RunnableAction(post));
        log.debug("POST responseCode: " + post.getResponseCode());
        Assert.assertEquals("expected POST response code = 303",
                303, post.getResponseCode());
        URL jobURL = post.getRedirectURL();
        URL jobPhaseURL = new URL(jobURL.toString() + "/phase");

        // start the job
        Map<String, Object> val = new HashMap<>();
        val.put("phase", "RUN");
        post = new HttpPost(jobPhaseURL , val, false);
        log.debug("POST: " + jobPhaseURL);
        Subject.doAs(actor, new RunnableAction(post));
        log.debug("POST responseCode: " + post.getResponseCode());
        Assert.assertEquals("expected POST response code = 303",
                303, post.getResponseCode());

        // polling: WAIT will block for up to 6 sec or until phase change or if job is in
        // a terminal phase
        URL jobPoll = new URL(jobURL + "?WAIT=6");
        int count = 0;
        boolean done = false;
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        JobReader reader = new JobReader();
        while (!done && count < 10) { // max 10*6 = 60 sec polling
            out = new ByteArrayOutputStream();
            log.debug("poll: " + jobPoll);
            HttpGet get = new HttpGet(jobPoll, out);
            Subject.doAs(actor, new RunnableAction(get));
            Assert.assertNull(get.getThrowable());
            Job job = reader.read(new StringReader(out.toString()));
            log.debug("current phase: " + job.getExecutionPhase());
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
}
