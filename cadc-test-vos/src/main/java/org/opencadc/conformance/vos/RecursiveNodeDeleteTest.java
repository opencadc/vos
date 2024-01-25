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

import ca.nrc.cadc.auth.AuthMethod;
import ca.nrc.cadc.auth.RunnableAction;
import ca.nrc.cadc.auth.SSLUtil;
import ca.nrc.cadc.net.HttpGet;
import ca.nrc.cadc.net.HttpPost;
import ca.nrc.cadc.reg.Standards;
import ca.nrc.cadc.reg.client.RegistryClient;
import ca.nrc.cadc.uws.ExecutionPhase;
import ca.nrc.cadc.uws.Job;
import ca.nrc.cadc.uws.JobReader;
import ca.nrc.cadc.uws.Result;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.StringReader;
import java.net.MalformedURLException;
import java.net.URI;
import java.net.URL;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import javax.security.auth.Subject;
import org.apache.log4j.Logger;
import org.junit.Assert;
import org.junit.Test;
import org.opencadc.gms.GroupURI;
import org.opencadc.vospace.NodeNotSupportedException;
import org.opencadc.vospace.NodeProperty;
import org.opencadc.vospace.VOSURI;
import org.opencadc.vospace.io.NodeParsingException;
import org.opencadc.vospace.io.NodeReader;

public class RecursiveNodeDeleteTest extends VOSTest {
    private static final Logger log = Logger.getLogger(RecursiveNodeDeleteTest.class);

    protected final URL recursiveDeleteServiceURL;

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
    
    protected RecursiveNodeDeleteTest(URI resourceID, File testCert) {
        super(resourceID, testCert);

        RegistryClient regClient = new RegistryClient();
        this.recursiveDeleteServiceURL = regClient.getServiceURL(resourceID, Standards.VOSPACE_RECURSIVE_DELETE, AuthMethod.CERT);
        log.info(String.format("%s: %s", Standards.VOSPACE_RECURSIVE_DELETE, recursiveDeleteServiceURL));
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
    public void testRecursiveDelete() throws Exception {
        // create a tree structure
        String baseDir = "testRecursiveDelete/";
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
        Job job = postRecursiveDelete(recursiveDeleteServiceURL, getVOSURI(baseDir), authSubject);
        Assert.assertEquals("Expected completed job", ExecutionPhase.COMPLETED, job.getExecutionPhase());
        Assert.assertEquals(1, job.getResultsList().size());
        Result res = job.getResultsList().get(0);
        Assert.assertEquals("successcount", res.getName());
        Assert.assertEquals(4, Integer.parseInt(res.getURI().getSchemeSpecificPart()));
        // cleanup
        cleanupNodeTree(testTree);

        if (!nodelockSupported) {
            return;
        }
        
        // repeat test but lock the subdirectory
        createNodeTree(testTree);
        result = get(subdirURL, 200, XML_CONTENT_TYPE, true);
        log.info("found: " + result.vosURI);
        result.node.isLocked = true;
        post(subdirURL, getVOSURI(subDir), result.node);

        // an error should result from try to delete a file in that directory
        job = postRecursiveDelete(recursiveDeleteServiceURL, getVOSURI(subdirURL + "file2"), authSubject);

        Assert.assertEquals("Expected error job", ExecutionPhase.ERROR, job.getExecutionPhase());
        // now try to delete the root node which results in a partial delete (aborted job)
        job = postRecursiveDelete(recursiveDeleteServiceURL, getVOSURI(baseDir), authSubject);
        Assert.assertEquals("Expected aborted job", ExecutionPhase.ABORTED, job.getExecutionPhase());
        Assert.assertEquals(2, job.getResultsList().size());
        for (Result jobResult : job.getResultsList()) {
            if ("errorcount".equalsIgnoreCase(jobResult.getName())) {
                Assert.assertEquals(1, Integer.parseInt(jobResult.getURI().getSchemeSpecificPart()));
            } else if ("successcount".equalsIgnoreCase(jobResult.getName())) {
                Assert.assertEquals(1, Integer.parseInt(jobResult.getURI().getSchemeSpecificPart()));
            } else {
                Assert.fail("Unexpected result " + jobResult.getName());
            }
        }

        // unlock to be able to cleanup
        result.node.isLocked = false;
        post(subdirURL, getVOSURI(subDir), result.node);

        // cleanup
        cleanupNodeTree(testTree);
    }

    @Test
    public void testRecursiveDeletePermissions() throws Exception {
        // create a tree structure
        String baseDir = "testRecursiveDelPerm/";
        String testDir = baseDir + "testDir/";
        String subDir = testDir + "subdir/";
        String[] testTree = {baseDir, testDir, testDir + "file1", subDir, subDir + "file2"};

        createNodeTree(testTree);

        // grant write permission to baseDir
        String[] writableBaseDir = {baseDir};
        makeWritable(writableBaseDir, accessGroup);

        // user without write permission on testDir or below, it cannot be removed
        Job job = postRecursiveDelete(recursiveDeleteServiceURL, getVOSURI(testDir), groupMember);
        Assert.assertEquals("Expected error job", ExecutionPhase.ERROR, job.getExecutionPhase());

        // grant write permission to testDir
        String[] writableTestDir = {testDir};
        makeWritable(writableTestDir, accessGroup);

        // can only delete the one file in the testDir
        VOSURI testDirURI = getVOSURI(testDir);
        job = postRecursiveDelete(recursiveDeleteServiceURL, testDirURI, groupMember);
        Assert.assertEquals("Expected aborted job", ExecutionPhase.ABORTED, job.getExecutionPhase());
        Assert.assertEquals(2, job.getResultsList().size());
        for (Result jobResult : job.getResultsList()) {
            if ("errorcount".equalsIgnoreCase(jobResult.getName())) {
                // subdir cannot be deleted
                Assert.assertEquals(1, Integer.parseInt(jobResult.getURI().getSchemeSpecificPart()));
            } else if ("successcount".equalsIgnoreCase(jobResult.getName())) {
                // file1 successfully deleted
                Assert.assertEquals(1, Integer.parseInt(jobResult.getURI().getSchemeSpecificPart()));
            } else {
                Assert.fail("Unexpected result " + jobResult.getName());
            }
        }

        // grant all write permission
        String[] testBottomTree = Arrays.copyOfRange(testTree, 3, 5);
        makeWritable(testBottomTree, accessGroup);

        job = postRecursiveDelete(recursiveDeleteServiceURL, testDirURI, groupMember);
        Assert.assertEquals("Expected completed job", ExecutionPhase.COMPLETED, job.getExecutionPhase());
        Assert.assertEquals(1, job.getResultsList().size());
        Result res = job.getResultsList().get(0);
        Assert.assertEquals("successcount", res.getName());
        Assert.assertEquals(3, Integer.parseInt(res.getURI().getSchemeSpecificPart()));

        // cleanup
        cleanupNodeTree(testTree);

    }

    private boolean checkProp(String nodeName, NodeProperty prop) throws NodeParsingException, NodeNotSupportedException, MalformedURLException {
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
    
    public Job postRecursiveDelete(URL asyncURL, VOSURI vosURI, Subject actor)
            throws Exception {
        log.info("postRecursiveDelete: " + asyncURL + " " + vosURI);
        Map<String, Object> val = new HashMap<>();
        val.put("target", vosURI.getURI());
        HttpPost post = new HttpPost(asyncURL, val, false);
        log.debug("POST: " + asyncURL);
        Subject.doAs(actor, new RunnableAction(post));
        log.debug("POST responseCode: " + post.getResponseCode());
        Assert.assertEquals("expected POST response code = 303",
                303, post.getResponseCode());
        URL jobURL = post.getRedirectURL();
        URL jobPhaseURL = new URL(jobURL.toString() + "/phase");

        // start the job
        val.clear();
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
