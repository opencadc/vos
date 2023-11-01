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
import ca.nrc.cadc.net.HttpDelete;
import ca.nrc.cadc.net.HttpGet;
import ca.nrc.cadc.net.HttpUpload;
import ca.nrc.cadc.uws.ExecutionPhase;
import ca.nrc.cadc.uws.Job;
import ca.nrc.cadc.uws.Result;
import java.io.File;
import java.io.InputStream;
import java.net.MalformedURLException;
import java.net.URI;
import java.net.URL;
import javax.security.auth.Subject;
import org.apache.log4j.Logger;
import org.junit.Assert;
import org.junit.Assume;
import org.junit.Test;
import org.opencadc.gms.GroupURI;
import org.opencadc.vospace.ContainerNode;
import org.opencadc.vospace.DataNode;
import org.opencadc.vospace.Node;
import org.opencadc.vospace.VOSURI;
import org.opencadc.vospace.io.NodeReader;

public class AsyncTest extends VOSTest {
    private static final Logger log = Logger.getLogger(AsyncTest.class);

    // permissions tests
    private GroupURI accessGroup;
    private Subject groupMember;

    // round trip tests
    private GroupURI group1;
    private GroupURI group2;

    protected boolean linkNodeProps = true;
    protected boolean paginationSupported = true;
    protected boolean nodelockSupported = true;

    protected boolean cleanupOnSuccess = true;

    protected AsyncTest(URI resourceID, File testCert) {
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

        Job job = postRecursiveDelete(recursiveDeleteServiceURL, getVOSURI(baseDir));
        Assert.assertEquals("Expected completed job", ExecutionPhase.COMPLETED, job.getExecutionPhase());
        Assert.assertEquals(1, job.getResultsList().size());
        Result res = job.getResultsList().get(0);
        Assert.assertEquals("delcount", res.getName());
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
        job = postRecursiveDelete(recursiveDeleteServiceURL, getVOSURI(subdirURL + "file2"));
        Assert.assertEquals("Expected error job", ExecutionPhase.ERROR, job.getExecutionPhase());

        // now try to delete the root node which results in a partial delete (aborted job)
        job = postRecursiveDelete(recursiveDeleteServiceURL, getVOSURI(baseDir));
        Assert.assertEquals("Expected aborted job", ExecutionPhase.ABORTED, job.getExecutionPhase());
        Assert.assertEquals(2, job.getResultsList().size());
        for (Result jobResult : job.getResultsList()) {
            if ("errorcount".equalsIgnoreCase(jobResult.getName())) {
                Assert.assertEquals(1, Integer.parseInt(jobResult.getURI().getSchemeSpecificPart()));
            } else if ("delcount".equalsIgnoreCase(jobResult.getName())) {
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

    private void createNodeTree(String[] nodes) throws Exception {
        // cleanup first
        cleanupNodeTree(nodes);

        // build the tree
        for (String nodeName : nodes) {
            URL nodeURL = getNodeURL(nodesServiceURL, nodeName);
            VOSURI nodeURI = getVOSURI(nodeName);
            Node node = null;
            if (nodeName.endsWith("/")) {
                node = new ContainerNode(nodeName);
            } else {
                node = new DataNode(nodeName);
            }
            log.info("put: " + nodeURI + " -> " + nodeURL);
            put(nodeURL, nodeURI, node);
        }
    }

    private void cleanupNodeTree(String[] nodes) throws MalformedURLException {
        for (int i = nodes.length - 1; i >= 0; i--) {
            URL nodeURL = getNodeURL(nodesServiceURL, nodes[i]);
            log.debug("deleting node " + nodeURL);
            delete(nodeURL, false);
        }
    }

    @Test
    public void testPermissions() throws Exception {

        Assume.assumeTrue("enablePermissionTest not called", accessGroup != null);
        
        // create a directory
        String parentName = "testPermissions";
        ContainerNode testNode = new ContainerNode(parentName);
        testNode.owner = authSubject;
        testNode.isPublic = false;

        final URL nodeURL = getNodeURL(nodesServiceURL, parentName);
        final VOSURI nodeURI = getVOSURI(parentName);

        String childName = "testGroupUser";
        ContainerNode childNode = new ContainerNode(childName);
        childNode.parent = testNode;
        String childPath = parentName + "/" + childName;
        final VOSURI childURI = getVOSURI(childPath);
        final URL childURL = getNodeURL(nodesServiceURL, childPath);
        
        // cleanup
        delete(childURL, false);
        delete(nodeURL, false);

        // PUT the node
        log.info("putAction: " + nodeURI + " -> " + nodeURL);
        put(nodeURL, nodeURI, testNode);

        // try to access it as a different user (memberUser) - it should fail
        HttpGet getAction = new HttpGet(nodeURL, true);
        Subject.doAs(groupMember, new RunnableAction(getAction));
        Assert.assertEquals(403, getAction.getResponseCode());

        // give groupMember read access through the group
        getAction = new HttpGet(nodeURL, true);
        testNode.getReadOnlyGroup().add(accessGroup);
        post(nodeURL, nodeURI, testNode);
        Subject.doAs(groupMember, new RunnableAction(getAction));
        Assert.assertEquals("expected GET response code = 200", 200, getAction.getResponseCode());
        Assert.assertNull("expected GET throwable == null", getAction.getThrowable());

        // permission denied to write in the container without write permission
        InputStream is = prepareInput(childURI, childNode);
        HttpUpload putAction = new HttpUpload(is, childURL);
        putAction.setRequestProperty("Content-Type", XML_CONTENT_TYPE);
        log.debug("PUT rejected " + childURL);
        Subject.doAs(groupMember, new RunnableAction(putAction));
        log.debug("PUT responseCode: " + putAction.getResponseCode());
        Assert.assertEquals("expected PUT response code = 403",
                403, putAction.getResponseCode());

        // same test after permission granted

        testNode.getReadWriteGroup().add(accessGroup);
        log.debug("Node update " + testNode.getReadWriteGroup());
        post(nodeURL, nodeURI, testNode);
        log.debug("PUT succeed " + childURL);
        is.reset();
        putAction = new HttpUpload(is, childURL);
        Subject.doAs(groupMember, new RunnableAction(putAction));
        log.debug("PUT responseCode: " + putAction.getResponseCode());
        Assert.assertEquals("expected PUT response code = 200",
                200, putAction.getResponseCode());
        Assert.assertNull("expected PUT throwable == null", putAction.getThrowable());

        log.debug("Delete node " + childURL);
        HttpDelete deleteAction = new HttpDelete(childURL, true);
        Subject.doAs(groupMember, new RunnableAction(deleteAction));
        log.debug("DELETE responseCode: " + deleteAction.getResponseCode());
        Assert.assertEquals("expected PUT response code = 200",
                200, deleteAction.getResponseCode());
        Assert.assertNull("expected PUT throwable == null", deleteAction.getThrowable());

        if (cleanupOnSuccess) {
            delete(nodeURL);
        }
    }

}
