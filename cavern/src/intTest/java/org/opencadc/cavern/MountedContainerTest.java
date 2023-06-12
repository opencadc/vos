/*
************************************************************************
*******************  CANADIAN ASTRONOMY DATA CENTRE  *******************
**************  CENTRE CANADIEN DE DONNÉES ASTRONOMIQUES  **************
*
*  (c) 2018.                            (c) 2018.
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

package org.opencadc.cavern;

import ca.nrc.cadc.auth.SSLUtil;
import ca.nrc.cadc.exec.BuilderOutputGrabber;
import ca.nrc.cadc.util.FileUtil;
import ca.nrc.cadc.util.Log4jInit;
import ca.nrc.cadc.util.StringUtil;
import ca.nrc.cadc.vos.ContainerNode;
import ca.nrc.cadc.vos.DataNode;
import ca.nrc.cadc.vos.Direction;
import ca.nrc.cadc.vos.LinkNode;
import ca.nrc.cadc.vos.Node;
import ca.nrc.cadc.vos.NodeProperty;
import ca.nrc.cadc.vos.Protocol;
import ca.nrc.cadc.vos.Transfer;
import ca.nrc.cadc.vos.TransferWriter;
import ca.nrc.cadc.vos.VOS;
import ca.nrc.cadc.vos.VOSURI;
import ca.nrc.cadc.vos.client.ClientTransfer;
import ca.nrc.cadc.vos.client.VOSpaceClient;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.PrivilegedActionException;
import java.time.Duration;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import javax.security.auth.Subject;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.opencadc.cavern.nodes.NodeUtil;

/**
 *
 * @author pdowler
 */
public class MountedContainerTest {
    private static final Logger log = Logger.getLogger(MountedContainerTest.class);

    private static File SSL_CERT;
    private static VOSURI baseURI;

    static {
        Log4jInit.setLevel("org.opencadc.cavern", Level.INFO);
        Log4jInit.setLevel("ca.nrc.cadc.vospace", Level.INFO);
        Log4jInit.setLevel("ca.nrc.cadc.vos", Level.INFO);
        Log4jInit.setLevel("ca.nrc.cadc.util", Level.INFO);
    }
    
    public MountedContainerTest() { 
    }
    
    @BeforeClass
    public static void staticInit() throws Exception {
        SSL_CERT = FileUtil.getFileFromResource("x509_CADCRegtest1.pem", TransferRunnerTest.class);

        Properties properties = System.getProperties();
        properties.forEach((k,v) -> log.info(k + ":" + v));

        String uriProp = MountedContainerTest.class.getName() + ".baseURI";
        String uri = System.getProperty(uriProp);
        log.debug(uriProp + " = " + uri);
        if (StringUtil.hasText(uri)) {
            baseURI = new VOSURI(new URI(uri));
        } else {
            throw new IllegalStateException("expected system property " + uriProp + " = <base vos URI>, found: " + uri);
        }
    }
    
    @Test
    public void testConsistencyCreateViaREST() {
        doit(true);
    }
    
    @Test
    public void testConsistencyCreateViaFS() {
        doit(false);
    }
    
    
    private void doit(boolean createViaREST) {
        Protocol sp = new Protocol(VOS.PROTOCOL_SSHFS);
        try {
            Subject s = SSLUtil.createSubject(SSL_CERT);
            VOSpaceClient vos = new VOSpaceClient(baseURI.getServiceURI());
            
            // idempotent create base
            TestActions.CreateNodeAction doit = new TestActions.CreateNodeAction(vos, new ContainerNode(baseURI), true);
            Node result = Subject.doAs(s, doit);
            Assert.assertNotNull(result);
            
            // create test container
            String vosuripath = baseURI.toString() + "/mountedContainerTest-" + System.currentTimeMillis();
            VOSURI containerURI = new VOSURI(vosuripath);
            doit = new TestActions.CreateNodeAction(vos, new ContainerNode(containerURI));
            result = Subject.doAs(s, doit);
            Assert.assertNotNull(result);
            
            // mount test container
            List<Protocol> protocols = new ArrayList<>();
            protocols.add(sp);
            Transfer t = new Transfer(containerURI.getURI(), Direction.BIDIRECTIONAL);
            t.getProtocols().addAll(protocols);
            TransferWriter tw = new TransferWriter();
            ByteArrayOutputStream out = new ByteArrayOutputStream();
            tw.write(t, out);
            log.debug("sending transfer: " + out.toString());
            ClientTransfer trans = Subject.doAs(s, new TestActions.CreateTransferAction(vos, t, false));
            Transfer trans2 = trans.getTransfer();

            Assert.assertNotNull(trans2);
            Assert.assertFalse("result protocols", trans2.getProtocols().isEmpty());
            // if service returns multiple protocol/endpoint pairs, test them all
            for (Protocol p : trans2.getProtocols()) {
                log.info("found protocol: " + p);
                Assert.assertEquals("protocol", sp, p);
                Assert.assertNotNull("endpoint", p.getEndpoint());
                log.info(p + " -> " + p.getEndpoint());
                URI uri = new URI(p.getEndpoint());
                Assert.assertEquals("sshfs", uri.getScheme());
                
                Path testDir = Files.createTempDirectory(MountedContainerTest.class.getSimpleName());
                testDir.toFile().deleteOnExit();
                try {
                    log.info("mount: " + uri + " -> " + testDir.toFile().getAbsolutePath() + " ...");
                    doMount(uri, testDir);
                    log.info("mounted: " + uri + " -> " + testDir.toFile().getAbsolutePath());
                    
                    doConsistencyCheck(vos, s, containerURI, testDir, createViaREST);
                    
                } finally {
                    log.info("unmount: " + testDir.toFile().getAbsolutePath() + " ...");
                    doUnmount(testDir);
                    log.info("unmounted: " + testDir.toFile().getAbsolutePath());
                }
            }
        } catch (Exception unexpected) {
            log.error("unexpected exception", unexpected);
            Assert.fail("unexpected exception: " + unexpected);
        }
    }

    private void doMount(URI endpoint, Path testDir) throws Exception {
        // wrapper script provides password for ssh auth on stdin
        File wf = FileUtil.getFileFromResource("sshfs-wrapper", MountedContainerTest.class);
        String wrapper = wf.getAbsolutePath();
        
        // sshfs:$user@$host[:$port]:$path
        String[] ss = endpoint.toASCIIString().split(":");
        StringBuilder mnt = new StringBuilder();
        mnt.append(ss[1]).append(":").append(ss[3]);
        String port = ss[2];
        String[] cmd = getMountCommand(wrapper, port, mnt, testDir);
        StringBuilder sb = new StringBuilder();
        for (String s : cmd) {
            log.info("command: " + s);
            sb.append(s).append(" ");
        }
        log.info("mount command: " + sb.toString());
        BuilderOutputGrabber grabber = new BuilderOutputGrabber();
        grabber.captureOutput(cmd);
        log.info("exit code from grabber for cmd: " + grabber.getExitValue());
        if (grabber.getExitValue() != 0) {
            throw new IOException("FAIL: " + sb + "\n" + grabber.getErrorOutput());
        }

    }
    
    private void doUnmount(Path testDir) throws Exception {
        String[] cmd = getUnmountCommand(testDir);
        BuilderOutputGrabber grabber = new BuilderOutputGrabber();

        grabber.captureOutput(cmd);
        if (grabber.getExitValue() != 0) {
            StringBuilder sb = new StringBuilder();
            for (String s : cmd) {
                sb.append(s).append(" ");
            }
            throw new IOException("FAIL: " + sb + "\n" + grabber.getErrorOutput());
        }
    }
    
    private void doConsistencyCheck(VOSpaceClient vos, final Subject s, VOSURI containerURI, Path mntDir, boolean createWithREST) throws Exception {
        // get numeric ID of test user
        /*
        User u = Subject.doAs(s, new PrivilegedExceptionAction<User>() {
            @Override
            public User run() throws Exception {
                LocalAuthority loc = new LocalAuthority();
                URI serviceID = loc.getServiceURI(Standards.UMS_USERS_01.toASCIIString());
                UserClient users = new UserClient(serviceID);
                Principal p = s.getPrincipals().iterator().next(); // first one
                return users.getUser(p);
            }
            
        });
        NumericPrincipal numericID = null;
        for (NumericPrincipal np : u.getIdentities(NumericPrincipal.class)) {
            numericID = np;
        }
        Assert.assertNotNull("found numericID", numericID);
        long posixUID = numericID.getUUID().getLeastSignificantBits();
        */
        
        // HACK: uidnumber on the server is currently distinct from NumericPrincipal
        long posixUID = 20006L;
                
        // create test nodes
        List<Node> testNodes = new ArrayList<>();
        List<NodeProperty> pub = new ArrayList<>();
        pub.add(new NodeProperty(VOS.PROPERTY_URI_ISPUBLIC, "true"));
        
        // create private ContainerNode
        VOSURI uriPrivCN = new VOSURI(containerURI.getURI().toASCIIString() + "/privateCN");
        testNodes.add(new ContainerNode(uriPrivCN));
            
        // create private DataNode
        VOSURI uriPrivDN = new VOSURI(containerURI.getURI().toASCIIString() + "/privateDN");
        testNodes.add(new DataNode(uriPrivDN));
        
        // create public ContainerNode
        VOSURI uriPubCN = new VOSURI(containerURI.getURI().toASCIIString() + "/publicCN");
        testNodes.add(new ContainerNode(uriPubCN, pub));
        
        // create public DataNode
        VOSURI uriPubDN = new VOSURI(containerURI.getURI().toASCIIString() + "/publicDN");
        testNodes.add(new DataNode(uriPubDN, pub));
        
        // create LinkNode (test below relies on this being after publicDN in the testNode list)
        VOSURI uriPubLN = new VOSURI(containerURI.getURI().toASCIIString() + "/link2publicDN");
        testNodes.add(new LinkNode(uriPubLN, uriPubDN.getURI()));
        
        if (createWithREST) {
            for (Node node : testNodes) {
                TestActions.CreateNodeAction doit = new TestActions.CreateNodeAction(vos, node, true);
                Node result = Subject.doAs(s, doit);
                Assert.assertNotNull(result);
                log.info("created: " + result.getUri());
            }
        } else {
            // create via mounted filesystem
            for (Node n : testNodes) {
                StringBuilder sb = new StringBuilder();
                if (n instanceof ContainerNode) {
                    sb.append("mkdir ");
                } else if (n instanceof DataNode) {
                    sb.append("touch ");
                } else {
                    sb.append("ln -s ");
                }
                if (n instanceof LinkNode) {
                    LinkNode nn = (LinkNode) n;
                    sb.append(uriPubDN.getName()).append(" "); // relative target
                }
                sb.append(mntDir).append("/").append(n.getName());
                
                String scmd = sb.toString();
                log.info("create: " + scmd);
                String[] cmd = scmd.split(" ");
                BuilderOutputGrabber grabber = new BuilderOutputGrabber();
                grabber.captureOutput(cmd);
                if (grabber.getExitValue() != 0) {
                    throw new IOException("FAIL: " + sb + "\n" + grabber.getErrorOutput());
                }
            }
        }

        // check consistency
        try {
            System.setProperty(NodeUtil.class.getName() + ".disable-get-attrs", "true");
            Path root = mntDir.getParent();
            URI localLinkTarget = null;
            for (Node node : testNodes) {
                // get from VOSpace API
                TestActions.GetNodeAction get = new TestActions.GetNodeAction(vos, node.getUri().getPath());
                Node restNode = null;
                try {
                    restNode = Subject.doAs(s, get);
                } catch (PrivilegedActionException ex) {
                    Throwable cause = ex.getCause();
                    log.error("failed to get node: " + node.getUri(), cause);
                }
                Assert.assertNotNull("rest node: " + node.getUri(), restNode);

                // get from mntDir: strip path since we mounted containerURI directly on mntDir
                VOSURI fsuri = new VOSURI("vos://" + node.getUri().getURI().getAuthority() + "/" + node.getName());
                Node fsNode = NodeUtil.get(mntDir, fsuri);
                Assert.assertNotNull("filesystem node: " + node.getUri(), fsNode);

                log.debug("found: " + restNode.getUri() + " aka " + fsNode.getUri().getPath());
                Assert.assertEquals("type", restNode.getClass(), fsNode.getClass());
                Assert.assertEquals("name", restNode.getName(), fsNode.getName());
                
                if (uriPubDN.equals(node.getUri())) {
                    localLinkTarget = fsNode.getUri().getURI();
                }
                if (restNode instanceof LinkNode) {
                    if (localLinkTarget == null) {
                        throw new RuntimeException("TEST BROKEN: trying to check LinkNode before finding target DataNode");
                    }
                    LinkNode rln = (LinkNode) restNode;
                    LinkNode fsln = (LinkNode) fsNode;
                    Assert.assertEquals("link target", localLinkTarget, fsln.getTarget());
                }
                for (NodeProperty rnp : restNode.getProperties()) {
                    String puri = rnp.getPropertyURI();
                    String rsp = restNode.getPropertyValue(puri);
                    String fsp = fsNode.getPropertyValue(puri);
                    if (VOS.PROPERTY_URI_CREATOR.equals(puri)) {
                        long fsUID = Long.parseLong(fsp);
                        log.warn("info " + puri + ": " + rsp + "(" + posixUID + ") vs " + fsUID);
                        Assert.assertEquals(puri, posixUID, fsUID);
                    } else {
                        log.info("compare: " + puri + ": " + rsp + " vs " + fsp);
                        if (puri.contains("#date")) {
                            Duration duration = Duration.between(LocalDateTime.parse(rsp), LocalDateTime.parse(fsp)).abs();
                            Assert.assertTrue(puri, duration.getSeconds() == 0);
                        } else {
                            Assert.assertEquals(puri, rsp, fsp);
                        }
                    }
                }
            }
        } finally {
            System.clearProperty(NodeUtil.class.getName() + ".disable-get-attrs");
        }
        
    }
    
    private String[] getMountCommand(String wrapper, String port, StringBuilder mnt, Path testDir) {
        // default to Linux
        String[] cmd = new String[] {
            "/bin/bash", wrapper, "-o", "password_stdin", "-p", port, mnt.toString(), testDir.toFile().getAbsolutePath()
        };

        if (isMac()) {
            // running on a MacOS
            cmd = new String[] {
                "/bin/bash", wrapper, "-o", "password_stdin", "-o", "allow_other,defer_permissions", "-p", port, mnt.toString(), testDir.toFile().getAbsolutePath()
            };
        }
            
        return cmd;
    }
    
    private String[] getUnmountCommand(Path testDir) {
        // default to Linux
        String[] cmd = new String[] {
            "fusermount", "-u", testDir.toFile().getAbsolutePath()
        };

        if (isMac()) {
            // running on a MacOS
            cmd = new String[] {
                "umount", testDir.toFile().getAbsolutePath()
            };
        }
            
        return cmd;
    }
    
    private boolean isMac() {
        boolean isMac = false;

        String[] cmd = new String[] {
            "uname", "-s"
        };
        BuilderOutputGrabber grabber = new BuilderOutputGrabber();
        grabber.captureOutput(cmd);
        if ((grabber.getExitValue() == 0) && ("Darwin".equals(grabber.getOutput()))) {
            isMac = true;
        }

        return isMac;
    }
}
