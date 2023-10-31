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
 *  $Revision: 4 $
 *
 ************************************************************************
 */

package org.opencadc.vospace.client;

import ca.nrc.cadc.auth.AuthenticationUtil;
import ca.nrc.cadc.net.ResourceAlreadyExistsException;
import ca.nrc.cadc.net.ResourceNotFoundException;
import ca.nrc.cadc.reg.Standards;
import ca.nrc.cadc.util.FileUtil;
import ca.nrc.cadc.util.Log4jInit;
import java.io.File;
import java.net.URI;
import java.security.AccessControlException;
import java.security.PrivilegedExceptionAction;
import javax.security.auth.Subject;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.junit.Assert;
import org.junit.Test;
import org.opencadc.conformance.vos.VOSTest;
import org.opencadc.vospace.ContainerNode;
import org.opencadc.vospace.DataNode;
import org.opencadc.vospace.LinkNode;
import org.opencadc.vospace.Node;
import org.opencadc.vospace.NodeProperty;
import org.opencadc.vospace.VOS;
import org.opencadc.vospace.VOSURI;
import org.opencadc.vospace.transfer.Direction;
import org.opencadc.vospace.transfer.Protocol;
import org.opencadc.vospace.transfer.Transfer;

/**
 * Base VOSpaceClient test code. This test code requires a running VOSpace service
 * and (probably) valid X509 proxy certificates.
 *
 * @author pdowler
 */
public class VOSpaceClientTest extends VOSTest {
    private static Logger log = Logger.getLogger(VOSpaceClientTest.class);
    

    static {
        Log4jInit.setLevel("org.opencadc.conformance.vos", Level.INFO);
        Log4jInit.setLevel("org.opencadc.vospace", Level.INFO);
        Log4jInit.setLevel("ca.nrc.cadc.net", Level.INFO);
    }
    
    private static URI RESOURCE_ID = URI.create("ivo://opencadc.org/cavern");
    
    private static File CERT = FileUtil.getFileFromResource(
            System.getProperty("user.name") + ".pem", VOSpaceClientTest.class);
    
    private String baseURI = "vos://opencadc.org~cavern/client-int-tests";
    
    
    public VOSpaceClientTest() {
        super(RESOURCE_ID, CERT);
        super.rootTestFolderName = "client-int-tests";
    }

    @Test
    public void duplicateNodeFailTest() throws Exception {
        VOSpaceClient client = new VOSpaceClient(resourceID);
        ContainerNode orig = new ContainerNode("duplicateNodeFailTest");
        VOSURI target = new VOSURI(baseURI + "/" + orig.getName());
        
        Subject.doAs(authSubject, (PrivilegedExceptionAction<Void>) () -> {
            // cleanup
            try {
                client.deleteNode(target.getPath());
            } catch (ResourceNotFoundException ignore) {
                log.info("cleanup: " + ignore);
            }
        
            // create
            Node created = client.createNode(target, orig);
            log.info("created: " + created);

            // create again
            try {
                created = client.createNode(target, orig);
                Assert.fail("expected ResourceAlreadyExistsException but created: " + created);
            } catch (ResourceAlreadyExistsException expected) {
                log.info("caught expected: " + expected);
            }

            // delete
            client.deleteNode(target.getPath());
            try {
                client.getNode(target.getPath());
            } catch (ResourceNotFoundException ex) {
                log.info("caught expected: " + ex);
            }
            
            return null;
        });
    }
    
    @Test
    public void containerNodeTest() throws Exception {
        VOSpaceClient client = new VOSpaceClient(resourceID);
        ContainerNode orig = new ContainerNode("containerNodeTest");
        VOSURI target = new VOSURI(baseURI + "/" + orig.getName());
        
        Subject.doAs(authSubject, (PrivilegedExceptionAction<Void>) () -> {
            // cleanup
            try {
                client.deleteNode(target.getPath());
            } catch (ResourceNotFoundException ignore) {
                log.info("cleanup: " + ignore);
            }
        
            // create
            Node created = client.createNode(target, orig);
            log.info("created: " + created);

            // get
            Node n1 = client.getNode(target.getPath());
            log.info("found: " + n1);
            Assert.assertNotNull(n1);
            Assert.assertEquals(orig.getName(), n1.getName());
            Assert.assertEquals(orig.getClass(), n1.getClass());
            
            // update
            orig.isPublic = true;
            orig.getProperties().add(new NodeProperty(VOS.PROPERTY_URI_DESCRIPTION, "my stuff"));
            client.setNode(target, orig);
            
            // get
            Node n2 = client.getNode(target.getPath());
            log.info("found: " + n2);
            Assert.assertNotNull(n2);
            Assert.assertEquals(orig.getName(), n2.getName());
            Assert.assertEquals(orig.getClass(), n2.getClass());
            Assert.assertEquals(orig.isPublic, n2.isPublic);
            Assert.assertEquals(orig.getPropertyValue(VOS.PROPERTY_URI_DESCRIPTION),
                    n2.getPropertyValue(VOS.PROPERTY_URI_DESCRIPTION));

            // delete
            client.deleteNode(target.getPath());
            try {
                client.getNode(target.getPath());
            } catch (ResourceNotFoundException ex) {
                log.info("caught expected: " + ex);
            }
            
            return null;
        });
    }
    
    @Test
    public void dataNodeTest() throws Exception {
        VOSpaceClient client = new VOSpaceClient(resourceID);
        DataNode orig = new DataNode("dataNodeTest");
        VOSURI target = new VOSURI(baseURI + "/" + orig.getName());
        
        Subject.doAs(authSubject, (PrivilegedExceptionAction<Void>) () -> {
            // cleanup
            try {
                client.deleteNode(target.getPath());
            } catch (ResourceNotFoundException ignore) {
                log.info("cleanup: " + ignore);
            }
        
            // create
            Node created = client.createNode(target, orig);
            log.info("created: " + created);

            // get
            Node n1 = client.getNode(target.getPath());
            log.info("found: " + n1);
            Assert.assertNotNull(n1);
            Assert.assertEquals(orig.getName(), n1.getName());
            Assert.assertEquals(orig.getClass(), n1.getClass());

            // update
            orig.isPublic = true;
            orig.getProperties().add(new NodeProperty(VOS.PROPERTY_URI_DESCRIPTION, "my stuff"));
            client.setNode(target, orig);
            
            // get
            Node n2 = client.getNode(target.getPath());
            log.info("found: " + n2);
            Assert.assertNotNull(n2);
            Assert.assertEquals(orig.getName(), n2.getName());
            Assert.assertEquals(orig.getClass(), n2.getClass());
            Assert.assertEquals(orig.isPublic, n2.isPublic);
            Assert.assertEquals(orig.getPropertyValue(VOS.PROPERTY_URI_DESCRIPTION),
                    n2.getPropertyValue(VOS.PROPERTY_URI_DESCRIPTION));

            // delete
            client.deleteNode(target.getPath());
            try {
                client.getNode(target.getPath());
            } catch (ResourceNotFoundException ex) {
                log.info("caught expected: " + ex);
            }
            
            return null;
        });
    }
    
    @Test
    public void linkNodeTest() throws Exception {
        VOSpaceClient client = new VOSpaceClient(resourceID);
        LinkNode orig = new LinkNode("linkNodeTest", URI.create(baseURI + "/linkTarget"));
        VOSURI target = new VOSURI(baseURI + "/" + orig.getName());
        
        Subject.doAs(authSubject, (PrivilegedExceptionAction<Void>) () -> {
            // cleanup
            try {
                client.deleteNode(target.getPath());
            } catch (ResourceNotFoundException ignore) {
                log.info("cleanup: " + ignore);
            }
        
            // create
            Node created = client.createNode(target, orig);
            log.info("created: " + created);

            // get
            Node n1 = client.getNode(target.getPath());
            log.info("found: " + n1);
            Assert.assertNotNull(n1);
            Assert.assertEquals(orig.getName(), n1.getName());
            Assert.assertEquals(orig.getClass(), n1.getClass());
            LinkNode actual = (LinkNode) n1;
            Assert.assertEquals(orig.getTarget(), actual.getTarget());

            // update
            orig.isPublic = true;
            Node mod = client.setNode(target, orig);
            log.info("modified: " + mod);
            
            // get
            Node n2 = client.getNode(target.getPath());
            log.info("found: " + n2);
            Assert.assertNotNull(n2);
            Assert.assertEquals(orig.getName(), n2.getName());
            Assert.assertEquals(orig.getClass(), n2.getClass());
            Assert.assertEquals(orig.isPublic, n2.isPublic);
            actual = (LinkNode) n2;
            Assert.assertEquals(orig.getTarget(), actual.getTarget());
            
            // failed update
            orig.getProperties().add(new NodeProperty(VOS.PROPERTY_URI_DESCRIPTION, "the missing link"));
            try {
                mod = client.setNode(target, orig);
                Assert.fail("expected IllegalArgumentException but got: " + mod);
            } catch (IllegalArgumentException ex) {
                log.info("caught expected: " + ex);
            }
            
            // delete
            client.deleteNode(target.getPath());
            try {
                client.getNode(target.getPath());
            } catch (ResourceNotFoundException ex) {
                log.info("caught expected: " + ex);
            }
            
            return null;
        });
    }
    
    @Test
    public void fileRountTripTest() throws Exception {
        VOSpaceClient client = new VOSpaceClient(resourceID);
        final File srcFile = FileUtil.getFileFromResource("round-trip.txt", VOSpaceClientTest.class);
        DataNode orig = new DataNode(srcFile.getName());
        VOSURI target = new VOSURI(baseURI + "/" + orig.getName());
        
        Subject.doAs(authSubject, (PrivilegedExceptionAction<Void>) () -> {
            // cleanup
            try {
                client.deleteNode(target.getPath());
            } catch (ResourceNotFoundException ignore) {
                log.info("cleanup: " + ignore);
            }
        
            Transfer push = new Transfer(Direction.pushToVoSpace);
            push.getTargets().add(target.getURI());
            Protocol p = new Protocol(VOS.PROTOCOL_HTTPS_PUT);
            p.setSecurityMethod(Standards.SECURITY_METHOD_CERT);
            push.getProtocols().add(p);
            ClientTransfer putTrans = client.createTransfer(push);
            putTrans.setFile(srcFile);
            putTrans.run();
            log.info("upload: " + putTrans.getPhase() + " " + putTrans.getThrowable());
            Assert.assertNull(putTrans.getThrowable());
            
            // get
            Node n1 = client.getNode(target.getPath());
            log.info("found: " + n1);
            Assert.assertNotNull(n1);
            Assert.assertEquals(orig.getName(), n1.getName());
            Assert.assertEquals(orig.getClass(), n1.getClass());

            // get file
            File destFile = new File("build/tmp/" + srcFile.getName());
            if (destFile.exists()) {
                destFile.delete();
            }
            Assert.assertFalse(destFile.exists());
            
            Transfer pull = new Transfer(Direction.pullFromVoSpace);
            pull.getTargets().add(target.getURI());
            p = new Protocol(VOS.PROTOCOL_HTTPS_GET);
            p.setSecurityMethod(Standards.SECURITY_METHOD_CERT);
            pull.getProtocols().add(p);
            ClientTransfer getTrans = client.createTransfer(pull);
            getTrans.setFile(destFile);
            getTrans.run();
            log.info("download: " + getTrans.getPhase() + " " + getTrans.getThrowable());
            Assert.assertNull(getTrans.getThrowable());
            Assert.assertTrue(destFile.exists());
            
            // delete
            client.deleteNode(target.getPath());
            try {
                client.getNode(target.getPath());
            } catch (ResourceNotFoundException ex) {
                log.info("caught expected: " + ex);
            }
            
            return null;
        });
    }
}
