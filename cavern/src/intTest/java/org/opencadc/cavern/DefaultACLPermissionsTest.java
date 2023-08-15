/*
************************************************************************
*******************  CANADIAN ASTRONOMY DATA CENTRE  *******************
**************  CENTRE CANADIEN DE DONNÉES ASTRONOMIQUES  **************
*
*  (c) 2019.                            (c) 2019.
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

import ca.nrc.cadc.auth.PosixPrincipal;
import ca.nrc.cadc.auth.SSLUtil;
import ca.nrc.cadc.util.FileUtil;
import ca.nrc.cadc.util.Log4jInit;
import ca.nrc.cadc.util.StringUtil;
import ca.nrc.cadc.vos.ContainerNode;
import ca.nrc.cadc.vos.DataNode;
import ca.nrc.cadc.vos.Node;
import ca.nrc.cadc.vos.NodeProperty;
import ca.nrc.cadc.vos.VOS;
import ca.nrc.cadc.vos.VOSURI;
import ca.nrc.cadc.vos.client.VOSpaceClient;
import java.io.File;
import java.net.URI;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Properties;
import java.util.Set;
import java.util.stream.Collectors;

import javax.security.auth.Subject;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.opencadc.gms.GroupURI;

/**
 * This test relies on the two base directories having default ACLs set to
 * group-readOnly and group-readWrite respectively.  That currently must
 * be done in a linux terminal. For example:
 * 
 * setfacl -d -m group:the-group-name:r-x read-only-dir
 * setfacl -d -m group:the-group-name:rwx read-write-dir
 * 
 * TODO: Turn this into an unit test that execs the above commands first.
 *
 * @author majorb
 */
public class DefaultACLPermissionsTest {

    private static final Logger log = Logger.getLogger(DefaultACLPermissionsTest.class);

    private static File SSL_CERT;

    private static VOSURI roBaseURI;
    private static VOSURI rwBaseURI;
    private static GroupURI testGroup;

    // Second group to be added for multiples.
    private static GroupURI testGroupReadAlt;
    private static GroupURI testGroupWriteAlt;


    static {
        Log4jInit.setLevel("org.opencadc.cavern", Level.DEBUG);
        Log4jInit.setLevel("ca.nrc.cadc.vospace", Level.INFO);
        Log4jInit.setLevel("ca.nrc.cadc.vos", Level.DEBUG);
        Log4jInit.setLevel("ca.nrc.cadc.net", Level.DEBUG);
    }

    public DefaultACLPermissionsTest() {
    }

    @BeforeClass
    public static void staticInit() throws Exception {
        SSL_CERT = FileUtil.getFileFromResource("x509_CADCRegtest1.pem", MetadataIntTest.class);

        String uriROProp = DefaultACLPermissionsTest.class.getName() + ".baseURI.readOnly";
        String uriRWProp = DefaultACLPermissionsTest.class.getName() + ".baseURI.readWrite";
        if (log.isDebugEnabled()) {
            Properties props = System.getProperties();
            Set<Object> keys = props.keySet();
            Iterator<Object> keyIt = keys.iterator();
            while (keyIt.hasNext()) {
                Object key = keyIt.next();
                log.debug("System prop: " + key + "=" + props.getProperty((String) key));
            }
        }
        String roUri = System.getProperty(uriROProp);
        log.debug(uriROProp + " = " + roUri);
        if (StringUtil.hasText(roUri)) {
            roBaseURI = new VOSURI(new URI(roUri));
        } else {
            throw new IllegalStateException("expected system property " + uriROProp + " = <base vos URI>, found: " + roUri);
        }
        String rwUri = System.getProperty(uriRWProp);
        log.debug(uriRWProp + " = " + rwUri);
        if (StringUtil.hasText(rwUri)) {
            rwBaseURI = new VOSURI(new URI(rwUri));
        } else {
            throw new IllegalStateException("expected system property " + uriRWProp + " = <base vos URI>, found: " + rwUri);
        }
        
        String testGroupProp = DefaultACLPermissionsTest.class.getName() + ".testGroup";
        String uri = System.getProperty(testGroupProp);
        log.debug(testGroupProp + " = " + uri);
        if (StringUtil.hasText(uri)) {
            testGroup = new GroupURI(new URI(uri));
        } else {
            throw new IllegalStateException("expected system property " + testGroupProp + " = <test group URI>, found: " + uri);
        }

        String testGroupReadAltProp = DefaultACLPermissionsTest.class.getName() + ".testGroupReadAlt";
        String testGroupReadAltURI = System.getProperty(testGroupReadAltProp);
        log.debug(testGroupReadAltProp + " = " + testGroupReadAltURI);
        if (StringUtil.hasText(testGroupReadAltURI)) {
            testGroupReadAlt = new GroupURI(new URI(testGroupReadAltURI));
        } else {
            throw new IllegalStateException("expected system property " + testGroupReadAltProp + " = <test group URI>, found: " + testGroupReadAltURI);
        }

        String testGroupWriteAltProp = DefaultACLPermissionsTest.class.getName() + ".testGroupWriteAlt";
        String testGroupWriteAltURI = System.getProperty(testGroupWriteAltProp);
        log.debug(testGroupWriteAltProp + " = " + testGroupWriteAltURI);
        if (StringUtil.hasText(testGroupWriteAltURI)) {
            testGroupWriteAlt = new GroupURI(new URI(testGroupWriteAltURI));
        } else {
            throw new IllegalStateException("expected system property " + testGroupWriteAltProp + " = <test group URI>, found: " + testGroupWriteAltURI);
        }
    }

    @Test
    public void testCreateChildContainerNodeMultiReadGroups() throws Exception {
        VOSpaceClient vos = new VOSpaceClient(roBaseURI.getServiceURI());
        String vosURIPath = roBaseURI.toString() + "/ro-containernode-multiread-" + System.currentTimeMillis();
        VOSURI turi = new VOSURI(vosURIPath);
        Subject s = SSLUtil.createSubject(SSL_CERT);
        s.getPrincipals().add(new PosixPrincipal(20006));

        ContainerNode expected = new ContainerNode(turi);
        final String[] readGroups = new String[] {
                testGroup.getURI().toASCIIString(),
                testGroupReadAlt.getURI().toASCIIString()
        };

        expected.getProperties().add(new NodeProperty(VOS.PROPERTY_URI_GROUPREAD,
                                                      String.join(" ", readGroups)));

        Subject.doAs(s, new TestActions.CreateNodeAction(vos, expected));

        Node actual = Subject.doAs(s, new TestActions.GetNodeAction(vos, turi.getPath()));
        Assert.assertNotNull("created", actual);
        final String readGroupNodePropertyValue = actual.getPropertyValue(VOS.PROPERTY_URI_GROUPREAD);
        final List<String> readGroupPropertyList =
                Arrays.stream(readGroupNodePropertyValue.split(" ")).collect(Collectors.toList());
        Assert.assertTrue("Should contain " + testGroup.getURI(),
                          readGroupPropertyList.contains(testGroup.getURI().toASCIIString()));
        Assert.assertTrue("Should contain " + testGroupReadAlt.getURI(),
                          readGroupPropertyList.contains(testGroupReadAlt.getURI().toASCIIString()));
        NodeProperty mask = actual.findProperty(VOS.PROPERTY_URI_GROUPMASK);
        Assert.assertNotNull(mask);
        Assert.assertEquals("r-x", mask.getPropertyValue());
    }

    @Test
    public void testCreateChildContainerNodeMultiWriteGroups() throws Exception {
        VOSpaceClient vos = new VOSpaceClient(rwBaseURI.getServiceURI());
        String vosURIPath = rwBaseURI.toString() + "/rw-containernode-multiwrite-" + System.currentTimeMillis();
        VOSURI turi = new VOSURI(vosURIPath);
        Subject s = SSLUtil.createSubject(SSL_CERT);

        ContainerNode expected = new ContainerNode(turi);
        final String[] writeGroups = new String[] {
                testGroup.getURI().toASCIIString(),
                testGroupWriteAlt.getURI().toASCIIString()
        };

        expected.getProperties().add(new NodeProperty(VOS.PROPERTY_URI_GROUPWRITE,
                                                      String.join(" ", writeGroups)));

        Subject.doAs(s, new TestActions.CreateNodeAction(vos, expected));

        Node actual = Subject.doAs(s, new TestActions.GetNodeAction(vos, turi.getPath()));
        Assert.assertNotNull("created", actual);
        final String writeGroupNodePropertyValue = actual.getPropertyValue(VOS.PROPERTY_URI_GROUPWRITE);
        final List<String> writeGroupPropertyList =
                Arrays.stream(writeGroupNodePropertyValue.split(" ")).collect(Collectors.toList());

        Assert.assertTrue("Should contain " + testGroup.getURI(),
                          writeGroupPropertyList.contains(testGroup.getURI().toASCIIString()));
        Assert.assertTrue("Should contain " + testGroupWriteAlt.getURI(),
                          writeGroupPropertyList.contains(testGroupWriteAlt.getURI().toASCIIString()));
        NodeProperty mask = actual.findProperty(VOS.PROPERTY_URI_GROUPMASK);
        Assert.assertNotNull(mask);
        Assert.assertEquals("rwx", mask.getPropertyValue());
    }
    
    @Test
    public void testCreateChildContainerNodeDefaultReadOnlyGroup() throws Exception {
        VOSpaceClient vos = new VOSpaceClient(roBaseURI.getServiceURI());
        String vosuripath = roBaseURI.toString() + "/ro-containernode-" + System.currentTimeMillis();
        VOSURI turi = new VOSURI(vosuripath);
        Subject s = SSLUtil.createSubject(SSL_CERT);

        try {
            ContainerNode expected = new ContainerNode(turi);
            Subject.doAs(s, new TestActions.CreateNodeAction(vos, expected));
            
            Node actual = Subject.doAs(s, new TestActions.GetNodeAction(vos, turi.getPath()));
            Assert.assertNotNull("created", actual);
            NodeProperty np = actual.findProperty(VOS.PROPERTY_URI_GROUPREAD);
            Assert.assertNotNull(np);
            Assert.assertEquals(testGroup.getURI().toASCIIString(), np.getPropertyValue());
            NodeProperty mask = actual.findProperty(VOS.PROPERTY_URI_GROUPMASK);
            Assert.assertNotNull(mask);
            Assert.assertEquals("r-x", mask.getPropertyValue());
            
        } catch (Exception unexpected) {
            log.error("unexpected exception", unexpected);
            Assert.fail("unexpected exception: " + unexpected);
        }
    }
    
    @Test
    public void testCreateChildContainerNodeDefaultReadWriteGroup() throws Exception {
        VOSpaceClient vos = new VOSpaceClient(rwBaseURI.getServiceURI());
        String vosuripath = rwBaseURI.toString() + "/rw-containernode-" + System.currentTimeMillis();
        VOSURI turi = new VOSURI(vosuripath);
        Subject s = SSLUtil.createSubject(SSL_CERT);

        try {
            ContainerNode expected = new ContainerNode(turi);
            Subject.doAs(s, new TestActions.CreateNodeAction(vos, expected));
            
            Node actual = Subject.doAs(s, new TestActions.GetNodeAction(vos, turi.getPath()));
            Assert.assertNotNull("created", actual);
            NodeProperty np = actual.findProperty(VOS.PROPERTY_URI_GROUPWRITE);
            Assert.assertNotNull(np);
            Assert.assertEquals(testGroup.getURI().toASCIIString(), np.getPropertyValue());
            NodeProperty mask = actual.findProperty(VOS.PROPERTY_URI_GROUPMASK);
            Assert.assertNotNull(mask);
            Assert.assertEquals("rwx", mask.getPropertyValue());
            
        } catch (Exception unexpected) {
            log.error("unexpected exception", unexpected);
            Assert.fail("unexpected exception: " + unexpected);
        }
    }
    
    @Test
    public void testCreateChildDataNodeDefaultReadOnlyGroup() throws Exception {
        VOSpaceClient vos = new VOSpaceClient(roBaseURI.getServiceURI());
        String vosuripath = roBaseURI.toString() + "/ro-datanode-" + System.currentTimeMillis();
        VOSURI turi = new VOSURI(vosuripath);
        Subject s = SSLUtil.createSubject(SSL_CERT);

        try {
            DataNode expected = new DataNode(turi);
            Subject.doAs(s, new TestActions.CreateNodeAction(vos, expected));
            
            Node actual = Subject.doAs(s, new TestActions.GetNodeAction(vos, turi.getPath()));
            Assert.assertNotNull("created", actual);
            NodeProperty np = actual.findProperty(VOS.PROPERTY_URI_GROUPREAD);
            Assert.assertNotNull(np);
            Assert.assertEquals(testGroup.getURI().toASCIIString(), np.getPropertyValue());
            
            NodeProperty mask = actual.findProperty(VOS.PROPERTY_URI_GROUPMASK);
            Assert.assertNotNull(mask);
            Assert.assertEquals("r--", mask.getPropertyValue());
            
        } catch (Exception unexpected) {
            log.error("unexpected exception", unexpected);
            Assert.fail("unexpected exception: " + unexpected);
        }
    }
    
    @Test
    public void testCreateChildDataNodeDefaultReadWriteGroup() throws Exception {
        VOSpaceClient vos = new VOSpaceClient(rwBaseURI.getServiceURI());
        String vosuripath = rwBaseURI.toString() + "/rw-datanode-" + System.currentTimeMillis();
        VOSURI turi = new VOSURI(vosuripath);
        Subject s = SSLUtil.createSubject(SSL_CERT);

        DataNode expected = new DataNode(turi);
        Subject.doAs(s, new TestActions.CreateNodeAction(vos, expected));

        Node actual = Subject.doAs(s, new TestActions.GetNodeAction(vos, turi.getPath()));
        Assert.assertNotNull("created", actual);
        NodeProperty np = actual.findProperty(VOS.PROPERTY_URI_GROUPWRITE);
        Assert.assertNotNull(np);
        Assert.assertEquals(testGroup.getURI().toASCIIString(), np.getPropertyValue());
        NodeProperty mask = actual.findProperty(VOS.PROPERTY_URI_GROUPMASK);
        Assert.assertNotNull(mask);
        Assert.assertEquals("rw-", mask.getPropertyValue());
    }
    
    @Test
    public void testContainerNodeTwoLevelsDeep() throws Exception {
        VOSpaceClient vos = new VOSpaceClient(roBaseURI.getServiceURI());
        String vosuripath = roBaseURI.toString() + "/ro-containernode-" + System.currentTimeMillis();
        VOSURI turi = new VOSURI(vosuripath);
        Subject s = SSLUtil.createSubject(SSL_CERT);

        ContainerNode expected = new ContainerNode(turi);
        Subject.doAs(s, new TestActions.CreateNodeAction(vos, expected));

        Node actual = Subject.doAs(s, new TestActions.GetNodeAction(vos, turi.getPath()));
        Assert.assertNotNull("created", actual);
        NodeProperty np = actual.findProperty(VOS.PROPERTY_URI_GROUPREAD);
        Assert.assertNotNull(np);
        Assert.assertEquals(testGroup.getURI().toASCIIString(), np.getPropertyValue());
        NodeProperty mask = actual.findProperty(VOS.PROPERTY_URI_GROUPMASK);
        Assert.assertNotNull(mask);
        Assert.assertEquals("r-x", mask.getPropertyValue());

        VOSURI curi = new VOSURI(turi.getURI().toString() + "/" + "container-child");
        ContainerNode child = new ContainerNode(curi);
        log.debug("Creating " + curi.getPath());
        Subject.doAs(s, new TestActions.CreateNodeAction(vos, child));
        log.debug("Creating " + curi.getPath() + ":OK");

        actual = Subject.doAs(s, new TestActions.GetNodeAction(vos, curi.getPath()));
        Assert.assertNotNull("created", actual);
        np = actual.findProperty(VOS.PROPERTY_URI_GROUPREAD);
        Assert.assertNotNull(np);
        Assert.assertEquals(testGroup.getURI().toASCIIString(), np.getPropertyValue());
        mask = actual.findProperty(VOS.PROPERTY_URI_GROUPMASK);
        Assert.assertNotNull(mask);
        Assert.assertEquals("r-x", mask.getPropertyValue());
    }

    @Test
    public void testDataNodeTwoLevelsDeep() throws Exception {
        VOSpaceClient vos = new VOSpaceClient(roBaseURI.getServiceURI());
        String vosuripath = roBaseURI.toString() + "/ro-containernode-" + System.currentTimeMillis();
        VOSURI turi = new VOSURI(vosuripath);
        Subject s = SSLUtil.createSubject(SSL_CERT);

        try {
            ContainerNode expected = new ContainerNode(turi);
            Subject.doAs(s, new TestActions.CreateNodeAction(vos, expected));
            
            Node actual = Subject.doAs(s, new TestActions.GetNodeAction(vos, turi.getPath()));
            Assert.assertNotNull("created", actual);
            NodeProperty np = actual.findProperty(VOS.PROPERTY_URI_GROUPREAD);
            Assert.assertNotNull(np);
            Assert.assertEquals(testGroup.getURI().toASCIIString(), np.getPropertyValue());
            NodeProperty mask = actual.findProperty(VOS.PROPERTY_URI_GROUPMASK);
            Assert.assertNotNull(mask);
            Assert.assertEquals("r-x", mask.getPropertyValue());
            
            VOSURI curi = new VOSURI(turi.getURI().toString() + "/" + "data-child");
            DataNode child = new DataNode(curi);
            Subject.doAs(s, new TestActions.CreateNodeAction(vos, child));
            
            actual = Subject.doAs(s, new TestActions.GetNodeAction(vos, curi.getPath()));
            Assert.assertNotNull("created", actual);
            np = actual.findProperty(VOS.PROPERTY_URI_GROUPREAD);
            Assert.assertNotNull(np);
            Assert.assertEquals(testGroup.getURI().toASCIIString(), np.getPropertyValue());
            mask = actual.findProperty(VOS.PROPERTY_URI_GROUPMASK);
            Assert.assertNotNull(mask);
            Assert.assertEquals("r--", mask.getPropertyValue());
            
        } catch (Exception unexpected) {
            log.error("unexpected exception", unexpected);
            Assert.fail("unexpected exception: " + unexpected);
        }
    }
    
    @Test
    public void testUpdateContainerNode() throws Exception {
        VOSpaceClient vos = new VOSpaceClient(roBaseURI.getServiceURI());
        String vosuripath = roBaseURI.toString() + "/ro-containernode-" + System.currentTimeMillis();
        VOSURI turi = new VOSURI(vosuripath);
        Subject s = SSLUtil.createSubject(SSL_CERT);

        try {
            ContainerNode expected = new ContainerNode(turi);
            Subject.doAs(s, new TestActions.CreateNodeAction(vos, expected));
            
            Node actual = Subject.doAs(s, new TestActions.GetNodeAction(vos, turi.getPath()));
            Assert.assertNotNull("created", actual);
            NodeProperty np = actual.findProperty(VOS.PROPERTY_URI_GROUPREAD);
            Assert.assertNotNull(np);
            Assert.assertEquals(testGroup.getURI().toASCIIString(), np.getPropertyValue());
            NodeProperty mask = actual.findProperty(VOS.PROPERTY_URI_GROUPMASK);
            Assert.assertNotNull(mask);
            Assert.assertEquals("r-x", mask.getPropertyValue());
            
            actual.getProperties().add(new NodeProperty("key", "value"));
            Node updated = Subject.doAs(s, new TestActions.UpdateNodeAction(vos, actual));
            
            Assert.assertNotNull("created", updated);
            np = updated.findProperty(VOS.PROPERTY_URI_GROUPREAD);
            Assert.assertNotNull(np);
            Assert.assertEquals(testGroup.getURI().toASCIIString(), np.getPropertyValue());
            np = updated.findProperty("key");
            Assert.assertNotNull(np);
            Assert.assertEquals("value", np.getPropertyValue());
            mask = actual.findProperty(VOS.PROPERTY_URI_GROUPMASK);
            Assert.assertNotNull(mask);
            Assert.assertEquals("r-x", mask.getPropertyValue());
            
        } catch (Exception unexpected) {
            log.error("unexpected exception", unexpected);
            Assert.fail("unexpected exception: " + unexpected);
        }
    }
    
    @Test
    public void testUpdateDataNode() throws Exception {
        VOSpaceClient vos = new VOSpaceClient(roBaseURI.getServiceURI());
        String vosuripath = roBaseURI.toString() + "/ro-datanode-" + System.currentTimeMillis();
        VOSURI turi = new VOSURI(vosuripath);
        Subject s = SSLUtil.createSubject(SSL_CERT);

        try {
            DataNode expected = new DataNode(turi);
            Subject.doAs(s, new TestActions.CreateNodeAction(vos, expected));
            
            Node actual = Subject.doAs(s, new TestActions.GetNodeAction(vos, turi.getPath()));
            Assert.assertNotNull("created", actual);
            NodeProperty np = actual.findProperty(VOS.PROPERTY_URI_GROUPREAD);
            Assert.assertNotNull(np);
            Assert.assertEquals(testGroup.getURI().toASCIIString(), np.getPropertyValue());
            NodeProperty mask = actual.findProperty(VOS.PROPERTY_URI_GROUPMASK);
            Assert.assertNotNull(mask);
            Assert.assertEquals("r--", mask.getPropertyValue());
            
            actual.getProperties().add(new NodeProperty("key", "value"));
            Node updated = Subject.doAs(s, new TestActions.UpdateNodeAction(vos, actual));
            
            Assert.assertNotNull("created", updated);
            np = updated.findProperty(VOS.PROPERTY_URI_GROUPREAD);
            Assert.assertNotNull(np);
            Assert.assertEquals(testGroup.getURI().toASCIIString(), np.getPropertyValue());
            np = updated.findProperty("key");
            Assert.assertNotNull(np);
            Assert.assertEquals("value", np.getPropertyValue());
            mask = actual.findProperty(VOS.PROPERTY_URI_GROUPMASK);
            Assert.assertNotNull(mask);
            Assert.assertEquals("r--", mask.getPropertyValue());
            
        } catch (Exception unexpected) {
            log.error("unexpected exception", unexpected);
            Assert.fail("unexpected exception: " + unexpected);
        }
    }
}
