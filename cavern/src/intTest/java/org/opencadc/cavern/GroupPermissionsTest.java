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

import ca.nrc.cadc.auth.SSLUtil;
import ca.nrc.cadc.util.FileUtil;
import ca.nrc.cadc.util.Log4jInit;
import ca.nrc.cadc.util.StringUtil;
import ca.nrc.cadc.vos.ContainerNode;
import ca.nrc.cadc.vos.Node;
import ca.nrc.cadc.vos.NodeProperty;
import ca.nrc.cadc.vos.VOS;
import ca.nrc.cadc.vos.VOSURI;
import ca.nrc.cadc.vos.client.VOSpaceClient;
import java.io.File;
import java.net.URI;
import javax.security.auth.Subject;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.opencadc.gms.GroupURI;

/**
 *
 * @author pdowler
 */
public class GroupPermissionsTest {

    private static final Logger log = Logger.getLogger(GroupPermissionsTest.class);

    private static File SSL_CERT;

    private static VOSURI baseURI;
    private static GroupURI testGroup;

    static {
        Log4jInit.setLevel("org.opencadc.cavern", Level.DEBUG);
        Log4jInit.setLevel("ca.nrc.cadc.vospace", Level.INFO);
        Log4jInit.setLevel("ca.nrc.cadc.vos", Level.INFO);
    }

    public GroupPermissionsTest() {
    }

    @BeforeClass
    public static void staticInit() throws Exception {
        SSL_CERT = FileUtil.getFileFromResource("x509_CADCRegtest1.pem", MetadataIntTest.class);

        String uriProp = GroupPermissionsTest.class.getName() + ".baseURI";
        String uri = System.getProperty(uriProp);
        log.debug(uriProp + " = " + uri);
        if (StringUtil.hasText(uri)) {
            baseURI = new VOSURI(new URI(uri));
        } else {
            throw new IllegalStateException("expected system property " + uriProp + " = <base vos URI>, found: " + uri);
        }
        
        String testGroupProp = GroupPermissionsTest.class.getName() + ".testGroup";
        uri = System.getProperty(testGroupProp);
        log.debug(testGroupProp + " = " + uri);
        if (StringUtil.hasText(uri)) {
            testGroup = new GroupURI(new URI(uri));
        } else {
            throw new IllegalStateException("expected system property " + testGroupProp + " = <test group URI>, found: " + uri);
        }
    }
    
    @Test
    public void testCreateContainerGroupReadOnly() throws Exception {
        VOSpaceClient vos = new VOSpaceClient(baseURI.getServiceURI());
        String vosuripath = baseURI.toString() + "/groupPermissionsTest-" + System.currentTimeMillis();
        VOSURI turi = new VOSURI(vosuripath);
        Subject s = SSLUtil.createSubject(SSL_CERT);

        try {
            VOSURI uri = new VOSURI(turi.getURI().toASCIIString() + "/create-containerNode-gro");
            ContainerNode expected = new ContainerNode(uri);
            expected.getProperties().add(new NodeProperty(VOS.PROPERTY_URI_GROUPREAD, testGroup.getURI().toASCIIString()));
            Subject.doAs(s, new TestActions.CreateNodeAction(vos, expected));
            
            Node actual = Subject.doAs(s, new TestActions.GetNodeAction(vos, uri.getPath()));
            Assert.assertNotNull("created", actual);
            NodeProperty np = actual.findProperty(VOS.PROPERTY_URI_GROUPREAD);
            Assert.assertNotNull(np);
            Assert.assertEquals(testGroup.getURI().toASCIIString(), np.getPropertyValue());
            
        } catch (Exception unexpected) {
            log.error("unexpected exception", unexpected);
            Assert.fail("unexpected exception: " + unexpected);
        }
    }
    
    @Test
    public void testSetContainerGroupReadOnly() throws Exception {
        VOSpaceClient vos = new VOSpaceClient(baseURI.getServiceURI());
        String vosuripath = baseURI.toString() + "/groupPermissionsTest-" + System.currentTimeMillis();
        VOSURI turi = new VOSURI(vosuripath);
        Subject s = SSLUtil.createSubject(SSL_CERT);

        try {
            VOSURI uri = new VOSURI(turi.getURI().toASCIIString() + "/update-containerNode-gro");
            ContainerNode expected = new ContainerNode(uri);
            Subject.doAs(s, new TestActions.CreateNodeAction(vos, expected));
            
            Node actual = Subject.doAs(s, new TestActions.GetNodeAction(vos, uri.getPath()));
            Assert.assertNotNull("created", actual);
            
            expected.getProperties().add(new NodeProperty(VOS.PROPERTY_URI_GROUPREAD, testGroup.getURI().toASCIIString()));
            Subject.doAs(s, new TestActions.UpdateNodeAction(vos, expected));
            
            actual = Subject.doAs(s, new TestActions.GetNodeAction(vos, uri.getPath()));
            Assert.assertNotNull("updated", actual);
            
            NodeProperty np = actual.findProperty(VOS.PROPERTY_URI_GROUPREAD);
            Assert.assertNotNull(np);
            Assert.assertEquals(testGroup.getURI().toASCIIString(), np.getPropertyValue());
            
        } catch (Exception unexpected) {
            log.error("unexpected exception", unexpected);
            Assert.fail("unexpected exception: " + unexpected);
        }
    }
    
    @Test
    public void testCreateContainerGroupReadWrite() throws Exception {
        VOSpaceClient vos = new VOSpaceClient(baseURI.getServiceURI());
        String vosuripath = baseURI.toString() + "/groupPermissionsTest-" + System.currentTimeMillis();
        VOSURI turi = new VOSURI(vosuripath);
        Subject s = SSLUtil.createSubject(SSL_CERT);

        try {
            VOSURI uri = new VOSURI(turi.getURI().toASCIIString() + "/create-containerNode-grw");
            ContainerNode expected = new ContainerNode(uri);
            expected.getProperties().add(new NodeProperty(VOS.PROPERTY_URI_GROUPWRITE, testGroup.getURI().toASCIIString()));
            Subject.doAs(s, new TestActions.CreateNodeAction(vos, expected));
            
            Node actual = Subject.doAs(s, new TestActions.GetNodeAction(vos, uri.getPath()));
            Assert.assertNotNull("created", actual);
            NodeProperty np = actual.findProperty(VOS.PROPERTY_URI_GROUPWRITE);
            Assert.assertNotNull(np);
            Assert.assertEquals(testGroup.getURI().toASCIIString(), np.getPropertyValue());
            
        } catch (Exception unexpected) {
            log.error("unexpected exception", unexpected);
            Assert.fail("unexpected exception: " + unexpected);
        }
    }
    
    @Test
    public void testSetContainerGroupReadWrite() throws Exception {
        VOSpaceClient vos = new VOSpaceClient(baseURI.getServiceURI());
        String vosuripath = baseURI.toString() + "/groupPermissionsTest-" + System.currentTimeMillis();
        VOSURI turi = new VOSURI(vosuripath);
        Subject s = SSLUtil.createSubject(SSL_CERT);

        try {
            VOSURI uri = new VOSURI(turi.getURI().toASCIIString() + "/update-containerNode-grw");
            ContainerNode expected = new ContainerNode(uri);
            Subject.doAs(s, new TestActions.CreateNodeAction(vos, expected));
            
            Node actual = Subject.doAs(s, new TestActions.GetNodeAction(vos, uri.getPath()));
            Assert.assertNotNull("created", actual);
            
            expected.getProperties().add(new NodeProperty(VOS.PROPERTY_URI_GROUPWRITE, testGroup.getURI().toASCIIString()));
            Subject.doAs(s, new TestActions.UpdateNodeAction(vos, expected));
            
            actual = Subject.doAs(s, new TestActions.GetNodeAction(vos, uri.getPath()));
            Assert.assertNotNull("updated", actual);
            
            NodeProperty np = actual.findProperty(VOS.PROPERTY_URI_GROUPWRITE);
            Assert.assertNotNull(np);
            Assert.assertEquals(testGroup.getURI().toASCIIString(), np.getPropertyValue());
            
        } catch (Exception unexpected) {
            log.error("unexpected exception", unexpected);
            Assert.fail("unexpected exception: " + unexpected);
        }
    }
}
