/*
 ************************************************************************
 *******************  CANADIAN ASTRONOMY DATA CENTRE  *******************
 **************  CENTRE CANADIEN DE DONNÉES ASTRONOMIQUES  **************
 *
 *  (c) 2010.                            (c) 2010.
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

package ca.nrc.cadc.vos.server.auth;

import static org.easymock.EasyMock.createMock;
import static org.easymock.EasyMock.expect;
import static org.easymock.EasyMock.replay;
import static org.easymock.EasyMock.verify;

import java.io.File;
import java.net.URI;
import java.net.URL;
import java.security.AccessControlException;
import java.security.PrivilegedExceptionAction;
import java.util.Calendar;
import java.util.GregorianCalendar;
import java.util.TimeZone;

import javax.security.auth.Subject;
import javax.security.auth.x500.X500Principal;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.junit.Assert;
import org.junit.Test;

import ca.nrc.cadc.auth.SignedToken;
import ca.nrc.cadc.auth.HttpPrincipal;
import ca.nrc.cadc.util.FileUtil;
import ca.nrc.cadc.util.Log4jInit;
import ca.nrc.cadc.util.RsaSignatureGenerator;
import ca.nrc.cadc.vos.ContainerNode;
import ca.nrc.cadc.vos.DataNode;
import ca.nrc.cadc.vos.Node;
import ca.nrc.cadc.vos.NodeLockedException;
import ca.nrc.cadc.vos.NodeProperty;
import ca.nrc.cadc.vos.VOS;
import ca.nrc.cadc.vos.VOSURI;
import ca.nrc.cadc.vos.server.NodeID;
import ca.nrc.cadc.vos.server.NodePersistence;

/**
 * Test class for the VOSpaceAuthorizer.
 */
public class VOSpaceAuthorizerTest
{
    private static final Logger log = Logger.getLogger(VOSpaceAuthorizerTest.class);
    
    private String NODE_OWNER = "cn=cadc authtest1 10627,ou=cadc,o=hia";
    private String NODE_OWNER_ID = "cadcauthtest1";

    static
    {
        Log4jInit.setLevel("ca.nrc.cadc.vos", Level.DEBUG);
    }

    @Test
    public void testReadPermissonOnRoot() throws Exception
    {
        VOSURI vos = new VOSURI(new URI("vos://cadc.nrc.ca!vospace/CADCAuthtest1"));
        ContainerNode node = new ContainerNode(vos);
        node.getProperties().add(new NodeProperty(VOS.PROPERTY_URI_CREATOR, NODE_OWNER));
        node.getProperties().add(new NodeProperty(VOS.PROPERTY_URI_ISPUBLIC, Boolean.TRUE.toString()));

        NodePersistence np = createMock(NodePersistence.class);
        expect(np.get(vos, false, true)).andReturn(node).once();
        replay(np);

        VOSpaceAuthorizer voSpaceAuthorizer = new VOSpaceAuthorizer();
        voSpaceAuthorizer.setNodePersistence(np);

        Subject subject = new Subject();
        subject.getPrincipals().add(new X500Principal(NODE_OWNER));

        // fake persistent node
        node.appData = new NodeID(new Long(123L), subject, NODE_OWNER);

        ReadPermissionAction action = new ReadPermissionAction( voSpaceAuthorizer, vos.getURI());
        Subject.doAs(subject, action);

        verify(np);
    }

    @Test
    public void testNodeLocked() throws Exception
    {
        VOSURI vos = new VOSURI(new URI("vos://cadc.nrc.ca!vospace/CADCAuthtest1"));
        ContainerNode node = new ContainerNode(vos);
        node.getProperties().add(new NodeProperty(VOS.PROPERTY_URI_CREATOR, NODE_OWNER));
        node.getProperties().add(new NodeProperty(VOS.PROPERTY_URI_ISPUBLIC, Boolean.TRUE.toString()));
        node.getProperties().add(new NodeProperty(VOS.PROPERTY_URI_ISLOCKED, Boolean.TRUE.toString()));

        NodePersistence np = createMock(NodePersistence.class);
        expect(np.get(vos, false, true)).andReturn(node).once();
        replay(np);

        VOSpaceAuthorizer voSpaceAuthorizer = new VOSpaceAuthorizer();
        voSpaceAuthorizer.setNodePersistence(np);

        Subject subject = new Subject();
        subject.getPrincipals().add(new X500Principal(NODE_OWNER));

        // fake persistent node
        node.appData = new NodeID(new Long(123L), subject, NODE_OWNER);

        WritePermissionAction action = new WritePermissionAction(voSpaceAuthorizer, vos.getURI());
        try
        {
            Subject.doAs(subject, action);
            Assert.fail("Should have received NodeLockedException");
        }
        catch (Exception e)
        {
            if (!(e instanceof NodeLockedException))
            {
                Assert.fail("Should have received NodeLockedException");
            }
        }

        verify(np);
    }


    @Test
    public void testCheckDelegation() throws Exception
    {
        // create keys
        RsaSignatureGenerator.genKeyPair(getCompleteKeysDirName());

        VOSURI vos = new VOSURI(new URI("vos://cadc.nrc.ca!vospace/CADCAuthtest1"));
        ContainerNode node = new ContainerNode(vos);
        node.getProperties().add(new NodeProperty(VOS.PROPERTY_URI_CREATOR, NODE_OWNER));

        NodePersistence np = createMock(NodePersistence.class);
        expect(np.get(vos, false, true)).andReturn(node).once();
        replay(np);

        VOSpaceAuthorizer voSpaceAuthorizer = new VOSpaceAuthorizer();
        voSpaceAuthorizer.setNodePersistence(np);

        Calendar expiry = new GregorianCalendar(TimeZone.getTimeZone("UTC"));
        expiry.add(Calendar.HOUR, 10);

        // create the delegation cookie
        SignedToken dt = new SignedToken(
                new HttpPrincipal(NODE_OWNER_ID), vos.getURI(), expiry.getTime(), null);

        Subject subject = new Subject();
        subject.getPrincipals().add(new HttpPrincipal(NODE_OWNER_ID));
        subject.getPublicCredentials().add(dt);

        // fake persistent node
        node.appData = new NodeID(new Long(123L), subject, NODE_OWNER);

        WritePermissionAction action = new WritePermissionAction(voSpaceAuthorizer, vos.getURI());
        Subject.doAs(subject, action);

        verify(np);


        // do the same with a subnode
        vos = new VOSURI(new URI("vos://cadc.nrc.ca!vospace/CADCAuthtest1/testnode"));
        node = new ContainerNode(vos);
        node.getProperties().add(new NodeProperty(VOS.PROPERTY_URI_CREATOR, NODE_OWNER));

        np = createMock(NodePersistence.class);
        expect(np.get(vos, false, true)).andReturn(node).once();
        replay(np);

        voSpaceAuthorizer = new VOSpaceAuthorizer();
        voSpaceAuthorizer.setNodePersistence(np);

        // create the delegation cookie
        dt = new SignedToken(
                new HttpPrincipal(NODE_OWNER_ID),
                vos.getParentURI().getURI(), expiry.getTime(), null);
        subject = new Subject();
        subject.getPrincipals().add(new HttpPrincipal(NODE_OWNER_ID));
        subject.getPublicCredentials().add(dt);

        // fake persistent node
        node.appData = new NodeID(new Long(123L), subject, NODE_OWNER);

        action = new WritePermissionAction( voSpaceAuthorizer, vos.getURI());
        Subject.doAs(subject, action);

        verify(np);

        // check scope missmatch
        np = createMock(NodePersistence.class);
        expect(np.get(vos, false, true)).andReturn(node).once();
        replay(np);

        voSpaceAuthorizer = new VOSpaceAuthorizer();
        voSpaceAuthorizer.setNodePersistence(np);

        dt = new SignedToken(
                new HttpPrincipal(NODE_OWNER_ID),
                new URI("vos://cadc.nrc.ca~vospace/otherspace"), expiry.getTime(), null);
        subject = new Subject();
        subject.getPrincipals().add(new HttpPrincipal(NODE_OWNER_ID));
        subject.getPublicCredentials().add(dt);

        // fake persistent node
        node.appData = new NodeID(new Long(123L), subject, NODE_OWNER);

        action = new WritePermissionAction(voSpaceAuthorizer, vos.getURI());
        try
        {
            Subject.doAs(subject, action);
            Assert.fail("Should have received AccessControlException");
        }
        catch (Exception e)
        {
            if (!(e instanceof AccessControlException))
            {
                Assert.fail("Should have received AccessControlException");
            }
        }

        verify(np);
    }
    
    @Test
    public void testReadMask() throws Exception {
        try {
            VOSpaceAuthorizer vauth = new VOSpaceAuthorizer();
            VOSURI vos = new VOSURI(new URI("vos://cadc.nrc.ca!vospace/CADCAuthtest1"));
            Node n = null;
            
            n = new DataNode(vos);
            Assert.assertTrue("no mask", vauth.applyMaskOnGroupRead(n));
            
            n = new DataNode(vos);
            n.getProperties().add(new NodeProperty(VOS.PROPERTY_URI_GROUPMASK, "abcd"));
            Assert.assertTrue("invalid mask", vauth.applyMaskOnGroupRead(n));
            
            n = new DataNode(vos);
            n.getProperties().add(new NodeProperty(VOS.PROPERTY_URI_GROUPMASK, "r--"));
            Assert.assertTrue("allow read", vauth.applyMaskOnGroupRead(n));
            
            n = new DataNode(vos);
            n.getProperties().add(new NodeProperty(VOS.PROPERTY_URI_GROUPMASK, "-wx"));
            Assert.assertFalse("disallow read", vauth.applyMaskOnGroupRead(n));
        } catch (Exception unexpected) {
            log.error("unexpected exception", unexpected);
            Assert.fail("unexpected exception: " + unexpected);
        }
    }
    
    @Test
    public void testWriteMask() throws Exception {
        try {
            VOSpaceAuthorizer vauth = new VOSpaceAuthorizer();
            VOSURI vos = new VOSURI(new URI("vos://cadc.nrc.ca!vospace/CADCAuthtest1"));
            Node n = null;
            
            n = new DataNode(vos);
            Assert.assertTrue("no mask", vauth.applyMaskOnGroupReadWrite(n));
            
            n = new DataNode(vos);
            n.getProperties().add(new NodeProperty(VOS.PROPERTY_URI_GROUPMASK, "abcd"));
            Assert.assertTrue("invalid mask", vauth.applyMaskOnGroupReadWrite(n));
            
            n = new DataNode(vos);
            n.getProperties().add(new NodeProperty(VOS.PROPERTY_URI_GROUPMASK, "rw-"));
            Assert.assertTrue("allow write", vauth.applyMaskOnGroupReadWrite(n));
            
            n = new DataNode(vos);
            n.getProperties().add(new NodeProperty(VOS.PROPERTY_URI_GROUPMASK, "r-x"));
            Assert.assertFalse("disallow write", vauth.applyMaskOnGroupReadWrite(n));
        } catch (Exception unexpected) {
            log.error("unexpected exception", unexpected);
            Assert.fail("unexpected exception: " + unexpected);
        }
    }


    private class ReadPermissionAction implements PrivilegedExceptionAction<Object>
    {
        private VOSpaceAuthorizer authorizer;
        private URI uri;

        ReadPermissionAction(VOSpaceAuthorizer authorizer, URI uri)
        {
            this.authorizer = authorizer;
            this.uri = uri;
        }

        public Object run() throws Exception
        {
            return authorizer.getReadPermission(uri);
        }

    }

    private class WritePermissionAction implements PrivilegedExceptionAction<Object>
    {
        private VOSpaceAuthorizer authorizer;
        private URI uri;

        WritePermissionAction(VOSpaceAuthorizer authorizer, URI uri)
        {
            this.authorizer = authorizer;
            this.uri = uri;
        }

        public Object run() throws Exception
        {
            return authorizer.getWritePermission(uri);
        }

    }


    /**
     * Return the complete name of the keys file to be created so that
     * the SignatureUtil class can find it. Do this by getting the path
     * of the class directory.
     * @return
     */
    public static String getCompleteKeysDirName()
    {
        URL classLocation =
                VOSpaceAuthorizer.class.getResource("VOSpaceAuthorizer.class");
        if (!"file".equalsIgnoreCase(classLocation.getProtocol()))
        {
            throw new
            IllegalStateException("SignatureUtil class is not stored in a file.");
        }
        final File classPath = FileUtil.getFileFromURL(classLocation).getParentFile();
        String packageName = VOSpaceAuthorizer.class.getPackage().getName();
        String packageRelPath = packageName.replace('.', File.separatorChar);

        String dir = classPath.getAbsolutePath().
                substring(0, classPath.getAbsolutePath().indexOf(packageRelPath));

        if (dir == null)
        {
            throw new RuntimeException("Cannot find the class directory");
        }
        return dir;
    }

}
