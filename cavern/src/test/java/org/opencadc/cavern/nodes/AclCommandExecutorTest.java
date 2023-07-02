/*
************************************************************************
*******************  CANADIAN ASTRONOMY DATA CENTRE  *******************
**************  CENTRE CANADIEN DE DONNÉES ASTRONOMIQUES  **************
*
*  (c) 2017.                            (c) 2017.
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

package org.opencadc.cavern.nodes;


import ca.nrc.cadc.exec.BuilderOutputGrabber;
import ca.nrc.cadc.util.Log4jInit;
import java.io.IOException;
import java.nio.file.FileSystem;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.GroupPrincipal;
import java.nio.file.attribute.PosixFilePermission;
import java.nio.file.attribute.PosixFilePermissions;
import java.nio.file.attribute.UserPrincipalLookupService;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;

/**
 *
 * @author pdowler
 */
public class AclCommandExecutorTest {
    private static final Logger log = Logger.getLogger(AclCommandExecutorTest.class);

    static {
        Log4jInit.setLevel("org.opencadc.cavern", Level.DEBUG);
    }

    static final String ROOT = System.getProperty("java.io.tmpdir") + "/cavern-tests";

    static final String GROUP = System.getProperty("user.name");

    static {
        try {
            Path root = FileSystems.getDefault().getPath(ROOT);
            if (!Files.exists(root)) {
                Files.createDirectory(root);
            }
        } catch (IOException ex) {
            throw new RuntimeException("TEST SETUP: failed to create test dir: " + ROOT, ex);
        }
    }
    
    public AclCommandExecutorTest() { 
    }
    
    // TODO: acl specific codes will be moved to a library, enable the test after
    @Ignore
    @Test
    public void testFileACL() {
        try {
            String name = "testFileACL-" + UUID.randomUUID().toString();

            FileSystem fs = FileSystems.getDefault();
            Path target = fs.getPath(ROOT, name);
            
            Set<PosixFilePermission> perms = new HashSet<>();
            perms.add(PosixFilePermission.OWNER_READ);
            perms.add(PosixFilePermission.OWNER_WRITE);
            
            Files.createFile(target, PosixFilePermissions.asFileAttribute(perms));
            
            UserPrincipalLookupService users = target.getFileSystem().getUserPrincipalLookupService();
            AclCommandExecutor acl = new AclCommandExecutor(target, users);
            GroupPrincipal group = users.lookupPrincipalByGroupName(GROUP);
            
            // RO
            acl.setReadOnlyACL(group, false);
            
            List<GroupPrincipal> roActual = acl.getReadOnlyACL(false);
            Assert.assertNotNull("read-only", roActual);
            Assert.assertTrue("read-only", roActual.contains(group));

            List<GroupPrincipal> rwActual = acl.getReadWriteACL(false);
            Assert.assertNull("null read-write", rwActual);
            
            acl.clearACL();
            roActual = acl.getReadOnlyACL(false);
            Assert.assertNull("clear read-only", roActual);
            
            // RW
            acl.setReadWriteACL(group, false);
            
            rwActual = acl.getReadWriteACL(false);
            Assert.assertNotNull("read-write", rwActual);
            Assert.assertTrue("read-write", rwActual.contains(group));
            
            roActual = acl.getReadOnlyACL(false);
            Assert.assertNull("null read-only", roActual);
            
            acl.clearACL();
            rwActual = acl.getReadWriteACL(false);
            Assert.assertNull("clear read-write", rwActual);
            
        } catch (Exception unexpected) {
            log.error("unexpected exception", unexpected);
            Assert.fail("unexpected exception: " + unexpected);
        }
    }
    
    // TODO: acl specific codes will be moved to a library, enable the test after
    @Ignore
    @Test
    public void testDirectoryACL() {
        try {
            String name = "testDirectoryACL-" + UUID.randomUUID().toString();

            FileSystem fs = FileSystems.getDefault();
            Path target = fs.getPath(ROOT, name);
            
            Set<PosixFilePermission> perms = new HashSet<>();
            perms.add(PosixFilePermission.OWNER_READ);
            perms.add(PosixFilePermission.OWNER_WRITE);
            
            Files.createDirectory(target, PosixFilePermissions.asFileAttribute(perms));
            
            UserPrincipalLookupService users = target.getFileSystem().getUserPrincipalLookupService();
            AclCommandExecutor acl = new AclCommandExecutor(target, users);
            GroupPrincipal group = users.lookupPrincipalByGroupName(GROUP);
            acl.setReadOnlyACL(group, false);

            List<GroupPrincipal> actual = acl.getReadOnlyACL(false);
            Assert.assertNotNull("read-only", actual);
            Assert.assertTrue("read-only", actual.contains(group));
            
            acl.clearACL();
            actual = acl.getReadOnlyACL(false);
            Assert.assertNull("clear read-only", actual);
            
            acl.setReadWriteACL(group, false);
            
            actual = acl.getReadWriteACL(false);
            Assert.assertNotNull("read-write", actual);
            Assert.assertTrue("read-write", actual.contains(group));
            
            acl.clearACL();
            actual = acl.getReadOnlyACL(false);
            Assert.assertNull("clear read-write", actual);
            
        } catch (Exception unexpected) {
            log.error("unexpected exception", unexpected);
            Assert.fail("unexpected exception: " + unexpected);
        }
    }

    @Test
    public void testSetFileACL() throws Exception {
        String name = "testSetFileACL-" + UUID.randomUUID();

        FileSystem fs = FileSystems.getDefault();
        Path target = fs.getPath(ROOT, name);

        Set<PosixFilePermission> perms = new HashSet<>();
        perms.add(PosixFilePermission.OWNER_READ);
        perms.add(PosixFilePermission.OWNER_WRITE);

        Files.createFile(target, PosixFilePermissions.asFileAttribute(perms));

        // Use LinkedHashSet to have a predictable order.
        final Set<GroupPrincipal> readGroupPrincipals = new LinkedHashSet<>();
        readGroupPrincipals.add(() -> "GROUPA");
        readGroupPrincipals.add(() -> "GROUPB");
        final Set<GroupPrincipal> writeGroupPrincipals = new LinkedHashSet<>();
        writeGroupPrincipals.add(() -> "GROUPW");
        UserPrincipalLookupService users = target.getFileSystem().getUserPrincipalLookupService();
        final boolean[] commandChecked = new boolean[] {false};

        final String[] expectedCommand = new String[] {
            "setfacl", "--set=user::rwx,group::rw-,other::---,mask::rw-,group:GROUPA:r--,group:GROUPB:r--,group:GROUPW:rw-",
            target.toString()
        };

        AclCommandExecutor acl = new AclCommandExecutor(target, users) {
            @Override
            void executeCommand(final String[] command) throws IOException {
                Assert.assertArrayEquals("Wrong command.", expectedCommand, command);
                commandChecked[0] = true;
            }
        };

        acl.setACL(readGroupPrincipals, writeGroupPrincipals, false);
        Assert.assertTrue("Command not checked", commandChecked[0]);
    }

    @Test
    public void testSetFileACLNoRead() throws Exception {
        String name = "testSetFileACLNoRead-" + UUID.randomUUID();

        FileSystem fs = FileSystems.getDefault();
        Path target = fs.getPath(ROOT, name);

        Set<PosixFilePermission> perms = new HashSet<>();
        perms.add(PosixFilePermission.OWNER_READ);
        perms.add(PosixFilePermission.OWNER_WRITE);

        Files.createFile(target, PosixFilePermissions.asFileAttribute(perms));

        final Set<GroupPrincipal> readGroupPrincipals = new LinkedHashSet<>();
        final Set<GroupPrincipal> writeGroupPrincipals = new LinkedHashSet<>();
        writeGroupPrincipals.add(() -> "GROUPW2");
        UserPrincipalLookupService users = target.getFileSystem().getUserPrincipalLookupService();
        final boolean[] commandChecked = new boolean[] {false};

        final String[] expectedCommand = new String[] {
                "setfacl", "--set=user::rwx,group::rw-,other::---,mask::rw-,group:GROUPW2:rw-", target.toString()
        };

        AclCommandExecutor acl = new AclCommandExecutor(target, users) {
            @Override
            void executeCommand(final String[] command) {
                Assert.assertArrayEquals("Wrong command.", expectedCommand, command);
                commandChecked[0] = true;
            }
        };

        acl.setACL(readGroupPrincipals, writeGroupPrincipals, false);
        Assert.assertTrue("Command not checked", commandChecked[0]);
    }
}
