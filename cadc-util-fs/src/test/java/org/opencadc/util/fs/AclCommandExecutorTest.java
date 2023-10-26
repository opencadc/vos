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
************************************************************************
 */

package org.opencadc.util.fs;

import ca.nrc.cadc.util.Log4jInit;
import java.io.IOException;
import java.nio.file.FileSystem;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.Path;
import java.nio.file.attribute.GroupPrincipal;
import java.nio.file.attribute.PosixFileAttributeView;
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
import org.junit.Test;

/**
 *
 * @author pdowler
 */
public class AclCommandExecutorTest {

    private static final Logger log = Logger.getLogger(AclCommandExecutorTest.class);

    static {
        Log4jInit.setLevel("org.opencadc.util.fs", Level.DEBUG);
        Log4jInit.setLevel("ca.nrc.cadc.exec", Level.DEBUG);
    }

    static final String ROOT = "build/tmp/acl-tests";

    static final String RESOLVABLE_RO_GROUP = "users";
    static final String RESOLVABLE_RW_GROUP = "adm";

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

    @Test
    public void testRawFileACL() throws Exception {
        Set<PosixFilePermission> perms = new HashSet<>();
        perms.add(PosixFilePermission.OWNER_READ);
        perms.add(PosixFilePermission.OWNER_WRITE);
        // typical on linux systems when uid==gid: the expectedCommand expects it
        perms.add(PosixFilePermission.GROUP_READ);
        perms.add(PosixFilePermission.GROUP_WRITE);

        String name = "testRawFileACL-" + UUID.randomUUID();
        FileSystem fs = FileSystems.getDefault();
        Path target = fs.getPath(ROOT, name).toAbsolutePath();
        
        Files.createFile(target, PosixFilePermissions.asFileAttribute(perms));
        
        testRawACL(target, false);
    }
    
    @Test
    public void testRawDirACL() throws Exception {
        Set<PosixFilePermission> perms = new HashSet<>();
        perms.add(PosixFilePermission.OWNER_READ);
        perms.add(PosixFilePermission.OWNER_WRITE);
        perms.add(PosixFilePermission.OWNER_EXECUTE);
        // typical on linux systems when uid==gid: the expectedCommand expects it
        perms.add(PosixFilePermission.GROUP_READ);
        perms.add(PosixFilePermission.GROUP_WRITE);
        perms.add(PosixFilePermission.GROUP_EXECUTE);

        String name = "testRawDirACL-" + UUID.randomUUID();
        FileSystem fs = FileSystems.getDefault();
        Path target = fs.getPath(ROOT, name).toAbsolutePath();
        
        Files.createDirectory(target, PosixFilePermissions.asFileAttribute(perms));
        
        testRawACL(target, true);
    }
    
    private void testRawACL(Path target, boolean isDir) throws Exception {
        Integer g1 = 123456;
        Integer g2 = 234567;
        Integer g3 = 654321;

        // Use LinkedHashSet to have a predictable order.
        final Set<Integer> readGroupPrincipals = new LinkedHashSet<>();
        readGroupPrincipals.add(g1);
        readGroupPrincipals.add(g2);
        final Set<Integer> writeGroupPrincipals = new LinkedHashSet<>();
        writeGroupPrincipals.add(g3);
        final boolean[] commandChecked = new boolean[]{false};

        final boolean worldReadable = false; // other::---
        final String[] expectedCommand = new String[4];
        expectedCommand[0] = "setfacl";
        expectedCommand[1] = "--physical";
        expectedCommand[2] = "--set=user::rw-,group::rw-,other::---,group:" 
                + g1 + ":r--,group:" + g2 + ":r--,group:" + g3 + ":rw-";
        expectedCommand[3] = target.toString();
        if (isDir) {
            expectedCommand[2] = "--set=user::rwx,group::rwx,other::---,group:" 
                    + g1 + ":r-x,group:" + g2 + ":r-x,group:" + g3 + ":rwx";
        }
        
        AclCommandExecutor acl = new AclCommandExecutor(target, isDir) {
            @Override
            void executeCommand(final String[] command) throws IOException {
                Assert.assertArrayEquals("Wrong command.", expectedCommand, command);
                commandChecked[0] = true;
            }
        };

        acl.setACL(worldReadable, readGroupPrincipals, writeGroupPrincipals);
        Assert.assertTrue("Command not checked", commandChecked[0]);

        // now actually run it for real
        acl = new AclCommandExecutor(target, isDir);
        acl.setACL(worldReadable, readGroupPrincipals, writeGroupPrincipals);

        // get ACLs and verify correct state
        Set<Integer> roActual = acl.getReadOnlyACL();
        Assert.assertNotNull(roActual);
        Assert.assertEquals(2, roActual.size());
        Assert.assertTrue(roActual.contains(g1));
        Assert.assertTrue(roActual.contains(g2));

        Set<Integer> rwActual = acl.getReadWriteACL();
        Assert.assertNotNull(rwActual);
        Assert.assertEquals(1, rwActual.size());
        Assert.assertTrue(rwActual.contains(g3));
        
        
        // get defaults
        Set<Integer> roDefault = acl.getReadOnlyACL(true);
        Assert.assertNotNull(roDefault);
        Assert.assertTrue(roDefault.isEmpty());

        Set<Integer> rwDefault = acl.getReadWriteACL(true);
        Assert.assertNotNull(rwDefault);
        Assert.assertTrue(rwDefault.isEmpty());

        if (isDir) {
            // set defaults
            acl.setACL(worldReadable, readGroupPrincipals, writeGroupPrincipals, true);
            
            roDefault = acl.getReadOnlyACL(true);
            Assert.assertNotNull(roDefault);
            Assert.assertFalse(roDefault.isEmpty());
            Assert.assertTrue(roDefault.contains(g1));
            Assert.assertTrue(roDefault.contains(g2));

            rwDefault = acl.getReadWriteACL(true);
            Assert.assertNotNull(rwDefault);
            Assert.assertFalse(rwDefault.isEmpty());
            Assert.assertTrue(rwDefault.contains(g3));
            
            acl.clearDefaultACL();
            
            roDefault = acl.getReadOnlyACL(true);
            Assert.assertNotNull(roDefault);
            Assert.assertTrue(roDefault.isEmpty());
            rwDefault = acl.getReadWriteACL(true);
            Assert.assertNotNull(rwDefault);
            Assert.assertTrue(rwDefault.isEmpty());
        } else {
            try {
                acl.setACL(worldReadable, readGroupPrincipals, writeGroupPrincipals, true);
                Assert.fail("expected IllegalArgumentException for setACL on file");
            } catch (IllegalArgumentException ex) {
                log.info("caught expected: " + ex);
            }
        }
        
        // manually set execute bit(s) on file
        Set<PosixFilePermission> perms = new HashSet<>();
        perms.add(PosixFilePermission.OWNER_EXECUTE);
        perms.add(PosixFilePermission.GROUP_EXECUTE);
        perms.add(PosixFilePermission.OTHERS_EXECUTE);
        
        PosixFileAttributeView pv = Files.getFileAttributeView(target, PosixFileAttributeView.class, LinkOption.NOFOLLOW_LINKS);
        pv.setPermissions(perms);
        
        Set<PosixFilePermission> curp = pv.readAttributes().permissions();
        Assert.assertTrue(curp.contains(PosixFilePermission.OWNER_EXECUTE));
        Assert.assertTrue(curp.contains(PosixFilePermission.GROUP_EXECUTE));
        Assert.assertTrue(curp.contains(PosixFilePermission.OTHERS_EXECUTE));
        
        // verify that applying ACLs preserves execute bit(s)
        acl.setACL(true, readGroupPrincipals, writeGroupPrincipals);
        roActual = acl.getReadOnlyACL();
        Assert.assertNotNull(roActual);
        Assert.assertEquals(2, roActual.size());
        Assert.assertTrue(roActual.contains(g1));
        Assert.assertTrue(roActual.contains(g2));

        rwActual = acl.getReadWriteACL();
        Assert.assertNotNull(rwActual);
        Assert.assertEquals(1, rwActual.size());
        Assert.assertTrue(rwActual.contains(g3));
        
        // check execute bit
        Set<PosixFilePermission> actual = pv.readAttributes().permissions();
        Assert.assertTrue(actual.contains(PosixFilePermission.OWNER_EXECUTE));
        Assert.assertTrue(actual.contains(PosixFilePermission.GROUP_EXECUTE));
        Assert.assertTrue(actual.contains(PosixFilePermission.OTHERS_EXECUTE));
    }

    @Test
    public void testRawFileACLNoRead() throws Exception {
        String name = "testRawFileACLNoRead-" + UUID.randomUUID();

        FileSystem fs = FileSystems.getDefault();
        Path target = fs.getPath(ROOT, name).toAbsolutePath();

        Set<PosixFilePermission> perms = new HashSet<>();
        perms.add(PosixFilePermission.OWNER_READ);
        perms.add(PosixFilePermission.OWNER_WRITE);

        Files.createFile(target, PosixFilePermissions.asFileAttribute(perms));

        Integer g3 = 654321;

        final Set<Integer> readGroupPrincipals = new LinkedHashSet<>();
        final Set<Integer> writeGroupPrincipals = new LinkedHashSet<>();
        writeGroupPrincipals.add(g3);
        final boolean[] commandChecked = new boolean[]{false};

        boolean worldReadable = false; // other::---
        final String[] expectedCommand = new String[]{
            "setfacl", "--physical", "--set=user::rw-,group::rw-,other::---,group:" + g3 + ":rw-", target.toString()
        };

        AclCommandExecutor acl = new AclCommandExecutor(target, false) {
            @Override
            void executeCommand(final String[] command) {
                Assert.assertArrayEquals("Wrong command.", expectedCommand, command);
                commandChecked[0] = true;
            }
        };

        acl.setACL(worldReadable, readGroupPrincipals, writeGroupPrincipals);
        Assert.assertTrue("Command not checked", commandChecked[0]);

        // TODO: get ACLs and verify correct state
    }
    
    @Test
    public void testResolvedFileACL() throws Exception {
        Set<PosixFilePermission> perms = new HashSet<>();
        perms.add(PosixFilePermission.OWNER_READ);
        perms.add(PosixFilePermission.OWNER_WRITE);
        // typical on linux systems when uid==gid: the expectedCommand expects it
        perms.add(PosixFilePermission.GROUP_READ);
        perms.add(PosixFilePermission.GROUP_WRITE);

        String name = "testResolvedFileACL-" + UUID.randomUUID();
        FileSystem fs = FileSystems.getDefault();
        Path target = fs.getPath(ROOT, name).toAbsolutePath();
        
        Files.createFile(target, PosixFilePermissions.asFileAttribute(perms));
        
        testResolvedACL(target, false);
    }
    
    @Test
    public void testResolvedDirACL() throws Exception {
        Set<PosixFilePermission> perms = new HashSet<>();
        perms.add(PosixFilePermission.OWNER_READ);
        perms.add(PosixFilePermission.OWNER_WRITE);
        perms.add(PosixFilePermission.OWNER_EXECUTE);
        // typical on linux systems when uid==gid: the expectedCommand expects it
        perms.add(PosixFilePermission.GROUP_READ);
        perms.add(PosixFilePermission.GROUP_WRITE);
        perms.add(PosixFilePermission.GROUP_EXECUTE);

        String name = "testRawDirACL-" + UUID.randomUUID();
        FileSystem fs = FileSystems.getDefault();
        Path target = fs.getPath(ROOT, name).toAbsolutePath();
        
        Files.createDirectory(target, PosixFilePermissions.asFileAttribute(perms));
        
        testResolvedACL(target, true);
    }
    
    private void testResolvedACL(Path target, boolean isDir) throws Exception {
        Set<String> readOnlyGroups = new HashSet<>();
        Set<String> readWriteGroups = new HashSet<>();

        // Use LinkedHashSet to have a predictable order.
        readOnlyGroups.add(RESOLVABLE_RO_GROUP);
        final Set<GroupPrincipal> writeGroupPrincipals = new LinkedHashSet<>();
        readWriteGroups.add(RESOLVABLE_RW_GROUP);
        final boolean[] commandChecked = new boolean[]{false};

        final String[] expectedCommand = new String[4];
        expectedCommand[0] = "setfacl";
        expectedCommand[1] = "--physical";
        expectedCommand[2] = "--set=user::rwx,group::rw-,other::---,mask::rw-,group:" 
                + RESOLVABLE_RO_GROUP + ":r--,group:" + RESOLVABLE_RW_GROUP + ":rw-";
        expectedCommand[3] = target.toString();
        if (isDir) {
            expectedCommand[2] = "--set=user::rwx,group::rwx,other::---,mask::rwx,group:" 
                    + RESOLVABLE_RO_GROUP + ":r-x,group:" + RESOLVABLE_RW_GROUP + ":rwx";
        }
        
        AclCommandExecutor acl = new AclCommandExecutor(target, isDir) {
            @Override
            void executeCommand(final String[] command) throws IOException {
                Assert.assertArrayEquals("Wrong command.", expectedCommand, command);
                commandChecked[0] = true;
            }
        };

        acl.setResolvedACL(readOnlyGroups, readWriteGroups);
        Assert.assertTrue("Command not checked", commandChecked[0]);

        // now actually run it for real
        acl = new AclCommandExecutor(target, isDir);
        acl.setResolvedACL(readOnlyGroups, readWriteGroups);

        // get ACLs and verify correct state
        Set<String> roActual = acl.getResolvedReadOnlyACL();
        Assert.assertNotNull(roActual);
        Assert.assertEquals(1, roActual.size());
        Assert.assertTrue(roActual.contains(RESOLVABLE_RO_GROUP));

        Set<String> rwActual = acl.getResolvedReadWriteACL();
        Assert.assertNotNull(rwActual);
        Assert.assertEquals(1, rwActual.size());
        Assert.assertTrue(rwActual.contains(RESOLVABLE_RW_GROUP));
    }
}
