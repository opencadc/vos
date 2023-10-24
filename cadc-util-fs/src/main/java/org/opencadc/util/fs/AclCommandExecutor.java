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

import ca.nrc.cadc.exec.BuilderOutputGrabber;
import ca.nrc.cadc.util.StringUtil;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.Path;
import java.nio.file.attribute.PosixFileAttributeView;
import java.nio.file.attribute.PosixFilePermission;
import java.nio.file.attribute.UserPrincipalLookupService;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;
import java.util.stream.Collectors;
import org.apache.log4j.Logger;

/**
 * Wrapper to manage directory and file ACLs with the permission concepts from
 * VOSpace: read-only and read-write.
 * 
 * @author pdowler
 */
public class AclCommandExecutor {
    private static final Logger log = Logger.getLogger(AclCommandExecutor.class);

    // these tools are from the acl package (fedora, ubuntu, ...)
    // aka https://savannah.nongnu.org/projects/acl
    private static final String GETACL = "getfacl";
    private static final String SETACL = "setfacl";
    private static final String FILE_RO = "r--";
    private static final String FILE_RX = "r-x"; // executable file: unused
    private static final String FILE_RW = "rw-";
    private static final String FILE_RWX = "rwx"; // executable file: unused
    private static final String DIR_RO = "r-x";
    private static final String DIR_RW = "rwx";
    
    private static final String[] CHECK_ACL_SUPPORT = new String[] {
        GETACL, "--help"
    };
    
    private final Path path;
    private final boolean isDir;
    private UserPrincipalLookupService users;
    
    /**
     * Constructor.
     * 
     * @param path the target path
     * @param isDir true for directory, false for normal file
     */
    public AclCommandExecutor(Path path, boolean isDir) { 
        this.path = path;
        this.isDir = isDir;
        BuilderOutputGrabber grabber = new BuilderOutputGrabber();
        grabber.captureOutput(CHECK_ACL_SUPPORT);
        if (grabber.getExitValue() != 0) {
            throw new UnsupportedOperationException("getfacl/setfacl not available");
        }
    }
    
    public void clearACL()  throws IOException {
        
        String[] cmd = new String[] {
            SETACL, "--remove-all", toAbsolutePath(path)
        };
        BuilderOutputGrabber grabber = new BuilderOutputGrabber();
        grabber.captureOutput(cmd);
        if (grabber.getExitValue() != 0) {
            throw new IOException("failed to clear ACLs on " + path + ": "
                + grabber.getErrorOutput(true));
        }
    }
    
    public void clearDefaultACL() throws IOException {
        String[] cmd = new String[] {
            SETACL, "--remove-default", toAbsolutePath(path)
        };
        BuilderOutputGrabber grabber = new BuilderOutputGrabber();
        grabber.captureOutput(cmd);
        if (grabber.getExitValue() != 0) {
            throw new IOException("failed to clear default ACLs on " + path + ": "
                + grabber.getErrorOutput(true));
        }
    }

    /**
     * Set the read and read/wite Groups for the current path using raw GID values.
     * This will cleanly set whatever group principals are provided and remove all other ACLs.
     * 
     * @param worldReadable    set the target to world-readable (r-x for dir, r-- for normal file)
     * @param readOnlyGroups   unique set of group IDs representing the ReadOnly groups
     * @param readWriteGroups  unique set of group IDs representing the ReadWrite groups
     * @throws IOException     if operation failed
     */
    public void setACL(boolean worldReadable, final Set<Integer> readOnlyGroups, final Set<Integer> readWriteGroups)
            throws IOException {
        setACL(worldReadable, readOnlyGroups, readWriteGroups, false);
    }
    
    /**
     * Set the read and read/wite Groups for the current path using raw GID values.
     * This will cleanly set whatever group principals are provided and remove all other ACLs.
     *
     * @param worldReadable    set the target to world-readable (r-x for dir, r-- for normal file)
     * @param readOnlyGroups   unique set of group IDs representing the ReadOnly groups
     * @param readWriteGroups  unique set of group IDs representing the ReadWrite groups
     * @param defaultACL       set default ACLs instead of actual ACLs
     * @throws IOException     if operation failed
     */
    public void setACL(boolean worldReadable, final Set<Integer> readOnlyGroups, 
            final Set<Integer> readWriteGroups, boolean defaultACL) throws IOException {
        if (readOnlyGroups == null || readWriteGroups == null) {
            throw new IllegalArgumentException("Null input to setACL().");
        }
        
        if (defaultACL && !isDir) {
            throw new IllegalArgumentException("only directories can have default ACLs");
        }
        
        // not settable via input args, but preserve execute bit on files (executables)
        PosixFileAttributeView pv = Files.getFileAttributeView(path, PosixFileAttributeView.class, LinkOption.NOFOLLOW_LINKS);
        Set<PosixFilePermission> perms = pv.readAttributes().permissions(); // current perms
        boolean ux = perms.contains(PosixFilePermission.OWNER_EXECUTE);
        boolean gx = perms.contains(PosixFilePermission.GROUP_EXECUTE);
        boolean ox = perms.contains(PosixFilePermission.OTHERS_EXECUTE);

        //final String uRreadOnlyPermission = isDir ? DIR_RO : (ux ? FILE_RX : FILE_RO); // unused: locked??
        final String uReadWritePermission = isDir ? DIR_RW : (ux ? FILE_RWX : FILE_RW);
        final String gReadWritePermission = isDir ? DIR_RW : (gx ? FILE_RWX : FILE_RW);
        
        final String readOnlyPermission = isDir ? DIR_RO : (ox ? FILE_RX : FILE_RO);
        final String readWritePermission = isDir ? DIR_RW : (ox ? FILE_RWX : FILE_RW);
        
        String otherPermission = worldReadable ? readOnlyPermission : "---";
        if (worldReadable && !isDir && ox) {
            otherPermission = FILE_RX;
        }
        
        final String readGroupCommandInput = readOnlyGroups.stream()
                                                                    .map(p -> "group:" + p.toString() + ":"
                                                                              + readOnlyPermission)
                                                                    .collect(Collectors.joining(","));
        final String writeGroupCommandInput = readWriteGroups.stream()
                                                                      .map(p -> "group:" + p.toString() + ":"
                                                                                + readWritePermission)
                                                                      .collect(Collectors.joining(","));

        StringBuilder sb = new StringBuilder();
        sb.append("--set=user::").append(uReadWritePermission);
        sb.append(",group::").append(gReadWritePermission); // always same as user
        sb.append(",other::").append(otherPermission);
        
        if (StringUtil.hasText(readGroupCommandInput) || StringUtil.hasText(writeGroupCommandInput)) {
            // always make group mask rwx
            String groupMask = readWritePermission;
            String groupListString = writeGroupCommandInput;
            if (StringUtil.hasText(readGroupCommandInput) && StringUtil.hasText(writeGroupCommandInput)) {
                //groupMask = readWritePermission;
                groupListString = String.join(",", readGroupCommandInput, writeGroupCommandInput);
            } else if (StringUtil.hasText(readGroupCommandInput)) {
                //groupMask = readOnlyPermission;
                groupListString = readGroupCommandInput;
            }

            // allow setfacl to recalculate mask
            //sb.append(",mask::").append(groupMask);
            sb.append(",").append(groupListString);
        }
        
        final List<String> commandList = new ArrayList<>();
        commandList.add(AclCommandExecutor.SETACL);
        commandList.add("--physical"); // do not follow symlinks
        if (defaultACL) {
            commandList.add("--default");
        }
        commandList.add(sb.toString());
        commandList.add(toAbsolutePath(path));

        final String[] commandArray = commandList.toArray(new String[0]);
        String logStr = Arrays.toString(commandArray);
        log.debug("Executing " + logStr);
        executeCommand(commandArray);
        log.debug("Executing " + logStr + ": OK");
    }
    
    /**
     * Set the read and read/wite Groups for the current path using resolved group names.  
     * This will cleanly set whatever group principals are provided and remove all other ACLs.
     * 
     * @param readOnlyGroups   unique set of group IDs representing the ReadOnly groups
     * @param readWriteGroups  unique set of group IDs representing the ReadWrite groups
     * @throws IOException     if operation failed
     * @deprecated use the GID version
     */
    @Deprecated
    public void setResolvedACL(final Set<String> readOnlyGroups, final Set<String> readWriteGroups)
            throws IOException {
        if (readOnlyGroups == null || readWriteGroups == null) {
            throw new IllegalArgumentException("Null input to setACL().");
        }

        final String readOnlyPermission = isDir ? AclCommandExecutor.DIR_RO : AclCommandExecutor.FILE_RO;
        final String readWritePermission = isDir ? AclCommandExecutor.DIR_RW : AclCommandExecutor.FILE_RW;

        final String readGroupCommandInput = readOnlyGroups.stream()
                                                                    .map(p -> "group:" + p + ":"
                                                                              + readOnlyPermission)
                                                                    .collect(Collectors.joining(","));
        final String writeGroupCommandInput = readWriteGroups.stream()
                                                                      .map(p -> "group:" + p + ":"
                                                                                + readWritePermission)
                                                                      .collect(Collectors.joining(","));

        if (StringUtil.hasText(readGroupCommandInput) || StringUtil.hasText(writeGroupCommandInput)) {
            final List<String> commandList = new ArrayList<>();
            final String groupMask;
            final String groupListString;
            if (StringUtil.hasText(readGroupCommandInput) && StringUtil.hasText(writeGroupCommandInput)) {
                groupMask = readWritePermission;
                groupListString = String.join(",", readGroupCommandInput, writeGroupCommandInput);
            } else if (StringUtil.hasText(readGroupCommandInput)) {
                groupMask = readOnlyPermission;
                groupListString = readGroupCommandInput;
            } else {
                groupMask = readWritePermission;
                groupListString = writeGroupCommandInput;
            }

            log.debug("Base Masks: \nuser::rwx\ngroup::" + groupMask);
            final String setACLInput = "--set=user::rwx" + ",group::" + groupMask + ",other::---,mask::" + groupMask
                                       + "," + groupListString;

            commandList.add(AclCommandExecutor.SETACL);
            commandList.add("--physical"); // do not follow symlinks
            commandList.add(setACLInput);
            commandList.add(toAbsolutePath(path));

            final String[] commandArray = commandList.toArray(new String[0]);
            log.debug("Executing " + Arrays.toString(commandArray));
            executeCommand(commandArray);
            log.debug("Executing " + Arrays.toString(commandArray) + ": OK");
        } else {
            log.debug("Nothing to do in setACL().");
        }
    }
    
    void executeCommand(final String[] command) throws IOException {
        final BuilderOutputGrabber grabber = new BuilderOutputGrabber();
        grabber.captureOutput(command);
        if (grabber.getExitValue() != 0) {
            throw new IOException("failed to execute " + Arrays.toString(command) + " on " + path
                                  + ": " + grabber.getErrorOutput(true));
        }
    }
    
    public Set<Integer> getReadOnlyACL() throws IOException {
        return getReadOnlyACL(false);
    }

    public Set<Integer> getReadOnlyACL(boolean defaultACL) throws IOException {
        String perm = FILE_RO.substring(0, 2); // ignore execute when reading file perms
        if (isDir) {
            perm = DIR_RO;
        }
        return getACL(perm, defaultACL, false);
    }
    
    public Set<Integer> getReadWriteACL() throws IOException {
        return getReadWriteACL(false);
    }
    
    public Set<Integer> getReadWriteACL(boolean defaultACL) throws IOException {
        String perm = FILE_RW.substring(0, 2); // ignore execute when reading file perms
        if (isDir) {
            perm = DIR_RW;
        }
        return getACL(perm, defaultACL, false);
    }

    @Deprecated
    public Set<String> getResolvedReadOnlyACL() throws IOException {
        String perm = FILE_RO.substring(0, 2); // ignore execute when reading file perms
        if (isDir) {
            perm = DIR_RO;
        }
        return getACL(perm, false, true);
    }

    @Deprecated
    public Set<String> getResolvedReadWriteACL() throws IOException {
        String perm = FILE_RW.substring(0, 2); // ignore execute when reading file perms
        if (isDir) {
            perm = DIR_RW;
        }
        return getACL(perm, false, true);
    }
    
    @Deprecated
    public String getMask() throws IOException {
        String[] cmd = new String[] {
            GETACL, "--omit-header", "--skip-base", "--physical", toAbsolutePath(path)
        };
        BuilderOutputGrabber grabber = new BuilderOutputGrabber();
        grabber.captureOutput(cmd);
        if (grabber.getExitValue() != 0) {
            throw new IOException("failed to get ACL mask on " + path + ": "
                + grabber.getErrorOutput(true));
        }
        String out = grabber.getOutput(true);
        String[] lines = out.split("[\n]");
        for (String s : lines) {
            String[] tokens = s.split(":");
            if ("mask".equals(tokens[0])) {
                log.debug("getMask(): found " + s + " -> " + tokens[2]);
                return tokens[2];
            }                   
            log.debug("getMask(): skip " + s);
        }
        log.debug("getMask(): found: null");
        return null;
    }
    
    // exec getfacl and parse output
    private Set getACL(String perm, boolean defaultACL, boolean resolve) throws IOException {
        log.debug("getACL: " + perm + " " + defaultACL + " " + resolve);
        Set aclList = new TreeSet();
        
        List<String> cmdList = new ArrayList<>();
        cmdList.add(GETACL);
        if (!resolve) {
            cmdList.add("--numeric");
        }
        cmdList.add("--omit-header");
        cmdList.add("--skip-base");
        cmdList.add("--physical"); // do not follow symlinks
        cmdList.add(toAbsolutePath(path));
        final String[] cmd = cmdList.toArray(new String[0]);
        
        BuilderOutputGrabber grabber = new BuilderOutputGrabber();
        grabber.captureOutput(cmd);
        if (grabber.getExitValue() != 0) {
            throw new IOException("failed to get ACL on " + path + ": " + grabber.getErrorOutput(true));
        }
        String out = grabber.getOutput(true);
        String[] lines = out.split("[\n]");
        for (String s : lines) {
            String[] tokens = s.split("[:#]"); // hash to split effective permissions when masked
            log.debug("raw: (" + tokens.length + ") " + s);
            String gidToken = null;
            String permToken = null;
            if (defaultACL && "default".equals(tokens[0]) && "group".equals(tokens[1])) {
                if (tokens.length >= 4 && tokens[2].length() > 0) {
                    gidToken = tokens[2];
                    permToken = tokens[3];
                }
            } else if (!defaultACL && "group".equals(tokens[0]) && tokens[1].length() > 0) {
                if (tokens.length == 3) {
                    gidToken = tokens[1];
                    permToken = tokens[2];
                } else if (tokens.length == 5) {
                    gidToken = tokens[1];
                    permToken = tokens[4]; // effective permissions due to masked
                }
            } else {
                log.debug("skip: " + s);
            }
            log.debug("found: " + gidToken + "," + permToken + " in " + s);
            if (gidToken != null && permToken != null && permToken.startsWith(perm)) {
                if (resolve) {
                    aclList.add(gidToken);
                } else {
                    aclList.add(Integer.parseInt(gidToken));
                }
            }                   
        }
        return aclList;
    }
    
    private String toAbsolutePath(Path p) {
        return p.toAbsolutePath().toString();
    }
}
