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
import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.attribute.GroupPrincipal;
import java.nio.file.attribute.UserPrincipalLookupService;

import org.apache.log4j.Logger;

/**
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
    private static final String FILE_RW = "rw-";
    private static final String DIR_RO = "r-x";
    private static final String DIR_RW = "rwx";
    
    private static final String[] CHECK_ACL_SUPPORT = new String[] {
        GETACL, "--help"
    };
    
    private final Path path;
    UserPrincipalLookupService users;
    
    public AclCommandExecutor(Path path, UserPrincipalLookupService users) { 
        this.path = path;
        this.users = users;
        BuilderOutputGrabber grabber = new BuilderOutputGrabber();
        grabber.captureOutput(CHECK_ACL_SUPPORT);
        if (grabber.getExitValue() != 0) {
            throw new UnsupportedOperationException("getfacl/setfacl not available");
        }
    }
    
    public void clearACL()  throws IOException {
        
        String[] cmd = new String[] {
            SETACL, "-b", toAbsolutePath(path)
        };
        BuilderOutputGrabber grabber = new BuilderOutputGrabber();
        grabber.captureOutput(cmd);
        if (grabber.getExitValue() != 0) {
            throw new IOException("failed to clear ACLs on " + path + ": "
                + grabber.getErrorOutput(true));
        }
    }
    
    public void setReadOnlyACL(GroupPrincipal group, boolean isDir) throws IOException {
        String perm = FILE_RO;
        if (isDir) {
            perm = DIR_RO;
        }
        setACL(group, perm);
    }
    
    public void setReadWriteACL(GroupPrincipal group, boolean isDir) throws IOException {
        String perm = FILE_RW;
        if (isDir) {
            perm = DIR_RW;
        }
        setACL(group, perm);
    }
    
    private void setACL(GroupPrincipal group, String perm) throws IOException {
        String[] cmd = new String[] {
            SETACL, "-m", "group:" + group.getName() + ":" + perm, toAbsolutePath(path)
        };
        BuilderOutputGrabber grabber = new BuilderOutputGrabber();
        grabber.captureOutput(cmd);
        if (grabber.getExitValue() != 0) {
            throw new IOException("failed to set read-only ACL on " + path + ": "
                + grabber.getErrorOutput(true));
        }
    }
    
    public GroupPrincipal getReadOnlyACL(boolean isDir) throws IOException {
        String perm = FILE_RO.substring(0, 2); // ignore execute when reading file perms
        if (isDir) {
            perm = DIR_RO;
        }
        return getACL(perm);
    }
    
    public GroupPrincipal getReadWriteACL(boolean isDir) throws IOException {
        String perm = FILE_RW.substring(0, 2); // ignore execute when reading file perms
        if (isDir) {
            perm = DIR_RW;
        }
        return getACL(perm);
    }
    
    public String getMask() throws IOException {
        String[] cmd = new String[] {
            GETACL, "--omit-header", "--skip-base", toAbsolutePath(path)
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
    
    private GroupPrincipal getACL(String perm) throws IOException {
        String[] cmd = new String[] {
            GETACL, "--omit-header", "--skip-base", toAbsolutePath(path)
        };
        BuilderOutputGrabber grabber = new BuilderOutputGrabber();
        grabber.captureOutput(cmd);
        if (grabber.getExitValue() != 0) {
            throw new IOException("failed to get ACL on " + path + ": "
                + grabber.getErrorOutput(true));
        }
        String out = grabber.getOutput(true);
        String[] lines = out.split("[\n]");
        for (String s : lines) {
            String[] tokens = s.split(":");
            if ("group".equals(tokens[0])
                    && tokens[1].length() > 0
                    && tokens[2].startsWith(perm)) {
                log.debug("getACL(" + perm + "): found " + s + " -> " + tokens[1]);
                return users.lookupPrincipalByGroupName(tokens[1]);
            }                   
            log.debug("getACL(" + perm + "): skip " + s);
        }
        log.debug("getACL(" + perm + "): found: null");
        return null;
    }
    
    private String toAbsolutePath(Path p) {
        return p.toAbsolutePath().toString();
    }
}
