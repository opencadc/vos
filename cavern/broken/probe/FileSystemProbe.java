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

package org.opencadc.cavern.probe;

import ca.nrc.cadc.auth.PosixPrincipal;
import ca.nrc.cadc.reg.Standards;
import ca.nrc.cadc.reg.client.LocalAuthority;
import ca.nrc.cadc.util.InvalidConfigException;
import ca.nrc.cadc.vos.ContainerNode;
import ca.nrc.cadc.vos.DataNode;
import ca.nrc.cadc.vos.LinkNode;
import ca.nrc.cadc.vos.Node;
import ca.nrc.cadc.vos.NodeProperty;
import ca.nrc.cadc.vos.VOS;
import ca.nrc.cadc.vos.VOSURI;
import ca.nrc.cadc.vos.server.NodeID;
import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.TreeSet;
import java.util.UUID;
import java.util.concurrent.Callable;
import javax.security.auth.Subject;
import org.apache.log4j.Logger;
import org.opencadc.cavern.nodes.NodeUtil;

/**
 *
 * @author pdowler
 */
public class FileSystemProbe implements Callable<Boolean> {
    private static final Logger log = Logger.getLogger(FileSystemProbe.class);

    private final Path root;
    private final String vosBaseURI;
    private final NodeUtil nodeUtil;
    
    private final PosixPrincipal ownerPrincipal;
    private final NodeID owner;
    private final NodeID linkTargetOwner;
    private final Integer gid = 123456789;
    private final URI groupURI;
    

    public FileSystemProbe(File baseDir, String vosBaseURI, String owner, String linkTargetOwner, String group) 
            throws InvalidConfigException {
        this.root = FileSystems.getDefault().getPath(baseDir.getAbsolutePath());
        this.vosBaseURI = vosBaseURI;
        this.nodeUtil = new NodeUtil(root);
        
        this.ownerPrincipal = new PosixPrincipal(8675309); // this is cadc-tomcat but could be any uid
        Subject osub = new Subject(false, new TreeSet<>(), new TreeSet<>(), new TreeSet<>());
        osub.getPrincipals().add(ownerPrincipal);
        this.owner = new NodeID(null, osub, ownerPrincipal);
        
        this.linkTargetOwner = this.owner;
        
        LocalAuthority loc = new LocalAuthority();
        URI gms = loc.getServiceURI(Standards.GMS_SEARCH_10.toASCIIString());
        if (gms != null) {
            this.groupURI = URI.create(gms.toASCIIString() + "?cavern-probe-test");
        } else {
            throw new InvalidConfigException("CONFIG:  local " + Standards.GMS_SEARCH_10);
        }
        for (String vs : root.getFileSystem().supportedFileAttributeViews()) {
            log.debug("FileAttributeView supported: " + vs);
        }
    }
    
    @Override
    public Boolean call() {
        log.info("START");

        boolean success = true;

        log.info("testing create file...");
        success = doCreateFile(false) & success;
        
        log.info("testing create directory...");
        success = doCreateDir(false) & success;

        log.info("testing create directory + child file...");
        success = doCreateFile(true) & success;
        
        log.info("testing create directory + child directory...");
        success = doCreateDir(true) & success;
        
        log.info("testing create symlink...");
        success = doCreateSymlink() & success;
        
        log.info("testing set file attribute...");
        success = doSetAttribute() & success;

        log.info("testing rename file...");
        success = doRenameFile() & success;
        
        log.info("testing move file...");
        success = doMoveFile() & success;
        
        log.info("testing copy file...");
        success = doCopyFile() & success;

        log.info("testing rename directory...");
        success = doRenameDir()& success;
        
        log.info("testing copy directory...");
        success = doCopyDir() & success;

        log.info("testing move directory...");
        success = doMoveDir() & success;

        log.info("testing copy symlink...");
        success = doCopySymlink() & success;

        log.info("testing move symlink...");
        success = doMoveSymlink() & success;

        log.info("checking ACL installation...");
        doCheckACL();

        log.info("END");

        return new Boolean(success);
    }

    public boolean doCheckACL() {
        try {
            if (groupURI == null) {
                log.warn("doCheckACL: groupURI not set -- skipping tests");
                return true;
            }
            
            String fail = " [FAIL]";
            String ok = " [OK]";
            String name = UUID.randomUUID().toString();
            VOSURI cnURI = new VOSURI(URI.create(vosBaseURI + "/" + name));
            ContainerNode cn1 = new ContainerNode(cnURI);
            cn1.getProperties().add(new NodeProperty(VOS.PROPERTY_URI_GROUPREAD, groupURI.toASCIIString()));
            NodeUtil.setOwner(cn1, owner);
            log.info("[acl] " +  cn1.getClass().getSimpleName() + " " + cn1.getUri() + " " + owner);
            Path pth = nodeUtil.create(cn1);
            if (pth == null || !Files.exists(pth)) {
                log.error("[acl] failed to create directory in fs: " + root + "/" + cnURI.getPath() + fail);
                return false;
            }

            VOSURI dnURI = new VOSURI(URI.create(cnURI.getURI().toASCIIString() + "/check-acl-test"));
            DataNode dn1 = new DataNode(dnURI);
            NodeUtil.setOwner(dn1, owner);
            dn1.getProperties().add(new NodeProperty(VOS.PROPERTY_URI_GROUPREAD, groupURI.toASCIIString()));
            log.info("[acl] " +  dn1.getClass().getSimpleName() + " " + dn1.getUri() + " " + owner);
            Path fpth = nodeUtil.create(dn1);
            if (fpth == null || !Files.exists(fpth)) {
                log.error("[acl] failed to create file in fs: " + root + "/" + dnURI.getPath() + fail);
                return false;
            }
            
            // extract and check props
            Node cn2 = nodeUtil.get(cnURI);
            if (cn2 == null) {
                log.error("[acl] failed to get ContainerNode: " + cnURI + " from " + root + fail);
                return false;
            }
            NodeProperty cnrg = cn2.findProperty(VOS.PROPERTY_URI_GROUPREAD);
            if (cnrg == null) {
                log.error("[acl] " + VOS.PROPERTY_URI_GROUPREAD + " not found: " + cnURI);
                return false;
            }
            if (groupURI.toASCIIString().equals(cnrg.getPropertyValue())) {
                log.info("[acl] found: " + VOS.PROPERTY_URI_GROUPREAD + " = " + cnrg.getPropertyValue() + ok);
            } else {
                log.error("[acl] incorrect permission: " + VOS.PROPERTY_URI_GROUPREAD + " = " + cnrg.getPropertyValue() + fail);
                return false;
            }
            
            Node dn2 = nodeUtil.get(dnURI);
            if (dn2 == null) {
                log.error("[acl] failed to get DataNode: " + dnURI + " from " + root + fail);
                return false;
            }
            NodeProperty dnrg = dn2.findProperty(VOS.PROPERTY_URI_GROUPREAD);
            if (dnrg == null) {
                log.error("[acl] " + VOS.PROPERTY_URI_GROUPREAD + " not found: " + dnURI);
                return false;
            }
            if (groupURI.toASCIIString().equals(dnrg.getPropertyValue())) {
                log.info("[acl] found: " + VOS.PROPERTY_URI_GROUPREAD + " = " + dnrg.getPropertyValue() + ok);
            } else {
                log.error("[acl] incorrect permission: " + VOS.PROPERTY_URI_GROUPREAD + " = " + dnrg.getPropertyValue() + fail);
                return false;
            }
            
            return true;
        } catch (Exception ex) {
            log.error("FAIL", ex);
            return false;
        }
    }
    
    public boolean doCreateDir(boolean depth) {
        try {
            final StringBuilder sb = new StringBuilder();
            int num = 0;
            VOSURI parent = new VOSURI(URI.create(vosBaseURI + "/"));
            String fail = " [FAIL]";
            String name = UUID.randomUUID().toString();
            VOSURI uri = new VOSURI(URI.create(parent.getURI().toASCIIString() + "/" + name));
            
            if (depth) {
                // make a parent directory
                ContainerNode n1 = new ContainerNode(uri);
                NodeUtil.setOwner(n1, owner);
                log.info("[dir] " +  n1.getClass().getSimpleName() + " " + n1.getUri() + " " + owner);
                Path pth = nodeUtil.create(n1);
                if (pth == null || !Files.exists(pth)) {
                    log.error("[dir] failed to create directory in fs: " + root + "/" + uri.getPath() + fail);
                    return false;
                }
                uri = new VOSURI(URI.create(uri.getURI().toASCIIString() + "/create-dir-test"));
            }
            
            ContainerNode n1 = new ContainerNode(uri);
            NodeUtil.setOwner(n1, owner);
            log.info("[dir] " +  n1.getClass().getSimpleName() + " " + n1.getUri() + " " + owner);
            
            Path pth = nodeUtil.create(n1);
            if (pth == null || !Files.exists(pth)) {
                log.error("[dir] failed to create directory in fs: " + root + uri.getPath() + fail);
                return false;
            }

            Node n2 = nodeUtil.get(uri);
            if (n2 == null) {
                log.error("[dir] failed to get ContainerNode: " + uri + " from " + uri.getParentURI().getPath() + fail);
                return false;
            }


            if (!n1.getClass().equals(n2.getClass())) {
                log.error("[dir] failed to restore node type: " + n1.getClass().getSimpleName()
                        + " != " + n2.getClass().getSimpleName());
                num++;
            }
            if (!n1.getName().equals(n2.getName())) {
                log.error("[dir] failed to restore node name: " + n1.getName() + " != " + n2.getName());
                num++;
            }
            PosixPrincipal owner2 = NodeUtil.getOwner(n2);
            if (owner == null || !owner.equals(owner2)) {
                log.error("[dir] failed to restore node owner: " + owner + " != " + owner2);
                num++;
            }

            sb.append("[dir] ").append(n2.getClass().getSimpleName()).append(" ").append(n2.getUri());
            sb.append(" -> ");
            sb.append(pth).append(" ").append(owner2);

            if (num == 0) {
                sb.append(" [OK]");
                log.info(sb.toString());
                return true;
            } else {
                sb.append(fail);
                log.error(sb.toString());
                return false;
            }
        } catch (IOException | InterruptedException ex) {
            log.error("FAIL", ex);
            return false;
        }
    }

    public boolean doCreateFile(boolean depth) {
        try {
            String fail = " [FAIL]";
            String name = UUID.randomUUID().toString();
            VOSURI uri = new VOSURI(URI.create(vosBaseURI + "/" + name));
            
            if (depth) {
                // make a parent directory
                ContainerNode n1 = new ContainerNode(uri);
                NodeUtil.setOwner(n1, owner);
                log.info("[dir] " +  n1.getClass().getSimpleName() + " " + n1.getUri() + " " + owner);
                Path pth = nodeUtil.create(n1);
                if (pth == null || !Files.exists(pth)) {
                    log.error("[dir] failed to create directory in fs: " + root + "/" + uri.getPath() + fail);
                    return false;
                }
                
                uri = new VOSURI(URI.create(uri.getURI().toASCIIString() + "/create-file-test"));
            }

            DataNode n1 = new DataNode(uri);
            NodeUtil.setOwner(n1, owner);
            log.info("[file] " +  n1.getClass().getSimpleName() + " " + n1.getUri() + " " + owner);
            
            Path pth = nodeUtil.create(n1);
            if (pth == null || !Files.exists(pth)) {
                log.error("[file] failed to create file in fs: " + root + uri.getPath() + fail);
                return false;
            }

            Node n2 = nodeUtil.get(uri);
            if (n2 == null) {
                log.error("[file] failed to get DataNode: " + uri + " from " + root + fail);
                return false;
            }

            int num = 0;
            if (!n1.getClass().equals(n2.getClass())) {
                log.error("[file] failed to restore node type: " + n1.getClass().getSimpleName()
                        + " != " + n2.getClass().getSimpleName());
                num++;
            }
            if (!n1.getName().equals(n2.getName())) {
                log.error("[file] failed to restore node name: " + n1.getName() + " != " + n2.getName());
                num++;
            }
            PosixPrincipal owner2 = NodeUtil.getOwner(n2);
            if (owner == null || !owner.equals(owner2)) {
                log.error("[file] failed to restore node owner: " + owner + " != " + owner2);
                num++;
            }

            StringBuilder sb = new StringBuilder();
            sb.append("[file] ").append(n2.getClass().getSimpleName()).append(" ").append(n2.getUri()).append(" -> ").append(pth).append(" ").append(owner2);
            if (num == 0) {
                sb.append(" [OK]");
                log.info(sb.toString());
                return true;
            } else {
                sb.append(" [FAIL]");
                log.error(sb.toString());
                return false;
            }
        } catch (IOException | InterruptedException ex) {
            log.error("FAIL", ex);
            return false;
        }
    }
    
    public boolean doCreateSymlink() {
        try {
            String name = UUID.randomUUID().toString();
            VOSURI turi = new VOSURI(URI.create(vosBaseURI + "/" + name));
            VOSURI uri = new VOSURI(URI.create(vosBaseURI + "/link-to-" + name));

            Node tn = new DataNode(turi);
            NodeUtil.setOwner(tn, linkTargetOwner);
            log.info("[symlink] " +  tn.getClass().getSimpleName() + " " + tn.getUri() + " " + owner);

            LinkNode n1 = new LinkNode(uri, turi.getURI());
            NodeUtil.setOwner(n1, owner);
            log.info("[symlink] " +  n1.getClass().getSimpleName() + " " + n1.getUri() + " " + owner);

            String fail = " [FAIL]";
            Path tp = nodeUtil.create(tn);
            if (tp == null || !Files.exists(tp)) {
                log.error("[symlink] failed to create file in fs: " + root + "/" + name + fail);
                return false;
            }

            Path pth = nodeUtil.create(n1);
            if (pth == null || !Files.exists(pth)) {
                log.error("[symlink] failed to create symlink in fs: " + root + "/" + name + fail);
                return false;
            }

            Node n2 = nodeUtil.get(uri);

            if (n2 == null) {
                log.error("[symlink] failed to get LinkNode: " + uri + " from " + root + fail);
                return false;
            }

            int num = 0;
            if (!n1.getClass().equals(n2.getClass())) {
                log.error("[symlink] failed to restore node type: " + n1.getClass().getSimpleName()
                        + " != " + n2.getClass().getSimpleName());
                num++;
            }
            if (!n1.getName().equals(n2.getName())) {
                log.error("[symlink] failed to restore node name: " + n1.getName() + " != " + n2.getName());
                num++;
            }
            PosixPrincipal owner2 = NodeUtil.getOwner(n2);
            if (owner == null || !owner.equals(owner2)) {
                log.error("[symlink] failed to restore node owner: " + owner + " != " + owner2);
                num++;
            }

            // LinkNode-specific checks
            LinkNode ln2 = (LinkNode) n2;
            if (!n1.getTarget().equals(ln2.getTarget())) {
                log.error("[symlink] failed to restore symlink target: " + n1.getTarget() + " != " + ln2.getTarget());
                num++;
            }

            // verify that the target node was not modified
            Node tn2 = nodeUtil.get(turi);
            if (tn2 == null) {
                log.error("[symlink] failed to get target node: " + turi + " from " + root);
                num++;
            } else {
                if (!tn.getClass().equals(tn2.getClass())) {
                    log.error("[symlink] target node type was modified by symlink: " + tn.getClass().getSimpleName()
                            + " != " + tn2.getClass().getSimpleName());
                    num++;
                }
                if (!tn.getName().equals(tn2.getName())) {
                    log.error("[symlink] target node name was modified by symlink: " + tn.getName() + " != " + tn2.getName());
                    num++;
                }
                PosixPrincipal towner2 = NodeUtil.getOwner(tn2);
                if (towner2 == null || !linkTargetOwner.equals(towner2)) {
                    log.error("[symlink] target node owner was modified by symlink: " + linkTargetOwner + " != " + owner2);
                    num++;
                }
            }

            StringBuilder sb = new StringBuilder();
            sb.append("[symlink] ").append(n2.getClass().getSimpleName()).append(" ").append(n2.getUri()).append(" -> ").append(pth).append(" ").append(owner2);
            if (num == 0) {
                sb.append(" [OK]");
                log.info(sb.toString());
                return true;
            } else {
                sb.append(" [FAIL]");
                log.error(sb.toString());
                return false;
            }
        } catch (IOException | InterruptedException ex) {
            log.error("FAIL", ex);
            return false;
        }
    }

    public boolean doSetAttribute() {
        try {
            String name = UUID.randomUUID().toString();
            VOSURI uri = new VOSURI(URI.create(vosBaseURI + "/" + name));
            String origMD5 = "74808746f32f28650559885297f76efa";
            
            DataNode n1 = new DataNode(uri);
            NodeUtil.setOwner(n1, owner);
            log.info("[file] " +  n1.getClass().getSimpleName() + " " + n1.getUri() + " " + owner);
            n1.getProperties().add(new NodeProperty(VOS.PROPERTY_URI_CONTENTMD5, origMD5));

            String fail = " [FAIL]";
            Path pth = nodeUtil.create(n1);
            if (pth == null || !Files.exists(pth)) {
                log.error("[file] failed to create file in fs: " + root + "/" + name + fail);
                return false;
            }

            Node n2 = nodeUtil.get(uri);
            if (n2 == null) {
                log.error("[file] failed to get DataNode: " + uri + " from " + root + fail);
                return false;
            }

            int num = 0;
            if (!n1.getClass().equals(n2.getClass())) {
                log.error("[file] failed to restore node type: " + n1.getClass().getSimpleName()
                        + " != " + n2.getClass().getSimpleName());
                num++;
            }
            if (!n1.getName().equals(n2.getName())) {
                log.error("[file] failed to restore node name: " + n1.getName() + " != " + n2.getName());
                num++;
            }
            PosixPrincipal owner2 = NodeUtil.getOwner(n2);
            if (owner == null || !owner.equals(owner2)) {
                log.error("[file] failed to restore node owner: " + owner + " != " + owner2);
                num++;
            }
            String md5 = n2.getPropertyValue(VOS.PROPERTY_URI_CONTENTMD5);
            if (md5 == null || !origMD5.equals(md5)) {
                log.error("[file] failed to restore node property: " + origMD5 + " != " + md5);
                num++;
            }
            
            StringBuilder sb = new StringBuilder();
            sb.append("[file] ").append(n2.getClass().getSimpleName()).append(" ").append(n2.getUri()).append(" -> ").append(pth).append(" ").append(owner2);
            if (num == 0) {
                sb.append(" [OK]");
                log.info(sb.toString());
                return true;
            } else {
                sb.append(" [FAIL]");
                log.error(sb.toString());
                return false;
            }
        } catch (IOException | InterruptedException ex) {
            log.error("FAIL", ex);
            return false;
        }
    }

    public boolean doCopyFile() {
        try {
            String name1 = UUID.randomUUID().toString();
            String name2 = UUID.randomUUID().toString();
            String name3 = UUID.randomUUID().toString();
            VOSURI cn1uri = new VOSURI(URI.create(vosBaseURI + "/" + name1));
            VOSURI cn2uri = new VOSURI(URI.create(vosBaseURI + "/" + name2));
            VOSURI dnuri = new VOSURI(URI.create(vosBaseURI + "/" + name1 + "/" + name3));

            ContainerNode srcDir = new ContainerNode(cn1uri);
            ContainerNode destDir = new ContainerNode(cn2uri);
            DataNode dn = new DataNode(dnuri);

            NodeUtil.setOwner(srcDir, owner);
            NodeUtil.setOwner(destDir, owner);
            NodeUtil.setOwner(dn, owner);

            Path pth = null;
            String fail = " [FAIL]";

            log.info("[copyfile setup] " +  srcDir.getClass().getSimpleName() + " " + srcDir.getUri() + " " + owner);
            pth = nodeUtil.create(srcDir);
            if (pth == null || !Files.isDirectory(pth)) {
                log.error("[copyfile setup] failed to create directory in fs: " + root + "/" + srcDir.getUri() + fail);
                return false;
            }

            log.info("[copyfile setup] " +  destDir.getClass().getSimpleName() + " " + destDir.getUri() + " " + owner);
            pth = nodeUtil.create(destDir);
            if (pth == null || !Files.isDirectory(pth)) {
                log.error("[copyfile setup] failed to create directory in fs: " + root + "/" + destDir.getUri() + fail);
                return false;
            }

            log.info("[copyfile setup] " +  dn.getClass().getSimpleName() + " " + dn.getUri() + " " + owner);
            pth = nodeUtil.create(dn);
            if (pth == null || !Files.exists(pth)) {
                log.error("[copyfile setup] failed to create file in sub-directory: " + root + "/" + dn.getUri() + fail);
                return false;
            }

            int num = 0;
            nodeUtil.copy(dn.getUri(), destDir.getUri(), ownerPrincipal);
            VOSURI expected = new VOSURI(URI.create(vosBaseURI + "/" + name2 + "/" + name3));
            Node copied = nodeUtil.get(expected);
            if (copied == null || !(copied instanceof DataNode)) {
                log.error("[copyfile action] copy+get failed to retrieve copied node, copied: " + copied.getClass().getSimpleName()
                        + ", " + copied);
                num++;
            }

            Node orig = nodeUtil.get(dn.getUri());
            if (orig == null || !(orig instanceof DataNode)) {
                log.error("[copyfile action] failed to retrieve original node, original: " + orig.getClass().getSimpleName()
                        + ", " + orig);
                num++;
            }

            StringBuilder sb = new StringBuilder();
            if (num == 0) {
                sb.append(" [OK]");
                log.info(sb.toString());
                return true;
            } else {
                sb.append(" [FAIL]");
                log.error(sb.toString());
                return false;
            }
        } catch (IOException | InterruptedException ex) {
            log.error("FAIL", ex);
            return false;
        }
    }

    public boolean doMoveFile() {
        try {
            String name1 = UUID.randomUUID().toString();
            String name2 = UUID.randomUUID().toString();
            String name3 = UUID.randomUUID().toString();
            VOSURI cn1uri = new VOSURI(URI.create(vosBaseURI + "/" + name1));
            VOSURI cn2uri = new VOSURI(URI.create(vosBaseURI + "/" + name2));
            VOSURI dnuri = new VOSURI(URI.create(vosBaseURI + "/" + name1 + "/" + name3));

            ContainerNode srcDir = new ContainerNode(cn1uri);
            ContainerNode destDir = new ContainerNode(cn2uri);
            DataNode dn = new DataNode(dnuri);

            NodeUtil.setOwner(srcDir, owner);
            NodeUtil.setOwner(destDir, owner);
            NodeUtil.setOwner(dn, owner);

            Path pth = null;
            String fail = " [FAIL]";

            log.info("[movefile setup] " +  srcDir.getClass().getSimpleName() + " " + srcDir.getUri() + " " + owner);
            pth = nodeUtil.create(srcDir);
            if (pth == null || !Files.isDirectory(pth)) {
                log.error("[movefile setup] failed to create directory in fs: " + root + "/" + srcDir.getUri() + fail);
                return false;
            }

            log.info("[movefile setup] " +  destDir.getClass().getSimpleName() + " " + destDir.getUri() + " " + owner);
            pth = nodeUtil.create(destDir);
            if (pth == null || !Files.isDirectory(pth)) {
                log.error("[movefile setup] failed to create directory in fs: " + root + "/" + destDir.getUri() + fail);
                return false;
            }

            log.info("[movefile setup] " +  dn.getClass().getSimpleName() + " " + dn.getUri() + " " + owner);
            pth = nodeUtil.create(dn);
            if (pth == null || !Files.exists(pth)) {
                log.error("[movefile setup] failed to create file in fs: " + root + "/" + dn.getUri() + fail);
                return false;
            }

            int num = 0;
            nodeUtil.move(dn.getUri(), destDir.getUri(), dn.getName(), ownerPrincipal);
            VOSURI expected = new VOSURI(URI.create(vosBaseURI + "/" + name2 + "/" + name3));
            Node moved = nodeUtil.get(expected);
            if (moved == null || !(moved instanceof DataNode)) {
                log.error("[movefile action] move+get failed to retrieve moved node, moved: " + moved.getClass().getSimpleName()
                        + ", " + moved);
                num++;
            }

            StringBuilder sb = new StringBuilder();
            if (num == 0) {
                sb.append(" [OK]");
                log.info(sb.toString());
                return true;
            } else {
                sb.append(" [FAIL]");
                log.error(sb.toString());
                return false;
            }
        } catch (IOException | InterruptedException ex) {
            log.error("FAIL", ex);
            return false;
        }
    }
    
    public boolean doRenameFile() {
        try {
            String name1 = UUID.randomUUID().toString();
            String name2 = UUID.randomUUID().toString();
            VOSURI dn1uri = new VOSURI(URI.create(vosBaseURI + "/" + name1));
            VOSURI dn2uri = new VOSURI(URI.create(vosBaseURI + "/" + name2));

            DataNode src = new DataNode(dn1uri);
            DataNode dest = new DataNode(dn2uri);

            NodeUtil.setOwner(src, owner);
            NodeUtil.setOwner(dest, owner);

            Path pth = null;
            String fail = " [FAIL]";

            log.info("[renameFile] " +  src.getClass().getSimpleName() + " " + src.getUri() + " " + owner);
            pth = nodeUtil.create(src);
            if (pth == null || !Files.isRegularFile(pth)) {
                log.error("[renameFile] failed to create file in fs: " + root + "/" + src.getUri() + fail);
                return false;
            }

            int num = 0;
            nodeUtil.move(src.getUri(), dest.getUri().getParentURI(), dest.getName(), ownerPrincipal);
            VOSURI expected = new VOSURI(URI.create(vosBaseURI + "/" + name2));
            Node moved = nodeUtil.get(expected);
            if (moved == null || !(moved instanceof DataNode)) {
                log.error("[renameFile] failed to retrieve moved node, moved: " + moved.getClass().getSimpleName()
                        + ", " + moved);
                num++;
            }

            StringBuilder sb = new StringBuilder();
            if (num == 0) {
                sb.append(" [OK]");
                log.info(sb.toString());
                return true;
            } else {
                sb.append(" [FAIL]");
                log.error(sb.toString());
                return false;
            }
        } catch (IOException | InterruptedException ex) {
            log.error("FAIL", ex);
            return false;
        }
    }

    public boolean doCopyDir() {
        try {
            String name1 = UUID.randomUUID().toString();
            String name2 = UUID.randomUUID().toString();
            String name3 = UUID.randomUUID().toString();
            VOSURI cn1uri = new VOSURI(URI.create(vosBaseURI + "/" + name1));
            VOSURI cn2uri = new VOSURI(URI.create(vosBaseURI + "/" + name2));
            VOSURI dnuri = new VOSURI(URI.create(vosBaseURI + "/" + name1 + "/" + name3));

            ContainerNode srcDir = new ContainerNode(cn1uri);
            ContainerNode destDir = new ContainerNode(cn2uri);
            DataNode dn = new DataNode(dnuri);

            NodeUtil.setOwner(srcDir, owner);
            NodeUtil.setOwner(destDir, owner);
            NodeUtil.setOwner(dn, owner);

            Path pth = null;
            String fail = " [FAIL]";

            log.info("[copydir setup] " +  srcDir.getClass().getSimpleName() + " " + srcDir.getUri() + " " + owner);
            pth = nodeUtil.create(srcDir);
            if (pth == null || !Files.isDirectory(pth)) {
                log.error("[copydir setup] failed to create directory in fs: " + root + "/" + srcDir.getUri() + fail);
                return false;
            }

            log.info("[copydir setup] " +  destDir.getClass().getSimpleName() + " " + destDir.getUri() + " " + owner);
            pth = nodeUtil.create(destDir);
            if (pth == null || !Files.isDirectory(pth)) {
                log.error("[copydir setup] failed to create directory in fs: " + root + "/" + destDir.getUri() + fail);
                return false;
            }

            log.info("[copydir setup] " +  dn.getClass().getSimpleName() + " " + dn.getUri() + " " + owner);
            pth = nodeUtil.create(dn);
            if (pth == null || !Files.exists(pth)) {
                log.error("[copydir setup] failed to create file in fs: " + root + "/" + dn.getUri() + fail);
                return false;
            }

            int num = 0;
            nodeUtil.copy(srcDir.getUri(), destDir.getUri(), ownerPrincipal);
            VOSURI expected = new VOSURI(URI.create(vosBaseURI + "/" + name2 + "/" + name1 + "/" + name3));
            Node copied = nodeUtil.get(expected);
            if (copied == null || !(copied instanceof DataNode)) {
                log.error("[copydir action] copy+get failed to retrieve copied node, copied: " + copied.getClass().getSimpleName()
                        + ", " + copied);
                num++;
            }

            Node orig = nodeUtil.get(dn.getUri());
            if (orig == null || !(orig instanceof DataNode)) {
                log.error("[copydir action] failed to retrieve original node, original: " + orig.getClass().getSimpleName()
                        + ", " + orig);
                num++;
            }

            StringBuilder sb = new StringBuilder();
            if (num == 0) {
                sb.append(" [OK]");
                log.info(sb.toString());
                return true;
            } else {
                sb.append(" [FAIL]");
                log.error(sb.toString());
                return false;
            }
        } catch (IOException | InterruptedException ex) {
            log.error("FAIL", ex);
            return false;
        }
    }

    public boolean doMoveDir() {
        try {
            String name1 = UUID.randomUUID().toString();
            String name2 = UUID.randomUUID().toString();
            String name3 = UUID.randomUUID().toString();
            VOSURI cn1uri = new VOSURI(URI.create(vosBaseURI + "/" + name1));
            VOSURI cn2uri = new VOSURI(URI.create(vosBaseURI + "/" + name2));
            VOSURI dnuri = new VOSURI(URI.create(vosBaseURI + "/" + name1 + "/" + name3));

            ContainerNode srcDir = new ContainerNode(cn1uri);
            ContainerNode destDir = new ContainerNode(cn2uri);
            DataNode dn = new DataNode(dnuri);

            NodeUtil.setOwner(srcDir, owner);
            NodeUtil.setOwner(destDir, owner);
            NodeUtil.setOwner(dn, owner);

            Path pth = null;
            String fail = " [FAIL]";

            log.info("[movedir setup] " +  srcDir.getClass().getSimpleName() + " " + srcDir.getUri() + " " + owner);
            pth = nodeUtil.create(srcDir);
            if (pth == null || !Files.isDirectory(pth)) {
                log.error("[movedir setup] failed to create directory in fs: " + root + "/" + srcDir.getUri() + fail);
                return false;
            }

            log.info("[movedir setup] " +  destDir.getClass().getSimpleName() + " " + destDir.getUri() + " " + owner);
            pth = nodeUtil.create(destDir);
            if (pth == null || !Files.isDirectory(pth)) {
                log.error("[movedir setup] failed to create directory in fs: " + root + "/" + destDir.getUri() + fail);
                return false;
            }

            log.info("[movedir setup] " +  dn.getClass().getSimpleName() + " " + dn.getUri() + " " + owner);
            pth = nodeUtil.create(dn);
            if (pth == null || !Files.exists(pth)) {
                log.error("[movedir setup] failed to create file in fs: " + root + "/" + dn.getUri() + fail);
                return false;
            }

            int num = 0;
            nodeUtil.move(srcDir.getUri(), destDir.getUri(), srcDir.getName(), ownerPrincipal);
            VOSURI expected = new VOSURI(URI.create(vosBaseURI + "/" + name2 + "/" + name1 + "/" + name3));
            Node moved = nodeUtil.get(expected);
            if (moved == null || !(moved instanceof DataNode)) {
                log.error("[movedir action] move+get failed to retrieve moved node, moved: " + moved.getClass().getSimpleName()
                        + ", " + moved);
                num++;
            }
            
            // TODO: make sure it was a move (src does not exist)

            StringBuilder sb = new StringBuilder();
            if (num == 0) {
                sb.append(" [OK]");
                log.info(sb.toString());
                return true;
            } else {
                sb.append(" [FAIL]");
                log.error(sb.toString());
                return false;
            }
        } catch (IOException | InterruptedException ex) {
            log.error("FAIL", ex);
            return false;
        }
    }

    public boolean doRenameDir() {
        try {
            String name1 = UUID.randomUUID().toString();
            String name2 = UUID.randomUUID().toString();
            VOSURI dn1uri = new VOSURI(URI.create(vosBaseURI + "/" + name1));
            VOSURI dn2uri = new VOSURI(URI.create(vosBaseURI + "/" + name2));

            ContainerNode src = new ContainerNode(dn1uri);
            ContainerNode dest = new ContainerNode(dn2uri);

            NodeUtil.setOwner(src, owner);
            NodeUtil.setOwner(dest, owner);

            Path pth = null;
            String fail = " [FAIL]";

            log.info("[renameDir] " +  src.getClass().getSimpleName() + " " + src.getUri() + " " + owner);
            pth = nodeUtil.create(src);
            if (pth == null || !Files.isDirectory(pth)) {
                log.error("[renameDir] failed to create directory in fs: " + root + "/" + src.getUri() + fail);
                return false;
            }

            int num = 0;
            nodeUtil.move(src.getUri(), dest.getUri().getParentURI(), dest.getName(), ownerPrincipal);
            VOSURI expected = new VOSURI(URI.create(vosBaseURI + "/" + name2));
            Node moved = nodeUtil.get(expected);
            if (moved == null || !(moved instanceof ContainerNode)) {
                log.error("[renameDir] failed to retrieve moved node, moved: " + moved.getClass().getSimpleName()
                        + ", " + moved);
                num++;
            }

            StringBuilder sb = new StringBuilder();
            if (num == 0) {
                sb.append(" [OK]");
                log.info(sb.toString());
                return true;
            } else {
                sb.append(" [FAIL]");
                log.error(sb.toString());
                return false;
            }
        } catch (IOException | InterruptedException ex) {
            log.error("FAIL", ex);
            return false;
        }
    }

    public boolean doCopySymlink() {
        try {
            String name1 = UUID.randomUUID().toString();
            String name2 = UUID.randomUUID().toString();
            String name3 = UUID.randomUUID().toString();
            String name4 = UUID.randomUUID().toString();
            VOSURI cn1uri = new VOSURI(URI.create(vosBaseURI + "/" + name1));
            VOSURI cn2uri = new VOSURI(URI.create(vosBaseURI + "/" + name2));
            VOSURI dnuri = new VOSURI(URI.create(vosBaseURI + "/" + name1 + "/" + name3));
            VOSURI lnuri = new VOSURI(URI.create(vosBaseURI + "/" + name1 + "/" + name4));

            ContainerNode srcDir = new ContainerNode(cn1uri);
            ContainerNode destDir = new ContainerNode(cn2uri);
            DataNode dn = new DataNode(dnuri);
            LinkNode ln = new LinkNode(lnuri, dn.getUri().getURI());

            NodeUtil.setOwner(srcDir, owner);
            NodeUtil.setOwner(destDir, owner);
            NodeUtil.setOwner(dn, owner);
            NodeUtil.setOwner(ln, owner);

            Path pth = null;
            String fail = " [FAIL]";

            log.info("[copysymlink setup] " +  srcDir.getClass().getSimpleName() + " " + srcDir.getUri() + " " + owner);
            pth = nodeUtil.create(srcDir);
            if (pth == null || !Files.isDirectory(pth)) {
                log.error("[copysymlink setup] failed to create directory in fs: " + root + "/" + srcDir.getUri() + fail);
                return false;
            }

            log.info("[copysymlink setup] " +  destDir.getClass().getSimpleName() + " " + destDir.getUri() + " " + owner);
            pth = nodeUtil.create(destDir);
            if (pth == null || !Files.isDirectory(pth)) {
                log.error("[copysymlink setup] failed to create directory in fs: " + root + "/" + destDir.getUri() + fail);
                return false;
            }

            log.info("[copysymlink setup] " +  dn.getClass().getSimpleName() + " " + dn.getUri() + " " + owner);
            pth = nodeUtil.create(dn);
            if (pth == null || !Files.exists(pth)) {
                log.error("[copysymlink setup] failed to create file in fs: " + root + "/" + dn.getUri() + fail);
                return false;
            }

            log.info("[copysymlink] " +  ln.getClass().getSimpleName() + " " + ln.getUri() + " " + owner);
            pth = nodeUtil.create(ln);
            if (pth == null || !Files.exists(pth)) {
                log.error("[copysymlink] failed to create symlink in fs: " + root + "/" + ln.getUri() + fail);
                return false;
            }

            int num = 0;
            nodeUtil.copy(ln.getUri(), destDir.getUri(), ownerPrincipal);
            VOSURI expected = new VOSURI(URI.create(vosBaseURI + "/" + name2 + "/" + name4));
            Node copied = nodeUtil.get(expected);
            if (copied == null || !(copied instanceof DataNode)) {
                log.error("[copysymlink action] copy+get failed to retrieve copied symlink, copied: " + copied.getClass().getSimpleName()
                        + ", " + copied);
                num++;
            }

            Node orig = nodeUtil.get(ln.getUri());
            if (orig == null || !(orig instanceof LinkNode)) {
                log.error("[copysymlink action] failed to retrieve original node, original: " + orig.getClass().getSimpleName()
                        + ", " + orig);
                num++;
            }

            StringBuilder sb = new StringBuilder();
            if (num == 0) {
                sb.append(" [OK]");
                log.info(sb.toString());
                return true;
            } else {
                sb.append(" [FAIL]");
                log.error(sb.toString());
                return false;
            }
        } catch (IOException | InterruptedException ex) {
            log.error("FAIL", ex);
            return false;
        }
    }

    public boolean doMoveSymlink() {
        try {
            String name1 = UUID.randomUUID().toString();
            String name2 = UUID.randomUUID().toString();
            String name3 = UUID.randomUUID().toString();
            String name4 = UUID.randomUUID().toString();
            VOSURI cn1uri = new VOSURI(URI.create(vosBaseURI + "/" + name1));
            VOSURI cn2uri = new VOSURI(URI.create(vosBaseURI + "/" + name2));
            VOSURI dnuri = new VOSURI(URI.create(vosBaseURI + "/" + name1 + "/" + name3));
            VOSURI lnuri = new VOSURI(URI.create(vosBaseURI + "/" + name1 + "/" + name4));

            ContainerNode srcDir = new ContainerNode(cn1uri);
            ContainerNode destDir = new ContainerNode(cn2uri);
            DataNode dn = new DataNode(dnuri);
            LinkNode ln = new LinkNode(lnuri, dn.getUri().getURI());

            NodeUtil.setOwner(srcDir, owner);
            NodeUtil.setOwner(destDir, owner);
            NodeUtil.setOwner(dn, owner);
            NodeUtil.setOwner(ln, owner);

            Path pth = null;
            String fail = " [FAIL]";

            log.info("[movesymlink setup] " +  srcDir.getClass().getSimpleName() + " " + srcDir.getUri() + " " + owner);
            pth = nodeUtil.create(srcDir);
            if (pth == null || !Files.isDirectory(pth)) {
                log.error("[movesymlink setup] failed to create directory in fs: " + root + "/" + srcDir.getUri() + fail);
                return false;
            }

            log.info("[movesymlink setup] " +  destDir.getClass().getSimpleName() + " " + destDir.getUri() + " " + owner);
            pth = nodeUtil.create(destDir);
            if (pth == null || !Files.isDirectory(pth)) {
                log.error("[movesymlink setup] failed to create directory in fs: " + root + "/" + destDir.getUri() + fail);
                return false;
            }

            log.info("[movesymlink setup] " +  dn.getClass().getSimpleName() + " " + dn.getUri() + " " + owner);
            pth = nodeUtil.create(dn);
            if (pth == null || !Files.exists(pth)) {
                log.error("[movesymlink setup] failed to create file in fs: " + root + "/" + dn.getUri() + fail);
                return false;
            }

            log.info("[movesymlink setup] " +  ln.getClass().getSimpleName() + " " + ln.getUri() + " " + owner);
            pth = nodeUtil.create(ln);
            if (pth == null || !Files.exists(pth)) {
                log.error("[movesymlink setup] failed to create symlink in fs: " + root + "/" + ln.getUri() + fail);
                return false;
            }

            int num = 0;
            nodeUtil.move(ln.getUri(), destDir.getUri(), ln.getName(), ownerPrincipal);
            VOSURI expected = new VOSURI(URI.create(vosBaseURI + "/" + name2 + "/" + name4));
            Node copied = nodeUtil.get(expected);
            if (copied == null || !(copied instanceof LinkNode)) {
                log.error("[movesymlink action] move+get failed to retrieve copied symlink, copied: " + copied.getClass().getSimpleName()
                        + ", " + copied);
                num++;
            }

            StringBuilder sb = new StringBuilder();
            if (num == 0) {
                sb.append(" [OK]");
                log.info(sb.toString());
                return true;
            } else {
                sb.append(" [FAIL]");
                log.error(sb.toString());
                return false;
            }
        } catch (IOException | InterruptedException ex) {
            log.error("FAIL", ex);
            return false;
        }
    }
}
