/*
************************************************************************
*******************  CANADIAN ASTRONOMY DATA CENTRE  *******************
**************  CENTRE CANADIEN DE DONNÉES ASTRONOMIQUES  **************
*
*  (c) 2024.                            (c) 2024.
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

package org.opencadc.cavern.files;

import ca.nrc.cadc.auth.AuthenticationUtil;
import ca.nrc.cadc.auth.IdentityManager;
import ca.nrc.cadc.auth.PosixPrincipal;
import ca.nrc.cadc.net.ResourceNotFoundException;
import ca.nrc.cadc.reg.Capabilities;
import ca.nrc.cadc.reg.Capability;
import ca.nrc.cadc.reg.Interface;
import ca.nrc.cadc.reg.Standards;
import ca.nrc.cadc.reg.client.RegistryClient;
import ca.nrc.cadc.uws.Job;
import ca.nrc.cadc.uws.Parameter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.URI;
import java.security.AccessControlException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import javax.security.auth.Subject;
import org.apache.log4j.Logger;
import org.opencadc.cavern.CavernConfig;
import org.opencadc.cavern.nodes.FileSystemNodePersistence;
import org.opencadc.permissions.Grant;
import org.opencadc.permissions.ReadGrant;
import org.opencadc.permissions.TokenTool;
import org.opencadc.permissions.WriteGrant;
import org.opencadc.vospace.ContainerNode;
import org.opencadc.vospace.DataNode;
import org.opencadc.vospace.LinkingException;
import org.opencadc.vospace.Node;
import org.opencadc.vospace.NodeNotFoundException;
import org.opencadc.vospace.VOS;
import org.opencadc.vospace.VOSURI;
import org.opencadc.vospace.server.LocalServiceURI;
import org.opencadc.vospace.server.PathResolver;
import org.opencadc.vospace.server.auth.VOSpaceAuthorizer;
import org.opencadc.vospace.server.transfers.TransferGenerator;
import org.opencadc.vospace.transfer.Direction;
import org.opencadc.vospace.transfer.Protocol;
import org.opencadc.vospace.transfer.Transfer;

public class CavernURLGenerator implements TransferGenerator {

    private static final Logger log = Logger.getLogger(CavernURLGenerator.class);

    private final String sshServerBase;
    private final CavernConfig config;
    
    private final FileSystemNodePersistence nodePersistence;
    private final VOSpaceAuthorizer authorizer;
    private Capability filesCap;

    public CavernURLGenerator(FileSystemNodePersistence nodePersistence) {
        this.nodePersistence = nodePersistence;
        this.authorizer = new VOSpaceAuthorizer(nodePersistence);
        this.config = nodePersistence.getConfig();
        
        String sb = config.getProperties().getFirstPropertyValue(CavernConfig.SSHFS_SERVER_BASE);
        // make sure server bas ends with /
        if (sb != null && !sb.endsWith("/")) {
            sb = sb + "/";
        }
        this.sshServerBase = sb;
    }

    @Override
    public List<Protocol> getEndpoints(VOSURI target, Transfer transfer, Job job, List<Parameter> additionalParams) throws Exception {
        log.debug("getEndpoints: " + target);
        if (target == null) {
            throw new IllegalArgumentException("target is required");
        }
        if (transfer == null) {
            throw new IllegalArgumentException("transfer is required");
        }
        List<Protocol> ret;
        try {
            PathResolver pr = new PathResolver(nodePersistence, authorizer, true);
            PathResolver.Resolved resolved = pr.resolveNode(target);
            log.debug("Resolved target node: " + resolved);
            final Subject currentSubject = AuthenticationUtil.getCurrentSubject();
            if (resolved.child != null) {
                if (resolved.child instanceof DataNode) {
                    ret = handleDataNode(resolved.child.parent, resolved.child.getName(), transfer);
                } else if (resolved.child instanceof ContainerNode) {
                    ret = handleContainerMount(target.getPath(), transfer, currentSubject);
                } else {
                    throw new UnsupportedOperationException(resolved.child.getClass().getSimpleName() + " transfer "
                            + target.getPath());
                }
            } else {
                // assume DataNode that needs to be created
                ret = handleDataNode(resolved.parent, resolved.childName, transfer);
            }

        } catch (NodeNotFoundException ex) {
            throw new FileNotFoundException(target.getPath());
        } catch (LinkingException ex) {
            throw new RuntimeException("OOPS: failed to resolve link?", ex);
        }
        return ret;
    }

    private List<Protocol> handleDataNode(Node parent, String name, Transfer trans) {
        log.debug("handleDataNode: " + parent +  " " + name);

        try {
            initFilesCap();
        } catch (IOException | ResourceNotFoundException ex) {
            throw new RuntimeException("CONFIG: failed to find files endpoint via registry self-lookup (ugh)", ex);
        }
        
        Direction dir = trans.getDirection();
        final Map<String,String> params = new TreeMap<>(); // empty for now

        // Use TokenTool to generate a preauth token
        File privateKeyFile = config.getPrivateKey();
        File pubKeyFile = config.getPublicKey();
        TokenTool gen = null; 
        if (pubKeyFile != null && privateKeyFile != null) {
            log.debug("found keys - creating TokenTool");
            gen = new TokenTool(pubKeyFile, privateKeyFile);
        }
        
        boolean allowAnon = true;
        Class<? extends Grant> grantClass;
        if (Direction.pullFromVoSpace.equals(dir)) {
            grantClass = ReadGrant.class;
        } else if (Direction.pushToVoSpace.equals(dir)) {
            grantClass = WriteGrant.class;
            allowAnon = false; // never really anon write
        } else {
            throw new UnsupportedOperationException("unsupported direction: " + dir);
        }

        IdentityManager im = nodePersistence.getIdentityManager();
        Set<URI> supportedSecurityMethods = im.getSecurityMethods();
        Subject caller = AuthenticationUtil.getCurrentSubject();
        Object userObject = im.toOwner(caller); // posix
        String callingUser = (userObject == null ? null : userObject.toString()); // uid, null for anon is OK

        LocalServiceURI loc = new LocalServiceURI(nodePersistence.getResourceID());
        VOSURI vp = loc.getURI(parent);
        String parentPath = vp.getPath();
        // this should be the resolved target URL
        VOSURI target = new VOSURI(nodePersistence.getResourceID(), parentPath + "/" + name);
        
        List<Protocol> returnList = new ArrayList<>();
        for (Protocol p : trans.getProtocols()) {
            log.debug("requested protocol: " + p);
            URI secM = p.getSecurityMethod();
            if (secM == null) {
                secM = Standards.SECURITY_METHOD_ANON;
            }
            Interface iface = filesCap.findInterface(secM);
            if (iface != null && supportedSecurityMethods.contains(secM)) {
                String baseURL = iface.getAccessURL().getURL().toExternalForm();
                boolean anon = Standards.SECURITY_METHOD_ANON.equals(secM);
                if (gen != null && anon) {
                    // create an additional anon with preauth token
                    StringBuilder sb2 = new StringBuilder(baseURL);

                    // Use CommonFormURI in case the incoming URI uses '!' instead of '~' in the authority.
                    URI resourceURI = target.getCommonFormURI().getURI();
                    String token = gen.generateToken(resourceURI, grantClass, callingUser);
                    sb2.append("/preauth:").append(token);
                    sb2.append(target.getPath());
                    Protocol pre = new Protocol(p.getUri(), sb2.toString(), params);
                    log.debug("added: " + pre);
                    returnList.add(pre);
                }
                if (allowAnon || !anon) {
                    StringBuilder sb = new StringBuilder(baseURL);
                    sb.append(target.getPath());
                    String endpoint = sb.toString();
                    Protocol pe = new Protocol(p.getUri(), endpoint, params);
                    pe.setSecurityMethod(p.getSecurityMethod());
                    log.debug("added: " + pe);
                    returnList.add(pe);
                }
            } else {
                log.debug("unsupported security method: " + p.getSecurityMethod() + " -- SKIP");
            }
        }
        return returnList;
    }
    
    private List<Protocol> handleContainerMount(String path, Transfer trans, Subject caller) {
        final Map<String,String> params = new TreeMap<>(); // empty for now
        
        PosixPrincipal pp = nodePersistence.getIdentityManager().toPosixPrincipal(caller);
        List<Protocol> ret = new ArrayList<>();
        for (Protocol p : trans.getProtocols()) {
            if (VOS.PROTOCOL_SSHFS.equals(p.getUri())) {
                if (sshServerBase == null) {
                    throw new UnsupportedOperationException("sshfs mount not configured");
                }
                // TODO: chose the right standardID for sshfs auth: pubkey? password?
                if (p.getSecurityMethod() == null || Standards.SECURITY_METHOD_ANON.equals(p.getSecurityMethod())) {
                    StringBuilder sb = new StringBuilder();
                    sb.append("sshfs:");
                    sb.append(pp.username).append("@");
                    sb.append(sshServerBase);
                    if (sb.charAt(sb.length() - 1) != '/') {
                        sb.append("/");
                    }
                    if (path.charAt(0) == '/') {
                        path = path.substring(1);
                    }
                    sb.append(path);
                    Protocol pe = new Protocol(p.getUri(), sb.toString(), params);
                    ret.add(pe);
                }
            } else {
                log.debug("unsupported container protocol: " + p.getUri());
            }
        }
        return ret;
    }

    // return the user
    Object validateToken(String token, VOSURI targetVOSURI, Class grantClass)
            throws AccessControlException, IOException {

        // Use TokenTool to generate a preauth token
        File pubKeyFile = config.getPublicKey();
        if (pubKeyFile == null || !pubKeyFile.exists()) {
            throw new AccessControlException("unable to validate preauth token: key not found");
        }
        
        TokenTool tk = new TokenTool(pubKeyFile);

        // Use this function in case the incoming URI uses '!' instead of '~'
        // in the authority.
        // This will translate the URI to use '~' in it's authority.
        VOSURI commonFormURI = targetVOSURI.getCommonFormURI();
        log.debug("targetURI passed in: " + targetVOSURI.toString());
        log.debug("targetURI for validation: " + commonFormURI.toString());
        log.debug("grant class: " + grantClass);

        return tk.validateToken(token, commonFormURI.getURI(), grantClass);
    }

    void initFilesCap() throws IOException, ResourceNotFoundException {
        if (filesCap == null) {
            // ugh: self lookup
            RegistryClient reg = new RegistryClient();
            Capabilities caps = reg.getCapabilities(nodePersistence.getResourceID());
            this.filesCap = caps.findCapability(Standards.VOSPACE_FILES_20);
        }
    }
}


