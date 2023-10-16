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
import ca.nrc.cadc.util.Base64;
import ca.nrc.cadc.util.MultiValuedProperties;
import ca.nrc.cadc.uws.Job;
import ca.nrc.cadc.uws.Parameter;
import java.beans.PersistenceDelegate;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.nio.file.FileSystem;
import java.nio.file.FileSystems;
import java.security.AccessControlException;
import java.util.ArrayList;
import java.util.List;
import javax.security.auth.Subject;
import org.apache.log4j.Logger;
import org.opencadc.cavern.CavernConfig;
import org.opencadc.cavern.FileSystemNodePersistence;
import org.opencadc.cavern.PosixIdentityManager;
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
import org.opencadc.vospace.server.PathResolver;
import org.opencadc.vospace.server.auth.VOSpaceAuthorizer;
import org.opencadc.vospace.server.transfers.TransferGenerator;
import org.opencadc.vospace.transfer.Direction;
import org.opencadc.vospace.transfer.Protocol;
import org.opencadc.vospace.transfer.Transfer;

public class CavernURLGenerator implements TransferGenerator {

    private static final Logger log = Logger.getLogger(CavernURLGenerator.class);

    private final String sshServerBase;
    private final PosixIdentityManager identityManager = new PosixIdentityManager();

    public static final String KEY_SIGNATURE = "sig";
    public static final String KEY_META = "meta";
    private static final String KEY_META_NODE = "node";
    private static final String KEY_META_DIRECTION = "dir";
    private static final String ANON_USER = "anon";
    private final MultiValuedProperties config;
    
    private final FileSystemNodePersistence nodePersistence;
    private final VOSpaceAuthorizer authorizer;


    public CavernURLGenerator(FileSystemNodePersistence nodePersistence, MultiValuedProperties config) {
        this.nodePersistence = nodePersistence;
        this.authorizer = new VOSpaceAuthorizer(nodePersistence);
        this.config = config;
        
        String sb = config.getFirstPropertyValue(CavernConfig.SSHFS_SERVER_BASE);
        // make sure server bas ends with /
        if (sb != null && !sb.endsWith("/")) {
            sb = sb + "/";
        }
        this.sshServerBase = sb;
    }

    // unit test
    
    public CavernURLGenerator() {
        this.sshServerBase = null;
        this.config = null;
        this.nodePersistence = null;
        this.authorizer = null;
    }

    @Override
    public List<Protocol> getEndpoints(VOSURI target, Transfer transfer, Job job, List<Parameter> additionalParams) throws Exception {
        if (target == null) {
            throw new IllegalArgumentException("target is required");
        }
        if (transfer == null) {
            throw new IllegalArgumentException("transfer is required");
        }

        try {
            PathResolver ps = new PathResolver(nodePersistence, authorizer, true);
            Node node = ps.getNode(target.getPath());

            List<Protocol> ret = new ArrayList<>();
            List<URI> uris;
            for (Protocol protocol : transfer.getProtocols()) {
                log.debug("addressing protocol: " + protocol);
                Subject currentSubject = AuthenticationUtil.getCurrentSubject();
                if (node instanceof ContainerNode) {
                    PosixPrincipal caller = identityManager.toPosixPrincipal(currentSubject);
                    uris = handleContainerMount(target, protocol, caller);
                } else if (node instanceof DataNode) {
                    uris = handleDataNode(target, (DataNode) node, protocol, currentSubject);
                } else {
                    throw new UnsupportedOperationException(node.getClass().getSimpleName() + " transfer "
                            + target.getPath());
                }
                for (URI u : uris) {
                    Protocol p = new Protocol(protocol.getUri(), u.toASCIIString(), null);
                    p.setSecurityMethod(protocol.getSecurityMethod());
                    ret.add(p);
                }
            }
            return ret;
        } catch (NodeNotFoundException ex) {
            throw new FileNotFoundException(target.getPath());
        } catch (LinkingException ex) {
            throw new RuntimeException("OOPS: failed to resolve link?", ex);
        }
    }



    private List<URI> handleDataNode(VOSURI target, DataNode node, Protocol protocol, Subject s) {
        String scheme = null;
        Direction dir = null;

        try {
            // Used in TokenTool.generateToken()
            Class<? extends Grant> grantClass = null;

            if (VOS.PROTOCOL_HTTPS_GET.equals(protocol.getUri())) {
                scheme = "https";
                dir = Direction.pullFromVoSpace;
                grantClass = ReadGrant.class;
            } else if (VOS.PROTOCOL_HTTPS_PUT.equals(protocol.getUri())) {
                scheme = "https";
                dir = Direction.pushToVoSpace;
                grantClass = WriteGrant.class;
            } else {
                throw new UnsupportedOperationException("unsupported protocol: " + protocol.getUri());
            }

            List<URL> baseURLs = getBaseURLs(target, protocol.getSecurityMethod(), scheme);
            if (baseURLs == null || baseURLs.isEmpty()) {
                log.debug("no matching interfaces ");
                return new ArrayList<URI>(0);
            }

            if (dir == null) {
                log.debug("no matching protocols");
                return new ArrayList<URI>(0);
            }

            // Use TokenTool to generate a preauth token
            File privateKeyFile = findFile(CavernConfig.PRIVATE_KEY);
            File pubKeyFile = findFile(CavernConfig.PUBLIC_KEY);
            TokenTool gen = null; 
            if (pubKeyFile != null && privateKeyFile != null) {
                gen = new TokenTool(pubKeyFile, privateKeyFile);
            }

            // Format of token is <base64 url encoded meta>.<base64 url encoded signature>
            IdentityManager im = AuthenticationUtil.getIdentityManager();
            Subject caller = AuthenticationUtil.getCurrentSubject();
            String callingUser = im.toDisplayString(caller); // should be null for anon

            // Use CommonFormURI in case the incoming URI uses '!' instead of '~' in the authority.
            StringBuilder path = new StringBuilder();
            if (gen != null) {
                String token = gen.generateToken(target.getCommonFormURI().getURI(), grantClass, callingUser);
                String encodedToken = new String(Base64.encode(token.getBytes()));
                path.append("/");
                path.append(encodedToken);
                path.append("/");
            }

            if (Direction.pushToVoSpace.equals(dir)) {
                // put to resolved path
                path.append(target.getPath());
            } else {
                // get from unresolved path so filename at end of url is correct
                path.append(target.getURI().getPath());
            }
            log.debug("Created request path: " + path.toString());

            // add the request path to each of the base URLs
            List<URI> returnList = new ArrayList<URI>(baseURLs.size());
            for (URL baseURL : baseURLs) {
                URI next;
                try {
                    next = new URI(baseURL.toString() + path.toString());
                    log.debug("Added url: " + next);
                    returnList.add(next);
                } catch (URISyntaxException e) {
                    throw new IllegalStateException("Could not generate transfer endpoint uri", e);
                }
            }

            return returnList;
        }
        finally {
        }
    }
    
    private List<URI> handleContainerMount(VOSURI target, Protocol protocol, PosixPrincipal caller) {
        if (sshServerBase == null) {
            throw new UnsupportedOperationException("CONFIG: sshfs mount not configured in "
                    + CavernConfig.CAVERN_PROPERTIES);
        }
        if (!VOS.PROTOCOL_SSHFS.equals(protocol.getUri())) {
            throw new IllegalArgumentException("cannot use protocol " + protocol.getUri() + " with ContainerNode "
                    + target.getPath());
        }
        
        List<URI> ret = new ArrayList<URI>();
        StringBuilder sb = new StringBuilder();
        sb.append("sshfs:");
        sb.append(caller.username).append("@");
        sb.append(sshServerBase);
        sb.append(target.getPath().substring(1)); // sshServerBase includes the initial /
        try {
            URI u = new URI(sb.toString());
            ret.add(u);
        } catch (URISyntaxException ex) {
            throw new RuntimeException("BUG: failed to generate mount endpoint URI", ex);
        }
        return ret;
    }

    public VOSURI validateToken(String token, VOSURI targetVOSURI, Direction direction)
            throws AccessControlException, IOException {

        log.debug("url encoded token: " + token);
        log.debug("direction: " + direction.toString());

        String decodedTokenbytes = new String(Base64.decode(token));
        log.debug("url decoded token: " + decodedTokenbytes);

        // Use this function in case the incoming URI uses '!' instead of '~'
        // in the authority.
        // This will translate the URI to use '~' in it's authority.
        VOSURI commonFormURI = targetVOSURI.getCommonFormURI();
        log.debug("targetURI passed in: " + targetVOSURI.toString());
        log.debug("targetURI for validation: " + commonFormURI.toString());
        if (token != null) {

            File publicKeyFile = findFile(CavernConfig.PUBLIC_KEY);
            TokenTool tk = new TokenTool(publicKeyFile);

            Class<? extends Grant> grantClass = null;
            if (Direction.pushToVoSpace.equals(direction)) {
                grantClass = WriteGrant.class;
            } else if (Direction.pullFromVoSpace.equals(direction)) {
                grantClass = ReadGrant.class;
            }

            log.debug("grant class: " + grantClass);

            // This will throw an AccessControlException if something is wrong with the
            // grantClass or targetURI. Can return null if user isn't in the meta key=value set
            String tokenUser = tk.validateToken(decodedTokenbytes, commonFormURI.getURI(), grantClass);

            if (tokenUser == null) {
                throw new AccessControlException("invalid token");
            }

            return commonFormURI;

        }

        throw new IllegalArgumentException("Missing node URI");
    }

    List<URL> getBaseURLs(VOSURI target, URI securityMethod, String scheme) {
        // find all the base endpoints
        
        URI serviceURI = target.getServiceURI();
        List<URL> baseURLs = new ArrayList<URL>();
        try {
            RegistryClient rc = new RegistryClient();
            Capabilities caps = rc.getCapabilities(serviceURI);
            Capability cap = caps.findCapability(Standards.DATA_10);
            List<Interface> interfaces = cap.getInterfaces();
            for (Interface ifc : interfaces) {
                log.debug("securityMethod match? " + securityMethod + " vs " + ifc.getSecurityMethods().size());
                log.debug("scheme match? " + scheme + " vs " + ifc.getAccessURL().getURL().getProtocol());
                if (securityMethod == null 
                        && (ifc.getSecurityMethods().isEmpty() || ifc.getSecurityMethods().contains(Standards.SECURITY_METHOD_ANON)
                        && ifc.getAccessURL().getURL().getProtocol().equals(scheme))) {
                    baseURLs.add(ifc.getAccessURL().getURL());
                    log.debug("Added anon interface");
                } else if (ifc.getSecurityMethods().contains(securityMethod)
                        && ifc.getAccessURL().getURL().getProtocol().equals(scheme)) {
                    baseURLs.add(ifc.getAccessURL().getURL());
                    log.debug("Added auth interface.");
                }
            }
        } catch (IOException | ResourceNotFoundException e) {
            throw new IllegalStateException("Error creating transfer urls", e);
        }
        return baseURLs;
    }

    protected File findFile(String key) {
        String value = this.config.getFirstPropertyValue(key);
        if (value == null) {
            return null;
        }
        File ret = new File(CavernConfig.DEFAULT_CONFIG_DIR, value);
        if (!ret.exists()) {
            throw new IllegalStateException(String.format("CONFIG: file %s not found for property %s",
                    ret.getAbsolutePath(), key));
        }
        return ret;
    }

}


