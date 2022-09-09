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
import ca.nrc.cadc.net.ResourceNotFoundException;
import ca.nrc.cadc.net.TransientException;
import ca.nrc.cadc.reg.Capabilities;
import ca.nrc.cadc.reg.Capability;
import ca.nrc.cadc.reg.Interface;
import ca.nrc.cadc.reg.Standards;
import ca.nrc.cadc.reg.client.RegistryClient;
import ca.nrc.cadc.util.FileUtil;
import ca.nrc.cadc.util.PropertiesReader;
import ca.nrc.cadc.util.Base64;
import ca.nrc.cadc.uws.Job;
import ca.nrc.cadc.uws.Parameter;
import ca.nrc.cadc.vos.ContainerNode;
import ca.nrc.cadc.vos.DataNode;
import ca.nrc.cadc.vos.Direction;
import ca.nrc.cadc.vos.LinkingException;
import ca.nrc.cadc.vos.Node;
import ca.nrc.cadc.vos.NodeNotFoundException;
import ca.nrc.cadc.vos.Protocol;
import ca.nrc.cadc.vos.Transfer;
import ca.nrc.cadc.vos.VOS;
import ca.nrc.cadc.vos.VOSURI;
import ca.nrc.cadc.vos.View;
import ca.nrc.cadc.vos.server.LocalServiceURI;
import ca.nrc.cadc.vos.server.PathResolver;
import ca.nrc.cadc.vos.server.transfers.TransferGenerator;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.nio.file.FileSystem;
import java.nio.file.FileSystems;
import java.nio.file.attribute.UserPrincipal;
import java.security.AccessControlException;
import java.util.ArrayList;
import java.util.List;
import java.util.MissingResourceException;
import org.apache.log4j.Logger;
import org.opencadc.cavern.FileSystemNodePersistence;
import org.opencadc.permissions.Grant;
import org.opencadc.permissions.ReadGrant;
import org.opencadc.permissions.TokenTool;
import org.opencadc.permissions.WriteGrant;

public class CavernURLGenerator implements TransferGenerator {

    private static final Logger log = Logger.getLogger(CavernURLGenerator.class);

    private static final String DEFAULT_CONFIG_DIR = System.getProperty("user.home") + "/config/";

    private final FileSystemNodePersistence nodes;
    private final String sshServerBase;

    public static final String KEY_SIGNATURE = "sig";
    public static final String KEY_META = "meta";
    private static final String KEY_META_NODE = "node";
    private static final String KEY_META_DIRECTION = "dir";
    
    private static final String PUB_KEY_FILENAME = "CavernPub.key";
    private static final String PRIV_KEY_FILENAME = "CavernPriv.key";
    private static final String CAVERN_INTERNAL_USER = "cavernInternalUser";

    public CavernURLGenerator() {
        this.nodes = new FileSystemNodePersistence();
        PropertiesReader pr = new PropertiesReader(FileSystemNodePersistence.CONFIG_FILE);
        String sb = pr.getFirstPropertyValue("SSHFS_SERVER_BASE");
        // make sure server bas ends with /
        if (sb != null && !sb.endsWith("/")) {
            sb = sb + "/";
        }
        this.sshServerBase = sb;
    }

    // for testing
    public CavernURLGenerator(String root) {
        this.nodes = new FileSystemNodePersistence(root);
        PropertiesReader pr = new PropertiesReader(FileSystemNodePersistence.CONFIG_FILE);
        this.sshServerBase = pr.getFirstPropertyValue("SSHFS_SERVER_BASE");
    }

    @Override
    public List<Protocol> getEndpoints(VOSURI target, Transfer transfer, View view,
            Job job, List<Parameter> additionalParams)
            throws FileNotFoundException, TransientException {
        if (target == null) {
            throw new IllegalArgumentException("target is required");
        }
        if (transfer == null) {
            throw new IllegalArgumentException("transfer is required");
        }

        try {
            // get the node to check the type - consider changing this API
            // so that the node instead of the URI is passed in so this step
            // can be avoided.
            FileSystem fs = FileSystems.getDefault();
            PathResolver ps = new PathResolver(nodes, true);
            Node node = ps.resolveWithReadPermissionCheck(target, null, true);

            List<Protocol> ret = new ArrayList<>();
            List<URI> uris;
            for (Protocol protocol : transfer.getProtocols()) {
                log.debug("addressing protocol: " + protocol);
                if (node instanceof ContainerNode) {
                    UserPrincipal caller = nodes.getPosixUser(AuthenticationUtil.getCurrentSubject());
                    uris = handleContainerMount(target, (ContainerNode) node, protocol, caller);
                } else if (node instanceof DataNode) {
                    uris = handleDataNode(target, (DataNode) node, protocol);
                } else {
                    throw new UnsupportedOperationException(node.getClass().getSimpleName() + " transfer " 
                        + node.getUri());
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
        } catch (IOException ex) {
            throw new RuntimeException("OOPS: failed to resolve subject to posix user", ex);
        } catch (LinkingException ex) {
            throw new RuntimeException("OOPS: failed to resolve link?", ex);
        }
    }



    private List<URI> handleDataNode(VOSURI target, DataNode node, Protocol protocol) {
        String scheme = null;
        Direction dir = null;

        try {
            // Used in TokenTool.generateToken()
            Class<? extends Grant> grantClass = null;

            switch (protocol.getUri()) {
                /**
                 * HTTP not currently supported
                case VOS.PROTOCOL_HTTP_GET:
                    scheme = "http";
                    dir = Direction.pullFromVoSpace;
                    break;
                case VOS.PROTOCOL_HTTP_PUT:
                    scheme = "http";
                    dir = Direction.pushToVoSpace;
                    break;
                */
                case VOS.PROTOCOL_HTTPS_GET:
                    scheme = "https";
                    dir = Direction.pullFromVoSpace;
                    grantClass = ReadGrant.class;
                    break;
                case VOS.PROTOCOL_HTTPS_PUT:
                    scheme = "https";
                    dir = Direction.pushToVoSpace;
                    grantClass = WriteGrant.class;
                    break;
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
            File privateKeyFile = findFile(PRIV_KEY_FILENAME);
            File pubKeyFile = findFile(PUB_KEY_FILENAME);

            TokenTool gen = new TokenTool(pubKeyFile, privateKeyFile);

            // Format of token is <base64 url encoded meta>.<base64 url encoded signature>
            String token = gen.generateToken(target.getURI(), grantClass, CAVERN_INTERNAL_USER);
            String encodedToken = new String(Base64.encode(token.getBytes()));

            // build the request path
            StringBuilder path = new StringBuilder();
            path.append("/");
            path.append(encodedToken);
//            path.append(token);
            path.append("/");

            if (Direction.pushToVoSpace.equals(dir)) {
                // put to resolved path
                path.append(node.getUri().getPath());
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
    
    private List<URI> handleContainerMount(VOSURI target, ContainerNode node, Protocol protocol, UserPrincipal caller) {
        if (sshServerBase == null) {
            throw new UnsupportedOperationException("CONFIG: sshfs mount not configured in " + FileSystemNodePersistence.CONFIG_FILE);
        }
        List<URI> ret = new ArrayList<URI>();
        StringBuilder sb = new StringBuilder();
        sb.append("sshfs:");
        sb.append(caller.getName()).append("@");
        sb.append(sshServerBase);
        sb.append(node.getUri().getPath().substring(1)); // sshServerBase includes the initial /
        try {
            URI u = new URI(sb.toString());
            ret.add(u);
        } catch (URISyntaxException ex) {
            throw new RuntimeException("BUG: failed to generate mount endpoint URI", ex);
        }
        return ret;
    }

    public VOSURI getNodeURI(String token, VOSURI targetVOSURI, Direction direction) throws AccessControlException, IOException {

        // TODO: behaviour will vary if token passed in or not
        log.debug("url encoded token: " + token);
        log.debug("direction: " + direction.toString());

        String decodedTokenbytes = new String(Base64.decode(token));
        log.debug("url decoded token: " + decodedTokenbytes);
//        log.debug("not decoding token as it wasn't encoded.)");
        URI targetURI = targetVOSURI.getURI();
        URI nodeURI = null;
        if (token != null) {

            File publicKeyFile = findFile(PUB_KEY_FILENAME);
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
            String tokenUser = tk.validateToken(decodedTokenbytes, targetURI, grantClass);
//            log.debug("using token only, not b64 encoded token...");
//            String tokenUser = tk.validateToken(token, targetURI, grantClass);

            if (tokenUser == null) {
                throw new AccessControlException("invalid token");
            }

            if (CAVERN_INTERNAL_USER.equals(tokenUser)) {
                nodeURI = targetURI;
            } else {
                throw new AccessControlException("Invalid user in token: " + tokenUser);
            }
        }

        log.debug("nodeURI: " + nodeURI.toString());

        if (nodeURI != null) {
            VOSURI vosURI = new VOSURI(nodeURI);
            log.debug("vosURI generated from node URI: " + vosURI);
            return vosURI;
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
            // TODO: test if this needs to be something else?
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

    protected static final File findFile(String fname) throws MissingResourceException {
        File ret = new File(DEFAULT_CONFIG_DIR, fname);
        if (!ret.exists()) {
            ret = FileUtil.getFileFromResource(fname, CavernURLGenerator.class);
        }
        return ret;
    }


    public static VOSURI getURIFromPath(String path) throws URISyntaxException {

        log.debug("getURIFromPath for " + path);
        LocalServiceURI localServiceURI = new LocalServiceURI();
        VOSURI baseURI = localServiceURI.getVOSBase();
        log.debug("baseURI for cavern deployment: " + baseURI.toString());

        int firstSlashIndex = path.indexOf("/");
        String pathStr = path.substring(firstSlashIndex + 1);
        log.debug("path: " + pathStr);
        String targetURIStr = baseURI.toString() + "/" + pathStr;
        log.debug("target URI for validating token: " + targetURIStr);

        URI targetURI = new URI(targetURIStr);
        log.debug("targetURI for system: " + targetURI.toString());
        VOSURI targetVOSURI = new VOSURI(targetURI);
        log.debug("targetVOSURI: " + targetVOSURI.getURI().toString());

        return targetVOSURI;

    }

}


