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
import ca.nrc.cadc.net.TransientException;
import ca.nrc.cadc.reg.Capabilities;
import ca.nrc.cadc.reg.Capability;
import ca.nrc.cadc.reg.Interface;
import ca.nrc.cadc.reg.Standards;
import ca.nrc.cadc.reg.client.RegistryClient;
import ca.nrc.cadc.util.PropertiesReader;
import ca.nrc.cadc.util.RsaSignatureGenerator;
import ca.nrc.cadc.util.RsaSignatureVerifier;
import ca.nrc.cadc.uws.Job;
import ca.nrc.cadc.uws.Parameter;
import ca.nrc.cadc.vos.ContainerNode;
import ca.nrc.cadc.vos.DataNode;
import ca.nrc.cadc.vos.Direction;
import ca.nrc.cadc.vos.LinkingException;
import ca.nrc.cadc.vos.Node;
import ca.nrc.cadc.vos.NodeNotFoundException;
import ca.nrc.cadc.vos.Protocol;
import ca.nrc.cadc.vos.VOS;
import ca.nrc.cadc.vos.VOSURI;
import ca.nrc.cadc.vos.View;
import ca.nrc.cadc.vos.server.PathResolver;
import ca.nrc.cadc.vos.server.transfers.TransferGenerator;
import java.io.ByteArrayInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.nio.file.FileSystem;
import java.nio.file.FileSystems;
import java.nio.file.attribute.UserPrincipal;
import java.security.AccessControlException;
import java.security.InvalidKeyException;
import java.security.SignatureException;
import java.util.ArrayList;
import java.util.List;
import org.apache.log4j.Logger;
import org.bouncycastle.util.encoders.Base64;
import org.opencadc.cavern.FileSystemNodePersistence;

public class CavernURLGenerator implements TransferGenerator {

    private static final Logger log = Logger.getLogger(CavernURLGenerator.class);

    //private String root;
    private final FileSystemNodePersistence nodes;
    private final String sshServerBase;

    public static final String KEY_SIGNATURE = "sig";
    public static final String KEY_META = "meta";
    private static final String KEY_META_NODE = "node";
    private static final String KEY_META_DIRECTION = "dir";

    public CavernURLGenerator() {
        this.nodes = new FileSystemNodePersistence();
        PropertiesReader pr = new PropertiesReader(FileSystemNodePersistence.CONFIG_FILE);
        this.sshServerBase = pr.getFirstPropertyValue("SSHFS_SERVER_BASE");
    }

    // for testing
    public CavernURLGenerator(String root) {
        this.nodes = new FileSystemNodePersistence(root);
        PropertiesReader pr = new PropertiesReader(FileSystemNodePersistence.CONFIG_FILE);
        this.sshServerBase = pr.getFirstPropertyValue("SSHFS_SERVER_BASE");
    }

    @Override
    public List<URI> getEndpoints(VOSURI target, Protocol protocol, View view,
            Job job, List<Parameter> additionalParams)
            throws FileNotFoundException, TransientException {
        if (target == null) {
            throw new IllegalArgumentException("target is required");
        }
        if (protocol == null) {
            throw new IllegalArgumentException("protocol is required");
        }

        try {
            // get the node to check the type - consider changing this API
            // so that the node instead of the URI is passed in so this step
            // can be avoided.
            FileSystem fs = FileSystems.getDefault();
            PathResolver ps = new PathResolver(nodes, true);
            Node node = ps.resolveWithReadPermissionCheck(target, null, true);

            if (node instanceof ContainerNode) {
                UserPrincipal caller = nodes.getPosixUser(AuthenticationUtil.getCurrentSubject());
                return handleContainerMount(target, (ContainerNode) node, protocol, caller);
            } else if (node instanceof DataNode) {
                return handleDataNode(target, (DataNode) node, protocol);
            } else {
                throw new UnsupportedOperationException(node.getClass().getSimpleName() + " transfer " 
                    + node.getUri());
            } 
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
            switch (protocol.getUri()) {
                case VOS.PROTOCOL_HTTP_GET:
                    scheme = "http";
                    dir = Direction.pullFromVoSpace;
                    break;
                case VOS.PROTOCOL_HTTP_PUT:
                    scheme = "http";
                    dir = Direction.pushToVoSpace;
                    break;
                case VOS.PROTOCOL_HTTPS_GET:
                    scheme = "https";
                    dir = Direction.pullFromVoSpace;
                    break;
                case VOS.PROTOCOL_HTTPS_PUT:
                    scheme = "https";
                    dir = Direction.pushToVoSpace;
                    break;
            }

            List<URL> baseURLs = getBaseURLs(target, protocol.getSecurityMethod(), scheme);
            if (baseURLs == null || baseURLs.isEmpty()) {
                log.debug("no matching interfaces ");
                return null;
            }

            // create the metadata and signature segments
            StringBuilder metaSb = new StringBuilder();
            metaSb.append(KEY_META_NODE).append("=").append(target.toString());
            metaSb.append("&");
            metaSb.append(KEY_META_DIRECTION).append("=").append(dir.getValue());
            byte[] metaBytes = metaSb.toString().getBytes();

            RsaSignatureGenerator sg = new RsaSignatureGenerator();
            String sig;
            try {
                byte[] sigBytes = sg.sign(new ByteArrayInputStream(metaBytes));
                sig = new String(Base64.encode(sigBytes));
                log.debug("Created signature: " + sig + " for meta: " + metaSb.toString());
            } catch (InvalidKeyException | IOException | RuntimeException e) {
                throw new IllegalStateException("Could not sign url", e);
            }
            String meta = new String(Base64.encode(metaBytes));
            log.debug("meta: " + meta);
            log.debug("sig: " + sig);

            // build the request path
            StringBuilder path = new StringBuilder();
            String metaURLEncoded = base64URLEncode(meta);
            String sigURLEncoded = base64URLEncode(sig);

            log.debug("metaURLEncoded: " + metaURLEncoded);
            log.debug("sigURLEncoded: " + sigURLEncoded);

            path.append("/");
            path.append(metaURLEncoded);
            path.append("/");
            path.append(sigURLEncoded);
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
                    throw new IllegalStateException("Could not generate trannsfer endpoint uri", e);
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
        sb.append(node.getUri().getPath());
        try {
            URI u = new URI(sb.toString());
            ret.add(u);
        } catch (URISyntaxException ex) {
            throw new RuntimeException("BUG: failed to generate mount endpoint URI", ex);
        }
        return ret;
    }

    public VOSURI getNodeURI(String meta, String sig, Direction direction) throws AccessControlException, IOException {

        if (sig == null || meta == null) {
            throw new IllegalArgumentException("Missing signature or meta info");
        }

        log.debug("sig: " + sig);
        log.debug("meta: " + meta);

        byte[] sigBytes = Base64.decode(base64URLDecode(sig));
        byte[] metaBytes = Base64.decode(base64URLDecode(meta));

        RsaSignatureVerifier sv = new RsaSignatureVerifier();
        boolean verified;
        try {
            verified = sv.verify(new ByteArrayInputStream(metaBytes), sigBytes);
        } catch (InvalidKeyException | RuntimeException e) {
            log.debug("Recieved invalid signature", e);
            throw new AccessControlException("Invalid signature");
        }
        if (!verified) {
            throw new AccessControlException("Invalid signature");
        }

        String[] metaParams = new String(metaBytes).split("&");
        String nodeURI = null;
        String dir = null;
        for (String metaParam : metaParams) {
            log.debug("Processing param: " + metaParam);
            String[] keyValue = metaParam.split("=");
            if (keyValue.length == 2) {
                if (KEY_META_NODE.equals(keyValue[0])) {
                    nodeURI = keyValue[1];
                }
                if (KEY_META_DIRECTION.equals(keyValue[0])) {
                    dir = keyValue[1];
                }
            }
        }
        log.debug("nodeURI: " + nodeURI);
        log.debug("dir: " + dir);
        if (dir == null) {
            throw new IllegalArgumentException("Direction not specified");
        }
        if (!direction.getValue().equals(dir)) {
            throw new IllegalArgumentException("Wrong direction");
        }
        if (nodeURI != null) {
            try {
                VOSURI vosURI = new VOSURI(nodeURI);
                return vosURI;
            } catch (URISyntaxException e) {
                throw new IllegalStateException("Invalid node URI");
            }
        }

        throw new IllegalArgumentException("Missing node URI");

    }

    public static String base64URLEncode(String s) {
        if (s == null) {
            return null;
        }
        return s.replace("/", "-").replace("+", "_");
    }

    public static String base64URLDecode(String s) {
        if (s == null) {
            return null;
        }
        return s.replace("-", "/").replace("_", "+");
    }

    List<URL> getBaseURLs(VOSURI target, URI securityMethod, String scheme) {
        // find all the base endpoints
        List<URL> baseURLs = new ArrayList<URL>();
        try {
            RegistryClient rc = new RegistryClient();
            URI serviceURI = target.getServiceURI();
            Capabilities caps = rc.getCapabilities(serviceURI);
            Capability cap = caps.findCapability(Standards.DATA_10);
            List<Interface> interfaces = cap.getInterfaces();
            for (Interface ifc : interfaces) {
                log.debug("securityMethod match? " + securityMethod + " vs " + ifc.getSecurityMethod());
                log.debug("scheme match? " + scheme + " vs " + ifc.getAccessURL().getURL().getProtocol());
                if (securityMethod == null && 
                        (ifc.getSecurityMethod() == null || Standards.SECURITY_METHOD_ANON.equals(ifc.getSecurityMethod()) &&
                        ifc.getAccessURL().getURL().getProtocol().equals(scheme))) {
                    baseURLs.add(ifc.getAccessURL().getURL());
                } else if (ifc.getSecurityMethod().equals(securityMethod)
                        && ifc.getAccessURL().getURL().getProtocol().equals(scheme)) {
                    baseURLs.add(ifc.getAccessURL().getURL());
                }
            }
        } catch (IOException e) {
            throw new IllegalStateException("Error creating transfer urls", e);
        }
        return baseURLs;
    }

}
