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

package ca.nrc.cadc.vos.server.web.action;

import java.io.IOException;
import java.net.URLEncoder;

import org.apache.log4j.Logger;

import ca.nrc.cadc.auth.AuthMethod;
import ca.nrc.cadc.auth.AuthenticationUtil;
import ca.nrc.cadc.reg.Capabilities;
import ca.nrc.cadc.reg.Capability;
import ca.nrc.cadc.reg.Interface;
import ca.nrc.cadc.reg.Standards;
import ca.nrc.cadc.reg.client.RegistryClient;
import ca.nrc.cadc.rest.InlineContentHandler;
import ca.nrc.cadc.rest.RestAction;
import ca.nrc.cadc.vos.DataNode;
import ca.nrc.cadc.vos.Direction;
import ca.nrc.cadc.vos.Node;
import ca.nrc.cadc.vos.NodeNotFoundException;
import ca.nrc.cadc.vos.VOS;
import ca.nrc.cadc.vos.VOSURI;
import ca.nrc.cadc.vos.server.NodePersistence;
import ca.nrc.cadc.vos.server.PathResolver;
import ca.nrc.cadc.vos.server.VOSpacePluginFactory;
import ca.nrc.cadc.vos.server.auth.VOSpaceAuthorizer;
import ca.nrc.cadc.vos.server.util.BeanUtil;
import java.net.URI;

/**
 * This class issues synctrans download URLs to data nodes.  It is a convenience
 * endpoint for predictable file downloading.  It is not currently part of the
 * VOSpace (2.1) specification but could be considered in future versions.
 * 
 * @author majorb
 *
 */
public class FilesAction extends RestAction {
    
    protected static Logger log = Logger.getLogger(FilesAction.class);
    
    public FilesAction() {
        super();
    }

    @Override
    protected InlineContentHandler getInlineContentHandler() {
        return null;
    }

    @Override
    public void doAction() throws Exception {
  
        String path = syncInput.getPath();
        log.debug("path: " + path);
        if (path == null) {
            sendError(404, "Not Found");
            return;
        }
        
        String vosURIPrefix = BeanUtil.getVosUriBase();
        String nodeURI = vosURIPrefix + "/" + path;
        VOSURI vosURI = new VOSURI(nodeURI);
        
        VOSpacePluginFactory pluginFactory = new VOSpacePluginFactory();
        NodePersistence np = pluginFactory.createNodePersistence();
        VOSpaceAuthorizer authorizer = new VOSpaceAuthorizer(true);
        authorizer.setNodePersistence(np);
        PathResolver resolver = new PathResolver(np, false);
        
        Node node = null;
        try {
            node = resolver.resolveWithReadPermissionCheck(vosURI, authorizer, true);
        } catch (NodeNotFoundException e) {
            sendError(404, "Not Found");
            return;
        }
        
        log.debug("node: " + node);
        // only data nodes (link nodes resolved above)
        if (!(node instanceof DataNode)) {
            sendError(400, "Not a DataNode");
            return;
        }
        
        // create the synctrans url
        RegistryClient regClient = new RegistryClient();
        AuthMethod authMethod = AuthenticationUtil.getAuthMethod(AuthenticationUtil.getCurrentSubject());
        String requestProtocol = syncInput.getProtocol();
        
        Capabilities caps = regClient.getCapabilities(vosURI.getServiceURI());
        Capability cap = caps.findCapability(Standards.VOSPACE_SYNC_21);
        Interface ifc = null;
        // TODO: it looks like this finds the last matching interface rather than the first... ok for only interface
        for (Interface next : cap.getInterfaces()) {
            for (URI sm : next.getSecurityMethods()) {
                AuthMethod am = Standards.getAuthMethod(sm);
                if (authMethod.equals(am)) {
                    if (ifc == null) {
                        ifc = next;
                    } else if (requestProtocol.equals(next.getAccessURL().getURL().getProtocol())) {
                        // prefer an interface with matching protocol
                        ifc = next;
                    }
                }
            }
        }
        
        log.debug("interface: " + ifc);
        if (ifc == null) {
            throw new RuntimeException("BUG: no interfaces match synctrans request");
        }
        
        String protocol = VOS.PROTOCOL_HTTPS_GET;
        if (ifc.getAccessURL().getURL().getProtocol().equals("http")) {
            protocol = VOS.PROTOCOL_HTTP_GET;
        }
        
        StringBuilder sb = new StringBuilder(ifc.getAccessURL().getURL().toString());
        sb.append("?");
        sb.append("TARGET=").append(URLEncoder.encode(vosURI.getURI().toString(), "UTF-8"));
        sb.append("&");
        sb.append("DIRECTION=").append(Direction.pullFromVoSpaceValue);
        sb.append("&");
        sb.append("PROTOCOL=").append(URLEncoder.encode(protocol, "UTF-8"));
        
        log.debug("redirect: " + sb.toString());
        syncOutput.setHeader("Location", sb.toString());
        syncOutput.setCode(303);
        
    }
    
    private void sendError(int code, String msg) throws IOException {
        syncOutput.setCode(code);
        syncOutput.setHeader("Content-Type", "text/plain");
        syncOutput.getOutputStream().write(msg.getBytes());
    }

}
