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

package org.opencadc.vospace.server.actions;

import ca.nrc.cadc.auth.AuthenticationUtil;
import ca.nrc.cadc.io.ResourceIterator;
import ca.nrc.cadc.net.HttpTransfer;
import ca.nrc.cadc.util.StringUtil;
import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.URI;
import javax.security.auth.Subject;
import org.apache.log4j.Logger;
import org.opencadc.vospace.ContainerNode;
import org.opencadc.vospace.Node;
import org.opencadc.vospace.NodeProperty;
import org.opencadc.vospace.VOS;
import org.opencadc.vospace.VOSURI;
import org.opencadc.vospace.io.NodeWriter;
import org.opencadc.vospace.server.NodeFault;
import org.opencadc.vospace.server.PathResolver;

/**
 * Class to perform the retrieval of a Node.
 *
 * @author majorb
 * @author adriand
 */
public class GetNodeAction extends NodeAction {
    protected static Logger log = Logger.getLogger(GetNodeAction.class);
    
    // query form parameter names
    public static final String QUERY_PARAM_DETAIL = "detail";
    public static final String QUERY_PARAM_VIEW = "view";
    public static final String QUERY_PARAM_URI = "uri";
    public static final String QUERY_PARAM_LIMIT = "limit";
    public static final String QUERY_PARAM_SORT_KEY = "sort";
    public static final String QUERY_PARAM_ORDER_KEY = "order";

    // computed props
    private static final NodeProperty PROP_READABLE = new NodeProperty(VOS.PROPERTY_URI_READABLE, Boolean.TRUE.toString());
    private static final NodeProperty PROP_WRITABLE = new NodeProperty(VOS.PROPERTY_URI_WRITABLE, Boolean.TRUE.toString());
    
    static {
        PROP_READABLE.readOnly = true;
        PROP_WRITABLE.readOnly = true;
    }
    
    /**
     * Basic empty constructor.
     */
    public GetNodeAction() {
    }

    @Override
    public void doAction() throws Exception {
        VOSURI target = getTargetURI();

        String view = syncInput.getParameter(QUERY_PARAM_VIEW);
        if ("data".equalsIgnoreCase(view)) {
            // makes the assumption that /files endpoint is a sibling of /nodes
            URI requestURI = URI.create(syncInput.getRequestURI());
            String filesPath = syncInput.getContextPath() + "/files" + target.getPath();
            // query params are not passed through
            URI filesURI = new URI(requestURI.getScheme(), requestURI.getHost(), filesPath, null);
            String location = filesURI.toASCIIString();
            log.debug("Redirecting view=data request to " + location);
            syncOutput.setHeader("Location", location);
            syncOutput.setCode(HttpURLConnection.HTTP_SEE_OTHER);
            return;
        }

        final String detailLevel = syncInput.getParameter(QUERY_PARAM_DETAIL);
        
        // get parent node
        PathResolver pathResolver = new PathResolver(nodePersistence, voSpaceAuthorizer);
        Node serverNode = pathResolver.getNode(target.getPath(), false);
        if (serverNode == null) {
            throw NodeFault.NodeNotFound.getStatus(target.toString());
        }
        log.debug("found: " + target + " as " + serverNode);
        
        if (serverNode instanceof ContainerNode) {
            ContainerNode node = (ContainerNode) serverNode;
            log.debug("node: " + node);

            // Check for read permission to list child nodes.
            Subject subject = AuthenticationUtil.getCurrentSubject();
            if (voSpaceAuthorizer.hasSingleNodeReadPermission(node, subject)) {
                // TBD: sorting parameters
                String sortParam = syncInput.getParameter(QUERY_PARAM_SORT_KEY);
                if (sortParam != null) {
                    throw new UnsupportedOperationException("sort by " + sortParam);
                }

                String sortOrderParam = syncInput.getParameter(QUERY_PARAM_ORDER_KEY);
                if (sortOrderParam != null) {
                    throw new UnsupportedOperationException("sort order " + sortOrderParam);
                }

                // paging parameters
                String pageLimitString = syncInput.getParameter(QUERY_PARAM_LIMIT);
                String startURI = syncInput.getParameter(QUERY_PARAM_URI);

                Integer pageLimit = null;
                if (pageLimitString != null) {
                    try {
                        pageLimit = Integer.parseInt(pageLimitString);
                        if (pageLimit < 0) {
                            throw new NumberFormatException();
                        }
                    } catch (NumberFormatException e) {
                        throw NodeFault.InvalidArgument.getStatus("value for limit must be a positive integer.");
                    }
                }

                String pageStart = null;
                if (StringUtil.hasText(startURI)) {
                    VOSURI vuri = new VOSURI(startURI);
                    String parentPath = vuri.getParent();
                    log.debug("pagination: target.path=" + target.getPath() + " start.parentPath=" + parentPath);
                    if (!target.getPath().equals(parentPath)) {
                        throw NodeFault.InvalidURI.getStatus(
                                "uri parameter (" + vuri.toString() + ") not a child of target uri ("
                                        + getTargetURI() + ").");
                    }
                    pageStart = vuri.getName();
                }

                long start = System.currentTimeMillis();
                log.debug(String.format("get children of %s: start=[%s] limit=[%s] detail=%s", target.getPath(), startURI, pageLimit, detailLevel));
                try {
                    ResourceIterator<Node> ci = nodePersistence.iterator(node, pageLimit, pageStart);
                    if (VOS.Detail.max.getValue().equals(detailLevel)) {
                        node.childIterator = new TagChildAccessRightsWrapper(ci, subject);
                    } else {
                        node.childIterator = ci;
                    }
                } catch (UnsupportedOperationException ex) {
                    throw NodeFault.OptionNotSupported.getStatus(ex.getMessage());
                }

                long end = System.currentTimeMillis();
                long dt = (end - start);
                log.debug("nodePersistence.iterator() elapsed time: " + dt + "ms");
            }
        }

        // get the properties if no detail level is specified (null) or if the
        // detail level is something other than 'min'.
        if (!VOS.Detail.min.getValue().equals(detailLevel)) {
            nodePersistence.getProperties(serverNode);

            if (VOS.Detail.max.getValue().equals(detailLevel)) {
                // to get here the node must have been readable so tag it as such
                serverNode.getProperties().add(PROP_READABLE);
                
                Subject subject = AuthenticationUtil.getCurrentSubject();
                if (voSpaceAuthorizer.hasSingleNodeWritePermission(serverNode, subject)) {
                    serverNode.getProperties().add(PROP_WRITABLE);
                }
            }
        }

        final NodeWriter nodeWriter = getNodeWriter();
        VOS.Detail detail = VOS.Detail.max;
        if (VOS.Detail.min.getValue().equals(detailLevel)) {
            // TODO: what about props that are fields in Node?
            detail = VOS.Detail.min;
        }

        syncOutput.setCode(200);
        syncOutput.setHeader(HttpTransfer.CONTENT_TYPE, getMediaType());
        // TODO: should the VOSURI in the output target or actual? eg resolveLinks=true
        nodeWriter.write(localServiceURI.getURI(serverNode), serverNode, syncOutput.getOutputStream(), detail);
    }
    
    private class TagChildAccessRightsWrapper implements ResourceIterator<Node> {
        private final ResourceIterator<Node> inner;
        private final Subject caller;

        public TagChildAccessRightsWrapper(ResourceIterator<Node> inner, Subject caller) {
            this.inner = inner;
            this.caller = caller;
        }
        
        @Override
        public boolean hasNext() {
            return inner.hasNext();
        }

        @Override
        public Node next() {
            Node ret = inner.next();
            if (voSpaceAuthorizer.hasSingleNodeReadPermission(ret, caller)) {
                ret.getProperties().add(PROP_READABLE);
            }
            if (voSpaceAuthorizer.hasSingleNodeWritePermission(ret, caller)) {
                ret.getProperties().add(PROP_WRITABLE);
            }
            return ret;
        }

        @Override
        public void close() throws IOException {
            inner.close();
        }
    }
}
