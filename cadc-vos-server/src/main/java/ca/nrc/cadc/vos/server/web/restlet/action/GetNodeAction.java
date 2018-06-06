/*
 ************************************************************************
 *******************  CANADIAN ASTRONOMY DATA CENTRE  *******************
 **************  CENTRE CANADIEN DE DONNÉES ASTRONOMIQUES  **************
 *
 *  (c) 2018.                            (c) 2018.
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

package ca.nrc.cadc.vos.server.web.restlet.action;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.security.AccessControlException;

import org.apache.log4j.Logger;
import org.restlet.data.Reference;

import ca.nrc.cadc.net.TransientException;
import ca.nrc.cadc.util.ObjectUtil;
import ca.nrc.cadc.util.StringUtil;
import ca.nrc.cadc.vos.ContainerNode;
import ca.nrc.cadc.vos.LinkingException;
import ca.nrc.cadc.vos.Node;
import ca.nrc.cadc.vos.NodeNotFoundException;
import ca.nrc.cadc.vos.NodeParsingException;
import ca.nrc.cadc.vos.NodeProperty;
import ca.nrc.cadc.vos.NodeWriter;
import ca.nrc.cadc.vos.VOS;
import ca.nrc.cadc.vos.VOSURI;
import ca.nrc.cadc.vos.server.AbstractView;
import ca.nrc.cadc.vos.server.PathResolver;
import ca.nrc.cadc.vos.server.PersistenceOptions;
import ca.nrc.cadc.vos.server.web.representation.NodeOutputRepresentation;
import ca.nrc.cadc.vos.server.web.representation.ViewRepresentation;

/**
 * Class to perform the retrieval of a Node.
 *
 * @author majorb
 */
public class GetNodeAction extends NodeAction
{

    protected static Logger log = Logger.getLogger(GetNodeAction.class);

    /**
     * Basic empty constructor.
     */
    public GetNodeAction()
    {
    }

    @Override
    protected Node getClientNode() throws URISyntaxException,
            NodeParsingException, IOException
    {
        // No client node in a GET
        return null;
    }

    @Override
    public Node doAuthorizationCheck()
        throws AccessControlException, FileNotFoundException, LinkingException, TransientException
    {
        // resolve any container links
        PathResolver pathResolver = new PathResolver(nodePersistence, resolveMetadata);
        try
        {
            return pathResolver.resolveWithReadPermissionCheck(vosURI,
                    partialPathVOSpaceAuthorizer, false);
        }
        catch (NodeNotFoundException e)
        {
            throw new FileNotFoundException(e.getMessage());
        }
    }

    @Override
    public NodeActionResult performNodeAction(Node clientNode, Node serverNode)
        throws URISyntaxException, FileNotFoundException, TransientException
    {
        long start;
        long end;

        System.out.println("detail: " + detailLevel);
        System.out.println("resolve: " + resolveMetadata);

        if (serverNode instanceof ContainerNode)
        {
            // Paging parameters
            String startURI = queryForm.getFirstValue(QUERY_PARAM_URI);
            String pageLimitString = queryForm.getFirstValue(QUERY_PARAM_LIMIT);

            // Sorting parameters
            String sortParam = queryForm.getFirstValue(QUERY_PARAM_SORT_KEY);
            URI sortParamURI = null;

            // Validate sortCol passed in against values in cadc-vos/vos.java
            if (sortParam != null) {
                switch (sortParam) {
                    case VOS.PROPERTY_URI_DATE:
                    case VOS.PROPERTY_URI_CONTENTLENGTH: {
                        sortParamURI = URI.create(sortParam);
                        break;
                    }
                    default:
                        throw new IllegalArgumentException("Sort by " + sortParam + " not supported.");
                }
            }

            // Asc/Desc order parameter
            String sortOrderParam = queryForm.getFirstValue(QUERY_PARAM_ORDER_KEY);
            Boolean sortAsc = null;
            if (sortOrderParam != null) {
                if (sortOrderParam.equalsIgnoreCase("asc")) {
                    sortAsc = true;
                } else if (sortOrderParam.equalsIgnoreCase("desc")) {
                    sortAsc = false;
                } else {
                    throw new IllegalArgumentException("Sort order parameter should be asc | desc");
                }
            }

            ContainerNode cn = (ContainerNode) serverNode;
            VOSURI startURIObject = null;

            // parse the pageLimit
            Integer pageLimit = null;
            if (pageLimitString != null)
            {
                try
                {
                    pageLimit = new Integer(pageLimitString);
                    if (pageLimit < 1) {
                        throw new NumberFormatException();
                    }
                }
                catch (NumberFormatException e)
                {
                    throw new IllegalArgumentException("value for limit must be a positive integer.");
                }
            }

            // validate startURI
            if (StringUtil.hasText(startURI))
            {
                startURIObject = new VOSURI(startURI);
                if (!vosURI.equals(startURIObject.getParentURI()))
                {
                    throw new IllegalArgumentException("uri parameter not a child of target uri.");
                }
            }

            // get the children as requested
            start = System.currentTimeMillis();
            // request for a subset of children
            if (sortParamURI == null && sortAsc == null) {
                nodePersistence.getChildren(cn, startURIObject, pageLimit, resolveMetadata);
            } else if (nodePersistence instanceof PersistenceOptions) {
                // non-standard sorting options
                ((PersistenceOptions) nodePersistence).getChildren(
                        cn, startURIObject, pageLimit, sortParamURI, sortAsc, resolveMetadata);
            } else {
                throw new IllegalArgumentException("Alternate sorting options not supported.");
            }
            log.debug(String.format(
                "Get children on resolveMetadata=[%b] returned [%s] nodes with startURI=[%s], pageLimit=[%s].",
                    resolveMetadata, cn.getNodes().size(), startURI, pageLimit));
            
            end = System.currentTimeMillis();
            log.debug("nodePersistence.getChildren() elapsed time: " + (end - start) + "ms");

            if (VOS.Detail.max.getValue().equals(detailLevel))
            {
                // add a property to child nodes if they are visible to
                // this request
                doTagChildrenAccessRights(cn);
            }
        }

        start = System.currentTimeMillis();

        // get the properties if no detail level is specified (null) or if the
        // detail level is something other than 'min'.
        if (!VOS.Detail.min.getValue().equals(detailLevel))
        {
            nodePersistence.getProperties(serverNode);

            if (VOS.Detail.max.getValue().equals(detailLevel))
            {
                doTagReadable(serverNode);
                doTagWritable(serverNode);
            }
        }

        end = System.currentTimeMillis();
        log.debug("nodePersistence.getProperties() elapsed time: " + (end - start) + "ms");

        AbstractView view;
        String viewReference = queryForm.getFirstValue(QUERY_PARAM_VIEW);
        try
        {
            view = getView();
        }
        catch (Exception ex)
        {
            log.error("failed to load view: " + viewReference, ex);
            // this should generate an InternalFault in NodeAction
            throw new RuntimeException("view was configured but failed to load: " + viewReference);
        }

        if (view == null)
        {
            // no view specified or found--return the xml representation
            final NodeWriter nodeWriter = getNodeWriter();
            nodeWriter.setStylesheetURL(getStylesheetURL());

            // clear the properties from server node if the detail
            // level is set to 'min'
            if (VOS.Detail.min.getValue().equals(detailLevel))
                serverNode.getProperties().clear();

            // if the request has gone through a link, change the
            // node paths back to be the 'unresolved path'
            if (!vosURI.getPath().equals(serverNode.getUri().getPath()))
            {
                log.debug("returning node paths back to one that include a link");
                unresolveNodePaths(vosURI, serverNode);
            }

            return new NodeActionResult(new NodeOutputRepresentation(serverNode, nodeWriter, getMediaType()));
        }
        else
        {
            Reference ref = request.getOriginalRef();
            URL url = ref.toUrl();
            view.setNode(serverNode, viewReference, url);
            URL redirectURL = view.getRedirectURL();
            if (redirectURL != null)
            {
                return new NodeActionResult(redirectURL);
            }
            else
            {
                // return a representation for the view
                return new NodeActionResult(new ViewRepresentation(view));
            }
        }
    }

    /**
     * Look for the stylesheet URL in the request context.
     * @return      The String URL of the stylesheet for this action.
     *              Null if no reference is provided.
     */
    public String getStylesheetURL()
    {
        log.debug("Stylesheet Reference is: " + stylesheetReference);
        if (stylesheetReference != null)
        {
            String scheme = request.getHostRef().getScheme();
            String server = request.getHostRef().getHostDomain();
            StringBuilder url = new StringBuilder();
            url.append(scheme);
            url.append("://");
            url.append(server);
            if (!stylesheetReference.startsWith("/"))
                url.append("/");
            url.append(stylesheetReference);
            return url.toString();
        }
        return null;
    }

    private void unresolveNodePaths(VOSURI vosURI, Node node) throws URISyntaxException
    {
        try
        {
            // change the target node
            ObjectUtil.setField((Node) node, vosURI, "uri");

            // change any children
            if (node instanceof ContainerNode)
            {
                ContainerNode containerNode = (ContainerNode) node;
                VOSURI childURI = null;
                for (Node child : containerNode.getNodes())
                {
                    childURI = new VOSURI(vosURI.toString() + "/" + child.getName());
                    ObjectUtil.setField(child, childURI, "uri");
                }
            }
        }
        catch (Exception e)
        {
            log.debug("failed to unresolve node paths", e);
            throw new RuntimeException(e);
        }
    }

    private void doTagChildrenAccessRights(ContainerNode cn)
    {
        for (final Node n : cn.getNodes())
        {
            doTagReadable(n);
            doTagWritable(n);
        }
    }

    /**
     * Tag the given node with a 'readable' property to indicate that it is
     * viewable (readable) by the requester.
     * @param n     The Node to check.
     */
    private void doTagReadable(final Node n)
    {
        final NodeProperty canReadProperty =
                new NodeProperty(VOS.PROPERTY_URI_READABLE,
                                 Boolean.TRUE.toString());
        canReadProperty.setReadOnly(true);

        try
        {
            voSpaceAuthorizer.getReadPermission(n);
            n.getProperties().add(canReadProperty);
        }
        catch (AccessControlException e)
        {
            // no read access, continue
        }
        catch (Exception e)
        {
            // error checking access, log a warning
            log.warn("Failed to check read permission", e);
        }
    }

    /**
     * Tag the given node with a 'writable' property to indicate that it is
     * updatable (writable) by the requester.
     * @param n     The Node to check.
     */
    private void doTagWritable(final Node n)
    {
        final NodeProperty canWriteProperty =
                new NodeProperty(VOS.PROPERTY_URI_WRITABLE,
                                 Boolean.TRUE.toString());
        canWriteProperty.setReadOnly(true);

        try
        {
            voSpaceAuthorizer.getWritePermission(n);
            n.getProperties().add(canWriteProperty);
        }
        catch (AccessControlException e)
        {
            // no write access, continue
        }
        catch (Exception e)
        {
            // error checking access, log a warning
            log.warn("Failed to check write permission", e);
        }
    }
}
