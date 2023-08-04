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

package org.opencadc.vospace.server.actions;

import ca.nrc.cadc.net.ResourceNotFoundException;
import ca.nrc.cadc.net.TransientException;
import ca.nrc.cadc.rest.RestAction;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.security.AccessControlException;
import javax.naming.Context;
import javax.naming.InitialContext;
import org.apache.log4j.Logger;
import org.opencadc.vospace.LinkingException;
import org.opencadc.vospace.Node;
import org.opencadc.vospace.VOS;
import org.opencadc.vospace.io.JsonNodeWriter;
import org.opencadc.vospace.io.NodeParsingException;
import org.opencadc.vospace.io.NodeWriter;
import org.opencadc.vospace.server.LocalServiceURI;
import org.opencadc.vospace.server.NodePersistence;
import org.opencadc.vospace.server.auth.VOSpaceAuthorizer;


/**
 * Abstract class encapsulating the behaviour of an action on a Node.  Clients
 * must ensure that setVosURI(), setNodeXML(), setVOSpaceAuthorizer(), and
 * setNodePersistence() are called before using any concrete implementations of
 * this class.
 *
 * @author majorb
 * @author adriand
 */
public abstract class NodeAction extends RestAction {
    protected static Logger log = Logger.getLogger(NodeAction.class);

    // query form parameter names
    public static final String QUERY_PARAM_VIEW = "view";
    public static final String QUERY_PARAM_URI = "uri";
    public static final String QUERY_PARAM_LIMIT = "limit";
    public static final String QUERY_PARAM_SORT_KEY = "sort";
    public static final String QUERY_PARAM_ORDER_KEY = "order";

    private static final String DEFAULT_FORMAT = "text/xml";
    private static final String JSON_FORMAT = "application/json";

    // some subclasses may need to determine hostname, request path, etc
    protected VOSpaceAuthorizer voSpaceAuthorizer;
    protected NodePersistence nodePersistence;
    protected String nodePath;
    protected String stylesheetReference;
    protected String detailLevel;
    protected boolean resolveMetadata = true;
    protected LocalServiceURI localServiceURI;

    protected NodeAction() {
        super();
    }

    /**
     * Set the authorizer to be used by this action.
     *
     * @param voSpaceAuthorizer VOSpace authorizer
     */
    public void setVOSpaceAuthorizer(VOSpaceAuthorizer voSpaceAuthorizer) {
        this.voSpaceAuthorizer = voSpaceAuthorizer;
    }

    /**
     * Set the persistence to be used by this action.
     *
     * @param nodePersistence persistence to be used
     */
    public void setNodePersistence(NodePersistence nodePersistence) {
        this.nodePersistence = nodePersistence;
    }

    /**
     * Set the stylesheet reference.
     *
     * @param stylesheetReference The URI reference string to the stylesheet
     *                            location.
     */
    public void setStylesheetReference(String stylesheetReference) {
        this.stylesheetReference = stylesheetReference;
    }

    /**
     * Set the detail level.
     *
     * @param detailLevel The value.
     */
    public void setDetailLevel(String detailLevel) {
        this.detailLevel = detailLevel;
    }

    /**
     * Set the value for resolve metadata.
     *
     * @param resolveMetadata The value.
     */
    public void setResolveMetadata(boolean resolveMetadata) {
        this.resolveMetadata = resolveMetadata;
    }

    protected String getMediaType() {
        String mediaType = DEFAULT_FORMAT;
        if (syncInput.getParameter("Accept") != null) {
            mediaType = syncInput.getParameter("Accept");
            if (!DEFAULT_FORMAT.equalsIgnoreCase(mediaType) && !JSON_FORMAT.equalsIgnoreCase(mediaType)) {
                throw new IllegalArgumentException("Media type " + mediaType + " not supported");
            }
        }
        return mediaType;
    }

    protected NodeWriter getNodeWriter() {
        String mt = getMediaType();
        if (JSON_FORMAT.equals(mt)) {
            return new JsonNodeWriter();
        }
        return new NodeWriter();
    }

    /*
    protected AbstractView getView() throws Exception {
        if (syncInput.getParameter(QUERY_PARAM_VIEW) == null) {
            return null;
        }

        URI viewReference = URI.create(syncInput.getParameter(QUERY_PARAM_VIEW));

        // the default view is the same as no view
        if (viewReference.equals(VOS.VIEW_DEFAULT)) {
            return null;
        }

        final Views views = new Views();
        AbstractView view = views.getView(viewReference);

        if (view == null) {
            throw new UnsupportedOperationException(
                    "No view configured matching reference: " + viewReference);
        }
        view.setNodePersistence(nodePersistence);
        view.setVOSpaceAuthorizer(voSpaceAuthorizer);

        return view;
    }
    */

    /**
     * Perform the action for which the subclass was designed.
     *
     * @param clientNode the node supplied by the client (may be null)
     * @param serverNode the persistent node returned from doAuthorizationCheck
     */
    protected abstract void performNodeAction(Node clientNode, Node serverNode)
            throws Exception;

    /**
     * Given the node URI and XML, return the Node object specified
     * by the client.
     *
     * @return the default implementation returns null
     * @throws URISyntaxException   if URI Syntax parsing problem
     * @throws NodeParsingException if Node parsing problem
     * @throws IOException          if generic IO problem
     */
    protected abstract Node getClientNode()
            throws URISyntaxException, NodeParsingException, IOException;

    /**
     * Perform an authorization check for the given node and return (if applicable)
     * the persistent version of the Node.
     *
     * @return the applicable persistent (server) Node
     * @throws AccessControlException    if permission is denied
     * @throws ResourceNotFoundException if the target node does not exist
     * @throws LinkingException          if a container link in the path could not be resolved
     */
    protected abstract Node doAuthorizationCheck()
            throws AccessControlException, ResourceNotFoundException, LinkingException, TransientException;

    /**
     * Entry point in performing the steps of a Node Action.  This includes:
     *
     * <p>Calling abstract method getClientNode()
     * Calling abstract method doAuthorizationCheck()
     * Calling abstract method performNodeAction()
     */
    @Override
    public void doAction() throws Exception {
        //TODO
        //String stylesheetReference = context.getParameters().getFirstValue(BeanUtil.VOS_STYLESHEET_REFERENCE);
        //context.getAttributes().put(BeanUtil.VOS_STYLESHEET_REFERENCE, stylesheetReference);

        String jndiNodePersistence = componentID + ".nodePersistence";
        try {
            Context ctx = new InitialContext();
            setNodePersistence((NodePersistence) ctx.lookup(jndiNodePersistence));
            localServiceURI = new LocalServiceURI(nodePersistence.getResourceID());
        } catch (Exception oops) {
            log.error("No NodePersistence implementation found with JNDI key " + jndiNodePersistence, oops);
        }

        VOSpaceAuthorizer authorizer = new VOSpaceAuthorizer(true);
        authorizer.setNodePersistence(nodePersistence);
        setVOSpaceAuthorizer(authorizer);
        nodePath = syncInput.getPath();

        // Create the client version of the node to be used for the operation
        Node clientNode = getClientNode();
        if (clientNode != null) {
            log.debug("client node: " + clientNode.getName());
        } else {
            log.debug("no client node");
        }

        // perform the authorization check
        long start = System.currentTimeMillis();
        Node serverNode = doAuthorizationCheck();
        long end = System.currentTimeMillis();
        log.debug("doAuthorizationCheck() elapsed time: " + (end - start) + "ms");
        log.debug("doAuthorizationCheck() returned server node: " + serverNode.getName());

        // perform the node action
        start = System.currentTimeMillis();
        performNodeAction(clientNode, serverNode);
        end = System.currentTimeMillis();
        log.debug("performNodeAction() elapsed time: " + (end - start) + "ms");
    }

}
