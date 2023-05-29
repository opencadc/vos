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

package org.opencadc.vospace.client;

import ca.nrc.cadc.auth.AuthMethod;
import ca.nrc.cadc.auth.AuthenticationUtil;
import ca.nrc.cadc.net.FileContent;
import ca.nrc.cadc.net.HttpDelete;
import ca.nrc.cadc.net.HttpGet;
import ca.nrc.cadc.net.HttpPost;
import ca.nrc.cadc.net.HttpTransfer;
import ca.nrc.cadc.net.HttpUpload;
import ca.nrc.cadc.net.NetUtil;
import ca.nrc.cadc.net.OutputStreamWrapper;
import ca.nrc.cadc.net.ResourceAlreadyExistsException;
import ca.nrc.cadc.reg.Standards;
import ca.nrc.cadc.reg.client.RegistryClient;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.io.StringWriter;
import java.io.Writer;
import java.net.MalformedURLException;
import java.net.URI;
import java.net.URL;
import java.nio.charset.Charset;
import java.security.AccessControlException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import javax.security.auth.Subject;

import org.apache.log4j.Logger;
import org.opencadc.vospace.ContainerNode;
import org.opencadc.vospace.Node;
import org.opencadc.vospace.NodeNotFoundException;
import org.opencadc.vospace.NodeProperty;
import org.opencadc.vospace.VOS;
import org.opencadc.vospace.VOSURI;
import org.opencadc.vospace.View;
import org.opencadc.vospace.io.NodeParsingException;
import org.opencadc.vospace.io.NodeReader;
import org.opencadc.vospace.io.NodeWriter;
import org.opencadc.vospace.transfer.Direction;
import org.opencadc.vospace.transfer.Protocol;
import org.opencadc.vospace.transfer.Transfer;
import org.opencadc.vospace.transfer.TransferParsingException;
import org.opencadc.vospace.transfer.TransferReader;
import org.opencadc.vospace.transfer.TransferWriter;

/**
 * VOSpace client library. This implementation
 *
 * @author zhangsa
 */
public class VOSpaceClient {
    private static Logger log = Logger.getLogger(VOSpaceClient.class);

    public static final String CR = System.getProperty("line.separator"); // OS independant new line

    private URI serviceID;
    boolean schemaValidation;

    /**
     * Constructor. XML Schema validation is enabled by default.
     *
     * @param serviceID
     */
    public VOSpaceClient(URI serviceID) {
        this(serviceID, true);
    }

    /**
     * Constructor. XML schema validation may be disabled, in which case the client
     * is likely to fail in horrible ways (e.g. NullPointerException) if it receives
     * invalid VOSpace (node or transfer) or UWS (job) documents. However, performance
     * may be improved.
     *
     * @param serviceID
     * @param enableSchemaValidation
     */
    public VOSpaceClient(URI serviceID, boolean enableSchemaValidation) {
        this.schemaValidation = enableSchemaValidation;
        this.serviceID = serviceID;
    }

    /**
     * Create the specified node. If the parent (container) nodes do not exist, they
     * will also be created.
     *
     * @param node
     * @return the created node
     */
    public Node createNode(VOSURI vosURI, Node node) {
        return this.createNode(vosURI, node, true);
    }

    /**
     * Create the specified node. If the parent (container) nodes do not exist, they
     * will also be created.
     *
     * @param node
     * @param checkForDuplicate If true, throw duplicate node exception if node already exists.
     * @return the created node
     */
    public Node createNode(VOSURI vosURI, Node node, boolean checkForDuplicate) {
        Node rtnNode = null;
        log.debug("createNode(), node=" + vosURI + ", checkForDuplicate=" + checkForDuplicate);
        try {
            VOSURI parentURI = vosURI.getParentURI();
            if (parentURI == null) {
                throw new RuntimeException("parent (root node) not found and cannot create: " + vosURI);
            }

            ContainerNode parent;
            try {
                // check for existence --get the node with minimal content.  Get the target child
                // if we need to check for duplicates.
                Node parentNode;
                if (checkForDuplicate) {
                    parentNode = this.getNode(parentURI.getPath(), "detail=min&limit=1&uri="
                        + NetUtil.encode(vosURI.toString()));
                } else {
                    parentNode = this.getNode(parentURI.getPath(), "detail=min&limit=0");
                }

                log.debug("found parent: " + parentURI);
                if (parentNode instanceof ContainerNode) {
                    parent = (ContainerNode) parentNode;
                } else {
                    throw new IllegalArgumentException("cannot create a child, parent is a "
                                                           + parentNode.getClass().getSimpleName());
                }
            } catch (NodeNotFoundException ex) {
                // if parent does not exist, just create it!!
                log.info("creating parent: " + parentURI);
                ContainerNode cn = new ContainerNode(parentURI.getName(), false);
                parent = (ContainerNode) createNode(parentURI, cn, false);
            }

            // check if target already exists: also could fail like this below due to race condition
            if (checkForDuplicate) {
                for (Node n : parent.getNodes()) {
                    if (n.getName().equals(node.getName())) {
                        throw new IllegalArgumentException("DuplicateNode: " + vosURI);
                    }
                }
            }
            URL vospaceURL = lookupServiceURL(Standards.VOSPACE_NODES_20);

            URL url = new URL(vospaceURL.toExternalForm() + vosURI.getPath());
            log.debug("createNode(), URL=" + url);

            NodeOutputStream out = new NodeOutputStream(vosURI, node);
            HttpUpload put = new HttpUpload(out, url);
            put.setRequestProperty(HttpTransfer.CONTENT_TYPE, "text/xml");

            try {
                put.prepare();
            } catch (Exception ex) {
                VOSClientUtil.checkFailure(ex);
            }

            if (put.failure != null) {
                // Errors not blurted as exceptions by
                // HttpUpload will be stored in 'failure.'
                // These need to be reported as well
                log.debug("HttpUpload failure for createNode: ", put.failure);
                VOSClientUtil.checkFailure(put.failure);
            }

            NodeReader nodeReader = new NodeReader(schemaValidation);
            NodeReader.NodeReaderResult result = nodeReader.read(put.getInputStream());
            rtnNode = result.node;
            log.debug("createNode, created node: " + rtnNode);
        } catch (IOException e) {
            log.debug("failed to create node", e);
            throw new IllegalStateException("failed to create node", e);
        } catch (NodeParsingException e) {
            log.debug("failed to create node", e);
            throw new IllegalStateException("failed to create node", e);
        } catch (NodeNotFoundException e) {
            log.debug("failed to create node", e);
            throw new IllegalStateException("Node not found", e);
        } catch (ResourceAlreadyExistsException e) {
            log.debug("failed to create node", e);
            throw new IllegalStateException("Node already exists", e);
        }
        return rtnNode;
    }

    /**
     * Get Node.
     *
     * @param path      The path to the Node.
     * @return          The Node instance.
     * @throws NodeNotFoundException when the requested node does not exist on the server
     */
    public Node getNode(String path)
        throws NodeNotFoundException {
        return getNode(path, null);
    }

    /**
     * Get Node.
     *
     * @param path      The path to the Node.
     * @param query     Optional query string
     * @return          The Node instance.
     * @throws NodeNotFoundException when the requested node does not exist on the server
     */
    public Node getNode(String path, String query)
        throws NodeNotFoundException {
        if (path.length() > 0 && !path.startsWith("/")) { // length 0 is root: no /
            path = "/" + path; // must be absolute
        }
        if (query != null) {
            path += "?" + query;
        }

        Node rtnNode = null;
        try {
            URL vospaceURL = lookupServiceURL(Standards.VOSPACE_NODES_20);
            URL url = new URL(vospaceURL.toExternalForm() + path);
            log.debug("getNode(), URL=" + url);

            ByteArrayOutputStream out = new ByteArrayOutputStream();
            HttpGet get = new HttpGet(url, out);

            get.run();

            VOSClientUtil.checkFailure(get.getThrowable());

            String xml = new String(out.toByteArray(), "UTF-8");
            log.debug("node xml:\n" + xml);
                    
            NodeReader nodeReader = new NodeReader(schemaValidation);
            NodeReader.NodeReaderResult result = nodeReader.read(xml);
            rtnNode = result.node;
            log.debug("getNode, returned node: " + rtnNode);
        } catch (IOException ex) {
            log.debug("failed to get node", ex);
            throw new IllegalStateException("failed to get node", ex);
        } catch (NodeParsingException e) {
            log.debug("failed to get node", e);
            throw new IllegalStateException("failed to get node", e);
        } catch (ResourceAlreadyExistsException e) {
            log.debug("failed to get node", e);
            throw new IllegalStateException("failed to get node", e);
        }
        return rtnNode;
    }

    /**
     * Add --recursiveMode option to command line, used by set only, and if set
     * setNode uses a different recursiveMode endpoint.
     */
    /**
     * @param node
     * @return
     */
    public Node setNode(VOSURI vosURI, Node node) {
        Node rtnNode = null;
        try {
            URL vospaceURL = lookupServiceURL(Standards.VOSPACE_NODES_20);
            URL url = new URL(vospaceURL.toExternalForm() + vosURI.getPath());
            log.debug("setNode: " + VOSClientUtil.xmlString(vosURI, node));
            log.debug("setNode: " + url);

            NodeWriter nodeWriter = new NodeWriter();
            StringBuilder nodeXML = new StringBuilder();
            nodeWriter.write(vosURI, node, nodeXML);

            FileContent nodeContent = new FileContent(nodeXML.toString(), "text/xml", Charset.forName("UTF-8"));
            HttpPost httpPost = new HttpPost(url, nodeContent, false);

            try {
                httpPost.prepare();
            } catch (Exception ex) {
                VOSClientUtil.checkFailure(ex);
            }

            NodeReader nodeReader = new NodeReader();
            NodeReader.NodeReaderResult result = nodeReader.read(httpPost.getInputStream());
            rtnNode =  result.node;
        } catch (IOException e) {
            throw new IllegalStateException("failed to set node", e);
        } catch (NodeParsingException e) {
            throw new IllegalStateException("failed to set node", e);
        } catch (NodeNotFoundException e) {
            throw new IllegalStateException("Node not found", e);
        } catch (ResourceAlreadyExistsException e) {
            log.debug("failed to set node", e);
            throw new IllegalStateException("failed to set node", e);
        }
        return rtnNode;
    }

    // create an async transfer job
    public ClientRecursiveSetNode setNodeRecursive(VOSURI vosURI, Node node) {
        try {
            URL vospaceURL = lookupServiceURL(Standards.VOSPACE_NODEPROPS_20);

            //String asyncNodePropsUrl = this.baseUrl + VOSPACE_ASYNC_NODEPROPS_ENDPONT;
            NodeWriter nodeWriter = new NodeWriter();
            Writer stringWriter = new StringWriter();
            nodeWriter.write(vosURI, node, stringWriter);
            //URL postUrl = new URL(asyncNodePropsUrl);

            FileContent nodeContent = new FileContent(stringWriter.toString(), "text/xml", Charset.forName("UTF-8"));
            HttpPost httpPost = new HttpPost(vospaceURL, nodeContent, false);

            httpPost.run();

            VOSClientUtil.checkFailure(httpPost.getThrowable());

            URL jobUrl = httpPost.getRedirectURL();
            log.debug("Job URL is: " + jobUrl.toString());

            // we have only created the job, not run it
            return new ClientRecursiveSetNode(jobUrl, node, schemaValidation);
        } catch (MalformedURLException e) {
            log.debug("failed to create transfer", e);
            throw new RuntimeException(e);
        } catch (IOException e) {
            log.debug("failed to create transfer", e);
            throw new RuntimeException(e);
        } catch (NodeNotFoundException e) {
            log.debug("Node not found", e);
            throw new RuntimeException(e);
        } catch (ResourceAlreadyExistsException e) {
            log.debug("failed to create transfer", e);
            throw new IllegalStateException("failed to create transfer", e);
        }
    }


    /**
     * Negotiate a transfer. The argument transfer specifies the target URI, the
     * direction, the proposed protocols, and an optional view.
     *
     * @param trans
     * @return a negotiated transfer
     */
    public ClientTransfer createTransfer(Transfer trans) {
        if (Direction.pushToVoSpace.equals(trans.getDirection())) {
            return createTransferSync(trans);
        }

        if (Direction.pullFromVoSpace.equals(trans.getDirection())) {
            return createTransferSync(trans);
        }

        if (Direction.BIDIRECTIONAL.equals(trans.getDirection())) {
            return createTransferSync(trans);
        }
        
        return createTransferASync(trans);
    }

    public List<NodeProperty> getProperties() {
        throw new UnsupportedOperationException("Feature under construction.");
    }

    public List<Protocol> getProtocols() {
        throw new UnsupportedOperationException("Feature under construction.");
    }

    public List<View> getViews() {
        throw new UnsupportedOperationException("Feature under construction.");
    }

    /**
     * Remove the Node associated with the given Path.
     * @param path      The path of the Node to delete.
     */
    public void deleteNode(final String path) {
        // length 0 is root: no
        // Path must be absolute
        final String nodePath = (path.length() > 0 && !path.startsWith("/"))
                                ? ("/" + path) : path;
        try {
            final URL vospaceURL = lookupServiceURL(Standards.VOSPACE_NODES_20);
            final URL url = new URL(vospaceURL.toExternalForm() + nodePath);
            final HttpDelete httpDelete = new HttpDelete(url, false);

            httpDelete.run();
            VOSClientUtil.checkFailure(httpDelete.getThrowable());
        } catch (MalformedURLException e) {
            log.debug(String.format("Error creating URL from %s", nodePath));
            throw new RuntimeException(e);
        } catch (NodeNotFoundException e) {
            log.debug("Node not found", e);
            throw new RuntimeException(e);
        } catch (ResourceAlreadyExistsException e) {
            log.debug("failed to delete node", e);
            throw new IllegalStateException("failed to delete node", e);
        }

    }

    protected RegistryClient getRegistryClient() {
        return new RegistryClient();
    }

    protected URL getServiceURL(URI serviceID, URI standard, AuthMethod authMethod) {
        RegistryClient registryClient = new RegistryClient();
        return registryClient.getServiceURL(serviceID, standard, authMethod);
    }

    // create an async transfer job
    private ClientTransfer createTransferASync(Transfer transfer) {
        try {
            URL vospaceURL = lookupServiceURL(Standards.VOSPACE_TRANSFERS_20);
            
            // TODO: figure out if the service supports VOSpace 2.1 transfer documents
            // that include securityMethod under protocol so we can set transfer.version
            // HACK: hard code to 2.1
            transfer.version = VOS.VOSPACE_21;

            TransferWriter transferWriter = new TransferWriter();
            Writer stringWriter = new StringWriter();
            transferWriter.write(transfer, stringWriter);

            FileContent transferContent = new FileContent(stringWriter.toString(), "text/xml", Charset.forName("UTF-8"));
            HttpPost httpPost = new HttpPost(vospaceURL, transferContent, false);

            httpPost.run();

            if (httpPost.getThrowable() != null) {
                log.debug("Unable to post transfer because ", httpPost.getThrowable());
                throw new RuntimeException("Unable to post transfer because " + httpPost.getThrowable().getMessage());
            }

            // Check uws job results
            URL jobUrl = httpPost.getRedirectURL();
            log.debug("Job URL is: " + jobUrl.toString());

            // we have only created the job, not run it
            return new ClientTransfer(jobUrl, transfer, schemaValidation);
        } catch (MalformedURLException e) {
            log.debug("failed to create transfer", e);
            throw new RuntimeException(e);
        } catch (IOException e) {
            log.debug("failed to create transfer", e);
            throw new RuntimeException(e);
        }
    }

    // create a transfer using the sync transfers job resource
    private ClientTransfer createTransferSync(Transfer transfer) {
        try {
            URL vospaceURL = lookupServiceURL(Standards.VOSPACE_SYNC_21);
            //transfer.version = VOS.VOSPACE_21;
            //if (vospaceURL == null) {
            //    //fallback to 2.0
            //    vospaceURL = lookupServiceURL(Standards.VOSPACE_SYNC_20);
            //    transfer.version = VOS.VOSPACE_20;
            //}
            
            log.debug("vospaceURL: " + vospaceURL);

            HttpPost httpPost = null;
            if (transfer.isQuickTransfer()) {
                // Assumption: quick transfer will be a single target
                if (transfer.getTargets().size() < 1) {
                    throw new IllegalArgumentException("No target found for quick transfer");
                }

                Map<String, Object> form = new HashMap<String, Object>();
                form.put("TARGET", transfer.getTargets().get(0));
                form.put("DIRECTION", transfer.getDirection().getValue());
                form.put("PROTOCOL", transfer.getProtocols().iterator().next().getUri()); // try first protocol?
                httpPost = new HttpPost(vospaceURL, form, false);
            } else {
                // POST the Job and get the redirect location.
                TransferWriter writer = new TransferWriter();
                StringWriter sw = new StringWriter();
                writer.write(transfer, sw);

                FileContent transferContent = new FileContent(sw.toString(), "text/xml", Charset.forName("UTF-8"));
                httpPost = new HttpPost(vospaceURL, transferContent, false);
            }

            if (transfer.getRemoteIP() != null) {
                httpPost.setRequestProperty(NetUtil.FORWARDED_FOR_CLIENT_IP_HEADER, transfer.getRemoteIP());
            }
            httpPost.run();

            if (httpPost.getThrowable() != null) {
                log.debug("Unable to post transfer because ", httpPost.getThrowable());
                throw new RuntimeException("Unable to post transfer because " + httpPost.getThrowable().getMessage());
            }


            URL redirectURL = httpPost.getRedirectURL();

            if (redirectURL == null) {
                throw new RuntimeException("Redirect not received from UWS.");
            }

            if (transfer.isQuickTransfer()) {
                log.debug("Quick transfer URL: " + redirectURL);
                // create a new transfer with a protocol with an end point
                List<Protocol> prots = new ArrayList<Protocol>();
                prots.add(new Protocol(transfer.getProtocols().iterator().next().getUri(), redirectURL.toString(), null));

                // Assumption: quick transfer will be a single target
                if (transfer.getTargets().size() < 1) {
                    throw new IllegalArgumentException("No target found for quick transfer.");
                }

                Transfer trf = new Transfer(transfer.getTargets().get(0), transfer.getDirection());
                trf.getProtocols().addAll(prots);
                return new ClientTransfer(null, trf, false);
            } else {
                log.debug("POST: transfer jobURL: " + redirectURL);

                // follow the redirect to run the job
                log.debug("GET - opening connection: " + redirectURL.toString());
                ByteArrayOutputStream out = new ByteArrayOutputStream();
                HttpGet get = new HttpGet(redirectURL, out);

                get.run();

                if (get.getThrowable() != null) {
                    log.debug("Unable to run the job", get.getThrowable());
                    throw new RuntimeException("Unable to run the job because " + get.getThrowable().getMessage());
                }
                String transDoc = new String(out.toByteArray(), "UTF-8");
                log.debug("transfer response: " + transDoc);
                TransferReader txfReader = new TransferReader(schemaValidation);
                Transfer trans = txfReader.read(transDoc, VOSURI.SCHEME);
                log.debug("GET - done: " + redirectURL);
                log.debug("negotiated transfer: " + trans);

                URL jobURL = extractJobURL(redirectURL);
                log.debug("extracted job url: " + jobURL);
                return new ClientTransfer(jobURL, trans, schemaValidation);
            }
        } catch (MalformedURLException e) {
            log.debug("failed to create transfer", e);
            throw new RuntimeException(e);
        } catch (IOException e) {
            log.debug("failed to create transfer", e);
            throw new RuntimeException(e);
        /*
        catch (JDOMException e) // from JobReader
        {
            log.debug("got bad job XML from service", e);
            throw new RuntimeException(e);
        }
        catch (ParseException e) // from JobReader
        {
            log.debug("got bad job XML from service", e);
            throw new RuntimeException(e);
        }
        */
        } catch (TransferParsingException e) {
            log.debug("got invalid XML from service", e);
            throw new RuntimeException(e);
        }
    }

    /**
     * Extract parts of the redirectURL that are needed.
     * @param redirectURL the redirect URL
     * @return the job URL
     * @throws MalformedURLException for invalid redirect URL
     */
    private URL extractJobURL(URL redirectURL)
        throws MalformedURLException {
        URL returnURL = null;

        if (redirectURL.toString().contains("transferDetails")) {
            // standard redirectURL in this case needs the last 2 elements removed
            // This had 'temporary hack' comment on it when it was around line 672,
            // where this function is called from
            returnURL = new URL(redirectURL.toString().substring(0, redirectURL.toString().length() - "/results/transferDetails".length()));
        } else {
            returnURL = redirectURL;
        }

        return returnURL;
    }

    private class NodeOutputStream implements OutputStreamWrapper {
        private VOSURI vosURI;
        private Node node;

        public NodeOutputStream(VOSURI vosURI, Node node) {
            this.vosURI = vosURI;
            this.node = node;
        }

        public void write(OutputStream out) throws IOException {
            NodeWriter writer = new NodeWriter();
            writer.write(this.vosURI, this.node, out);
        }
    }

    // temporary fix -- the code below needs to be moved out to a library
    private URL lookupServiceURL(final URI standard)
            throws AccessControlException {
        Subject subject = AuthenticationUtil.getCurrentSubject();
        AuthMethod am = AuthenticationUtil.getAuthMethodFromCredentials(subject);

        URL serviceURL = getRegistryClient().getServiceURL(this.serviceID, standard, am);

        if (serviceURL == null) {
            throw new RuntimeException(
                    String.format("Unable to get Service URL for '%s', '%s', '%s'",
                                  serviceID.toString(), standard, am));
        }

        return serviceURL;
    }

}
