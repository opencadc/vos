/*
************************************************************************
*******************  CANADIAN ASTRONOMY DATA CENTRE  *******************
**************  CENTRE CANADIEN DE DONNÉES ASTRONOMIQUES  **************
*
*  (c) 2009.                            (c) 2009.
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
*  $Revision: 4 $
*
************************************************************************
*/
package ca.nrc.cadc.vos.server;

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.io.UnsupportedEncodingException;
import java.net.URI;
import java.net.URL;
import java.security.PrivilegedExceptionAction;
import java.util.ArrayList;
import java.util.List;

import javax.security.auth.Subject;

import org.apache.log4j.Logger;
import org.restlet.data.Disposition;
import org.restlet.data.Encoding;
import org.restlet.data.MediaType;

import ca.nrc.cadc.auth.AuthMethod;
import ca.nrc.cadc.net.TransientException;
import ca.nrc.cadc.reg.client.RegistryClient;
import ca.nrc.cadc.vos.ContainerNode;
import ca.nrc.cadc.vos.DataNode;
import ca.nrc.cadc.vos.LinkNode;
import ca.nrc.cadc.vos.LinkingException;
import ca.nrc.cadc.vos.Node;
import ca.nrc.cadc.vos.NodeNotFoundException;
import ca.nrc.cadc.vos.VOSURI;
import ca.nrc.cadc.vos.server.transfers.TransferUtil;

/**
 * The manifest view of a container node is a list of files and
 * file download information for all data nodes contained within
 * the container node and containers in that node.
 *
 * @author majorb
 */
public class ManifestView extends AbstractView
{

    static final String MANIFEST_OK = "OK";
    static final String MANIFEST_ERROR = "ERROR";
    static final char MANIFEST_FIELD_SEPARATOR = '\t';
    static final String MANIFEST_CONTENT_TYPE = "application/x-download-manifest+txt";

    private static Logger log = Logger.getLogger(ManifestView.class);

    // The server to which to redirect
    private String server;

    // The redirect scheme
    private String scheme;

    private int port;

    // The content disposition (file name)
    private String contentDisposition;

    private final RegistryClient regClient;
    private AuthMethod forceAuthMethod;
    private PathResolver resolver;

    /**
     * ManifestView constructor.
     */
    public ManifestView()
    {
        super();
        this.regClient = new RegistryClient();
    }

    /**
     * ManifestView constructor.
     * @param uri
     */
    public ManifestView(URI uri)
    {
        super(uri);
        this.regClient = new RegistryClient();
    }

    /**
     * Setup the ManifestView with the given node.
     */
    @Override
    public void setNode(Node node, String viewReference, URL requestURL)
        throws UnsupportedOperationException, TransientException
    {
        super.setNode(node, viewReference, requestURL);

        //if (!(node instanceof ContainerNode))
        //{
        //    throw new UnsupportedOperationException("ManifestView is only for container nodes.");
        //}
        scheme = requestURL.getProtocol();
        server = requestURL.getHost();
        // TODO: check for non-std port
        port = -1;

        // check to see if a protocol was specified
        String query = requestURL.getQuery();
        log.debug("query: " + query);

        String protocolReference = getFirstValue("protocol", query);
        log.debug("protocol="+protocolReference);
        if (protocolReference != null)
        {
            if ("http".equalsIgnoreCase(protocolReference) ||
                    "https".equalsIgnoreCase(protocolReference))
            {
                scheme = protocolReference;
            }
        }
        String auth = getFirstValue("auth", query);
        log.debug("auth="+auth);
        if (auth != null)
        {
            try
            {
                this.forceAuthMethod = AuthMethod.getAuthMethod(auth);
                // HACK: since all https is X509 client cert, auth also changes protocol
                if (AuthMethod.CERT.equals(forceAuthMethod))
                    scheme = "https";
                else
                    scheme = "http";
            }
            catch(IllegalArgumentException ex)
            {
                throw new UnsupportedOperationException("uknown auth method: " + auth);
            }
        }

        contentDisposition = "attachment;filename=" + node.getName() + ".manifest";
        log.debug("scheme="+scheme);
    }

    private String getFirstValue(String key, String query)
    {
        String[] parts = query.split("&");

        for (String p : parts)
        {
            String[] kv = p.split("=");
            if (kv.length == 2 && key.equalsIgnoreCase(kv[0]))
                return kv[1];
        }
        return null;
    }

    private void writeNode(Node node, PrintWriter writer, String destination)
        throws IOException
    {
        getVOSpaceAuthorizer().getReadPermission(node);
        if (node instanceof DataNode)
        {
            writeDataNode((DataNode) node, writer, destination);
        }
        else if (node instanceof LinkNode)
        {
            writeLinkNode((LinkNode) node, writer, destination);
        }
        else if (node instanceof ContainerNode)
        {
            writeContainerNode((ContainerNode) node, writer, destination);
        }
        else
        {
            log.warn("Found instance of Node of unknown type: "
                    + node.getClass().getName());
        }
    }

    private void writeContainerNode(ContainerNode container, PrintWriter writer, String rootDestination)
        throws IOException
    {
        int batchSize = 500;
        VOSURI lastSeen = null;
        int lastBatch = Integer.MAX_VALUE;
        while (lastBatch > 0)
        {
            try
            {
                nodePersistence.getChildren(container, lastSeen, batchSize);
            }
            catch (Exception e)
            {
                // This exception shouldn't occur as this node has already been
                // retrieved once.  If it does occur, the operation must be
                // aborted.
                log.error(e);
                writer.println(MANIFEST_ERROR
                        + MANIFEST_FIELD_SEPARATOR
                        + e.getMessage());
                return;
            }

            if (lastSeen != null)
            {
                Node n = container.getNodes().get(0);
                if (lastSeen.equals(n.getUri()))
                    container.getNodes().remove(0);
            }

            // loop control: this must be after removing the repeated lastSeen
            // above or we will never get to 0
            lastBatch = container.getNodes().size(); // technically correct
            if (lastBatch < batchSize - 1)
                lastBatch = 0; // otpmisation to skip getChildren that only returns lastSeen

            String destination = null;
            // go through each of the children
            for (Node child : container.getNodes())
            {
                destination = rootDestination + "/" + child.getName();

                try
                {
                    writeNode(child, writer, destination);
                    lastSeen = child.getUri();
                }
                catch (Exception e)
                {
                    log.debug("Failed to create manifest: " + e.getMessage(), e);
                    writer.println(MANIFEST_ERROR
                            + MANIFEST_FIELD_SEPARATOR
                            + e.getMessage()
                            + MANIFEST_FIELD_SEPARATOR
                            + destination);
                }
            }
            container.getNodes().clear();
        }
    }

    private void writeDataNode(DataNode child, PrintWriter writer, String destination)
        throws IOException
    {
        AuthMethod am = null;
        if (forceAuthMethod != null)
        {
            if (DataView.isPublic(child))
                am = AuthMethod.ANON;
            else
                am = forceAuthMethod;
        }

        writer.println(MANIFEST_OK
                + MANIFEST_FIELD_SEPARATOR
                + TransferUtil.getSynctransParamURL(scheme, child.getUri(), am, regClient)
                + MANIFEST_FIELD_SEPARATOR
                + destination);
    }

    private void writeLinkNode(LinkNode link, PrintWriter writer, String destination)
        throws IOException
    {
        if ("vos".equals(link.getTarget().getScheme()))
        {
            try
            {
                Node tnode = resolver.resolveWithReadPermissionCheck(new VOSURI(link.getTarget()), getVOSpaceAuthorizer(), true);
                writeNode(tnode, writer, destination);
            }
            catch(NodeNotFoundException ex)
            {
                String msg = "link target not found for " + link.getUri().getURI().toASCIIString() + " : " + ex.getMessage();
                log.debug(msg, ex);
                writer.println(MANIFEST_ERROR
                        + MANIFEST_FIELD_SEPARATOR
                        + msg
                        + MANIFEST_FIELD_SEPARATOR
                        + destination);
            }
            catch (LinkingException ex)
            {
                String msg = "link error for " + link.getUri().getURI().toASCIIString() + " : " + ex.getMessage();
                log.debug(msg, ex);
                writer.println(MANIFEST_ERROR
                        + MANIFEST_FIELD_SEPARATOR
                        + msg
                        + MANIFEST_FIELD_SEPARATOR
                        + destination);
            }
            catch (TransientException ex)
            {
                log.debug("Failed to resolve "
                        + link.getUri().getURI().toASCIIString() + " : " + ex.getMessage(), ex);
                writer.println(MANIFEST_ERROR
                        + MANIFEST_FIELD_SEPARATOR
                        + ex.getMessage()
                        + MANIFEST_FIELD_SEPARATOR
                        + destination);
            }
            finally { }
        }
        else
        {
            writer.println(MANIFEST_OK
                + MANIFEST_FIELD_SEPARATOR
                + link.getTarget().toASCIIString()
                + MANIFEST_FIELD_SEPARATOR
                + destination);
        }
    }


    /**
     * Write the manifest output to the output stream.
     *
     * @param out OutputStream to write to.
     * @throws IOException thrown if there was some problem writing to the OutputStream.
     */
    @Override
    public void write(OutputStream out)
        throws IOException
    {
        try
        {
            this.resolver = new PathResolver(nodePersistence);
            ManifestRunner manifestRunner = new ManifestRunner(out);
            if (subject == null)
            {
                manifestRunner.run();
            }
            else
            {
                Subject.doAs(subject, manifestRunner);
            }
        }
        catch (Exception e)
        {
            throw new IOException(e.getMessage(), e);
        }
    }

    /**
     * ManifestView not accepted for any nodes.
     */
    @Override
    public boolean canAccept(Node node)
    {
        return false;
    }

    /**
     * ManifestView is provided for all container nodes.
     */
    @Override
    public boolean canProvide(Node node)
    {
        return (node instanceof ContainerNode);
    }

    /**
     * Return the content length of the data for the view.
     * Content length is not known in this view as the data is streamed.
     */
    @Override
    public long getContentLength()
    {
        return -1;
    }

    /**
     * Return the content type of the data for the view.
     */
    @Override
    public MediaType getMediaType()
    {
        return new MediaType(MANIFEST_CONTENT_TYPE);
    }

    @Override
    public Disposition getDisposition()
    {
        return new Disposition(contentDisposition);
    }

    /**
     * Return the content encoding of the data for the view.
     */
    @Override
    public List<Encoding> getEncodings()
    {
        return new ArrayList<Encoding>(0);
    }

    /**
     * Return the MD5 Checksum of the data for the view.
     * Content MD5 is not known in this view as the data is streamed.
     */
    @Override
    public String getContentMD5()
    {
        return null;
    }

    /**
     * Class used to simply hold formatting information
     */
    private class ManifestFormat
    {
        private boolean firstLine = true;

        public boolean isFirstLine()
        {
            return firstLine;
        }
        public void setFirstLine(boolean firstLine)
        {
            this.firstLine = firstLine;
        }
    }

        /**
     * Wrapper to allow the creation of the manifest to run in the
     * subject's context.
     */
    class ManifestRunner implements PrivilegedExceptionAction<Object>
    {
        OutputStream out;

        ManifestRunner(OutputStream out)
        {
            this.out = out;
        }

        @Override
        public Object run() throws Exception
        {
            PrintWriter writer = null;
            try
            {
                // auto-flush each line
                writer = new PrintWriter(new BufferedWriter(new OutputStreamWriter(out, "UTF-8")), true);
                Node n = getNode();
                String destination = n.getName();
                writeNode(node, writer, destination);
                return null;
            }
            catch (UnsupportedEncodingException e)
            {
                throw new RuntimeException("UTF-8 encoding not supported", e);
            }
            finally
            {
                if (writer != null)
                {
                    writer.close();
                }
            }
        }

    }

}
