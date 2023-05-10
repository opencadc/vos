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

package ca.nrc.cadc.vos;

import ca.nrc.cadc.util.StringBuilderWriter;
import ca.nrc.cadc.vos.VOS.NodeBusyState;

import java.io.IOException;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.UnsupportedEncodingException;
import java.io.Writer;
import java.net.URI;
import java.util.HashMap;
import java.util.Map;

import org.apache.log4j.Logger;
import org.jdom2.Attribute;
import org.jdom2.Document;
import org.jdom2.Element;
import org.jdom2.Namespace;
import org.jdom2.ProcessingInstruction;
import org.jdom2.output.Format;
import org.jdom2.output.XMLOutputter;

/**
 * Writes a Node as XML to an output.
 * 
 * @author jburke
 */
public class NodeWriter implements XmlProcessor {
    /*
     * The VOSpace Namespaces.
     */
    protected static Namespace xsiNamespace;
    
    static {
        xsiNamespace = Namespace.getNamespace("xsi", XSI_NAMESPACE);
    }

    private static Logger log = Logger.getLogger(NodeWriter.class);
    
    private String stylesheetURL = null;
    
    private Namespace vosNamespace;

    public NodeWriter() {
        this(VOSPACE_NS_20);
    }
    
    public NodeWriter(String vospaceNamespace) {
        this.vosNamespace = Namespace.getNamespace("vos", vospaceNamespace);
    }

    public void setStylesheetURL(String stylesheetURL) {
        this.stylesheetURL = stylesheetURL;
    }
    
    public String getStylesheetURL() {
        return stylesheetURL;
    }

    /**
     * Write aNode to an OutputStream using UTF-8 encoding.
     *
     * @param node Node to write.
     * @param out OutputStream to write to.
     * @throws IOException if the writer fails to write.
     */
    public void write(Node node, OutputStream out) throws IOException {
        OutputStreamWriter outWriter;
        try {
            outWriter = new OutputStreamWriter(out, "UTF-8");
        } catch (UnsupportedEncodingException e) {
            throw new RuntimeException("UTF-8 encoding not supported", e);
        }
        write(node, outWriter);
    }

    /**
     * Write to root Element to a writer.
     *
     * @param root Root Element to write.
     * @param writer Writer to write to.
     * @throws IOException if the writer fails to write.
     */
    @SuppressWarnings("unchecked")
    protected void write(Element root, Writer writer) throws IOException {
        XMLOutputter outputter = new XMLOutputter();
        outputter.setFormat(Format.getPrettyFormat());
        Document document = new Document(root);
        if (stylesheetURL != null) {
            Map<String, String> instructionMap = new HashMap<String, String>(2);
            instructionMap.put("type", "text/xsl");
            instructionMap.put("href", stylesheetURL);
            ProcessingInstruction pi = new ProcessingInstruction("xml-stylesheet", instructionMap);
            document.getContent().add(0, pi);
        }
        outputter.output(document, writer);
    }

    /**
     * Write a node to a StringBuilder.
     * @param node
     * @param builder
     * @throws IOException
     */
    public void write(Node node, StringBuilder builder) throws IOException {
        write(node, new StringBuilderWriter(builder));
    }

    /**
     * A wrapper to write node without specifying its type
     *
     */
    public void write(Node node, Writer writer) throws IOException {
        long start = System.currentTimeMillis();
        Element root = getRootElement(node);
        write(root, writer);
        long end = System.currentTimeMillis();
        log.debug("Write elapsed time: " + (end - start) + "ms");
    }

    /**
     *  Build the root Element of a Node.
     *
     * @param node Node.
     * @return root Element.
     */
    protected Element getRootElement(Node node) {
        // Create the root element (node).
        Element root = getNodeElement(node);
        //root.addNamespaceDeclaration(vosNamespace);
        root.addNamespaceDeclaration(xsiNamespace);
        return root;
    }

    /**
     * Builds a single node element.
     *
     * @param node
     * @return
     */
    protected Element getNodeElement(Node node) {
        Element ret = new Element("node", vosNamespace);
        ret.setAttribute("uri", node.getUri().toString());
        ret.setAttribute("type", vosNamespace.getPrefix() + ":" + node.getClass().getSimpleName(), xsiNamespace);

        Element props = getPropertiesElement(node);
        ret.addContent(props);

        if (node instanceof ContainerNode) {
            ContainerNode cn = (ContainerNode) node;
            ret.addContent(getNodesElement(cn));
        } else if ((node instanceof DataNode)
            || (node instanceof UnstructuredDataNode)
            || (node instanceof StructuredDataNode)) {
            ret.addContent(getAcceptsElement(node));
            ret.addContent(getProvidesElement(node));
            DataNode dn = (DataNode) node;
            ret.setAttribute("busy", (dn.getBusy().equals(NodeBusyState.notBusy) ? "false" : "true"));
        } else if (node instanceof LinkNode) {
            LinkNode ln = (LinkNode) node;
            Element targetEl = new Element("target", vosNamespace);
            targetEl.setText(ln.getTarget().toString());
            ret.addContent(targetEl);
        }
        return ret;
    }
    
    /**
     * Build the properties Element of a Node.
     *
     * @param node Node.
     * @return properties Element.
     */
    protected Element getPropertiesElement(Node node) {
        Element ret = new Element("properties", vosNamespace);
        for (NodeProperty nodeProperty : node.getProperties()) {
            Element property = new Element("property", vosNamespace);
            if (nodeProperty.isMarkedForDeletion()) {
                property.setAttribute(new Attribute("nil", "true", xsiNamespace));
            } else {
                property.setText(nodeProperty.getPropertyValue());
            }
            property.setAttribute("uri", nodeProperty.getPropertyURI()); 
            property.setAttribute("readOnly", (nodeProperty.isReadOnly() ? "true" : "false"));
            ret.addContent(property);
        }
        return ret;
    }
    
    /**
     * Build the accepts Element of a Node.
     *
     * @param node Node.
     * @return accepts Element.
     */
    protected Element getAcceptsElement(Node node) {
        Element accepts = new Element("accepts", vosNamespace);
        for (URI viewURI : node.accepts()) {
            Element viewElement = new Element("view", vosNamespace);
            viewElement.setAttribute("uri", viewURI.toString());
            accepts.addContent(viewElement);
        }
        return accepts;
    }
    
    /**
     * Build the accepts Element of a Node.
     *
     * @param node Node.
     * @return accepts Element.
     */
    protected Element getProvidesElement(Node node) {
        Element provides = new Element("provides", vosNamespace);
        for (URI viewURI : node.provides()) {
            Element viewElement = new Element("view", vosNamespace);
            viewElement.setAttribute("uri", viewURI.toString());
            provides.addContent(viewElement);
        }
        return provides;
    }

    
    /**
     * Build the nodes Element of a ContainerNode.
     * 
     * @param node Node.
     * @return nodes Element.
     */
    protected Element getNodesElement(ContainerNode node) {
        Element nodes = new Element("nodes", vosNamespace);
        for (Node childNode : node.getNodes()) {
            Element nodeElement = getNodeElement(childNode);
            nodes.addContent(nodeElement);
        }
        return nodes;
    }

}
