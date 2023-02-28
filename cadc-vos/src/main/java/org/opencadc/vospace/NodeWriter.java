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

package org.opencadc.vospace;

import ca.nrc.cadc.auth.AuthenticationUtil;
import ca.nrc.cadc.auth.IdentityManager;
import ca.nrc.cadc.date.DateUtil;
import ca.nrc.cadc.util.StringBuilderWriter;

import java.io.IOException;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.charset.StandardCharsets;
import java.text.DateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

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
     * Write a Node to an OutputStream using UTF-8 encoding.
     *
     * @param node Node to write.
     * @param out OutputStream to write to.
     * @throws IOException if the writer fails to write.
     */
    public void write(VOSURI vosURI, Node node, OutputStream out)
        throws IOException {
        OutputStreamWriter outWriter;
        outWriter = new OutputStreamWriter(out, StandardCharsets.UTF_8);
        write(vosURI, node, outWriter);
    }

    /**
     * Write to root Element to a writer.
     *
     * @param root Root Element to write.
     * @param writer Writer to write to.
     * @throws IOException if the writer fails to write.
     */
    protected void write(Element root, Writer writer)
        throws IOException {
        XMLOutputter outputter = new XMLOutputter();
        outputter.setFormat(Format.getPrettyFormat());
        Document document = new Document(root);
        if (stylesheetURL != null) {
            Map<String, String> instructionMap = new HashMap<>(2);
            instructionMap.put("type", "text/xsl");
            instructionMap.put("href", stylesheetURL);
            ProcessingInstruction pi = new ProcessingInstruction("xml-stylesheet", instructionMap);
            document.getContent().add(0, pi);
        }
        outputter.output(document, writer);
    }

    /**
     * Write a node to a StringBuilder.
     *
     * @param vosURI The VOSURI of the node.
     * @param node The node to write.
     * @param builder A StringBuilder.
     * @throws IOException if there is an error writing the node.
     */
    public void write(VOSURI vosURI, Node node, StringBuilder builder)
        throws IOException {
        write(vosURI, node, new StringBuilderWriter(builder));
    }

    /**
     * A wrapper to write node without specifying its type
     *
     */
    public void write(VOSURI vosURI, Node node, Writer writer)
        throws IOException {
        long start = System.currentTimeMillis();
        Element root = getRootElement(vosURI, node);
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
    protected Element getRootElement(VOSURI vosURI, Node node) {
        // Create the root element (node).
        Element root = getNodeElement(vosURI, node);
        //root.addNamespaceDeclaration(vosNamespace);
        root.addNamespaceDeclaration(xsiNamespace);
        return root;
    }

    /**
     * Builds a single node element.
     *
     * @param node The node
     * @return
     */
    protected Element getNodeElement(VOSURI vosURI, Node node) {
        Element nodeElement = new Element("node", vosNamespace);
        nodeElement.setAttribute("uri", vosURI.toString());
        nodeElement.setAttribute("type", vosNamespace.getPrefix() + ":"
            + node.getClass().getSimpleName(), xsiNamespace);


        Set<NodeProperty> properties = new TreeSet<>();

        if (node instanceof ContainerNode) {
            ContainerNode cn = (ContainerNode) node;

            // Node variables serialized as node properties
            addNodeVariablesToProperties(node, properties);

            // ContainerNode variables serialized as node properties
            addContainerNodeVariablesToProperties(cn, properties);

            // add node properties to node field properties
            properties.addAll(node.properties);

            // add all properties to the document
            nodeElement.addContent(getPropertiesElement(properties));

            // add child node to the document
            nodeElement.addContent(getNodesElement(vosURI, cn));
        } else if (node instanceof DataNode) {
            DataNode dn = (DataNode) node;
            nodeElement.setAttribute("busy", Boolean.toString(dn.busy));

            // Node variables serialized as node properties
            addNodeVariablesToProperties(node, properties);

            // DataNode variables serialized as node properties
            addDataNodeVariablesToProperties(dn, properties);

            // add node properties to node field properties
            properties.addAll(node.properties);

            // add all properties to the document
            nodeElement.addContent(getPropertiesElement(properties));

            // add views to the DataNode in the document
            nodeElement.addContent(getAcceptsElement(node));
            nodeElement.addContent(getProvidesElement(node));
        } else if (node instanceof LinkNode) {

            // Node variables serialized as node properties
            addNodeVariablesToProperties(node, properties);

            // add node properties to node field properties
            properties.addAll(node.properties);

            // add all properties to the document
            nodeElement.addContent(getPropertiesElement(properties));

            // add target element
            LinkNode ln = (LinkNode) node;
            Element targetEl = new Element("target", vosNamespace);
            targetEl.setText(ln.getTarget().toString());
            nodeElement.addContent(targetEl);
        }

        return nodeElement;
    }

    /**
     * Add Node instance variables to the node properties.
     *
     * @param node a Node instance.
     * @param properties a Set of NodeProperty.
     */
    protected void addNodeVariablesToProperties(Node node, Set<NodeProperty> properties) {
        if (node.creatorID != null) {
            IdentityManager identityManager = AuthenticationUtil.getIdentityManager();
            String subjectString = identityManager.toDisplayString(node.creatorID);
            properties.add(new NodeProperty(VOS.PROPERTY_URI_CREATOR, subjectString));
        }
        if (node.isLocked != null) {
            properties.add(new NodeProperty(VOS.PROPERTY_URI_ISLOCKED, Boolean.toString(node.isLocked)));
        }
        if (node.isPublic != null) {
            properties.add(new NodeProperty(VOS.PROPERTY_URI_ISPUBLIC, Boolean.toString(node.isPublic)));
        }
        if (!node.readOnlyGroup.isEmpty()) {
            StringBuilder sb = new StringBuilder();
            for (URI group : node.readOnlyGroup) {
                if (sb.length() > 0) {
                    sb.append(VOS.PROPERTY_DELIM_GROUPREAD);
                }
                sb.append(group.toASCIIString());
            }
            properties.add(new NodeProperty(VOS.PROPERTY_URI_GROUPREAD, sb.toString()));
        }
        if (!node.readWriteGroup.isEmpty()) {
            StringBuilder sb = new StringBuilder();
            for (URI group : node.readWriteGroup) {
                if (sb.length() > 0) {
                    sb.append(VOS.PROPERTY_DELIM_GROUPWRITE);
                }
                sb.append(group.toASCIIString());
            }
            properties.add(new NodeProperty(VOS.PROPERTY_URI_GROUPWRITE, sb.toString()));
        }
    }

    /**
     * Add ContainerNode instance variables to the node properties.
     *
     * @param node a Node instance.
     * @param properties a Set of NodeProperty.
     */
    protected void addContainerNodeVariablesToProperties(ContainerNode node, Set<NodeProperty> properties) {
        properties.add(new NodeProperty(VOS.PROPERTY_URI_INHERIT_PERMISSIONS,
                                        Boolean.toString(node.isInheritPermissions())));
    }

    /**
     * Add DataNode instance variables to the node properties.
     *
     * @param node a Node instance.
     * @param properties a Set of NodeProperty.
     */
    protected void addDataNodeVariablesToProperties(DataNode node, Set<NodeProperty> properties) {

        properties.add(new NodeProperty(VOS.PROPERTY_URI_STORAGEID, node.getStorageID().toASCIIString()));

        if (node.getContentChecksum() != null) {
            properties.add(new NodeProperty(VOS.PROPERTY_URI_CONTENTMD5, node.getContentChecksum().toASCIIString()));
        }

        Date lastModified = null;
        if (node.getContentLastModified() != null) {
            lastModified = node.getContentLastModified();
        } else if (node.getLastModified() != null) {
            lastModified = node.getLastModified();
        }
        if (lastModified != null) {
            DateFormat df = DateUtil.getDateFormat(DateUtil.IVOA_DATE_FORMAT, DateUtil.UTC);
            properties.add(new NodeProperty(VOS.PROPERTY_URI_DATE, df.format(lastModified)));
        }

        if (node.getContentLength() != null) {
            properties.add(new NodeProperty(VOS.PROPERTY_URI_CONTENTLENGTH, node.getContentLength().toString()));
        }

        if (node.contentType != null) {
            properties.add(new NodeProperty(VOS.PROPERTY_URI_TYPE, node.contentType));
        }

        if (node.contentEncoding != null) {
            properties.add(new NodeProperty(VOS.PROPERTY_URI_CONTENTENCODING, node.contentEncoding));
        }
    }

    /**
     * Build the properties Element of a Node.
     *
     * @param properties The set of NodeProperty's.
     * @return A properties Element.
     */
    protected Element getPropertiesElement(Set<NodeProperty> properties) {
        Element ret = new Element("properties", vosNamespace);
        for (NodeProperty nodeProperty : properties) {
            Element property = new Element("property", vosNamespace);
            if (nodeProperty.isMarkedForDeletion()) {
                property.setAttribute(new Attribute("nil", "true", xsiNamespace));
            } else {
                property.setText(nodeProperty.getValue());
            }
            property.setAttribute("uri", nodeProperty.getKey().toASCIIString());
            if (nodeProperty.readOnly != null) {
                property.setAttribute("readOnly", Boolean.toString(nodeProperty.readOnly));
            }
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
        for (URI viewURI : node.accepts) {
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
        for (URI viewURI : node.provides) {
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
    protected Element getNodesElement(VOSURI vosURI, ContainerNode node) {
        Element nodes = new Element("nodes", vosNamespace);
        for (Node childNode : node.getNodes()) {
            VOSURI childURI = NodeUtil.getChildURI(vosURI, childNode.getName());
            Element nodeElement = getNodeElement(childURI, childNode);
            nodes.addContent(nodeElement);
        }
        return nodes;
    }

}
