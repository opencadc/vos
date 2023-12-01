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

package org.opencadc.vospace.io;

import ca.nrc.cadc.date.DateUtil;
import ca.nrc.cadc.util.StringBuilderWriter;
import ca.nrc.cadc.xml.ContentConverter;
import ca.nrc.cadc.xml.IterableContent;
import java.io.IOException;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.text.DateFormat;
import java.util.HashMap;
import java.util.HashSet;
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
import org.opencadc.gms.GroupURI;
import org.opencadc.vospace.ContainerNode;
import org.opencadc.vospace.DataNode;
import org.opencadc.vospace.LinkNode;
import org.opencadc.vospace.Node;
import org.opencadc.vospace.NodeProperty;
import org.opencadc.vospace.NodeUtil;
import org.opencadc.vospace.VOS;
import org.opencadc.vospace.VOSURI;

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
    private Set<URI> immutableProps;

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
    
    public void setImmutableProperties(Set<URI> immutableProps) {
        this.immutableProps = immutableProps;
    }

    /**
     * Write a Node to an OutputStream using UTF-8 encoding.
     *
     * @param vosURI absolute URI of the node
     * @param node the node
     * @param out destination
     * @param detail detail level: min - no properties, max/properties - include properties.
     * @throws IOException if the writer fails to write.
     */
    public void write(VOSURI vosURI, Node node, OutputStream out, VOS.Detail detail)
        throws IOException {
        OutputStreamWriter outWriter;
        outWriter = new OutputStreamWriter(out, StandardCharsets.UTF_8);
        write(vosURI, node, outWriter, detail);
    }

    /**
     * Write to root Element to a writer.
     *
     * @param root Root Element to write.
     * @param writer Writer to write to.
     * @param detail detail level: min - no properties, max/properties - include properties.
     * @throws IOException if the writer fails to write.
     */
    protected void write(Element root, Writer writer, VOS.Detail detail)
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
     * @param detail detail level: min - no properties, max/properties - include properties.
     * @throws IOException if there is an error writing the node.
     */
    public void write(VOSURI vosURI, Node node, StringBuilder builder, VOS.Detail detail)
        throws IOException {
        write(vosURI, node, new StringBuilderWriter(builder), detail);
    }

    /**
     * A wrapper to write node without specifying its type
     *
     * @param vosURI absolute URI of the node
     * @param node the node
     * @param writer destination
     * @param detail detail level: min - no properties, max/properties - include properties.
     * @throws IOException if the writer fails to write.
     */
    public void write(VOSURI vosURI, Node node, Writer writer, VOS.Detail detail)
        throws IOException {
        long start = System.currentTimeMillis();
        Element root = getRootElement(vosURI, node, detail);
        write(root, writer, detail);
        long end = System.currentTimeMillis();
        log.debug("Write elapsed time: " + (end - start) + "ms");
    }

    /**
     *  Build the root Element of a Node.
     *
     * @param vosURI absolute URI of the node
     * @param node Node.
     * @param detail detail level: min - no properties, max/properties - include properties.
     * @return root Element.
     */
    protected Element getRootElement(VOSURI vosURI, Node node, VOS.Detail detail) {
        // Create the root element (node).
        Element root = getNodeElement(vosURI, node, detail);
        //root.addNamespaceDeclaration(vosNamespace);
        root.addNamespaceDeclaration(xsiNamespace);
        return root;
    }

    /**
     * Builds a single node element.
     *
     * @param vosURI absolute URI of the node
     * @param node Node.
     * @param detail detail level: min - no properties, max/properties - include properties.
     * @return
     */
    protected Element getNodeElement(VOSURI vosURI, Node node, VOS.Detail detail) {
        Element nodeElement = new Element("node", vosNamespace);
        nodeElement.setAttribute("uri", vosURI.toString());
        nodeElement.setAttribute("type", vosNamespace.getPrefix() + ":"
            + node.getClass().getSimpleName(), xsiNamespace);

        Set<NodeProperty> properties = new TreeSet<>();

        if (node instanceof ContainerNode) {
            ContainerNode cn = (ContainerNode) node;

            if (!VOS.Detail.min.equals(detail)) {
                // Node variables serialized as node properties
                addNodeVariablesToProperties(node, properties);

                // ContainerNode variables serialized as node properties
                addContainerNodeVariablesToProperties(cn, properties);

                // add node properties to node field properties
                properties.addAll(node.getProperties());
            }

            // add all properties to the document
            nodeElement.addContent(getPropertiesElement(properties));

            // add child node to the document
            nodeElement.addContent(getNodesElement(vosURI, cn, detail));
        } else if (node instanceof DataNode) {
            DataNode dn = (DataNode) node;

            if (!VOS.Detail.min.equals(detail)) {
                nodeElement.setAttribute("busy", Boolean.toString(dn.busy));
                // Node variables serialized as node properties
                addNodeVariablesToProperties(node, properties);

                // DataNode variables serialized as node properties
                addDataNodeVariablesToProperties(dn, properties);

                // add node properties to node field properties
                properties.addAll(node.getProperties());
            }

            // add all properties to the document
            nodeElement.addContent(getPropertiesElement(properties));

            // add views to the DataNode in the document
            nodeElement.addContent(getAcceptsElement(dn));
            nodeElement.addContent(getProvidesElement(dn));
        } else if (node instanceof LinkNode) {
            LinkNode ln = (LinkNode) node;

            if (!VOS.Detail.min.equals(detail)) {
                // Node variables serialized as node properties
                addNodeVariablesToProperties(node, properties);

                // LinkNode variables serialized as node properties
                addLinkNodeVariablesToProperties(ln, properties);

                // add node properties to node field properties
                properties.addAll(node.getProperties());
            }

            // add all properties to the document
            nodeElement.addContent(getPropertiesElement(properties));

            // add target element
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
        if (node.ownerDisplay != null) {
            properties.add(new NodeProperty(VOS.PROPERTY_URI_CREATOR, node.ownerDisplay));
        }
        if (node.isLocked != null) {
            properties.add(new NodeProperty(VOS.PROPERTY_URI_ISLOCKED, Boolean.toString(node.isLocked)));
        } else if (node.clearIsLocked) {
            properties.add(new NodeProperty(VOS.PROPERTY_URI_ISLOCKED)); // markForDeletion
        }

        if (node.isPublic != null) {
            properties.add(new NodeProperty(VOS.PROPERTY_URI_ISPUBLIC, Boolean.toString(node.isPublic)));
        } else if (node.clearIsPublic) {
            properties.add(new NodeProperty(VOS.PROPERTY_URI_ISPUBLIC)); // markForDeletion
        }

        if (!node.getReadOnlyGroup().isEmpty()) {
            StringBuilder sb = new StringBuilder();
            for (GroupURI group : node.getReadOnlyGroup()) {
                if (sb.length() > 0) {
                    sb.append(VOS.PROPERTY_DELIM_GROUPREAD);
                }
                sb.append(group.getURI().toASCIIString());
            }
            properties.add(new NodeProperty(VOS.PROPERTY_URI_GROUPREAD, sb.toString()));
        } else if (node.clearReadOnlyGroups) {
            properties.add(new NodeProperty(VOS.PROPERTY_URI_GROUPREAD)); // markForDeletion
        }

        if (!node.getReadWriteGroup().isEmpty()) {
            StringBuilder sb = new StringBuilder();
            for (GroupURI group : node.getReadWriteGroup()) {
                if (sb.length() > 0) {
                    sb.append(VOS.PROPERTY_DELIM_GROUPWRITE);
                }
                sb.append(group.getURI().toASCIIString());
            }
            properties.add(new NodeProperty(VOS.PROPERTY_URI_GROUPWRITE, sb.toString()));
        } else if (node.clearReadWriteGroups) {
            properties.add(new NodeProperty(VOS.PROPERTY_URI_GROUPWRITE));
        }
    }

    /**
     * Add ContainerNode instance variables to the node properties.
     *
     * @param node a Node instance.
     * @param properties a Set of NodeProperty.
     */
    protected void addContainerNodeVariablesToProperties(ContainerNode node, Set<NodeProperty> properties) {
        if (node.inheritPermissions != null) {
            properties.add(new NodeProperty(VOS.PROPERTY_URI_INHERIT_PERMISSIONS, node.inheritPermissions.toString()));
        } else if (node.clearInheritPermissions) {
            properties.add(new NodeProperty(VOS.PROPERTY_URI_INHERIT_PERMISSIONS));
        }
        if (node.getLastModified() != null) {
            DateFormat df = DateUtil.getDateFormat(DateUtil.IVOA_DATE_FORMAT, DateUtil.UTC);
            properties.add(new NodeProperty(VOS.PROPERTY_URI_DATE, df.format(node.getLastModified())));
        }
    }

    /**
     * Add LinkNode instance variables to the node properties.
     *
     * @param node a Node instance.
     * @param properties a Set of NodeProperty.
     */
    protected void addLinkNodeVariablesToProperties(LinkNode node, Set<NodeProperty> properties) {
        if (node.getLastModified() != null) {
            DateFormat df = DateUtil.getDateFormat(DateUtil.IVOA_DATE_FORMAT, DateUtil.UTC);
            properties.add(new NodeProperty(VOS.PROPERTY_URI_DATE, df.format(node.getLastModified())));
        }
    }

    /**
     * Add DataNode instance variables to the node properties.
     *
     * @param node a Node instance.
     * @param properties a Set of NodeProperty.
     */
    protected void addDataNodeVariablesToProperties(DataNode node, Set<NodeProperty> properties) {
        // currently none since busy is an attribute handled elsewhere
        // TODO for date for DataNode it's not clear which of the Node.lastModified, Artifact.lastModified,
        //  Artifact.contentLastModified is appropriate here
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
            Boolean readOnly = null;
            if (immutableProps != null && immutableProps.contains(nodeProperty.getKey())) {
                readOnly = true;
            } else if (nodeProperty.readOnly != null) {
                readOnly = nodeProperty.readOnly;
            }
            if (readOnly != null && readOnly) {
                // note: the && readOnly is an optimization to only add attr when it is true
                property.setAttribute("readOnly", Boolean.toString(readOnly));
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
    protected Element getAcceptsElement(DataNode node) {
        Element accepts = new Element("accepts", vosNamespace);
        for (URI viewURI : node.getAccepts()) {
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
    protected Element getProvidesElement(DataNode node) {
        Element provides = new Element("provides", vosNamespace);
        for (URI viewURI : node.getProvides()) {
            Element viewElement = new Element("view", vosNamespace);
            viewElement.setAttribute("uri", viewURI.toString());
            provides.addContent(viewElement);
        }
        return provides;
    }

    /**
     * Build the nodes Element of a ContainerNode.
     *
     * <p>Note: it uses the Node.childIterator to get the child nodes.
     * 
     * @param vosURI absolute URI of the node
     * @param node Node.
     * @param detail detail level: min - no properties, max/properties - include properties.
     * @return nodes Element.
     */
    protected Element getNodesElement(VOSURI vosURI, ContainerNode node, VOS.Detail detail) {
        if (node.childIterator != null) {
            ContentConverter<Element, Node> cc = new ContentConverter<Element, Node>() {
                public Element convert(Node node) {
                    VOSURI childURI = NodeUtil.getChildURI(vosURI, node.getName());
                    return getNodeElement(childURI, node, detail);
                }
            };
            return new IterableContent<Element, Node>("nodes", vosNamespace, node.childIterator, cc);
        } else {
            return new Element("nodes", vosNamespace);
        }
    }

}
