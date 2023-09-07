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
import ca.nrc.cadc.xml.XmlUtil;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.io.StringReader;
import java.io.UnsupportedEncodingException;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.charset.StandardCharsets;
import java.text.DateFormat;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import org.apache.log4j.Logger;
import org.jdom2.Document;
import org.jdom2.Element;
import org.jdom2.JDOMException;
import org.jdom2.Namespace;
import org.opencadc.gms.GroupURI;
import org.opencadc.vospace.ContainerNode;
import org.opencadc.vospace.DataNode;
import org.opencadc.vospace.LinkNode;
import org.opencadc.vospace.Node;
import org.opencadc.vospace.NodeNotSupportedException;
import org.opencadc.vospace.NodeProperty;
import org.opencadc.vospace.NodeUtil;
import org.opencadc.vospace.StructuredDataNode;
import org.opencadc.vospace.UnstructuredDataNode;
import org.opencadc.vospace.VOS;
import org.opencadc.vospace.VOSURI;

/**
 * Constructs a Node from an XML source. This class is not thread safe but it is
 * re-usable so it can safely be used to sequentially parse multiple XML node
 * documents.
 *
 * @author jburke
 */
public class NodeReader implements XmlProcessor {
    private static final Logger log = Logger.getLogger(NodeReader.class);
    
    protected Map<String, String> schemaMap;
    protected Namespace xsiNamespace;

    /**
     * Constructor. XML Schema validation is enabled by default.
     */
    public NodeReader() {
        this(true);
    }

    /**
     * Constructor. XML schema validation may be disabled, in which case the client
     * is likely to fail in horrible ways (e.g. NullPointerException) if it receives
     * invalid documents. However, performance may be improved.
     *
     * @param enableSchemaValidation
     */
    public NodeReader(boolean enableSchemaValidation) {
        if (enableSchemaValidation) {
            String vospaceSchemaUrl21 = XmlUtil.getResourceUrlString(VOSPACE_SCHEMA_RESOURCE_21, NodeReader.class);
            log.debug("vospaceSchemaUrl21: " + vospaceSchemaUrl21);

            String xlinkSchemaUrl = XmlUtil.getResourceUrlString(XLINK_SCHEMA_RESOURCE, NodeReader.class);
            log.debug("xlinkSchemaUrl: " + xlinkSchemaUrl);

            if (vospaceSchemaUrl21 == null) {
                throw new RuntimeException(String.format("failed to load %s from classpath", VOSPACE_SCHEMA_RESOURCE_21));
            }
            if (xlinkSchemaUrl == null) {
                throw new RuntimeException(String.format("failed to load %s from classpath", XLINK_SCHEMA_RESOURCE));
            }

            schemaMap = new HashMap<>();
            // namespace remains '2.0'
            schemaMap.put(VOSPACE_NS_20, vospaceSchemaUrl21);
            schemaMap.put(XLINK_NAMESPACE, xlinkSchemaUrl);
            log.debug("schema validation enabled");
        } else {
            log.debug("schema validation disabled");
        }

        xsiNamespace = Namespace.getNamespace("http://www.w3.org/2001/XMLSchema-instance");
    }

    /**
     *  Construct a Node from an XML String source.
     *
     * @param xml The XML string for the node document.
     * @return A Node representation of the XML document.
     * @throws NodeParsingException if there is an error parsing the XML.
     */
    public NodeReaderResult read(String xml)
        throws NodeParsingException, NodeNotSupportedException {
        NodeUtil.assertNotNull(NodeReader.class, "xml", "xml");

        try {
            return read(new StringReader(xml));
        } catch (IOException ioe) {
            throw new NodeParsingException("Error reading XML: " + ioe.getMessage(), ioe);
        }
    }

    /**
     * Construct a Node from a InputStream.
     *
     * @param in The InputStream for the node document.
     * @return A Node representation of the InputStream.
     * @throws IOException if there is an error reading the input stream.
     * @throws NodeParsingException if there is an error parsing the XML.
     */
    public NodeReaderResult read(InputStream in)
        throws IOException, NodeParsingException, NodeNotSupportedException {
        NodeUtil.assertNotNull(NodeReader.class, "in", "in");
        if (in == null) {
            throw new IOException("The InputStream is closed");
        }

        try {
            return read(new InputStreamReader(in, StandardCharsets.UTF_8));
        } catch (UnsupportedEncodingException e) {
            throw new RuntimeException("UTF-8 encoding not supported");
        }
    }

    /**
     *  Construct a Node from a Reader.
     *
     * @param reader The reader for the node document.
     * @return A Node representation of the reader.
     * @throws IOException if there is an error reading the input stream.
     * @throws NodeParsingException if there is an error parsing the XML.
     */
    public NodeReaderResult read(Reader reader)
            throws NodeParsingException, IOException, NodeNotSupportedException {
        NodeUtil.assertNotNull(NodeReader.class, "reader", "reader");

        // Create a JDOM Document from the XML
        Document document;
        try {
            document = XmlUtil.buildDocument(reader, schemaMap);
        } catch (JDOMException e) {
            throw new NodeParsingException("XML failed schema validation: " + e.getMessage(), e);
        }

        // Root element and namespace of the Document
        Element root = document.getRootElement();
        Namespace namespace = root.getNamespace();
        log.debug("node namespace uri: " + namespace.getURI());
        log.debug("node namespace prefix: " + namespace.getPrefix());

        int version;
        if (VOSPACE_NS_20.equals(namespace.getURI())) {
            version = VOS.VOSPACE_20;
        } else {
            throw new IllegalArgumentException("unexpected VOSpace namespace: " + namespace.getURI());
        }

        /* Node base elements */
        // uri attribute of the node element
        String uri = root.getAttributeValue("uri");
        if (uri == null) {
            throw new NodeParsingException("uri attribute not found in root element");
        }
        log.debug("node attribute uri: " + uri);

        VOSURI vosURI;
        try {
            vosURI = new VOSURI(uri);
        } catch (URISyntaxException e) {
            throw new NodeParsingException("invalid node uri: " + uri, e);
        }
        log.debug("node VOSURI: " + vosURI);

        // Get the xsi:type attribute which defines the Node class
        String xsiType = root.getAttributeValue("type", xsiNamespace);
        if (xsiType == null) {
            throw new NodeParsingException("xsi:type attribute not found in root element: " + uri);
        }

        // Split the type attribute into namespace and Node type
        String[] types = xsiType.split(":");
        String type = types[1];
        log.debug("node type: " + type);

        Node node;
        if (type.equals(ContainerNode.class.getSimpleName())) {
            node = buildContainerNode(root, namespace, vosURI);
        } else if (type.equals(LinkNode.class.getSimpleName())) {
            node = buildLinkNode(root, namespace, vosURI);
        } else if (type.equals(DataNode.class.getSimpleName())
            || type.equals(StructuredDataNode.class.getSimpleName())
            || type.equals(UnstructuredDataNode.class.getSimpleName())) {
            node = buildDataNode(root, namespace, vosURI, type);
        } else {
            throw new NodeNotSupportedException("unsupported node type: " + type);
        }
        //node.version = version;

        return new NodeReaderResult(vosURI, node);
    }

    /**
     * Constructs a LinkNode from the document element.
     *
     * @param element a node element in the document.
     * @param namespace the document namespace.
     * @param vosURI the node uri attribute.
     * @return a LinkNode.
     * @throws NodeParsingException if there is an error parsing the XML.
     */
    protected Node buildLinkNode(Element element, Namespace namespace, VOSURI vosURI)
        throws NodeParsingException {
        
        // target element in the node element
        Element target = element.getChild("target", namespace);
        if (target == null) {
            throw new NodeParsingException("target element not found for: " + vosURI);
        }
        log.debug("node target: " + target.getText());

        // Instantiate a LinkNode class
        LinkNode node;
        try {
            node = new LinkNode(vosURI.getName(), new URI(target.getText()));
        } catch (URISyntaxException e) {
            throw new NodeParsingException("invalid LinkNode target uri: " + target.getText(), e);
        }

        // get all node properties
        Set<NodeProperty> properties = getProperties(element, namespace);

        // set node variables from the properties
        setNodeVariables(element, namespace, node, properties);

        // add remaining properties as node properties
        node.getProperties().addAll(properties);

        return node;
    }
    
    /**
     * Constructs a ContainerNode from the document element.
     *
     * @param element a node element of the document.
     * @param namespace the document namespace.
     * @param vosURI vosuri from the node uri attribute.
     * @return a ContainerNode
     * @throws NodeParsingException if there is an error parsing the XML.
     */
    protected Node buildContainerNode(Element element, Namespace namespace, VOSURI vosURI)
        throws NodeParsingException {
        Set<NodeProperty> properties = getProperties(element, namespace);
        
        // instantiate a ContainerNode class
        ContainerNode node = new ContainerNode(vosURI.getName());
        node.inheritPermissions = getBooleanProperty(VOS.PROPERTY_URI_INHERIT_PERMISSIONS, properties);
        
        setNodeVariables(element, namespace, node, properties);
        node.getProperties().addAll(properties);

        // get nodes element
        Element nodes = element.getChild("nodes", namespace);
        if (nodes == null) {
            throw new NodeParsingException("nodes element not found in ContainerNode: " + vosURI);
        }

        // list of child nodes
        List<Element> nodesList = nodes.getChildren("node", namespace);
        for (Element childNode : nodesList) {
            String childUri = childNode.getAttributeValue("uri");
            if (childUri == null) {
                throw new NodeParsingException("uri attribute not found in ContainerNode nodes element: " + vosURI);
            }

            VOSURI childVosURI;
            try {
                childVosURI = new VOSURI(childUri);
            } catch (URISyntaxException e) {
                throw new NodeParsingException("invalid child node uri: " + childUri, e);
            }

            // Get the xsi:type attribute which defines the Node class
            String xsiType = childNode.getAttributeValue("type", xsiNamespace);
            if (xsiType == null) {
                throw new NodeParsingException("xsi:type attribute not found in child node element: " + childUri);
            }

            // Split the type attribute into namespace and Node type
            String[] types = xsiType.split(":");
            String type = types[1];
            log.debug("node type: " + type);

            if (type.equals(ContainerNode.class.getSimpleName())) {
                node.getNodes().add(buildContainerNode(childNode, namespace, childVosURI));
            } else if (type.equals(LinkNode.class.getSimpleName())) {
                node.getNodes().add(buildLinkNode(childNode, namespace, childVosURI));
            } else if (type.equals(DataNode.class.getSimpleName())
                || type.equals(StructuredDataNode.class.getSimpleName())
                || type.equals(UnstructuredDataNode.class.getSimpleName())) {
                node.getNodes().add(buildDataNode(childNode, namespace, childVosURI, type));
            } else {
                throw new NodeParsingException("unsupported node type " + type);
            }
            
            log.debug("added child node: " + childUri);
        }

        return node;
    }

    /**
     * Constructs a DataNode from the document element.
     *
     * @param element a node element in the document.
     * @param namespace the document namespace.
     * @param vosURI vosuri from the node uri attribute.
     * @return a DataNode.
     * @throws NodeParsingException if there is an error parsing the XML.
     */
    protected Node buildDataNode(Element element, Namespace namespace, VOSURI vosURI, String type)
        throws NodeParsingException {

        // Instantiate a DataNode class
        DataNode node;
        if (type.equals(DataNode.class.getSimpleName())) {
            node = new DataNode(vosURI.getName());
        } else if (type.equals(StructuredDataNode.class.getSimpleName())) {
            node = new StructuredDataNode(vosURI.getName());
        } else if (type.equals(UnstructuredDataNode.class.getSimpleName())) {
            node = new UnstructuredDataNode(vosURI.getName());
        } else {
            throw new NodeParsingException("unsupported node type " + type);
        }

        // node busy attribute
        String busy = element.getAttributeValue("busy");
        if (busy == null) {
            String error = "busy attribute not found in DataNode: " + vosURI;
            throw new NodeParsingException(error);
        }
        node.busy = Boolean.parseBoolean(busy);
        log.debug("busy: " + node.busy);

        Set<NodeProperty> properties = getProperties(element, namespace);
        setNodeVariables(element, namespace, node, properties);
        node.getProperties().addAll(properties);

        return node;
    }

    /**
     * Get a Set of URI describing the node supported view's.
     *
     * @param element a node element of the document.
     * @param namespace the document namespace.
     * @param parent the parent element to the view elements.
     * @return a set of view URI.
     * @throws NodeParsingException if there is an error parsing the XML.
     */
    protected List<URI> getViewURIs(Element element, Namespace namespace, String parent)
        throws NodeParsingException {
        
        // new View List
        List<URI> list = new ArrayList<>();
        
        // view parent element
        Element parentElement = element.getChild(parent, namespace);
        if (parentElement == null) {
            return list;
        }

        // view elements
        List<Element> views = parentElement.getChildren("view", namespace);
        for (Element view : views) {
            // view uri attribute
            String viewUri = view.getAttributeValue("uri");
            if (viewUri == null) {
                throw new NodeParsingException("uri attribute not found in view element: " + parent);
            }
            log.debug(parent + " view uri: " + viewUri);

            try {
                list.add(new URI(viewUri));
            } catch (URISyntaxException e) {
                throw new NodeParsingException("invalid uri attribute in view element: " + viewUri);
            }
        }

        return list;
    }

    /**
     * Get a set of group URI's for the given node property key. Multiple groups
     * are parsed from the property value using the given group delimiter.
     *
     * @param key a node property key.
     * @param delimiter the group separator.
     * @param properties set of the node properties.
     * @return a set of group URI's.
     */
    protected Set<GroupURI> getGroupURIs(URI key, String delimiter, Set<NodeProperty> properties)
        throws NodeParsingException {
        Set<GroupURI> groups = new TreeSet<>();
        NodeProperty nodeProperty = getNodeProperty(key, properties);
        if (nodeProperty != null && nodeProperty.getValue() != null) {
            String[] values = nodeProperty.getValue().split(delimiter);
            for (String value : values) {
                GroupURI groupURI;
                try {
                    groupURI = new GroupURI(new URI(value));
                } catch (URISyntaxException e) {
                    throw new NodeParsingException(String.format("node property %s has invalid group URI %s",
                                                                 key.toASCIIString(), value));
                }
                groups.add(groupURI);
            }
            properties.remove(nodeProperty);
        }
        return groups;
    }

    /**
     * Set the Node class instance variables from the node element properties,
     * and remove the instance variable properties from the set of
     * all properties.
     *
     * @param element a node element of the document.
     * @param namespace the document namespace.
     * @param node a node instance.
     * @param properties set of node properties.
     * @throws NodeParsingException  if there is an error parsing the XML.
     */
    protected void setNodeVariables(Element element, Namespace namespace,
                                    Node node, Set<NodeProperty> properties)
        throws NodeParsingException {

        node.ownerDisplay = getStringProperty(VOS.PROPERTY_URI_CREATOR, properties);
        node.isLocked = getBooleanProperty(VOS.PROPERTY_URI_ISLOCKED, properties);
        node.isPublic = getBooleanProperty(VOS.PROPERTY_URI_ISPUBLIC, properties);
        if (node instanceof DataNode) {
            DataNode dn = (DataNode) node;
            dn.getAccepts().addAll(getViewURIs(element, namespace, "accepts"));
            dn.getProvides().addAll(getViewURIs(element, namespace, "provides"));
        }
        node.getReadOnlyGroup().addAll(getGroupURIs(VOS.PROPERTY_URI_GROUPREAD, VOS.PROPERTY_DELIM_GROUPREAD, properties));
        node.getReadWriteGroup().addAll(getGroupURIs(VOS.PROPERTY_URI_GROUPWRITE, VOS.PROPERTY_DELIM_GROUPWRITE, properties));
    }

    /**
     * Builds a Set of NodeProperty's from the properties element.
     *
     * @param element a node Element of the Document.
     * @param namespace the document Namespace.
     * @return a set of NodeProperty's.
     * @throws NodeParsingException if there is an error parsing the XML.
     */
    protected Set<NodeProperty> getProperties(Element element, Namespace namespace)
        throws NodeParsingException {

        // new NodeProperty List
        Set<NodeProperty> nodeProperties = new TreeSet<>();

        // properties element
        Element properties = element.getChild("properties", namespace);
        if (properties == null) {
            return nodeProperties;
        }

        // properties property elements
        List<Element> propertyList = properties.getChildren("property", namespace);
        for (Element property : propertyList) {

            String propertyUri = property.getAttributeValue("uri");
            if (propertyUri == null) {
                throw new NodeParsingException("uri attribute not found in property element: " + property);
            }

            // the property key
            URI key;
            try {
                key  = new URI(propertyUri);
            } catch (URISyntaxException e) {
                throw new NodeParsingException("invalid properties URI: " + propertyUri);
            }

            // the property value
            String value = property.getText();

            // xsi:nil set to true indicates Property is to be deleted
            String xsiNil = property.getAttributeValue("nil", xsiNamespace);
            boolean markedForDeletion = false;
            if (xsiNil != null) {
                markedForDeletion = Boolean.parseBoolean(xsiNil);
            }

            // create a new NodeProperty
            NodeProperty nodeProperty;
            if (markedForDeletion) {
                nodeProperty = new NodeProperty(key);
            } else {
                nodeProperty = new NodeProperty(key, value);
            }

            // set readOnly attribute
            String readOnly = property.getAttributeValue("readOnly");
            if (readOnly != null) {
                nodeProperty.readOnly = Boolean.parseBoolean(readOnly);
            }

            nodeProperties.add(nodeProperty);
        }

        return nodeProperties;
    }

    /**
     * Get a NodeProperty from the Set of all properties for the given key.
     *
     * @param key the NodeProperty key.
     * @param properties the set of all node properties.
     * @return the NodeProperty for the given key, else null if not found.
     */
    public NodeProperty getNodeProperty(URI key, Set<NodeProperty> properties) {
        for (NodeProperty nodeProperty : properties) {
            if (nodeProperty.getKey().equals(key)) {
                return nodeProperty;
            }
        }
        return null;
    }

    /**
     * Get the String value of a NodeProperty for the given key. If the NodeProperty is
     * found in the Set of all NodeProperty's it is removed from the Set.
     *
     * @param key a NodeProperty key.
     * @param properties the set of all node properties.
     * @return the NodeProperty String value for the given key, else null if not found.
     */
    public String getStringProperty(URI key, Set<NodeProperty> properties) {
        NodeProperty nodeProperty = getNodeProperty(key, properties);
        if (nodeProperty != null) {
            properties.remove(nodeProperty);
            return nodeProperty.getValue();
        }
        return null;
    }

    /**
     * Get the URI value of a NodeProperty for the given key. If the NodeProperty is
     * found in the Set of all NodeProperty's it is removed from the Set.
     *
     * @param key a NodeProperty key.
     * @param properties the set of all node properties.
     * @return the NodeProperty URI value for the given key, else null if not found.
     */
    public URI getURIProperty(URI key, Set<NodeProperty> properties)
        throws NodeParsingException {
        NodeProperty nodeProperty = getNodeProperty(key, properties);
        if (nodeProperty != null) {
            try {
                properties.remove(nodeProperty);
                if (nodeProperty.getValue() != null) {
                    return new URI(nodeProperty.getValue());
                }
            } catch (URISyntaxException e) {
                throw new NodeParsingException(String.format("invalid URI property: %s = %s ",
                                                             key, nodeProperty.getValue()), e);
            }
        }
        return null;
    }

    /**
     * Get the Date value of a NodeProperty for the given key. If the NodeProperty is
     * found in the Set of all NodeProperty's it is removed from the Set.
     *
     * @param key a NodeProperty key.
     * @param properties the set of all node properties.
     * @return the NodeProperty Date value for the given key, else null if not found.
     */
    public Date getDateProperty(URI key, Set<NodeProperty> properties)
        throws NodeParsingException {
        NodeProperty nodeProperty = getNodeProperty(key, properties);
        if (nodeProperty != null) {
            try {
                properties.remove(nodeProperty);
                if (nodeProperty.getValue() != null) {
                    DateFormat dateFormat = DateUtil.getDateFormat(DateUtil.IVOA_DATE_FORMAT, DateUtil.UTC);
                    return dateFormat.parse(nodeProperty.getValue());
                }
            } catch (ParseException e) {
                throw new NodeParsingException(String.format("invalid Date property: %s = %s ",
                                                             key, nodeProperty.getValue()), e);
            }
        }
        return null;
    }

    /**
     * Get the Long value of a NodeProperty for the given key. If the NodeProperty is
     * found in the Set of all NodeProperty's it is removed from the Set.
     *
     * @param key a NodeProperty key.
     * @param properties the set of all node properties.
     * @return the NodeProperty Long value for the given key, else null if not found.
     */
    public Long getLongProperty(URI key, Set<NodeProperty> properties)
        throws NodeParsingException {
        NodeProperty nodeProperty = getNodeProperty(key, properties);
        if (nodeProperty != null) {
            try {
                properties.remove(nodeProperty);
                if (nodeProperty.getValue() != null) {
                    return Long.parseLong(nodeProperty.getValue());
                }
            } catch (NumberFormatException e) {
                throw new NodeParsingException(String.format("invalid Long property: %s = %s ",
                                                             key, nodeProperty.getValue()), e);
            }
        }
        return null;
    }

    /**
     * Get the Boolean value of a NodeProperty for the given key. If the NodeProperty is
     * found in the Set of all NodeProperty's it is removed from the Set.
     *
     * @param key a NodeProperty key.
     * @param properties the set of all node properties.
     * @return the NodeProperty Boolean value for the given key, else null if not found.
     */
    public Boolean getBooleanProperty(URI key, Set<NodeProperty> properties) {
        NodeProperty nodeProperty = getNodeProperty(key, properties);
        if (nodeProperty != null) {
            properties.remove(nodeProperty);
            if (nodeProperty.getValue() != null) {
                return Boolean.parseBoolean(nodeProperty.getValue());
            }
        }
        return null;
    }

    public static class NodeReaderResult {
        public VOSURI vosURI;
        public Node node;

        public NodeReaderResult(VOSURI vosURI, Node node) {
            this.vosURI = vosURI;
            this.node = node;
        }
    }

}
