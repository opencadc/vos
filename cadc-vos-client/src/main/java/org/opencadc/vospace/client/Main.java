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
import ca.nrc.cadc.auth.CertCmdArgUtil;
import ca.nrc.cadc.auth.RunnableAction;
import ca.nrc.cadc.auth.X509CertificateChain;
import ca.nrc.cadc.date.DateUtil;
import ca.nrc.cadc.net.NetUtil;
import ca.nrc.cadc.net.NetrcAuthenticator;
import ca.nrc.cadc.net.ResourceNotFoundException;
import ca.nrc.cadc.reg.Standards;
import ca.nrc.cadc.util.ArgumentMap;
import ca.nrc.cadc.util.Log4jInit;
import ca.nrc.cadc.util.StringUtil;
import ca.nrc.cadc.uws.ErrorSummary;
import ca.nrc.cadc.uws.ExecutionPhase;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.security.AccessControlException;
import java.security.cert.CertificateExpiredException;
import java.security.cert.CertificateNotYetValidException;
import java.security.cert.X509Certificate;
import java.text.DateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Properties;
import java.util.Set;
import java.util.TreeSet;
import javax.security.auth.Subject;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.opencadc.gms.GroupURI;
import org.opencadc.vospace.ContainerNode;
import org.opencadc.vospace.DataNode;
import org.opencadc.vospace.LinkNode;
import org.opencadc.vospace.Node;
import org.opencadc.vospace.NodeLockedException;
import org.opencadc.vospace.NodeNotFoundException;
import org.opencadc.vospace.NodeProperty;
import org.opencadc.vospace.NodeUtil;
import org.opencadc.vospace.VOS;
import org.opencadc.vospace.VOSURI;
import org.opencadc.vospace.View;
import org.opencadc.vospace.transfer.Direction;
import org.opencadc.vospace.transfer.Protocol;
import org.opencadc.vospace.transfer.Transfer;

/**
 * Main method for the command-line VOSpace client.
 *
 * @author pdowler, zhangsa
 */
public class Main implements Runnable {
    public static final String ARG_HELP = "help";
    public static final String ARG_VERBOSE = "verbose";
    public static final String ARG_DEBUG = "debug";
    public static final String ARG_H = "h";
    public static final String ARG_V = "v";
    public static final String ARG_D = "d";
    public static final String ARG_XSV = "xsv";
    public static final String ARG_NO_RETRY = "noretry";
    public static final String ARG_VIEW = "view";
    public static final String ARG_CREATE = "create";
    public static final String ARG_DELETE = "delete";
    public static final String ARG_SET = "set";
    public static final String ARG_COPY = "copy";
    public static final String ARG_MOVE = "move";
    //public static final String ARG_TARGET = "target";
    public static final String ARG_PUBLIC = "public";
    public static final String ARG_GROUP_READ = "group-read";
    public static final String ARG_GROUP_WRITE = "group-write";
    public static final String ARG_PROP = "prop";
    //public static final String ARG_SRC = "src";
    public static final String ARG_LINK = "link";
    //public static final String ARG_DEST = "dest";
    public static final String ARG_CONTENT_TYPE = "content-type";
    public static final String ARG_CONTENT_ENCODING = "content-encoding";
    public static final String ARG_CONTENT_MD5 = "content-md5";
    public static final String ARG_RECURSIVE = "recursive";
    public static final String ARG_LOCK = "lock";
    public static final String ARG_QUICK = "quick";
    public static final String ARG_INHERIT_PERMISSIONS = "inheritPermissions";

    public static final String VOS_PREFIX = "vos://";

    private static Logger log = Logger.getLogger(Main.class);
    private static final int INIT_STATUS = 1; // exit code for initialisation failure
    private static final int NET_STATUS = 2;  // exit code for client-server failures

    //private static final int MAX_CHILD_SIZE = 1000;

    /**
     * Supported node type
     */
    public enum NodeType {
        CONTAINER_NODE, LINK_NODE, DATA_NODE
    }

    /**
     * Operations of VoSpace Client.
     */
    public enum Operation {
        VIEW, CREATE, DELETE, SET, COPY, MOVE
    }

    private NodeType nodeType;
    private Operation operation;
    private VOSURI target;
    private Set<NodeProperty> properties;
    private String contentType;
    private String contentEncoding;

    private URI source;
    private URI destination;
    private Direction transferDirection = null;
    private VOSpaceClient client = null;
    private Subject subject;
    private boolean retryEnabled = false;
    private boolean quickTransfer = false;

    private boolean recursiveMode = false;

    private boolean inheritPermissions;
    private URI link;

    /**
     * @param args  The arguments passed into this command.
     */
    public static void main(String[] args) {
        ArgumentMap argMap = new ArgumentMap(args);
        Main command = new Main();

        if (argMap.isSet(ARG_HELP) || argMap.isSet(ARG_H)) {
            usage();
            System.exit(0);
        }

        // Set debug mode
        if (argMap.isSet(ARG_DEBUG) || argMap.isSet(ARG_D)) {
            Log4jInit.setLevel("org.opencadc.vospace", Level.DEBUG);
            Log4jInit.setLevel("ca.nrc.cadc.net", Level.DEBUG);
            //Log4jInit.setLevel("ca.nrc.cadc.reg", Level.DEBUG);
        } else if (argMap.isSet(ARG_VERBOSE) || argMap.isSet(ARG_V)) {
            Log4jInit.setLevel("org.opencadc.vospace", Level.INFO);
            Log4jInit.setLevel("ca.nrc.cadc.net", Level.INFO);
            //Log4jInit.setLevel("ca.nrc.cadc.reg", Level.INFO);
        } else {
            Log4jInit.setLevel("ca", Level.WARN);
        }

        try {
            command.validateCommand(argMap);
            command.validateCommandArguments(argMap);
        } catch (IllegalArgumentException ex) {
            msg("illegal argument(s): " + ex.getMessage());
            msg("");
            usage();
            System.exit(INIT_STATUS);
        }

        try {
            command.init(argMap);
            log.debug("calling subject: " + command.subject);
            Subject.doAs(command.subject, new RunnableAction(command));
        } catch (IllegalArgumentException ex) {
            msg("illegal arguments(s): " + ex.getMessage());
            msg("");
            System.exit(INIT_STATUS);
        } catch (Throwable t) {
            log.error("unexpected failure", t);
            System.exit(NET_STATUS);
        }
        System.exit(0);
    }

    // encapsulate all messages to console here
    private static void msg(String s) {
        System.out.println(s);
    }

    public void run() {
        log.debug("run - START");
        if (this.operation.equals(Operation.CREATE)) {
            doCreate();
        } else if (this.operation.equals(Operation.DELETE)) {
            doDelete();
        } else if (this.operation.equals(Operation.VIEW)) {
            doView();
        } else if (this.operation.equals(Operation.COPY)) {
            doCopy();
        } else if (this.operation.equals(Operation.MOVE)) {
            doMove();
        } else if (this.operation.equals(Operation.SET)) {
            if (recursiveMode) {
                doRecursiveSet();
            } else {
                doSet();
            }
        }
        log.debug("run - DONE");
    }

    private void doSet() {
        log.debug("doSet");
        try {
            log.debug("target.getPath()" + this.target.getPath());
            // TODO: here we get the node in order to figure out the type, but
            // maybe we could just POST a vanilla Node object?
            Node n = this.client.getNode(this.target.getPath(), "limit=0");
            Node up = null;
            if (n instanceof ContainerNode) {
                up = new ContainerNode(target.getName());
                up.getProperties().addAll(properties);
            } else if (n instanceof DataNode) {
                up = new DataNode(target.getName());
                up.getProperties().addAll(properties);
            } else if (n instanceof LinkNode) {
                URI link = ((LinkNode) n).getTarget();
                up = new LinkNode(target.getName(), link);
                up.getProperties().addAll(properties);
            } else {
                throw new UnsupportedOperationException("unexpected node type: " + n.getClass().getName());
            }

            this.client.setNode(this.target, up);
            log.info("updated properties: " + this.target);
        } catch (ResourceNotFoundException ex) {
            msg("not found: " + target);
            System.exit(NET_STATUS);
        } catch (Throwable t) {
            msg("failed to set properties on node: " + target);
            if (t.getMessage() != null) {
                msg("          reason: " + t.getMessage());
            } else {
                msg("          reason: " + t);
            }
            if (t.getCause() != null) {
                msg("          reason: " + t.getCause());
            }
            System.exit(NET_STATUS);
        }
    }

    private void doDelete() {
        log.debug("doDelete");
        try {
            log.debug("target.getPath()" + this.target.getPath());
            this.client.deleteNode(this.target.getPath());
            log.info("deleted: " + this.target);
        } catch (NodeLockedException nlex) {
            msg("node locked: " + target);
            System.exit(NET_STATUS);
        } catch (Throwable t) {
            msg("failed to delete: " + target);
            if (t.getMessage() != null) {
                msg("          reason: " + t.getMessage());
            } else {
                msg("          reason: " + t);
            }
            if (t.getCause() != null) {
                msg("          reason: " + t.getCause());
            }
            System.exit(NET_STATUS);
        }
    }

    private void doCopy() {
        log.debug("doCopy");
        try {
            if (this.transferDirection.equals(Direction.pushToVoSpace)) {
                copyToVOSpace();
            } else if (this.transferDirection.equals(Direction.pullFromVoSpace)) {
                copyFromVOSpace();
            }
        } catch (NullPointerException ex) {
            log.error("BUG", ex);
            System.exit(NET_STATUS);
        } catch (Throwable t) {
            log.debug(t);
            if (t instanceof IllegalArgumentException) {
                throw (IllegalArgumentException) t;
            } else {
                msg("failed to copy: " + source + " -> " + destination);
                if (t.getCause() != null) {
                    if (t.getCause().getMessage() != null) {
                        msg("          reason: " + t.getCause().getMessage());
                    } else {
                        msg("          reason: " + t.getCause());
                    }
                } else {
                    if (t.getMessage() != null) {
                        msg("          reason: " + t.getMessage());
                    } else {
                        msg("          reason: " + t);
                    }
                }
            }
            System.exit(NET_STATUS);
        }
    }

    private void doMove() {
        log.debug("doMove");
        try {
            if (Direction.pushToVoSpace.equals(transferDirection)) {
                moveToVOSpace();
            } else if (Direction.pullFromVoSpace.equals(transferDirection)) {
                moveFromVOSpace();
                // TODO: cofirm copy worked by checking MD5, length
                // TODO: delete src file from VOSpace, see TODO below
            } else { //if (this.transferDirection.getValue().startsWith(VOS_PREFIX))
                moveWithinVOSpace();
            }
        } catch (NullPointerException ex) {
            log.error("BUG", ex);
            System.exit(NET_STATUS);
        /* TODO: Add this catch when we add delete src file from VOSpace for
         *       Direction.pullFromVOSpace (see TODO above)
        catch(NodeLockedException nlex)
        {
            if (destination == null)
                msg("alinga-- failed to move: " + source + " -> " + transferDirection.getValue());
            else
                msg("alinga-- failed to move: " + source + " -> " + destination);
            msg("          reason: " + nlex.getMessage());
            System.exit(NET_STATUS);
        }
        */
        } catch (Throwable t) {
            if (t instanceof IllegalArgumentException) {
                throw (IllegalArgumentException) t;
            } else {
                if (destination == null) {
                    msg("failed to move: " + source + " -> " + transferDirection.getValue());
                } else {
                    msg("failed to move: " + source + " -> " + destination);
                }
                if (t.getCause() != null) {
                    if (t.getCause().getMessage() != null) {
                        msg("          reason: " + t.getCause().getMessage());
                    } else {
                        msg("          reason: " + t.getCause());
                    }
                } else {
                    if (t.getMessage() != null) {
                        msg("          reason: " + t.getMessage());
                    } else {
                        msg("          reason: " + t);
                    }
                }
            }
            System.exit(NET_STATUS);
        }
    }

    private void doCreate() {
        try {
            Node node;
            switch (this.nodeType) {
                case CONTAINER_NODE:
                    ContainerNode cn = new ContainerNode(this.target.getName());
                    cn.inheritPermissions = inheritPermissions;
                    node = cn;
                    break;
                case LINK_NODE:
                    node = new LinkNode(this.target.getName(), this.link);
                    break;
                case DATA_NODE:
                    node = new DataNode(this.target.getName());
                    break;
                default:
                    throw new RuntimeException("BUG. Unsupported node type " + this.nodeType);
            }
            node.getProperties().addAll(this.properties);

            Node nodeRtn = client.createNode(this.target, node, false);
            log.info("created: " + nodeRtn.getName());
        } catch (Throwable t) {
            msg("failed to create: " + target);
            if (t.getMessage() != null) {
                msg("          reason: " + t.getMessage());
            } else {
                msg("          reason: " + t);
            }
            if (t.getCause() != null) {
                msg("          reason: " + t.getCause());
            }
            System.exit(NET_STATUS);
        }
    }

    private void doView() {
        
        try {
            Node n = client.getNode(target.getPath());

            msg(getType(n) + ": " + target);
            msg("owner: " + n.ownerDisplay);
            msg("last modified: " + safePropertyRef(n, VOS.PROPERTY_URI_DATE));
            msg("size: " + getContentLength(n,true));
            msg("is locked: " + n.isLocked);
            msg("is public: " + n.isPublic);
            msg("readable by:");
            for (GroupURI ro : n.getReadOnlyGroup()) {
                msg("\t" + ro.getURI());
            }
            msg("readable and writable by:");
            for (GroupURI rw : n.getReadWriteGroup()) {
                msg("\t" + rw.getURI());
            }

            if (n instanceof ContainerNode) {
                final String quotaSize = safePropertyRef(n, VOS.PROPERTY_URI_QUOTA);

                if (StringUtil.hasText(quotaSize)) {
                    msg("quota size: " + FileSizeType.getHumanReadableSize(Long.parseLong(quotaSize))
                            + " (" + quotaSize + " bytes)");
                }

                ContainerNode cn = (ContainerNode) n;
                if (!cn.getNodes().isEmpty()) {
                    StringBuilder sb = new StringBuilder();
                    sb.append(pad("child nodes: ", 32));
                    sb.append(pad("size",12));
                    sb.append(pad("public",8));
                    sb.append(pad("owner",12));
                    sb.append(pad("last modified",26));
                    msg(sb.toString());
                }

                log.debug("get container node returned : " + cn.getNodes().size() + " children.");
                printChildList(target, cn.getNodes());

            } else if (n instanceof DataNode) {
                msg("type: " + safePropertyRef(n, VOS.PROPERTY_URI_TYPE));
                msg("encoding: " + safePropertyRef(n, VOS.PROPERTY_URI_CONTENTENCODING));
                msg("md5sum: " + safePropertyRef(n, VOS.PROPERTY_URI_CONTENTMD5));
            } else if (n instanceof LinkNode) {
                msg("link uri: " + ((LinkNode) n).getTarget());
            } else {
                log.debug("class of returned node: " + n.getClass().getName());
            }
        } catch (AccessControlException ex) {
            msg("permission denied: " + target);
            System.exit(NET_STATUS);
        } catch (ResourceNotFoundException ex) {
            msg("not found: " + target);
            System.exit(NET_STATUS);
        } catch (Throwable t) {
            msg("failed to view: " + target);
            if (t.getMessage() != null) {
                msg("          reason: " + t.getMessage());
            } else {
                msg("          reason: " + t);
            }
            if (t.getCause() != null) {
                msg("          reason: " + t.getCause());
            }
            System.exit(NET_STATUS);
        }
    }

    private void printChildList(VOSURI parent, List<Node> children) {
        StringBuilder sb = null;
        for (Node child : children) {
            sb = new StringBuilder();
            String name = child.getName();
            if (child instanceof ContainerNode) {
                name += "/";
            }
            String pub = (child.isPublic == null ? "false" : child.isPublic.toString());
            VOSURI childURI = NodeUtil.getChildURI(parent, child.getName());
            sb.append(pad(name,32));
            sb.append(pad(getContentLength(child,true),12));
            sb.append(pad(pub,8));
            sb.append(pad(child.ownerDisplay,12));
            sb.append(pad(safePropertyRef(child, VOS.PROPERTY_URI_DATE),26));
            msg(sb.toString());
        }
    }

    private String pad(String s, int len) {
        if (s.length() >= len) {
            len = s.length() + 1;
        }
        StringBuilder sb = new StringBuilder();
        sb.append(s);
        for (int i = s.length(); i < len; i++) {
            sb.append(" ");
        }
        return sb.toString();
    }

    private String getContentLength(Node n, boolean simple) {
        String contentLength = safePropertyRef(n, VOS.PROPERTY_URI_CONTENTLENGTH);
        if (!StringUtil.hasText(contentLength)) {
            return "0";
        }
        if (simple) {
            return contentLength;
        }
        return FileSizeType.getHumanReadableSize(Long.parseLong(contentLength))
                + " (" + contentLength + " bytes)";
    }

    private void copyToVOSpace()
        throws Throwable {
        
        // upload with view doesn't actually make any sense, maybe OBSOLETE?
        //URI originalDestination = null;
        //if (StringUtil.hasText(destination.getQuery())) {
        //    originalDestination = new URI(destination.toString());
        //    destination = new URI(destination.toString().replace("?" + destination.getQuery(), ""));
        //}
        //final View view = new View(VOS.VIEW_DEFAULT);
        //if (originalDestination != null) {
        //    view = createAcceptsView(new VOSURI(originalDestination), null);
        //}

        Protocol proto = null;
        if (subject != null) {
            proto = new Protocol(VOS.PROTOCOL_HTTPS_PUT);
            proto.setSecurityMethod(Standards.SECURITY_METHOD_CERT);
        } else {
            proto = new Protocol(VOS.PROTOCOL_HTTPS_PUT);
        }
        log.debug("copyToVOSpace: " + proto);
        List<Protocol> protocols = new ArrayList<>();
        protocols.add(proto);

        log.debug("this.source: " + source);
        File fileToUpload = new File(source);

        Transfer transfer = new Transfer(destination, Direction.pushToVoSpace);
        transfer.setContentLength(fileToUpload.length());
        transfer.getProtocols().addAll(protocols);
        //transfer.setView(view);
        transfer.setQuickTransfer(this.quickTransfer);
        transfer.version = VOS.VOSPACE_21;

        ClientTransfer clientTransfer = client.createTransfer(transfer);

        // set http headers for put
        if (contentType != null) {
            log.debug("copyToVOSpaceFast: setting content-type = " + contentType);
            clientTransfer.setRequestProperty("Content-Type", contentType);
        }
        if (contentEncoding != null) {
            log.debug("copyToVOSpaceFast: setting content-encoding = " + contentEncoding);
            clientTransfer.setRequestProperty("Content-Encoding", contentEncoding);
        }

        if (retryEnabled) {
            clientTransfer.setMaxRetries(Integer.MAX_VALUE);
        }
        clientTransfer.setTransferListener(new VOSpaceTransferListener(false));
        clientTransfer.setFile(fileToUpload);

        clientTransfer.runTransfer();
        if (!quickTransfer) {
            checkPhase(clientTransfer);
        }
        Node node = client.getNode(destination.getPath());

        boolean checkProps = contentType != null || contentEncoding != null || !properties.isEmpty();
        if (checkProps || log.isDebugEnabled()) {
            VOSURI destinationURI = new VOSURI(destination);
            log.debug("clientTransfer getTarget: " + node);
            log.debug("Node returned from getNode, after doUpload: " + VOSClientUtil.xmlString(destinationURI, node,
                      VOS.Detail.max));
            if (checkProps) {
                log.debug("checking properties after put: " + node.getName());
                boolean updateProps = false;
                for (NodeProperty np : properties) {
                    updateProps = updateProps || updateProperty(node, np.getKey(), np.getValue());
                }
                if (updateProps) {
                    log.debug("updating properties after put: " + node.getName());
                    client.setNode(destinationURI, node);
                }
            }
        }
    }

    private boolean updateProperty(Node node, URI key, String value) {
        log.debug("checking property: " + key + " vs " + value);
        boolean ret = false;
        if (value != null) {
            NodeProperty cur = node.getProperty(key);
            if (cur == null) {
                log.debug("adding property: " + key + " = " + value);
                node.getProperties().add(new NodeProperty(key, value));
                ret = true;
            } else if (!value.equals(cur.getValue())) {
                log.debug("setting property: " + key + " = '" + value
                              + "', was '" + cur.getValue() + "'");
                cur.setValue(value);
                ret = true;
            }
        }
        return ret;
    }

    private void copyFromVOSpace()
        throws Throwable {
        final View view = new View(VOS.VIEW_DEFAULT);
        //if (StringUtil.hasText(source.getQuery())) {
        //    view = createProvidesView(new VOSURI(source), null);
        //    source = new URI(source.toString().replace("?" + source.getQuery(), ""));
        //}

        Protocol proto = null;
        if (subject != null) {
            proto = new Protocol(VOS.PROTOCOL_HTTPS_GET);
            proto.setSecurityMethod(Standards.SECURITY_METHOD_CERT);
        } else {
            proto = new Protocol(VOS.PROTOCOL_HTTP_GET);
        }
        log.debug("copyFromVOSpace: " + proto);
        List<Protocol> protocols = new ArrayList<Protocol>();
        protocols.add(proto);

        Transfer transfer = new Transfer(source, Direction.pullFromVoSpace);
        transfer.getProtocols().addAll(protocols);
        transfer.setView(view);
        transfer.setQuickTransfer(this.quickTransfer);
        transfer.version = VOS.VOSPACE_21; // testing VOSpace-2.1
        ClientTransfer clientTransfer = client.createTransfer(transfer);

        log.debug("this.source: " + source);
        File fileToSave = new File(destination);
        if (fileToSave.exists()) {
            log.info("overwriting existing file: " + destination);
        }

        if (retryEnabled) {
            clientTransfer.setMaxRetries(Integer.MAX_VALUE);
        }
        clientTransfer.setTransferListener(new VOSpaceTransferListener(true));
        clientTransfer.setFile(fileToSave);

        clientTransfer.runTransfer();
        if (!quickTransfer) {
            checkPhase(clientTransfer);
        }
    }

    private void moveToVOSpace()
        throws Throwable {
        File sourceFile = new File(source);
        if (!sourceFile.isFile()) {
            msg("Cannot move local directories to VOSpace.");
            System.exit(-1);
        }
        copyToVOSpace();
        log.debug("copied local file " + source + " to " + this.destination.getPath());
        Node uploadedNode = this.client.getNode(this.destination.getPath(), "limit=0");
        if (uploadedNode == null) {
            msg("Failed to upload, keeping local file.");
            System.exit(-1);
        }
        NodeProperty uploadedSizeProp = uploadedNode.getProperty(VOS.PROPERTY_URI_CONTENTLENGTH);
        long uploadedSize = Long.parseLong(uploadedSizeProp.getValue());
        log.debug("uploaded size: " + uploadedSize);
        log.debug("original size: " + sourceFile.length());
        if (uploadedSize != sourceFile.length()) {
            msg("Uploaded file size does not match that of local file, keeping local file.");
            System.exit(-1);
        }
        sourceFile.delete();
        log.debug("deleted local file: " + source);
    }

    private void moveFromVOSpace()
        throws Throwable {
        Node sourceNode = this.client.getNode(this.source.getPath(), "limit=0");
        if (sourceNode instanceof ContainerNode) {
            msg("Cannot move containers from VOSpace to local file system.");
            System.exit(-1);
        }
        copyFromVOSpace();
        log.debug("copied " + this.destination.getPath() + " to local file " + source);

        NodeProperty downloadedSizeProp = sourceNode.getProperty(VOS.PROPERTY_URI_CONTENTLENGTH);
        File destFile = new File(destination);
        long downloadedSize = Long.parseLong(downloadedSizeProp.getValue());
        log.debug("downloaded size: " + downloadedSize);
        log.debug("original size: " + destFile.length());
        if (downloadedSize != destFile.length()) {
            msg("Downloaded file size does not match that of local file, keeping remote file.");
            System.exit(-1);
        }
        this.client.deleteNode(this.source.getPath());
        log.debug("deleted vos file: " + this.source);
    }

    private void doRecursiveSet() {
        try {
            log.debug("target.getPath()" + this.target.getPath());
            // TODO: here we get the node in order to figure out the type, but
            // maybe we could just POST a vanilla Node object?
            Node n = this.client.getNode(this.target.getPath(), "limit=0");
            Node up = null;
            if (n instanceof ContainerNode) {
                up = new ContainerNode(target.getName());
            } else if (n instanceof DataNode) {
                up = new DataNode(target.getName());
            } else if (n instanceof LinkNode) {
                URI link = ((LinkNode) n).getTarget();
                up = new LinkNode(target.getName(), link);
            } else {
                throw new UnsupportedOperationException("unexpected node type: " + n.getClass().getName());
            }
            up.getProperties().addAll(properties);

            ClientRecursiveSetNode recSetNode = client.setNodeRecursive(target, up);

            Thread abortThread = new ClientAbortThread(recSetNode.getJobURL());
            Runtime.getRuntime().addShutdownHook(abortThread);
            recSetNode.setMonitor(true);
            recSetNode.run();
            Runtime.getRuntime().removeShutdownHook(abortThread);
            checkPhase(recSetNode);

            log.info("updated properties recursively: " + this.target);
        } catch (ResourceNotFoundException ex) {
            msg("not found: " + target);
            System.exit(NET_STATUS);
        } catch (Throwable t) {
            msg("failed to set properties recursively on node: " + target);
            if (t.getMessage() != null) {
                msg("          reason: " + t.getMessage());
            } else {
                msg("          reason: " + t);
            }
            if (t.getCause() != null) {
                msg("          reason: " + t.getCause());
            }
            System.exit(NET_STATUS);
        }
    }

    private void moveWithinVOSpace()
        throws Throwable {
        VOSURI dest = new VOSURI(destination);
        Transfer transfer = new Transfer(source, dest.getURI(), false);
        ClientTransfer trans = client.createTransfer(transfer);

        Thread abortThread = new ClientAbortThread(trans.getJobURL());
        Runtime.getRuntime().addShutdownHook(abortThread);
        trans.setMonitor(true);
        trans.runTransfer();
        Runtime.getRuntime().removeShutdownHook(abortThread);
        checkPhase(trans);
    }

    private void checkPhase(ClientTransfer trans)
            throws IOException, RuntimeException {
        ExecutionPhase ep = trans.getPhase();
        if (ExecutionPhase.ERROR.equals(ep)) {
            ErrorSummary es = trans.getServerError();
            throw new RuntimeException(es.getSummaryMessage());
        } else if (ExecutionPhase.ABORTED.equals(ep)) {
            throw new RuntimeException("transfer aborted by service");
        } else {
            log.info("final transfer job state: " + ep.name());
        }
    }

    private void checkPhase(ClientRecursiveSetNode recSetNode)
        throws IOException, RuntimeException {
        ExecutionPhase ep = recSetNode.getPhase();
        if (ExecutionPhase.ERROR.equals(ep)) {
            ErrorSummary es = recSetNode.getServerError();
            throw new RuntimeException(es.getSummaryMessage());
        } else if (ExecutionPhase.ABORTED.equals(ep)) {
            throw new RuntimeException("recursive set node aborted by service");
        } else if (!ExecutionPhase.COMPLETED.equals(ep)) {
            log.warn("unexpected job state: " + ep.name());
        }
    }

    /*
    private View createAcceptsView(VOSURI vosuri, Node node) 
        throws URISyntaxException {
        AcceptsProvidesAbstraction nodeViewWrapper = new AcceptsProvidesAbstraction() {
            public List<URI> getViews(Node node) { 
                return node.accepts;
            }
        };
        return createView(vosuri, nodeViewWrapper, node);
    }

    
    private View createProvidesView(VOSURI vosuri, Node node) 
        throws URISyntaxException {
        AcceptsProvidesAbstraction nodeViewWrapper = new AcceptsProvidesAbstraction() {
            public List<URI> getViews(Node node) { 
                return node.provides;
            }
        };
        return createView(vosuri, nodeViewWrapper, node);
    }
    */
    
    /*
    private View createView(VOSURI vosURI, AcceptsProvidesAbstraction acceptsOrProvides, Node node)
            throws URISyntaxException {
        // parse the query string
        String queryString = vosURI.getQuery();
        final String viewKey = "view=";
        String[] queries = queryString.split("&");
        String viewRef = null;
        List<String> params = new ArrayList<>();
        for (String query : queries) {
            if (query.startsWith(viewKey)) {
                if (viewRef != null) {
                    throw new IllegalArgumentException("Too many view references.");
                }
                viewRef = query.substring(viewKey.length());
            } else {
                params.add(query);
            }
        }
        if (viewRef == null) {
            log.debug("View not found in query string, using default view");
            return null;
        }

        // get the node object if necessary
        if (node == null) {
            try {
                node = client.getNode(vosURI.getPath(), "limit=0");
            } catch (NodeNotFoundException e) {
                throw new IllegalArgumentException("Node " + vosURI.getPath() + " not found.");
            }
        }

        // determine if the view is supported
        URI viewURI = null;
        for (URI uri : acceptsOrProvides.getViews(node)) {
            if (viewRef.equals(uri.getFragment())) {
                viewURI = uri;
            }
        }

        if (viewURI == null) {
            throw new IllegalArgumentException("View '" + viewRef + "' not supported by node " + vosURI);
        }

        // add the view parameters
        View view = new View(viewURI);
        if (params.size() > 0) {
            String viewURIFragment = viewURI.getFragment();
            String paramURIBase = viewURI.toString().replace("#" + viewURIFragment, "");
            for (String param : params) {
                int eqIndex = param.indexOf('=');
                if (eqIndex > 0) {
                    String key = param.substring(0, eqIndex);
                    URI paramURI = new URI(paramURIBase + "#" + key);
                    View.Parameter viewParam = new View.Parameter(paramURI, param.substring(eqIndex + 1));
                    view.getParameters().add(viewParam);
                }
            }
        }
        return view;
    }
    */
    
    private static String ZERO_LENGTH = "";

    private String safePropertyRef(Node n, URI key) {
        if (n == null || key == null) {
            return ZERO_LENGTH;
        }
        String ret = n.getPropertyValue(key);
        if (ret == null) {
            return ZERO_LENGTH;
        }
        return ret;
    }

    private String getType(Node n) {
        if (n instanceof ContainerNode) {
            return "container";
        }
        if (n instanceof DataNode) {
            return "data";
        }
        if (n instanceof LinkNode) {
            return "link";
        }
        return ZERO_LENGTH;
    }

    /**
     * Initialize command member variables based on arguments passed in.
     *
     * @param argMap    The parsed arguments to this command.
     */
    private void init(ArgumentMap argMap) {
        URI serverUri = null;
        try {
            // setup optional authentication for harvesting from a web service
            this.subject = AuthenticationUtil.getAnonSubject();
            if (argMap.isSet("netrc")) {
                this.subject = AuthenticationUtil.getSubject(new NetrcAuthenticator(true));
            } else if (argMap.isSet("cert")) {
                // no default finding cert in known location
                this.subject = CertCmdArgUtil.initSubject(argMap);
            }
            AuthMethod meth = AuthenticationUtil.getAuthMethodFromCredentials(subject);
            log.info("authentication using: " + meth);
            
            // check that loaded certficate chain is valid right now
            // TODO: should this be moved into CertCmdArgUtil?
            if (subject != null) {
                Set<X509CertificateChain> certs = subject.getPublicCredentials(X509CertificateChain.class);
                if (!certs.isEmpty()) {
                    DateFormat df = DateUtil.getDateFormat(DateUtil.ISO_DATE_FORMAT, DateUtil.LOCAL);
                    X509CertificateChain chain = certs.iterator().next(); // the first one
                    Date start = null;
                    Date end = null;
                    for (X509Certificate c : chain.getChain()) {
                        try {
                            start = c.getNotBefore();
                            end = c.getNotAfter();
                            c.checkValidity();
                        } catch (CertificateNotYetValidException exp) {
                            log.error("certificate is not valid yet (valid from " + df.format(start)
                                          + " to " + df.format(end) + ")");
                            System.exit(INIT_STATUS);
                        } catch (CertificateExpiredException exp) {
                            log.error("certificate has expired (valid from " + df.format(start)
                                          + " to " + df.format(end) + ")");
                            System.exit(INIT_STATUS);
                        }
                    }
                }
            } else {
                this.subject = AuthenticationUtil.getAnonSubject();
            }
        } catch (Exception ex) {
            log.error("failed to load certificates: " + ex.getMessage());
            System.exit(INIT_STATUS);
        }


        try {
            List<String> args = argMap.getPositionalArgs();
            if (this.operation.equals(Operation.COPY) || this.operation.equals(Operation.MOVE)) {
                if (this.operation.equals(Operation.COPY) && argMap.isSet(ARG_QUICK)) {
                    this.quickTransfer = true;
                }
                
                if (args.size() != 2) {
                    throw new IllegalArgumentException(operation + " requires 2 positional args, found: " + args.size());
                }
                String strSrc = args.get(0);
                String strDest = args.get(1);
                if (!strSrc.startsWith(VOS_PREFIX) && strDest.startsWith(VOS_PREFIX)) {
                    this.transferDirection = Direction.pushToVoSpace;
                    try {
                        this.destination = new URI(strDest);
                        serverUri = new VOSURI(strDest).getServiceURI();
                    } catch (URISyntaxException e) {
                        throw new IllegalArgumentException("Invalid VOS URI: " + strDest);
                    }
                    File f = new File(strSrc);
                    if (!f.exists() || !f.canRead()) {
                        throw new IllegalArgumentException("Source file " + strSrc
                                                               + " does not exist or cannot be read.");
                    }

                    try {
                        this.source = new URI("file", f.getAbsolutePath(), null);
                    } catch (URISyntaxException e) {
                        throw new IllegalArgumentException("Invalid file path: " + strSrc);
                    }
                } else if (strSrc.startsWith(VOS_PREFIX) && !strDest.startsWith(VOS_PREFIX)) {
                    this.transferDirection = Direction.pullFromVoSpace;
                    try {
                        serverUri = new VOSURI(strSrc).getServiceURI();
                        this.source = new URI(strSrc);
                    } catch (URISyntaxException e) {
                        throw new IllegalArgumentException("Invalid VOS URI: " + strSrc);
                    }
                    File f = new File(strDest);
                    if (f.exists()) {
                        if (!f.canWrite()) {
                            throw new IllegalArgumentException("Destination file " + strDest + " is not writable.");
                        }
                    } else {
                        File parent = f.getParentFile();
                        if (parent == null) {
                            String cwd = System.getProperty("user.dir");
                            parent = new File(cwd);
                        }
                        if (parent.isDirectory()) {
                            if (!parent.canWrite()) {
                                throw new IllegalArgumentException("The parent directory of destination file "
                                                                       + strDest + " is not writable.");
                            }
                        } else {
                            throw new IllegalArgumentException("Destination file "
                                                                   + strDest + " is not within a directory.");
                        }
                    }
                    this.destination = f.toURI();
                } else if (!strSrc.startsWith(VOS_PREFIX) && !strDest.startsWith(VOS_PREFIX)) {
                    // local copy/move
                    throw new IllegalArgumentException("Local copy and move operations not yet supported.");
                } else {
                    // server to server copy/move
                    if (this.operation.equals(Operation.COPY)) {
                        throw new IllegalArgumentException("Copy within vospace is not yet supported.");
                    }

                    URI destServerUri = null;
                    try {
                        this.source = new URI(strSrc);
                        serverUri = new VOSURI(source).getServiceURI();
                    } catch (URISyntaxException e) {
                        throw new IllegalArgumentException("Invalid VOS URI: " + strSrc);
                    }
                    try {
                        this.destination = new URI(strDest);
                        destServerUri = new VOSURI(destination).getServiceURI();
                    } catch (URISyntaxException e) {
                        throw new IllegalArgumentException("Invalid VOS URI: " + strDest);
                    }

                    if (!serverUri.equals(destServerUri)) {
                        throw new IllegalArgumentException("Move between two vospace services is not yet supported.");
                    }
                }
            } else {
                if (args.size() != 1) {
                    throw new IllegalArgumentException(operation + " requires 1 positional arg, found: " + args.size());
                }
                String strTarget = args.get(0);
                try {
                    this.target = new VOSURI(strTarget);
                    serverUri = this.target.getServiceURI();
                } catch (URISyntaxException e) {
                    throw new IllegalArgumentException("Invalid VOS URI: " + strTarget);
                }
            }
        } catch (NullPointerException nex) {
            log.error("BUG", nex);
            System.exit(-1);
        } catch (Exception ex) {
            log.error(ex.toString());
            System.exit(INIT_STATUS);
        }

        // check if schema validation should be disabled
        boolean doVal = true;
        String schemaVal = argMap.getValue(ARG_XSV);
        if (schemaVal != null && "off".equals(schemaVal)) {
            doVal = false;
            log.info("XML schema validation: disabled");
        }

        this.client = new VOSpaceClient(serverUri, doVal);

        this.retryEnabled = !argMap.isSet(ARG_NO_RETRY);

        log.info("vospace: " + serverUri + " @ " + client.getBaseURL());
    }

    /**
     * @param argMap  The parsed arguments to this command.
     * @throws IllegalArgumentException
     *                  If more or less than one operation was requested.
     */
    private void validateCommand(ArgumentMap argMap)
            throws IllegalArgumentException {
        int numOp = 0;
        if (argMap.isSet(ARG_VIEW)) {
            numOp++;
            this.operation = Operation.VIEW;
        }
        if (argMap.isSet(ARG_CREATE)) {
            numOp++;
            this.operation = Operation.CREATE;
        }
        if (argMap.isSet(ARG_DELETE)) {
            numOp++;
            this.operation = Operation.DELETE;
        }
        if (argMap.isSet(ARG_SET)) {
            numOp++;
            this.operation = Operation.SET;
        }
        if (argMap.isSet(ARG_COPY)) {
            numOp++;
            this.operation = Operation.COPY;
        }
        if (argMap.isSet(ARG_MOVE)) {
            numOp++;
            this.operation = Operation.MOVE;
        }

        if (numOp == 0) {
            throw new IllegalArgumentException("At least one operation must be defined.");
        } else if (numOp > 1) {
            throw new IllegalArgumentException("Only one operation may be defined.");
        }
    }

    /**
     * @param argMap        The parsed out arguments to this command.
     * @throws IllegalArgumentException  For any required missing arguments.
     */
    private void validateCommandArguments(ArgumentMap argMap)
        throws IllegalArgumentException {
        List<String> args = argMap.getPositionalArgs();
        if (this.operation.equals(Operation.COPY) || this.operation.equals(Operation.MOVE)) {
            if (args.size() != 2) {
                throw new IllegalArgumentException(operation + " requires 2 positional args, found: " + args.size());
            }
        } else {
            if (args.size() != 1) {
                throw new IllegalArgumentException(operation + " requires 1 positional arg, found: " + args.size());
            }
            
            String strTarget = args.get(0);
            if (this.operation.equals(Operation.CREATE)) {
                // create default (true) is a ContainerNode
                String strNodeType = argMap.getValue(ARG_CREATE);
                if (("true".equalsIgnoreCase(strNodeType))
                    || (ContainerNode.class.getSimpleName().equalsIgnoreCase(strNodeType))) {
                    this.nodeType = NodeType.CONTAINER_NODE;
                    String inheritPermissions = argMap.getValue(ARG_INHERIT_PERMISSIONS);
                    if (inheritPermissions == null) {
                        throw new IllegalArgumentException("Argument --inheritPermissions is required for ContainerNode");
                    }
                    this.inheritPermissions = Boolean.parseBoolean(inheritPermissions);
                } else if ((DataNode.class.getSimpleName().equalsIgnoreCase(strNodeType))) {
                    this.nodeType = NodeType.DATA_NODE;
                } else if (LinkNode.class.getSimpleName().equalsIgnoreCase(strNodeType)) {
                    this.nodeType = NodeType.LINK_NODE;
                    String link = argMap.getValue(ARG_LINK);
                    if (link == null) {
                        throw new IllegalArgumentException("Argument --link is required for LinkNode");
                    }
                    try {
                        this.link = new URI(link);
                    } catch (URISyntaxException e) {
                        throw new IllegalArgumentException("Invalid LinkNode --link URI: " + link);
                    }
                } else {
                    throw new IllegalArgumentException("Unsupported node type: " + strNodeType);
                }
            }
        }

        // optional properties
        this.properties = new TreeSet<NodeProperty>();

        String propFile = argMap.getValue(ARG_PROP);
        if (propFile != null) {
            File f = new File(propFile);
            if (f.exists()) {
                if (f.canRead()) {
                    try {
                        Properties p = new Properties();
                        p.load(new FileReader(f));
                        for (String key : p.stringPropertyNames()) {
                            URI keyURI;
                            try {
                                keyURI = new URI(key);
                            } catch (URISyntaxException e) {
                                throw new IllegalArgumentException("invalid node properties key: " + key);
                            }
                            String val = p.getProperty(key);
                            properties.add(new NodeProperty(keyURI, val));
                        }
                    } catch (IOException ex) {
                        log.info("failed to read properties file: " + f.getAbsolutePath()
                                     + "(" + ex.getMessage() + ", skipping)");
                    }
                } else {
                    log.info("cannot read properties file: " + f.getAbsolutePath()
                                 + " (permission denied, skipping)");
                }
            } else {
                log.info("cannot read properties file: " + f.getAbsolutePath()
                             + " (does not exist, skipping)");
            }
        }

        this.contentType = argMap.getValue(ARG_CONTENT_TYPE);
        this.contentEncoding = argMap.getValue(ARG_CONTENT_ENCODING);

        // support --public and --public=true; everything else sets it to false
        boolean isPublicSet = argMap.isSet(ARG_PUBLIC);
        boolean isPublicValue = true;
        if (isPublicSet) {
            String s = argMap.getValue(ARG_PUBLIC);
            if (s != null  && s.trim().length() > 0
                && !s.trim().equalsIgnoreCase("true")) {
                if (s.equalsIgnoreCase("false")) {
                    isPublicValue = false;
                } else {
                    isPublicSet = false;
                    log.info("--public value not recognized: " + s.trim() + ".  Ignoring.");
                }
            }
        }

        // support --lock and --lock=true; everything else sets it to false
        boolean isLockSet = argMap.isSet(ARG_LOCK);
        boolean isLockValue = true;
        if (isLockSet) {
            String s = argMap.getValue(ARG_LOCK);
            if (s != null  && s.trim().length() > 0 && !s.trim().equalsIgnoreCase("true")) {
                if (s.equalsIgnoreCase("false")) {
                    isLockValue = false;
                } else {
                    isLockSet = false;
                    log.info("--lock value not recognized: " + s.trim() + ".  Ignoring.");
                }
            }
        }

        this.recursiveMode = argMap.isSet(ARG_RECURSIVE);

        if (contentType != null) {
            properties.add(new NodeProperty(VOS.PROPERTY_URI_TYPE, contentType));
        }
        if (contentEncoding != null) {
            properties.add(new NodeProperty(VOS.PROPERTY_URI_CONTENTENCODING, contentEncoding));
        }

        String contentMD5 = argMap.getValue(ARG_CONTENT_MD5);
        if (contentMD5 != null) {
            properties.add(new NodeProperty(VOS.PROPERTY_URI_CONTENTMD5, contentMD5));
        }

        String groupRead = argMap.getValue(ARG_GROUP_READ);
        if (groupRead != null) {
            properties.add(new NodeProperty(VOS.PROPERTY_URI_GROUPREAD, groupRead));
        }

        String groupWrite = argMap.getValue(ARG_GROUP_WRITE);
        if (groupWrite != null) {
            properties.add(new NodeProperty(VOS.PROPERTY_URI_GROUPWRITE, groupWrite));
        }

        if (isLockSet) {
            properties.add(new NodeProperty(VOS.PROPERTY_URI_ISLOCKED, Boolean.toString(isLockValue)));
        }

        if (isPublicSet) {
            properties.add(new NodeProperty(VOS.PROPERTY_URI_ISPUBLIC, Boolean.toString(isPublicValue)));
        }
    }

    /**
     * Print the usage report for this command.
     */
    public static void usage() {
        /*
         * Note: When using "Format" in Eclipse, shorter lines in this string array are squeezed into one line.
         * This makes it hard to read or edit.
         *
         * A workaround is, lines are purposely extended with blank spaces to a certain length,
         * where the EOL is at about column 120 (including leading indenting spaces).
         *
         * In this way, it's still easy to read and edit and the formatting operation does not change it's layout.
         *
         */
        String[] um = {
            "",
            "Usage: cadc-vos-client [-v|--verbose|-d|--debug] [--xsv=off] <operation> ...",
            "    <-h | --help>       : view command line help",
            "",
            "authentication options:",
            CertCmdArgUtil.getCertArgUsage(),
            // TODO: token usage
            "",
            "advanced/sketchy options:",
            "    --xsv=off     : disables XML schema validation; use at your own risk",
            "",
            "operations:",
            "    --view <target URI>",
            "    --create[=<ContainerNode|LinkNode|DataNode>] <node URI>  : default: ContainerNode",
            "    --delete <target URI>",
            "    --set <target URI>",
            "    --copy <source URI> <destination URI>",
            "    --move <source URI> <destination URI>",
            "",
            "create and set options:",
            "",
            "    [--inheritPermissions=<true|false>}                                                           ",
            "    [--link=<link URI>]      : the URI to which the LinkNode is pointing",
            "    [--prop=<properties file>]                                                                    ",
            "    [--content-type=<mimetype of source>]       : DataNode only",
            "    [--content-encoding=<encoding of source>]   : DataNode only",
            "    [--group-read=<group URIs (in double quotes, space separated, 4 maximum)>]                    ",
            "    [--group-write=<group URIs (in double quotes, space separated, 4 maximum)>]                   ",
            "    [--lock]                                                                                      ",
            "    [--public]                                                                                    ",
            "    [--prop=<properties file>]                                                                    ",
            "    [--recursive]                                                                                 ",
            "",
            "copy:",
            "",
            "  One of source and destination may be a VOSpace node URI ('vos' URI scheme) and the other may be an",
            "  absolute or relative path to a file.  If the target node does not exist, a                      ",
            "  DataNode is created and data copied.  If it does exist, the data and                            ",
            "  properties are overwritten.",
            "",
            "move:                                                                                   ",
            "",
            "  Both the source and destination must be VOSpace nodes in the same VOSpace service. If the",
            "  source is a ContainerNode, then move is a recursive operation:  the source node and all child",
            "  nodes are moved to the new location. If the destination node is an existing ContainerNode, the",
            "  source node it moved into the destination and retains the same name; otherwise, the source node",
            "  is also renamed by the move."
        };
        for (String line : um) {
            msg(line);
        }
    }

    /**
     * Interface to allow abstraction between accepts and provides views.
     */
    private interface AcceptsProvidesAbstraction {
        List<URI> getViews(Node node);
    }
    
}
