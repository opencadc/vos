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

package org.opencadc.vospace.server.auth;

import ca.nrc.cadc.auth.AuthenticationUtil;
import ca.nrc.cadc.auth.SignedToken;
import ca.nrc.cadc.cred.client.CredUtil;
import ca.nrc.cadc.net.ResourceNotFoundException;
import ca.nrc.cadc.profiler.Profiler;
import java.io.IOException;
import java.net.URI;
import java.security.AccessControlException;
import java.security.Principal;
import java.security.cert.CertificateExpiredException;
import java.security.cert.CertificateNotYetValidException;
import java.util.HashSet;
import java.util.Set;
import javax.security.auth.Subject;
import org.apache.log4j.Logger;
import org.opencadc.gms.GroupURI;
import org.opencadc.gms.IvoaGroupClient;
import org.opencadc.vospace.Node;
import org.opencadc.vospace.NodeLockedException;
import org.opencadc.vospace.NodeProperty;
import org.opencadc.vospace.VOS;
import org.opencadc.vospace.VOSURI;
import org.opencadc.vospace.server.LocalServiceURI;
import org.opencadc.vospace.server.NodePersistence;
import org.opencadc.vospace.server.Utils;


/**
 * Authorization implementation for VO Space.
 *
 * <p>Important:  This class cannot be re-used between HTTP Requests--A new
 * instance must be created each time.
 *
 * <p>The nodePersistence object must be set for every instance.
 *
 * @author majorb
 * @author adriand
 */
public class VOSpaceAuthorizer {
    protected static final Logger log = Logger.getLogger(VOSpaceAuthorizer.class);

    //public static final String MODE_KEY = VOSpaceAuthorizer.class.getName() + ".state";
    //public static final String OFFLINE = "Offline";
    //public static final String OFFLINE_MSG = "System is offline for maintainence";
    //public static final String READ_ONLY = "ReadOnly";
    //public static final String READ_ONLY_MSG = "System is in read-only mode for maintainence";
    //public static final String READ_WRITE = "ReadWrite";
    
    //private boolean readable = true;
    //private boolean writable = true;

    private final NodePersistence nodePersistence;
    private final IvoaGroupClient gmsClient = new IvoaGroupClient();
    private final Set<GroupURI> callerGroups = new HashSet<>();  //cache of caller groups used to minimize service calls

    private final Profiler profiler = new Profiler(VOSpaceAuthorizer.class);

    private boolean disregardLocks = false;

    public VOSpaceAuthorizer(NodePersistence nodePersistence) {
        if (nodePersistence == null) {
            throw new IllegalStateException("BUG: nodePersistence cannot be null");
        }
        this.nodePersistence = nodePersistence;
    }

    public void setDisregardLocks(boolean disregardLocks) {
        this.disregardLocks = disregardLocks;
    }

    /*
    private void initState() {
        String key = VOSpaceAuthorizer.MODE_KEY;
        String val = System.getProperty(key);
        if (OFFLINE.equals(val)) {
            readable = false;
            writable = false;
        } else if (READ_ONLY.equals(val)) {
            writable = false;
        }
    }

    public void checkServiceStatus(boolean toWrite) {
        initState();
        if (toWrite && !writable) {
            throw new IllegalStateException(READ_ONLY_MSG);
        }
        if (!readable) {
            throw new IllegalStateException(OFFLINE_MSG);
        }
    }
    */
    
    /**
     * Check the read permission on a single node.
     *
     * <p>For full node authorization, use getReadPermission(Node).
     *
     * @param node The node to check.
     * @throws AccessControlException If permission is denied.
     */
    public boolean hasSingleNodeReadPermission(Node node, Subject subject) {
        log.debug("hasSingleNodeReadPermission: " + Utils.getPath(node)
            + "\n" + node);

        if ((node.isPublic != null) && node.isPublic) {
            return true; // OK
        }

        // return true if this is the owner of the node or if a member
        // of the groupRead or groupWrite property
        if (subject != null) {
            if (isOwner(node, subject)) {
                log.debug("Node owner granted read permission.");
                return true; // OK
            }

            checkDelegation(node, subject);

            if (log.isDebugEnabled()) {
                log.debug(String.format(
                        "Checking group read permission on node \"%s\" (groupRead=\"%s\")",
                        node.getName(), node.getReadOnlyGroup()));
            }
            if (applyMaskOnGroupRead(node)) {
                if (hasMembership(node.getReadOnlyGroup(), subject)) {
                    return true; // OK
                }
            }

            // the GROUPWRITE property means the user has read+write permission
            // so check that too
            if (log.isDebugEnabled()) {
                log.debug(String.format(
                        "Checking group write permission on node \"%s\" (groupWrite=\"%s\")",
                        node.getName(), node.getReadWriteGroup()));
            }
            if (applyMaskOnGroupReadWrite(node)) {
                return hasMembership(node.getReadWriteGroup(), subject); // OK
            }
        }

        return false;
    }

    /**
     * Check the write permission on a single node.
     *
     * <p>For full node authorization, use getWritePermission(Node).
     *
     * @param node The node to check.
     * @throws AccessControlException If permission is denied.
     */
    public boolean hasSingleNodeWritePermission(Node node, Subject subject) {
        log.debug("hasSingleNodeWritePermission: " + Utils.getPath(node));

        if (!disregardLocks && (node.isLocked != null) && node.isLocked) {
            log.debug("Node locked");
            return false;
        }
        if (subject != null) {
            if (isOwner(node, subject)) {
                log.debug("Node owner granted write permission.");
                return true; // OK
            }

            checkDelegation(node, subject);

            if (log.isDebugEnabled()) {
                log.debug(String.format(
                        "Checking group write permission on node \"%s\" (groupWrite=\"%s\")",
                        node.getName(), node.getReadWriteGroup()));
            }
            if (applyMaskOnGroupReadWrite(node)) {
                return hasMembership(node.getReadWriteGroup(), subject); // OK
            }
        }
        return false;
    }
    
    /**
     * Given the groupURI, determine if the user identified by the subject
     * has membership.
     *
     * @param groups  The list of groups for a Node
     * @param subject The user's subject
     * @return True if the user is a member
     */
    private boolean hasMembership(Set<GroupURI> groups, Subject subject) {
        if (subject.getPrincipals().isEmpty()) {
            return false;
        }

        if (groups == null || groups.isEmpty()) {
            return false;
        }

        Exception firstFail = null;
        RuntimeException wrapException = null;
        try {
            // need credentials in the subject to call GMS
            if (CredUtil.checkCredentials(subject)) {
                // make gms calls to see if the user has group membership
                Set<GroupURI> nodeGroups = new HashSet<>(groups);
                Set<GroupURI> diff = new HashSet<>(nodeGroups);
                diff.removeAll(callerGroups);
                if (diff.size() < groups.size()) {
                    // a subset is already verified as part of the caller groups
                    log.debug("Found groups in cache");
                    return true;
                }
                try {
                    Set<GroupURI> newCallerGroups = gmsClient.getMemberships(nodeGroups);

                    if (!newCallerGroups.isEmpty()) {
                        log.debug("Found groups on the GMS service");
                        callerGroups.addAll(newCallerGroups);
                        return true;
                    }
                } catch (InterruptedException | IOException | ResourceNotFoundException ex) {
                    log.debug("failed to call canfar gms service", ex);
                    if (firstFail == null) {
                        firstFail = ex;
                        wrapException = new RuntimeException("failed to check membership with group service", ex);
                    }
                }
            }
        } catch (AccessControlException | CertificateExpiredException | CertificateNotYetValidException ex) {
            wrapException = new RuntimeException("failed credentials check", ex);
        }

        if (wrapException != null) {
            throw wrapException;
        }
        log.debug("User not member of groups " + groups);

        return false;
    }

    // HACK: need to create a privaledged subject for use with CredClient calls
    // but this needs to be configurable somehow...
    // for now: compatibility with current system config
    //    private Subject createOpsSubject()
    //    {
    //        File pemFile = new File(System.getProperty("user.home") + "/.pub/proxy.pem");
    //        return SSLUtil.createSubject(pemFile);
    //    }

    /**
     * Check if the specified subject is the owner of the node.
     *
     * @param subject subject
     * @param node    node to check
     * @return true of the current subject is the owner, otherwise false
     */
    private boolean isOwner(Node node, Subject subject) {
        Subject owner = node.owner;
        if (owner == null) {
            throw new IllegalStateException("BUG: no owner found for node: " + node);
        }

        Set<Principal> ownerPrincipals = owner.getPrincipals();
        Set<Principal> callerPrincipals = subject.getPrincipals();

        for (Principal ownerPrin : ownerPrincipals) {
            for (Principal callerPrin : callerPrincipals) {
                if (log.isDebugEnabled()) {
                    log.debug(String.format(
                            "Checking owner of node \"%s\" (owner=\"%s\") where user=\"%s\"",
                            node.getName(), ownerPrin, callerPrin));
                }
                if (AuthenticationUtil.equals(ownerPrin, callerPrin)) {
                    return true; // caller===owner
                }
            }
        }
        return false;
    }

    /**
     * Return false if mask blocks read
     */
    boolean applyMaskOnGroupRead(Node n) {
        NodeProperty np = n.getProperty(VOS.PROPERTY_URI_GROUPMASK);
        if (np == null || np.getValue() == null) {
            return true;
        }
        String mask = np.getValue();
        // format is rwx, each of which can also be -
        if (mask.length() != 3) {
            log.debug("invalid mask format: " + mask);
            return true;
        }
        if (mask.charAt(0) == 'r') {
            log.debug("mask allows read: " + mask);
            return true;
        }
        log.debug("mask disallows read: " + mask);
        return false;
    }

    /**
     * Return false if mask blocks write
     */
    boolean applyMaskOnGroupReadWrite(Node n) {
        NodeProperty np = n.getProperty(VOS.PROPERTY_URI_GROUPMASK);
        if (np == null || np.getValue() == null) {
            return true;
        }
        String mask = np.getValue();
        // format is rwx, each of which can also be -
        if (mask.length() != 3) {
            log.debug("invalid mask format: " + mask);
            return true;
        }
        if (mask.charAt(0) == 'r' && mask.charAt(1) == 'w') {
            log.debug("mask allows write: " + mask);
            return true;
        }
        log.debug("mask disallows write: " + mask);
        return false;
    }

    /**
     * check for delegation cookie and, if present, does an authorization
     * against it.
     *
     * @param node    - node authorization is performed against
     * @param subject - user
     * @throws AccessControlException - unauthorized access
     */
    private void checkDelegation(Node node, Subject subject) throws AccessControlException {
        Set<SignedToken> tokens = subject.getPublicCredentials(SignedToken.class);
        for (SignedToken token : tokens) {
            VOSURI scope = new VOSURI(token.getScope());
            LocalServiceURI lsURI = new LocalServiceURI(nodePersistence.getResourceID());
            VOSURI tmp = lsURI.getURI(node);
            while (tmp != null) {
                if (scope.equals(tmp)) {
                    return;
                }
                tmp = tmp.getParentURI();
            }
            String msg = "Scoped search (" + scope + ") on node (" + Utils.getPath(node) + ")- accessed denied";
            log.debug(msg);
            throw new AccessControlException(msg);
        }
    }

}

