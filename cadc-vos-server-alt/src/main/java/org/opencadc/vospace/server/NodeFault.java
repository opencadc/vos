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

package org.opencadc.vospace.server;

import ca.nrc.cadc.auth.NotAuthenticatedException;
import ca.nrc.cadc.io.ByteLimitExceededException;
import ca.nrc.cadc.net.ResourceAlreadyExistsException;
import ca.nrc.cadc.net.ResourceLockedException;
import ca.nrc.cadc.net.ResourceNotFoundException;
import ca.nrc.cadc.net.TransientException;
import ca.nrc.cadc.rest.InlineContentException;
import ca.nrc.cadc.util.StringUtil;
import java.security.AccessControlException;
import org.opencadc.vospace.VOS;
import org.opencadc.vospace.server.actions.InlineNodeHandler;

/**
 * Enumeration of type types of faults that can occur
 * with node processing.
 *
 * @author majorb
 * @author adriand
 */
public enum NodeFault {
    // IVOA Standard Faults - not an exhaustive list
    PermissionDenied(VOS.IVOA_FAULT_PERMISSION_DENIED),
    InvalidURI(VOS.IVOA_FAULT_INVALID_URI),
    NodeNotFound(VOS.IVOA_FAULT_NODE_NOT_FOUND),
    DuplicateNode(VOS.IVOA_FAULT_DUPLICATE_NODE),
    InvalidToken(VOS.IVOA_FAULT_INVALID_TOKEN),
    InvalidArgument(VOS.IVOA_FAULT_INVALID_ARG),
    TypeNotSupported(VOS.IVOA_FAULT_TYPE_NOT_SUPPORTED),
    // CADC defined faults
    ContainerNotFound(VOS.CADC_FAULT_CONTAINER_NOT_FOUND),
    RequestEntityTooLarge(VOS.CADC_FAULT_REQUEST_TOO_LARGE),
    UnreadableLinkTarget(VOS.CADC_FAULT_UNREADABLE_LINK),
    ServiceBusy(VOS.CADC_FAULT_SERVICE_BUSY),
    NodeLocked(VOS.CADC_FAULT_NODE_LOCKED),
    NotAuthenticated(VOS.CADC_FAULT_NOT_AUTHENTICATED);

    private String status;
    private boolean serviceFailure;

    private NodeFault(String fault) {
        this.status = fault;
        this.serviceFailure = false;
    }

    public Exception getStatus(String msg) {
        String exMsg = status;
        if (StringUtil.hasText(msg)) {
            exMsg += ": " + msg;
        }
        switch (status) {
            case VOS.IVOA_FAULT_PERMISSION_DENIED:
                return new AccessControlException(exMsg);
            case VOS.CADC_FAULT_NOT_AUTHENTICATED:
                return new NotAuthenticatedException(exMsg);
            case VOS.IVOA_FAULT_INVALID_URI:
            case VOS.IVOA_FAULT_INVALID_ARG:
            case VOS.IVOA_FAULT_INVALID_TOKEN:
            case VOS.IVOA_FAULT_TYPE_NOT_SUPPORTED:
                return new IllegalArgumentException(exMsg);
            case VOS.IVOA_FAULT_NODE_NOT_FOUND:
            case VOS.CADC_FAULT_CONTAINER_NOT_FOUND:
            case VOS.CADC_FAULT_UNREADABLE_LINK:
                return new ResourceNotFoundException(exMsg);
            case VOS.IVOA_FAULT_DUPLICATE_NODE:
                return new ResourceAlreadyExistsException(exMsg);
            case VOS.CADC_FAULT_SERVICE_BUSY:
                return new TransientException(exMsg);
            case VOS.CADC_FAULT_NODE_LOCKED:
                return new ResourceLockedException(exMsg);
            case VOS.CADC_FAULT_REQUEST_TOO_LARGE:
                return new ByteLimitExceededException(exMsg, InlineNodeHandler.INPUT_LIMIT);
            default:
                return new RuntimeException("BUG: Unknown fault type - " + status);
        }
    }

    public String toString() {
        return name();
    }

    public boolean isServiceFailure() {
        return serviceFailure;
    }

    public void setServiceFailure(boolean serviceFailure) {
        this.serviceFailure = serviceFailure;
    }

}
