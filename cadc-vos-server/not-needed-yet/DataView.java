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
 *  $Revision: 4 $
 *
 ************************************************************************
 */

package org.opencadc.vospace.server;

import ca.nrc.cadc.auth.AuthMethod;
import ca.nrc.cadc.net.TransientException;
import java.net.URI;
import java.net.URL;
import java.util.List;
import java.util.Map;
import org.apache.log4j.Logger;
import org.opencadc.vospace.DataNode;
import org.opencadc.vospace.Node;
import org.opencadc.vospace.VOSURI;
import org.opencadc.vospace.server.transfers.TransferUtil;
import org.opencadc.vospace.server.transfers.TransferView;

/**
 * View for getting the data of a DataNode.
 *
 * @author majorb
 */
public class DataView extends AbstractView implements TransferView {

    private static Logger log = Logger.getLogger(DataView.class);

    private String scheme;
    private AuthMethod forceAuthMethod;

    /**
     * DataView constructor.
     */
    public DataView() {
        super();
    }

    /**
     * DataView constructor.
     *
     * @param uri
     */
    public DataView(URI uri) {
        super(uri);
    }

    public static boolean isPublic(Node n) {
        while (n != null) {
            if (!n.isPublic) {
                return false;
            }
            n = n.parent;
        }
        return true;
    }

    /**
     * Setup the URL for getting data for the node.
     */
    @Override
    public void setNode(Node node, String viewReference, URL requestURL)
            throws UnsupportedOperationException, TransientException {
        super.setNode(node, viewReference, requestURL);
        if (!(node instanceof DataNode)) {
            throw new UnsupportedOperationException("Node must be a DataNode");
        }

        this.scheme = requestURL.getProtocol();

        // check to see if a auth was specified
        String query = requestURL.getQuery();
        log.debug("query: " + query);
        String auth = getFirstValue("auth", query);
        log.debug("auth=" + auth);
        if (auth != null) {
            try {
                this.forceAuthMethod = AuthMethod.getAuthMethod(auth);
                // HACK: since all https is X509 client cert, auth also changes protocol
                if (AuthMethod.CERT.equals(forceAuthMethod)) {
                    scheme = "https";
                } else {
                    scheme = "http";
                }
            } catch (IllegalArgumentException ex) {
                throw new UnsupportedOperationException("uknown auth method: " + auth);
            }
        }
    }

    private String getFirstValue(String key, String query) {
        if (query == null) {
            return null;
        }

        String[] parts = query.split("&");

        for (String p : parts) {
            String[] kv = p.split("=");
            if (kv.length == 2 && key.equalsIgnoreCase(kv[0])) {
                return kv[1];
            }
        }
        return null;
    }

    /**
     * Return the URL for getting data.
     */
    @Override
    public URL getRedirectURL() {
        if (node == null) {
            throw new IllegalStateException("getRedirectURL called with node=null ");
        }
        if (requestURL == null) {
            throw new IllegalStateException("getRedirectURL called with requestURL=null ");
        }

        AuthMethod am = null;
        if (forceAuthMethod != null) {
            if (DataView.isPublic(node)) {
                am = AuthMethod.ANON;
            } else {
                am = forceAuthMethod;
            }
        }
        LocalServiceURI localServiceURI = new LocalServiceURI(nodePersistence.getResourceID());
        return TransferUtil.getSynctransParamURL(scheme, localServiceURI.getURI(node), am, null, localServiceURI);
    }

    /**
     * DataView not accepted for any nodes.
     */
    @Override
    public boolean canAccept(Node node) {
        return false;
    }

    /**
     * DataView is provided for all data nodes.
     */
    @Override
    public boolean canProvide(Node node) {
        return (node instanceof DataNode);
    }

    @Override
    public Map<String, List<String>> getViewParams(VOSURI target, List<ca.nrc.cadc.uws.Parameter> additionalParameters) {
        // no parameters for DataView
        return null;
    }

    @Override
    public List<ca.nrc.cadc.uws.Parameter> cleanseParameters(List<ca.nrc.cadc.uws.Parameter> additionalParameters) {
        return additionalParameters;
    }

}
