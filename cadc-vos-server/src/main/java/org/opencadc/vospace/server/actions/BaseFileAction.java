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

package org.opencadc.vospace.server.actions;

import ca.nrc.cadc.rest.InlineContentHandler;
import ca.nrc.cadc.rest.RestAction;
import java.net.URI;
import java.net.URISyntaxException;
import java.text.ParseException;
import java.util.Date;
import javax.naming.Context;
import javax.naming.InitialContext;
import org.apache.log4j.Logger;
import org.opencadc.vospace.DataNode;
import org.opencadc.vospace.NodeProperty;
import org.opencadc.vospace.VOS;
import org.opencadc.vospace.VOSURI;
import org.opencadc.vospace.io.NodeWriter;
import org.opencadc.vospace.server.LocalServiceURI;
import org.opencadc.vospace.server.NodePersistence;
import org.opencadc.vospace.server.Utils;
import org.opencadc.vospace.server.auth.VOSpaceAuthorizer;

/**
 * Common functionality of an action on files end point.
 * @author adriand
 */
public abstract class BaseFileAction extends RestAction {
    protected static Logger log = Logger.getLogger(BaseFileAction.class);

    // some subclasses may need to determine hostname, request path, etc
    protected VOSpaceAuthorizer voSpaceAuthorizer;
    protected NodePersistence nodePersistence;
    protected LocalServiceURI localServiceURI;

    protected BaseFileAction() {
        super();
    }

    @Override
    public void initAction() throws Exception {
        String jndiNodePersistence = super.appName + "-" + NodePersistence.class.getName();
        try {
            Context ctx = new InitialContext();
            this.nodePersistence = ((NodePersistence) ctx.lookup(jndiNodePersistence));
            this.voSpaceAuthorizer = new VOSpaceAuthorizer(nodePersistence);        
            localServiceURI = new LocalServiceURI(nodePersistence.getResourceID());
        } catch (Exception oops) {
            throw new RuntimeException("BUG: NodePersistence implementation not found with JNDI key " + jndiNodePersistence, oops);
        }

        checkReadable();
    }

    protected void setResponseHeaders(DataNode node) {
        syncOutput.setHeader("Content-Disposition", "inline; filename=\"" + node.getName() + "\"");
        for (NodeProperty prop : node.getProperties()) {
            if (prop.getKey().equals(VOS.PROPERTY_URI_DATE)) {
                try {
                    Date lastMod = NodeWriter.getDateFormat().parse(prop.getValue());
                    syncOutput.setLastModified(lastMod);

                } catch (ParseException e) {
                    log.warn("BUG: Unexpected date format " + prop.getValue() + " for " + Utils.getPath(node));
                }
            }
            if (prop.getKey().equals(VOS.PROPERTY_URI_CONTENTLENGTH)) {
                syncOutput.setHeader("Content-Length", prop.getValue());
            }
            if (prop.getKey().equals(VOS.PROPERTY_URI_CONTENTENCODING)) {
                syncOutput.setHeader("Content-Encoding", prop.getValue());
            }
            if (prop.getKey().equals(VOS.PROPERTY_URI_TYPE)) {
                syncOutput.setHeader("Content-Type", prop.getValue());
            }
            if (prop.getKey().equals(VOS.PROPERTY_URI_CONTENTMD5)) {
                try {
                    URI md5 = new URI("md5:" + prop.getValue());
                    syncOutput.setDigest(md5);
                } catch (URISyntaxException ex) {
                    log.error("found invalid checksum attribute " + prop.getValue() + " on node " + Utils.getPath(node));
                    // yes, just skip: users can set attributes so hard to tell if this is a bug or
                    // user mistake
                }
            }
        }
    }
}