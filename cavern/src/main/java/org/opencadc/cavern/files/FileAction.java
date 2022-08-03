/*
************************************************************************
*******************  CANADIAN ASTRONOMY DATA CENTRE  *******************
**************  CENTRE CANADIEN DE DONNÉES ASTRONOMIQUES  **************
*
*  (c) 2017.                            (c) 2017.
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

package org.opencadc.cavern.files;

import ca.nrc.cadc.rest.InlineContentHandler;
import ca.nrc.cadc.rest.RestAction;
import ca.nrc.cadc.util.PropertiesReader;
import ca.nrc.cadc.vos.Direction;
import ca.nrc.cadc.vos.VOSURI;

import ca.nrc.cadc.vos.server.LocalServiceURI;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.attribute.GroupPrincipal;
import java.nio.file.attribute.UserPrincipalLookupService;
import java.security.AccessControlException;

import org.apache.log4j.Logger;
import org.opencadc.cavern.PosixIdentityManager;

/**
 *
 * @author majorb
 */
public abstract class FileAction extends RestAction {
    private static final Logger log = Logger.getLogger(FileAction.class);

    private String root;
    private UserPrincipalLookupService upLookupSvc;
    private PosixIdentityManager identityManager;

    private VOSURI nodeURI;

    protected FileAction() {
        PropertiesReader pr = new PropertiesReader("Cavern.properties");
        this.root = pr.getFirstPropertyValue("VOS_FILESYSTEM_ROOT");
        if (this.root == null) {
            throw new IllegalStateException("VOS_FILESYSTEM_ROOT not configured.");
        }

        Path rootPath = Paths.get(getRoot());
        this.upLookupSvc = rootPath.getFileSystem().getUserPrincipalLookupService();
        this.identityManager = new PosixIdentityManager(upLookupSvc);
    }

    @Override
    protected InlineContentHandler getInlineContentHandler() {
        return null;
    }

    protected VOSURI getNodeURI() throws AccessControlException, IOException, URISyntaxException {
        initTarget();
        return nodeURI;
    }

    protected String getRoot() {
        return root;
    }

    protected UserPrincipalLookupService getUpLookupSvc() {
        return upLookupSvc;
    }

    protected PosixIdentityManager getIdentityManager() {
        return identityManager;
    }

    protected abstract Direction getDirection();

    private void initTarget() throws AccessControlException, IOException, URISyntaxException {
        if (nodeURI == null) {
            String path = syncInput.getPath();

//            LocalServiceURI localServiceURI = new LocalServiceURI();
//            VOSURI baseURI = localServiceURI.getVOSBase();
//            log.debug("baseURI for target node: " + baseURI.toString());

            String[] parts = path.split("/");
            if (parts.length < 2) {
                throw new IllegalArgumentException("Invalid request");
            }
//            String meta = parts[0];
//            String sig = parts[1];
//            log.debug("meta: " + meta);
//            log.debug("sig: " + sig);
            String token = parts[0];
            log.debug("token: " + token);

//            int firstSlashIndex = path.indexOf("/");
//            String pathStr = path.substring(firstSlashIndex + 1);
//            log.debug("path: " + pathStr);
//            String targetURIStr = baseURI.toString() + "/" + pathStr;
//            log.debug("target URI for validating token: " + targetURIStr);
//            // TODO: could be that having an error trap here is better than just
//            // throwing it in the signature
//            URI targetURI = new URI(targetURIStr);

            CavernURLGenerator urlGen = new CavernURLGenerator();
            VOSURI targetVOSURI = urlGen.getURIFromPath(path);
            Direction direction = this.getDirection();

//            nodeURI = urlGen.getNodeURI(meta, sig, direction);
            nodeURI = urlGen.getNodeURI(token, targetVOSURI, direction);
            log.debug("Init node uri: " + nodeURI);
        }
    }
}
