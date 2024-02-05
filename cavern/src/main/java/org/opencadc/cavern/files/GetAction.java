/*
************************************************************************
*******************  CANADIAN ASTRONOMY DATA CENTRE  *******************
**************  CENTRE CANADIEN DE DONNÉES ASTRONOMIQUES  **************
*
*  (c) 2022.                            (c) 2022.
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

import ca.nrc.cadc.io.ByteCountOutputStream;
import ca.nrc.cadc.net.ResourceNotFoundException;
import java.io.FileNotFoundException;
import java.nio.file.AccessDeniedException;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.security.AccessControlException;
import org.apache.log4j.Logger;
import org.opencadc.vospace.LinkingException;
import org.opencadc.vospace.NodeNotFoundException;

/**
 * Get file bytes from the filesystem.
 * 
 * @author pdowler
 */

public class GetAction extends HeadAction {
    private static final Logger log = Logger.getLogger(GetAction.class);

    public GetAction() {
        super();
    }

    @Override
    public void doAction()  throws Exception {
        ByteCountOutputStream out = null;
        try {
            Path source = resolveAndSetMetadata();

            if (source.toFile().length() == 0) {
                syncOutput.setCode(204);
                log.debug("Empty file " + source);
            } else {
                out = new ByteCountOutputStream(syncOutput.getOutputStream());
                log.debug("Starting copy of file " + source);
                Files.copy(source, out);
                log.debug("Completed copy of file " + source);
                out.flush();
            }

        } catch (NodeNotFoundException | FileNotFoundException | NoSuchFileException e) {
            log.debug("404 error with GET: ",  e);
            throw new ResourceNotFoundException(e.getMessage());
        } catch (LinkingException e) {
            log.debug("400 error with GET: ",  e);
            throw new IllegalArgumentException(e.getMessage());
        } catch (AccessControlException | AccessDeniedException e) {
            log.debug(e);
            throw new AccessControlException(e.getMessage());
        } finally {
            if (out != null && out.getByteCount() > 0L) {
                logInfo.setBytes(out.getByteCount());
            }
        }
    }
}
