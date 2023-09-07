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

import ca.nrc.cadc.io.ByteCountInputStream;
import ca.nrc.cadc.io.ByteLimitExceededException;
import ca.nrc.cadc.net.ResourceNotFoundException;
import ca.nrc.cadc.net.TransientException;
import ca.nrc.cadc.rest.InlineContentException;
import ca.nrc.cadc.rest.InlineContentHandler;
import java.io.IOException;
import java.io.InputStream;
import org.apache.log4j.Logger;
import org.opencadc.vospace.NodeNotSupportedException;
import org.opencadc.vospace.VOS;
import org.opencadc.vospace.io.NodeParsingException;
import org.opencadc.vospace.io.NodeReader;
import org.opencadc.vospace.server.NodeFault;

/**
 * Read xml from input stream and return a node.
 * 
 * @author pdowler
 */
public class InlineNodeHandler implements InlineContentHandler {
    private static final Logger log = Logger.getLogger(InlineNodeHandler.class);

    private static final String KB_LIMIT = "32KiB";
    private static final long INPUT_LIMIT = 32 * 1024L;
    
    private final String tag;
    
    public InlineNodeHandler(String tag) {
        this.tag = tag;
    }

    @Override
    public Content accept(String name, String contentType, InputStream inputStream) 
            throws InlineContentException, IOException, TransientException {
        
        try {
            ByteCountInputStream bs = new ByteCountInputStream(inputStream, INPUT_LIMIT);
            NodeReader r = new NodeReader();
            NodeReader.NodeReaderResult result = r.read(bs);

            InlineContentHandler.Content content = new InlineContentHandler.Content();
            content.name = tag;
            content.value = result;
            return content;
        } catch (NodeNotSupportedException ex) {
            throw (IllegalArgumentException)NodeFault.TypeNotSupported.getStatus(ex.getMessage());
        } catch (ByteLimitExceededException ex) {
            throw (InlineContentException)
                    NodeFault.RequestEntityTooLarge.getStatus("invalid document too large (max: " + KB_LIMIT + ")");
        } catch (NodeParsingException ex) {
            throw (IllegalArgumentException)NodeFault.InvalidArgument.getStatus("invalid input: " + ex.getMessage());
        }
        
    }
}
