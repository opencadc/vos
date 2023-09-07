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

package org.opencadc.util.fs;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.nio.file.FileSystemException;
import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.Path;
import java.nio.file.attribute.UserDefinedFileAttributeView;

import org.apache.log4j.Logger;

/**
 * A class to handle the extended attributes of a file system.
 * 
 * @author pdowler
 *
 */
public class ExtendedFileAttributes {
    private static final Logger log = Logger.getLogger(ExtendedFileAttributes.class);
    
    /**
     * Set the attribute for the current path. If attributeValue is null, the attribute will be deleted. 
     * @param path   The path where the attribute will be set. Must not be null.
     * @param attributeKey  The name of the attribute to be set. Must not be null.
     * @param attributeValue     The value of the attribute to be set. A null value means the attribute is to be deleted.
     * @throws IOException  If setting failed.
     */
    public static void setFileAttribute(Path path, String attributeKey, String attributeValue) throws IOException {
        if (path == null || attributeKey == null) {
            throw new IllegalArgumentException("path or attributeKey cannot be null");
        }

        log.debug("setFileAttribute: " + path);
        UserDefinedFileAttributeView udv = Files.getFileAttributeView(path,
            UserDefinedFileAttributeView.class, LinkOption.NOFOLLOW_LINKS);
        if (attributeValue != null) {
            attributeValue = attributeValue.trim();
            log.debug("attribute: " + attributeKey + " = " + attributeValue);
            ByteBuffer buf = ByteBuffer.wrap(attributeValue.getBytes(Charset.forName("UTF-8")));
            udv.write(attributeKey, buf);
        } else {
            try {
                log.debug("attribute: " + attributeKey + " (delete)");
                udv.delete(attributeKey);
            } catch (FileSystemException ex) {
                log.debug("assume no such attr: " + ex);
            }
        }
    }
    
    /**
     * Get the value of specified attribute for the current path. 
     * @param path   The path where the attribute resides in. Must not be null.
     * @param attributeKey  The name of the attribute to get. Must not be null.
     * @throws IOException  If getting failed.
     */
    static String getFileAttribute(Path path, String attributeName) throws IOException {
        if (path == null || attributeName == null) {
            throw new IllegalArgumentException("path or attributeName cannot be null");
        }

        try {
            UserDefinedFileAttributeView udv = Files.getFileAttributeView(path,
                UserDefinedFileAttributeView.class, LinkOption.NOFOLLOW_LINKS);

            int sz = udv.size(attributeName);
            ByteBuffer buf = ByteBuffer.allocate(2 * sz);
            udv.read(attributeName, buf);
            return new String(buf.array(), Charset.forName("UTF-8")).trim();
        } catch (FileSystemException ex) {
            log.debug("assume no such attr: " + ex);
            return null;
        }
    }
}
