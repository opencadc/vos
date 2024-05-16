/*
 ************************************************************************
 *******************  CANADIAN ASTRONOMY DATA CENTRE  *******************
 **************  CENTRE CANADIEN DE DONNÉES ASTRONOMIQUES  **************
 *
 *  (c) 2024.                            (c) 2024.
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
 *
 ************************************************************************
 */

package org.opencadc.util.fs;

import ca.nrc.cadc.util.StringUtil;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;

/**
 * Utility class to work with Paths.
 */
public class PathUtil {
    private static final Logger LOGGER = Logger.getLogger(PathUtil.class);

    /**
     * Work top down through the path to check the root and each subsequent path element for one that contains the
     * quota element.  This method starts at the first path element.
     *
     * <p>Example:
     * <code>
     * Path directory = Path.of("/root/foo/bar")
     * PathUtil.scanPathForQuota(directory, "my.quota.key", "foobarns");
     * </code>
     * <p>// Will check, in order
     * <ul>
     * <li>"/root"</li>
     * <li>"/root/foo"</li>
     * <li>"/root/foo/bar"</li>
     * </ul>
     *
     * @param directory The path to check.
     * @param quotaKey  The key of the quota's extended attribute.
     * @param namespace The namespace of the quota's extended attribute.
     * @return Long quota in bytes, or null if not found.
     * @throws IOException If the extended attribute cannot be read.
     */
    public static Long scanPathForQuota(final Path directory, final String quotaKey,
                                        final String namespace) throws IOException {
        LOGGER.debug("scanPathForQuota: " + directory);
        return PathUtil.scanPathForQuota(directory, 0, quotaKey, namespace);
    }

    /**
     * Work top down through the path to check the root and each subsequent path element for one that contains the
     * quota element.  This method begins at <code>pathElementStartIndex</code>.
     *
     * @param directory             The path to check.
     * @param pathElementStartIndex The index of the path element within the directory path.
     * @param quotaAttributeKey     The key of the quota's extended attribute.
     * @param namespace             The namespace of the quota's extended attribute.
     * @return Long quota in bytes, or null if not found.
     * @throws IOException If the extended attribute cannot be read.
     */
    public static Long scanPathForQuota(final Path directory, final int pathElementStartIndex,
                                        final String quotaAttributeKey, final String namespace) throws IOException {
        LOGGER.debug("scanPathForQuota: " + directory + " {" + pathElementStartIndex + "}");
        final int pathElementCount = directory.getNameCount();

        // No more path elements to check, so bail out.
        if (pathElementCount > pathElementStartIndex) {
            // Add one to the end index as it's exclusive.
            final String quotaAttributeValue = ExtendedFileAttributes.getFileAttribute(
                    Paths.get("/", directory.subpath(0, pathElementStartIndex + 1).toString()),
                    quotaAttributeKey, namespace);
            if (StringUtil.hasText(quotaAttributeValue)) {
                final long quotaInBytes = Long.parseLong(quotaAttributeValue);
                LOGGER.debug("scanPathForQuota: " + directory + " {" + pathElementStartIndex + "}: OK");
                return quotaInBytes;
            } else {
                return PathUtil.scanPathForQuota(directory, pathElementStartIndex + 1, quotaAttributeKey, namespace);
            }
        } else {
            // We've exhausted the path elements, so return null.
            LOGGER.debug("scanPathForQuota: " + directory + " {" + pathElementStartIndex + "}: MISSING");
            return null;
        }
    }
}
