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

package org.opencadc.cavern.nodes;

import ca.nrc.cadc.util.StringUtil;
import org.apache.log4j.Logger;
import org.opencadc.util.fs.ExtendedFileAttributes;

import java.io.IOException;
import java.nio.file.Path;

public class CephFSQuotaPlugin implements QuotaPlugin {
    private static final Logger LOGGER = Logger.getLogger(CephFSQuotaPlugin.class);
    private static final String QUOTA_ATTR_KEY = "ceph.quota.max_bytes";


    @Override
    public Long getBytesUsed(Path directory) {
        return 0L;
    }

    /**
     * Obtain the quota amount, in bytes, set at the highest level of the provided path.  This method will traverse,
     * beginning at the first Path element, until it finds a directory containing the CephFS Quota attribute.
     * Returning a null value means no quota is set, and users can write until the underlying storage is full.
     *
     * @param directory directory to check
     * @return null if the directory input is null, or if no directory contains the quota attribute.
     */
    @Override
    public Long getQuota(Path directory) {
        if (directory == null) {
            return null;
        }

        try {
            return CephFSQuotaPlugin.findPathWithQuota(directory, 0);
        } catch (IOException ioException) {
            LOGGER.error("Unable to find a folder containing " + CephFSQuotaPlugin.QUOTA_ATTR_KEY);
            return null;
        }
    }

    private static Long findPathWithQuota(final Path directory, final int pathElementIndex) throws IOException {
        LOGGER.debug("findPathWithQuota: " + directory + " {" + pathElementIndex + "}");
        final int pathElementCount = directory.getNameCount();

        // No more path elements to check, so bail out.
        if (pathElementCount > pathElementIndex) {
            // Add one to the end index as it's exclusive.
            final String quotaAttributeValue =
                    ExtendedFileAttributes.getFileAttribute(directory, CephFSQuotaPlugin.QUOTA_ATTR_KEY);
            if (StringUtil.hasText(quotaAttributeValue)) {
                final long quotaInBytes = Long.parseLong(quotaAttributeValue);
                LOGGER.debug("findPathWithQuota: " + directory + " {" + pathElementIndex + "}: OK");
                return quotaInBytes;
            } else {
                return CephFSQuotaPlugin.findPathWithQuota(directory, pathElementIndex + 1);
            }
        } else {
            LOGGER.debug("findPathWithQuota: " + directory + " {" + pathElementIndex + "}: MISSING");
            return null;
        }
    }

    @Override
    public void setQuota(Path directory, Long quota) {

    }
}
