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


/**
 * QuotaPlugin implementation that can get and set specific values for interacting with CephFS.  This class relies
 * on the Extended Attributes to be set on the appropriate folder.
 */
public class CephFSQuotaPlugin implements QuotaPlugin {
    private static final Logger LOGGER = Logger.getLogger(CephFSQuotaPlugin.class);
    private static final String NAMESPACE = "ceph";
    private static final String QUOTA_ATTR_KEY = "quota.max_bytes";
    private static final String RECURSIVE_SIZE_ATTR_KEY = "dir.rbytes";


    @Override
    public Long getBytesUsed(Path directory) {
        LOGGER.debug("getBytesUsed: " + directory);
        if (directory == null) {
            throw new IllegalArgumentException("Cannot get size of null directory");
        }

        try {
            final String recursiveBytesUsedValue =
                    ExtendedFileAttributes.getFileAttribute(directory, CephFSQuotaPlugin.RECURSIVE_SIZE_ATTR_KEY,
                                                            CephFSQuotaPlugin.NAMESPACE);

            // NumberFormatException is thrown if the attribute value (recursiveBytesUsedValue) is null or not a number.
            final long recursiveBytesUsed = Long.parseLong(recursiveBytesUsedValue);

            LOGGER.debug("getBytesUsed: " + directory + ": OK");
            return recursiveBytesUsed;
        } catch (IOException | NumberFormatException exception) {
            LOGGER.warn("Unable to find recursive folder size of " + directory + " ("
                        + CephFSQuotaPlugin.NAMESPACE + "." + CephFSQuotaPlugin.QUOTA_ATTR_KEY + ")");
            return null;
        }
    }

    /**
     * Obtain the quota amount, in bytes, set at the highest level of the provided path.  This method will traverse,
     * beginning at the first Path element, until it finds a directory containing the CephFS Quota attribute.
     * Returning a null value means no quota is set, and users can write until the underlying storage is full.
     *
     * <p>Refer to <a href="https://docs.ceph.com/en/quincy/cephfs/quota/">Quota Docs</a> for details.
     *
     * @param directory directory to check
     * @return null if the directory input is null, or if no directory contains the quota attribute.
     */
    @Override
    public Long getQuota(Path directory) {
        LOGGER.debug("getQuota: " + directory);
        if (directory == null) {
            throw new IllegalArgumentException("Cannot check quota of null directory");
        }

        try {
            final String quotaAttributeValue = ExtendedFileAttributes.getFileAttribute(directory,
                                                                                       CephFSQuotaPlugin.QUOTA_ATTR_KEY,
                                                                                       CephFSQuotaPlugin.NAMESPACE);
            final Long quotaInBytes;

            if (StringUtil.hasText(quotaAttributeValue)) {
                final long parsedQuotaValue = Long.parseLong(quotaAttributeValue);

                // No quota set, so set to null.  Checking for <= 0 is likely unnecessary, but here to be thorough.
                // Quota from the documentation:
                // > Note that if the value of the extended attribute is 0 that means the quota is not set.
                if (parsedQuotaValue <= 0) {
                    quotaInBytes = null;
                } else {
                    quotaInBytes = parsedQuotaValue;
                }
            } else {
                quotaInBytes = null;
            }

            LOGGER.debug("getQuota: " + directory + ": OK");

            return quotaInBytes;
        } catch (IOException | NumberFormatException exception) {
            throw new RuntimeException(exception.getMessage(), exception);
        }
    }

    @Override
    public void setQuota(Path directory, Long quota) {
        LOGGER.debug("setQuota: " + directory);
        if (directory == null) {
            throw new IllegalArgumentException("Attempt to set quota on null directory.  Doing nothing.");
        }

        final String quotaValue;

        if (quota == null) {
            quotaValue = null;
        } else if (quota <= 0) {
            throw new IllegalArgumentException("Invalid quota value: " + quota);
        } else {
            quotaValue = Long.toString(quota);
        }

        try {
            ExtendedFileAttributes.setFileAttribute(directory, CephFSQuotaPlugin.QUOTA_ATTR_KEY, quotaValue,
                                                    CephFSQuotaPlugin.NAMESPACE);
        } catch (IOException ioException) {
            throw new RuntimeException(ioException.getMessage(), ioException);
        }
    }
}
