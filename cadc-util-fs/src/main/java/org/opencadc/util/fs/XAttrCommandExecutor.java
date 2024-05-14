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

import ca.nrc.cadc.exec.BuilderOutputGrabber;
import ca.nrc.cadc.util.StringUtil;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.regex.Matcher;
import java.util.regex.Pattern;


public class XAttrCommandExecutor {
    private static final Logger LOGGER = Logger.getLogger(XAttrCommandExecutor.class);

    static final Pattern GET_COMMAND_OUTPUT_PATTERN = Pattern.compile(".*=\"(.*)\"");
    static final String GET_COMMAND = "getfattr";
    static final String SET_COMMAND = "setfattr";

    static final String ATTRIBUTE_NAME_SWITCH = "--name=%s";
    static final String REMOVE_SWITCH = "--remove=%s";
    static final String ATTRIBUTE_VALUE_SWITCH = "--value=%s";


    private static String execute(final String[] command) throws IOException {
        LOGGER.debug("execute: '" + String.join(" ", command) + "'");
        final BuilderOutputGrabber grabber = new BuilderOutputGrabber();
        grabber.captureOutput(command);

        if (grabber.getExitValue() != 0) {
            throw new IOException("Command '" + String.join(" ", command) + "' failed:\n"
                                  + grabber.getErrorOutput());
        } else {
            final String output = grabber.getOutput(true);

            // Skip lines that begin with script comments ("#").
            final String value = Arrays.stream(output.split("\n"))
                                       .filter(line -> !line.startsWith("#"))
                                       .findFirst()
                                       .orElse(null);

            LOGGER.debug("execute: '" + String.join(" ", command) + "': OK");

            return value;
        }
    }

    /**
     * Obtain the value of a given attribute key.
     *
     * @param path         The Path to get an attribute for.
     * @param attributeKey The full attribute key to lookup (i.e. user.foo.bar), or other non-user keys if supported.
     * @return String value
     * @throws IOException if the attribute key doesn't exist or cannot be read.
     */
    public static String get(final Path path, final String attributeKey) throws IOException {
        if (!StringUtil.hasText(attributeKey)) {
            throw new IllegalArgumentException("Cannot process empty key");
        }

        final String[] command = new String[] {
                XAttrCommandExecutor.GET_COMMAND,
                String.format(XAttrCommandExecutor.ATTRIBUTE_NAME_SWITCH, attributeKey),
                path.toAbsolutePath().toString()
        };

        // getfattr command returns the full attribute in key=value format.
        final String commandOutput = XAttrCommandExecutor.execute(command);
        final Matcher commandOutputMatcher = XAttrCommandExecutor.GET_COMMAND_OUTPUT_PATTERN.matcher(commandOutput);

        if (!commandOutputMatcher.find()) {
            throw new IllegalStateException("Unknown command output: " + commandOutput);
        } else {
            return commandOutputMatcher.group(1);
        }
    }

    /**
     * Remove the attribute for the given key.
     *
     * @param path         The Path to modify.
     * @param attributeKey The key of the attribute to remove.
     */
    public static void remove(final Path path, final String attributeKey) throws IOException {
        if (!StringUtil.hasText(attributeKey)) {
            throw new IllegalArgumentException("Cannot process empty key");
        }

        final String[] command = new String[] {
                XAttrCommandExecutor.SET_COMMAND,
                String.format(XAttrCommandExecutor.REMOVE_SWITCH, attributeKey),
                path.toAbsolutePath().toString()
        };

        XAttrCommandExecutor.execute(command);
    }

    /**
     * Remove the attribute for the given key.
     *
     * @param path         The Path to modify.
     * @param attributeKey The key of the attribute to remove.
     */
    public static void set(final Path path, final String attributeKey, final String attributeValue) throws IOException {
        if (!StringUtil.hasText(attributeKey)) {
            throw new IllegalArgumentException("Cannot process empty key");
        }

        final String[] command = new String[] {
                XAttrCommandExecutor.SET_COMMAND,
                String.format(XAttrCommandExecutor.ATTRIBUTE_NAME_SWITCH, attributeKey),
                String.format(XAttrCommandExecutor.ATTRIBUTE_VALUE_SWITCH, attributeValue),
                path.toAbsolutePath().toString()
        };

        XAttrCommandExecutor.execute(command);
    }
}
