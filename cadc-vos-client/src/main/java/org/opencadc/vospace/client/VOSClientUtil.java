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

package org.opencadc.vospace.client;

import ca.nrc.cadc.net.ResourceAlreadyExistsException;
import ca.nrc.cadc.net.ResourceNotFoundException;
import ca.nrc.cadc.util.StringUtil;
import ca.nrc.cadc.uws.ErrorSummary;
import ca.nrc.cadc.uws.Job;
import ca.nrc.cadc.uws.JobWriter;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.StringWriter;
import java.security.AccessControlException;
import org.apache.log4j.Logger;
import org.jdom2.Document;
import org.jdom2.output.Format;
import org.jdom2.output.XMLOutputter;
import org.opencadc.vospace.Node;
import org.opencadc.vospace.NodeNotFoundException;
import org.opencadc.vospace.VOS;
import org.opencadc.vospace.VOSURI;
import org.opencadc.vospace.io.NodeWriter;

/**
 * @author zhangsa
 *
 */
public class VOSClientUtil {
    private static Logger log = Logger.getLogger(VOSClientUtil.class);

    /**
     * get the XML string of a job.
     *
     * @param job
     * @return XML string of the job
     *
     * @author Sailor Zhang
     */
    public static String xmlString(Job job) {
        String xml = null;
        StringWriter sw = new StringWriter();
        try {
            JobWriter jobWriter = new JobWriter();
            jobWriter.write(job, sw);
            xml = sw.toString();
            sw.close();
        } catch (IOException e) {
            xml = "Error getting XML string from job: " + e.getMessage();
        }
        return xml;
    }

    /**
     * get the XML string of node.
     *
     * @param node
     * @return XML string of the node
     *
     * @author Sailor Zhang
     */
    public static String xmlString(VOSURI vosURI, Node node, VOS.Detail detail) {
        String xml;
        StringBuilder sb = new StringBuilder();
        try {
            NodeWriter nodeWriter = new NodeWriter();
            nodeWriter.write(vosURI, node, sb, detail);
            xml = sb.toString();
        } catch (IOException e) {
            xml = "Error getting XML string from node: " + e.getMessage();
        }
        return xml;
    }

    public static String xmlString(Document doc) {
        XMLOutputter outputter = new XMLOutputter();
        outputter.setFormat(Format.getPrettyFormat());
        return outputter.outputString(doc);
    }

    public static void checkFailure(Throwable failure)
        throws NodeNotFoundException, RuntimeException, ResourceAlreadyExistsException {
        if (failure != null) {
            if (failure instanceof IllegalArgumentException) {
                throw (IllegalArgumentException) failure;
            }
            if (failure instanceof ResourceAlreadyExistsException) {
                throw (ResourceAlreadyExistsException) failure;
            }
            if (failure instanceof RuntimeException) {
                throw (RuntimeException) failure;
            }
            if (failure instanceof FileNotFoundException || failure instanceof ResourceNotFoundException) {
                throw new NodeNotFoundException("not found.", failure);
            }
            throw new IllegalStateException(failure);
        }
    }

    public static void checkTransferFailure(ClientTransfer clientTransfer)
        throws ResourceNotFoundException, IOException {
        // check to see if anything went wrong
        ErrorSummary errorSummary = null;

        try {
            errorSummary = clientTransfer.getServerError();
        } catch (Exception e) {
            log.error("error reading job", e);
            throw new RuntimeException("Internal error, error reading job:", e);
        }

        // Check uws job results
        if (errorSummary != null) {
            String errorMsg = errorSummary.getSummaryMessage();

            if (StringUtil.hasLength(errorMsg)) {
                if (errorMsg.contains("Invalid Argument")
                    || VOS.IVOA_FAULT_INVALID_ARG.equals(errorMsg)) {
                    log.debug("Invalid Argument found: " + errorMsg);
                    throw new IllegalArgumentException(errorMsg);
                }

                // not a data node
                if (errorMsg.contains("not a data node")) {
                    log.debug("not a data node");
                    throw new IllegalArgumentException(errorMsg);
                }

                // not not found
                if (VOS.IVOA_FAULT_NODE_NOT_FOUND.equals(errorMsg)
                    || errorMsg.contains("node not found")) {
                    log.debug("node not found");
                    throw new ResourceNotFoundException(errorMsg);
                }

                // permission denied
                if (VOS.IVOA_FAULT_PERMISSION_DENIED.equals(errorMsg)) {
                    throw new AccessControlException(VOS.IVOA_FAULT_PERMISSION_DENIED);
                }

                // some other fault
                log.error("unhandled fault: " + errorMsg);
                throw new RuntimeException("client transfer job error: " + errorMsg);
            }
        }
    }

}
