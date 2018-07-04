/*
************************************************************************
*******************  CANADIAN ASTRONOMY DATA CENTRE  *******************
**************  CENTRE CANADIEN DE DONNÉES ASTRONOMIQUES  **************
*
*  (c) 2018.                            (c) 2018.
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

package org.opencadc.cavern;


import ca.nrc.cadc.reg.Standards;
import ca.nrc.cadc.uws.ErrorSummary;
import ca.nrc.cadc.uws.ExecutionPhase;
import ca.nrc.cadc.vos.Direction;
import ca.nrc.cadc.vos.Node;
import ca.nrc.cadc.vos.NodeNotFoundException;
import ca.nrc.cadc.vos.Protocol;
import ca.nrc.cadc.vos.Transfer;
import ca.nrc.cadc.vos.VOS;
import ca.nrc.cadc.vos.VOSURI;
import ca.nrc.cadc.vos.View;
import ca.nrc.cadc.vos.client.ClientTransfer;
import ca.nrc.cadc.vos.client.VOSpaceClient;
import java.io.File;
import java.net.URI;
import java.security.PrivilegedExceptionAction;
import java.util.ArrayList;
import java.util.List;
import org.apache.log4j.Logger;

/**
 *
 * @author pdowler
 */
public final class TestActions {
    private static final Logger log = Logger.getLogger(TestActions.class);

    private TestActions() { 
    }
    
    static class CreateTransferAction implements PrivilegedExceptionAction<ClientTransfer> {

        VOSpaceClient vos;
        Transfer trans;
        boolean run;

        CreateTransferAction(VOSpaceClient vos, Transfer trans, boolean run) {
            this.vos = vos;
            this.trans = trans;
            this.run = run;
        }

        @Override
        public ClientTransfer run() throws Exception {
            ClientTransfer ct = vos.createTransfer(trans);
            if (run) {
                ct.run();
            }
            return ct;
        }

    }
    
    static class CreateNodeAction implements PrivilegedExceptionAction<Node>
    {
        VOSpaceClient vos;
        Node node;
        boolean checkIfExists = false;

        CreateNodeAction(VOSpaceClient vos, Node node)
        {
            this.vos = vos;
            this.node = node;
        }
        
        CreateNodeAction(VOSpaceClient vos, Node node, boolean createIfExists)
        {
            this.vos = vos;
            this.node = node;
            this.checkIfExists = createIfExists;
        }
        
        public Node run() throws Exception
        {
            try
            {
                if (checkIfExists) {
                    try {
                        Node cur = vos.getNode(node.getUri().getPath());
                        return cur;
                    } catch (NodeNotFoundException ignore) {
                        log.debug("not found: " + node.getUri() + "... creating");
                    }
                }
                return vos.createNode(node);
            }
            catch (Exception ex) {
                throw new RuntimeException("create: " + node.getUri() + " failed", ex);
            }
        }
    }
    
    static class UpdateNodeAction implements PrivilegedExceptionAction<Node>
    {
        VOSpaceClient vos;
        Node node;

        UpdateNodeAction(VOSpaceClient vos, Node node)
        {
            this.vos = vos;
            this.node = node;
        }
        public Node run() throws Exception
        {
            try
            {
                return vos.setNode(node);
            }
            catch (Exception ex) {
                throw new RuntimeException("create: " + node.getUri() + " failed", ex);
            }
        }
    }
    
    static class DeleteNodeAction implements PrivilegedExceptionAction<Node>
    {
        VOSpaceClient vos;
        String path;

        DeleteNodeAction(VOSpaceClient vos, String path)
        {
            this.vos = vos;
            this.path = path;
        }
        public Node run() throws Exception
        {
            try
            {
                vos.deleteNode(path);
            }
            catch (Exception ignore) {}
            return null;
        }
    }

    static class GetNodeAction implements PrivilegedExceptionAction<Node>
    {
        VOSpaceClient vos;
        String path;

        GetNodeAction(VOSpaceClient vos, String path)
        {
            this.vos = vos;
            this.path = path;
        }
        public Node run() throws Exception
        {
            return vos.getNode(path);
        }
    }

    static class UploadNodeAction implements PrivilegedExceptionAction<Node>
    {
        VOSpaceClient vos;
        VOSURI uri;
        String md5;
        File file;

        UploadNodeAction(VOSpaceClient vos, VOSURI uri, String md5, File file)
        {
            this.vos = vos;
            this.uri = uri;
            this.md5 = md5;
            this.file = file;
        }
        public Node run() throws Exception
        {
            View view = new View(new URI(VOS.VIEW_DEFAULT));
            List<Protocol> protocols = new ArrayList<Protocol>();
            
            // https nut no cert: presigned
            protocols.add(new Protocol(VOS.PROTOCOL_HTTPS_PUT)); 
            
            // https with client cert: in case broad SSL config always requires certs
            Protocol p = new Protocol(VOS.PROTOCOL_HTTPS_PUT);
            p.setSecurityMethod(Standards.SECURITY_METHOD_CERT);
            protocols.add(p);

            Transfer transfer = new Transfer(uri.getURI(), Direction.pushToVoSpace, view, protocols);
            transfer.version = VOS.VOSPACE_21; // needed to write securityMethod in xml
            ClientTransfer clientTransfer = vos.createTransfer(transfer);

            clientTransfer.setFile(file);
            if (md5 != null)
                clientTransfer.setRequestProperty("Content-MD5", md5);

            clientTransfer.runTransfer();

            ExecutionPhase ep = clientTransfer.getPhase();
            if ( ExecutionPhase.ERROR.equals(ep) )
            {
                ErrorSummary es = clientTransfer.getServerError();
                throw new RuntimeException(es.getSummaryMessage());
            }
            else if ( ExecutionPhase.ABORTED.equals(ep) )
                throw new RuntimeException("transfer aborted by service");
            // else: could be COMPLETED or still EXECUTING in current cadc-vos-server impl, but
            // in practice the job should reach COMPLETED when the transfer negotiation is done

            return vos.getNode(uri.getPath());
        }
    }

}
