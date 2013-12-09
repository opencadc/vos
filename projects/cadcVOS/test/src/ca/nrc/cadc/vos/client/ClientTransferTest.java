/*
************************************************************************
*******************  CANADIAN ASTRONOMY DATA CENTRE  *******************
**************  CENTRE CANADIEN DE DONNÉES ASTRONOMIQUES  **************
*
*  (c) 2009.                            (c) 2009.
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
*  $Revision: 4 $
*
************************************************************************
*/

package ca.nrc.cadc.vos.client;

import static org.junit.Assert.assertEquals;

import java.io.File;
import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;
import java.security.AccessControlException;
import java.util.ArrayList;
import java.util.List;

import org.junit.Test;

import ca.nrc.cadc.net.HttpTransfer;
import ca.nrc.cadc.util.Log4jInit;
import ca.nrc.cadc.vos.Direction;
import ca.nrc.cadc.vos.Protocol;
import ca.nrc.cadc.vos.Transfer;
import ca.nrc.cadc.vos.VOS;
import ca.nrc.cadc.vos.VOSURI;
import org.apache.log4j.Level;

/**
 * Base VOSpaceClient test code. This test code requires a running VOSpace service
 * and (probably) valid X509 proxy certficates. TODO: provide this as an integration
 * test  or rewrite it using a mock http layer (sounds hard).
 * 
 * @author zhangsa
 */
//@Ignore("Broken - Please fix soon.\n" +
//        "jenkinsd 2011.01.17")
public class ClientTransferTest
{
    static final String URL1 = "http://example.com/someplace/123";
    static final String URL2 = "http://example.com/someplace/124";
    static final String URL3 = "http://example.com/someplace/125";

    static
    {
        Log4jInit.setLevel(ClientTransferTest.class.getPackage().getName(), Level.INFO);
    }
    
    String endpoint;
    VOSpaceClient client;

    @Test
    public void testDownloadFirstEndpoint() throws Exception
    {
        List<Protocol> pe = new ArrayList<Protocol>();

        pe.add(new Protocol(VOS.PROTOCOL_HTTP_GET, URL1, null));
        pe.add(new Protocol(VOS.PROTOCOL_HTTP_GET, URL2, null));
        pe.add(new Protocol(VOS.PROTOCOL_HTTP_GET, URL3, null));
        pe.add(new Protocol(VOS.PROTOCOL_HTTPS_GET, "https://example.com/otherplace/456", null));
        pe.add(new Protocol(VOS.PROTOCOL_HTTPS_GET, "http://example.com/someplace/333", null));
        pe.add(new Protocol(VOS.PROTOCOL_HTTPS_GET, "http://example.com/someplace/122", null));
        VOSURI target = new VOSURI("vos://example.com!vospace/mydir/myfile");
        Transfer trans = new Transfer(target, Direction.pullFromVoSpace, pe);
        ClientTransfer ct = new ClientTransfer(new URL("http://someexample.com"), trans, false)
        {
            @Override
            protected void runHttpTransfer(HttpTransfer transfer)
            {
                
                try
                {
                    assertEquals("Use first endpoint", new URL(URL1), transfer.getURL());
                }
                catch (MalformedURLException e)
                {
                    throw new IllegalArgumentException(e);
                }
            }
        };
        ct.setFile(new File("/dev/null"));
        ct.runTransfer();
    }
    
    @Test
    public void testDownloadSecondEndpoint() throws Exception
    {
        List<Protocol> pe = new ArrayList<Protocol>();
        // url1 not available so check it's going to URL2
        pe.add(new Protocol(VOS.PROTOCOL_HTTP_GET, URL1, null));
        pe.add(new Protocol(VOS.PROTOCOL_HTTP_GET, URL2, null));
        pe.add(new Protocol(VOS.PROTOCOL_HTTP_GET, URL3, null));
        pe.add(new Protocol(VOS.PROTOCOL_HTTPS_GET, "https://example.com/otherplace/456", null));
        pe.add(new Protocol(VOS.PROTOCOL_HTTPS_GET, "http://example.com/someplace/333", null));
        pe.add(new Protocol(VOS.PROTOCOL_HTTPS_GET, "http://example.com/someplace/122", null));
        VOSURI target = new VOSURI("vos://example.com!vospace/mydir/myfile");
        Transfer trans = new Transfer(target, Direction.pullFromVoSpace, pe);
        ClientTransfer ct = new ClientTransfer(new URL("http://someexample.com"), trans, false)
        {
            @Override
            protected void runHttpTransfer(HttpTransfer transfer)
            {
                if (transfer.getURL().toString().equals(URL1))
                {
                    transfer.failure = new IllegalArgumentException("Ex");
                    return;
                }
                try
                {
                    assertEquals("Use first endpoint", new URL(URL2), transfer.getURL());
                }
                catch (MalformedURLException e)
                {
                    throw new IllegalArgumentException(e);
                }
            }
        };
        ct.setFile(new File("/dev/null"));
        ct.runTransfer();
    }
    
    
    @Test(expected=IOException.class)
    public void testDownloadFailedEndpoints() throws Exception
    {
        List<Protocol> pe = new ArrayList<Protocol>();
        // url1 not available so check it's going to URL2
        pe.add(new Protocol(VOS.PROTOCOL_HTTPS_GET, "https://example.com/otherplace/456", null));
        pe.add(new Protocol(VOS.PROTOCOL_HTTPS_GET, "http://example.com/someplace/333", null));
        pe.add(new Protocol(VOS.PROTOCOL_HTTPS_GET, "http://example.com/someplace/122", null));
        VOSURI target = new VOSURI("vos://example.com!vospace/mydir/myfile");
        Transfer trans = new Transfer(target, Direction.pullFromVoSpace, pe);
        ClientTransfer ct = new ClientTransfer(new URL("http://someexample.com"), trans, false)
        {
            @Override
            protected void runHttpTransfer(HttpTransfer transfer)
            {
                 transfer.failure = new AccessControlException("Ex");
            }
        };
        ct.setFile(new File("/dev/null"));
        ct.runTransfer();
    }


    @Override
    public String toString()
    {
        return "VOSpaceClientTest [client=" + client + ", getClass()=" + getClass() + ", hashCode()=" + hashCode()
                + ", toString()=" + super.toString() + "]";
    }
}
