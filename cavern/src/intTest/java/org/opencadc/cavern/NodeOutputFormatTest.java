/*
************************************************************************
*******************  CANADIAN ASTRONOMY DATA CENTRE  *******************
**************  CENTRE CANADIEN DE DONNÉES ASTRONOMIQUES  **************
*
*  (c) 2011.                            (c) 2011.
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
*  $Revision: 5 $
*
************************************************************************
*/

package org.opencadc.cavern;


import ca.nrc.cadc.auth.AuthMethod;
import ca.nrc.cadc.net.HttpDownload;
import ca.nrc.cadc.reg.Standards;
import ca.nrc.cadc.reg.client.RegistryClient;
import ca.nrc.cadc.util.Log4jInit;
import ca.nrc.cadc.vos.Node;
import ca.nrc.cadc.vos.NodeReader;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.InputStreamReader;
import java.net.URI;
import java.net.URL;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.json.JSONObject;
import org.junit.Assert;
import org.junit.Test;

/**
 * Test that the node writing on the server side prodcues valid output for all
 * supported content-type(s).
 * 
 * @author pdowler
 */
public class NodeOutputFormatTest 
{
    private static final Logger log = Logger.getLogger(NodeOutputFormatTest.class);

    static
    {
        Log4jInit.setLevel("ca.nrc.cadc.vospace", Level.INFO);
    }
    
    static final String VOSACE_SCHEMA = "http://www.ivoa.net/xml/VOSpace/v2.0";
    
    static final String VOSPACE_NODES = "ivo://canfar.net/cavern";
    
    static final String XML = "text/xml";
    static final String JSON = "application/json";
    
    public NodeOutputFormatTest() { }
    
    @Test
    public void testXML()
    {
        try
        {
            RegistryClient rc = new RegistryClient();
//            URL url = rc.getServiceURL(new URI(VOSPACE_NODES), "http", null, AuthMethod.ANON);
            URL url = rc.getServiceURL(new URI(VOSPACE_NODES), Standards.VOSPACE_NODES_20, AuthMethod.ANON);
            ByteArrayOutputStream bos = new ByteArrayOutputStream();
            HttpDownload get = new HttpDownload(url, bos);
            get.run();
            Assert.assertNull("GET failure", get.getThrowable());
            
            Assert.assertEquals(XML, get.getContentType());
            
            NodeReader r = new NodeReader();
            Node n = r.read(new ByteArrayInputStream(bos.toByteArray()));
            log.debug("got valid node: " + n);
        }
        catch(Exception ex)
        {
            log.error("unexpected exception", ex);
            Assert.fail("unexpected exception: " + ex);
        }
    }
    
    
    @Test
    public void testJSON() throws Exception
    {
        try
        {
            RegistryClient rc = new RegistryClient();
//            URL url = rc.getServiceURL(new URI(VOSPACE_NODES), "http", null, AuthMethod.ANON);
            URL url = rc.getServiceURL(new URI(VOSPACE_NODES), Standards.VOSPACE_NODES_20, AuthMethod.ANON);
            ByteArrayOutputStream bos = new ByteArrayOutputStream();
            HttpDownload get = new HttpDownload(url, bos);
            get.setRequestProperty("Accept", JSON);
            get.run();
            Assert.assertNull("GET failure", get.getThrowable());
            
            Assert.assertEquals(JSON, get.getContentType());
            
            ByteArrayInputStream bis = new ByteArrayInputStream(bos.toByteArray());
            InputStreamReader isr = new InputStreamReader(bis);
            StringBuilder sb = new StringBuilder();
            char[] buf = new char[16384];
            int n = isr.read(buf);
            while ( n > 0 )
            {
                sb.append(buf, 0, n);
                n = isr.read(buf);
            }
            String str = sb.toString();
            log.debug(str);
            JSONObject doc = new JSONObject(str);
            JSONObject node = doc.getJSONObject("vos:node");
            
            String xmlns = node.getString("@xmlns:vos");
            Assert.assertNotNull(xmlns);
            Assert.assertEquals(VOSACE_SCHEMA, xmlns);
            
            String vosuri = node.getString("@uri");
            Assert.assertNotNull(vosuri);
            Assert.assertEquals("vos://canfar.net~cavern", vosuri);
        }
        catch(Exception ex)
        {
            log.error("unexpected exception", ex);
            Assert.fail("unexpected exception: " + ex);
        }

    }
}
