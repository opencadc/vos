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
*  $Revision: 4 $
*
************************************************************************
*/

package ca.nrc.cadc.conformance.vos;

import ca.nrc.cadc.date.DateUtil;
import ca.nrc.cadc.util.Log4jInit;
import ca.nrc.cadc.vos.ContainerNode;
import ca.nrc.cadc.vos.Node;
import ca.nrc.cadc.vos.NodeReader;
import ca.nrc.cadc.vos.VOS;
import ca.nrc.cadc.vos.VOSURI;
import com.meterware.httpunit.WebResponse;
import java.text.DateFormat;
import java.util.Date;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.junit.Assert;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertEquals;

/**
 * Test case for creating ContainerNodes.
 *
 * @author adriand
 */
public class GetSortContainerNodeTest extends VOSNodeTest
{
    private static Logger log = Logger.getLogger(GetSortContainerNodeTest.class);

    static
    {
        Log4jInit.setLevel("ca.nrc.cadc.conformance.vos", Level.INFO);
    }
    
    public GetSortContainerNodeTest()
    {
        super();
    }


    @Test
    public void getSortedContainerNode()
    {
        try
        {
            log.debug("getSortedContainerNode");

            // Parent node.
            TestNode testNode = getSampleContainerNode();

            // Add ContainerNode to the VOSpace.
            WebResponse response = put(testNode.sampleNode);
            assertEquals("PUT response code should be 200", 200, response.getResponseCode());

            // Child node sortA.
            ContainerNode sortA = new ContainerNode(new VOSURI(testNode.sampleNode.getUri() + "/sortA"));
            response = put(sortA);
            assertEquals("PUT response code should be 200", 200, response.getResponseCode());

            // Child node ad.
            ContainerNode nodeAad = new ContainerNode(new VOSURI(sortA.getUri() + "/ad"));
            response = put(nodeAad);
            assertEquals("PUT response code should be 200", 200, response.getResponseCode());

            // Child node ab.
            ContainerNode nodeAab = new ContainerNode(new VOSURI(sortA.getUri() + "/ab"));
            response = put(nodeAab);
            assertEquals("PUT response code should be 200", 200, response.getResponseCode());

            // Child node aa.
            ContainerNode nodeAaa = new ContainerNode(new VOSURI(sortA.getUri() + "/aa"));
            response = put(nodeAaa);
            assertEquals("PUT response code should be 200", 200, response.getResponseCode());

            // Request Parameters to get sort by last modified
            // Assert: should be in reverse alphabetic order (as last node added will be
            // the first returned theoretically.
            Map<String, String> parameters = new HashMap<String, String>();
            parameters.put("sort", VOS.PROPERTY_URI_DATE);

            // Get the node from vospace
            response = get(sortA, parameters);
            assertEquals("GET response code should be 200", 200, response.getResponseCode());

            // Get the response (an XML document)
            String xml = response.getText();
            log.debug("GET XML:\r\n" + xml);

            // Validate against the VOSPace schema.
            NodeReader reader = new NodeReader();
            ContainerNode validatedNode = (ContainerNode) reader.read(xml);

            // Check sort is correct
            Date childDate = null;
            DateFormat df = DateUtil.getDateFormat(DateUtil.IVOA_DATE_FORMAT, DateUtil.UTC);
            for (Node chld : validatedNode.getNodes()) {
                String s = chld.getPropertyValue(VOS.PROPERTY_URI_DATE);
                Date d = df.parse(s);
                if (childDate != null) {
                    Assert.assertTrue("lastModified order incorrect for " + chld.getName(), childDate.compareTo(d) <= 0);
                }
                childDate = d;
            }

            // Verify default sort is by name
            response = get(sortA);
            assertEquals("GET response code should be 200", 200, response.getResponseCode());

            // Get the response (an XML document)
            xml = response.getText();
            log.debug("GET XML:\r\n" + xml);

            // Validate against the VOSPace schema.
            reader = new NodeReader();
            validatedNode = (ContainerNode) reader.read(xml);

            // Check sort is correct
            String childName = null;
            for (Node chld : validatedNode.getNodes()) {
                String s = chld.getName();
                if (childName != null) {
                    Assert.assertTrue("name order incorrect for " + chld.getName(), childName.compareTo(s) <= 0);
                }
                childName = s;
            }

            // Delete the nodes.
            response = delete(nodeAad);
            assertEquals("DELETE response code should be 200", 200, response.getResponseCode());
            response = delete(nodeAab);
            assertEquals("DELETE response code should be 200", 200, response.getResponseCode());
            response = delete(nodeAaa);
            assertEquals("DELETE response code should be 200", 200, response.getResponseCode());
            response = delete(sortA);
            assertEquals("DELETE response code should be 200", 200, response.getResponseCode());
            response = delete(testNode.sampleNode);
            assertEquals("DELETE response code should be 200", 200, response.getResponseCode());

            log.debug("getSortedContainerNode passed.");
        }
        catch (Exception unexpected)
        {
            log.error("unexpected exception: " + unexpected);
            Assert.fail("unexpected exception: " + unexpected);
        }
    }


    @Test
    public void getSizeSortedContainerNode()
    {
        try
        {
            log.debug("getSizeSortedContainerNode");

            // Use existing node for this test. - CADCRegtest1/vopace-static-test/transfer
            String nodeName = "vos://cadc.nrc.ca!vospace/CADCRegtest1/vospace-static-test/transfer";
            ContainerNode node = new ContainerNode(new VOSURI(nodeName));

            // Request Parameters to get sort by last modified
            // Assert: should be in reverse alphabetic order (as last node added will be
            // the first returned theoretically.
            Map<String, String> parameters = new HashMap<String, String>();
            parameters.put("sort", VOS.PROPERTY_URI_DATE);

            // Get the node from vospace
            WebResponse response = get(node, parameters);
            assertEquals("GET response code should be 200", 200, response.getResponseCode());

            // Get the response (an XML document)
            String xml = response.getText();
            log.debug("GET XML:\r\n" + xml);

            // Validate against the VOSPace schema.
            NodeReader reader = new NodeReader();
            ContainerNode validatedNode = (ContainerNode) reader.read(xml);

            Date childDate = null;
            DateFormat df = DateUtil.getDateFormat(DateUtil.IVOA_DATE_FORMAT, DateUtil.UTC);
            for (Node chld : validatedNode.getNodes()) {
                String s = chld.getPropertyValue(VOS.PROPERTY_URI_DATE);
                Date d = df.parse(s);
                if (childDate != null) {
                    Assert.assertTrue("lastModified order incorrect for " + chld.getName(), childDate.compareTo(d) <= 0);
                }
                childDate = d;
            }

            // Test contentLength sort
            parameters = new HashMap<String, String>();
            parameters.put("sort", VOS.PROPERTY_URI_CONTENTLENGTH);
            parameters.put("order", "desc");

            // Get the node from vospace
            response = get(node, parameters);
            assertEquals("GET response code should be 200", 200, response.getResponseCode());

            // Get the response (an XML document)
            xml = response.getText();
            log.debug("GET XML:\r\n" + xml);

            // Validate against the VOSPace schema.
            reader = new NodeReader();
            validatedNode = (ContainerNode) reader.read(xml);

            Integer childSize = null;

            for (Node chld : validatedNode.getNodes()) {
                String s = chld.getPropertyValue(VOS.PROPERTY_URI_CONTENTLENGTH);
                Integer size = Integer.getInteger(s);
                if (childSize != null) {
                    Assert.assertTrue("size order incorrect for " + chld.getName(), childSize.compareTo(size) <= 0);
                }
                childSize = size;
            }

            log.debug("getSizeSortedContainerNode passed.");
        }
        catch (Exception unexpected)
        {
            log.error("unexpected exception: " + unexpected);
            Assert.fail("unexpected exception: " + unexpected);
        }
    }


    @Test
    public void getDescSortContainerNode()
    {
        try
        {
            log.debug("getDescSortContainerNode");

            // Parent node.
            TestNode testNode = getSampleContainerNode();

            // Add ContainerNode to the VOSpace.
            WebResponse response = put(testNode.sampleNode);
            assertEquals("PUT response code should be 200", 200, response.getResponseCode());

            // Child node sortA.
            ContainerNode sortA = new ContainerNode(new VOSURI(testNode.sampleNode.getUri() + "/sortA"));
            response = put(sortA);
            assertEquals("PUT response code should be 200", 200, response.getResponseCode());

            // Create them in a mixed order
            // Child node ad.
            ContainerNode nodeAad = new ContainerNode(new VOSURI(sortA.getUri() + "/ad"));
            response = put(nodeAad);
            assertEquals("PUT response code should be 200", 200, response.getResponseCode());

            // Child node aa.
            ContainerNode nodeAaa = new ContainerNode(new VOSURI(sortA.getUri() + "/aa"));
            response = put(nodeAaa);
            assertEquals("PUT response code should be 200", 200, response.getResponseCode());

            // Child node ab.
            ContainerNode nodeAab = new ContainerNode(new VOSURI(sortA.getUri() + "/ab"));
            response = put(nodeAab);
            assertEquals("PUT response code should be 200", 200, response.getResponseCode());


            // Request Parameters to get sort by name, desc (name is default column)
            // Assert: should be in reverse alphabetic order (as last node added will be
            // the first returned theoretically.
            Map<String, String> parameters = new HashMap<String, String>();
            parameters.put("order", "desc");

            // Get the node from vospace
            response = get(sortA, parameters);
            assertEquals("GET response code should be 200", 200, response.getResponseCode());

            // Get the response (an XML document)
            String xml = response.getText();
            log.debug("GET XML:\r\n" + xml);

            // Validate against the VOSPace schema.
            NodeReader reader = new NodeReader();
            ContainerNode validatedNode = (ContainerNode) reader.read(xml);

            // Check sort is correct
            String childName = null;
            for (Node chld : validatedNode.getNodes()) {
                String s = chld.getName();
                if (childName != null) {
                    Assert.assertTrue("name order incorrect for " + chld.getName(), childName.compareTo(s) >= 0);
                }
                childName = s;
            }

            // Check sort by other column name works as well
            // order is already defined above
            parameters.put("sort", VOS.PROPERTY_URI_DATE);

            // Get the node from vospace
            response = get(sortA, parameters);
            assertEquals("GET response code should be 200", 200, response.getResponseCode());

            // Get the response (an XML document)
            xml = response.getText();
            log.debug("GET XML:\r\n" + xml);

            // Validate against the VOSPace schema.
            reader = new NodeReader();
            validatedNode = (ContainerNode) reader.read(xml);

            Date childDate = null;
            DateFormat df = DateUtil.getDateFormat(DateUtil.IVOA_DATE_FORMAT, DateUtil.UTC);
            for (Node chld : validatedNode.getNodes()) {
                String s = chld.getPropertyValue(VOS.PROPERTY_URI_DATE);
                Date d = df.parse(s);
                if (childDate != null) {
                    Assert.assertTrue("lastModified order incorrect for " + chld.getName(), childDate.compareTo(d) >= 0);
                }
                childDate = d;
            }

            // Delete the nodes.
            response = delete(nodeAad);
            assertEquals("DELETE response code should be 200", 200, response.getResponseCode());
            response = delete(nodeAab);
            assertEquals("DELETE response code should be 200", 200, response.getResponseCode());
            response = delete(nodeAaa);
            assertEquals("DELETE response code should be 200", 200, response.getResponseCode());
            response = delete(sortA);
            assertEquals("DELETE response code should be 200", 200, response.getResponseCode());
            response = delete(testNode.sampleNode);
            assertEquals("DELETE response code should be 200", 200, response.getResponseCode());

            log.debug("getSortedContainerNode passed.");
        }
        catch (Exception unexpected)
        {
            log.error("unexpected exception: " + unexpected);
            Assert.fail("unexpected exception: " + unexpected);
        }
    }
    
}
