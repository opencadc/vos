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

package org.opencadc.vospace;

import static org.skyscreamer.jsonassert.JSONAssert.assertEquals;

import ca.nrc.cadc.auth.IdentityManager;
import ca.nrc.cadc.auth.X500IdentityManager;
import java.io.StringWriter;
import java.io.Writer;
import java.net.URI;

import org.json.JSONObject;
import org.junit.Test;

public class JsonNodeWriterTest {
    @Test
    public void writeWithBadNumeric() throws Exception {

        // Set the IdentityManager class
        System.setProperty(IdentityManager.class.getName(), X500IdentityManager.class.getName());

        final String testXMLString = "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n"
            + "<vos:node xmlns:vos=\"http://www.ivoa.net/xml/VOSpace/v2.0\" xmlns:xsi=\"http://www.w3.org/2001/XMLSchema-instance\" uri=\"vos://cadc.nrc.ca!vospace/OSSOS/measure3\" xsi:type=\"vos:ContainerNode\">\n"
            + "  <vos:properties>\n"
            + "    <vos:property uri=\"ivo://cadc.nrc.ca/vospace/core#inheritPermissions\">false</vos:property>\n"
            + "    <vos:property uri=\"ivo://canfar.uvic.ca/OSSOS#O13AO_object_count\" readOnly=\"false\">0</vos:property>\n"
            + "    <vos:property uri=\"ivo://canfar.uvic.ca/ossos#1-object_count-DRYRUN\" readOnly=\"true\">20</vos:property>\n"
            + "    <vos:property uri=\"ivo://ivoa.net/vospace/core#creator\">cn=cadctest_007,ou=cadc,o=hia,c=ca</vos:property>\n"
            + "    <vos:property uri=\"ivo://ivoa.net/vospace/core#date\">2016-01-20T15:14:57.443</vos:property>\n"
            + "    <vos:property uri=\"ivo://ivoa.net/vospace/core#groupread\">ivo://cadc.nrc.ca/gms#OSSOS</vos:property>\n"
            + "    <vos:property uri=\"ivo://ivoa.net/vospace/core#groupwrite\">ivo://cadc.nrc.ca/gms#OSSOS-Admin ivo://cadc.nrc.ca/gms#OSSOS-Worker</vos:property>\n"
            + "    <vos:property uri=\"ivo://ivoa.net/vospace/core#ispublic\">false</vos:property>\n"
            + "    <vos:property uri=\"ivo://ivoa.net/vospace/core#length\">-89323123449</vos:property>\n"
            + "  </vos:properties>\n" + "  <vos:nodes>\n"
            + "    <vos:node uri=\"vos://cadc.nrc.ca!vospace/OSSOS/measure3/2013A-E\" xsi:type=\"vos:ContainerNode\">\n"
            + "      <vos:properties>\n"
            + "        <vos:property uri=\"ivo://cadc.nrc.ca/vospace/core#inheritPermissions\">false</vos:property>\n"
            + "        <vos:property uri=\"ivo://cadc.nrc.ca/vospace/core#islocked\">true</vos:property>\n"
            + "        <vos:property uri=\"ivo://ivoa.net/vospace/core#creator\">cn=mtb55_5be,ou=cadc,o=hia,c=ca</vos:property>\n"
            + "        <vos:property uri=\"ivo://ivoa.net/vospace/core#date\">2015-12-17T17:44:17.897</vos:property>\n"
            + "        <vos:property uri=\"ivo://ivoa.net/vospace/core#groupread\">ivo://cadc.nrc.ca/gms#OSSOS ivo://cadc.nrc.ca/gms#OSSOS-Admin ivo://cadc.nrc.ca/gms#OSSOS-Worker</vos:property>\n"
            + "        <vos:property uri=\"ivo://ivoa.net/vospace/core#groupwrite\">ivo://cadc.nrc.ca/gms#OSSOS-Admin ivo://cadc.nrc.ca/gms#OSSOS-Worker</vos:property>\n"
            + "        <vos:property uri=\"ivo://ivoa.net/vospace/core#ispublic\">false</vos:property>\n"
            + "        <vos:property uri=\"ivo://ivoa.net/vospace/core#length\">30388581</vos:property>\n"
            + "      </vos:properties>\n" + "      <vos:nodes />\n" + "    </vos:node>\n"
            + "    <vos:node uri=\"vos://cadc.nrc.ca!vospace/OSSOS/measure3/2013A-E_April9\" xsi:type=\"vos:ContainerNode\">\n"
            + "      <vos:properties>\n"
            + "        <vos:property uri=\"ivo://cadc.nrc.ca/vospace/core#inheritPermissions\">true</vos:property>\n"
            + "        <vos:property uri=\"ivo://cadc.nrc.ca/vospace/core#islocked\">true</vos:property>\n"
            + "        <vos:property uri=\"ivo://ivoa.net/vospace/core#creator\">cn=mtb55_5be,ou=cadc,o=hia,c=ca</vos:property>\n"
            + "        <vos:property uri=\"ivo://ivoa.net/vospace/core#date\">2015-12-17T17:57:32.107</vos:property>\n"
            + "        <vos:property uri=\"ivo://ivoa.net/vospace/core#groupread\">ivo://cadc.nrc.ca/gms#OSSOS-Admin ivo://cadc.nrc.ca/gms#OSSOS-Worker</vos:property>\n"
            + "        <vos:property uri=\"ivo://ivoa.net/vospace/core#groupwrite\">ivo://cadc.nrc.ca/gms#OSSOS-Admin ivo://cadc.nrc.ca/gms#OSSOS-Worker</vos:property>\n"
            + "        <vos:property uri=\"ivo://ivoa.net/vospace/core#ispublic\">false</vos:property>\n"
            + "        <vos:property uri=\"ivo://ivoa.net/vospace/core#length\">3812708</vos:property>\n"
            + "      </vos:properties>\n" + "      <vos:nodes />\n" + "    </vos:node>\n" + "  </vos:nodes>\n"
            + "</vos:node>";

        final String expectedJSONString = "{\n" +
            "  \"vos:node\" : {\n" +
            "    \"@xmlns:vos\" : \"http://www.ivoa.net/xml/VOSpace/v2.0\",\n" +
            "    \"@xmlns:xsi\" : \"http://www.w3.org/2001/XMLSchema-instance\",\n" +
            "    \"@uri\" : \"vos://cadc.nrc.ca!vospace/OSSOS/measure3\",\n" +
            "    \"@xsi:type\" : \"vos:ContainerNode\",\n" +
            "    \"vos:properties\" : {\n" +
            "      \"$\" : [\n" +
            "        {\n" +
            "          \"property\" : {\n" +
            "            \"@uri\" : \"ivo://cadc.nrc.ca/vospace/core#inheritPermissions\",\n" +
            "            \"$\" : \"false\"\n" +
            "          }\n" +
            "        },\n" +
            "        {\n" +
            "          \"property\" : {\n" +
            "            \"@uri\" : \"ivo://canfar.uvic.ca/OSSOS#O13AO_object_count\",\n" +
            "            \"@readOnly\" : \"false\",\n" +
            "            \"$\" : \"0\"\n" +
            "          }\n" +
            "        },\n" +
            "        {\n" +
            "          \"property\" : {\n" +
            "            \"@uri\" : \"ivo://canfar.uvic.ca/ossos#1-object_count-DRYRUN\",\n" +
            "            \"@readOnly\" : \"true\",\n" +
            "            \"$\" : \"20\"\n" +
            "          }\n" +
            "        },\n" +
            "        {\n" +
            "          \"property\" : {\n" +
            "            \"@uri\" : \"ivo://ivoa.net/vospace/core#creator\",\n" +
            "            \"$\" : \"cn=cadctest_007,ou=cadc,o=hia,c=ca\"\n" +
            "          }\n" +
            "        },\n" +
            "        {\n" +
            "          \"property\" : {\n" +
            "            \"@uri\" : \"ivo://ivoa.net/vospace/core#date\",\n" +
            "            \"$\" : \"2016-01-20T15:14:57.443\"\n" +
            "          }\n" +
            "        },\n" +
            "        {\n" +
            "          \"property\" : {\n" +
            "            \"@uri\" : \"ivo://ivoa.net/vospace/core#groupread\",\n" +
            "            \"$\" : \"ivo://cadc.nrc.ca/gms#OSSOS\"\n" +
            "          }\n" +
            "        },\n" +
            "        {\n" +
            "          \"property\" : {\n" +
            "            \"@uri\" : \"ivo://ivoa.net/vospace/core#groupwrite\",\n" +
            "            \"$\" : \"ivo://cadc.nrc.ca/gms#OSSOS-Admin ivo://cadc.nrc.ca/gms#OSSOS-Worker\"\n" +
            "          }\n" +
            "        },\n" +
            "        {\n" +
            "          \"property\" : {\n" +
            "            \"@uri\" : \"ivo://ivoa.net/vospace/core#ispublic\",\n" +
            "            \"$\" : \"false\"\n" +
            "          }\n" +
            "        },\n" +
            "        {\n" +
            "          \"property\" : {\n" +
            "            \"@uri\" : \"ivo://ivoa.net/vospace/core#length\",\n" +
            "            \"$\" : \"-89323123449\"\n" +
            "          }\n" +
            "        }\n" +
            "      ]\n" +
            "    },\n" +
            "    \"vos:nodes\" : {\n" +
            "      \"$\" : [\n" +
            "        {\n" +
            "          \"node\" : {\n" +
            "            \"@uri\" : \"vos://cadc.nrc.ca!vospace/OSSOS/measure3/2013A-E\",\n" +
            "            \"@xsi:type\" : \"vos:ContainerNode\",\n" +
            "            \"vos:properties\" : {\n" +
            "              \"$\" : [\n" +
            "                {\n" +
            "                  \"property\" : {\n" +
            "                    \"@uri\" : \"ivo://cadc.nrc.ca/vospace/core#inheritPermissions\",\n" +
            "                    \"$\" : \"false\"\n" +
            "                  }\n" +
            "                },\n" +
            "                {\n" +
            "                  \"property\" : {\n" +
            "                    \"@uri\" : \"ivo://cadc.nrc.ca/vospace/core#islocked\",\n" +
            "                    \"$\" : \"true\"\n" +
            "                  }\n" +
            "                },\n" +
            "                {\n" +
            "                  \"property\" : {\n" +
            "                    \"@uri\" : \"ivo://ivoa.net/vospace/core#creator\",\n" +
            "                    \"$\" : \"cn=mtb55_5be,ou=cadc,o=hia,c=ca\"\n" +
            "                  }\n" +
            "                },\n" +
            "                {\n" +
            "                  \"property\" : {\n" +
            "                    \"@uri\" : \"ivo://ivoa.net/vospace/core#date\",\n" +
            "                    \"$\" : \"2015-12-17T17:44:17.897\"\n" +
            "                  }\n" +
            "                },\n" +
            "                {\n" +
            "                  \"property\" : {\n" +
            "                    \"@uri\" : \"ivo://ivoa.net/vospace/core#groupread\",\n" +
            "                    \"$\" : \"ivo://cadc.nrc.ca/gms#OSSOS ivo://cadc.nrc.ca/gms#OSSOS-Admin ivo://cadc.nrc.ca/gms#OSSOS-Worker\"\n" +
            "                  }\n" +
            "                },\n" +
            "                {\n" +
            "                  \"property\" : {\n" +
            "                    \"@uri\" : \"ivo://ivoa.net/vospace/core#groupwrite\",\n" +
            "                    \"$\" : \"ivo://cadc.nrc.ca/gms#OSSOS-Admin ivo://cadc.nrc.ca/gms#OSSOS-Worker\"\n" +
            "                  }\n" +
            "                },\n" +
            "                {\n" +
            "                  \"property\" : {\n" +
            "                    \"@uri\" : \"ivo://ivoa.net/vospace/core#ispublic\",\n" +
            "                    \"$\" : \"false\"\n" +
            "                  }\n" +
            "                },\n" +
            "                {\n" +
            "                  \"property\" : {\n" +
            "                    \"@uri\" : \"ivo://ivoa.net/vospace/core#length\",\n" +
            "                    \"$\" : \"30388581\"\n" +
            "                  }\n" +
            "                }\n" +
            "              ]\n" +
            "            },\n" +
            "            \"vos:nodes\" : {\n" +
            "              \"$\" : [\n" +
            "              ]\n" +
            "            }\n" +
            "          }\n" +
            "        },\n" +
            "        {\n" +
            "          \"node\" : {\n" +
            "            \"@uri\" : \"vos://cadc.nrc.ca!vospace/OSSOS/measure3/2013A-E_April9\",\n" +
            "            \"@xsi:type\" : \"vos:ContainerNode\",\n" +
            "            \"vos:properties\" : {\n" +
            "              \"$\" : [\n" +
            "                {\n" +
            "                  \"property\" : {\n" +
            "                    \"@uri\" : \"ivo://cadc.nrc.ca/vospace/core#inheritPermissions\",\n" +
            "                    \"$\" : \"true\"\n" +
            "                  }\n" +
            "                },\n" +
            "                {\n" +
            "                  \"property\" : {\n" +
            "                    \"@uri\" : \"ivo://cadc.nrc.ca/vospace/core#islocked\",\n" +
            "                    \"$\" : \"true\"\n" +
            "                  }\n" +
            "                },\n" +
            "                {\n" +
            "                  \"property\" : {\n" +
            "                    \"@uri\" : \"ivo://ivoa.net/vospace/core#creator\",\n" +
            "                    \"$\" : \"cn=mtb55_5be,ou=cadc,o=hia,c=ca\"\n" +
            "                  }\n" +
            "                },\n" +
            "                {\n" +
            "                  \"property\" : {\n" +
            "                    \"@uri\" : \"ivo://ivoa.net/vospace/core#date\",\n" +
            "                    \"$\" : \"2015-12-17T17:57:32.107\"\n" +
            "                  }\n" +
            "                },\n" +
            "                {\n" +
            "                  \"property\" : {\n" +
            "                    \"@uri\" : \"ivo://ivoa.net/vospace/core#groupread\",\n" +
            "                    \"$\" : \"ivo://cadc.nrc.ca/gms#OSSOS-Admin ivo://cadc.nrc.ca/gms#OSSOS-Worker\"\n" +
            "                  }\n" +
            "                },\n" +
            "                {\n" +
            "                  \"property\" : {\n" +
            "                    \"@uri\" : \"ivo://ivoa.net/vospace/core#groupwrite\",\n" +
            "                    \"$\" : \"ivo://cadc.nrc.ca/gms#OSSOS-Admin ivo://cadc.nrc.ca/gms#OSSOS-Worker\"\n" +
            "                  }\n" +
            "                },\n" +
            "                {\n" +
            "                  \"property\" : {\n" +
            "                    \"@uri\" : \"ivo://ivoa.net/vospace/core#ispublic\",\n" +
            "                    \"$\" : \"false\"\n" +
            "                  }\n" +
            "                },\n" +
            "                {\n" +
            "                  \"property\" : {\n" +
            "                    \"@uri\" : \"ivo://ivoa.net/vospace/core#length\",\n" +
            "                    \"$\" : \"3812708\"\n" +
            "                  }\n" +
            "                }\n" +
            "              ]\n" +
            "            },\n" +
            "            \"vos:nodes\" : {\n" +
            "              \"$\" : [\n" +
            "              ]\n" +
            "            }\n" +
            "          }\n" +
            "        }\n" +
            "      ]\n" +
            "    }\n" +
            "  }\n" +
            "}";

        final VOSURI vosURI = new VOSURI(URI.create("vos://cadc.nrc.ca!vospace/OSSOS/measure3"));
        final JsonNodeWriter testSubject = new JsonNodeWriter();
        final NodeReader nodeReader = new NodeReader(false);
        final NodeReader.NodeReaderResult result = nodeReader.read(testXMLString);
        final Writer writer = new StringWriter();

        testSubject.write(vosURI, result.node, writer);

        final JSONObject expectedJSON = new JSONObject(expectedJSONString);
        final JSONObject resultJSON = new JSONObject(writer.toString());

        assertEquals(expectedJSON, resultJSON, false);
    }
}
