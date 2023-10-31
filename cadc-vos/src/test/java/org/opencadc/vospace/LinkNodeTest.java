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

import ca.nrc.cadc.util.Log4jInit;
import java.net.URI;
import java.util.Set;
import java.util.TreeSet;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 * @author yeunga
 */
public class LinkNodeTest {
    private static Logger log = Logger.getLogger(LinkNodeTest.class);
    private static String ROOT_NODE;
    private static String VOS_URI = "vos://cadc.nrc.ca!vospace";

    /**
     * @throws java.lang.Exception
     */
    @BeforeClass
    public static void setUpBeforeClass() throws Exception {
        Log4jInit.setLevel("org.opencadc.vospace", Level.INFO);
        ROOT_NODE = System.getProperty("user.name") + "/";
    }

    @Test
    public void testInstantiationsWithoutProperties() throws Exception {
        final String slashPath1 = "/" + ROOT_NODE + TestUtil.uniqueStringOnTime();
        final String slashPath2 = "/" + ROOT_NODE + TestUtil.uniqueStringOnTime();
        VOSURI uri = new VOSURI(VOS_URI + slashPath1);
        URI target = new URI(VOS_URI + slashPath2);
        LinkNode node = new LinkNode(uri.getName(), target);
        Assert.assertEquals(uri.getName(), node.getName());
        Assert.assertEquals(target, node.getTarget());
    }

    @Test
    public void testInstantiationsWithProperties() throws Exception {
        final String slashPath1 = "/" + ROOT_NODE + TestUtil.uniqueStringOnTime();
        final String slashPath2 = "/" + ROOT_NODE + TestUtil.uniqueStringOnTime();
        VOSURI uri = new VOSURI(VOS_URI + slashPath1);
        URI target = new URI(VOS_URI + slashPath2);

        Set<NodeProperty> properties = new TreeSet<NodeProperty>();
        properties.add(new NodeProperty(VOS.PROPERTY_URI_TITLE, "sz_title"));
        properties.add(new NodeProperty(VOS.PROPERTY_URI_CREATOR, "sz_creator"));

        LinkNode node = new LinkNode(uri.getName(), target);
        node.getProperties().addAll(properties);

        Assert.assertEquals(uri.getName(), node.getName());
        Assert.assertEquals(target, node.getTarget());
        Set<NodeProperty> actualProperties = node.getProperties();
        Assert.assertEquals(properties.size(), actualProperties.size());
        for (NodeProperty property : properties) {
            Assert.assertTrue(actualProperties.contains(property));
        }
    }
}
