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

package org.opencadc.vospace.server;

import ca.nrc.cadc.util.Log4jInit;
import java.net.URI;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.Set;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.junit.Assert;
import org.junit.Test;
import org.opencadc.vospace.ContainerNode;
import org.opencadc.vospace.Node;
import org.opencadc.vospace.NodeProperty;

/**
 * Utility methods
 *
 *  @author adriand
 *
 */
public class UtilsTest
{
    private static final Logger log = Logger.getLogger(UtilsTest.class);

    static {
        Log4jInit.setLevel("org.opencadc.raven", Level.DEBUG);
    }

    @Test
    public void testGetParentPath() throws Exception {
        Assert.assertNull(Utils.getParentPath(null));
        Assert.assertNull(Utils.getParentPath("abc"));
        Assert.assertNull(Utils.getParentPath("/abc"));
        Assert.assertNull(Utils.getParentPath("/abc/"));
        Assert.assertNull(Utils.getParentPath("abc/"));
        Assert.assertEquals("abc", Utils.getParentPath("abc/def"));
        Assert.assertEquals("abc", Utils.getParentPath("/abc/def"));
        Assert.assertEquals("abc", Utils.getParentPath("abc/def/"));
        Assert.assertEquals("abc/def", Utils.getParentPath("abc/def/ghi"));
        Assert.assertEquals("abc/def", Utils.getParentPath("/abc/def/ghi"));
    }

    @Test
    public void testIsRoot() throws Exception {
        Assert.assertTrue(Utils.isRoot(""));
        Assert.assertTrue(Utils.isRoot("/"));
        Assert.assertTrue(Utils.isRoot(null));
        Assert.assertFalse(Utils.isRoot("abc"));
        Assert.assertFalse(Utils.isRoot("/abc"));
        Assert.assertFalse(Utils.isRoot("abc/"));
    }

    @Test
    public  void testGetNodeList() throws Exception {
        Node testNode = new ContainerNode("foo", false);
        Assert.assertEquals(1, Utils.getNodeList(testNode).size());

        testNode = new ContainerNode("ghi", false);
        testNode.parent = new ContainerNode("def", false);
        testNode.parent.parent = new ContainerNode("abc", false);

        LinkedList<Node> nodeList = Utils.getNodeList(testNode);
        Assert.assertEquals(3, nodeList.size());
        Assert.assertEquals("ghi", nodeList.get(0).getName());
        Assert.assertEquals("def", nodeList.get(1).getName());
        Assert.assertEquals("abc", nodeList.get(2).getName());
    }

    @Test
    public void testGetPath() throws Exception {
        Node testNode = new ContainerNode("ghi", false);
        testNode.parent = new ContainerNode("def", false);
        testNode.parent.parent = new ContainerNode("abc", false);

        Assert.assertEquals("abc/def/ghi", Utils.getPath(testNode));
        Assert.assertEquals("abc/def", Utils.getPath(testNode.parent));
        Assert.assertEquals("abc", Utils.getPath(testNode.parent.parent));
    }

    @Test
    public void testUpdateNodeProperties() throws Exception {
        NodeProperty np1 = new NodeProperty(URI.create("prop1"), "val1");

        Set<NodeProperty> oldProps = new HashSet<>();
        Set<NodeProperty> newProps = new HashSet<>();

        // no new or old properties
        Utils.updateNodeProperties(oldProps, newProps);
        Assert.assertEquals(0, oldProps.size());

        // add property
        newProps.add(np1);
        Utils.updateNodeProperties(oldProps, newProps);
        Assert.assertEquals(1, oldProps.size());
        Assert.assertTrue(oldProps.contains(np1));

        // no changes
        newProps.clear();
        Utils.updateNodeProperties(oldProps, newProps);
        Assert.assertEquals(1, oldProps.size());
        Assert.assertTrue(oldProps.contains(np1));

        // delete property
        NodeProperty np1_del = new NodeProperty(URI.create("prop1")); // mark for deletion
        newProps.add(np1_del);
        Utils.updateNodeProperties(oldProps, newProps);
        Assert.assertEquals(0, oldProps.size());

        // delete non existing property (this is the sanitize scenario in CreateNode action
        Utils.updateNodeProperties(oldProps, newProps);
        Assert.assertEquals(0, oldProps.size());

        // add new element and change value of existing
        oldProps.add(np1);
        NodeProperty np2 = new NodeProperty(URI.create("prop2"), "val2");
        newProps.clear();
        newProps.add(np2);
        NodeProperty np1_update = new NodeProperty(URI.create("prop1"), "val1_updated");
        newProps.add(np1_update);
        Utils.updateNodeProperties(oldProps, newProps);
        Assert.assertEquals(2, oldProps.size());
        Assert.assertTrue(oldProps.contains(np1_update));
        Assert.assertTrue(oldProps.contains(np2));
    }
}
