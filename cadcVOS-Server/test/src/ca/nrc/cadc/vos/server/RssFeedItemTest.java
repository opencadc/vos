/*
 ************************************************************************
 *******************  CANADIAN ASTRONOMY DATA CENTRE  *******************
 **************  CENTRE CANADIEN DE DONNÉES ASTRONOMIQUES  **************
 *
 *  (c) 2010.                            (c) 2010.
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

package ca.nrc.cadc.vos.server;

import java.util.Date;
import java.util.Iterator;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.junit.Assert;
import org.junit.Test;

import ca.nrc.cadc.util.Log4jInit;
import ca.nrc.cadc.vos.ContainerNode;
import ca.nrc.cadc.vos.DataNode;
import ca.nrc.cadc.vos.server.util.FixedSizeTreeSet;

/**
 *
 * @author jburke
 */
public class RssFeedItemTest
{
    private static final Logger log = Logger.getLogger(RssFeedItemTest.class);
    static
    {
        Log4jInit.setLevel("ca.nrc.cadc.vos.server", Level.INFO);
    }

    public RssFeedItemTest() { }

    /**
     * Test of compareTo method, of class RssFeedItem.
     */
    @Test
    public void testCompareTo() {
        try
        {
            RssFeedItem older = new RssFeedItem(new Date(0L), null);
            RssFeedItem newer = new RssFeedItem(new Date(1000L), null);

            Assert.assertEquals(-1, newer.compareTo(older));
            Assert.assertEquals(0, newer.compareTo(newer));
            Assert.assertEquals(1, older.compareTo(newer));

        }
        catch(Exception unexpected)
        {
            log.error("unexpected exception", unexpected);
            Assert.fail("unexpected exception: " + unexpected);
        }
    }

    @Test
    public void testDateSorting()
    {
        try
        {
            FixedSizeTreeSet<RssFeedItem> set = new FixedSizeTreeSet<RssFeedItem>();
            set.setMaxSize(3);

            ContainerNode parent = RssFeedTest.createContainerNode("/parent", null, 2011, 1, 1);
            DataNode child1 = RssFeedTest.createDataNode("/parent/child1", parent, 2011, 2, 1);
            DataNode child2 = RssFeedTest.createDataNode("/parent/child2", parent, 2011, 3, 1);
            DataNode child3 = RssFeedTest.createDataNode("/parent/child3", parent, 2010, 4, 1);

            set.add(new RssFeedItem(new Date(10000000l), parent));
            set.add(new RssFeedItem(new Date(20000000l), child1));
            set.add(new RssFeedItem(new Date(30000000l), child2));
            set.add(new RssFeedItem(new Date(40000000l), child3));

            Assert.assertFalse(set.isEmpty());
            Assert.assertTrue(set.size() == 3);

            Iterator<RssFeedItem> it = set.iterator();
            Assert.assertEquals(child3, it.next().node);
            Assert.assertEquals(child2, it.next().node);
            Assert.assertEquals(child1, it.next().node);
        }
        catch(Exception unexpected)
        {
            log.error("unexpected exception", unexpected);
            Assert.fail("unexpected exception: " + unexpected);
        }
    }
}