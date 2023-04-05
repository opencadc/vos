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

import ca.nrc.cadc.auth.HttpPrincipal;
import ca.nrc.cadc.auth.NumericPrincipal;
import ca.nrc.cadc.util.Log4jInit;
import java.net.URI;
import java.security.MessageDigest;
import java.security.Principal;
import java.util.Set;
import java.util.TreeSet;
import java.util.UUID;
import javax.security.auth.Subject;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.junit.Assert;
import org.junit.Test;

/**
 *
 * @author pdowler
 */
public class NodeEntityTest {
    private static final Logger log = Logger.getLogger(NodeEntityTest.class);

    static {
        Log4jInit.setLevel("org.opencadc.persist", Level.INFO);
        Log4jInit.setLevel("org.opencadc.vospace", Level.INFO);
    }
    
    public NodeEntityTest() { 
    }
    
    @Test
    public void testContainerNode() {
        try {
            ContainerNode n = new ContainerNode("foo", false);
            final URI mcs1 = n.computeMetaChecksum(MessageDigest.getInstance("MD5"));
            
            // not state
            //n.appData = 1L;
            //URI mcs = n.computeMetaChecksum(MessageDigest.getInstance("MD5"));
            //Assert.assertEquals(mcs1, mcs);
            
            //n.version = VOS.VOSPACE_20;
            //mcs = n.computeMetaChecksum(MessageDigest.getInstance("MD5"));
            //Assert.assertEquals(mcs1, mcs);
            
            NumericPrincipal np = new NumericPrincipal(UUID.randomUUID());
            HttpPrincipal up = new HttpPrincipal("foo");
            Set<Principal> pset = new TreeSet<>();
            n.creatorID = new Subject();
            n.creatorID.getPrincipals().add(up);
            n.creatorID.getPrincipals().add(np);
            URI mcs = n.computeMetaChecksum(MessageDigest.getInstance("MD5"));
            Assert.assertEquals(mcs1, mcs);
            
            n.accepts.add(VOS.VIEW_BINARY);
            mcs = n.computeMetaChecksum(MessageDigest.getInstance("MD5"));
            Assert.assertEquals(mcs1, mcs);
            
            n.provides.add(VOS.VIEW_DEFAULT);
            mcs = n.computeMetaChecksum(MessageDigest.getInstance("MD5"));
            Assert.assertEquals(mcs1, mcs);
            
            // state
            n.ownerID = np;
            mcs = n.computeMetaChecksum(MessageDigest.getInstance("MD5"));
            Assert.assertNotEquals(mcs1, mcs);
            n.ownerID = null;
            
            n.isLocked = true;
            mcs = n.computeMetaChecksum(MessageDigest.getInstance("MD5"));
            Assert.assertNotEquals(mcs1, mcs);
            n.isLocked = null;
            
            n.isPublic = true;
            mcs = n.computeMetaChecksum(MessageDigest.getInstance("MD5"));
            Assert.assertNotEquals(mcs1, mcs);
            n.isPublic = null;
            
            n.properties.add(new NodeProperty(VOS.PROPERTY_URI_AVAILABLESPACE, "666"));
            mcs = n.computeMetaChecksum(MessageDigest.getInstance("MD5"));
            Assert.assertNotEquals(mcs1, mcs);
            n.properties.clear();
            
            n.readOnlyGroup.add(URI.create("ivo://opencadc.org/gms#g1"));
            n.readOnlyGroup.add(URI.create("ivo://opencadc.org/gms#g2"));
            mcs = n.computeMetaChecksum(MessageDigest.getInstance("MD5"));
            Assert.assertNotEquals(mcs1, mcs);
            final URI rog1 = mcs;
            n.readOnlyGroup.clear();
            
            n.readOnlyGroup.add(URI.create("ivo://opencadc.org/gms#g2"));
            n.readOnlyGroup.add(URI.create("ivo://opencadc.org/gms#g1"));
            mcs = n.computeMetaChecksum(MessageDigest.getInstance("MD5"));
            Assert.assertEquals(rog1, mcs); // must be sorted
            Assert.assertNotEquals(mcs1, mcs);
            n.readOnlyGroup.clear();
            
            n.readWriteGroup.add(URI.create("ivo://opencadc.org/gms#g1"));
            n.readWriteGroup.add(URI.create("ivo://opencadc.org/gms#g2"));
            mcs = n.computeMetaChecksum(MessageDigest.getInstance("MD5"));
            Assert.assertNotEquals(mcs1, mcs);
            final URI rwg1 = mcs;
            n.readWriteGroup.clear();
            
            n.readWriteGroup.add(URI.create("ivo://opencadc.org/gms#g2"));
            n.readWriteGroup.add(URI.create("ivo://opencadc.org/gms#g1"));
            mcs = n.computeMetaChecksum(MessageDigest.getInstance("MD5"));
            Assert.assertEquals(rwg1, mcs); // must be sorted
            Assert.assertNotEquals(mcs1, mcs);
            n.readWriteGroup.clear();
            
            // child entities
            n.nodes.add(new ContainerNode("foo", false));
            n.nodes.add(new DataNode("bar"));
            mcs = n.computeMetaChecksum(MessageDigest.getInstance("MD5"));
            Assert.assertEquals(mcs1, mcs);
            
        } catch (Exception unexpected) {
            log.error("unexpected exception", unexpected);
            Assert.fail("unexpected exception: " + unexpected);
        }
    }
}