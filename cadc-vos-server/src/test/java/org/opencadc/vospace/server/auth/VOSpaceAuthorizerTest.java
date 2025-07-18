/*
************************************************************************
*******************  CANADIAN ASTRONOMY DATA CENTRE  *******************
**************  CENTRE CANADIEN DE DONNÉES ASTRONOMIQUES  **************
*
*  (c) 2024.                            (c) 2024.
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

package org.opencadc.vospace.server.auth;

import ca.nrc.cadc.auth.HttpPrincipal;
import ca.nrc.cadc.auth.NumericPrincipal;
import ca.nrc.cadc.io.ResourceIterator;
import ca.nrc.cadc.net.TransientException;
import java.net.URI;
import java.util.Set;
import java.util.UUID;
import javax.security.auth.Subject;
import org.apache.log4j.Logger;
import org.junit.Assert;
import org.junit.Test;
import org.opencadc.vospace.ContainerNode;
import org.opencadc.vospace.Node;
import org.opencadc.vospace.NodeNotSupportedException;
import org.opencadc.vospace.NodeProperty;
import org.opencadc.vospace.NodeUtil;
import org.opencadc.vospace.VOS;
import org.opencadc.vospace.server.NodePersistence;
import org.opencadc.vospace.server.Utils;
import org.opencadc.vospace.server.Views;
import org.opencadc.vospace.server.transfers.TransferGenerator;

/**
 *
 * @author pdowler
 */
public class VOSpaceAuthorizerTest {
    private static final Logger log = Logger.getLogger(VOSpaceAuthorizerTest.class);

    public VOSpaceAuthorizerTest() { 
    }
    
    @Test
    public void testAllocationOwner() {
        NumericPrincipal n1 = new NumericPrincipal(UUID.randomUUID());
        HttpPrincipal h1 = new HttpPrincipal("somebody");
        Subject allocationOwner = new Subject();
        allocationOwner.getPrincipals().add(n1);
        allocationOwner.getPrincipals().add(h1);
        
        Subject other = new Subject();
        other.getPrincipals().add(new NumericPrincipal(UUID.randomUUID()));
        other.getPrincipals().add(new HttpPrincipal("other"));
        
        ContainerNode root = new ContainerNode(new UUID(0L, 0L), "");
        root.owner = new Subject();
        root.owner.getPrincipals().add(new NumericPrincipal(UUID.randomUUID()));
        
        ContainerNode home = new ContainerNode("home");
        home.parent = root;
        home.owner = root.owner;
        
        ContainerNode alloc = new ContainerNode("alloc");
        alloc.parent = home;
        alloc.owner = allocationOwner;
        
        ContainerNode sub1 = new ContainerNode("sub1");
        sub1.parent = alloc;
        sub1.owner = other;
        
        ContainerNode sub2 = new ContainerNode("sub2");
        sub2.parent = sub1;
        sub2.owner = other;
        
        final Subject caller = new Subject();
        DummyNodePersistent tnpi = new DummyNodePersistent();
        final VOSpaceAuthorizer auth = new VOSpaceAuthorizer(tnpi);
        
        caller.getPrincipals().addAll(allocationOwner.getPrincipals());
        Assert.assertFalse("sub2", auth.isAllocationOwner(sub2, caller));
        Assert.assertFalse("sub1", auth.isAllocationOwner(sub1, caller));
        Assert.assertFalse("alloc", auth.isAllocationOwner(alloc, caller));
        Assert.assertFalse("home", auth.isAllocationOwner(home, caller));
        Assert.assertFalse("root", auth.isAllocationOwner(root, caller));
        
        // owner of child nodes
        caller.getPrincipals().clear();
        caller.getPrincipals().addAll(other.getPrincipals());
        Assert.assertFalse("sub2", auth.isAllocationOwner(sub2, caller));
        Assert.assertFalse("sub1", auth.isAllocationOwner(sub1, caller));
        Assert.assertFalse("alloc", auth.isAllocationOwner(alloc, caller));
        Assert.assertFalse("home", auth.isAllocationOwner(home, caller));
        Assert.assertFalse("root", auth.isAllocationOwner(root, caller));
        
        // anon
        caller.getPrincipals().clear();
        Assert.assertFalse("sub2", auth.isAllocationOwner(sub2, caller));
        Assert.assertFalse("sub1", auth.isAllocationOwner(sub1, caller));
        Assert.assertFalse("alloc", auth.isAllocationOwner(alloc, caller));
        Assert.assertFalse("home", auth.isAllocationOwner(home, caller));
        Assert.assertFalse("root", auth.isAllocationOwner(root, caller));
        
        // random caller
        caller.getPrincipals().add(new NumericPrincipal(UUID.randomUUID()));
        caller.getPrincipals().add(new HttpPrincipal("caller"));
        Assert.assertFalse("sub2", auth.isAllocationOwner(sub2, caller));
        Assert.assertFalse("sub1", auth.isAllocationOwner(sub1, caller));
        Assert.assertFalse("alloc", auth.isAllocationOwner(alloc, caller));
        Assert.assertFalse("home", auth.isAllocationOwner(home, caller));
        Assert.assertFalse("root", auth.isAllocationOwner(root, caller));
        
        // make alloc an actual allocation
        tnpi.allocParentNode = home;

        Assert.assertFalse("sub2", auth.isAllocationOwner(sub2, caller));
        Assert.assertFalse("sub1", auth.isAllocationOwner(sub1, caller));
        Assert.assertFalse("alloc", auth.isAllocationOwner(alloc, caller));
        Assert.assertFalse("home", auth.isAllocationOwner(home, caller));
        Assert.assertFalse("root", auth.isAllocationOwner(root, caller));
        
        // alloc owner
        caller.getPrincipals().clear();
        caller.getPrincipals().add(h1);
        Assert.assertTrue("sub2", auth.isAllocationOwner(sub2, caller));
        Assert.assertTrue("sub1", auth.isAllocationOwner(sub1, caller));
        //Assert.assertTrue("alloc", auth.isAllocationOwner(alloc, caller));
        Assert.assertFalse("home", auth.isAllocationOwner(home, caller));
        Assert.assertFalse("root", auth.isAllocationOwner(root, caller));
        
        // root owner is never an alloc owner
        caller.getPrincipals().clear();
        caller.getPrincipals().addAll(root.owner.getPrincipals());
        Assert.assertFalse("sub2", auth.isAllocationOwner(sub2, caller));
        Assert.assertFalse("sub1", auth.isAllocationOwner(sub1, caller));
        Assert.assertFalse("alloc", auth.isAllocationOwner(alloc, caller));
        Assert.assertFalse("home", auth.isAllocationOwner(home, caller));
        Assert.assertFalse("root", auth.isAllocationOwner(root, caller));
        
    }

    class DummyNodePersistent implements NodePersistence {
        public ContainerNode allocParentNode = null;


        @Override
        public boolean isAllocation(ContainerNode node) {
            // very basic algorithm that detects if a node is an allocation node
            if (allocParentNode == null) {
                return false;
            }
            if (node.parent == null) {
                return false;
            }
            return Utils.getPath(allocParentNode).equals(Utils.getPath(node.parent));
        }

        /**
         * Check if the given caller can administer allocations.  Used by create operations.
         *
         * @param caller The caller subject, used to check permissions.
         * @return True if the given caller can create new administer new allocations, false otherwise.
         */
        @Override
        public boolean isAdmin(Subject caller) {
            return false;
        }

        /**
         * Get the admin grant for the given caller. This is used to log the specific API Key call.
         *
         * @param caller The caller subject, used to pull the token.
         * @return String grant, or null if not applicable or not available.
         */
        @Override
        public String getAdminGrant(Subject caller) {
            return null;
        }

        // methods below are not used/implemented
        @Override
        public Set<URI> getAdminProps() {
            return null;
        }

        @Override
        public URI getResourceID() {
            return null;
        }

        @Override
        public ContainerNode getRootNode() {
            return null;
        }

        @Override
        public Set<URI> getImmutableProps() {
            return null;
        }

        /**
         * Get the default value of a Node Property.  This will likely be used by create operations when values are missing.
         *
         * @param propertyKey The URI of the property key to get the default value for.
         * @return String default value for the given property key, or null if no default value is set.
         */
        @Override
        public String getDefaultPropertyValue(URI propertyKey) {
            return null;
        }

        @Override
        public Views getViews() {
            return null;
        }

        @Override
        public TransferGenerator getTransferGenerator() {
            return null;
        }

        @Override
        public Node get(ContainerNode parent, String name) throws TransientException {
            return null;
        }

        @Override
        public void getProperties(Node node) throws TransientException {

        }

        @Override
        public Node put(Node node) throws NodeNotSupportedException, TransientException {
            return null;
        }

        @Override
        public void move(Node node, ContainerNode dest, String newName) {

        }

        @Override
        public void delete(Node node) throws TransientException {

        }

        @Override
        public ResourceIterator<Node> iterator(ContainerNode parent, Integer limit, String start) {
            return null;
        }
    }
}
