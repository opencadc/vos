/*
 ************************************************************************
 *******************  CANADIAN ASTRONOMY DATA CENTRE  *******************
 **************  CENTRE CANADIEN DE DONNÉES ASTRONOMIQUES  **************
 *
 *  (c) 2016.                            (c) 2016.
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
 *
 ************************************************************************
 */

package ca.nrc.cadc.vos.server.web.restlet.action;


import ca.nrc.cadc.vos.*;

import org.junit.Assert;
import org.restlet.data.Form;
import org.restlet.data.MediaType;

import java.net.URI;
import java.security.AccessControlException;
import java.util.ArrayList;
import java.util.List;

import static org.easymock.EasyMock.*;


public class GetDetailedNodeActionTest extends GetNodeActionTest
{
    final ContainerNode mockServerNode = createMock(ContainerNode.class);
    final VOSURI testVOSURI = new VOSURI(URI.create(VOS_URI_PREFIX
                                                    + "/user/"));
    final List<NodeProperty> serverNodeProperties = new ArrayList<>();
    final List<Node> childNodes = new ArrayList<>();
    final ContainerNode childDir1 = new ContainerNode(
            new VOSURI(URI.create(VOS_URI_PREFIX + "/user/childdir1")));
    final ContainerNode childDir2 = new ContainerNode(
            new VOSURI(URI.create(VOS_URI_PREFIX + "/user/childdir2")));
    final DataNode childFile1 = new DataNode(
            new VOSURI(URI.create(VOS_URI_PREFIX + "/user/childfile1")));


    /**
     * Any necessary preface action before the performNodeAction method is
     * called to be tested.  This is a good place for Mock expectations and/or
     * replays to be set.
     *
     * @throws Exception If anything goes wrong, just pass it up.
     */
    @Override
    protected void prePerformNodeAction() throws Exception
    {
        final Form queryForm = createMock(Form.class);

        childNodes.add(childDir1);
        childNodes.add(childDir2);
        childNodes.add(childFile1);

        // Override this here.
        getTestSubject().setVosURI(testVOSURI);

        expect(queryForm.getFirstValue("view")).andReturn(
                "VIEW/REFERENCE").once();
        expect(queryForm.getFirstValue("detail")).andReturn(
                VOS.Detail.max.getValue()).once();
        expect(queryForm.getFirstValue("uri")).andReturn(null).once();
        expect(queryForm.getFirstValue("limit")).andReturn(null).once();

        getTestSubject().setQueryForm(queryForm);

        expect(getMockRef().toUrl()).andReturn(getMockURL()).atLeastOnce();
        expect(getMockRequest().getOriginalRef()).andReturn(
                getMockRef()).atLeastOnce();

        expect(mockServerNode.getProperties()).andReturn(
                serverNodeProperties).times(2);

        getMockAbstractView().setNode(mockServerNode, "VIEW/REFERENCE",
                                      getMockURL());
        expectLastCall().once();

        expect(getMockAbstractView().getRedirectURL()).andReturn(null).once();
        expect(getMockAbstractView().getMediaType())
                .andReturn(MediaType.TEXT_XML).
                once();

        getMockNodePersistence().getProperties(mockServerNode);
        expectLastCall().once();

        expect(mockServerNode.getNodes()).andReturn(childNodes).times(2);
        expect(mockServerNode.getUri()).andReturn(testVOSURI).anyTimes();

        expect(mockAuth.getReadPermission(childDir1)).andReturn(
                childDir1).once();
        expect(mockAuth.getReadPermission(childDir2)).andThrow(
                new AccessControlException("Unauthorized.")).once();
        expect(mockAuth.getReadPermission(childFile1)).andReturn(
                childFile1).once();

        expect(mockAuth.getWritePermission(childDir1)).andThrow(
                new AccessControlException("Unauthorized write.")).once();
        expect(mockAuth.getWritePermission(childDir2)).andReturn(
                childDir2).once();
        expect(mockAuth.getWritePermission(childFile1)).andThrow(
                new AccessControlException("Unauthorized write.")).once();

        expect(mockAuth.getReadPermission(mockServerNode)).andReturn(
                mockServerNode).once();
        expect(mockAuth.getWritePermission(mockServerNode)).andReturn(
                mockServerNode).once();

        expect(mockPartialPathAuth.getReadPermission(testVOSURI.getURI())).
                andReturn(mockServerNode).atLeastOnce();

        getMockNodePersistence().getChildren(mockServerNode);
        expectLastCall().once();

        replay(queryForm);
        replay(getMockRef());
        replay(getMockRequest());
        replay(getMockAbstractView());
        replay(getMockNodePersistence());
        replay(mockAuth);
        replay(mockServerNode);
        replay(mockPartialPathAuth);
    }

    @Override
    public Node getMockNodeS()
    {
        return mockServerNode;
    }

    /**
     * Any necessary post method call result checking.  This is a good place
     * for any Mock verifications to take place as well.
     *
     * @param result The result of the performNodeAction call.
     * @throws Exception If anything goes wrong, just pass it up.
     */
    @Override
    protected void postPerformNodeAction(final NodeActionResult result)
            throws Exception
    {
        verify(getMockRef());
        verify(getMockRequest());
        verify(getMockAbstractView());
        verify(getMockNodePersistence());
        verify(mockServerNode);

        Assert.assertEquals("Should have readable property.",
                            "true", childDir1.getPropertyValue(
                        VOS.PROPERTY_URI_READABLE));
        Assert.assertEquals("Should have readable property.",
                            "true", childFile1.getPropertyValue(
                        VOS.PROPERTY_URI_READABLE));
        Assert.assertNull("Should not have readable property.",
                          childDir2.getPropertyValue(
                                  VOS.PROPERTY_URI_READABLE));
        Assert.assertEquals("Should have readable property.",
                            "true", childDir2.getPropertyValue(
                        VOS.PROPERTY_URI_WRITABLE));

        final NodeProperty writableProperty =
                new NodeProperty(VOS.PROPERTY_URI_WRITABLE, "true");
        final NodeProperty readableProperty =
                new NodeProperty(VOS.PROPERTY_URI_READABLE, "true");

        Assert.assertEquals("Should have writable property in server node.",
                            "true", serverNodeProperties.get(
                        serverNodeProperties.indexOf(writableProperty))
                                    .getPropertyValue());
        Assert.assertEquals("Should have readable property in server node.",
                            "true", serverNodeProperties.get(
                        serverNodeProperties.indexOf(readableProperty))
                                    .getPropertyValue());
    }
}
