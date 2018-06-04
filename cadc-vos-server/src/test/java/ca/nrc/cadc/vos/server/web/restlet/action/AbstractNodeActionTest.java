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

package ca.nrc.cadc.vos.server.web.restlet.action;


import static org.easymock.EasyMock.createMock;

import ca.nrc.cadc.vos.server.GetChildParameters;
import java.net.URI;
import java.net.URL;

import org.junit.Test;
import org.restlet.Request;
import org.restlet.data.Reference;

import ca.nrc.cadc.vos.AbstractCADCVOSTest;
import ca.nrc.cadc.vos.ContainerNode;
import ca.nrc.cadc.vos.Node;
import ca.nrc.cadc.vos.VOSURI;
import ca.nrc.cadc.vos.server.NodePersistence;
import ca.nrc.cadc.vos.server.auth.VOSpaceAuthorizer;


public abstract class AbstractNodeActionTest<N extends NodeAction>
        extends AbstractCADCVOSTest<N>
{
    protected final String VOS_URI_PREFIX = "vos://example.com!vospace";

    private Node mockNodeC = createMock(Node.class);
    private Node mockNodeS = createMock(Node.class);

    private NodePersistence mockNodePersistence =
            createMock(NodePersistence.class);
    private Request mockRequest = createMock(Request.class);
    protected VOSpaceAuthorizer mockAuth = createMock(VOSpaceAuthorizer.class);
    protected VOSpaceAuthorizer mockPartialPathAuth = createMock(VOSpaceAuthorizer.class);
    private Reference mockRef = createMock(Reference.class);
    protected ContainerNode mockParentNode = createMock(ContainerNode.class);
    protected VOSURI mockVOS = createMock(VOSURI.class);
    protected VOSURI mockParentVOS = createMock(VOSURI.class);
    private URL fakeURL;

    protected String nodeName = "child";
    protected String vosURI = VOS_URI_PREFIX + "/parent/" + nodeName;
    protected URI vosURIObject;

    @Test
    public void performNodeAction() throws Exception
    {
        fakeURL = new URL("http://example/com/foo");
        vosURIObject = new URI(vosURI);
        getTestSubject().setNodePersistence(mockNodePersistence);
        getTestSubject().setPartialPathVOSpaceAuthorizer(mockPartialPathAuth);
        getTestSubject().setRequest(mockRequest);
        getTestSubject().setVOSpaceAuthorizer(mockAuth);
        getTestSubject().setVosURI(mockVOS);

        prePerformNodeAction();
        Node cnode = getTestSubject().doAuthorizationCheck();
        NodeActionResult result =
                getTestSubject().performNodeAction(cnode, getMockNodeS());
        postPerformNodeAction(result);
    }

    /**
     * Any necessary preface action before the performNodeAction method is
     * called to be tested.  This is a good place for Mock expectations and/or
     * replays to be set.
     *
     * @throws Exception    If anything goes wrong, just pass it up.
     */
    protected abstract void prePerformNodeAction() throws Exception;

    /**
     * Any necessary post method call result checking.  This is a good place
     * for any Mock verifications to take place as well.
     *
     * @param result        The result of the performNodeAction call.
     * @throws Exception    If anything goes wrong, just pass it up.
     */
    protected abstract void postPerformNodeAction(
            final NodeActionResult result) throws Exception;

    public VOSpaceAuthorizer getMockAuth()
    {
        return mockAuth;
    }

    public Node getMockNodeC()
    {
        return mockNodeC;
    }

    public Node getMockNodeS()
    {
        return mockNodeS;
    }

    public NodePersistence getMockNodePersistence()
    {
        return mockNodePersistence;
    }

    public Request getMockRequest()
    {
        return mockRequest;
    }
    public Reference getMockRef()
    {
        return mockRef;
    }
    public URL getMockURL()
    {
        return fakeURL;
    }
}
