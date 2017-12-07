/*
************************************************************************
*******************  CANADIAN ASTRONOMY DATA CENTRE  *******************
**************  CENTRE CANADIEN DE DONNÉES ASTRONOMIQUES  **************
*
*  (c) 2009.                            (c) 2009.
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

package org.opencadc.cavern;

import java.io.File;
import java.net.URI;
import java.security.PrivilegedExceptionAction;
import java.util.ArrayList;
import java.util.List;

import javax.security.auth.Subject;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.junit.BeforeClass;
import org.junit.Test;

import ca.nrc.cadc.auth.SSLUtil;
import ca.nrc.cadc.util.FileUtil;
import ca.nrc.cadc.util.Log4jInit;
import ca.nrc.cadc.util.StringUtil;
import ca.nrc.cadc.uws.ErrorSummary;
import ca.nrc.cadc.uws.ExecutionPhase;
import ca.nrc.cadc.vos.Direction;
import ca.nrc.cadc.vos.Node;
import ca.nrc.cadc.vos.Protocol;
import ca.nrc.cadc.vos.Transfer;
import ca.nrc.cadc.vos.VOS;
import ca.nrc.cadc.vos.VOSURI;
import ca.nrc.cadc.vos.View;
import ca.nrc.cadc.vos.client.ClientTransfer;
import ca.nrc.cadc.vos.client.VOSpaceClient;
import junit.framework.Assert;

public class MetadataIntTest
{

    private static final Logger log = Logger.getLogger(MetadataIntTest.class);

    private static File SSL_CERT;

//    private static String baseURL;
    private static VOSURI baseURI;

    static
    {
        Log4jInit.setLevel("ca.nrc.cadc.vospace", Level.INFO);
        Log4jInit.setLevel("ca.nrc.cadc.vos", Level.INFO);
    }

    public MetadataIntTest()
    {
    }

    @BeforeClass
    public static void staticInit() throws Exception
    {
        SSL_CERT = FileUtil.getFileFromResource("x509_CADCRegtest1.pem", MetadataIntTest.class);

        String uriProp = MetadataIntTest.class.getName() + ".baseURI";
        String uri = System.getProperty(uriProp);
        log.debug(uriProp + " = " + uri);
        if ( StringUtil.hasText(uri) )
        {
            baseURI = new VOSURI(new URI(uri));
//            nodeURI = vuri.getURI().toASCIIString();
//            RegistryClient rc = new RegistryClient();
//            URL vos = rc.getServiceURL(vuri.getServiceURI(), "https");
//            baseURL = vos.toExternalForm();
        }
        else
            throw new IllegalStateException("expected system property " + uriProp + " = <base vos URI>, found: " + uri);
    }

    private static class DeleteNodeAction implements PrivilegedExceptionAction<Node>
    {
        VOSpaceClient vos;
        String path;

        DeleteNodeAction(VOSpaceClient vos, String path)
        {
            this.vos = vos;
            this.path = path;
        }
        public Node run() throws Exception
        {
            try
            {
                vos.deleteNode(path);
            }
            catch (Exception ignore) {}
            return null;
        }
    }

    private static class GetNodeAction implements PrivilegedExceptionAction<Node>
    {
        VOSpaceClient vos;
        String path;

        GetNodeAction(VOSpaceClient vos, String path)
        {
            this.vos = vos;
            this.path = path;
        }
        public Node run() throws Exception
        {
            return vos.getNode(path);
        }
    }

    private static class UploadNodeAction implements PrivilegedExceptionAction<Node>
    {
        VOSpaceClient vos;
        VOSURI uri;
        String md5;
        File file;

        UploadNodeAction(VOSpaceClient vos, VOSURI uri, String md5, File file)
        {
            this.vos = vos;
            this.uri = uri;
            this.md5 = md5;
            this.file = file;
        }
        public Node run() throws Exception
        {
            View view = new View(new URI(VOS.VIEW_DEFAULT));
            List<Protocol> protocols = new ArrayList<Protocol>();
            protocols.add(new Protocol(VOS.PROTOCOL_HTTPS_PUT));

            Transfer transfer = new Transfer(uri.getURI(), Direction.pushToVoSpace, view, protocols);
            ClientTransfer clientTransfer = vos.createTransfer(transfer);

            clientTransfer.setFile(file);
            if (md5 != null)
                clientTransfer.setRequestProperty("Content-MD5", md5);

            clientTransfer.runTransfer();

            ExecutionPhase ep = clientTransfer.getPhase();
            if ( ExecutionPhase.ERROR.equals(ep) )
            {
                ErrorSummary es = clientTransfer.getServerError();
                throw new RuntimeException(es.getSummaryMessage());
            }
            else if ( ExecutionPhase.ABORTED.equals(ep) )
                throw new RuntimeException("transfer aborted by service");
            else if ( !ExecutionPhase.COMPLETED.equals(ep) )
                throw new RuntimeException("unexpected job state: " + ep.name());

            return vos.getNode(uri.getPath());
        }
    }

    @Test
    public void testMetadataUpdates() throws Exception
    {
        VOSpaceClient vos = new VOSpaceClient(baseURI.getServiceURI());
        String vosuripath = baseURI.toString() + "/metadataIntTest-" + System.currentTimeMillis();
        VOSURI uri = new VOSURI(vosuripath);
        Subject s = SSLUtil.createSubject(SSL_CERT);

        try
        {
            File testFile1 = new File("src/test/resources/md5file1");
            File testFile2 = new File("src/test/resources/md5file2");
            String correctSize1 = "25";
            String correctSize2 = "33";
            String correctMD51 = "86bec12f968870284258e4f239e1300c";
            String correctMD52 = "7589efa0a3af85d73b6e9d37ec5f2e7c";
            String incorrectMD5 = "12343d07086471dbf52398083a993cf7";

            UploadNodeAction upload = null;
            GetNodeAction get = null;
            Node result = null;

            // 1. put a new file, fail -> check md5 is null
            upload = new UploadNodeAction(vos, uri, incorrectMD5, testFile1);
            try
            {
                result = Subject.doAs(s, upload);
                Assert.fail("Should have failed due to incorrect md5");
            }
            catch (Exception e)
            {
                log.debug(e);
                // expected
            }
            get = new GetNodeAction(vos, uri.getPath());
            result = Subject.doAs(s, get);
            log.debug("MD5: " + result.getPropertyValue(VOS.PROPERTY_URI_CONTENTMD5) + " (expecting null)");
            Assert.assertNull("Wrong MD5", result.getPropertyValue(VOS.PROPERTY_URI_CONTENTMD5));
            Assert.assertEquals("Wrong Size", "0", result.getPropertyValue(VOS.PROPERTY_URI_CONTENTLENGTH));

            // 2. put a new file -> check md5 correct
            upload = new UploadNodeAction(vos, uri, correctMD51, testFile1);
            result = Subject.doAs(s, upload);
            log.debug("expected md5: " + correctMD51 + "  actual: " + result.getPropertyValue(VOS.PROPERTY_URI_CONTENTMD5));
            Assert.assertEquals("Wrong MD5", correctMD51, result.getPropertyValue(VOS.PROPERTY_URI_CONTENTMD5));
            Assert.assertEquals("Wrong Size", correctSize1, result.getPropertyValue(VOS.PROPERTY_URI_CONTENTLENGTH));

            // 3. replace a file -> check new md5 correct
            upload = new UploadNodeAction(vos, uri, correctMD52, testFile2);
            result = Subject.doAs(s, upload);
            Assert.assertEquals("Wrong MD5", correctMD52, result.getPropertyValue(VOS.PROPERTY_URI_CONTENTMD5));
            Assert.assertEquals("Wrong Size", correctSize2, result.getPropertyValue(VOS.PROPERTY_URI_CONTENTLENGTH));

            // 4. replace a file, fail -> check md5 is previous value
            upload = new UploadNodeAction(vos, uri, incorrectMD5, testFile1);
            try
            {
                result = Subject.doAs(s, upload);
                Assert.fail("Should have failed due to incorrect md5");
            }
            catch (Exception e)
            {
                log.debug(e);
                // expected
            }
            get = new GetNodeAction(vos, uri.getPath());
            result = Subject.doAs(s, get);
            Assert.assertEquals("Wrong MD5", correctMD52, result.getPropertyValue(VOS.PROPERTY_URI_CONTENTMD5));
            Assert.assertEquals("Wrong Size", correctSize2, result.getPropertyValue(VOS.PROPERTY_URI_CONTENTLENGTH));
        }
        finally
        {
            try
            {
//                DeleteNodeAction delete = new DeleteNodeAction(vos, uri.getPath());
//                Subject.doAs(s, delete);
            }
            catch (Throwable t)
            {
                // ok
            }
        }

    }

}
