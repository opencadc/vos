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
package ca.nrc.cadc.vos.client.ui;

import ca.nrc.cadc.util.Log4jInit;
import ca.nrc.cadc.vos.ContainerNode;
import ca.nrc.cadc.vos.DataNode;
import ca.nrc.cadc.vos.VOSURI;
import java.io.File;
import java.net.URI;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.junit.BeforeClass;
import org.junit.Test;
import static org.junit.Assert.*;

/**
 *
 * @author jburke
 */
public class FileSystemScannerTest
{
    private static Logger log = Logger.getLogger(FileSystemScannerTest.class);

    private static VOSURI TEST_VOSURI;

    public FileSystemScannerTest() { }

    @BeforeClass
    public static void setUpClass() throws Exception
    {
        Log4jInit.setLevel("ca.nrc.cadc.vos.client.ui", Level.DEBUG);
        TEST_VOSURI = new VOSURI(new URI("vos://cadc.nrc.ca!vospace/root"));
    }

    @Test
    public void testIsSymLink()
    {
        try
        {
            FileSystemScanner scanner = new FileSystemScanner();

            File file = new File("test/src/resources/testFile");
            File symlink = new File("test/src/resources/testSymLink");
            
            assertFalse("File is not a symlink", scanner.isSymLink(file));
            assertTrue("Should return true for a symlink", scanner.isSymLink(symlink));
        }
        catch(Exception unexpected)
        {
            log.error("unexpected exception", unexpected);
            fail("unexpected exception: " + unexpected);
        }
    }

    @Test
    public void testQueueDataNode()
    {
        try
        {
            CommandQueue queue = new CommandQueue(1, null);
            File sourceFile = new File("test");
            FileSystemScanner scanner = new FileSystemScanner(sourceFile, TEST_VOSURI, queue);

            File file = new File("test/src/resources/testFile");
            scanner.queueDataNode(file);

            assertNotNull(queue.peek());
        }
        catch(Exception unexpected)
        {
            log.error("unexpected exception", unexpected);
            fail("unexpected exception: " + unexpected);
        }
    }

    @Test
    public void testQueueContainerNode()
    {
        try
        {
            CommandQueue queue = new CommandQueue(1, null);
            File sourceFile = new File("test");
            FileSystemScanner scanner = new FileSystemScanner(sourceFile, TEST_VOSURI, queue);

            File file = new File("test/src/resources/testFile");
            scanner.queueContainerNode(file);

            assertNotNull(queue.peek());
        }
        catch(Exception unexpected)
        {
            log.error("unexpected exception", unexpected);
            fail("unexpected exception: " + unexpected);
        }
    }

    @Test
    public void testGetRelativePath()
    {
        try
        {
            File sourceFile = new File("/a/b/c");
            FileSystemScanner scanner = new FileSystemScanner(sourceFile, null, null);
            
            File file = new File("/a/b/c/d/e/f");
            String path = scanner.getRelativePath(file);
            assertEquals("/c/d/e/f", path);
        }
        catch(Exception unexpected)
        {
            log.error("unexpected exception", unexpected);
            fail("unexpected exception: " + unexpected);
        }
    }

    @Test
    public void testOfferToCommandQueue()
    {
        try
        {
            CommandQueue queue = new CommandQueue(1, null);
            FileSystemScanner scanner = new FileSystemScanner(null, null, queue);

            ContainerNode node = new ContainerNode(TEST_VOSURI);
            VOSpaceCommand command = new CreateDirectory(node);
            
            scanner.offerToCommandQueue(command);

            assertNotNull("Queue peek should return a VOSpaceCommand", queue.peek());
        }
        catch(Exception unexpected)
        {
            log.error("unexpected exception", unexpected);
            fail("unexpected exception: " + unexpected);
        }
    }
    
}
