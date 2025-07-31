/*
************************************************************************
*******************  CANADIAN ASTRONOMY DATA CENTRE  *******************
**************  CENTRE CANADIEN DE DONNÉES ASTRONOMIQUES  **************
*
*  (c) 2017.                            (c) 2017.
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

package org.opencadc.cavern.nodes;

import ca.nrc.cadc.auth.AuthenticationUtil;
import ca.nrc.cadc.auth.HttpPrincipal;
import ca.nrc.cadc.auth.IdentityManager;
import ca.nrc.cadc.auth.NoOpIdentityManager;
import ca.nrc.cadc.auth.PosixPrincipal;
import ca.nrc.cadc.util.Log4jInit;
import java.security.Principal;
import java.security.PrivilegedExceptionAction;
import java.util.Set;
import javax.security.auth.Subject;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.junit.Assert;
import org.junit.Test;

/**
 *
 * @author pdowler
 */
public class PosixIdentityManagerTest {
    private static final Logger log = Logger.getLogger(PosixIdentityManagerTest.class);

    static {
        Log4jInit.setLevel("org.opencadc.cavern", Level.INFO);
    }

    Subject subject = AuthenticationUtil.getAnonSubject();
    
    public PosixIdentityManagerTest() {
    }

    @Test
    public void testNull() {
        System.setProperty(PosixIdentityManager.WRAPPED_IDENTITY_MANAGER_CLASS_PROPERTY, NoOpIdentityManager.class.getName());

        try {
            PosixIdentityManager im = new PosixIdentityManager();

            Assert.assertNull("toOwner", im.toOwner(null));

            Assert.assertNull("toSubject", im.toSubject(null));

            Assert.assertNull("toDisplayString", im.toDisplayString(null));
            
        } catch (Exception unexpected) {
            log.error("unexpected exception", unexpected);
            Assert.fail("unexpected exception: " + unexpected);
        } finally {
            System.getProperties().remove(PosixIdentityManager.WRAPPED_IDENTITY_MANAGER_CLASS_PROPERTY);
        }
    }

    @Test
    public void testCache() {
        log.warn("technical debt: testCache not implemented yet");
    }
    
    /*
    // this requires a configured and functional IdentityManager + back end services
    // and doesn't belong in unit test
    @Test
    public void testRoundTrip() {
        try {
            // request subject contains: numeric, http, posix
            PosixPrincipal orig = new PosixPrincipal(54321);
            Subject s = AuthenticationUtil.getAnonSubject();
            s.getPrincipals().add(orig);
            s.getPrincipals().add(new HttpPrincipal("somebody"));
            
            PosixIdentityManager im = new PosixIdentityManager();
            
            // the value to "store"
            log.info("orig: " + s);
            Object o = im.toOwner(s);
            Assert.assertNotNull(o);
            log.info("toOwner: " + o.getClass().getSimpleName() + " " + o);
            
            Subject restored = Subject.doAs(subject, (PrivilegedExceptionAction<Subject>) () -> im.toSubject(o));
            log.info("restored: " + restored);
            
            // default IM to delegate to cannot augment
            Set<Principal> all = restored.getPrincipals();
            Assert.assertNotNull(all);
            Assert.assertEquals(1, all.size());
            
            Set<PosixPrincipal> ps = restored.getPrincipals(PosixPrincipal.class);
            Assert.assertNotNull(ps);
            Assert.assertFalse(ps.isEmpty());
            PosixPrincipal actual = ps.iterator().next();
            Assert.assertEquals(orig, actual);
            

        } catch (Exception unexpected) {
            log.error("unexpected exception", unexpected);
            Assert.fail("unexpected exception: " + unexpected);
        }
    }
    */
}
