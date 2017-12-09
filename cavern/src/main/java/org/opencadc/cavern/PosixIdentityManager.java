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

package org.opencadc.cavern;

import ca.nrc.cadc.auth.HttpPrincipal;
import ca.nrc.cadc.auth.IdentityManager;
import java.io.IOException;
import java.nio.file.attribute.UserPrincipal;
import java.nio.file.attribute.UserPrincipalLookupService;
import java.security.Principal;
import java.util.HashSet;
import java.util.Set;
import javax.security.auth.Subject;
import javax.security.auth.x500.X500Principal;
import org.apache.log4j.Logger;

/**
 * An IdentityManager implementation that can map from POSIX file system object
 * owner to subject.
 *
 * @author pdowler
 */
public class PosixIdentityManager implements IdentityManager {

    private static final Logger log = Logger.getLogger(PosixIdentityManager.class);

    private UserPrincipalLookupService users;
    
    private PosixIdentityManager() {
    }
    
    public PosixIdentityManager(UserPrincipalLookupService users) {
        this.users = users;
    }

    @Override
    public Subject toSubject(Object o) {
        if (o == null) {
            return null;
        }
        
        if (o instanceof UserPrincipal) {
            // convert posix UserPrincipal to external HttpPrincipal
            UserPrincipal p = (UserPrincipal) o;
            HttpPrincipal hp = new HttpPrincipal(p.getName());
            Set<Principal> pset = new HashSet<Principal>();
            pset.add(hp);
            Subject ret = new Subject(false, pset, new HashSet(), new HashSet());
            // TODO: augment subject?
            return ret;
        }
        throw new IllegalArgumentException("invalid owner type: " + o.getClass().getName());
    }

    public UserPrincipal toUserPrincipal(Subject subject) throws IOException {
        if (subject == null) {
            return null;
        }
        
        X500Principal x500Principal = null;
        Set<Principal> principals = subject.getPrincipals();
        for (Principal principal : principals) {
            if (principal instanceof HttpPrincipal) {
                return users.lookupPrincipalByName(principal.getName());
            }
        }
        return null;
    }
    
    @Override
    public Object toOwner(Subject subject) {
        if (subject == null) {
            return null;
        }
        
        X500Principal x500Principal = null;
        Set<Principal> principals = subject.getPrincipals();
        for (Principal principal : principals) {
            if (principal instanceof HttpPrincipal) {
                return principal.getName();
            }
            if (principal instanceof X500Principal) {
                x500Principal = (X500Principal) principal;
            }
        }

        if (x500Principal == null) {
            return null;
        }

        // The user has connected with a valid client cert but does not have an account:
        // create an auto-approved account with their x500Principal
        UserPrincipal up = createX500User(x500Principal);
        subject.getPrincipals().add(up);
        return up.getName();
    }

    @Override
    public int getOwnerType() {
        throw new UnsupportedOperationException();
    }

    @Override
    public String toOwnerString(Subject subject) {
        if (subject == null) {
            return null;
        }
        
        Set<Principal> principals = subject.getPrincipals();
        for (Principal principal : principals) {
            // vospace output should be X500 DN
            if (principal instanceof X500Principal) { 
                return principal.getName();
            }
        }
        
        // TODO: lazy augment?
        return null;
    }

    private UserPrincipal createX500User(X500Principal x500Principal) {
        // TODO: 
        // create a new username -- UUID.randomID()?
        // create new User with X500principal and UserPrincipal
        // wait for that user to be locally resolvable so we can return one that will work
        throw new UnsupportedOperationException("create user from unknown X500Principal");
    }
}
