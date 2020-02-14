/*
************************************************************************
*******************  CANADIAN ASTRONOMY DATA CENTRE  *******************
**************  CENTRE CANADIEN DE DONNÉES ASTRONOMIQUES  **************
*
*  (c) 2019.                            (c) 2019.
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

import ca.nrc.cadc.auth.AuthMethod;
import ca.nrc.cadc.auth.AuthenticationUtil;
import ca.nrc.cadc.reg.Standards;
import ca.nrc.cadc.reg.client.RegistryClient;
import ca.nrc.cadc.util.PropertiesReader;
import ca.nrc.cadc.vos.server.auth.VOSpaceAuthorizer;
import ca.nrc.cadc.vosi.AvailabilityPlugin;
import ca.nrc.cadc.vosi.AvailabilityStatus;
import ca.nrc.cadc.vosi.avail.CheckException;
import ca.nrc.cadc.vosi.avail.CheckResource;
import ca.nrc.cadc.vosi.avail.CheckWebService;
import java.io.File;
import java.net.URI;
import java.security.AccessControlContext;
import java.security.AccessController;
import java.security.Principal;
import javax.security.auth.Subject;
import javax.security.auth.x500.X500Principal;
import org.apache.log4j.Logger;
import org.opencadc.cavern.probe.FileSystemProbe;

/**
 *
 * @author pdowler
 */
public class ServiceAvailability implements AvailabilityPlugin {

    private static final Logger log = Logger.getLogger(ServiceAvailability.class);

    private static String CRED_AVAIL = "ivo://cadc.nrc.ca/cred";
    private static String AC_AVAIL = "ivo://cadc.nrc.ca/gms";

    // identity of caller who is allowed to change service state remotely
    // TODO: this should be configured someplace
    private static final Principal TRUSTED = new X500Principal("cn=servops_4a2,ou=cadc,o=hia,c=ca");

    public ServiceAvailability() {
    }

    @Override
    public void setAppName(String string) {
        //no-op
    }

    @Override
    public boolean heartbeat() {
        return true;
    }

    @Override
    public AvailabilityStatus getStatus() {
        boolean isGood = true;
        String note = "service is accepting requests";
        try {
            String state = getState();
            if (VOSpaceAuthorizer.OFFLINE.equals(state)) {
                return new AvailabilityStatus(false, null, null, null, VOSpaceAuthorizer.OFFLINE_MSG);
            }
            if (VOSpaceAuthorizer.READ_ONLY.equals(state)) {
                return new AvailabilityStatus(false, null, null, null, VOSpaceAuthorizer.READ_ONLY_MSG);
            }

            // File system probe
            PropertiesReader pr = new PropertiesReader("Cavern.properties");
            String rootPath = pr.getFirstPropertyValue("PROBE_ROOT");
            log.debug("rootPath: " + rootPath);
            String owner = pr.getFirstPropertyValue("PROBE_OWNER");
            log.debug("owner: " + owner);
            String linkTargetOwner = pr.getFirstPropertyValue("PROBE_LINKOWER");
            log.debug("linkTargetOwner: " + linkTargetOwner);
            File root = new File(rootPath);

            FileSystemProbe fsp = new FileSystemProbe(root, owner, linkTargetOwner, null);
            Boolean success = fsp.call();
            if (success == null || !success) {
                return new AvailabilityStatus(false, null, null, null, "File system probe failed");
            }

            // ReadWrite: proceed with live checks
            // check filesystem status: readable, writable, space available?
            // check job persistence status: memory available, cleaner thread keeping up?

            // check other services we depend on
            RegistryClient reg = new RegistryClient();
            String url;
            CheckResource checkResource;

            url = reg.getServiceURL(URI.create(CRED_AVAIL), Standards.VOSI_AVAILABILITY, AuthMethod.ANON).toExternalForm();
            checkResource = new CheckWebService(url);
            checkResource.check();

            url = reg.getServiceURL(URI.create(AC_AVAIL), Standards.VOSI_AVAILABILITY, AuthMethod.ANON).toExternalForm();
            checkResource = new CheckWebService(url);
            checkResource.check();
        } catch (CheckException ce) {
            // tests determined that the resource is not working
            isGood = false;
            note = ce.getMessage();
        } catch (Throwable t) {
            // the test itself failed
            log.debug("failure", t);
            isGood = false;
            note = "test failed, reason: " + t;
        }

        return new AvailabilityStatus(isGood, null, null, null, note);
    }

    @Override
    public void setState(String state) {
        AccessControlContext acContext = AccessController.getContext();
        Subject subject = Subject.getSubject(acContext);

        if (subject == null) {
            return;
        }

        Principal caller = AuthenticationUtil.getX500Principal(subject);
        if (AuthenticationUtil.equals(TRUSTED, caller)) {
            String key = VOSpaceAuthorizer.class.getName() + ".state";
            if (VOSpaceAuthorizer.OFFLINE.equals(state)) {
                System.setProperty(key, VOSpaceAuthorizer.OFFLINE);
            } else if (VOSpaceAuthorizer.READ_ONLY.equals(state)) {
                System.setProperty(key, VOSpaceAuthorizer.READ_ONLY);
            } else if (VOSpaceAuthorizer.READ_WRITE.equals(state)) {
                System.setProperty(key, VOSpaceAuthorizer.READ_WRITE);
            }
            log.info("WebService state changed to " + state + " by " + caller + " [OK]");
        } else {
            log.warn("WebService state change to " + state + " by " + caller + " [DENIED]");
        }
    }

    private String getState() {
        String key = VOSpaceAuthorizer.MODE_KEY;
        String ret = System.getProperty(key);
        if (ret == null) {
            return VOSpaceAuthorizer.READ_WRITE;
        }
        return ret;
    }
}
