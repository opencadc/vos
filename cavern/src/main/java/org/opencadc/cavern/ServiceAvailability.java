/*
************************************************************************
*******************  CANADIAN ASTRONOMY DATA CENTRE  *******************
**************  CENTRE CANADIEN DE DONNÉES ASTRONOMIQUES  **************
*
*  (c) 2022.                            (c) 2022.
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
import ca.nrc.cadc.reg.client.LocalAuthority;
import ca.nrc.cadc.reg.client.RegistryClient;
import ca.nrc.cadc.vosi.Availability;
import ca.nrc.cadc.vosi.AvailabilityPlugin;
import ca.nrc.cadc.vosi.avail.CheckCertificate;
import ca.nrc.cadc.vosi.avail.CheckException;
import ca.nrc.cadc.vosi.avail.CheckResource;
import ca.nrc.cadc.vosi.avail.CheckWebService;
import java.io.File;
import java.net.URI;
import java.net.URL;
import java.security.AccessControlContext;
import java.security.AccessController;
import java.security.Principal;
import java.util.NoSuchElementException;
import javax.security.auth.Subject;
import javax.security.auth.x500.X500Principal;
import org.apache.log4j.Logger;

/**
 *
 * @author pdowler
 */
public class ServiceAvailability implements AvailabilityPlugin {

    private static final Logger log = Logger.getLogger(ServiceAvailability.class);

    private static final File AAI_PEM_FILE = new File(System.getProperty("user.home") + "/.ssl/cadcproxy.pem");
    
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
    public Availability getStatus() {
        boolean isGood = true;
        String note = "service is accepting requests";
        try {

            // File system probe
            // PropertiesReader pr = new PropertiesReader("cavern.properties");
            // MultiValuedProperties conf = pr.getAllProperties();
            // String rootPath = conf.getFirstPropertyValue("PROBE_ROOT");
            // log.debug("rootPath: " + rootPath);
            // String owner = conf.getFirstPropertyValue("PROBE_OWNER");
            // log.debug("owner: " + owner);
            // String linkTargetOwner = conf.getFirstPropertyValue("PROBE_LINKOWER");
            // log.debug("linkTargetOwner: " + linkTargetOwner);
            // File root = new File(rootPath);

            // VOSURI baseURI = new LocalServiceURI().getVOSBase();
            // if (baseURI == null) {
            //     return new Availability(false, "Missing resourceID in VOSpaceWS.properties");
            // }
            
            // FileSystemProbe fsp = new FileSystemProbe(root, baseURI.toString(), owner, linkTargetOwner, null);
            // Boolean success = fsp.call();
            // if (success == null || !success) {
            //     return new Availability(false, "File system probe failed");
            // }

            // ReadWrite: proceed with live checks
            // check filesystem status: readable, writable, space available?
            // check job persistence status: memory available, cleaner thread keeping up?

            // check other services we depend on
            RegistryClient reg = new RegistryClient();
            LocalAuthority localAuthority = new LocalAuthority();
            
            URI credURI = null;
            try {
                credURI = localAuthority.getResourceID(Standards.CRED_PROXY_10);

                // The CDP is not applicable to all deployments, so don't throw an exception
                if (credURI != null) {
                    URL url = reg.getServiceURL(credURI, Standards.VOSI_AVAILABILITY, AuthMethod.ANON);
                    if (url != null) {
                        CheckResource checkResource = new CheckWebService(url);
                        checkResource.check();
                    } else {
                        log.debug("check skipped: " + credURI + " does not provide " + Standards.VOSI_AVAILABILITY);
                    }
                } else {
                    log.debug("check skipped: " + Standards.CRED_PROXY_10 + " not applicable");
                }
            } catch (NoSuchElementException | IllegalArgumentException ex) {
                log.debug("not configured: " + Standards.CRED_PROXY_10);
            }

            URI usersURI = null;
            try {
                usersURI = localAuthority.getResourceID(Standards.UMS_USERS_10);
                URL url = reg.getServiceURL(usersURI, Standards.VOSI_AVAILABILITY, AuthMethod.ANON);
                if (url != null) {
                    CheckResource checkResource = new CheckWebService(url);
                    checkResource.check();
                } else {
                    log.debug("check skipped: " + usersURI + " does not provide " + Standards.VOSI_AVAILABILITY);
                }
            } catch (NoSuchElementException | IllegalArgumentException ex) {
                log.debug("not configured: " + Standards.UMS_USERS_10);
            }

            try {
                URI groupsURI = localAuthority.getResourceID(Standards.GMS_SEARCH_10);
                if (!groupsURI.equals(usersURI)) {
                    URL url = reg.getServiceURL(groupsURI, Standards.VOSI_AVAILABILITY, AuthMethod.ANON);
                    if (url != null) {
                        CheckResource checkResource = new CheckWebService(url);
                        checkResource.check();
                    } else {
                        log.debug("check skipped: " + groupsURI + " does not provide " + Standards.VOSI_AVAILABILITY);
                    }
                }
            } catch (NoSuchElementException | IllegalArgumentException ex) {
                log.debug("not configured: " + Standards.GMS_SEARCH_10);
            }

            URI posixUserMapURI = null;
            try {
                posixUserMapURI = localAuthority.getResourceID(Standards.POSIX_USERMAP);
                URL url = reg.getServiceURL(posixUserMapURI, Standards.VOSI_AVAILABILITY, AuthMethod.ANON);
                if (url != null) {
                    CheckResource checkResource = new CheckWebService(url);
                    checkResource.check();
                } else {
                    log.debug("check skipped: " + posixUserMapURI + " does not provide " + Standards.VOSI_AVAILABILITY);
                }
            } catch (NoSuchElementException | IllegalArgumentException ex) {
                log.debug("not configured: " + Standards.POSIX_USERMAP);
            }

            try {
                URI posixGroupMapURI = localAuthority.getResourceID(Standards.POSIX_GROUPMAP);
                if (!posixGroupMapURI.equals(posixUserMapURI)) {
                    URL url = reg.getServiceURL(posixGroupMapURI, Standards.VOSI_AVAILABILITY, AuthMethod.ANON);
                    if (url != null) {
                        CheckResource checkResource = new CheckWebService(url);
                        checkResource.check();
                    } else {
                        log.debug("check skipped: " + posixGroupMapURI + " does not provide " + Standards.VOSI_AVAILABILITY);
                    }
                }
            } catch (NoSuchElementException | IllegalArgumentException ex) {
                log.debug("not configured: " + Standards.POSIX_GROUPMAP);
            }
            
            if (credURI != null || usersURI != null) {
                if (AAI_PEM_FILE.exists() && AAI_PEM_FILE.canRead()) {
                    // check for a certificate needed to perform network A&A ops
                    CheckCertificate checkCert = new CheckCertificate(AAI_PEM_FILE);
                    checkCert.check();
                } else {
                    log.debug("AAI cert not found or unreadable");
                }
            }
            
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

        return new Availability(isGood, note);
    }

    @Override
    public void setState(String state) {
        throw new UnsupportedOperationException("TODO: re-implement using cadc-rest mechanism");
    }
}
