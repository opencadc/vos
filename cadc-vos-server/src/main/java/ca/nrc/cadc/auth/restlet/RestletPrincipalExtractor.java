/*
************************************************************************
*******************  CANADIAN ASTRONOMY DATA CENTRE  *******************
**************  CENTRE CANADIEN DE DONNÉES ASTRONOMIQUES  **************
*
*  (c) 2018.                            (c) 2018.
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
*  $Revision: 5 $
*
************************************************************************
 */

package ca.nrc.cadc.auth.restlet;

import ca.nrc.cadc.auth.AuthenticationUtil;
import ca.nrc.cadc.auth.AuthorizationTokenPrincipal;
import ca.nrc.cadc.auth.BearerTokenPrincipal;
import ca.nrc.cadc.auth.CookiePrincipal;
import ca.nrc.cadc.auth.SignedToken;
import ca.nrc.cadc.auth.HttpPrincipal;
import ca.nrc.cadc.auth.InvalidSignedTokenException;
import ca.nrc.cadc.auth.NotAuthenticatedException;
import ca.nrc.cadc.auth.PrincipalExtractor;
import ca.nrc.cadc.auth.SSLUtil;
import ca.nrc.cadc.auth.SSOCookieCredential;
import ca.nrc.cadc.auth.SSOCookieManager;
import ca.nrc.cadc.auth.X509CertificateChain;
import ca.nrc.cadc.net.NetUtil;
import ca.nrc.cadc.util.ArrayUtil;
import ca.nrc.cadc.util.StringUtil;
import java.io.IOException;
import java.security.AccessControlException;
import java.security.Principal;
import java.security.cert.X509Certificate;
import java.util.Arrays;
import java.util.Collection;
import java.util.Enumeration;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import org.apache.log4j.Logger;
import org.restlet.Request;
import org.restlet.data.Cookie;
import org.restlet.data.Form;
import org.restlet.util.Series;

/**
 * Principal Extractor implementation using a Restlet Request.
 */
public class RestletPrincipalExtractor implements PrincipalExtractor {

    private static final Logger log = Logger.getLogger(RestletPrincipalExtractor.class);

    private final Request request;
    private X509CertificateChain chain;
    private Set<Principal> principals = new HashSet<>();

    /**
     * Hidden no-arg constructor for testing.
     */
    RestletPrincipalExtractor() {
        this.request = null;
    }

    /**
     * Create this extractor from the given Restlet Request.
     *
     * @param req The Restlet Request.
     */
    public RestletPrincipalExtractor(final Request req) {
        this.request = req;
    }

    private void init() {

        if (chain == null) {
            final Collection<X509Certificate> requestCertificates
                = (Collection<X509Certificate>) getRequest().getAttributes().get(
                    "org.restlet.https.clientCertificates");
            if ((requestCertificates != null) && (!requestCertificates.isEmpty())) {
                this.chain = new X509CertificateChain(requestCertificates);
                principals.add(this.chain.getPrincipal());
            }
        }
        
        log.debug("Value of CERT_HEADER_ENABLE sys prop: " + System.getProperty(CERT_HEADER_ENABLE));        
        if (chain == null && "true".equals(System.getProperty(CERT_HEADER_ENABLE))) {
            Form allHeaders = (Form) getRequest().getAttributes().get("org.restlet.http.headers");
            String certString = allHeaders.getFirstValue(CERT_HEADER_FIELD, true);
            log.debug(CERT_HEADER_FIELD + ":\n" + certString + "\n");
            if (certString != null && certString.length() > 0) {
                try {
                    byte[] certBytes = SSLUtil.getCertificates(certString.getBytes());
                    chain = new X509CertificateChain(SSLUtil.readCertificateChain(certBytes), null);
                    principals.add(chain.getPrincipal());
                } catch (Exception e) {
                    log.error("Failed to read certificate", e);
                    throw new AccessControlException("Failed to read certificate: " + e.getMessage());
                }
            }
        }

        Form headers = (Form) getRequest().getAttributes().get("org.restlet.http.headers");

        // custom header (deprecated)
        String cadcTokenHeader = headers.getFirstValue(AuthenticationUtil.AUTH_HEADER);
        if (cadcTokenHeader != null) {
            AuthorizationTokenPrincipal principal = new AuthorizationTokenPrincipal(AuthenticationUtil.AUTH_HEADER, cadcTokenHeader);
            principals.add(principal);
        }

        // authorization header
        String[] authTokens = headers.getValuesArray(AuthenticationUtil.AUTHORIZATION_HEADER);
        for (String authToken : authTokens) {
            if (BearerTokenPrincipal.isBearerToken(authToken)) {
                // deprecated in favour of the common token handling mechanism below
                BearerTokenPrincipal bearerTokenPrincipal = new BearerTokenPrincipal(authToken);
                principals.add(bearerTokenPrincipal);
            } else {
                AuthorizationTokenPrincipal principal = new AuthorizationTokenPrincipal(AuthenticationUtil.AUTHORIZATION_HEADER, authToken);
                principals.add(principal);
            }
        }

        // add HttpPrincipal
        final String httpUser = getAuthenticatedUsername();
        if (StringUtil.hasText(httpUser)) // user from HTTP AUTH
        {
            principals.add(new HttpPrincipal(httpUser));
        } 

        Series<Cookie> cookies = getRequest().getCookies();
        log.debug("cookie count: " + cookies.size());
        log.debug("principal count: " + principals.size());
        log.debug(principals);
        if (cookies == null || (cookies.size() == 0)) {
            return;
        }

        for (Cookie ssoCookie : cookies) {
            log.debug(ssoCookie.toString());

            if (SSOCookieManager.DEFAULT_SSO_COOKIE_NAME.equals(
                    ssoCookie.getName())
                    && StringUtil.hasText(ssoCookie.getValue())) {
                
                CookiePrincipal cookiePrincipal = new CookiePrincipal(ssoCookie.getName(), ssoCookie.getValue());
                principals.add(cookiePrincipal);
            }
        }
    }

    public X509CertificateChain getCertificateChain() {
        init();
        return chain;
    }

    public Set<Principal> getPrincipals() {
        init();
        return principals;
    }

    /**
     * Obtain the Username submitted with the Request.
     *
     * @return String username, or null if none found.
     */
    protected String getAuthenticatedUsername() {
        final String username;

        if (!getRequest().getClientInfo().getPrincipals().isEmpty()) {
            // Put in to support Safari not injecting a Challenge Response.
            // Grab the first principal's name as the username.
            // update: this is *always* right and works with realms; the previous
            // call to getRequest().getChallengeResponse().getIdentifier() would
            // return whatever username the caller provided in a non-authenticating call
            username = getRequest().getClientInfo().getPrincipals().get(0).getName();
            log.debug("username: " + username);
        } else {
            username = null;
        }

        return username;
    }

    public Request getRequest() {
        return request;
    }
}
