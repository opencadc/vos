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
*  $Revision: 4 $
*
************************************************************************
*/

package ca.nrc.cadc.vos.server.transfers;

import java.net.MalformedURLException;
import java.net.URI;
import java.net.URL;
import java.security.AccessControlContext;
import java.security.AccessController;
import java.security.InvalidParameterException;
import java.util.ArrayList;
import java.util.List;

import javax.security.auth.Subject;

import org.apache.log4j.Logger;

import ca.nrc.cadc.auth.AuthMethod;
import ca.nrc.cadc.auth.AuthenticationUtil;
import ca.nrc.cadc.net.NetUtil;
import ca.nrc.cadc.reg.Standards;
import ca.nrc.cadc.reg.client.RegistryClient;
import ca.nrc.cadc.uws.Job;
import ca.nrc.cadc.uws.Parameter;
import ca.nrc.cadc.vos.Direction;
import ca.nrc.cadc.vos.Protocol;
import ca.nrc.cadc.vos.Transfer;
import ca.nrc.cadc.vos.VOS;
import ca.nrc.cadc.vos.VOSURI;
import ca.nrc.cadc.vos.View;
import ca.nrc.cadc.vos.server.DataView;
import ca.nrc.cadc.vos.server.LocalServiceURI;
import ca.nrc.cadc.vos.server.VOSpacePluginFactory;
import ca.nrc.cadc.vos.server.Views;

public class TransferUtil
{
    private static Logger log = Logger.getLogger(TransferUtil.class);

    public static final URI VOSPACE_RESOURCE_ID;
    static
    {
        LocalServiceURI serviceURI = new LocalServiceURI();
        VOSPACE_RESOURCE_ID = serviceURI.getURI();
    }

    public static final String ANONYMOUS_USER = "anonUser";

    // name of the resolved path to the data node in the job results
    public static final String DATA_NODE = "dataNode";
    public static final String LINK_NODE = "linkNode";

    /**
     *
     * @param protocol
     * @return
     */
    public static String getScheme(final String protocol)
    {
        String scheme = null;

        if (protocol.equals(VOS.PROTOCOL_HTTP_GET) ||
                protocol.equals(VOS.PROTOCOL_HTTP_PUT))
        {
            scheme = "http";
        }
        else if (protocol.equals(VOS.PROTOCOL_HTTPS_GET) ||
                protocol.equals(VOS.PROTOCOL_HTTPS_PUT))
        {
            scheme = "https";
        }
        else if (protocol.equals(VOS.PROTOCOL_SSHFS)) {
            scheme = "sshfs";
        }
        else
        {
            log.debug("unknown protocol: " + protocol);
        }

        return scheme;
    }

    public static URL getSynctransParamURL(String scheme, VOSURI uri)
    {
        return getSynctransParamURL(scheme, uri, null, null);
    }

    public static URL getSynctransParamURL(String scheme, VOSURI uri, AuthMethod forceAuthMethod, RegistryClient reg)
    {
        if (reg == null)
            reg = new RegistryClient();
        try
        {
            AccessControlContext acContext = AccessController.getContext();
            Subject subject = Subject.getSubject(acContext);
            AuthMethod am = forceAuthMethod;
            if (am == null)
                am = AuthenticationUtil.getAuthMethod(subject); // default: perserve
            if (am == null)
                am = AuthMethod.ANON;

            log.debug("getSynctransParamURL: " + scheme + " " + am + " " + uri);
            StringBuilder sb = new StringBuilder();
            Protocol protocol = null;
            if ("http".equalsIgnoreCase(scheme))
            {
                protocol = new Protocol(VOS.PROTOCOL_HTTP_GET);
            }
            else if ("https".equalsIgnoreCase(scheme))
            {
                protocol = new Protocol(VOS.PROTOCOL_HTTPS_GET);
            }
            else
            {
                throw new IllegalArgumentException("Unknown protocol: " + scheme);
            }

            sb.append("?");
            // add parameters for synctrans
            sb.append("TARGET=").append(NetUtil.encode(uri.toString()));
            sb.append("&DIRECTION=").append(NetUtil.encode(Direction.pullFromVoSpaceValue));
            sb.append("&PROTOCOL=").append(NetUtil.encode(protocol.getUri()));

            URL serviceURL = reg.getServiceURL(VOSPACE_RESOURCE_ID, Standards.VOSPACE_SYNC_21, am);
            URL url = new URL(serviceURL.toExternalForm() + sb.toString());

            log.debug("DataView URL: " + am + " : " + url);
            return url;
        }
        catch (MalformedURLException e)
        {
            String message = "BUG: misconfigured service URL";
            log.error(message, e);
            throw new IllegalStateException(message, e);
        }
    }



    public static List<Protocol> getTransferEndpoints(final Transfer transfer, final Job job, List<Parameter> additionalParameters)
            throws Exception
    {
        VOSpacePluginFactory storageFactory = new VOSpacePluginFactory();
        TransferGenerator transferGenerator = storageFactory.createTransferGenerator();

        // CADC-10640: For now, only 1 target is supported for all vospace functions.
        // When zip & tar views are supported, there will be more than one, and this code
        // section will have to adjust. For now, it will check there is something in the
        // list and use the first entry.
        if (transfer.getTargets().isEmpty()) {
            throw new IllegalArgumentException(("No targets found in transfer."));
        }
        VOSURI target = new VOSURI(transfer.getTargets().get(0));
        View view = createView(transfer, additionalParameters);

        if (transfer.getContentLength() != null)
        {
            additionalParameters.add(new Parameter(VOS.PROPERTY_URI_CONTENTLENGTH, transfer.getContentLength().toString()));
        }

        List<Protocol> protocolList = transferGenerator.getEndpoints(target, transfer, view, job, additionalParameters);
        log.debug(transferGenerator.getClass().getSimpleName() + " generated: " + protocolList.size());
        return protocolList;
    }

    /**
     * Generate a Protocol list with endpoints set to /vault/pkg/<jobid>. One entry for every protocol in
     * the transfer provided. Use the jobID from the job provided.
     * @param transfer
     * @param job
     * @return
     * @throws MalformedURLException
     * @throws MalformedURLException
     */
    public static List<Protocol> getPackageEndpoints(final Transfer transfer, final Job job)
        throws MalformedURLException, IllegalArgumentException {

        // package view is redirected to /vault/pkg/<jobid>
        // making an endpoint for each protocol provided

        List<Protocol> protocolList = transfer.getProtocols();

        if (!protocolList.isEmpty()) {

            StringBuilder sb = new StringBuilder();
            String jobID = job.getID();
            sb.append("/").append(jobID);

            RegistryClient regClient = new RegistryClient();
            LocalServiceURI localServiceURI = new LocalServiceURI();
            URI serviceURI = localServiceURI.getURI();
            AuthMethod authMethod = AuthenticationUtil.getAuthMethod(AuthenticationUtil.getCurrentSubject());
            URL serviceURL = regClient.getServiceURL(serviceURI, Standards.PKG_10, authMethod);
            if (serviceURL == null) {
                throw new RuntimeException("Couldn't get serviceURL for URI: " + serviceURI.toString());
            }
            URL location = new URL(serviceURL.toExternalForm() + sb.toString());
            String loc = location.toExternalForm();
            log.debug("Package endpoint: " + loc);

            for (Protocol p : protocolList) {
                p.setEndpoint(loc);
            }
        }

        // return the list even if it is empty
        return protocolList;
    }


    private static View createView(Transfer transfer, List<Parameter> additionalParameters) throws Exception
    {
        // create the appropriate view object
        View view = null;
        if (transfer.getView() != null)
        {
            Views views = new Views();
            view = views.getView(transfer.getView().getURI());
            if (view != null)
                view.setParameters(transfer.getView().getParameters());
        }
        else if (additionalParameters != null)
        {
            // check for a view parameter
            for (Parameter param : additionalParameters)
            {
                if (param.getName().equalsIgnoreCase("view"))
                {
                    Views views = new Views();
                    view = views.getView(param.getValue());
                }
            }
        }

        if (view == null)
        {
            // default view is the data view
            view = new DataView();
        }

        return view;
    }

    // check for supported combinations of protocol + securityMethod
    @Deprecated
    public static boolean isSupported(Protocol p)
    {
        if (VOS.PROTOCOL_HTTP_GET.equals(p.getUri()) || VOS.PROTOCOL_HTTP_PUT.equals(p.getUri()))
        {
            if (p.getSecurityMethod() == null) // anon only
                return true;
        }
        else if (VOS.PROTOCOL_HTTPS_GET.equals(p.getUri()) || VOS.PROTOCOL_HTTPS_PUT.equals(p.getUri()))
        {
            if (Standards.SECURITY_METHOD_CERT.equals(p.getSecurityMethod()))
                return true;
        }
        return false;
    }

    public static String getNamespace(final VOSURI vosURI)
    {
        String path = vosURI.getPath();
        String fileName = vosURI.getName();
        String ns = path.substring(0, path.length() - fileName.length());
        if (ns.startsWith("/") && ns.length() > 1)
            ns = ns.substring(1);
        if (ns.endsWith("/") && ns.length() > 1)
            ns = ns.substring(0, ns.length() - 1);
        return ns;
    }

    /**
     * Check target list size in transfer provided.
     * @param transfer
     * @throws TransferException
     */
    public static void confirmSingleTarget(Transfer transfer) throws TransferException {
        int targetListSize = transfer.getTargets().size();
        if (targetListSize > 1) {
            throw new TransferException("TooManyTargets (" + targetListSize + ")");
        }

        if (targetListSize == 0) {
            throw new TransferException("NoTargetsFound");
        }

    }

    public static boolean isPackageTransfer(Transfer transfer) {
        boolean isPackageRequest = false;
        if (transfer.getView() != null) {
            if (Standards.PKG_10.equals(transfer.getView().getURI())) {
               isPackageRequest = true;
            }
        }
        return isPackageRequest;
    }

}
