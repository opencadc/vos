package ca.nrc.cadc.vos.server.transfers;

import java.net.MalformedURLException;
import java.net.URI;
import java.net.URL;
import java.security.AccessControlContext;
import java.security.AccessController;
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
        else
        {
            log.debug("unknown protocol: " + protocol);
        }

        return scheme;
    }

    /**
     * Determine the HTTP operation specified in a protocol string.
     * @param protocol One of VOS.PROTOCOL_HTTP_GET, VOS.PROTOCOL_HTTP_PUT
     * @return "get" for HTTP_GET and "put" for HTTP_PUT
     */
    public static String getOperation(final String protocol)
    {
        String op = null;

        if (protocol.equals(VOS.PROTOCOL_HTTP_GET) ||
                protocol.equals(VOS.PROTOCOL_HTTPS_GET))
        {
            op = "get";
        }
        else if (protocol.equals(VOS.PROTOCOL_HTTP_PUT) ||
                protocol.equals(VOS.PROTOCOL_HTTPS_PUT))
        {
            op = "put";
        }
        else
        {
            log.debug("unknown protocol: " + protocol);
        }

        return op;
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
        List<Protocol> protocolList = new ArrayList<Protocol>();
        List<Protocol> httpProtocolList = new ArrayList<Protocol>();
        List<Protocol> httpsProtocolList = new ArrayList<Protocol>();

        VOSpacePluginFactory storageFactory = new VOSpacePluginFactory();
        TransferGenerator transferGenerator = storageFactory.createTransferGenerator();

        VOSURI target = new VOSURI(transfer.getTarget());
        View view = createView(transfer, additionalParameters);

        if (transfer.getContentLength() != null)
        {
            additionalParameters.add(new Parameter(VOS.PROPERTY_URI_CONTENTLENGTH, transfer.getContentLength().toString()));
        }

        for (Protocol protocol: transfer.getProtocols())
        {
            String scheme = getScheme(protocol.getUri());
            if ( scheme != null)
            {
                List<Protocol> pList;

                List<URL> urlList = transferGenerator.getURLs(target, protocol, view, job, additionalParameters);

                if (scheme.equalsIgnoreCase("http"))
                {
                    pList = httpProtocolList;
                }
                else
                {
                    pList = httpsProtocolList;
                }

                for (URL url: urlList)
                {
                    Protocol np = new Protocol(protocol.getUri(), url.toString(), null);
                    np.setSecurityMethod(protocol.getSecurityMethod());
                    pList.add(np);
                }
            }
        }
        // give http URL's higher priority
        protocolList.addAll(httpProtocolList);
        protocolList.addAll(httpsProtocolList);

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
    public static boolean isSupported(Protocol p)
    {
        if (VOS.PROTOCOL_HTTP_GET.equals(p.getUri()) || VOS.PROTOCOL_HTTP_PUT.equals(p.getUri()))
        {
            if (p.getSecurityMethod() == null) // anon only
                return true;
        }
        else if (VOS.PROTOCOL_HTTPS_GET.equals(p.getUri()) || VOS.PROTOCOL_HTTPS_PUT.equals(p.getUri()))
        {
            if (Standards.getSecurityMethod(AuthMethod.CERT).equals(p.getSecurityMethod()))
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


}
