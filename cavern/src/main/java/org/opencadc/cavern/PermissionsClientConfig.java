package org.opencadc.cavern;

import ca.nrc.cadc.reg.client.LocalAuthority;
import ca.nrc.cadc.util.InvalidConfigException;
import java.net.MalformedURLException;
import java.net.URI;
import java.net.URL;
import org.apache.log4j.Logger;
import org.opencadc.permissions.client.srcnet.PAPI;

public class PermissionsClientConfig {
    private static final Logger log = Logger.getLogger(PermissionsClientConfig.class);
    
    private final URL permissionsApiBaseUrl;
    private final URL permissionsApiAuthBaseUrl;

    // Hard-coded values for now.
    private final String serviceName = "canfar-api";
    private final String authoriseType = "route";
    //private final String routePath = "/home/{username}";
    private final String version = "1";
    private final String method = "PUT";


    PermissionsClientConfig() {
        LocalAuthority loc = new LocalAuthority();
        final URI aapi = loc.getResourceID(PAPI.STD_AUTH_API, true);
        final URI papi = loc.getResourceID(PAPI.STD_PERM_API, true);
        
        if (aapi == null && papi == null) {
            this.permissionsApiAuthBaseUrl = null;
            this.permissionsApiBaseUrl = null;
            return;
        }
        
        try {
            this.permissionsApiAuthBaseUrl = aapi.toURL();
        } catch (MalformedURLException ex) {
            throw new InvalidConfigException("invalid URL: " + PAPI.STD_AUTH_API + " = " + aapi, ex);
        }
        try {
            this.permissionsApiBaseUrl = papi.toURL();
        } catch (MalformedURLException ex) {
            throw new InvalidConfigException("invalid URL: " + PAPI.STD_PERM_API + " = " + papi, ex);
        }
    }
    
    boolean isConfigured() {
        return permissionsApiAuthBaseUrl != null && permissionsApiBaseUrl != null;
    }

    public URL getPermissionsApiBaseUrl() {
        return permissionsApiBaseUrl;
    }

    public URL getPermissionsApiAuthBaseUrl() {
        return permissionsApiAuthBaseUrl;
    }
    
    public String getServiceName() {
        return serviceName;
    }

    public String getAuthoriseType() {
        return authoriseType;
    }

    //public String getRoutePath() {
    //    return routePath;
    //}

    public String getVersion() {
        return version;
    }

    public String getMethod() {
        return method;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append(PermissionsClientConfig.class.getSimpleName()).append(": ");
        sb.append("\n").append(PAPI.STD_PERM_API).append(" = ").append(permissionsApiBaseUrl);
        sb.append("\n").append(PAPI.STD_AUTH_API).append(" = ").append(permissionsApiAuthBaseUrl);
        return sb.toString();
    }
    
    
}
