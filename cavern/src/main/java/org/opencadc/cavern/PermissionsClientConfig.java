package org.opencadc.cavern;

import ca.nrc.cadc.util.MultiValuedProperties;
import ca.nrc.cadc.util.StringUtil;
import java.net.MalformedURLException;
import java.net.URI;
import java.net.URL;

public class PermissionsClientConfig {
    private static final String PREFIX = ".papi";
    private static final String PERMISSIONS_API_BASE_URL = PREFIX + ".baseUrl";
    private static final String PERMISSIONS_API_AUTH_BASE_URL = PREFIX + ".authBaseUrl";

    private final String configPrefix;

    private final String permissionsApiBaseUrl;
    private final String permissionsApiAuthBaseUrl;

    // Hard-coded values for now.
    private final String serviceName = "canfar-api";
    private final String authoriseType = "route";
    private final String routePath = "/nodes/home/{username}";
    private final String version = "1";
    private final String method = "PUT";


    /**
     * Read in the configuration from properties.  This will be NULL if no base URL is set (presumed to not be
     * configured for this Cavern instance).
     * @param properties    The MultiValuedProperties instance.
     * @param configPrefix  The cavern prefix to look in the config file.
     * @return  PermissionsClientConfig instance, or null if not configured.
     */
    static PermissionsClientConfig fromProperties(final MultiValuedProperties properties, final String configPrefix) {
        final String baseUrl = properties.getFirstPropertyValue(configPrefix + PERMISSIONS_API_BASE_URL);
        return StringUtil.hasText(baseUrl)
                ? new PermissionsClientConfig(properties, configPrefix)
                : null;
    }

    private PermissionsClientConfig(final MultiValuedProperties properties, final String configPrefix) {
        this.configPrefix = configPrefix;

        this.permissionsApiBaseUrl = properties.getFirstPropertyValue(configPrefix + PERMISSIONS_API_BASE_URL);
        this.permissionsApiAuthBaseUrl = properties.getFirstPropertyValue(configPrefix + PERMISSIONS_API_AUTH_BASE_URL);
    }

    public URL getPermissionsApiBaseUrl() {
        try {
            return URI.create(this.permissionsApiBaseUrl).toURL();
        } catch (MalformedURLException malformedURLException) {
            throw new IllegalStateException("Invalid URL for " + this.configPrefix + PERMISSIONS_API_BASE_URL + ": "
                    + this.permissionsApiBaseUrl, malformedURLException);
        }
    }

    public URL getPermissionsApiAuthBaseUrl() {
        try {
            return URI.create(this.permissionsApiAuthBaseUrl).toURL();
        } catch (MalformedURLException malformedURLException) {
            throw new IllegalStateException("Invalid URL for " + this.configPrefix + PERMISSIONS_API_AUTH_BASE_URL + ": "
                    + this.permissionsApiAuthBaseUrl, malformedURLException);
        }
    }

    public String getServiceName() {
        return serviceName;
    }

    public String getAuthoriseType() {
        return authoriseType;
    }

    public String getRoutePath() {
        return routePath;
    }

    public String getVersion() {
        return version;
    }

    public String getMethod() {
        return method;
    }
}
