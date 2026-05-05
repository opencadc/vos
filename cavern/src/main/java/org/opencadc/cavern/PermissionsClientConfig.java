package org.opencadc.cavern;

import ca.nrc.cadc.util.MultiValuedProperties;
import ca.nrc.cadc.util.StringUtil;

import java.net.MalformedURLException;
import java.net.URI;
import java.net.URL;

public class PermissionsClientConfig {
    private static final String PREFIX = ".permissions";
    private static final String PERMISSIONS_API_BASE_URL = PREFIX + ".baseUrl";
    private static final String PERMISSIONS_API_AUTH_BASE_URL = PREFIX + ".authBaseUrl";
    private static final String PERMISSIONS_API_SERVICE_NAME = PREFIX + ".serviceName";
    private static final String PERMISSIONS_API_AUTHORISE_TYPE = PREFIX + ".authoriseType";
    private static final String PERMISSIONS_API_ROUTE = PREFIX + ".route";
    private static final String PERMISSIONS_API_VERSION = PREFIX + ".version";
    private static final String PERMISSIONS_API_METHOD = PREFIX + ".method";

    private final String configPrefix;

    private final String permissionsApiBaseUrl;
    private final String permissionsApiAuthBaseUrl;
    private final String serviceName;
    private final String authoriseType;
    private final String routePath;
    private final String version;
    private final String method;

    /**
     * Read in the configuration from properties.  This will be NULL if no base URL is set (presumed to not be
     * configured for this Cavern instance).
     * @param properties    The MultiValuedProperties instance.
     * @param configPrefix  The cavern prefix to look in the config file.
     * @return  PermissionsClientConfig instance, or null if not configured.
     */
    public static PermissionsClientConfig fromProperties(final MultiValuedProperties properties, final String configPrefix) {
        final String baseUrl = properties.getFirstPropertyValue(configPrefix + PERMISSIONS_API_BASE_URL);
        return StringUtil.hasText(baseUrl)
                ? new PermissionsClientConfig(properties, configPrefix)
                : null;
    }

    private PermissionsClientConfig(final MultiValuedProperties properties, final String configPrefix) {
        this.configPrefix = configPrefix;

        this.permissionsApiBaseUrl = properties.getFirstPropertyValue(configPrefix + PERMISSIONS_API_BASE_URL);
        this.permissionsApiAuthBaseUrl = properties.getFirstPropertyValue(configPrefix + PERMISSIONS_API_AUTH_BASE_URL);
        this.serviceName = properties.getFirstPropertyValue(configPrefix + PERMISSIONS_API_SERVICE_NAME);
        this.authoriseType = properties.getFirstPropertyValue(configPrefix + PERMISSIONS_API_AUTHORISE_TYPE);
        this.routePath = properties.getFirstPropertyValue(configPrefix + PERMISSIONS_API_ROUTE);
        this.version = properties.getFirstPropertyValue(configPrefix + PERMISSIONS_API_VERSION);
        this.method = properties.getFirstPropertyValue(configPrefix + PERMISSIONS_API_METHOD);
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
