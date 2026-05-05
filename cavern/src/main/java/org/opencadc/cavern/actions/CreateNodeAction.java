package org.opencadc.cavern.actions;

import ca.nrc.cadc.auth.AuthenticationUtil;
import ca.nrc.cadc.auth.AuthorizationToken;
import ca.nrc.cadc.util.InvalidConfigException;
import java.security.AccessControlException;
import java.security.PrivilegedExceptionAction;
import javax.security.auth.Subject;
import org.json.JSONObject;
import org.opencadc.cavern.CavernConfig;
import org.opencadc.cavern.PermissionsClientConfig;
import org.opencadc.cavern.nodes.FileSystemNodePersistence;
import org.opencadc.permissions.client.srcnet.AuthorisationResult;
import org.opencadc.permissions.client.srcnet.PermissionsAPIClient;

/**
 * Overridden create action to allow the use of a permissions client to access a remote service and do verification
 * that the caller has permissions to create an allocation.  Administrative access is then assumed.
 */
public class CreateNodeAction extends org.opencadc.vospace.server.actions.CreateNodeAction {
    @Override
    public void doAction() throws Exception {
        final FileSystemNodePersistence fileSystemNodePersistence;

        // Should never happen, but here for completeness.
        if (this.nodePersistence instanceof FileSystemNodePersistence) {
            fileSystemNodePersistence = (FileSystemNodePersistence) this.nodePersistence;
        } else {
            throw new IllegalStateException("Node persistence is not an instance of FileSystemNodePersistence");
        }

        final Subject validatedSubject =
                fileSystemNodePersistence.getIdentityManager().validate(AuthenticationUtil.getCurrentSubject());
        final CavernConfig config = fileSystemNodePersistence.getConfig();
        final PermissionsClientConfig permissionsClientConfig = config.getPermissionsClientConfig();
        // Permissions Client was configured.
        if (permissionsClientConfig != null) {
            log.debug("permissions client config is present, validating permissions API token if present");
            final PermissionsAPIClient permissionsAPIClient =
                    new PermissionsAPIClient(permissionsClientConfig.getPermissionsApiBaseUrl(),
                            permissionsClientConfig.getPermissionsApiAuthBaseUrl());
            final AuthorisationResult authorisationResult;
            if (permissionsClientConfig.getAuthoriseType().equalsIgnoreCase("plugin")) {
                log.debug("Authorising with plugin type");
                authorisationResult = permissionsAPIClient.authorisePlugin(
                        permissionsClientConfig.getServiceName(),
                        getAuthorizationToken(validatedSubject).getCredentials(),
                        new JSONObject(),
                        permissionsClientConfig.getVersion());
            } else if (permissionsClientConfig.getAuthoriseType().equalsIgnoreCase("route")) {
                log.debug("Authorising with route type");
                authorisationResult = permissionsAPIClient.authoriseRoute(
                        permissionsClientConfig.getServiceName(),
                        getAuthorizationToken(validatedSubject).getCredentials(),
                        permissionsClientConfig.getRoutePath(),
                        permissionsClientConfig.getMethod(),
                        new JSONObject(),
                        permissionsClientConfig.getVersion());
            } else {
                throw new InvalidConfigException("Invalid authoriseType in permissions client config: "
                        + permissionsClientConfig.getAuthoriseType());
            }

            handleAuthenticatedAction(fileSystemNodePersistence, permissionsClientConfig, authorisationResult);
        }
        super.doAction();
    }

    private void handleAuthenticatedAction(FileSystemNodePersistence fileSystemNodePersistence,
                                           PermissionsClientConfig permissionsClientConfig,
                                           AuthorisationResult authorisationResult) throws Exception {
        if (authorisationResult.isAuthorised) {
            log.info("CAVERN ADMIN GRANT: " + permissionsClientConfig.getServiceName());
            Subject.doAs(fileSystemNodePersistence.getRootNode().owner, (PrivilegedExceptionAction<Void>) () -> {
                super.doAction();
                return null;
            });
        } else {
            throw new AccessControlException(
                    "Subject is not authorized to create user allocations due to Permissions API Rules.");
        }
    }

    /**
     * Obtain the given Subject's bearer token. Null if none found.,
     *
     * @param subject The Subject to look through.
     * @return AuthorizationToken with type "Bearer", or null if none found.
     */
    public static AuthorizationToken getAuthorizationToken(final Subject subject) {
        return subject.getPublicCredentials(AuthorizationToken.class).stream()
                .filter(token -> AuthenticationUtil.CHALLENGE_TYPE_BEARER.equalsIgnoreCase(token.getType()))
                .findFirst()
                .orElse(null);
    }
}
