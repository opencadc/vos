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
import org.opencadc.vospace.ContainerNode;
import org.opencadc.vospace.Node;

/**
 * Overridden create action to allow the use of a permissions client to access a remote service and do verification
 * that the caller has permissions to create an allocation.  Administrative access is then assumed.
 */
public class CreateNodeAction extends org.opencadc.vospace.server.actions.CreateNodeAction {
    @Override
    public void doAction() throws Exception {
            final FileSystemNodePersistence fileSystemNodePersistence = getFileSystemNodePersistence();
            final Subject validatedSubject = validateCurrentSubject(fileSystemNodePersistence);
            final CavernConfig config = fileSystemNodePersistence.getConfig();
            final PermissionsClientConfig permissionsClientConfig = config.getPermissionsClientConfig();
            final Node inputNode = getInputNode();

            if (shouldUsePermissionsClient(permissionsClientConfig, inputNode, fileSystemNodePersistence)) {
                log.debug("Using permissions client to allocate user.");
                final AuthorisationResult authorisationResult =
                        authoriseAllocation(validatedSubject, permissionsClientConfig);
                handleAuthenticatedAction(fileSystemNodePersistence, permissionsClientConfig, authorisationResult);
            }

            super.doAction();
    }

        private FileSystemNodePersistence getFileSystemNodePersistence() {
            // Should never happen, but here for completeness.
            if (this.nodePersistence instanceof FileSystemNodePersistence) {
                return (FileSystemNodePersistence) this.nodePersistence;
            }

            throw new IllegalStateException("Node persistence is not an instance of FileSystemNodePersistence");
        }

        private Subject validateCurrentSubject(final FileSystemNodePersistence fileSystemNodePersistence)
                throws InvalidConfigException {
            return fileSystemNodePersistence.getIdentityManager().validate(AuthenticationUtil.getCurrentSubject());
        }

        private boolean shouldUsePermissionsClient(final PermissionsClientConfig permissionsClientConfig,
                                                   final Node inputNode,
                                                   final FileSystemNodePersistence fileSystemNodePersistence) {
            return permissionsClientConfig != null
                    && inputNode instanceof ContainerNode
                    && fileSystemNodePersistence.isAllocation(((ContainerNode) inputNode));
        }

        private AuthorisationResult authoriseAllocation(final Subject validatedSubject,
                                                        final PermissionsClientConfig permissionsClientConfig)
                throws Exception {
            log.debug("permissions client config is present, validating permissions API token if present");

            final PermissionsAPIClient permissionsAPIClient =
                    new PermissionsAPIClient(permissionsClientConfig.getPermissionsApiBaseUrl(),
                            permissionsClientConfig.getPermissionsApiAuthBaseUrl());

            return permissionsAPIClient.authoriseRoute(
                    permissionsClientConfig.getServiceName(),
                    getAuthorizationToken(validatedSubject).getCredentials(),
                    permissionsClientConfig.getRoutePath(),
                    permissionsClientConfig.getMethod(),
                    new JSONObject(),
                    permissionsClientConfig.getVersion());
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
