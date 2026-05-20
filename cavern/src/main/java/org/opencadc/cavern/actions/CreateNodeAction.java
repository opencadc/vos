package org.opencadc.cavern.actions;

import ca.nrc.cadc.auth.AuthenticationUtil;
import ca.nrc.cadc.auth.AuthorizationToken;
import java.security.AccessControlException;
import java.security.PrivilegedActionException;
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
        if (isSelfAllocation()) {
            log.debug("Using permissions client to allocate user.");
            authorize();
            try {
                Subject.doAs(getFileSystemNodePersistence().getRootNode().owner, (PrivilegedExceptionAction<Void>) () -> {
                    super.doAction();
                    return null;
                });
            } catch (PrivilegedActionException e) {
                // Use the underlying Exception as it's the relevant one.
                if (e.getException() != null) {
                    throw e.getException();
                }
                throw e;
            }
        } else {
            // Handle normal create action.
            super.doAction();
        }
    }

    private FileSystemNodePersistence getFileSystemNodePersistence() {
        // Should never happen, but here for completeness.
        if (this.nodePersistence instanceof FileSystemNodePersistence) {
            return (FileSystemNodePersistence) this.nodePersistence;
        }

        throw new IllegalStateException("Node persistence is not an instance of FileSystemNodePersistence");
    }

    private boolean isSelfAllocation() {
        final Subject caller = AuthenticationUtil.getCurrentSubject();
        final Node inputNode = getInputNode();
        final FileSystemNodePersistence fileSystemNodePersistence = getFileSystemNodePersistence();
        final Subject owner = fileSystemNodePersistence.getRootNode().owner;
        return !owner.equals(caller)
                && inputNode instanceof ContainerNode
                && fileSystemNodePersistence.isAllocation(((ContainerNode) inputNode));
    }

    private void authorize() throws Exception {
        final Subject caller = AuthenticationUtil.getCurrentSubject();
        final FileSystemNodePersistence fileSystemNodePersistence = getFileSystemNodePersistence();
        final CavernConfig config = fileSystemNodePersistence.getConfig();
        final PermissionsClientConfig permissionsClientConfig = config.getPermissionsClientConfig();

        log.debug("permissions client config is present, validating permissions API token if present");
        if (permissionsClientConfig == null) {
            throw new IllegalStateException("Permissions client config is null");
        }

        final PermissionsAPIClient permissionsAPIClient =
                new PermissionsAPIClient(permissionsClientConfig.getPermissionsApiBaseUrl(),
                        permissionsClientConfig.getPermissionsApiAuthBaseUrl());

        final AuthorisationResult authorisationResult = permissionsAPIClient.authoriseRoute(
                permissionsClientConfig.getServiceName(),
                CreateNodeAction.getAuthorizationToken(caller).getCredentials(),
                permissionsClientConfig.getRoutePath(),
                permissionsClientConfig.getMethod(),
                new JSONObject(),
                permissionsClientConfig.getVersion());

        if (!authorisationResult.isAuthorised) {
            throw new AccessControlException(
                    "Subject is not authorized to create user allocations due to Permissions API Rules.");
        }

        log.info("CAVERN ADMIN GRANT: " + permissionsClientConfig.getServiceName());
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
