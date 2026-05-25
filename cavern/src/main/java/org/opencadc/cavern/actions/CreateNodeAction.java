package org.opencadc.cavern.actions;

import ca.nrc.cadc.auth.AuthenticationUtil;
import ca.nrc.cadc.auth.AuthorizationToken;
import ca.nrc.cadc.auth.PosixPrincipal;
import ca.nrc.cadc.cred.client.CredUtil;
import java.security.AccessControlException;
import java.util.Set;
import javax.security.auth.Subject;
import org.json.JSONObject;
import org.opencadc.cavern.CavernConfig;
import org.opencadc.cavern.PermissionsClientConfig;
import org.opencadc.cavern.nodes.FileSystemNodePersistence;
import org.opencadc.gms.GroupURI;
import org.opencadc.gms.IvoaGroupClient;
import org.opencadc.permissions.client.srcnet.AuthorisationResult;
import org.opencadc.permissions.client.srcnet.PermissionsAPIClient;
import org.opencadc.vospace.VOSURI;

/**
 * Overridden create action to allow the use of a permissions client to access a remote service and do verification
 * that the caller has permissions to create an allocation.  Administrative access is then assumed.
 */
public class CreateNodeAction extends org.opencadc.vospace.server.actions.CreateNodeAction {
    
    private FileSystemNodePersistence getFileSystemNodePersistence() {
        // Should never happen, but here for completeness.
        if (this.nodePersistence instanceof FileSystemNodePersistence) {
            return (FileSystemNodePersistence) this.nodePersistence;
        }

        throw new RuntimeException("BUG: NodePersistence is not an instance of FileSystemNodePersistence");
    }

    @Override
    protected void checkSelfAllocatePermission(Subject caller, VOSURI uri) 
            throws AccessControlException, Exception {
        final FileSystemNodePersistence fileSystemNodePersistence = getFileSystemNodePersistence();
        final CavernConfig config = fileSystemNodePersistence.getConfig();
        
        // two mechanisms to authorize self-allocate:
        // cavern condfigured with self-allocate group
        // cavern configured to use SRCNet Permissions API
        final Set<GroupURI> allowedGroups = config.getSelfAllocateGroups();
        final PermissionsClientConfig permissionsClientConfig = config.getPermissionsClientConfig();
        if (allowedGroups.isEmpty() && permissionsClientConfig == null) {
            log.debug("self-allocate mechanism not configured -- could be a misconfiguration or intentional");
            super.checkSelfAllocatePermission(caller, uri);
            return;
        }

        // HACK: hard code restriction to allow home directory only
        String path = uri.getPath();
        PosixPrincipal pp = getPosixUser(caller);
        if (pp == null || pp.username == null) {
            throw new AccessControlException("permission denied: self-allocate " + uri.getURI().toASCIIString()
                    + " reason: caller has no PosixPrincipal username");
        }
        String homeDir = "/home/" + pp.username; // TODO: get from local posix-mapper
        if (!path.equals(homeDir)) {
            throw new AccessControlException("permission denied: self-allocate " + uri.getURI().toASCIIString()
                    + " reason: target does not match expected home dir '" + homeDir + "'");
        }

        if (permissionsClientConfig != null) {
            log.debug("permissions client config is present, validating permissions API token if present");
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

            log.debug("self-allocate grant: " + permissionsClientConfig.getServiceName() + "(" + authorisationResult.isAuthorised + ")");
            if (authorisationResult.isAuthorised) {
                return;
            }
        }
        
        if (!allowedGroups.isEmpty()) {
            IvoaGroupClient gms = new IvoaGroupClient();
            CredUtil.checkCredentials(caller); // TODO: ignore return?
            Set<GroupURI> mem = gms.getMemberships(allowedGroups);
            if (!mem.isEmpty()) {
                return;
            }
        }
        
        throw new AccessControlException("permission denied: self-allocate " + uri.getURI().toASCIIString());
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
    
    private PosixPrincipal getPosixUser(Subject subject) {
        return subject.getPrincipals(PosixPrincipal.class).stream().findFirst().orElse(null);
    }
}
