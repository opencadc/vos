/*
 ************************************************************************
 *******************  CANADIAN ASTRONOMY DATA CENTRE  *******************
 **************  CENTRE CANADIEN DE DONNÉES ASTRONOMIQUES  **************
 *
 *  (c) 2023.                            (c) 2023.
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
 *  : 5 $
 *
 ************************************************************************
 */

package org.opencadc.cavern;

import ca.nrc.cadc.auth.AuthenticationUtil;
import ca.nrc.cadc.auth.HttpPrincipal;
import ca.nrc.cadc.auth.IdentityManager;
import ca.nrc.cadc.auth.PosixPrincipal;
import ca.nrc.cadc.util.InvalidConfigException;
import ca.nrc.cadc.util.MultiValuedProperties;
import ca.nrc.cadc.util.PropertiesReader;
import java.io.File;
import java.net.URI;
import java.nio.file.Path;
import java.nio.file.Paths;
import javax.security.auth.Subject;
import org.apache.log4j.Logger;

public class CavernConfig {

    private static final Logger log = Logger.getLogger(CavernConfig.class);

    public static final String DEFAULT_CONFIG_DIR = System.getProperty("user.home") + "/config/";
    public static final String CAVERN_PROPERTIES = "cavern.properties";
    private static final String CAVERN_KEY = CavernConfig.class.getPackage().getName();
    public static final String RESOURCE_ID = CAVERN_KEY + ".resourceID";
    public static final String FILESYSTEM_BASE_DIR = CAVERN_KEY + ".filesystem.baseDir";
    public static final String FILESYSTEM_SUB_PATH = CAVERN_KEY + ".filesystem.subPath";
    public static final String ROOT_OWNER = CAVERN_KEY + ".filesystem.rootOwner";
    public static final String SSHFS_SERVER_BASE = CAVERN_KEY + ".sshfs.serverBase";
    
    // HACK (temporary?): provide complete matching PosixPrincipal for root owner
    public static final String ROOT_OWNER_USERNAME = CAVERN_KEY + ".filesystem.rootOwner.username";
    public static final String ROOT_OWNER_UID = CAVERN_KEY + ".filesystem.rootOwner.uid";
    public static final String ROOT_OWNER_GID = CAVERN_KEY + ".filesystem.rootOwner.gid";
    
    private final URI resourceID;
    private final Path root;
    private final Path secrets;
    
    // generated and assigned by CavernInitAction
    File privateKey;
    File publicKey;
    
    private final MultiValuedProperties mvp;

    public CavernConfig() {
        PropertiesReader propertiesReader = new PropertiesReader(CAVERN_PROPERTIES);
        this.mvp = propertiesReader.getAllProperties();
        if (mvp.isEmpty()) {
            throw new IllegalStateException("CONFIG: file not found or no properties found in file - "
                    + CAVERN_PROPERTIES);
        }

        StringBuilder sb = new StringBuilder();
        sb.append("CONFIG: incomplete/invalid: ");

        boolean resourceProp = checkProperty(mvp, sb, RESOURCE_ID, true);
        boolean baseDirProp = checkProperty(mvp, sb, FILESYSTEM_BASE_DIR, true);
        boolean subPathProp = checkProperty(mvp, sb, FILESYSTEM_SUB_PATH, true);
        boolean rootOwnerProp = checkProperty(mvp, sb, ROOT_OWNER, true);
        
        //boolean privateKeyProp = checkProperty(mvp, sb, PRIVATE_KEY, false);
        //boolean publicKeyProp = checkProperty(mvp, sb, PUBLIC_KEY, false);
        boolean sshfsServerBaseProp = checkProperty(mvp, sb, SSHFS_SERVER_BASE, false);

        if (!resourceProp || !baseDirProp || !subPathProp || !rootOwnerProp) {
            throw new IllegalStateException(sb.toString());
        }

        String s = mvp.getFirstPropertyValue(RESOURCE_ID);
        this.resourceID = URI.create(s);
        
        String baseDir = mvp.getFirstPropertyValue(CavernConfig.FILESYSTEM_BASE_DIR);
        String subPath = mvp.getFirstPropertyValue(CavernConfig.FILESYSTEM_SUB_PATH);
        String sep = "/";
        if (baseDir.endsWith("/") || subPath.startsWith("/")) {
            sep = "";
        }
        this.root = Paths.get(baseDir + sep + subPath);
        
        sep = "/";
        if (baseDir.endsWith("/")) {
            sep = "";
        }
        this.secrets = Paths.get(baseDir + sep + "secrets");
    }

    public URI getResourceID() {
        return resourceID;
    }
    
    public Path getRoot() {
        return root;
    }
    
    public Path getSecrets() {
        return secrets;
    }

    public File getPrivateKey() {
        return privateKey;
    }

    public File getPublicKey() {
        return publicKey;
    }
    
    public Subject getRootOwner() {
        final String owner = mvp.getFirstPropertyValue(ROOT_OWNER);
        if (owner == null) {
            throw new InvalidConfigException(CavernConfig.ROOT_OWNER + " cannot be null");
        }
        Subject ret = new Subject();
        ret.getPrincipals().add(new HttpPrincipal(owner));
        
        String username = mvp.getFirstPropertyValue(ROOT_OWNER_USERNAME);
        String uidStr = mvp.getFirstPropertyValue(ROOT_OWNER_UID);
        String gidStr = mvp.getFirstPropertyValue(ROOT_OWNER_GID);
        if (username != null && uidStr != null && gidStr != null) {
            int uid = Integer.parseInt(uidStr);
            int gid = Integer.parseInt(gidStr);
            PosixPrincipal pp = new PosixPrincipal(uid);
            pp.defaultGroup = gid;
            pp.username = username;
            ret.getPrincipals().add(pp);
        } else {
            IdentityManager im = AuthenticationUtil.getIdentityManager();
            ret = im.augment(ret);
        }
        return ret;
    }
    
    // for non-mandatory prop use
    public MultiValuedProperties getProperties() {
        return mvp;
    }

    private boolean checkProperty(MultiValuedProperties properties, StringBuilder sb,
                                         String key, boolean required) {
        boolean ok = true;
        String value = properties.getFirstPropertyValue(key);
        if (value == null && !required) {
            return false;
        }
        sb.append("\n\t").append(key).append(" - ");
        if (value == null) {
            sb.append("MISSING");
            ok = false;
        } else {
            sb.append("OK");
        }
        sb.append("\n");
        return ok;
    }

}
