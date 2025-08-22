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
************************************************************************
*/

package org.opencadc.cavern;

import ca.nrc.cadc.auth.AuthenticationUtil;
import ca.nrc.cadc.auth.IdentityManager;
import ca.nrc.cadc.db.DBUtil;
import ca.nrc.cadc.rest.InitAction;
import ca.nrc.cadc.util.InvalidConfigException;
import ca.nrc.cadc.util.RsaSignatureGenerator;
import ca.nrc.cadc.uws.server.impl.InitDatabaseUWS;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.Path;
import java.nio.file.attribute.PosixFilePermission;
import java.nio.file.attribute.PosixFilePermissions;
import java.util.HashSet;
import java.util.Set;
import javax.naming.Context;
import javax.naming.InitialContext;
import javax.naming.NamingException;
import javax.sql.DataSource;
import org.apache.log4j.Logger;
import org.opencadc.cavern.nodes.FileSystemNodePersistence;
import org.opencadc.cavern.nodes.PosixIdentityManager;
import org.opencadc.vospace.server.NodePersistence;

/**
 * Based on similar files from storage-inventory (luskan, by example)
 * @author jeevesh
 */
public class CavernInitAction extends InitAction {
    private static final Logger log = Logger.getLogger(CavernInitAction.class);

    private String jndiNodePersistence;
    
    public CavernInitAction() {
    }

    @Override
    public void doInit() {
        initNodePersistence();
        initIdentityManager();
        initDatabase();
    }
    
    private void initDatabase() {
        try {
            log.info("InitDatabaseUWS: START");
            DataSource uws = DBUtil.findJNDIDataSource("jdbc/uws");
            InitDatabaseUWS uwsi = new InitDatabaseUWS(uws, null, "uws");
            uwsi.doInit();
            log.info("InitDatabaseUWS: OK");
        }  catch (Exception ex) {
            throw new RuntimeException("INIT FAIL", ex);
        }
    }
    
    private void initNodePersistence() {
        this.jndiNodePersistence = appName + "-" + NodePersistence.class.getName();
        try {
            Context ctx = new InitialContext();
            try {
                ctx.unbind(jndiNodePersistence);
            } catch (NamingException ignore) {
                log.debug("unbind previous JNDI key (" + jndiNodePersistence + ") failed... ignoring");
            }
            FileSystemNodePersistence npi = new FileSystemNodePersistence();
            ctx.bind(jndiNodePersistence, npi);

            log.info("created JNDI key: " + jndiNodePersistence + " impl: " + npi.getClass().getName());
            
            initSecrets(npi.getConfig());
        } catch (NamingException ex) {
            log.error("Failed to create JNDI Key " + jndiNodePersistence, ex);
        }
    }

    private void initIdentityManager() {
        final String configuredIdentityManagerClassName = System.getProperty(IdentityManager.class.getName());
        if (configuredIdentityManagerClassName == null) {
            throw new InvalidConfigException("No existing IdentityManager found.  Ensure that the " + IdentityManager.class.getName()
                                                 + " System Property is set to a valid implementation.");
        }

        // Assuming there isn't more than one Cavern deployed within a JVM.
        PosixIdentityManager.JNDI_NODE_PERSISTENCE_PROPERTY = this.jndiNodePersistence;

        // To be used by the PosixIdentityManager to wrap the existing IdentityManager.
        System.setProperty(PosixIdentityManager.WRAPPED_IDENTITY_MANAGER_CLASS_PROPERTY, configuredIdentityManagerClassName);

        // Override the existing IdentityManager.
        System.setProperty(IdentityManager.class.getName(), PosixIdentityManager.class.getName());

        log.info(IdentityManager.class.getName() + " = " + PosixIdentityManager.class.getName()
                + " delegating to " + configuredIdentityManagerClassName);
    }
    
    // generate key pair for preauth URL generation
    private void initSecrets(CavernConfig conf) {
        Path secrets = conf.getSecrets();
        try {
            if (!Files.exists(secrets, LinkOption.NOFOLLOW_LINKS)) {
                Set<PosixFilePermission> perms = new HashSet<>();
                perms.add(PosixFilePermission.OWNER_READ);
                perms.add(PosixFilePermission.OWNER_WRITE);
                perms.add(PosixFilePermission.OWNER_EXECUTE);
                Files.createDirectories(secrets, PosixFilePermissions.asFileAttribute(perms)); // private: 700
            }
        } catch (IOException ex) {
            throw new RuntimeException("INIT secrets FAIL", ex);
        }
        
        try {
            File sdir = secrets.toFile();
            File pubKey = new File(sdir, "public-preauth.key");
            File privKey = new File(sdir, "private-preauth.key");
            if (pubKey.exists() && privKey.exists()) {
                log.info("found existing preauth keys: " + pubKey.getAbsolutePath() + " " + privKey.getAbsolutePath());
            } else {
                log.info("generating preauth keys: " + pubKey.getAbsolutePath() + " " + privKey.getAbsolutePath());
                RsaSignatureGenerator.genKeyPair(pubKey, privKey, 2048);
            }
            // private: 600
            Set<PosixFilePermission> perms = new HashSet<>();
            perms.add(PosixFilePermission.OWNER_READ);
            perms.add(PosixFilePermission.OWNER_WRITE);
            Files.setPosixFilePermissions(pubKey.toPath(), perms);
            Files.setPosixFilePermissions(privKey.toPath(), perms);
            conf.publicKey = pubKey;
            conf.privateKey = privKey;
        } catch (IOException ex) {
            throw new RuntimeException("INIT secrets FAIL", ex);
        }
    }

    @Override
    public void doShutdown() {
        try {
            Context ctx = new InitialContext();
            ctx.unbind(jndiNodePersistence);
            jndiNodePersistence = null;
        } catch (NamingException oops) {
            log.error("unbind failed during destroy", oops);
        }
    }
}
