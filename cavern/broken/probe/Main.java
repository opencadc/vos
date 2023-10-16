/*
************************************************************************
*******************  CANADIAN ASTRONOMY DATA CENTRE  *******************
**************  CENTRE CANADIEN DE DONNÉES ASTRONOMIQUES  **************
*
*  (c) 2019.                            (c) 2019.
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

package org.opencadc.cavern.probe;

import ca.nrc.cadc.util.ArgumentMap;
import ca.nrc.cadc.util.Log4jInit;
import java.io.File;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.opencadc.vospace.VOSURI;

/**
 *
 * @author pdowler
 */
public class Main {

    private static final Logger log = Logger.getLogger(Main.class);

    private static String[] logPackages = new String[]{
        "org.opencadc.cavern",
        "ca.nrc.cadc.vos"
    };

    static void usage() {
        System.out.println("usage: cavern [-h|--help]");
        System.out.println("usage: cavern [-v|--verbose|-d|--debug] [--views]");
        
        System.out.println("              --owner=<posix username> ");
        System.out.println("              --target-owner=<posix username>)");
        System.out.println("              --dir=<test directory>");
        System.out.println("              --baseURI=<base VOS URI (no trailing slash)>");
        System.out.println("              --group=<test group that target-owner belongs to>");
        
        System.out.println("Note: the target-owner owns the target of a link and should differ from");
        System.out.println("      the owner so that correct behaviour of symlinks can be verified");
    }

    public static void main(String[] args) {
        try {
            ArgumentMap am = new ArgumentMap(args);
            if (am.isSet("h") || am.isSet("help")) {
                usage();
                System.exit(0);
            }

            // Set debug mode
            Level level = Level.WARN;
            if (am.isSet("d") || am.isSet("debug")) {
                level = Level.DEBUG;
            } else if (am.isSet("v") || am.isSet("verbose")) {
                level = Level.INFO;
            }
            for (String pkg : logPackages) {
                Log4jInit.setLevel(pkg, level);
            }

            boolean ok = true;
            
            String owner = am.getValue("owner");
            if (owner == null) {
                log.error("missing required argument: --owner=<posix username>");
                ok = false;
            }
            
            String targetOwner = am.getValue("target-owner");
            if (targetOwner == null) {
                log.error("missing required argument: --target-owner=<posix username>");
                ok = false;
            }
            
            String group = am.getValue("group");
            if (group == null) {
                log.error("missing required argument: --group=<posix group name>");
                ok = false;
            }
            
            File baseDir = null;
            String dir = am.getValue("dir");
            if (dir != null) {
                baseDir = new File(dir);
                log.info("    base dir: " + baseDir.getAbsolutePath());
                log.info("      exists:" + baseDir.exists());
                log.info("is directory:" + baseDir.isDirectory());
                log.info("    readable:" + baseDir.canRead());
                log.info("    writable:" + baseDir.canWrite());
                if (!baseDir.exists()) {
                    log.error("not found: " + baseDir);
                    ok = false;
                } else if (!baseDir.isDirectory()) {
                    log.error("not a directory: " + baseDir);
                    ok = false;
                } else if (!baseDir.canRead() || !baseDir.canWrite()) {
                    log.error("permission denied: " + baseDir);
                    ok = false;
                } else {
                    log.info("base dir & permissions: OK");
                }
            } else {
                log.error("missing required argument: --dir=<test directory>");
                ok = false;
            }
            
            String baseURI = am.getValue("baseURI");
            if (baseURI != null) {
                try {
                    new VOSURI(baseURI);
                    if (baseURI.endsWith("/")) {
                        log.error("trailing slash on base vos uri");
                        ok = false;
                    }
                } catch (Throwable t) {
                    log.error("not a valid vos uri: " + baseURI);
                    ok = false;
                }
            } else {
                log.error("missing required argument: --baseURI=<base VOS URI>");
                ok = false;
            }
            
            if (!ok) {
                usage();
                System.exit(1);
            }
            
            log.info("user: " + owner);
            log.info("alt user: " + targetOwner + " [owner of target file in symlink tests]");

            FileSystemProbe probe = new FileSystemProbe(baseDir, baseURI, owner, targetOwner, group);
            Boolean success = probe.call();
            if (success == null || !success) {
                System.exit(1);
            }
        } catch (Throwable t) {
            log.error("unexpected failure", t);
            System.exit(1);
        }
        System.exit(0);
    }

    public Main() {
    }
}
