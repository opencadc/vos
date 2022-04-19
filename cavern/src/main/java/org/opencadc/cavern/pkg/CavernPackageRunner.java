/*
************************************************************************
*******************  CANADIAN ASTRONOMY DATA CENTRE  *******************
**************  CENTRE CANADIEN DE DONNÉES ASTRONOMIQUES  **************
*
*  (c) 2022.                            (c) 2022.
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
*  $Revision: 5 $
*
************************************************************************
*/

package org.opencadc.cavern.pkg;

import ca.nrc.cadc.uws.JobInfo;
import ca.nrc.cadc.vos.Transfer;
import ca.nrc.cadc.vos.TransferParsingException;
import ca.nrc.cadc.vos.TransferReader;
import ca.nrc.cadc.vos.VOSURI;
import java.io.IOException;
import java.net.MalformedURLException;
import java.util.EnumSet;
import org.opencadc.pkg.server.PackageItem;
import org.opencadc.pkg.server.PackageRunner;

import org.opencadc.cavern.nodes.NodeUtil;
import java.nio.file.attribute.BasicFileAttributes;
import java.nio.file.Files;
import java.nio.file.FileVisitResult;
import java.nio.file.FileVisitor;
import java.nio.file.FileVisitOption;
import java.nio.file.Path;

import java.net.URI;
import java.net.URL;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.log4j.Logger;


public class CavernPackageRunner extends PackageRunner {
    private static final Logger log = Logger.getLogger(CavernPackageRunner.class);
    private Transfer packageTransfer;

    public CavernPackageRunner() { }

    @Override
    protected void initPackage() throws IllegalArgumentException {
        log.debug("initPackage started");
        try {
            // check job is valid
            // construct a package name
            log.debug("job id passed in: " + job.getID());

            // Get the target list from the job
            TransferReader tr = new TransferReader();
            JobInfo jobInfo = job.getJobInfo();
            log.debug("job info: " + jobInfo.toString());

            this.packageTransfer = tr.read(jobInfo.getContent(), VOSURI.SCHEME);

            List<URI> targetList = packageTransfer.getTargets();

            if (targetList.size() > 1) {
                this.packageName = "cadc-download-" + job.getID();
            } else {
                this.packageName = getFilenamefromURI(targetList.get(0));
            }
            log.debug("package name: " + this.packageName);
        } catch (IOException | TransferParsingException e) {
            throw new RuntimeException("ERROR reading jobInfo: ", e);
        }
        log.debug("initPackage ended");
    }

    @Override
    public Iterator<PackageItem> getItems() throws IOException {
        log.debug("getItems started");
        List<PackageItem> packageItems = new ArrayList<>();
        Path rootPath = NodeUtil.getRootPath();
        log.debug("root path:" + rootPath.toString());

        // packageTransfer is populated in initPackage
        List<URI> targetList = packageTransfer.getTargets();
        log.debug("targetList: " );

        for (URI targetURI : targetList) {

            VOSURI targetVOSURI = new VOSURI(targetURI);

            log.debug("targetVOSURI: " + targetURI.toString());
            // Creates a Path object with the VOS_FILESYSTEM_ROOT at the front
            Path targetPath = NodeUtil.nodeToPath(rootPath, targetVOSURI);
            log.debug("targetPath: " + targetPath.toString());
            log.debug("targetPath notExists: " + Files.notExists(targetPath));
            log.debug("targetPath isDirectory: " + Files.isDirectory(targetPath));
            log.debug("targetPath isRegularFile: " + Files.isRegularFile(targetPath));

            // This is needed to set the relative path of everything in ManifestList
            // packageParentPath will be pruned off the file paths returned
            // from walkFileTree
            Path packageParentPath = targetPath.getParent();
            if (packageParentPath == null) {
                // At the verly least the CANFAR root path should be
                // passed in so it can be pruned off the file paths
                packageParentPath = rootPath;
            }

            if (Files.isRegularFile(targetPath)) {
                // links and files
                log.debug("file item found (no tree walk) " + targetPath);
                addToPkgList(targetPath, packageParentPath, packageItems);
            } else {
                // Build the full path to files, including canfar_root
                log.debug("not regular file - will walk it " + targetPath);

                ManifestVisitor mv = new ManifestVisitor(rootPath);
                Files.walkFileTree(targetPath, EnumSet.of(FileVisitOption.FOLLOW_LINKS), Integer.MAX_VALUE, mv);
                log.debug("manifest list size: " + mv.getManifestList().size());

                for (Path p : mv.getManifestList()) {
                    log.debug("path: " + p);
                    // p doesn't have VOS_FILESYSTEM_ROOT or rootPath attached.
                    // system won't find it.
                    Path fullPath = rootPath.resolve(p);

                    log.debug("file item found after tree walk: " + fullPath);
                    addToPkgList(fullPath, packageParentPath, packageItems);
                }
            }

        }
        log.debug("target list: " + targetList.toString());
        log.debug("getItems returning");
        return packageItems.iterator();
    }


    /**
     * Add the targetPath to the package list (piList).
     * @param targetPath - Path of target to add to PackageItem list
     * @param packageParentPath - Path to root of the package being built
     * @param piList - list of PackageItem being added to
     */
    private void addToPkgList(Path targetPath, Path packageParentPath, List<PackageItem> piList) {
        log.debug("adding path to list: " + targetPath.toString());
        try {
            // Generate a file:// URL using the targetPath passed in
            // Assumption here is canfar_root is well known, so there isn't a need
            // to compare the path parts - it has been added to anything in the ManifestList,
            // so subpath using count of parts is safe here
            URL fileURL = new URL("file://" + targetPath.toString());
            log.debug("file uri: " + fileURL.toString());

            Path relativePath;
            int parentPathLength = packageParentPath.getNameCount();
            log.debug("parent path length: " + parentPathLength);

            int targetPathLength = targetPath.getNameCount();
            log.debug("targetPath part count: " + targetPathLength);
            relativePath = targetPath.subpath(parentPathLength, targetPathLength);

            String relativePathStr = "/" + relativePath.toString();
            log.debug("relativePath: " + relativePathStr);

            log.debug("adding to PackageItem list");

            PackageItem pi = new PackageItem(fileURL, relativePathStr);
            piList.add(pi);
        }  catch (MalformedURLException mue) {
            log.info("Malformed URL: " + targetPath.toString() + " - skipping...");
        }
    }

    /**
     * FileVisitor implementation that adds files visited to the PackageItem
     * list owned by CavernPackageRunner
     */
    private static class ManifestVisitor implements FileVisitor<Path> {

        private Path root;
        private List<Path> manifestList;

        ManifestVisitor(Path root) {
            this.root = root;
            manifestList = new ArrayList<>();
        }

        @Override
        public FileVisitResult preVisitDirectory(Path t, BasicFileAttributes bfa)
            throws IOException {
            log.debug("ManifestVisitor: pre-visit directory: " + t);
            return FileVisitResult.CONTINUE;
        }

        @Override
        public FileVisitResult visitFile(Path t, BasicFileAttributes bfa)
            throws IOException {
            log.debug("ManifestVisitor: visiting file: " + t);

            // Add the foot to the name of the file being visited
            Path file = root.relativize(t);
            log.debug("ManifestVisitor: adding file to manifest list: " + file);
            manifestList.add(file);
            return FileVisitResult.CONTINUE;
        }

        @Override
        public FileVisitResult visitFileFailed(Path t, IOException ioe)
            throws IOException {
            return FileVisitResult.CONTINUE;
        }

        @Override
        public FileVisitResult postVisitDirectory(Path t, IOException ioe)
            throws IOException {
            return FileVisitResult.CONTINUE;
        }

        public List<Path> getManifestList() {
            return manifestList;
        }
    }

    /**
     * Build a filename from the URI provided
     * @return - name of last element in URI path.
     */
    private static String getFilenamefromURI(URI vosuri) {
        String path = vosuri.getPath();
        int i = path.lastIndexOf("/");
        if (i >= 0) {
            path = path.substring(i + 1);
        }
        return path;
    }

}
