package org.opencadc.vospace.server.async;

import ca.nrc.cadc.auth.AuthenticationUtil;
import ca.nrc.cadc.io.ResourceIterator;
import ca.nrc.cadc.uws.Job;
import ca.nrc.cadc.uws.Parameter;
import ca.nrc.cadc.uws.Result;
import java.net.URI;
import java.util.ArrayList;
import java.util.List;
import javax.security.auth.Subject;
import org.apache.log4j.Logger;
import org.opencadc.vospace.ContainerNode;
import org.opencadc.vospace.DataNode;
import org.opencadc.vospace.Node;
import org.opencadc.vospace.VOSURI;
import org.opencadc.vospace.server.Utils;

public class RecursiveSizeRunner extends AbstractRecursiveRunner {
    private static final Logger log = Logger.getLogger(RecursiveSizeRunner.class);

    private long totalBytes = 0L;
    private int maxDepth = 0;

    @Override
    public void setJob(Job job) {
        this.job = job;

        for (Parameter p : job.getParameterList()) {
            if ("target".equalsIgnoreCase(p.getName())) {
                this.target = new VOSURI(URI.create(p.getValue()));
            } else if ("maxdepth".equalsIgnoreCase(p.getName())) {
                try {
                    int v = Integer.parseInt(p.getValue());
                    if (v < 0) {
                        v = 0;
                    }
                    this.maxDepth = v;
                } catch (NumberFormatException ignore) {
                    // keep default 0
                }
            }
        }
        if (target == null) {
            throw new IllegalArgumentException("target argument required");
        }
        log.debug("target: " + target + " maxDepth=" + maxDepth);
    }

    @Override
    protected boolean performAction(Node node) throws Exception {
        if (!(node instanceof ContainerNode)) {
            sendError("target must be a ContainerNode");
            return false;
        }
        ContainerNode root = (ContainerNode) node;
        if (!nodePersistence.isAllocation(root)) {
            // Enforces "only on an allocation"
            sendError("target is not an allocation root");
            return false;
        }
        Subject subject = AuthenticationUtil.getCurrentSubject();
        if (!authorizer.hasSingleNodeReadPermission(root, subject)) {
            sendError("no read permission on allocation root");
            return false;
        }

        // compute size for whole allocation (full recursion, independent of maxDepth)
        return accumulate(root, subject, 0);
    }

    private boolean accumulate(Node node, Subject subject, int depth) throws Exception {
        if (Thread.currentThread().isInterrupted()) {
            throw new InterruptedException();
        }

        // ensure job still EXECUTING; reuse existing phase checks
        checkJobPhase();

        // permission check for each node; deny == skip subtree but count as error
        if (!authorizer.hasSingleNodeReadPermission(node, subject)) {
            log.debug("no read permission on node " + Utils.getPath(node));
            //incErrorCount(); not counting errors for permission-denied nodes, instead outputting permissions denied for the whole subtree
            return false;
        }

        boolean success = true;

        if (node instanceof DataNode) {
            DataNode dn = (DataNode) node;
            Long bytes = dn.bytesUsed;
            if (bytes == null) {
                bytes = 0L;
            }
            totalBytes += bytes;
            incSuccessCount();
            return true;
        }

        if (node instanceof ContainerNode) {
            ContainerNode cn = (ContainerNode) node;

            // TODO: verify if bytesUsed is up-to-date to use. or else need to traverse child tree.
            if (cn.bytesUsed != null && depth > 0) {
                totalBytes += cn.bytesUsed;
                incSuccessCount();
                return true;
            }

            List<ContainerNode> childContainers = new ArrayList<>();
            try (ResourceIterator<Node> iter = nodePersistence.iterator(cn, null, null)) {
                while (iter.hasNext()) {
                    Node child = iter.next();
                    if (child instanceof ContainerNode) {
                        childContainers.add((ContainerNode) child);
                    } else {
                        success &= accumulate(child, subject, depth + 1);
                    }
                }
            }
            for (ContainerNode childContainer : childContainers) {
                success &= accumulate(childContainer, subject, depth + 1);
            }
        }

        return success;
    }

    @Override
    protected List<Result> getAdditionalResults() {
        List<Result> out = new ArrayList<>();
        out.add(new Result("bytesUsed", URI.create("final:" + totalBytes)));
        // Future: additional per-depth or per-container details can be encoded here.
        return out;
    }
}