package org.opencadc.cavern;

import ca.nrc.cadc.conformance.vos.AsyncPullFromVOSpaceTest;
import ca.nrc.cadc.conformance.vos.AsyncPushToVOSpaceTest;
import ca.nrc.cadc.conformance.vos.CreateContainerNodeTest;
import ca.nrc.cadc.conformance.vos.CreateDataNodeTest;
import ca.nrc.cadc.conformance.vos.CreateLinkNodeTest;
import ca.nrc.cadc.conformance.vos.DeleteContainerNodeTest;
import ca.nrc.cadc.conformance.vos.DeleteDataNodeTest;
import ca.nrc.cadc.conformance.vos.DeleteLinkNodeTest;
import ca.nrc.cadc.conformance.vos.GetContainerNodeTest;
import ca.nrc.cadc.conformance.vos.GetDataNodeTest;
import ca.nrc.cadc.conformance.vos.GetLinkNodeTest;
import ca.nrc.cadc.conformance.vos.MoveContainerNodeTest;
import ca.nrc.cadc.conformance.vos.MoveDataNodeTest;
import ca.nrc.cadc.conformance.vos.MoveLinkNodeTest;
import ca.nrc.cadc.conformance.vos.SetContainerNodeTest;
import ca.nrc.cadc.conformance.vos.SetDataNodeTest;
import ca.nrc.cadc.conformance.vos.SetLinkNodeTest;
import ca.nrc.cadc.conformance.vos.SyncPullFromVOSpaceTest;
import ca.nrc.cadc.conformance.vos.SyncPushToVOSpaceTest;
import ca.nrc.cadc.util.Log4jInit;

import org.apache.log4j.Level;
import org.junit.runner.RunWith;
import org.junit.runners.Suite;

@RunWith(Suite.class)
@Suite.SuiteClasses
({
    CreateContainerNodeTest.class,
    CreateDataNodeTest.class,
    CreateLinkNodeTest.class,

    GetContainerNodeTest.class,
    GetDataNodeTest.class,
    GetLinkNodeTest.class,

    DeleteContainerNodeTest.class,
    DeleteDataNodeTest.class,
    DeleteLinkNodeTest.class,

    SetContainerNodeTest.class,
    SetDataNodeTest.class,
    SetLinkNodeTest.class,

    MoveContainerNodeTest.class,
    MoveDataNodeTest.class,
    MoveLinkNodeTest.class,

    SyncPullFromVOSpaceTest.class,
    SyncPushToVOSpaceTest.class,

    AsyncPullFromVOSpaceTest.class,
    AsyncPushToVOSpaceTest.class
})

public class VOSConformanceTestSuite
{
    static
    {
        Log4jInit.setLevel("ca.nrc.cadc.vospace", Level.DEBUG);
        Log4jInit.setLevel("ca.nrc.cadc.vos", Level.DEBUG);
    }
}