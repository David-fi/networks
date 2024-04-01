import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.*;

class FullNodeTest {
    private FullNode systemUnderTest;
    private TemporaryNode requester;

    @BeforeEach
    public void setUp(){
        systemUnderTest = new FullNode();
        requester = new TemporaryNode();

        assertTrue(systemUnderTest.listen("127.0.0.1", 20000));
        systemUnderTest.handleIncomingConnections("Test Full Node", "127.0.0.1:20000");

        assertTrue(requester.start("Test Temporary Node", "127.0.0.1:20000"));
    }

    @Test
    void verifyNodeStoresData(){
        assertTrue(requester.store("Key", "Value"));
        assertEquals("Value\n", systemUnderTest.getKeyValueStore().get("Key\n"));
    }

    @Test
    void verifyMultipleGetWorks(){
        requester.store("Key1", "Value1");
        requester.store("Key2", "Value2");
        assertEquals("Value1 Value2", requester.get("Key1\nKey2"));
    }

    @Test
    void verifyMultiplePutWorks(){
        requester.store("Key1\nKey2", "Value1\nValue2");
        assertEquals("Value1", requester.get("Key1"));
    }

    @Test
    void verifyEchoWorks(){
        assertTrue(requester.sendEchoRequest());
    }

    @Test
    void verifyMultipleTemporaryNodesCanConnect(){
        TemporaryNode requesterTwo = new TemporaryNode();
//        systemUnderTest.handleIncomingConnections("Test Full Node", "127.0.0.1:20000");
        requesterTwo.start("Second Temporary Node", "127.0.0.1:20000");

        assertTrue(requester.store("Key", "Value"));
        assertEquals("Value", requesterTwo.get("Key"));

        requesterTwo.endConnection("Test Closed");
    }

    @Test
    void verifyMultipleNodesCanConnectWithEachOther(){
        FullNode fullNodeTwo = new FullNode();
        assertTrue(fullNodeTwo.listen("127.0.0.1", 20001));
        fullNodeTwo.handleIncomingConnections("Second Full Node", "127.0.0.1:20001");

        fullNodeTwo.disconnectNode();
    }

    @AfterEach
    public void closeNodes(){
        systemUnderTest.disconnectNode();
        requester.endConnection("Test Closed");

    }

}