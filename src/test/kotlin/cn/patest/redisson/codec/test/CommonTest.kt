package cn.patest.redisson.codec.test

import cn.patest.redisson.codec.ProtobufCodec
import org.junit.*
import org.redisson.codec.SnappyCodec
import org.testcontainers.containers.GenericContainer

class CommonTest {

    @Before
    fun before() {
        container.start()
    }

    @After
    fun after() {
        container.stop()
    }

    @Test
    fun notProtobufObject() {
        TestUtil.runTest(ProtobufCodec(), container.containerIpAddress, container.firstMappedPort)
    }

    @Test
    fun notProtobufAndSnappy() {
        TestUtil.runTest(SnappyCodec(ProtobufCodec()), container.containerIpAddress, container.firstMappedPort)
    }

    val container = KGenericContainer("redis:5.0.3-alpine").withExposedPorts(6379)

    class KGenericContainer(name: String) : GenericContainer<KGenericContainer>(name)
}
