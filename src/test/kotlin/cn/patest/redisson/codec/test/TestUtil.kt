package cn.patest.redisson.codec.test

import cn.patest.proto.test.TestProtos.*
import org.assertj.core.api.Assertions.assertThat
import org.redisson.Redisson
import org.redisson.client.codec.BaseCodec
import org.redisson.config.Config
import java.io.Serializable
import java.util.*

object TestUtil {

    class SimpleObject(
        var id: String = "10",
        var name: String = "simple"
    ) : Serializable

    val message = TestMessage.newBuilder()
        .setId(10)
        .setName("test").build()

    val nestedMessage = NestedMessage.newBuilder()
        .setNested(message)
        .setId(20)
        .setName("nested").build()

    private fun message(id: Long, name: String) = NestedMessage.newBuilder()
        .setId(id)
        .setName(name)
        .setNested(TestMessage.newBuilder()
            .setId(id + 100)
            .setName(name + "100")).build()

    fun runTest(codec: BaseCodec, ipAddr: String, port: Int) {
        val redisson = Redisson.create(Config().also {
            it.codec = codec
            it.useSingleServer().setAddress("redis://$ipAddr:$port")
        })

        val map = redisson.getMap<Int, Map<String, Any>>("getAll")
        val a = HashMap<String, Any>()
        a["double"] = 100000.0
        a["float"] = 100.0f
        a["int"] = 100
        a["long"] = 10000000000L
        a["boolt"] = true
        a["boolf"] = false
        a["string"] = "testString"
        a["array"] = listOf(1, 2.0, "adsfasdfsdf")
        a["obj"] = message
        a["nested"] = nestedMessage

        map.fastPut(1, a)
        val resa = map[1]
        assertThat(a).isEqualTo(resa)

        val set = redisson.getSet<SimpleObject>("set")
        set.clear()

        set.add(SimpleObject("1", "2"))
        set.add(SimpleObject("1", "2"))
        set.add(SimpleObject("2", "3"))
        set.add(SimpleObject("3", "4"))
        set.add(SimpleObject("5", "6"))

        assertThat(set).hasSize(4)
        assertThat(set.contains(SimpleObject("1", "2"))).isTrue()
        assertThat(set.contains(SimpleObject("2", "3"))).isTrue()
        assertThat(set.contains(SimpleObject("3", "3"))).isFalse()
        assertThat(set.contains(SimpleObject("5", "6"))).isTrue()
        assertThat(set.contains(SimpleObject("5", "7"))).isFalse()

        val protoSet = redisson.getSet<NestedMessage>("message")
        protoSet.clear()

        protoSet.add(message(1, "1"))
        protoSet.add(message(2, "2"))
        protoSet.add(message(3, "3"))
        protoSet.add(message(3, "4"))
        protoSet.add(message(1, "1"))

        assertThat(protoSet).hasSize(4)
        assertThat(protoSet.contains(message(1, "1"))).isTrue()
        assertThat(protoSet.contains(message(2, "2"))).isTrue()
        assertThat(protoSet.contains(message(3, "3"))).isTrue()
        assertThat(protoSet.contains(message(3, "4"))).isTrue()
        assertThat(protoSet.contains(message(3, "5"))).isFalse()
        assertThat(protoSet.contains(message(1, "2"))).isFalse()
        assertThat(protoSet.contains(message(2, "1"))).isFalse()

        val bucket = redisson.getBucket<SimpleObject>("simpleObjectBucket")
        bucket.delete()
        bucket.set(SimpleObject("1", "1"))
        assertThat(bucket.get()).isEqualToComparingFieldByFieldRecursively(SimpleObject("1", "1"))
        bucket.compareAndSet(SimpleObject("1", "1"), SimpleObject("2", "2"))
        assertThat(bucket.get()).isEqualToComparingFieldByFieldRecursively(SimpleObject("2", "2"))
        bucket.compareAndSet(SimpleObject("1", "1"), SimpleObject("3", "3"))
        assertThat(bucket.get()).isEqualToComparingFieldByFieldRecursively(SimpleObject("2", "2"))

        val newBucket = redisson.getBucket<NestedMessage>("nestedMessageBucket")
        newBucket.delete()
        newBucket.set(message(1, "1"))
        assertThat(newBucket.get()).isEqualTo(message(1, "1"))
        newBucket.compareAndSet(message(1, "1"), message(2, "2"))
        assertThat(newBucket.get()).isEqualTo(message(2, "2"))
        newBucket.compareAndSet(message(1, "1"), message(3, "3"))
        assertThat(newBucket.get()).isEqualTo(message(2, "2"))

        redisson.shutdown()
    }
}
