package cn.patest.redisson.codec.test

import cn.patest.redisson.codec.ProtobufCodec
import io.kotlintest.specs.StringSpec
import org.redisson.codec.SnappyCodec

class CommonTest : StringSpec({

    "not protobuf object" {
        TestUtil.runTest(ProtobufCodec())
    }

    "not protobuf and snappy" {
        TestUtil.runTest(SnappyCodec(ProtobufCodec()))
    }

})
