package cn.patest.redisson.codec

import com.google.protobuf.GeneratedMessageV3
import io.netty.buffer.*
import org.redisson.client.codec.*
import org.redisson.client.protocol.*
import org.redisson.codec.FstCodec
import java.nio.ByteBuffer

class ProtobufCodec(private val innerCodec: Codec = FstCodec()) : BaseCodec() {

    private val BYTES_INT = java.lang.Integer.BYTES
    private val EMPTY_INT_BYTES_ARRAY = ByteArray(BYTES_INT)

    private val decoder: Decoder<Any> = Decoder { buf, state ->
        val sizeByteArray = ByteArray(BYTES_INT)
        buf.readBytes(sizeByteArray)
        val classNameLength = ByteBuffer.wrap(sizeByteArray).int
        if (classNameLength == 0) {
            // Not a message.
            innerCodec.valueDecoder.decode(Unpooled.wrappedBuffer(buf), state)
        } else {
            val classNameByteArray = ByteArray(classNameLength)
            buf.readBytes(classNameByteArray)
            val className = classNameByteArray.toString(Charsets.UTF_8)
            val newBuilderMethod = Class.forName(className).getDeclaredMethod("newBuilder")
            val builder = newBuilderMethod.invoke(null) as GeneratedMessageV3.Builder<*>
            val messageByteArray = ByteArray(buf.readableBytes())
            buf.readBytes(messageByteArray)
            builder.mergeFrom(messageByteArray).build()
        }
    }

    private val encoder: Encoder = Encoder { obj ->
        // Check again for smart cast.
        if (obj is GeneratedMessageV3) {
            val out = ByteBufAllocator.DEFAULT.buffer()
            val stream = ByteBufOutputStream(out)
            val className = obj::class.java.name.toByteArray()
            val size = className.size
            val sizeArray = ByteBuffer.allocate(BYTES_INT).putInt(size).array()
            stream.write(sizeArray)
            stream.write(className)
            stream.write(obj.toByteArray())
            out
        } else {
            Unpooled.wrappedBuffer(Unpooled.wrappedBuffer(EMPTY_INT_BYTES_ARRAY), innerCodec.valueEncoder.encode(obj))
        }
    }

    override fun getMapKeyDecoder() = decoder

    override fun getValueEncoder() = encoder

    override fun getMapValueEncoder() = encoder

    override fun getValueDecoder() = decoder

    override fun getMapValueDecoder() = decoder

    override fun getMapKeyEncoder() = encoder

}
