package br.uff.samba.web.repository

import com.datastax.driver.core.TypeCodec
import com.datastax.driver.extras.codecs.MappingCodec
import com.datastax.driver.core.TypeTokens


class CodecFrozenList : MappingCodec<List<String>, List<String>>(TypeCodec.list(TypeCodec.varchar()), TypeTokens.listOf(String::class.java)) {
    override fun deserialize(value: List<String>): List<String> {
        return value
    }

    override fun serialize(value: List<String>): List<String> {
        TODO("not implemented") //To change body of created functions use File | Settings | File Templates.
    }
}