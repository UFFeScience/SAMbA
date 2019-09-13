package br.uff.samba.web.conf

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule
import org.springframework.beans.BeansException
import org.springframework.context.ApplicationContext
import org.springframework.context.ApplicationContextAware
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import org.springframework.context.annotation.Primary
import org.springframework.web.servlet.config.annotation.WebMvcConfigurerAdapter
import java.time.LocalDateTime

@Configuration
class WebConfig : ApplicationContextAware, WebMvcConfigurerAdapter() {

    private var applicationContext: ApplicationContext? = null

    @Throws(BeansException::class)
    override fun setApplicationContext(applicationContext: ApplicationContext) {
        this.applicationContext = applicationContext
    }

    @Bean
    @Primary
    fun serializingObjectMapper(): ObjectMapper {

        val objectMapper = ObjectMapper()
        val javaTimeModule = JavaTimeModule()
        javaTimeModule.addSerializer(LocalDateTime::class.java, LocalDateTimeFomatter())
//        javaTimeModule.addDeserializer(LocalDate::class.java, LocalDateDeserializer())
        objectMapper.registerModule(javaTimeModule)
        return objectMapper

    }
}