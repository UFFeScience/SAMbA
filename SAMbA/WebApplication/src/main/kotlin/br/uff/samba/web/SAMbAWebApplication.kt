package br.uff.samba.web

import org.springframework.boot.SpringApplication
import org.springframework.boot.autoconfigure.SpringBootApplication

@SpringBootApplication
class SAMbAWebApplication

fun main(args: Array<String>) {
    SpringApplication.run(SAMbAWebApplication::class.java, *args)
}
