package com.biron.singer.core.logging

import ch.qos.logback.classic.Level
import ch.qos.logback.classic.LoggerContext
import ch.qos.logback.classic.encoder.PatternLayoutEncoder
import ch.qos.logback.classic.spi.ILoggingEvent
import ch.qos.logback.core.ConsoleAppender
import ch.qos.logback.core.joran.spi.ConsoleTarget
import org.slf4j.LoggerFactory

class LoggingConfigurer(
	private val basePackage: String,
) {
	private val ctx by lazy { LoggerFactory.getILoggerFactory() as LoggerContext }

	private val mainAppender: ConsoleAppender<ILoggingEvent> = ConsoleAppender<ILoggingEvent>().apply {
		name = "Console"
		context = ctx
		target = ConsoleTarget.SystemErr.toString()
		encoder = PatternLayoutEncoder().apply {
			context = ctx
			pattern = "[%-5level] %7.20r %msg%n%throwable"
			start()
		}
		start()
	}

	init {
		ctx.getLogger("ROOT").apply {
			level = Level.WARN
			detachAndStopAllAppenders()
			addAppender(mainAppender)
		}
		ctx.start()
	}

	fun reconfigure(useStdoutInsteadOfStdErr: Boolean, basePackageLogLevel: org.slf4j.event.Level = org.slf4j.event.Level.INFO) {
		if (useStdoutInsteadOfStdErr) mainAppender.apply {
			target = ConsoleTarget.SystemOut.toString()
			start()
		}
		ctx.getLogger(basePackage).apply {
			level = Level.convertAnSLF4JLevel(basePackageLogLevel)
		}
	}

}
