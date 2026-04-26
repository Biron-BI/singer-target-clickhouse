package com.biron.singerTargetClickhouse.utilsTest

import java.nio.file.FileSystem
import java.nio.file.Files

fun FileSystem.createFileWithContent(path: String, content: String) =
	createFileWithContent(path, content.encodeToByteArray())

fun FileSystem.createFileWithContent(path: String, content: ByteArray) =
	getPath(path).also {
		Files.createDirectories(it.parent)
		Files.createFile(it)
		Files.write(it, content)
	}
