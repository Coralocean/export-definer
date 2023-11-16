package com.coralocean.util;


import com.coralocean.exception.ExportException;

import java.io.File;
import java.io.IOException;
import java.nio.file.*;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.AbstractMap;
import java.util.Optional;

public final class FileUtil {

    public static void mkdir(String path) {
        Optional<File> existOptional = existDir(path);
        if ( !existOptional.isPresent() ) {
            try {
                Path filePath = Paths.get(path);
                Files.createDirectories(filePath);
            }catch (IOException e) {
                throw new ExportException(e.getMessage());
            }
        }
    }

    private static Optional<File> existDir(String path) {
        if ( null == path ) {
            return Optional.empty();
        }
        File file = new File(path);
        if ( file.exists() && file.isDirectory() ) {
            return Optional.of(file);
        }
        return Optional.empty();
    }

    private static Optional<File> existFile(String path) {
        if ( null == path ) {
            return Optional.empty();
        }
        File file = new File(path);
        if ( file.exists() && file.isFile() ) {
            return Optional.of(file);
        }
        return Optional.empty();
    }

    public static boolean exist(String path ) {
        if ( null == path ) {
            return false;
        }
        return new File(path).exists();
    }

    public static void delDir(String dirPath) {
        Optional<File> fileOptional = existDir(dirPath);
        if ( fileOptional.isPresent() ) {
            Path path = Paths.get(dirPath);
            try {
                Files.walkFileTree(path, new SimpleFileVisitor<Path>() {
                    @Override
                    public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) throws IOException {
                        // 删除文件
                        Files.delete(file);
                        return FileVisitResult.CONTINUE;
                    }

                    @Override
                    public FileVisitResult postVisitDirectory(Path dir, IOException exc) throws IOException {
                        // 删除目录
                        Files.delete(dir);
                        return FileVisitResult.CONTINUE;
                    }
                });
            }catch (IOException e) {
                throw new ExportException(e.getMessage());
            }
        }
    }

    public static byte[] readBytes(String filePath) {
        Optional<File> fileOptional = existFile(filePath);
        if (fileOptional.isPresent()) {
            try {
                Path path = Paths.get(filePath);
                Files.readAllBytes(path);
            }catch (IOException e) {
                throw new ExportException(e.getMessage());
            }
        }
        return null;

    }
}
