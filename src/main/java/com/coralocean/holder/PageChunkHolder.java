package com.coralocean.holder;

import com.coralocean.exception.ExportException;
import com.coralocean.upload.OssService;
import com.coralocean.upload.UploadPartETag;
import com.coralocean.upload.UploadResult;
import com.coralocean.util.FileUtil;
import com.coralocean.writer.PrintWriterCsvWriter;
import lombok.extern.slf4j.Slf4j;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.concurrent.*;
import java.util.function.Consumer;

@Slf4j
public class PageChunkHolder {

    private final OssService ossService;

    private final ThreadPoolExecutor poolExecutor;

    private final SimpleDateFormat DATE_FORMATER = new SimpleDateFormat("yyyyMMdd");

    private final String tempExportDir;



    public PageChunkHolder(String tempExportDir, ThreadPoolExecutor poolExecutor,
                           OssService ossService) {
        this.tempExportDir = tempExportDir;
        this.poolExecutor = poolExecutor;
        this.ossService = ossService;
    }


    public void executeTaskAsync(String exportName, BlockingReader reader, Consumer<UploadResult> ossUrlHandleExecutor) {
        CompletableFuture.runAsync(() -> {
            executeTask(exportName, reader, ossUrlHandleExecutor);
        }, poolExecutor).whenComplete((resolve, reject) -> {
            if (reject != null) {
                log.error("{} 导出失败： {}", exportName, reject.getMessage(), reject);
            } else {
                log.info("{} 导出成功", exportName);
            }
        });
    }

    public void executeTaskAsync(BlockingReader reader, Consumer<UploadResult> ossUrlHandleExecutor) {
        CompletableFuture.runAsync(() -> {
            executeTask(reader, ossUrlHandleExecutor);
        }, poolExecutor).whenComplete((resolve, reject) -> {
            if (reject != null) {
                log.error("导出失败： {}", reject.getMessage(), reject);
            } else {
                log.info("导出成功");
            }
        });
        ;
    }

    public void executeTask(BlockingReader reader, Consumer<UploadResult> ossUrlHandleExecutor) {
        executeTask(reader.chunkType().getExportName(), reader, ossUrlHandleExecutor);

    }


    public void executeTask(String exportName, BlockingReader reader, Consumer<UploadResult> ossUrlHandleExecutor) {
        int i = exportName.lastIndexOf(".");
        if (i != -1) {
            exportName = exportName.substring(0, i);
        }

        List<String> chunkDataFIlePaths = new ArrayList<>();
        String saveFilePathDir = getSaveFilePathDir(reader.chunkType().getExportType());
        try {
            int chunkNum = 0;
            Long nextOffsetId = null;
            int chunks = reader.chunks();
            CountDownLatch countDownLatch = new CountDownLatch(chunks);

            DataChunk beforeNext = reader.beforeNext();
            if (beforeNext != null) {
                beforeNext.setSaveFileName(getSaveFilePath(saveFilePathDir, beforeNext.getSaveFileName(), 0));
                wrietChunk(beforeNext);
            }

            List<Future<?>> futures = new ArrayList<>();
            while (reader.hasNext(nextOffsetId)) {
                DataChunk next = reader.next(nextOffsetId);
                next.setSaveFileName(getSaveFilePath(saveFilePathDir, next.getSaveFileName(), chunkNum));
                chunkDataFIlePaths.add(next.getSaveFileName());
                chunkNum++;
                nextOffsetId = next.getNextOffset();
                CompletableFuture<Void> future = CompletableFuture.runAsync(() -> {
                    wrietChunk(next, countDownLatch);
                }, poolExecutor).whenComplete((resolve, reject) -> {
                    if (reject != null) {
                        throw new ExportException(reject.getMessage());
                    }
                });
                futures.add(future);
            }

            DataChunk afterNext = reader.afterNext();
            if (afterNext != null) {
                afterNext.setSaveFileName(getSaveFilePath(saveFilePathDir, afterNext.getSaveFileName(), chunks-1));
                wrietChunk(afterNext);
            }
            countDownLatch.await(3, TimeUnit.MINUTES);
            waitAndThrowException(futures);
            log.info("导出完成，开始合并数据");
            boolean flag = checkChunks(chunkDataFIlePaths);
            if (!flag) {
                // 说明文件有生成失败的, 此时需要告知
                log.error("生成分片文件失败！！-{}", saveFilePathDir);
                return;
            }
            UploadResult uploadResult = uploadOss(reader.chunkType(), exportName, chunkDataFIlePaths);
            ossUrlHandleExecutor.accept(uploadResult);
        } catch (Exception e) {
            // 纠错
            log.error(e.getMessage(), e);
        } finally {
            deleteTempDir(saveFilePathDir);
        }
    }

    private String getSaveFilePathDir(String chunkType) {
        // 从配置文件读取
        String dir = tempExportDir + chunkType + "/" + UUID.randomUUID() + "/";
        FileUtil.mkdir(dir);
        log.info("写入文件夹: {}", dir);
        return dir;
    }

    private static String getSaveFilePath(String savePathDir, String saveName, int chunkNum) {
        int index = saveName.indexOf(".");
        String fileName, suffix;
        if (index == -1) {
            fileName = saveName;
            suffix = ".csv";
        } else {
            fileName = saveName.substring(0, index);
            suffix = saveName.substring(index);
        }
        return savePathDir + fileName + "_chunk_" + chunkNum + suffix;
    }


    public void wrietChunk(DataChunk dataChunk, CountDownLatch countDownLatch) {
        try {
            log.info("执行写任务-{}", dataChunk.getSaveFileName());
            PrintWriterCsvWriter.write(dataChunk.getSaveFileName(), dataChunk.getResultSet());
        } finally {
            countDownLatch.countDown();
        }
    }

    public void wrietChunk(DataChunk dataChunk) {
        log.info("执行写任务-{}", dataChunk.getSaveFileName());
        PrintWriterCsvWriter.write(dataChunk.getSaveFileName(), dataChunk.getResultSet());
    }

    private UploadResult uploadOss(ExportType exportType, String saveName, List<String> chunkDataFilePaths) {
        // 只生成了一个文件
        if (chunkDataFilePaths.size() == 0) {
            return handleSingle(exportType, saveName, chunkDataFilePaths);
        }
        String objectName, uploadId;
        try {
            objectName = genExportObjectName(exportType, saveName);
            log.info("fileKey: {}", objectName);
            uploadId = ossService.getUploadId(objectName);
        } catch (Exception e) {
            log.error("生成uploadId失败： {}", e.getMessage());
            return new UploadResult();
        }
        try {
            Map<String, UploadPartETag> uploadPartETags = new ConcurrentHashMap<>();
            CountDownLatch countDownLatch = new CountDownLatch(chunkDataFilePaths.size());
            List<Future<?>> futures = new ArrayList<>();
            for (String chunkDataFilePath : chunkDataFilePaths) {
                int partNumber = parseChunkPart(chunkDataFilePath);
                CompletableFuture<UploadPartETag> future = CompletableFuture.supplyAsync(() -> {
                    try {
                        byte[] bytes = readFileBytes(chunkDataFilePath);
                        return ossService.chunkedUpload(objectName, partNumber, uploadId, bytes);
                    } finally {
                        countDownLatch.countDown();
                    }
                }, poolExecutor).whenComplete((resolve, reject) -> {
                    if (reject != null || resolve == null) {
                        throw new ExportException("分片上传失败，建议调整分片大小");
                    } else {
                        uploadPartETags.put(String.valueOf(partNumber), resolve);
                    }
                });
                futures.add(future);
            }
            countDownLatch.await(2, TimeUnit.MINUTES);
            waitAndThrowException(futures);
            log.info("开始合并文件");
            List<UploadPartETag> eTags = new ArrayList<>();
            for (Map.Entry<String, UploadPartETag> entry : uploadPartETags.entrySet()) {
                eTags.add(entry.getValue());
            }
            eTags.sort(Comparator.comparing(UploadPartETag::getPartNumber));
            String ossUrl = ossService.completeMultipartUpload(objectName, uploadId, eTags);
            return new UploadResult(ossUrl);
        } catch (Exception e) {
            log.error("分片上传文件失败： {}", e.getMessage());
            if (uploadId != null) {
                log.info("中断分片任务");
                ossService.abortMultipartUpload(objectName, uploadId);
            }
            return handleSingle(exportType, saveName, chunkDataFilePaths);
        }
    }

    private void waitAndThrowException(List<Future<?>> futures) throws InterruptedException, ExecutionException, TimeoutException {
        // 其实这里可以优化一下，收集所有的错误异常，但是这里为了性能考虑就不这么实现了
        for (Future<?> future : futures) {
            future.get(2, TimeUnit.MINUTES);
        }
    }

    public UploadResult handleSingle(ExportType exportType, String saveName, List<String> chunkDataFilePaths) {
        log.info("单文件上传： {}", saveName);
        String saveFilePathDir = getSaveFilePathDir(exportType.getExportType());
        String saveFilePath = saveFilePathDir + saveName + exportType.getExportSuffix();
        return handleSingleFile(chunkDataFilePaths, exportType, saveFilePath);
    }


    private boolean checkChunks(List<String> chunkPaths) {
        if (chunkPaths.isEmpty()) return false;
        for (String chunkPath : chunkPaths) {
            if (!FileUtil.exist(chunkPath)) {
                return false;
            }
        }
        return true;
    }

    private String genExportObjectName(ExportType exportType, String fileName) {
        return "settlement" + File.separator + exportType.getExportType() + File.separator + DATE_FORMATER.format(new Date()) +
                File.separator + UUID.randomUUID() + File.separator + fileName + exportType.getExportSuffix();
    }

    private static void deleteTempDir(String saveFileDir) {
        FileUtil.delDir(saveFileDir);
    }

    private static int parseChunkPart(String chunkDataFilePath) {
        String[] split = chunkDataFilePath.substring(0, chunkDataFilePath.lastIndexOf(".")).split("_");
        return Integer.parseInt(split[split.length - 1]) + 1;
    }

    private static byte[] readFileBytes(String chunkDataFilePath) {
       return FileUtil.readBytes(chunkDataFilePath);
    }

    private UploadResult handleSingleFile(List<String> chunkFiles, ExportType exportType, String targetFilePath) {
        boolean merged = mergeChunkFiles(chunkFiles, targetFilePath);
        String fileName = parseFileName(targetFilePath);
        if (merged) {
            String objectName = genExportObjectName(exportType, fileName);
            return uploadFile(objectName, targetFilePath);
        }
        return new UploadResult();
    }

    private static String parseFileName(String targetFilePath) {
        String substring = targetFilePath.substring(targetFilePath.lastIndexOf("/"));
        String[] split = substring.split("\\.");
        return split[0];
    }


    private boolean mergeChunkFiles(List<String> chunkFiles, String targetFile) {
        log.info("合并文件， targetFile: {}", targetFile);
        try (BufferedOutputStream targetStream = new BufferedOutputStream(Files.newOutputStream(Paths.get(targetFile)))) {
            for (String chunkFile : chunkFiles) {
                try (BufferedInputStream chunkStream = new BufferedInputStream(Files.newInputStream(Paths.get(chunkFile)))) {
                    byte[] buffer = new byte[2048];
                    int bytesRead;
                    while ((bytesRead = chunkStream.read(buffer)) != -1) {
                        targetStream.write(buffer, 0, bytesRead);
                    }
                }
            }
            return true;
        } catch (Exception e) {
            log.error(e.getMessage(), e);
            return false;
        }
    }

    private UploadResult uploadFile(String objectName, String targetFile) {
        if (ossService != null) {
            try (BufferedInputStream inputStream = new BufferedInputStream(Files.newInputStream(Paths.get(targetFile)))) {
                String ossFileUrl = ossService.uploadFile(inputStream, objectName);
                return new UploadResult(ossFileUrl);
            } catch (IOException e) {
                log.error(e.getMessage(), e);
                return new UploadResult();
            }
        } else {
            try {
                byte[] bytes = FileUtil.readBytes(targetFile);
                String s = ossService.uploadFile(bytes, objectName);
                return new UploadResult(s);
            } catch (Exception e) {
                log.error(e.getMessage(), e);
                return new UploadResult();
            }
        }

    }


}
