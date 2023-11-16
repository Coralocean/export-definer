package com.coralocean.upload;

import java.io.BufferedInputStream;
import java.util.List;

public interface OssService {
    UploadPartETag chunkedUpload(String objectName, int partNumber, String uploadId, byte[] bytes);

    String completeMultipartUpload(String objectName, String uploadId, List<UploadPartETag> eTags);

    void abortMultipartUpload(String objectName, String uploadId);

    String getUploadId(String objectName);

    String uploadFile(byte[] bytes, String objectName);

    String uploadFile(BufferedInputStream inputStream, String objectName);
}
