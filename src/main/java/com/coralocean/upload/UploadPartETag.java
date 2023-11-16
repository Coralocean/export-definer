package com.coralocean.upload;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.Serializable;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class UploadPartETag implements Serializable {
    private int partNumber;
    private String eTag;
    private long partSize;
    private Long partCRC;
}
