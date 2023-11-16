package com.coralocean.upload;

import lombok.Data;

@Data
public  class UploadResult {
    String ossUrl;
    Boolean isSuccess;

    public UploadResult(String ossUrl) {
        this.ossUrl = ossUrl;
        this.isSuccess = ossUrl != null;
    }

    public UploadResult() {
        this.isSuccess = false ;
    }

}
