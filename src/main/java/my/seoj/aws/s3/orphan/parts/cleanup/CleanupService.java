package my.seoj.aws.s3.orphan.parts.cleanup;

import org.apache.log4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.AbortMultipartUploadRequest;
import com.amazonaws.services.s3.model.ListMultipartUploadsRequest;
import com.amazonaws.services.s3.model.ListPartsRequest;
import com.amazonaws.services.s3.model.MultipartUpload;
import com.amazonaws.services.s3.model.MultipartUploadListing;
import com.amazonaws.services.s3.model.PartListing;
import com.amazonaws.services.s3.model.PartSummary;

/**
 * Service which aborts any multipart uploads which has not been completed nor aborted.
 */
@Service
public class CleanupService
{
    private static final Logger logger = Logger.getLogger(CleanupService.class);

    private AmazonS3 s3;

    @Autowired
    public CleanupService(AmazonS3 s3)
    {
        this.s3 = s3;
    }

    /**
     * Aborts any multipart uploads which has not been completed or aborted. The cleanup procedure will also log the size of the uploads that will be aborted
     * into INFO log.
     * 
     * @param s3BucketName - The name of the S3 bucket to clean up
     */
    public void cleanup(String s3BucketName)
    {
        boolean truncated = false;
        String uploadIdMarker = null;
        String keyMarker = null;
        do
        {
            ListMultipartUploadsRequest request = new ListMultipartUploadsRequest(s3BucketName);
            request.setUploadIdMarker(uploadIdMarker);
            request.setKeyMarker(keyMarker);

            MultipartUploadListing multipartUploadListing = s3.listMultipartUploads(request);

            for (MultipartUpload multipartUpload : multipartUploadListing.getMultipartUploads())
            {
                String s3ObjectKey = multipartUpload.getKey();
                long size = getSize(s3BucketName, multipartUpload);

                logger.info("cleanup(): s3BucketName = " + s3BucketName + ", s3ObjectKey = " + s3ObjectKey + ", size = " + size);
                s3.abortMultipartUpload(new AbortMultipartUploadRequest(s3BucketName, s3ObjectKey, multipartUpload.getUploadId()));
            }

            if (truncated = multipartUploadListing.isTruncated())
            {
                uploadIdMarker = multipartUploadListing.getUploadIdMarker();
                keyMarker = multipartUploadListing.getKeyMarker();
            }
        }
        while (truncated);
    }

    private long getSize(String s3BucketName, MultipartUpload multipartUpload)
    {
        long size = 0;
        boolean truncated = false;
        Integer partNumberMarker = null;
        do
        {
            ListPartsRequest listPartsRequest = new ListPartsRequest(s3BucketName, multipartUpload.getKey(), multipartUpload.getUploadId());
            listPartsRequest.setPartNumberMarker(partNumberMarker);

            PartListing partListing = s3.listParts(listPartsRequest);
            for (PartSummary part : partListing.getParts())
            {
                size += part.getSize();
            }

            if (truncated = partListing.isTruncated())
            {
                partNumberMarker = partListing.getNextPartNumberMarker();
            }
        }
        while (truncated);
        return size;
    }
}
