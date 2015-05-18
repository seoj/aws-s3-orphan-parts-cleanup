package my.seoj.aws.s3.orphan.parts.cleanup;

import org.apache.log4j.Level;
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
 * <p>
 * Service which aborts any multipart uploads which has not been completed nor aborted.
 * </p>
 * <p>
 * This service uses a pre-configured {@link AmazonS3}, which specifies the credentials and proxy settings when initialized through Spring context. The
 * configuration can be found in {@link CleanupConfiguration}.
 * </p>
 * <p>
 * For testing purposes, a customized {@link AmazonS3} implementation may be passed in to the constructor.
 * </p>
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
        // List upload markers. Null implies initial list request.
        String uploadIdMarker = null;
        String keyMarker = null;

        boolean truncated = false;
        do
        {
            // Create the list multipart request, optionally using the last markers.
            ListMultipartUploadsRequest request = new ListMultipartUploadsRequest(s3BucketName);
            request.setUploadIdMarker(uploadIdMarker);
            request.setKeyMarker(keyMarker);

            // Request listing
            MultipartUploadListing multipartUploadListing = s3.listMultipartUploads(request);

            for (MultipartUpload multipartUpload : multipartUploadListing.getMultipartUploads())
            {
                String s3ObjectKey = multipartUpload.getKey();

                // Only retrieve size information when appropriate logging level because size information is expensive.
                if (logger.getLevel().isGreaterOrEqual(Level.INFO))
                {
                    long size = getSize(s3BucketName, multipartUpload);
                    logger.info("cleanup(): s3BucketName = " + s3BucketName + ", s3ObjectKey = " + s3ObjectKey + ", size = " + size);
                }

                // Abort the upload
                s3.abortMultipartUpload(new AbortMultipartUploadRequest(s3BucketName, s3ObjectKey, multipartUpload.getUploadId()));
            }

            // Determine whether there are more uploads to list
            if (truncated = multipartUploadListing.isTruncated())
            {
                // Record the list markers
                uploadIdMarker = multipartUploadListing.getUploadIdMarker();
                keyMarker = multipartUploadListing.getKeyMarker();
            }
        }
        while (truncated); // Repeat listing until no more results are found
    }

    /**
     * Retrieves the total size in bytes for the given bucket and upload.
     * 
     * @param s3BucketName - The name of the S3 bucket which the upload belongs to.
     * @param multipartUpload - The multipart upload
     * @return The total size in bytes
     */
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
