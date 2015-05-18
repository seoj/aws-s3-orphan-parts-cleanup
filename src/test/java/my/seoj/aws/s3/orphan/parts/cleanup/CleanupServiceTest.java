package my.seoj.aws.s3.orphan.parts.cleanup;

import static org.junit.Assert.assertEquals;

import java.io.ByteArrayInputStream;
import java.util.Arrays;
import java.util.List;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.InitiateMultipartUploadRequest;
import com.amazonaws.services.s3.model.InitiateMultipartUploadResult;
import com.amazonaws.services.s3.model.ListMultipartUploadsRequest;
import com.amazonaws.services.s3.model.MultipartUpload;
import com.amazonaws.services.s3.model.MultipartUploadListing;
import com.amazonaws.services.s3.model.UploadPartRequest;

@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(classes = CleanupConfiguration.class)
public class CleanupServiceTest
{
    private static final int MIN_PART_SIZE = 1024 * 1024 * 5;

    @Autowired
    private AmazonS3 s3;

    @Autowired
    private CleanupService cleanupService;

    @Value("${test.s3.bucket.name}")
    private String s3BucketName;

    private String s3ObjectKey;
    private byte[] partContent;

    public CleanupServiceTest()
    {
        s3ObjectKey = String.valueOf(System.currentTimeMillis());

        partContent = new byte[MIN_PART_SIZE];
        Arrays.fill(partContent, (byte) 'a');
    }

    /**
     * Case 1:
     * <ol>
     * <li>Initiate upload</li>
     * <li>Upload a part</li>
     * <li>Request cleanup</li>
     * <li>Get active uploads</li>
     * </ol>
     * <p>
     * Expects no active uploads
     * </p>
     */
    @Test
    public void test()
    {
        String uploadId = initiateUpload();
        uploadPart(uploadId);
        cleanup();
        List<MultipartUpload> uploads = getActiveUploads();

        assertEquals("number of active uploads", 0, uploads.size());
    }

    private List<MultipartUpload> getActiveUploads()
    {
        ListMultipartUploadsRequest listMultipartUploadsRequest = new ListMultipartUploadsRequest(s3BucketName);
        MultipartUploadListing multipartUploadListing = s3.listMultipartUploads(listMultipartUploadsRequest);
        return multipartUploadListing.getMultipartUploads();
    }

    private void cleanup()
    {
        cleanupService.cleanup(s3BucketName);
    }

    private void uploadPart(String uploadId)
    {
        UploadPartRequest uploadPartRequest = new UploadPartRequest();
        uploadPartRequest.setBucketName(s3BucketName);
        uploadPartRequest.setKey(s3ObjectKey);
        uploadPartRequest.setUploadId(uploadId);
        uploadPartRequest.setInputStream(new ByteArrayInputStream(partContent));
        uploadPartRequest.setPartSize(partContent.length);
        uploadPartRequest.setPartNumber(1);

        s3.uploadPart(uploadPartRequest);
    }

    private String initiateUpload()
    {
        InitiateMultipartUploadRequest initiateMultipartUploadRequest = new InitiateMultipartUploadRequest(s3BucketName, s3ObjectKey);
        InitiateMultipartUploadResult initiateMultipartUploadResult = s3.initiateMultipartUpload(initiateMultipartUploadRequest);
        return initiateMultipartUploadResult.getUploadId();
    }
}
