package my.seoj.aws.s3.orphan.parts.cleanup;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import com.amazonaws.services.s3.AmazonS3;

@Service
public class CleanupService
{
    @Autowired
    private AmazonS3 s3;

    public void cleanup(String s3BucketName)
    {
        // TODO Auto-generated method stub

    }
}
