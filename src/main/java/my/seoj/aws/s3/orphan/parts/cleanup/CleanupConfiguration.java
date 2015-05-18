package my.seoj.aws.s3.orphan.parts.cleanup;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.support.PropertySourcesPlaceholderConfigurer;
import org.springframework.core.io.FileSystemResource;

import com.amazonaws.ClientConfiguration;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;

@Configuration
@ComponentScan(basePackages = "my.seoj.aws.s3.orphan.parts.cleanup")
public class CleanupConfiguration
{
    @Bean
    public static PropertySourcesPlaceholderConfigurer propertySourcesPlaceholderConfigurer()
    {
        PropertySourcesPlaceholderConfigurer propertySourcesPlaceholderConfigurer = new PropertySourcesPlaceholderConfigurer();
        propertySourcesPlaceholderConfigurer.setLocation(new FileSystemResource(".properties"));
        return propertySourcesPlaceholderConfigurer;
    }

    @Value("${access.key}")
    private String accessKey;

    @Value("${secret.key}")
    private String secretKey;

    @Value("${proxy.host}")
    private String proxyHost;

    @Value("${proxy.port}")
    private Integer proxyPort;

    @Bean
    public AmazonS3 s3()
    {
        BasicAWSCredentials awsCredentials = new BasicAWSCredentials(accessKey, secretKey);

        ClientConfiguration clientConfiguration = new ClientConfiguration();
        clientConfiguration.setProxyHost(proxyHost);
        clientConfiguration.setProxyPort(proxyPort);

        return new AmazonS3Client(awsCredentials, clientConfiguration);
    }
}
