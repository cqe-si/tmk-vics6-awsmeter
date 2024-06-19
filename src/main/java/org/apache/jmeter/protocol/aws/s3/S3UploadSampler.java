package org.apache.jmeter.protocol.aws.s3;

import org.apache.jmeter.config.Argument;
import org.apache.jmeter.protocol.aws.AWSClientSDK2;
import org.apache.jmeter.protocol.aws.AWSSampler;
import org.apache.jmeter.protocol.java.sampler.JavaSamplerContext;
import org.apache.jmeter.config.Arguments;
import org.apache.jmeter.samplers.SampleResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.core.SdkClient;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;
import software.amazon.awssdk.services.s3.model.S3Exception;

import java.io.File;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * S3 アップロード実行サンプラー
 * 
 * @see
 */
public class S3UploadSampler extends AWSSampler implements AWSClientSDK2 {

    protected static Logger log = LoggerFactory.getLogger(S3UploadSampler.class);

    private static final String S3_BUCKET_NAME = "s3_bucket_name";
    private static final String S3_OBJECT_KEY = "s3_object_key";
    private static final String UPLOAD_PATH = "upload_path";

    private static final List<Argument> S3_PARAMETERS = Stream.of(
            new Argument(S3_BUCKET_NAME, EMPTY, "S3のバケット名"),
            new Argument(S3_OBJECT_KEY, EMPTY, "バケットの中のS3のパス"),
            new Argument(UPLOAD_PATH, EMPTY, "アップロードするファイルのローカルパス"))
            .collect(Collectors.toList());

    private S3Client s3Client;

    @Override
    public SdkClient createSdkClient(Map<String, String> credentials) {
        return S3Client.builder()
                .region(Region.of(getAWSRegion(credentials)))
                .credentialsProvider(getAwsCredentialsProvider(credentials))
                .build();
    }

    @Override
    public Arguments getDefaultParameters() {
        Arguments defaultParameters = new Arguments();
        defaultParameters.setArguments(Stream.of(AWS_PARAMETERS, S3_PARAMETERS)
                .flatMap(List::stream)
                .collect(Collectors.toList()));
        return defaultParameters;
    }

    @Override
    public void setupTest(JavaSamplerContext context) {
        log.info("Setup S3 Uploader.");
        Map<String, String> credentials = new HashMap<>();

        context.getParameterNamesIterator()
                .forEachRemaining(k -> {
                    credentials.put(k, context.getParameter(k));
                    log.info("Parameter: " + k + ", value: " + credentials.get(k));
                });

        log.info("Create S3 Client.");
        s3Client = (S3Client) createSdkClient(credentials);
    }

    @Override
    public SampleResult runTest(JavaSamplerContext context) {
        SampleResult result = newSampleResult();
        sampleResultStart(result, String.format("Bucket Name: %s %nObject Key: %s %nUpload Path: %s",
                context.getParameter(S3_BUCKET_NAME),
                context.getParameter(S3_OBJECT_KEY),
                context.getParameter(UPLOAD_PATH)));

        try {
            log.info("Uploading Object.");
            PutObjectRequest putObjectRequest = PutObjectRequest.builder()
                    .bucket(context.getParameter(S3_BUCKET_NAME))
                    .key(context.getParameter(S3_OBJECT_KEY))
                    .build();

            File file = new File(context.getParameter(UPLOAD_PATH));
            if (!file.exists() || !file.isFile()) {
                throw new IllegalArgumentException("The file does not exist or is not a file: " + context.getParameter(UPLOAD_PATH));
            }

            long startTime = System.currentTimeMillis();

            s3Client.putObject(putObjectRequest, file.toPath());

            long endTime = System.currentTimeMillis();
            long elapsedTime = endTime - startTime; // アップロードにかかった時間（ミリ秒）
            log.info("Upload time: {} ms", elapsedTime);

            sampleResultSuccess(result, "Upload successful.");
        } catch (S3Exception | IllegalArgumentException e) {
            sampleResultFail(result, e.getMessage(), e.toString());
        }

        return result;
    }

    @Override
    public void teardownTest(JavaSamplerContext context) {
        log.info("Close S3 Client.");
        Optional.ofNullable(s3Client)
                .ifPresent(client -> client.close());
    }
}
