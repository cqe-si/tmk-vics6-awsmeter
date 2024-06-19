package org.apache.jmeter.protocol.aws.s3;

import org.apache.jmeter.config.Argument;
import org.apache.jmeter.protocol.aws.AWSClientSDK2;
import org.apache.jmeter.protocol.aws.AWSSampler;
import org.apache.jmeter.protocol.java.sampler.JavaSamplerContext;
import org.apache.jmeter.config.Arguments;
import org.apache.jmeter.samplers.SampleResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.core.ResponseBytes;
import software.amazon.awssdk.core.ResponseInputStream;
import software.amazon.awssdk.core.SdkClient;
import software.amazon.awssdk.core.async.AsyncResponseTransformer;
import software.amazon.awssdk.core.sync.ResponseTransformer;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.GetObjectResponse;
import software.amazon.awssdk.services.s3.model.S3Exception;

import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * S3 ダウンロード実行サンプラー
 * 
 * @author KeiTashima
 * @since 05/22/2024
 * @see
 */
public class S3DownloadSampler extends AWSSampler implements AWSClientSDK2 {

    protected static Logger log = LoggerFactory.getLogger(S3DownloadSampler.class);

    private static final String S3_BUCKET_NAME = "s3_bucket_name";
    private static final String S3_OBJECT_KEY = "s3_object_key";
    private static final String DOWNLOAD_PATH = "download_path";
    private static final String BUFFER_SIZE = "buffer_size";

    private static final List<Argument> S3_PARAMETERS = Stream.of(
            new Argument(S3_BUCKET_NAME, EMPTY, "S3のバケット名"),
            new Argument(S3_OBJECT_KEY, EMPTY, "バケットの中のS3のパス"),
            new Argument(DOWNLOAD_PATH, EMPTY,
                    "ダウンロード先のパス（ローカルのパス）。空の場合はファイル保存なしモードとして動作するが、DL中にディスクにフラッシュしないのでメモリ消費が多くなる"),
            new Argument(BUFFER_SIZE, "1048576", "ダウンロード時にファイル保存する場合のバッファサイズ（バイト）"))
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

        log.info("Setup S3 Downloader.");
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
        sampleResultStart(result, String.format("Bucket Name: %s %nObject Key: %s %nDownload Path: %s",
                context.getParameter(S3_BUCKET_NAME),
                context.getParameter(S3_OBJECT_KEY),
                context.getParameter(DOWNLOAD_PATH)));

        try {
            log.info("Downloading Object.");
            GetObjectRequest getObjectRequest = GetObjectRequest.builder()
                    .bucket(context.getParameter(S3_BUCKET_NAME))
                    .key(context.getParameter(S3_OBJECT_KEY))
                    .build();

            long startTime = System.currentTimeMillis();

            if (context.getParameter(DOWNLOAD_PATH).isEmpty()) {
                ResponseBytes<GetObjectResponse> objectBytes = s3Client.getObject(getObjectRequest,
                        ResponseTransformer.toBytes());
                log.info("Received Object. File size: {} bytes", objectBytes.asByteArray().length);
            } else {
                try (ResponseInputStream<GetObjectResponse> s3ObjectStream = s3Client.getObject(getObjectRequest,
                        ResponseTransformer.toInputStream());
                        OutputStream outputStream = new FileOutputStream(context.getParameter(DOWNLOAD_PATH))) {

                    long contentLength = s3ObjectStream.response().contentLength();
                    int bufferSize = Integer.parseInt(context.getParameter(BUFFER_SIZE));
                    byte[] buffer = new byte[bufferSize];
                    int bytesRead;
                    long totalBytesRead = 0;
                    int lastLoggedProgress = 0;
                    while ((bytesRead = s3ObjectStream.read(buffer)) != -1) {
                        outputStream.write(buffer, 0, bytesRead);
                        totalBytesRead += bytesRead;
                        int progress = (int) ((double) totalBytesRead / contentLength * 100);
                        if (log.isInfoEnabled() && progress / 10 > lastLoggedProgress) {
                            lastLoggedProgress = progress / 10;
                            log.info("Receiving Object: {}% ({} / {} bytes)", String.format("%.1f", (double) progress),
                                    totalBytesRead, contentLength);
                        }
                    }
                    outputStream.flush();
                }
            }

            long endTime = System.currentTimeMillis();
            long elapsedTime = endTime - startTime; // ダウンロードにかかった時間（ミリ秒）
            log.info("Download time: {} ms", elapsedTime);

            sampleResultSuccess(result, "Download successful.");
        } catch (S3Exception | IOException e) {
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