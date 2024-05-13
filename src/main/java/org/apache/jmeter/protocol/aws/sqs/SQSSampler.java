package org.apache.jmeter.protocol.aws.sqs;

import org.apache.jmeter.protocol.aws.AWSClientSDK2;
import org.apache.jmeter.protocol.aws.AWSSampler;
import org.apache.jmeter.protocol.java.sampler.JavaSamplerContext;
import org.apache.jmeter.samplers.Entry;
import org.apache.jmeter.samplers.SampleResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.core.SdkClient;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.sqs.SqsClient;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

/**
 * SQS Sampler class to connect and publish messages in SQS queues.
 * 
 * @author Kei Tashima
 * @since 05/13/2024
 * @see
 */
public abstract class SQSSampler extends AWSSampler implements AWSClientSDK2 {

    /**
     * Log attribute.
     */
    protected static Logger log = LoggerFactory.getLogger(SQSSampler.class);

    /**
     * SQS Queue Name.
     */
    protected static final String SQS_QUEUE_NAME = "sqs_queue_name";

    /**
     * AWS SQS Client
     */
    protected SqsClient sqsClient;

    protected abstract String getSetupTestLogMessage();

    protected abstract String getTeardownTestLogMessage();

    /**
     * Create AWS SQS Client.
     * 
     * @param credentials
     *                    Represents the input of JMeter Java Request parameters.
     * @return SqsClient extends SdkClient super class.
     */
    @Override
    public SdkClient createSdkClient(Map<String, String> credentials) {
        return SqsClient.builder()
                .region(Region.of(getAWSRegion(credentials)))
                .credentialsProvider(getAwsCredentialsProvider(credentials))
                .build();
    }

    /**
     * Read test parameters and initialize AWS SQS client.
     * 
     * @param context to get the arguments values on Java Sampler.
     */
    @Override
    public void setupTest(JavaSamplerContext context) {

        log.info("Setup SQS Sampler. {}", this.getSetupTestLogMessage());
        Map<String, String> credentials = new HashMap<>();

        context.getParameterNamesIterator().forEachRemaining(k -> {
            credentials.put(k, context.getParameter(k));
            log.info("Parameter: {}, value: {}", k, credentials.get(k));
        });

        log.info("Create SQS Cleint.");
        sqsClient = (SqsClient) createSdkClient(credentials);
    }

    /**
     * Close AWS SQS Client after run single thread.
     * 
     * @param context
     *                Arguments values on Java Sampler.
     */
    @Override
    public void teardownTest(JavaSamplerContext context) {
        log.info("Teardown SQS Sampler. {}", this.getTeardownTestLogMessage());
        log.info("Close SQS Client.");
        Optional.ofNullable(sqsClient)
                .ifPresent(client -> client.close());
    }
}
