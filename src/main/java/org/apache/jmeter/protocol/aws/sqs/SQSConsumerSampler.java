package org.apache.jmeter.protocol.aws.sqs;

import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import com.google.gson.Gson;

import org.apache.jmeter.config.Argument;
import org.apache.jmeter.config.Arguments;
import org.apache.jmeter.protocol.java.sampler.JavaSamplerContext;
import org.apache.jmeter.samplers.SampleResult;
import org.apache.jmeter.threads.JMeterContextService;
import org.apache.jmeter.threads.JMeterVariables;

import software.amazon.awssdk.services.sqs.model.DeleteMessageRequest;
import software.amazon.awssdk.services.sqs.model.GetQueueUrlRequest;
import software.amazon.awssdk.services.sqs.model.Message;
import software.amazon.awssdk.services.sqs.model.MessageAttributeValue;
import software.amazon.awssdk.services.sqs.model.ReceiveMessageRequest;
import software.amazon.awssdk.services.sqs.model.ReceiveMessageResponse;
import software.amazon.awssdk.services.sqs.model.SqsException;

/**
 * SQS メッセージの受信サンプラー
 * ※ FIFOのみ対応。もしかしたらスタンダードは別の処理が必要かも
 * ※ キューを受信時、自動でキューメッセージを削除します。キュー失敗時の処理対応は未実装。
 * 
 * @author KeiTashima
 * @since 05/13/2024
 * @see
 */
public class SQSConsumerSampler extends SQSSampler {

    /**
     * メッセージのポーリング時間の設定
     */
    protected static final String SQS_WAIT_TIME_SECONDS = "sqs_wait_time_seconds";

    /**
     * 可視性タイムアウトの設定
     */
    protected static final String SQS_VISIBILITY_TIMEOUT = "sqs_visibility_timeout";

    /**
     * メッセージ受信したとき、受信メッセージボディを格納する変数名
     */
    protected static final String SQS_RECEIVED_MESSAGE_BODY_REF_NAME = "sqs_received_message_body_ref_name";

    /**
     * メッセージ受信したとき、受信メッセージ属性を格納する変数名
     */
    protected static final String SQS_RECEIVED_MESSAGE_ATTRIBUTES_REF_NAME = "sqs_received_message_attributes_ref_name";

    /**
     * List of Arguments to SQS FIFO Queue.
     */
    private static final List<Argument> SQS_PARAMETERS = Stream.of(
            new Argument(SQS_QUEUE_NAME, EMPTY),
            new Argument(SQS_WAIT_TIME_SECONDS, "10", "キューにメッセージが到着するまでの待機時間（秒単位）。メッセージが利用可能な場合、指定した時間よりも早く応答が返されます。"),
            new Argument(SQS_VISIBILITY_TIMEOUT, "30", "メッセージがキューから非表示になるまでの時間（秒単位）。"),
            new Argument(SQS_RECEIVED_MESSAGE_BODY_REF_NAME, "received_message_body", "受信メッセージボディを格納する変数名"),
            new Argument(SQS_RECEIVED_MESSAGE_ATTRIBUTES_REF_NAME, "received_message_attribute", "受信メッセージ属性を格納する変数名"))
            .collect(Collectors.toList());

    /**
     * Initial values for test parameter. They are show in Java Request test
     * sampler.
     * AWS parameters and SQS parameters.
     * 
     * @return Arguments to set as default on Java Request.
     */
    @Override
    public Arguments getDefaultParameters() {
        Arguments defaultParameters = new Arguments();
        defaultParameters.setArguments(Stream.of(AWS_PARAMETERS, SQS_PARAMETERS)
                .flatMap(List::stream)
                .collect(Collectors.toList()));
        return defaultParameters;
    }

    /**
     * Main method to execute the test on single thread. Create Message and publish
     * it on SQS FIFO Queue.
     * 
     * @param context
     *                Arguments values on Java Sampler.
     * @return SampleResult, captures data such as whether the test was successful,
     *         the response code and message, any request or response data, and the
     *         test start/end times
     */
    @Override
    public SampleResult runTest(JavaSamplerContext context) {

        SampleResult result = newSampleResult();
        sampleResultStart(result, String.format("Queue Name: %s %nMsg Body refName : %s %nMsg Attribute refName : %s",
                context.getParameter(SQS_QUEUE_NAME),
                context.getParameter(SQS_RECEIVED_MESSAGE_BODY_REF_NAME),
                context.getParameter(SQS_RECEIVED_MESSAGE_ATTRIBUTES_REF_NAME)));

        String messageVariableName = context.getParameter(SQS_RECEIVED_MESSAGE_BODY_REF_NAME, "");
        String attributeVariableName = context.getParameter(SQS_RECEIVED_MESSAGE_ATTRIBUTES_REF_NAME, "");

        try {
            log.info("try to receive message from queue");
            ReceiveMessageResponse msgRcv = sqsClient.receiveMessage(createReceiveMessageRequest(context));
            List<Message> messages = msgRcv.messages();

            if (!messages.isEmpty()) {
                Message message = messages.get(0);
                String messageBody = message.body();
                Map<String, MessageAttributeValue> attributes = message.messageAttributes();

                // メッセージ属性をJSON形式に変換
                Gson gson = new Gson();
                String jsonAttributes = gson.toJson(attributes);

                JMeterVariables variables = JMeterContextService.getContext().getVariables();
                variables.put(messageVariableName, messageBody); // メッセージを保存
                variables.put(attributeVariableName, jsonAttributes); // アトリビュートを保存

                log.info("message received. message: {}, attributes: {}", messageBody,
                        attributes.entrySet().stream().map(e -> String.format("%s: %s", e.getKey(), e.getValue()))
                                .collect(Collectors.joining(", ", "{", "}")) + " (size: " + attributes.size() + ")");

                // メッセージの削除処理
                log.info("delete message from queue");
                sqsClient.deleteMessage(deleteMessageRequest(context, message.receiptHandle()));

                result.setResponseData(messageBody, StandardCharsets.UTF_8.name());
                result.setDataType(SampleResult.TEXT);
                result.setResponseMessage("Message and attributes received successfully.");
                result.setSuccessful(true);
            } else {
                sampleResultSuccess(result, "No messages received.");
            }
        } catch (SqsException exc) {
            sampleResultFail(result, exc.awsErrorDetails().errorCode(), exc.awsErrorDetails().errorMessage());
        }

        return result;
    }

    private ReceiveMessageRequest createReceiveMessageRequest(JavaSamplerContext context) {
        return ReceiveMessageRequest.builder()
                .queueUrl(sqsClient.getQueueUrl(GetQueueUrlRequest.builder()
                        .queueName(context.getParameter(SQS_QUEUE_NAME))
                        .build())
                        .queueUrl())
                .messageAttributeNames("All") // メッセージ属性名を指定
                .maxNumberOfMessages(1) // 1回のリクエストで取得するメッセージ数
                .waitTimeSeconds(Integer.parseInt(context.getParameter(SQS_WAIT_TIME_SECONDS, "10")))
                .visibilityTimeout(Integer.parseInt(context.getParameter(SQS_VISIBILITY_TIMEOUT, "30")))
                .build();
    }

    private DeleteMessageRequest deleteMessageRequest(JavaSamplerContext context, String receiptHandle) {
        return DeleteMessageRequest.builder()
                .queueUrl(sqsClient.getQueueUrl(GetQueueUrlRequest.builder()
                        .queueName(context.getParameter(SQS_QUEUE_NAME))
                        .build())
                        .queueUrl())
                .receiptHandle(receiptHandle)
                .build();
    }

    @Override
    protected String getSetupTestLogMessage() {
        return SQSConsumerSampler.class.toString();
    }

    @Override
    protected String getTeardownTestLogMessage() {
        return SQSConsumerSampler.class.toString();
    }
}
