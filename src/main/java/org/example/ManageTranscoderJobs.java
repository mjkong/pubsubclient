package org.example;

import com.google.cloud.pubsub.v1.stub.GrpcSubscriberStub;
import com.google.cloud.pubsub.v1.stub.SubscriberStub;
import com.google.cloud.pubsub.v1.stub.SubscriberStubSettings;
import com.google.cloud.video.transcoder.v1.Job;
import com.google.cloud.video.transcoder.v1.TranscoderServiceClient;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.google.pubsub.v1.*;
import org.example.pubsub.PubSubUtils;
import org.example.transcoder.TranscoderUtils;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.logging.Logger;

public class ManageTranscoderJobs {

    public static void main(String... args) throws Exception {

        String projectId = "mjprj-02";
        // Transcoder API를 실행하기 위한 리전
        String transcoderAPIRegion = "asia-southeast1";
        // 변환해야할 동영상을 업로드하면 해당 정보가 저장되는 Topic의 Subscription.
        String newlyAddedVideoTopicSubId = "newsAddedVideo-sub";
        // Transcoder Job의 완료 정보가 저장되는 Topic의 Subscription.
        String createdJobsTopicSubId = "createJobs-sub";
        Integer defaultNumOfMessages = 10;
        String preset = "preset/web-hd";
        // 변환할 동영상을 업로드하기위한 버킷
        String inputBucket = "gs://trascodertest";
        // 변환 완료된 파일이 업로드 될 Bucket
        String outputBucket = "gs://transcodertestoutput";
        // Transcoder Job 생성시 입력하는 항목으로 Transcoder Job 의 결과가 입력되기 위한 Pub/Sub의 Topic
        String topicDestination = "projects/mjprj-02/topics/createdJobs";


        ManageTranscoderJobs.clearCompletedJobs(projectId, transcoderAPIRegion, createdJobsTopicSubId);
        ManageTranscoderJobs.createJobs(projectId, transcoderAPIRegion, newlyAddedVideoTopicSubId, defaultNumOfMessages, inputBucket, outputBucket, topicDestination);

    }

    public static void createJobs(String projectId, String region, String subscriptionId, Integer defaultNumOfMessages, String inputBucket, String outputBucket, String topicDestination) throws IOException {
        SubscriberStubSettings subscriberStubSettings =
            SubscriberStubSettings.newBuilder()
                .setTransportChannelProvider(
                    SubscriberStubSettings.defaultGrpcTransportProviderBuilder()
                        .setMaxInboundMessageSize(20 * 1024 * 1024) // 20MB (maximum message size).
                        .build())
                .build();

        try (SubscriberStub subscriber = GrpcSubscriberStub.create(subscriberStubSettings)) {

            int numOfMessages = 0;

            // Transcoder API의 Resource quota limit 조회
            int limits = Long.valueOf(TranscoderUtils.getTranscoderAPIQuota(region)).intValue();
            //Transcoder Job list를 기준으로 할 경우 TranscoderUtils.listJobs 를 활용
            int createdJobCnt = PubSubUtils.getReceivedMessageList(projectId,"createJobs-sub", 100).size();
//            int createdJobCnt = Iterators.size(TranscoderUtils.listJobs(projectId, region).iterateAll().iterator());

            if(createdJobCnt >= limits) {
                System.out.println("The quota limit for Transcoder has been exceeded");
                return;
            }

            // Quota Limit에서 현재 실행중인 Job 개수의 차 만큼 새로 job 생성
            numOfMessages = limits - createdJobCnt;

            String subscriptionName = ProjectSubscriptionName.format(projectId, subscriptionId);
            PullRequest pullRequest = PullRequest.newBuilder().setMaxMessages(numOfMessages).setSubscription(subscriptionName).build();

            // Use pullCallable().futureCall to asynchronously perform this operation.
            PullResponse pullResponse = subscriber.pullCallable().call(pullRequest);

            // Stop the program if the pull response is empty to avoid acknowledging
            // an empty list of ack IDs.
            if (pullResponse.getReceivedMessagesList().isEmpty()) {
                System.out.println("No message was pulled. Exiting.");
                return;
            }

            List<String> ackIds = new ArrayList<>();
            for (ReceivedMessage message : pullResponse.getReceivedMessagesList()) {
                PubsubMessage pubsubMessage = message.getMessage();
                String objectName = pubsubMessage.getAttributesOrThrow("objectId");
                String bucketId = pubsubMessage.getAttributesOrThrow("bucketId");
                String inputFile = inputBucket + File.separatorChar + objectName;
                String outputFileLocation = outputBucket + File.separatorChar + objectName + File.separatorChar;

                Job job = null;
                job = TranscoderUtils.createJobFromConfig(projectId,region, inputFile, outputFileLocation , topicDestination);
                if(job != null && job.getStateValue() != 4 ){
                    ackIds.add(message.getAckId());
                }
            }

            if(ackIds.size() > 0) {
                // Acknowledge received messages.
                AcknowledgeRequest acknowledgeRequest =
                    AcknowledgeRequest.newBuilder()
                        .setSubscription(subscriptionName)
                        .addAllAckIds(ackIds)
                        .build();

                // Use acknowledgeCallable().futureCall to asynchronously perform this operation.
                subscriber.acknowledgeCallable().call(acknowledgeRequest);
            }
        }
    }

    public static void clearCompletedJobs(String projectId, String transcoderAPIRegion, String createdJobsTopicSubId) throws Exception {

        List<String> ackIds = new ArrayList<>();
        int numOfMessages = 100;
        TranscoderServiceClient.ListJobsPagedResponse jobs = TranscoderUtils.listJobs(projectId, transcoderAPIRegion);
//        List<ReceivedMessage> messageList = PubSubUtils.getReceivedMessageList(projectId, createdJobsTopicSubId, 100);


        PullResponse pullResponse = null;

        SubscriberStubSettings subscriberStubSettings =
                SubscriberStubSettings.newBuilder()
                        .setTransportChannelProvider(
                                SubscriberStubSettings.defaultGrpcTransportProviderBuilder()
                                        .setMaxInboundMessageSize(20 * 1024 * 1024) // 20MB (maximum message size).
                                        .build())
                        .build();

        try (SubscriberStub subscriber = GrpcSubscriberStub.create(subscriberStubSettings)) {
            String subscriptionName = ProjectSubscriptionName.format(projectId, createdJobsTopicSubId);
            PullRequest pullRequest = PullRequest.newBuilder().setMaxMessages(numOfMessages).setSubscription(subscriptionName).build();

            // Use pullCallable().futureCall to asynchronously perform this operation.
            pullResponse = subscriber.pullCallable().call(pullRequest);

            for(ReceivedMessage message: pullResponse.getReceivedMessagesList()){
                String data = message.getMessage().getData().toStringUtf8();
                JsonObject rootObject = JsonParser.parseString(data).getAsJsonObject();
                JsonObject jobObj = rootObject.get("job").getAsJsonObject();
                String jobName = jobObj.get("name").getAsString();
                String jobStatus = jobObj.get("state").getAsString();

                if("SUCCEEDED".equalsIgnoreCase(jobStatus)){
                    String jobId = jobName.split("jobs/")[1];
                    TranscoderUtils.deleteJob(projectId, transcoderAPIRegion, jobId);
                    ackIds.add(message.getAckId());
                }
            }

            if(ackIds.size() > 0) {

                AcknowledgeRequest acknowledgeRequest =
                        AcknowledgeRequest.newBuilder()
                                .setSubscription(subscriptionName)
                                .addAllAckIds(ackIds)
                                .build();

                // Use acknowledgeCallable().futureCall to asynchronously perform this operation.
                subscriber.acknowledgeCallable().call(acknowledgeRequest);
            }
        }
    }
}
