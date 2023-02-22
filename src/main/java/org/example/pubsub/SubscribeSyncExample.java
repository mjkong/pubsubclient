package org.example.pubsub;

import com.google.api.serviceusage.v1beta1.*;
import com.google.cloud.pubsub.v1.stub.GrpcSubscriberStub;
import com.google.cloud.pubsub.v1.stub.SubscriberStub;
import com.google.cloud.pubsub.v1.stub.SubscriberStubSettings;
import com.google.cloud.video.transcoder.v1.*;
import com.google.common.collect.Iterators;
import com.google.pubsub.v1.*;
import org.example.transcoder.TranscoderUtils;

import java.io.File;
import java.io.IOException;
import java.util.*;

public class SubscribeSyncExample {

    final static String inputBucket = "gs://trascodertest";
    final static String outputBucket = "gs://transcodertestoutput";

    public static void main(String... args) throws Exception {
        // TODO(developer): Replace these variables before running the sample.
        String projectId = "mjprj-02";
        String region = "asia-southeast1";
        String subscriptionId = "newsAddedVideo-sub";
        Integer defaultNumOfMessages = 10;
        String preset = "preset/web-hd";

//        subscribeSyncExample(projectId, region, subscriptionId,defaultNumOfMessages);

        TranscoderServiceClient.ListJobsPagedResponse listjobs = TranscoderUtils.listJobs(projectId, region);
        for(Job job : listjobs.iterateAll()){
            System.out.println(job.getName());
            System.out.println(job.getState());
        }

//        long limits = getTranscoderAPIQuota(region);

//        TranscoderUtils.deleteJob(projectId, region, "a745c0f0-7243-4a2c-90e6-4a1cb8f21ae4");
//        TranscoderUtils.deleteJob(projectId, region, "90edbd3d-b322-4884-a655-8d6b53970032");
//        TranscoderUtils.deleteJob(projectId, region, "23468be3-785b-4b59-9470-62976d13f51f");
//        TranscoderUtils.deleteJob(projectId, region, "3ce48149-1d28-486d-9d29-c540c32cc890");

    }

    public static void subscribeSyncExample(
        String projectId, String region, String subscriptionId, Integer defaultNumOfMessages) throws IOException {
            SubscriberStubSettings subscriberStubSettings =
                SubscriberStubSettings.newBuilder()
                        .setTransportChannelProvider(
                                SubscriberStubSettings.defaultGrpcTransportProviderBuilder()
                                        .setMaxInboundMessageSize(20 * 1024 * 1024) // 20MB (maximum message size).
                                        .build())
                        .build();

        try (SubscriberStub subscriber = GrpcSubscriberStub.create(subscriberStubSettings)) {

            int numOfMessages = 0;

            int limits = Long.valueOf(TranscoderUtils.getTranscoderAPIQuota(region)).intValue();

            //
            int createdJobCnt = PubSubUtils.getReceivedMessageList(projectId,"createJobs-sub", 100).size();
//            int createdJobCnt = Iterators.size(TranscoderUtils.listJobs(projectId, region).iterateAll().iterator());

            if(createdJobCnt >= limits) {
                System.out.println("The quota limit for Transcoder has been exceeded");
                return;
            }

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
                String topicDestination = "projects/mjprj-02/topics/createdJobs";

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


    public static void createJobFromPreset(
            String projectId, String location, String inputUri, String outputUri, String preset)
            throws IOException {
        // Initialize client that will be used to send requests. This client only needs to be created
        // once, and can be reused for multiple requests.
        try (TranscoderServiceClient transcoderServiceClient = TranscoderServiceClient.create()) {

//            TranscoderServiceClient.ListJobsPagedResponse listJobs= transcoderServiceClient.listJobs();

            var createJobRequest =
                    CreateJobRequest.newBuilder()
                            .setJob(
                                    Job.newBuilder()
                                            .setInputUri(inputUri)
                                            .setOutputUri(outputUri)
                                            .setTemplateId(preset)
                                            .build())
                            .setParent(LocationName.of(projectId, location).toString())
                            .build();

            // Send the job creation request and process the response.
            Job job = transcoderServiceClient.createJob(createJobRequest);
            System.out.println("Job: " + job.getName());
        }
    }


}
