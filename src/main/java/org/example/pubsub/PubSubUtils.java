package org.example.pubsub;

import com.google.cloud.pubsub.v1.stub.GrpcSubscriberStub;
import com.google.cloud.pubsub.v1.stub.SubscriberStub;
import com.google.cloud.pubsub.v1.stub.SubscriberStubSettings;
import com.google.pubsub.v1.*;

import java.io.IOException;
import java.util.List;

public class PubSubUtils {

    public static List<ReceivedMessage> getReceivedMessageList(String projectId, String subscriptionId, Integer numOfMessages) throws IOException {

        PullResponse pullResponse = null;

        SubscriberStubSettings subscriberStubSettings =
                SubscriberStubSettings.newBuilder()
                        .setTransportChannelProvider(
                                SubscriberStubSettings.defaultGrpcTransportProviderBuilder()
                                        .setMaxInboundMessageSize(20 * 1024 * 1024) // 20MB (maximum message size).
                                        .build())
                        .build();

        try (SubscriberStub subscriber = GrpcSubscriberStub.create(subscriberStubSettings)) {
            String subscriptionName = ProjectSubscriptionName.format(projectId, subscriptionId);
            PullRequest pullRequest = PullRequest.newBuilder().setMaxMessages(numOfMessages).setSubscription(subscriptionName).build();

            // Use pullCallable().futureCall to asynchronously perform this operation.
            pullResponse = subscriber.pullCallable().call(pullRequest);
        }

        return pullResponse.getReceivedMessagesList();
    }

    public static void ack(String subscriptionName, List<String> ackIds) throws Exception {

        SubscriberStubSettings subscriberStubSettings =
                SubscriberStubSettings.newBuilder()
                        .setTransportChannelProvider(
                                SubscriberStubSettings.defaultGrpcTransportProviderBuilder()
                                        .setMaxInboundMessageSize(20 * 1024 * 1024) // 20MB (maximum message size).
                                        .build())
                        .build();
        try (SubscriberStub subscriber = GrpcSubscriberStub.create(subscriberStubSettings)) {

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
