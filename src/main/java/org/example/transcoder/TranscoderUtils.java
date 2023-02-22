package org.example.transcoder;

import com.google.api.serviceusage.v1beta1.*;
import com.google.cloud.video.transcoder.v1.*;

import com.google.cloud.video.transcoder.v1.VideoStream;
import com.google.cloud.video.transcoder.v1.VideoStream.H264CodecSettings;

import java.io.IOException;
import java.util.Iterator;
import java.util.List;

public class TranscoderUtils {

    // Gets a job.
    public static Job getJob(String projectId, String location, String jobId) throws IOException {
        // Initialize client that will be used to send requests. This client only needs to be created
        // once, and can be reused for multiple requests.
        Job job = null;
        try (TranscoderServiceClient transcoderServiceClient = TranscoderServiceClient.create()) {
            JobName jobName =
                    JobName.newBuilder().setProject(projectId).setLocation(location).setJob(jobId).build();
            var getJobRequest = GetJobRequest.newBuilder().setName(jobName.toString()).build();

            // Send the get job request and process the response.
            job = transcoderServiceClient.getJob(getJobRequest);
        }

        return job;
    }

    // Lists the jobs for a given location.
    public static TranscoderServiceClient.ListJobsPagedResponse listJobs(String projectId, String location) throws IOException {
        // Initialize client that will be used to send requests. This client only needs to be created
        // once, and can be reused for multiple requests.
        TranscoderServiceClient.ListJobsPagedResponse response = null;

        try (TranscoderServiceClient transcoderServiceClient = TranscoderServiceClient.create()) {

            var listJobsRequest =
                    ListJobsRequest.newBuilder()
                            .setParent(LocationName.of(projectId, location).toString())
                            .build();

            // Send the list jobs request and process the response.
            response = transcoderServiceClient.listJobs(listJobsRequest);

//            for (Job job : response.iterateAll()) {
//                System.out.println(job.getName());
//            }
        }

        return response;
    }

    // Gets the state of a job.
    public static Job.ProcessingState getJobState(String projectId, String location, String jobId)
            throws IOException {
        // Initialize client that will be used to send requests. This client only needs to be created
        // once, and can be reused for multiple requests.
        Job.ProcessingState status = null;
        try (TranscoderServiceClient transcoderServiceClient = TranscoderServiceClient.create()) {
            JobName jobName =
                    JobName.newBuilder().setProject(projectId).setLocation(location).setJob(jobId).build();
            var getJobRequest = GetJobRequest.newBuilder().setName(jobName.toString()).build();

            // Send the get job request and process the response.
            Job job = transcoderServiceClient.getJob(getJobRequest);
            status = job.getState();
            System.out.println("Job state: " + job.getState());
        }
        return status;
    }

    public static Job createJobFromConfig(String projectId, String location, String inputUri, String outputUri, String topicDestination) throws IOException {
        // Initialize client that will be used to send requests. This client only needs to be created
        // once, and can be reused for multiple requests.

        Job job = null;
        try (TranscoderServiceClient transcoderServiceClient = TranscoderServiceClient.create()) {

            VideoStream videoStream0 =
                    VideoStream.newBuilder()
                            .setH264(
                                    H264CodecSettings.newBuilder()
                                            .setBitrateBps(550000)
                                            .setFrameRate(60)
                                            .setHeightPixels(360)
                                            .setWidthPixels(640))
                            .build();

            VideoStream videoStream1 =
                    VideoStream.newBuilder()
                            .setH264(
                                    H264CodecSettings.newBuilder()
                                            .setBitrateBps(2500000)
                                            .setFrameRate(60)
                                            .setHeightPixels(720)
                                            .setWidthPixels(1280))
                            .build();

            AudioStream audioStream0 =
                    AudioStream.newBuilder().setCodec("aac").setBitrateBps(64000).build();

            JobConfig config =
                    JobConfig.newBuilder()
                            .addInputs(Input.newBuilder().setKey("input0").setUri(inputUri))
                            .setOutput(Output.newBuilder().setUri(outputUri))
                            .addElementaryStreams(
                                    ElementaryStream.newBuilder()
                                            .setKey("video_stream0")
                                            .setVideoStream(videoStream0))
                            .addElementaryStreams(
                                    ElementaryStream.newBuilder()
                                            .setKey("video_stream1")
                                            .setVideoStream(videoStream1))
                            .addElementaryStreams(
                                    ElementaryStream.newBuilder()
                                            .setKey("audio_stream0")
                                            .setAudioStream(audioStream0))
                            .addMuxStreams(
                                    MuxStream.newBuilder()
                                            .setKey("sd")
                                            .setContainer("mp4")
                                            .addElementaryStreams("video_stream0")
                                            .addElementaryStreams("audio_stream0")
                                            .build())
                            .addMuxStreams(
                                    MuxStream.newBuilder()
                                            .setKey("hd")
                                            .setContainer("mp4")
                                            .addElementaryStreams("video_stream1")
                                            .addElementaryStreams("audio_stream0")
                                            .build())
                            .setPubsubDestination(
                                    PubsubDestination.newBuilder()
                                            .setTopic(topicDestination)
                                            .build()
                            )
                            .build();

            var createJobRequest =
                    CreateJobRequest.newBuilder()
                            .setJob(
                                    Job.newBuilder()
                                            .setInputUri(inputUri)
                                            .setOutputUri(outputUri)
                                            .setConfig(config)
                                            .build())
                            .setParent(LocationName.of(projectId, location).toString())
                            .build();

            // Send the job creation request and process the response.
            job = transcoderServiceClient.createJob(createJobRequest);
            System.out.println("Job: " + job.getName());
        }
        return job;
    }

    // Deletes a job.
    public static void deleteJob(String projectId, String location, String jobId) throws IOException {
        // Initialize client that will be used to send requests. This client only needs to be created
        // once, and can be reused for multiple requests.
        try (TranscoderServiceClient transcoderServiceClient = TranscoderServiceClient.create()) {
            JobName jobName =
                    JobName.newBuilder().setProject(projectId).setLocation(location).setJob(jobId).build();
            var deleteJobRequest = DeleteJobRequest.newBuilder().setName(jobName.toString()).build();

            // Send the delete job request and process the response.
            transcoderServiceClient.deleteJob(deleteJobRequest);
            System.out.println("Deleted job");
        }
    }

    public static long getTranscoderAPIQuota(String region){

        long limits = 0;

        ServiceUsageClient serviceUsageClient = null;
        try {
            serviceUsageClient = ServiceUsageClient.create();
            GetConsumerQuotaMetricRequest request = GetConsumerQuotaMetricRequest.newBuilder()
                    .setName("projects/271512203165/services/transcoder.googleapis.com/consumerQuotaMetrics/transcoder.googleapis.com%2Fconcurrent-job-count")
                    .setView(QuotaView.FULL).build();
            ConsumerQuotaMetric metric = serviceUsageClient.getConsumerQuotaMetric(request);

            List<QuotaBucket> buckets = metric.getConsumerQuotaLimitsList().get(0).getQuotaBucketsList();
            Iterator iter = buckets.iterator();

            while(iter.hasNext()){
                QuotaBucket bucket = (QuotaBucket) iter.next();
                if(bucket.getDimensionsMap().containsValue(region)){
                    limits = bucket.getDefaultLimit();
                    break;
                }
            }

        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        return limits;
    }
}
