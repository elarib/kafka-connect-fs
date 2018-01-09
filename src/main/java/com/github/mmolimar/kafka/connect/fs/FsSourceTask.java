package com.github.mmolimar.kafka.connect.fs;

import com.github.mmolimar.kafka.connect.fs.file.FileMetadata;
import com.github.mmolimar.kafka.connect.fs.file.Offset;
import com.github.mmolimar.kafka.connect.fs.file.reader.FileReader;
import com.github.mmolimar.kafka.connect.fs.policy.Policy;
import com.github.mmolimar.kafka.connect.fs.util.ReflectionUtils;
import com.github.mmolimar.kafka.connect.fs.util.Version;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.source.SourceRecord;
import org.apache.kafka.connect.source.SourceTask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

public class FsSourceTask extends SourceTask {
    private static final Logger log = LoggerFactory.getLogger(FsSourceTask.class);

    private AtomicBoolean stop;
    private FsSourceTaskConfig config;
    private Policy policy;
    private FileReader reader;
    List<FileMetadata> files;
    private long batchSize;
    private long currentOffset = 0L;

    @Override
    public String version() {
        return Version.getVersion();
    }

    @Override
    public void start(Map<String, String> properties) {
        try {
            config = new FsSourceTaskConfig(properties);

            if (config.getClass(FsSourceTaskConfig.POLICY_CLASS).isAssignableFrom(Policy.class)) {
                throw new ConfigException("Policy class " +
                        config.getClass(FsSourceTaskConfig.POLICY_CLASS) + "is not a sublass of " + Policy.class);
            }
            if (config.getClass(FsSourceTaskConfig.FILE_READER_CLASS).isAssignableFrom(FileReader.class)) {
                throw new ConfigException("FileReader class " +
                        config.getClass(FsSourceTaskConfig.FILE_READER_CLASS) + "is not a sublass of " + FileReader.class);
            }
            this.batchSize = config.getLong(FsSourceTaskConfig.BATCH_SIZE);

            Class<Policy> policyClass = (Class<Policy>) Class.forName(properties.get(FsSourceTaskConfig.POLICY_CLASS));
            FsSourceTaskConfig taskConfig = new FsSourceTaskConfig(properties);
            policy = ReflectionUtils.makePolicy(policyClass, taskConfig);
        } catch (ConfigException ce) {
            log.error("Couldn't start FsSourceTask:", ce);
            throw new ConnectException("Couldn't start FsSourceTask due to configuration error", ce);
        } catch (Throwable t) {
            log.error("Couldn't start FsSourceConnector:", t);
            throw new ConnectException("A problem has occurred reading configuration:" + t.getMessage());
        }

        stop = new AtomicBoolean(false);
    }

    @Override
    public List<SourceRecord> poll() throws InterruptedException {
        final List<SourceRecord> results = new ArrayList<>();
        List<FileMetadata> filesToRemove = new ArrayList<>();
        while (stop != null && !stop.get() && !policy.hasEnded()) {
            log.info("Polling for new data");
            if (files == null) files = filesToProcess();

            log.info("Files : " + files.toString());

            Iterator<FileMetadata> filesIterator = files.iterator();


            while(filesIterator.hasNext() && results.size() < this.batchSize){
                FileMetadata metadata = filesIterator.next();
                log.info("Metadata", metadata);
                try {
                    if (this.reader == null) this.reader = policy.offer(metadata, context.offsetStorageReader());

                    log.info("Processing records for file {}", metadata);
                    while (reader.hasNext() && results.size() < this.batchSize) {
                        results.add(convert(metadata, reader.currentOffset(), reader.next()));
                    }
                    if(!reader.hasNext()) {
                        log.warn("Close reader");
                        this.reader.close();
                        this.reader = null;
                        filesToRemove.add(metadata);
                    }
                } catch (ConnectException | IOException e) {
                    //when an exception happens reading a file, the connector continues
                    log.error("Error reading file from FS: " + metadata.getPath() + ". Keep going...", e);
                }
            }
            // Remove all files polled
            if (!filesToRemove.isEmpty()) files.removeAll(filesToRemove);
            currentOffset += results.size();
            if(results.isEmpty())
                this.stop();
            log.info("Flush results size of : " + results.size() + ", current offset : " + currentOffset);
            return results;
        }

        return null;
    }

    private List<FileMetadata> filesToProcess() {
        try {
            return asStream(policy.execute())
                    .filter(metadata -> metadata.getLen() > 0)
                    .collect(Collectors.toList());
        } catch (IOException | ConnectException e) {
            //when an exception happens executing the policy, the connector continues
            log.error("Cannot retrive files to process from FS: " + policy.getURIs() + ". Keep going...", e);
            return Collections.EMPTY_LIST;
        }
    }

    private <T> Stream<T> asStream(Iterator<T> src) {
        Iterable<T> iterable = () -> src;
        return StreamSupport.stream(iterable.spliterator(), false);
    }

    private SourceRecord convert(FileMetadata metadata, Offset offset, Struct struct) {
        return new SourceRecord(
                new HashMap<String, Object>() {
                    {
                        put("path", metadata.getPath());
                        //TODO manage blocks
                        //put("blocks", metadata.getBlocks().toString());
                    }
                },
                Collections.singletonMap("offset", offset.getRecordOffset()),
                config.getTopic(),
                struct.schema(),
                struct
        );
    }

    @Override
    public void stop() {
        if (stop != null) {
            stop.set(true);
        }
        if (policy != null) {
            policy.interrupt();
        }
    }
}
