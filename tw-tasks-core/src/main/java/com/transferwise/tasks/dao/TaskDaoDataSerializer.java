package com.transferwise.tasks.dao;

import com.transferwise.common.baseutils.ExceptionUtils;
import com.transferwise.tasks.CompressionAlgorithm;
import com.transferwise.tasks.ITasksService.AddTaskRequest.CompressionRequest;
import com.transferwise.tasks.TasksProperties;
import java.io.IOException;
import java.io.InputStream;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;
import net.jpountz.lz4.LZ4BlockInputStream;
import net.jpountz.lz4.LZ4BlockOutputStream;
import net.jpountz.lz4.LZ4Factory;
import org.apache.commons.io.IOUtils;
import org.apache.commons.io.input.UnsynchronizedByteArrayInputStream;
import org.apache.commons.io.output.UnsynchronizedByteArrayOutputStream;
import org.springframework.beans.factory.annotation.Autowired;

public class TaskDaoDataSerializer implements ITaskDaoDataSerializer {

  private static final int COMPRESSION_NONE = 0;
  private static final int COMPRESSION_GZIP = 1;
  private static final int COMPRESSION_LZ4 = 2;

  @Autowired
  private TasksProperties tasksProperties;

  @Override
  public SerializeResult serialize(byte[] inputData, CompressionRequest compressionRequest) {
    return ExceptionUtils.doUnchecked(() -> {
      CompressionRequest compression = compressionRequest;

      if (compression == null) {
        TasksProperties.Compression globalCompression = tasksProperties.getCompression();
        if (inputData.length <= globalCompression.getMinSize()) {
          compression = new CompressionRequest().setAlgorithm(globalCompression.getAlgorithm())
              .setBlockSizeBytes(globalCompression.getBlockSizeBytes()).setLevel(globalCompression.getLevel());
        }
      }

      if (compression == null || compression.getAlgorithm() == null || compression.getAlgorithm() == CompressionAlgorithm.NONE) {
        return doNotCompress(inputData);
      }

      CompressionAlgorithm algorithm = compression.getAlgorithm();
      if (algorithm == CompressionAlgorithm.RANDOM) {
        algorithm = CompressionAlgorithm.getRandom();
      }

      if (algorithm == CompressionAlgorithm.NONE) {
        return doNotCompress(inputData);
      }
      if (algorithm == CompressionAlgorithm.GZIP) {
        return compressGzip(inputData);
      }
      if (algorithm == CompressionAlgorithm.LZ4) {
        return compressLz4(inputData, compression);
      }

      throw new IllegalStateException("No support for compression algorithm of " + algorithm + ".");
    });
  }

  protected SerializeResult doNotCompress(byte[] inputData) {
    return new SerializeResult().setDataFormat(COMPRESSION_NONE).setData(inputData);
  }

  protected SerializeResult compressLz4(byte[] inputData, CompressionRequest compression) throws IOException {
    UnsynchronizedByteArrayOutputStream bos = new UnsynchronizedByteArrayOutputStream();
    try (LZ4BlockOutputStream compressOut = new LZ4BlockOutputStream(bos, compression.getBlockSizeBytes(),
        LZ4Factory.fastestJavaInstance().fastCompressor())) {
      compressOut.write(inputData);
    }
    return new SerializeResult().setDataFormat(COMPRESSION_LZ4).setData(bos.toByteArray());
  }

  protected SerializeResult compressGzip(byte[] inputData) throws IOException {
    UnsynchronizedByteArrayOutputStream bos = new UnsynchronizedByteArrayOutputStream();
    try (GZIPOutputStream compressOut = new GZIPOutputStream(bos)) {
      compressOut.write(inputData);
    }
    return new SerializeResult().setDataFormat(COMPRESSION_GZIP).setData(bos.toByteArray());
  }

  @Override
  public byte[] deserialize(int dataFormat, byte[] data) {
    return ExceptionUtils.doUnchecked(() -> {
      if (data == null) {
        return null;
      }

      if (dataFormat == COMPRESSION_GZIP) {
        UnsynchronizedByteArrayInputStream bis = new UnsynchronizedByteArrayInputStream(data);
        try (InputStream is = new GZIPInputStream(bis)) {
          return IOUtils.toByteArray(is);
        }
      }

      if (dataFormat == COMPRESSION_LZ4) {
        UnsynchronizedByteArrayInputStream bis = new UnsynchronizedByteArrayInputStream(data);
        try (InputStream is = new LZ4BlockInputStream(bis, LZ4Factory.fastestJavaInstance().fastDecompressor())) {
          return IOUtils.toByteArray(is);
        }
      }

      return data;
    });
  }
}
